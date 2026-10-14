use events::{Message, Update};
use super::helpers::write_branchable_collection_block;
use super::*;

struct DeleteRetryAttempt<S: Store> {
    db: Arc<DB<S>>,
    collection_name: String,
    collection: Collection,
    doc_id: DocID,
    signing_config: Option<defra_core::signing::SigningConfig>,
    prepared_inputs: Option<query::prepared::PreparedInputMemo>,
}

impl<S: Store + 'static> storage::corekv::RetryAttemptFn for DeleteRetryAttempt<S> {
    fn run<'a>(
        &'a mut self,
        retry: &'a mut dyn storage::corekv::RetryTxn,
    ) -> storage::corekv::RetryAttemptFuture<'a> {
        Box::pin(async move {
            let mutator = AutoCommitMutator {
                db: Arc::clone(&self.db),
                prepared_inputs: self.prepared_inputs.clone(),
            };
            match mutator.run_delete_attempt(retry, self).await {
                Ok(value) => Ok(Box::new(value) as storage::corekv::RetryAttemptValue),
                Err(error) => Err(storage::corekv::RetryAttemptError::Abort(Box::new(error))),
            }
        })
    }
}

impl<S: Store + 'static> AutoCommitMutator<S> {
    pub(super) async fn delete_impl(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        self.db
            .check_node_access(None, acp::nac::NodePermission::DocumentDelete)
            .await
            .map_err(|e| query::error::QueryError::permission_denied(e.to_string()))?;

        let (_collection_guard, collection) = self.guarded_collection(collection_name).await?;

        let attempt = DeleteRetryAttempt {
            db: Arc::clone(&self.db),
            collection_name: collection_name.to_string(),
            collection: collection.clone(),
            doc_id: doc_id.clone(),
            signing_config: get_signing_config(),
            prepared_inputs: self.prepared_inputs.clone(),
        };
        let result = self
            .db
            .store()
            .transact_retry(storage::corekv::RetryOptions::new(4), Box::new(attempt))
            .await;
        let (canonical_doc_id, commit_result) = match result {
            Ok(value) => *value
                .downcast::<(DocID, Option<CommitArtifacts>)>()
                .map_err(|_| query::error::QueryError::execution("invalid delete retry result"))?,
            Err(error) => match error.downcast_abort::<query::error::QueryError>() {
                Ok(error) => return Err(error),
                Err(error) => {
                    return Err(crate::error::commit_query_error(
                        crate::error::Error::Storage(error.into_storage()),
                    ));
                }
            },
        };
        let Some((cid, block, col_data)) = commit_result else {
            return Ok(DeleteResult::new(doc_id.clone(), false));
        };
        let mut result = DeleteResult::with_commit(canonical_doc_id, true, cid, block);
        if let Some((col_cid, col_bytes)) = col_data {
            result.broadcast_cid = Some(col_cid);
            result.broadcast_block = Some(col_bytes);
        }
        Ok(result)
    }

    async fn run_delete_attempt(
        &self,
        retry: &mut dyn storage::corekv::RetryTxn,
        attempt: &DeleteRetryAttempt<S>,
    ) -> query::error::Result<(DocID, Option<CommitArtifacts>)> {
        let io = retry
            .begin_attempt_io()
            .map_err(|error| crate::error::commit_query_error(error.into()))?;
        let basic_txn = datastore::BasicTxn::from_retry_attempt_io(io, 0);
        let mut txn = DbTxn::<S>::new(basic_txn);

        self.db
            .check_node_access_in_txn(
                txn.acpstore().map_err(crate::error::commit_query_error)?,
                acp::nac::NodePermission::DocumentDelete,
            )
            .await
            .map_err(crate::error::commit_query_error)?;
        let datastore = txn.datastore().map_err(|error| {
            query::error::QueryError::execution(format!("failed to get datastore: {error}"))
        })?;
        let systemstore = txn.systemstore().map_err(|error| {
            query::error::QueryError::execution(format!("failed to get systemstore: {error}"))
        })?;
        let blockstore = txn.blockstore().map_err(|error| {
            query::error::QueryError::execution(format!("failed to get blockstore: {error}"))
        })?;
        let headstore = txn.headstore().map_err(|error| {
            query::error::QueryError::execution(format!("failed to get headstore: {error}"))
        })?;
        let result: query::error::Result<(DocID, Option<CommitArtifacts>)> = async {
            let Some((doc_short_id, canonical_doc_id)) = attempt
                .collection
                .resolve_doc_identity(&systemstore, &attempt.doc_id)
                .await
                .map_err(|error| query::error::QueryError::execution(error.to_string()))?
            else {
                return Ok((attempt.doc_id.clone(), None));
            };
            let index_manager = IndexManager::from_indexes(
                attempt.collection.resolved_root_id(),
                attempt.collection.schema(),
                attempt.collection.write_indexes(),
            )
            .map_err(|error| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager: {error}"
                ))
            })?;
            let existed = attempt
                .collection
                .delete_with_indexes(&datastore, &canonical_doc_id, doc_short_id, &index_manager)
                .await
                .map_err(|error| {
                    query::error::QueryError::execution(format!("delete error: {error}"))
                })?;
            if !existed {
                return Ok((canonical_doc_id, None));
            }
            crate::write::autocommit::helpers::unregister_deleted_doc(
                self.db.document_acp(),
                &systemstore,
                &attempt.collection,
                &canonical_doc_id,
            )
            .await?;
            let doc_id_string = canonical_doc_id.to_string();
            let block_result = write_delete_block_with_inputs(
                &blockstore,
                &headstore,
                &doc_id_string,
                doc_short_id,
                &attempt.collection.version_id(),
                attempt.signing_config.as_ref(),
                attempt.prepared_inputs.as_ref(),
            )
            .await
            .map_err(|error| match error {
                crate::block::builder::BuildError::NeedsInput(request) => {
                    query::error::QueryError::NeedsInput(request)
                }
                other => query::error::QueryError::execution(format!(
                    "failed to write delete block: {other}"
                )),
            })?;
            crate::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &block_result.cid.to_string(),
                &doc_id_string,
            )
            .await
            .map_err(|error| query::error::QueryError::execution(error.to_string()))?;
            let col_data = write_branchable_collection_block(
                &self.db,
                &attempt.collection_name,
                &attempt.collection,
                &blockstore,
                &headstore,
                block_result.cid,
                doc_short_id,
                attempt.signing_config.as_ref(),
                attempt.prepared_inputs.as_ref(),
            )
            .await?;
            Ok((
                canonical_doc_id,
                Some((block_result.cid, block_result.block, col_data)),
            ))
        }
        .await;
        drop(datastore);
        drop(systemstore);
        drop(blockstore);
        drop(headstore);
        let (canonical_doc_id, artifacts) = match result {
            Ok(result) => result,
            Err(error) => {
                if let Err(discard_error) = txn.discard() {
                    warn!(error = %discard_error, "Failed to discard retry transaction after delete error");
                }
                return Err(error);
            }
        };
        if let Some((cid, block, col_data)) = &artifacts {
            if let Some(bus) = self.db.event_bus().cloned() {
                let collection = attempt.collection.clone();
                let doc_id = canonical_doc_id.to_string();
                let cid = *cid;
                let block = block.clone();
                let col_data = col_data.clone();
                txn.on_retry_success_ordered(Box::new(move || {
                    bus.publish(Message::update(Update::new(
                        doc_id.clone(),
                        cid,
                        collection.collection_id().to_string(),
                        block,
                        false,
                        false,
                    )));
                    if let Some((col_cid, col_block)) = col_data {
                        bus.publish(Message::update(Update::new_with_subject_doc_id(
                            String::new(),
                            doc_id,
                            col_cid,
                            collection.collection_id().to_string(),
                            col_block,
                            false,
                            false,
                        )));
                    }
                }))
                .map_err(|error| crate::error::commit_query_error(error.into()))?;
            }

        }
        let (io, callbacks) = txn
            .into_retry_attempt()
            .map_err(|error| crate::error::commit_query_error(error.into()))?;
        retry
            .finish_attempt_io(io, callbacks)
            .map_err(|error| crate::error::commit_query_error(error.into()))?;
        Ok((canonical_doc_id, artifacts))
    }

    /// Delete multiple documents in a single transaction.
    ///
    /// Mirrors `create_many_impl`: one transaction for all deletes, one commit.
    /// This avoids creating N separate MVCC snapshots and N COW epochs when
    /// purging large batches (e.g. 686 documents caused a 20GB memory spike
    /// with per-document transactions).
    pub async fn delete_many_impl(
        &self,
        collection_name: &str,
        doc_ids: &[DocID],
    ) -> query::error::Result<Vec<DeleteResult>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }

        if doc_ids.len() == 1 {
            return self
                .delete_impl(collection_name, &doc_ids[0])
                .await
                .map(|r| vec![r]);
        }

        let (_collection_guard, collection) = self.guarded_collection(collection_name).await?;
        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id().to_string();
        let sign_config = get_signing_config();

        let index_manager =
            IndexManager::from_indexes(short_id, collection.schema(), collection.write_indexes())
                .map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        // Single transaction for all deletes
        let mut txn = self.new_mutation_txn().await?;
        self.db
            .check_node_access_in_txn(
                txn.acpstore().map_err(crate::error::commit_query_error)?,
                acp::nac::NodePermission::DocumentDelete,
            )
            .await
            .map_err(crate::error::commit_query_error)?;

        let mut results: Vec<(DocID, bool, Option<CommitArtifacts>)> =
            Vec::with_capacity(doc_ids.len());

        for doc_id in doc_ids {
            // Delete from datastore + indexes
            let (existed, doc_short_id, canonical_doc_id) = {
                let datastore = txn.datastore().map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to get datastore for collection '{}': {}",
                        collection_name, e
                    ))
                })?;
                let systemstore = txn.systemstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
                })?;

                let (doc_short_id, canonical_doc_id) =
                    match collection.resolve_doc_identity(&systemstore, doc_id).await {
                        Ok(Some(identity)) => identity,
                        Ok(None) => {
                            results.push((doc_id.clone(), false, None));
                            continue;
                        }
                        Err(e) => {
                            return Err(query::error::QueryError::execution(format!(
                                "failed to resolve document identity for '{doc_id}': {e}"
                            )));
                        }
                    };

                match collection
                    .delete_with_indexes(
                        &datastore,
                        &canonical_doc_id,
                        doc_short_id,
                        &index_manager,
                    )
                    .await
                {
                    Ok(existed) => (existed, doc_short_id, canonical_doc_id),
                    Err(e) => {
                        warn!(
                            collection = %collection_name,
                            doc_id = %doc_id,
                            error = %e,
                            "Failed to delete document in batch"
                        );
                        results.push((doc_id.clone(), false, None));
                        continue;
                    }
                }
            };

            // Skip block-write and event emit for missing docs; DeleteNode
            // treats existed==false as a no-op.
            if !existed {
                results.push((canonical_doc_id, existed, None));
                continue;
            }

            // Write delete block (composite with status=2)
            let commit_result: Option<CommitArtifacts> = {
                let blockstore = txn.blockstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
                })?;
                let headstore = txn.headstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get headstore: {}", e))
                })?;

                let block_result = write_delete_block_with_inputs(
                    &blockstore,
                    &headstore,
                    &canonical_doc_id.to_string(),
                    doc_short_id,
                    &schema_version_id,
                    sign_config.as_ref(),
                    self.prepared_inputs.as_ref(),
                )
                .await
                .map_err(|e| match e {
                    crate::block::builder::BuildError::NeedsInput(request) => {
                        query::error::QueryError::NeedsInput(request)
                    }
                    e => query::error::QueryError::execution(format!(
                        "failed to write delete block for collection {}: {}",
                        collection_name, e
                    )),
                })?;
                let composite_cid = block_result.cid;

                let systemstore = txn.systemstore().map_err(|e| {
                    query::error::QueryError::execution(format!("failed to get systemstore: {}", e))
                })?;
                crate::docid::map::set_block_doc_id_mapping(
                    &systemstore,
                    &composite_cid.to_string(),
                    &canonical_doc_id.to_string(),
                )
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

                let col_block_data = write_branchable_collection_block(
                    &self.db,
                    collection_name,
                    &collection,
                    &blockstore,
                    &headstore,
                    composite_cid,
                    doc_short_id,
                    sign_config.as_ref(),
                    self.prepared_inputs.as_ref(),
                )
                .await?;

                Some((composite_cid, block_result.block, col_block_data))
            };

            results.push((canonical_doc_id, existed, commit_result));
        }

        for (doc_id, _, commit_result) in &results {
            if let Some((cid, block, col_data)) = commit_result {
                self.register_ordered_update_event(
                    &mut txn,
                    collection_name,
                    &collection,
                    doc_id.to_string(),
                    *cid,
                    block.clone(),
                    col_data.clone(),
                )?;
            }
        }

        // Single commit for entire batch
        if let Err(e) = txn.commit().await {
            warn!(
                collection = %collection_name,
                error = %e,
                "Failed to commit batch delete transaction"
            );
            return Err(crate::error::commit_query_error(e));
        }

        // Emit events after commit, only when blocks were written.
        let mut delete_results = Vec::with_capacity(results.len());
        for (doc_id, existed, _commit_result) in results {
            delete_results.push(DeleteResult::new(doc_id, existed));
        }

        Ok(delete_results)
    }
}
