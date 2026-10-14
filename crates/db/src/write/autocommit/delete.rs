use super::helpers::write_branchable_collection_block;
use super::*;
use events::{Message, Update};

struct DeleteRetryAttempt<S: Store> {
    db: Arc<DB<S>>,
    collection_name: String,
    collection: Collection,
    doc_id: DocID,
    signing_config: Option<defra_core::signing::SigningConfig>,
    prepared_inputs: Option<query::prepared::PreparedInputMemo>,
}

struct DeleteManyRetryAttempt<S: Store> {
    db: Arc<DB<S>>,
    collection_name: String,
    collection: Collection,
    doc_ids: Vec<DocID>,
    signing_config: Option<defra_core::signing::SigningConfig>,
    prepared_inputs: Option<query::prepared::PreparedInputMemo>,
}

impl<S: Store + 'static> storage::corekv::RetryAttemptFn for DeleteManyRetryAttempt<S> {
    fn run<'a>(
        &'a mut self,
        retry: &'a mut dyn storage::corekv::RetryTxn,
    ) -> storage::corekv::RetryAttemptFuture<'a> {
        Box::pin(async move {
            let mutator = AutoCommitMutator {
                db: Arc::clone(&self.db),
                prepared_inputs: self.prepared_inputs.clone(),
            };
            match mutator.run_delete_many_attempt(retry, self).await {
                Ok(value) => Ok(Box::new(value) as storage::corekv::RetryAttemptValue),
                Err(error) => Err(storage::corekv::RetryAttemptError::Abort(Box::new(error))),
            }
        })
    }
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
            prepared_inputs: Some(self.prepared_inputs.clone().unwrap_or_default()),
        };
        let result = self
            .db
            .store()
            .transact_retry(storage::corekv::RetryOptions::new(4), Box::new(attempt))
            .await;
        let (canonical_doc_id, commit_result, doc_short_id) = match result {
            Ok(value) => *value
                .downcast::<(DocID, Option<CommitArtifacts>, Option<u64>)>()
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
        if let Some(doc_short_id) = doc_short_id {
            super::helpers::prune_heads_after_commit(
                &self.db,
                &collection,
                std::iter::once(doc_short_id),
            )
            .await;
        }
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
    ) -> query::error::Result<(DocID, Option<CommitArtifacts>, Option<u64>)> {
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
        let result: query::error::Result<(DocID, Option<CommitArtifacts>, Option<u64>)> = async {
            let Some((doc_short_id, canonical_doc_id)) = attempt
                .collection
                .resolve_doc_identity(&systemstore, &attempt.doc_id)
                .await
                .map_err(|error| query::error::QueryError::execution(error.to_string()))?
            else {
                return Ok((attempt.doc_id.clone(), None, None));
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
                return Ok((canonical_doc_id, None, None));
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
                attempt.collection.version_id(),
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
                Some(doc_short_id),
            ))
        }
        .await;
        drop(datastore);
        drop(systemstore);
        drop(blockstore);
        drop(headstore);
        let (canonical_doc_id, artifacts, doc_short_id) = match result {
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
                .map_err(crate::error::commit_query_error)?;
            }
        }
        let (io, callbacks) = txn
            .into_retry_attempt()
            .map_err(|error| crate::error::commit_query_error(error.into()))?;
        retry
            .finish_attempt_io(io, callbacks)
            .map_err(|error| crate::error::commit_query_error(error.into()))?;
        Ok((canonical_doc_id, artifacts, doc_short_id))
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
        let attempt = DeleteManyRetryAttempt {
            db: Arc::clone(&self.db),
            collection_name: collection_name.to_string(),
            collection: collection.clone(),
            doc_ids: doc_ids.to_vec(),
            signing_config: get_signing_config(),
            prepared_inputs: Some(self.prepared_inputs.clone().unwrap_or_default()),
        };
        let result = self
            .db
            .store()
            .transact_retry(storage::corekv::RetryOptions::new(4), Box::new(attempt))
            .await;
        let results = match result {
            Ok(value) => *value
                .downcast::<Vec<(DocID, bool, Option<CommitArtifacts>, Option<u64>)>>()
                .map_err(|_| {
                    query::error::QueryError::execution("invalid batch delete retry result")
                })?,
            Err(error) => match error.downcast_abort::<query::error::QueryError>() {
                Ok(error) => return Err(error),
                Err(error) => {
                    return Err(crate::error::commit_query_error(
                        crate::error::Error::Storage(error.into_storage()),
                    ));
                }
            },
        };
        let doc_short_ids: Vec<_> = results
            .iter()
            .filter_map(|(_, _, _, short_id)| *short_id)
            .collect();
        super::helpers::prune_heads_after_commit(&self.db, &collection, doc_short_ids).await;
        Ok(results
            .into_iter()
            .map(|(doc_id, existed, _, _)| DeleteResult::new(doc_id, existed))
            .collect())
    }

    async fn run_delete_many_attempt(
        &self,
        retry: &mut dyn storage::corekv::RetryTxn,
        attempt: &DeleteManyRetryAttempt<S>,
    ) -> query::error::Result<Vec<(DocID, bool, Option<CommitArtifacts>, Option<u64>)>> {
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
        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {e}"))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get systemstore: {e}"))
        })?;
        let blockstore = txn.blockstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get blockstore: {e}"))
        })?;
        let headstore = txn.headstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get headstore: {e}"))
        })?;
        let result: query::error::Result<Vec<(DocID, bool, Option<CommitArtifacts>, Option<u64>)>> =
            async {
                let index_manager = IndexManager::from_indexes(
                    attempt.collection.resolved_root_id(),
                    attempt.collection.schema(),
                    attempt.collection.write_indexes(),
                )
                .map_err(|e| {
                    query::error::QueryError::execution(format!(
                        "failed to create index manager: {e}"
                    ))
                })?;
                let mut results = Vec::with_capacity(attempt.doc_ids.len());
                for requested_id in &attempt.doc_ids {
                    let Some((doc_short_id, canonical_id)) = attempt
                        .collection
                        .resolve_doc_identity(&systemstore, requested_id)
                        .await
                        .map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "failed to resolve document identity for '{requested_id}': {e}"
                            ))
                        })?
                    else {
                        results.push((requested_id.clone(), false, None, None));
                        continue;
                    };
                    let existed = attempt
                        .collection
                        .delete_with_indexes(
                            &datastore,
                            &canonical_id,
                            doc_short_id,
                            &index_manager,
                        )
                        .await
                        .map_err(|e| {
                            query::error::QueryError::execution(format!("delete error: {e}"))
                        })?;
                    if !existed {
                        results.push((canonical_id, false, None, None));
                        continue;
                    }
                    crate::write::autocommit::helpers::unregister_deleted_doc(
                        self.db.document_acp(),
                        &systemstore,
                        &attempt.collection,
                        &canonical_id,
                    )
                    .await?;
                    let block_result = write_delete_block_with_inputs(
                        &blockstore,
                        &headstore,
                        &canonical_id.to_string(),
                        doc_short_id,
                        &attempt.collection.version_id(),
                        attempt.signing_config.as_ref(),
                        attempt.prepared_inputs.as_ref(),
                    )
                    .await
                    .map_err(|e| match e {
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
                        &canonical_id.to_string(),
                    )
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
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
                    results.push((
                        canonical_id,
                        true,
                        Some((block_result.cid, block_result.block, col_data)),
                        Some(doc_short_id),
                    ));
                }
                Ok(results)
            }
            .await;
        drop(datastore);
        drop(systemstore);
        drop(blockstore);
        drop(headstore);
        let results = match result {
            Ok(results) => results,
            Err(error) => {
                let _ = txn.discard();
                return Err(error);
            }
        };
        if let Some(bus) = self.db.event_bus().cloned() {
            for (doc_id, _, artifacts, _) in &results {
                let Some((cid, block, col_data)) = artifacts else {
                    continue;
                };
                let bus = Arc::clone(&bus);
                let collection = attempt.collection.clone();
                let doc_id = doc_id.to_string();
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
                .map_err(crate::error::commit_query_error)?;
            }
        }
        let (io, callbacks) = txn
            .into_retry_attempt()
            .map_err(|e| crate::error::commit_query_error(e.into()))?;
        retry
            .finish_attempt_io(io, callbacks)
            .map_err(|e| crate::error::commit_query_error(e.into()))?;
        Ok(results)
    }
}
