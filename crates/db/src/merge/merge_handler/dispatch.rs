use async_trait::async_trait;
use cid::Cid;
use defra_core::block::{Block, CrdtDelta};
use defra_core::merge::{
    BlockMetadata, ExplicitReplayAuthorization, MergeBlock, MergeErrorDisposition, MergeHandler,
    MergeOutcome, RecoveredBlockMetadata,
};
use storage::corekv::Store;

use super::{DbMergeHandler, MergeError, cid_set};
use crate::merge::governance::{REDRIVE_BUDGET, RedrivenMerge};
use kovan_queue::seg_queue::SegQueue;

struct MergeRetryAttempt<S: Store, B: blockstore::Blockstore> {
    handler: DbMergeHandler<S, B>,
    cid: Cid,
    block_data: Vec<u8>,
    doc_id: Option<String>,
    collection_id: Option<String>,
    creator: Option<String>,
    sender_peer: Option<String>,
    is_explicit_replicator: bool,
    authorization: Option<ExplicitReplayAuthorization>,
    is_recovery: bool,
    is_schema_block: bool,
}

impl<S: Store + 'static, B: blockstore::Blockstore + 'static> storage::corekv::RetryAttemptFn
    for MergeRetryAttempt<S, B>
{
    fn run<'a>(
        &'a mut self,
        retry: &'a mut dyn storage::corekv::RetryTxn,
    ) -> storage::corekv::RetryAttemptFuture<'a> {
        Box::pin(async move {
            match self.run_attempt(retry).await {
                Ok(outcome) => Ok(Box::new(outcome) as storage::corekv::RetryAttemptValue),
                Err(error) => Err(storage::corekv::RetryAttemptError::Abort(Box::new(error))),
            }
        })
    }
}

impl<S: Store + 'static, B: blockstore::Blockstore + 'static> MergeRetryAttempt<S, B> {
    async fn run_attempt(
        &mut self,
        retry: &mut dyn storage::corekv::RetryTxn,
    ) -> Result<MergeOutcome, MergeError> {
        let io = retry
            .begin_attempt_io()
            .map_err(|error| MergeError::Database(crate::error::Error::Storage(error.into())))?;
        let basic_txn = datastore::BasicTxn::from_retry_attempt_io(io, 0);
        let mut txn = crate::txn::DbTxn::<S>::new(basic_txn);
        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let systemstore = txn.systemstore()?;
        let batch_merged = cid_set();
        let batch_merged_collections = cid_set();
        let pending_events = SegQueue::new();
        let pending_post_commit_actions = SegQueue::new();
        let pending_field_block_finalizations = SegQueue::new();
        let metadata = BlockMetadata {
            doc_id: self.doc_id.as_deref(),
            collection_id: self.collection_id.as_deref(),
            creator: self.creator.as_deref(),
            sender_peer: self.sender_peer.as_deref(),
            is_explicit_replicator: self.is_explicit_replicator,
            explicit_replay_authorization: self.authorization.clone(),
            verified_creator: None,
            is_recovery: self.is_recovery,
            is_schema_block: self.is_schema_block,
        };
        let result = self
            .handler
            .process_block_in_txn(
                &datastore,
                &headstore,
                &systemstore,
                &self.cid,
                &self.block_data,
                &metadata,
                &batch_merged,
                &batch_merged_collections,
                &pending_events,
                &pending_post_commit_actions,
                &pending_field_block_finalizations,
            )
            .await;
        drop(datastore);
        drop(headstore);
        drop(systemstore);
        let outcome = match result {
            Ok(MergeOutcome::Rejected { reason }) => {
                let _ = txn.discard();
                return Err(MergeError::RetryRejected(reason));
            }
            Ok(outcome) => outcome,
            Err(error) => {
                let _ = txn.discard();
                return Err(error);
            }
        };

        let handler = self.handler.clone();
        let merged_composites: Vec<_> = batch_merged.keys().collect();
        let merged_collections: Vec<_> = batch_merged_collections.keys().collect();
        let mut events = Vec::with_capacity(pending_events.len());
        while let Some(event) = pending_events.pop() {
            events.push(event.message);
        }
        let mut actions = Vec::new();
        while let Some(action) = pending_post_commit_actions.pop() {
            actions.push(action.action);
        }
        let mut field_cids = Vec::new();
        while let Some(finalization) = pending_field_block_finalizations.pop() {
            field_cids.extend(finalization.cids);
        }
        field_cids.sort_unstable();
        field_cids.dedup();
        txn.on_success_ordered_async(Box::new(move || {
            Box::pin(async move {
                handler
                    .merged_composites
                    .extend(merged_composites.iter().map(|cid| (*cid, ())));
                handler
                    .merged_collections
                    .extend(merged_collections.iter().map(|cid| (*cid, ())));
                for cid in &merged_composites {
                    handler.release_merged_composite(cid, None).await;
                }
                for action in actions {
                    if let Err(error) = action.run().await {
                        tracing::warn!(error = %error, "Post-commit merge action failed");
                    }
                }
                handler
                    .best_effort_finalize_linked_field_blocks(&field_cids)
                    .await;
                Ok(())
            })
        }))?;
        if !events.is_empty() {
            if let Some(bus) = self.handler.db.event_bus().cloned() {
                txn.on_success_ordered(Box::new(move || bus.publish_batch(events)))?;
            }
        }
        let (io, callbacks) = txn
            .into_retry_attempt()
            .map_err(crate::error::Error::Storage)?;
        retry
            .finish_attempt_io(io, callbacks)
            .map_err(|error| MergeError::Database(crate::error::Error::Storage(error.into())))?;
        Ok(outcome)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: blockstore::Blockstore + 'static> MergeHandler
    for DbMergeHandler<S, B>
{
    type Error = MergeError;

    fn error_disposition(&self, error: &Self::Error) -> MergeErrorDisposition {
        error.disposition()
    }

    async fn validate_authorization(
        &self,
        authorization: Option<&ExplicitReplayAuthorization>,
        block: &MergeBlock,
    ) -> Result<(), Self::Error> {
        self.validate_explicit_replay_authorization(authorization, block)
            .await
    }

    async fn recover_block_metadata(
        &self,
        cid: &Cid,
        block_data: &[u8],
    ) -> Result<Option<RecoveredBlockMetadata>, Self::Error> {
        self.recover_metadata_from_block(cid, block_data).await
    }

    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        let result = self.merge_with_retries(cid, block_data, metadata).await;
        self.redrive_deferred().await;
        result
    }

    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        let results = self.try_batch_merge_with_split(blocks).await;
        self.redrive_deferred().await;
        results
    }
}

impl<S: Store + 'static, B: blockstore::Blockstore + 'static> DbMergeHandler<S, B> {
    async fn merge_with_retries(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, MergeError> {
        const MAX_TXN_RETRIES: usize = 5;
        let recovered = if metadata.is_recovery {
            self.recover_metadata_from_block(cid, block_data).await?
        } else {
            None
        };
        let doc_id = metadata
            .doc_id
            .map(str::to_owned)
            .or_else(|| recovered.as_ref().map(|value| value.doc_id.clone()));
        let collection_id = metadata
            .collection_id
            .map(str::to_owned)
            .or_else(|| recovered.as_ref().map(|value| value.collection_id.clone()));
        let creator = metadata
            .creator
            .map(str::to_owned)
            .or_else(|| recovered.as_ref().map(|value| value.creator.clone()));
        // Keep the established lock order: collection guard, then per-document
        // merge queue, then a transaction snapshot. The guards span every
        // engine retry so a local write cannot interleave between attempts.
        let guard_collection_id = if let Some(schema_or_collection_id) = collection_id.as_deref() {
            self.block_collection(schema_or_collection_id, None)
                .await?
                .map(|collection| collection.collection_id().to_string())
        } else {
            None
        };
        let _collection_guard = if let Some(collection_id) = guard_collection_id.as_deref() {
            Some(
                self.db
                    .collection_read_guard(collection_id)
                    .await
                    .map_err(MergeError::Database)?,
            )
        } else {
            None
        };
        let _doc_guard = if let Some(doc_id) = doc_id.as_deref() {
            Some(self.merge_queue.acquire(doc_id).await)
        } else {
            None
        };
        let attempt = MergeRetryAttempt {
            handler: self.clone(),
            cid: *cid,
            block_data: block_data.to_vec(),
            doc_id: doc_id.clone(),
            collection_id: collection_id.clone(),
            creator,
            sender_peer: metadata.sender_peer.map(str::to_owned),
            is_explicit_replicator: metadata.is_explicit_replicator,
            authorization: metadata.explicit_replay_authorization.clone(),
            is_recovery: metadata.is_recovery,
            is_schema_block: metadata.is_schema_block,
        };
        let result = self
            .db
            .store()
            .transact_retry(
                storage::corekv::RetryOptions::new(MAX_TXN_RETRIES as u32),
                Box::new(attempt),
            )
            .await;
        // Head reclamation is a separate best-effort transaction. Run it
        // only after the retry engine has committed and released its ordered
        // commit token, so it cannot repeat inside a losing attempt.
        if result.is_ok() {
            if let (Some(collection_id), Some(doc_id)) =
                (collection_id.as_deref(), doc_id.as_deref())
            {
                if let Ok(Some(collection)) = self.block_collection(collection_id, None).await {
                    if let Ok(txn) = self.db.new_txn(true).await {
                        if let Ok(systemstore) = txn.systemstore() {
                            if let Ok(Some(doc_short_id)) = crate::docid::map::get_doc_short_id(
                                &systemstore,
                                collection.resolved_root_id(),
                                doc_id,
                            )
                            .await
                            {
                                self.db.maybe_prune_document_heads(doc_short_id).await;
                            }
                        }
                        let _ = txn.discard();
                    }
                    if collection.schema().is_branchable {
                        self.db
                            .maybe_prune_collection_heads(collection.resolved_root_id())
                            .await;
                    }
                }
            }
        }
        let result = match result {
            Ok(value) => value
                .downcast::<MergeOutcome>()
                .map(|outcome| *outcome)
                .map_err(|_| MergeError::MergeFailed("invalid merge retry result".into())),
            Err(error) => match error.downcast_abort::<MergeError>() {
                Ok(error) => Err(error),
                Err(error) => Err(MergeError::Database(crate::error::Error::Storage(
                    error.into_storage(),
                ))),
            },
        };
        match result {
            Err(MergeError::RetryRejected(reason)) => Ok(MergeOutcome::rejected(reason)),
            Err(error) if error.disposition() == MergeErrorDisposition::Terminal => {
                Ok(MergeOutcome::rejected(error.to_string()))
            }
            other => other,
        }
    }

    /// Re-merge deferred composites whose awaited CIDs have merged, through
    /// the normal path: signatures are verified and the validator judges each
    /// one again. Bounded by [`REDRIVE_BUDGET`] per call; composites released
    /// by a re-driven merge join the same drain.
    pub async fn redrive_deferred(&self) {
        let mut budget = REDRIVE_BUDGET;
        while budget > 0 {
            let Some(entry) = self.deferred.take_ready() else {
                return;
            };
            budget -= 1;
            let data = match self.blockstore.get(&entry.cid).await {
                Ok(Some(data)) => data,
                Ok(None) => continue,
                Err(error) => {
                    tracing::debug!(cid = %entry.cid, %error, "Deferred composite unreadable for re-drive");
                    continue;
                }
            };
            // A swept composite has no carrier, so it names no creator and no
            // sending peer; an id left empty reads as absent, not as "".
            let mut metadata = BlockMetadata::normal(
                &entry.doc_id,
                &entry.collection_id,
                &entry.creator,
                entry.sender_peer.as_deref(),
                entry.is_explicit_replicator,
            )
            .with_explicit_replay_authorization(entry.explicit_replay_authorization.clone());
            metadata.doc_id = metadata.doc_id.filter(|id| !id.is_empty());
            metadata.collection_id = metadata.collection_id.filter(|id| !id.is_empty());
            metadata.creator = metadata.creator.filter(|creator| !creator.is_empty());
            let outcome = self.merge_with_retries(&entry.cid, &data, metadata).await;
            tracing::debug!(cid = %entry.cid, ?outcome, "Re-drove deferred composite");
            if matches!(outcome, Ok(MergeOutcome::Merged)) {
                if let Some(sink) = self.redriven_merge_sink() {
                    sink.forward(RedrivenMerge::new(&entry, data)).await;
                }
            }
        }
        if self.deferred.has_ready() {
            tracing::debug!("Re-drive budget spent; remaining composites drain on the next merge");
        }
    }

    /// One merge attempt for a single block. Conflict retry lives in the
    /// `MergeHandler::handle_block` wrapper above (Go's `executeMerge` split).
    pub(crate) async fn merge_block_attempt(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, MergeError> {
        tracing::debug!(
            cid = %cid,
            block_size = block_data.len(),
            is_recovery = metadata.is_recovery,
            "Handling block for merge"
        );

        // Decode the block from DAG-CBOR
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        tracing::debug!(
            cid = %cid,
            delta_type = ?std::mem::discriminant(&block.delta),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            "Block decoded successfully"
        );

        // Verify block signature for P2P blocks (skip during recovery).
        // On success, populate verified_creator with the cryptographically
        // verified signer identity. Invalid signatures reject the block.
        let mut metadata = metadata;
        if !metadata.is_recovery {
            let verified = self.verify_block_signature(cid, &block, block_data).await?;
            metadata.verified_creator = verified;
        }

        // A standalone encrypted field block can only be merged once the
        // block-CID -> DocID ownership index names a single owner (recorded
        // by the composite merge that links it); otherwise it is merged via
        // its composite instead (see `process_lww_delta`/`process_counter_delta`).
        // Check that BEFORE attempting decryption: decrypting requires a KMS
        // fetch that may cross the network, and paying for that round trip
        // only to discard the result when ownership turns out to be unknown
        // wastes a fetch the composite merge will redundantly repeat moments
        // later (and, under load, can blow the caller's retry/poll budget).
        if block.encryption.is_some()
            && matches!(block.delta, CrdtDelta::Lww(_) | CrdtDelta::Counter(_))
            && self.resolve_field_block_doc_id(cid).await?.is_none()
        {
            // Still REQUEST the DEK (detached), Go-parity with sync-time
            // GetKeys: it warms the local key store for the composite merge
            // and keeps the serve-side authorization decision — including the
            // observable denial for unauthorized nodes — prompt.
            if let Some(enc_cid) = block.encryption {
                self.spawn_dek_prefetch(enc_cid, &metadata);
            }
            return Ok(MergeOutcome::terminal_skip(
                "field block has no unambiguous owner; merged via its composite",
            ));
        }

        // Decrypt delta data if the block has encryption.
        // If decryption fails (encryption key block unavailable), skip the
        // standalone field merge -- the composite merge will re-attempt
        // decryption when it processes the linked field blocks.
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Lww(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(error @ MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone LWW block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(error) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone LWW block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Counter(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(error @ MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone Counter block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(error) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone Counter block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                    }
                }
                _ => &block,
            }
        } else {
            &block
        };

        // Process based on delta type
        match &effective_block.delta {
            CrdtDelta::Lww(payload) => self.process_lww_delta(cid, payload, &metadata).await,
            CrdtDelta::Counter(payload) => {
                self.process_counter_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta(cid, &block, payload, &metadata, false, 0)
                    .await
            }
            CrdtDelta::Collection(payload) => {
                self.process_collection_delta(cid, &block, payload, &metadata, 0)
                    .await
            }
            CrdtDelta::FieldDefinition(_) => {
                // Field definitions are processed as part of CollectionDefinition
                tracing::debug!(cid = %cid, "FieldDefinition delta - skipping (processed with collection)");
                Ok(MergeOutcome::terminal_skip(
                    "field definition processed with collection",
                ))
            }
            CrdtDelta::CollectionDefinition(payload) => {
                self.process_collection_definition_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::CollectionSet(_) => {
                tracing::debug!(cid = %cid, "CollectionSet delta - skipping");
                Ok(MergeOutcome::terminal_skip("collection set delta"))
            }
            // Only the variant discriminant is reported — `CrdtDelta` carries
            // field-value bytes from user documents and must not be formatted
            // into error strings that may end up in logs.
            other => Err(MergeError::UnsupportedDelta(format!(
                "unhandled CrdtDelta variant in merge dispatch: {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }
}
