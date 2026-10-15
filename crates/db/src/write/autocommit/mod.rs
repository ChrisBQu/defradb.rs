//! Auto-committing document mutator for non-transactional mutations.
//!
//! This mutator wraps a database and automatically creates and commits
//! a write transaction for each mutation operation. This enables mutations
//! without explicit transaction management while still providing proper
//! transactional semantics per operation.

pub mod batch;
mod create;
mod delete;
pub(crate) mod helpers;
mod read;
pub mod update;

use async_trait::async_trait;
use bytes::Bytes;
use cid::Cid;
use document::{DocID, Document};
use query::mutator::{CreateResult, DeleteResult, DocMutator, MutationBatch, UpdateResult};
use std::sync::Arc;
use storage::corekv::{OnCommit, Store};
use tracing::warn;

use crate::block::builder::{
    write_collection_block_with_inputs, write_delete_block_with_inputs,
    write_document_blocks_with_inputs,
};
use crate::collection::Collection;
use crate::database::DB;
use crate::index::IndexManager;
use crate::read::lensed::fetcher::LensedDocFetcher;
use crate::txn::DbTxn;

/// Captured per-mutation commit data: the document block (cid + bytes) plus,
/// for branchable collections, the collection block (cid + bytes).
pub(super) type CommitArtifacts = (Cid, Bytes, Option<(Cid, Bytes)>);
use defra_core::RequestContext;

pub use batch::BatchMutator;

#[cfg(test)]
mod bearer_hook_tests;

/// Document mutator that auto-commits transactions for each operation.
///
/// This is useful for mutations that don't need explicit transaction control.
/// Each operation creates a new write transaction, performs the mutation,
/// and commits (or discards on error).
///
/// # Transaction Semantics
///
/// Each mutation is atomic: it either succeeds entirely or fails without
/// partial changes. However, multiple mutations are NOT atomic with respect
/// to each other - if you need multiple operations to be atomic, use
/// `DbDocMutator` with explicit transaction management instead.
pub struct AutoCommitMutator<S: Store> {
    pub(super) db: Arc<DB<S>>,
    pub(super) prepared_inputs: Option<query::prepared::PreparedInputMemo>,
}

impl<S: Store> AutoCommitMutator<S> {
    /// Create a new auto-committing mutator wrapping the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            prepared_inputs: None,
        }
    }

    pub(crate) fn with_prepared_inputs_value(
        &self,
        inputs: query::prepared::PreparedInputMemo,
    ) -> Self {
        Self {
            db: self.db.clone(),
            prepared_inputs: Some(inputs),
        }
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>)
    where
        S: 'static,
    {
        self.db.set_document_acp(acp);
    }

    async fn new_mutation_txn(&self) -> query::error::Result<DbTxn<S>> {
        self.db.new_txn(false).await.map_err(|error| {
            query::error::QueryError::execution(format!("failed to create txn: {error}"))
        })
    }

    pub(super) fn register_created_doc_with_acp(
        &self,
        txn: &mut crate::txn::DbTxn<S>,
        collection: &Collection,
        doc_id: String,
    ) -> query::error::Result<()> {
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(());
        };
        let Some(creator_did) = defra_core::signing::get_broadcast_creator_did() else {
            return Ok(());
        };
        let Some(acp) = self.db.document_acp().cloned() else {
            return Ok(());
        };
        if acp.supports_transactions() {
            return Ok(());
        }
        let creator = identity::Did::new(&creator_did).map_err(|error| {
            query::error::QueryError::execution(format!("invalid mutation creator DID: {error}"))
        })?;

        let policy_id = policy.id.clone();
        let resource_name = policy.resource_name.clone();
        let request_bearer_token = defra_core::signing::get_request_bearer_token(creator.as_str());
        let registration = txn.on_commit(OnCommit::OrderedAsync(Box::new(move || {
            let acp = Arc::clone(&acp);
            let policy_id = policy_id.clone();
            let resource_name = resource_name.clone();
            let doc_id = doc_id.clone();
            let creator = creator.clone();
            let request_bearer_token = request_bearer_token.clone();
            Box::pin(defra_core::signing::scope_request_bearer_token(
                Some(creator.to_string()),
                request_bearer_token,
                async move {
                    acp.redrive_registration(&policy_id, &resource_name, &doc_id)
                        .await
                        .map_err(|error| {
                            format!("failed to register document with ACP before publishing update: {error}")
                        })
                },
            ))
        })));
        registration.map_err(|error| query::error::QueryError::execution(error.to_string()))
    }

    pub(super) fn register_deleted_doc_with_acp(
        &self,
        txn: &mut crate::txn::DbTxn<S>,
        collection: &Collection,
        doc_id: String,
    ) -> query::error::Result<()> {
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(());
        };
        let Some(acp) = self.db.document_acp().cloned() else {
            return Ok(());
        };
        if acp.supports_transactions() {
            return Ok(());
        }

        let policy_id = policy.id.clone();
        let resource_name = policy.resource_name.clone();
        let creator = defra_core::signing::get_broadcast_creator_did();
        let request_bearer_token = creator
            .as_deref()
            .and_then(defra_core::signing::get_request_bearer_token);
        txn.on_commit(OnCommit::OrderedAsync(Box::new(move || {
            let acp = Arc::clone(&acp);
            let policy_id = policy_id.clone();
            let resource_name = resource_name.clone();
            let doc_id = doc_id.clone();
            let creator = creator.clone();
            let request_bearer_token = request_bearer_token.clone();
            Box::pin(defra_core::signing::scope_request_bearer_token(
                creator,
                request_bearer_token,
                async move {
                    acp.unregister_doc_object(&policy_id, &resource_name, &doc_id)
                        .await
                        .map_err(|error| {
                            format!("failed to unregister ACP document {doc_id}: {error}")
                        })
                },
            ))
        })))
        .map_err(|error| query::error::QueryError::execution(error.to_string()))
    }

    pub async fn new_batch_components(
        &self,
    ) -> query::error::Result<(Arc<BatchMutator<S>>, Arc<LensedDocFetcher<S>>)> {
        let txn = self.new_mutation_txn().await?;
        let acp_registration_projection = txn.acp_registration_projection();
        let fetcher = Arc::new(LensedDocFetcher::new(
            self.db.clone(),
            txn,
            self.db.lens_store.clone(),
        ));
        let batch = Arc::new(BatchMutator::new_with_prepared_inputs(
            self.db.clone(),
            fetcher.shared_txn(),
            self.prepared_inputs.clone(),
            acp_registration_projection,
        ));
        Ok((batch, fetcher))
    }

    pub(crate) fn new_retryable_batch_controller(&self) -> Arc<BatchMutator<S>> {
        Arc::new(BatchMutator::new_retry_controller(
            self.db.clone(),
            self.prepared_inputs.clone(),
        ))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocMutator for AutoCommitMutator<S> {
    fn engine_retries_batches(&self) -> bool {
        true
    }

    fn supports_projected_updates(&self) -> bool {
        true
    }

    fn with_prepared_inputs(
        &self,
        inputs: query::prepared::PreparedInputMemo,
    ) -> Option<Arc<dyn DocMutator>> {
        Some(Arc::new(Self {
            db: self.db.clone(),
            prepared_inputs: Some(inputs),
        }))
    }

    fn requires_write_preparation(&self, context: &RequestContext) -> bool {
        let embedding = self.db.options().embedding_config();
        self.db.kms().is_some()
            || !embedding.url.is_empty()
            || !embedding.model.is_empty()
            || context.signing().is_some()
    }

    async fn resolve_prepared_input(
        &self,
        context: &RequestContext,
        request: &query::prepared::PreparedInputRequest,
    ) -> query::error::Result<std::sync::Arc<[u8]>> {
        crate::write::prepare::resolve_input(&self.db, context, request).await
    }

    async fn prepared_embedding_request(
        &self,
        collection_name: &str,
        field_name: &str,
        doc: &document::Document,
    ) -> query::error::Result<Option<query::prepared::PreparedInputRequest>> {
        let collection = self.get_collection_or_err(collection_name)?;
        let Some(embedding) = collection
            .schema()
            .vector_embeddings
            .iter()
            .find(|embedding| embedding.field_name == field_name)
        else {
            return Ok(None);
        };
        crate::search::embedding::prepared_embedding_request(
            embedding,
            doc,
            &self.db.options().embedding_config(),
        )
        .map(Some)
        .map_err(query::error::QueryError::execution)
    }

    async fn prepare_write(
        &self,
        context: &RequestContext,
        collection_name: &str,
        doc: Document,
        modified_fields: Option<rapidhash::RapidHashSet<String>>,
    ) -> query::error::Result<Document> {
        self.prepare_request_write(context, collection_name, doc, modified_fields)
            .await
    }

    fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        AutoCommitMutator::set_document_acp(self, acp);
    }

    fn manages_external_acp_hooks(&self) -> bool {
        true
    }

    async fn begin_batch(&self) -> query::error::Result<Option<MutationBatch>> {
        let batch = self.new_retryable_batch_controller();
        let controller: Arc<dyn query::mutator::MutationBatchController> = batch;
        Ok(Some(MutationBatch::new_retryable(controller)))
    }

    async fn create(
        &self,
        context: &RequestContext,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<CreateResult> {
        self.create_with_resolved_inputs(context, collection_name, doc)
            .await
    }

    async fn create_many(
        &self,
        context: &RequestContext,
        collection_name: &str,
        docs: Vec<Document>,
    ) -> query::error::Result<Vec<CreateResult>> {
        self.create_many_with_resolved_inputs(context, collection_name, docs)
            .await
    }

    async fn update(
        &self,
        context: &RequestContext,
        collection_name: &str,
        doc: Document,
        modified_fields: rapidhash::RapidHashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_with_resolved_inputs(context, collection_name, None, doc, modified_fields)
            .await
    }

    async fn update_if_unchanged(
        &self,
        context: &RequestContext,
        collection_name: &str,
        expected: Document,
        doc: Document,
        modified_fields: rapidhash::RapidHashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_with_resolved_inputs(
            context,
            collection_name,
            Some(expected),
            doc,
            modified_fields,
        )
        .await
    }

    async fn update_if_fields_unchanged(
        &self,
        context: &RequestContext,
        collection_name: &str,
        expected: Document,
        expected_fields: rapidhash::RapidHashSet<String>,
        doc: Document,
        modified_fields: rapidhash::RapidHashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_with_projected_resolved_inputs(
            context,
            collection_name,
            Some(expected),
            Some(expected_fields),
            doc,
            modified_fields,
        )
        .await
    }

    async fn delete(
        &self,
        context: &RequestContext,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        self.delete_with_resolved_inputs(context, collection_name, doc_id)
            .await
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        self.exists_impl(collection_name, doc_id).await
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        self.get_for_update_impl(collection_name, doc_id).await
    }
}
