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
use storage::corekv::Store;
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
use defra_core::encryption::get_encryption_config;
use defra_core::signing::get_signing_config;

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

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
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
        let registration = txn.on_success_ordered_async(Arc::new(move || {
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
                    if !acp
                        .is_doc_registered(&policy_id, &resource_name, &doc_id)
                        .await
                        .map_err(|error| format!("failed to check ACP registration before publishing update: {error}"))?
                    {
                        acp.register_doc_object(&creator, &policy_id, &resource_name, &doc_id)
                            .await
                            .map_err(|error| format!("failed to register document with ACP before publishing update: {error}"))?;
                    }
                    Ok(())
                },
            ))
        }));
        registration.map_err(|error| query::error::QueryError::execution(error.to_string()))
    }

    pub async fn new_batch_components(
        &self,
    ) -> query::error::Result<(Arc<BatchMutator<S>>, Arc<LensedDocFetcher<S>>)> {
        let txn = self.new_mutation_txn().await?;
        let fetcher = Arc::new(LensedDocFetcher::new(
            self.db.clone(),
            txn,
            self.db.lens_store.clone(),
        ));
        let batch = Arc::new(BatchMutator::new_with_prepared_inputs(
            self.db.clone(),
            fetcher.shared_txn(),
            self.prepared_inputs.clone(),
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

    fn with_prepared_inputs(
        &self,
        inputs: query::prepared::PreparedInputMemo,
    ) -> Option<Arc<dyn DocMutator>> {
        Some(Arc::new(Self {
            db: self.db.clone(),
            prepared_inputs: Some(inputs),
        }))
    }

    fn requires_write_preparation(&self) -> bool {
        let embedding = self.db.options().embedding_config();
        self.db.kms().is_some()
            || !embedding.url.is_empty()
            || !embedding.model.is_empty()
            || get_signing_config().is_some()
    }

    async fn resolve_prepared_input(
        &self,
        request: &query::prepared::PreparedInputRequest,
    ) -> query::error::Result<std::sync::Arc<[u8]>> {
        match request.kind() {
            query::prepared::PreparedInputKind::Embedding => {
                let response = crate::search::embedding::resolve_prepared_embedding_request(
                    request.exact_request(),
                )
                .await
                .map_err(|error| {
                    query::error::QueryError::execution(format!("embedding error: {error}"))
                })?;
                Ok(std::sync::Arc::from(response))
            }
            query::prepared::PreparedInputKind::Signature => {
                let input =
                    defra_core::signing::RemoteSignatureInput::decode(request.exact_request())
                        .map_err(query::error::QueryError::execution)?;
                let signer = get_signing_config().ok_or_else(|| {
                    query::error::QueryError::execution(
                        "remote signature input has no configured signer",
                    )
                })?;
                let response = input
                    .resolve(&signer)
                    .map_err(query::error::QueryError::execution)?;
                Ok(std::sync::Arc::from(response))
            }
            query::prepared::PreparedInputKind::KmsKey => {
                let operation: crate::write::prepare::PreparedKmsOperation =
                    serde_json::from_slice(request.exact_request()).map_err(|error| {
                        query::error::QueryError::execution(format!(
                            "invalid prepared KMS request: {error}"
                        ))
                    })?;
                let kms = self.db.kms().ok_or_else(|| {
                    query::error::QueryError::execution(
                        "prepared KMS input has no configured KMS provider",
                    )
                })?;
                let context =
                    match &operation {
                        crate::write::prepare::PreparedKmsOperation::GetForRead {
                            caller_identity: Some(did),
                            ..
                        } => kms::RequestContext::with_user(identity::Did::new(did).map_err(
                            |error| query::error::QueryError::execution(error.to_string()),
                        )?),
                        _ => kms::RequestContext::anonymous(),
                    };
                let (cid, key) = match operation {
                    crate::write::prepare::PreparedKmsOperation::Get { cid }
                    | crate::write::prepare::PreparedKmsOperation::GetForRead { cid, .. } => {
                        let cid = cid::Cid::try_from(cid.as_str()).map_err(|error| {
                            query::error::QueryError::execution(format!(
                                "invalid encryption CID in prepared KMS request: {error}"
                            ))
                        })?;
                        let key = kms
                            .get_keys(&context, &[cid])
                            .await
                            .map_err(|error| {
                                query::error::QueryError::execution(format!(
                                    "kms get_keys for {cid}: {error}"
                                ))
                            })?
                            .wait_all()
                            .await
                            .map_err(|error| {
                                query::error::QueryError::execution(format!(
                                    "kms get_keys for {cid}: {error}"
                                ))
                            })?
                            .remove(&cid)
                            .ok_or_else(|| {
                                query::error::QueryError::execution(format!(
                                    "Encryption block {cid} not found"
                                ))
                            })?;
                        (cid, key.to_vec())
                    }
                    crate::write::prepare::PreparedKmsOperation::Generate { doc_id, field } => {
                        let scope = kms::KeyScope::Document { doc_id, field };
                        let (cid, key) = kms
                            .generate_key(&kms::RequestContext::anonymous(), scope)
                            .await
                            .map_err(|error| {
                                query::error::QueryError::execution(format!(
                                    "kms generate_key: {error}"
                                ))
                            })?;
                        (cid, key.to_vec())
                    }
                };
                let response = serde_json::to_vec(&crate::write::prepare::PreparedKmsResponse {
                    cid: cid.to_string(),
                    key,
                })
                .map_err(|error| query::error::QueryError::execution(error.to_string()))?;
                Ok(std::sync::Arc::from(response))
            }
            query::prepared::PreparedInputKind::NodePermission => {
                let (identity, permission): (String, String) =
                    serde_json::from_slice(request.exact_request()).map_err(|_| {
                        query::error::QueryError::execution(
                            "invalid prepared node permission request",
                        )
                    })?;
                let identity = identity::Did::new(identity).map_err(|_| {
                    query::error::QueryError::execution(
                        "invalid identity in node permission request",
                    )
                })?;
                let permission = match permission.as_str() {
                    "update-document" => acp::nac::NodePermission::DocumentUpdate,
                    "delete-document" => acp::nac::NodePermission::DocumentDelete,
                    "truncate-collection" => acp::nac::NodePermission::CollectionTruncate,
                    _ => {
                        return Err(query::error::QueryError::execution(
                            "invalid prepared node permission",
                        ))
                    }
                };
                self.db
                    .check_node_access(Some(&identity), permission)
                    .await
                    .map_err(|error| {
                        query::error::QueryError::permission_denied(error.to_string())
                    })?;
                Ok(std::sync::Arc::from(b"true".as_slice()))
            }
            kind => Err(query::error::QueryError::execution(format!(
                "prepared {kind:?} input resolution is not available"
            ))),
        }
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
        collection_name: &str,
        doc: Document,
        modified_fields: Option<rapidhash::RapidHashSet<String>>,
        encryption: Option<defra_core::encryption::EncryptionConfig>,
    ) -> query::error::Result<Document> {
        self.prepare_request_write(collection_name, doc, modified_fields, encryption)
            .await
    }

    fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        AutoCommitMutator::set_document_acp(self, acp);
    }

    async fn begin_batch(&self) -> query::error::Result<Option<MutationBatch>> {
        let batch = self.new_retryable_batch_controller();
        let controller: Arc<dyn query::mutator::MutationBatchController> = batch;
        Ok(Some(MutationBatch::new_retryable(controller)))
    }

    async fn create(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<CreateResult> {
        self.create_with_resolved_inputs(collection_name, doc).await
    }

    async fn create_many(
        &self,
        collection_name: &str,
        docs: Vec<Document>,
    ) -> query::error::Result<Vec<CreateResult>> {
        self.create_many_with_resolved_inputs(collection_name, docs)
            .await
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: rapidhash::RapidHashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_with_resolved_inputs(collection_name, None, doc, modified_fields)
            .await
    }

    async fn update_if_unchanged(
        &self,
        collection_name: &str,
        expected: Document,
        doc: Document,
        modified_fields: rapidhash::RapidHashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_with_resolved_inputs(collection_name, Some(expected), doc, modified_fields)
            .await
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        self.delete_with_resolved_inputs(collection_name, doc_id)
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
