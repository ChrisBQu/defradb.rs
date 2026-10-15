use std::sync::Arc;

use document::{Document, WritePreparation};
use rapidhash::RapidHashSet;
use storage::corekv::Store;

use crate::{block::builder::DocStorageIdentity, database::DB};
use defra_core::encryption::EncryptionConfig;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub(crate) enum PreparedKmsOperation {
    Get {
        cid: String,
    },
    /// Every key one read decrypts with, under its caller's identity.
    ReadKeys {
        cids: Vec<String>,
        caller_identity: Option<String>,
    },
    Generate {
        doc_id: String,
        field: Option<String>,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PreparedKmsResponse {
    pub cid: String,
    pub key: Vec<u8>,
}

/// One key of a [`PreparedKmsOperation::ReadKeys`] answer: the key, or why
/// the KMS released none.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct PreparedReadKey {
    pub cid: String,
    pub key: Result<Vec<u8>, String>,
}

pub(crate) fn kms_input_request(
    operation: PreparedKmsOperation,
) -> query::prepared::PreparedInputRequest {
    query::prepared::PreparedInputRequest::new(
        query::prepared::PreparedInputKind::KmsKey,
        serde_json::to_vec(&operation).expect("prepared KMS operation is serializable"),
    )
}

/// Create blocks computed outside an auto-commit attempt. The schema version
/// is checked at insertion time because a structural schema write can
/// invalidate the payload before the mutation retries.
#[derive(Debug, Clone)]
pub(crate) struct PreparedCreateBlocks {
    pub schema_version_id: String,
    pub computed: crate::block::builder::ComputedBlocks,
}

pub(crate) async fn prepare_keys<S: Store + 'static>(
    db: &DB<S>,
    doc: &mut Document,
    identity: DocStorageIdentity,
    modified_fields: Option<&RapidHashSet<String>>,
    config: Option<&EncryptionConfig>,
) -> query::error::Result<()> {
    prepare_keys_with_stores(db, doc, identity, modified_fields, config, None, None).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_keys_with_stores<S: Store + 'static>(
    db: &DB<S>,
    doc: &mut Document,
    identity: DocStorageIdentity,
    modified_fields: Option<&RapidHashSet<String>>,
    config: Option<&EncryptionConfig>,
    stores: Option<(&datastore::NamespaceView, &datastore::NamespaceView)>,
    prior: Option<&WritePreparation>,
) -> query::error::Result<()> {
    if let Some(prepared) = doc.write_preparation() {
        if prepared.collection_short_id != identity.collection_short_id
            || prepared.doc_short_id != identity.doc_short_id
        {
            return Err(query::error::QueryError::execution(
                "prepared write identity changed",
            ));
        }
        return Ok(());
    }
    let Some(kms) = db.kms() else { return Ok(()) };
    let mut prepared = WritePreparation::new(identity.collection_short_id, identity.doc_short_id);
    prepared.set_requires_kms_keys(true);
    let fields: Vec<_> = doc
        .values()
        .keys()
        .filter(|field| {
            field.as_str() != "_docID"
                && modified_fields.is_none_or(|fields| fields.contains(*field))
        })
        .cloned()
        .collect();
    let mut scopes: Vec<_> = fields
        .iter()
        .filter_map(|field| {
            config
                .filter(|config| config.should_encrypt_field(field))
                .map(|config| {
                    (
                        field.clone(),
                        config
                            .should_encrypt_individual_field(field)
                            .then(|| field.clone()),
                    )
                })
        })
        .collect();
    if modified_fields.is_some() {
        let all_fields: Vec<_> = doc.values().keys().cloned().collect();
        let policy = if let Some(prior) = prior {
            (prior.encrypt_doc, prior.encrypted_fields.clone())
        } else if let Some((blockstore, headstore)) = stores {
            crate::block::builder::inherited_encryption_policy(
                blockstore,
                headstore,
                identity,
                &all_fields,
            )
            .await
            .map_err(query::error::QueryError::execution)?
        } else {
            let txn = db
                .new_txn(true)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            let policy = {
                let blockstore = txn
                    .blockstore()
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                let headstore = txn
                    .headstore()
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                crate::block::builder::inherited_encryption_policy(
                    &blockstore,
                    &headstore,
                    identity,
                    &all_fields,
                )
                .await
                .map_err(query::error::QueryError::execution)?
            };
            txn.discard()
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            policy
        };
        prepared.encrypt_doc = policy.0;
        prepared.encrypted_fields = policy.1;
    }
    prepared.encrypt_doc |= config.is_some_and(|config| config.encrypt_doc);
    for field in fields {
        if prepared.encrypt_doc
            && !prepared.encrypted_fields.contains(&field)
            && !scopes.iter().any(|(explicit, _)| explicit == &field)
        {
            scopes.push((field, None));
        }
    }
    for (field, key_field) in scopes {
        let scope = kms::KeyScope::Document {
            doc_id: hex::encode(identity.doc_ref_bytes()),
            field: key_field.clone(),
        };
        let (cid, key) = kms
            .generate_key(&kms::RequestContext::anonymous(), scope)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("kms generate_key: {e}")))?;
        prepared.insert_scoped_key(field, cid, key, key_field);
    }
    doc.set_write_preparation(Arc::new(prepared));
    Ok(())
}

/// Fetch the one external input `request` names, outside any transaction, for
/// the per-request memo the next attempt reads it from. The single resolver of
/// every input kind a write prepares: embeddings, signatures, KMS keys and
/// node permissions.
pub(crate) async fn resolve_input<S>(
    db: &DB<S>,
    request: &query::prepared::PreparedInputRequest,
) -> query::error::Result<Arc<[u8]>>
where
    S: Store + 'static,
{
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
            let input = defra_core::signing::RemoteSignatureInput::decode(request.exact_request())
                .map_err(query::error::QueryError::execution)?;
            let signer = defra_core::signing::get_signing_config().ok_or_else(|| {
                query::error::QueryError::execution("remote signature input has no configured signer")
            })?;
            let response = input
                .resolve(&signer)
                .map_err(query::error::QueryError::execution)?;
            Ok(std::sync::Arc::from(response))
        }
        query::prepared::PreparedInputKind::KmsKey => {
            let operation: PreparedKmsOperation = serde_json::from_slice(request.exact_request())
                .map_err(|error| {
                query::error::QueryError::execution(format!(
                    "invalid prepared KMS request: {error}"
                ))
            })?;
            let kms = db.kms().ok_or_else(|| {
                query::error::QueryError::execution(
                    "prepared KMS input has no configured KMS provider",
                )
            })?;
            let (cid, key) = match operation {
                PreparedKmsOperation::ReadKeys {
                    cids,
                    caller_identity,
                } => {
                    let context = match caller_identity {
                        Some(did) => {
                            kms::RequestContext::with_user(identity::Did::new(&did).map_err(
                                |error| query::error::QueryError::execution(error.to_string()),
                            )?)
                        }
                        None => kms::RequestContext::anonymous(),
                    };
                    let cids = cids
                        .iter()
                        .map(|cid| {
                            cid::Cid::try_from(cid.as_str()).map_err(|error| {
                                query::error::QueryError::execution(format!(
                                    "invalid encryption CID in prepared KMS request: {error}"
                                ))
                            })
                        })
                        .collect::<query::error::Result<Vec<_>>>()?;
                    let answers = kms.get_keys_each(&context, &cids).await.map_err(|error| {
                        query::error::QueryError::execution(format!("kms get_keys: {error}"))
                    })?;
                    let keys: Vec<PreparedReadKey> = cids
                        .iter()
                        .zip(answers)
                        .map(|(cid, answer)| PreparedReadKey {
                            cid: cid.to_string(),
                            key: answer.map(|key| key.to_vec()).map_err(|e| e.to_string()),
                        })
                        .collect();
                    let response = serde_json::to_vec(&keys)
                        .map_err(|error| query::error::QueryError::execution(error.to_string()))?;
                    return Ok(std::sync::Arc::from(response));
                }
                PreparedKmsOperation::Get { cid } => {
                    let cid = cid::Cid::try_from(cid.as_str()).map_err(|error| {
                        query::error::QueryError::execution(format!(
                            "invalid encryption CID in prepared KMS request: {error}"
                        ))
                    })?;
                    let key = kms
                        .get_keys(&kms::RequestContext::anonymous(), &[cid])
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
                PreparedKmsOperation::Generate { doc_id, field } => {
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
            let response = serde_json::to_vec(&PreparedKmsResponse {
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
            // An attempt with no acting identity asks for the wildcard actor.
            let identity = if identity == identity::Did::wildcard().as_str() {
                identity::Did::wildcard()
            } else {
                identity::Did::new(identity).map_err(|_| {
                    query::error::QueryError::execution(
                        "invalid identity in node permission request",
                    )
                })?
            };
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
            db.check_node_access(Some(&identity), permission)
                .await
                .map_err(|error| query::error::QueryError::permission_denied(error.to_string()))?;
            Ok(std::sync::Arc::from(b"true".as_slice()))
        }
        kind => Err(query::error::QueryError::execution(format!(
            "prepared {kind:?} input resolution is not available"
        ))),
    }
}

pub(crate) fn prepared_identity(doc: &Document) -> Option<DocStorageIdentity> {
    doc.write_preparation().map(|prepared| {
        DocStorageIdentity::new(prepared.collection_short_id, prepared.doc_short_id)
    })
}

impl<S: Store + 'static> crate::AutoCommitMutator<S> {
    pub(super) async fn prepare_request_write(
        &self,
        collection_name: &str,
        mut doc: Document,
        mut modified_fields: Option<RapidHashSet<String>>,
        config: Option<EncryptionConfig>,
    ) -> query::error::Result<Document> {
        let collection = self.get_collection_or_err(collection_name)?;
        let effective_encryption_config =
            config.or_else(defra_core::encryption::get_encryption_config);
        let generated_embeddings = if let Some(fields) = modified_fields.as_mut() {
            let generated = crate::search::set_embedding(
                &collection.schema().vector_embeddings,
                &mut doc,
                false,
                Some(fields),
                &self.db.options().embedding_config(),
            )
            .await
            .map_err(|error| {
                query::error::QueryError::execution(format!("embedding error: {error}"))
            })?;
            fields.extend(generated.iter().cloned());
            for embedding in &collection.schema().vector_embeddings {
                if !fields.contains(&embedding.field_name)
                    && embedding
                        .fields
                        .iter()
                        .any(|source| fields.contains(source))
                {
                    fields.insert(embedding.field_name.clone());
                    if doc.get(&embedding.field_name).is_none() {
                        doc.set(&embedding.field_name, document::NormalValue::Null);
                    }
                }
            }
            generated
        } else {
            crate::search::set_embedding(
                &collection.schema().vector_embeddings,
                &mut doc,
                true,
                None,
                &self.db.options().embedding_config(),
            )
            .await
            .map_err(|error| {
                query::error::QueryError::execution(format!("embedding error: {error}"))
            })?
        };
        let signing_config = defra_core::signing::get_signing_config();
        let prepare_create = modified_fields.is_none()
            && (!generated_embeddings.is_empty() || signing_config.is_some());
        if self.db.kms().is_none() && !prepare_create {
            if modified_fields.is_none() && crate::write::prepare::prepared_identity(&doc).is_none()
            {
                let identity = DocStorageIdentity::new(
                    collection.resolved_root_id(),
                    self.db
                        .next_doc_short_id()
                        .await
                        .map_err(|error| query::error::QueryError::execution(error.to_string()))?,
                );
                let mut preparation = document::WritePreparation::new(
                    identity.collection_short_id,
                    identity.doc_short_id,
                );
                preparation.set_requires_kms_keys(false);
                doc.set_write_preparation(Arc::new(preparation));
            }
            return Ok(doc);
        }
        let prior = doc.write_preparation().cloned();
        let identity = if let Some(prior) = &prior {
            doc.clear_write_preparation();
            DocStorageIdentity::new(prior.collection_short_id, prior.doc_short_id)
        } else if modified_fields.is_some() {
            let txn = self
                .db
                .new_txn(true)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            let identity = {
                let systemstore = txn
                    .systemstore()
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                let doc_id = doc.id().ok_or_else(|| {
                    query::error::QueryError::execution("update requires a document ID")
                })?;
                let (short_id, canonical) = collection
                    .require_doc_identity(&systemstore, doc_id)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                doc.set_id(canonical);
                DocStorageIdentity::new(collection.resolved_root_id(), short_id)
            };
            txn.discard()
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            identity
        } else {
            DocStorageIdentity::new(
                collection.resolved_root_id(),
                self.db
                    .next_doc_short_id()
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            )
        };
        prepare_keys_with_stores(
            &self.db,
            &mut doc,
            identity,
            modified_fields.as_ref(),
            effective_encryption_config.as_ref(),
            None,
            prior.as_deref(),
        )
        .await?;

        let preparation = match doc.write_preparation() {
            Some(preparation) => Arc::clone(preparation),
            None => {
                let preparation = Arc::new(WritePreparation::new(
                    identity.collection_short_id,
                    identity.doc_short_id,
                ));
                doc.set_write_preparation(Arc::clone(&preparation));
                preparation
            }
        };
        if modified_fields.is_none() {
            if let Some(signing_config) = signing_config.as_ref() {
                let blocks = crate::block::builder::compute_document_blocks(
                    &doc,
                    collection.version_id(),
                    identity,
                    effective_encryption_config.as_ref(),
                    Some(signing_config),
                )
                .map_err(|error| {
                    query::error::QueryError::execution(format!(
                        "failed to prepare signed create blocks: {error}"
                    ))
                })?;
                let mut write = (*preparation).clone();
                write.set_payload(Arc::new(PreparedCreateBlocks {
                    schema_version_id: collection.version_id().to_string(),
                    computed: blocks,
                }));
                doc.set_write_preparation(Arc::new(write));
            }
        }
        Ok(doc)
    }
}
