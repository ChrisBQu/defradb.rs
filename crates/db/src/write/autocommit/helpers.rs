use super::*;
use bytes::Bytes;

use crate::block::builder::BlockResult;
use crate::collection::Collection;
use crdt::traits::{Context, ValueReader};
use crdt::{Counter, CounterDelta, NumericKind};
use datastore::NamespaceView;
use defra_core::types::DocId as CrdtDocId;
use document::NormalValue;
use schema::{FieldKind, ScalarKind};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_branchable_collection_block<S: storage::corekv::Store + 'static>(
    _db: &crate::database::DB<S>,
    collection_name: &str,
    collection: &Collection,
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc_cid: Cid,
    _doc_short_id: u64,
    signing_config: Option<&defra_core::signing::SigningConfig>,
    prepared_inputs: Option<&query::prepared::PreparedInputMemo>,
) -> query::error::Result<Option<(Cid, Bytes)>> {
    if !collection.schema().is_branchable {
        return Ok(None);
    }

    let written = write_collection_block_with_inputs(
        blockstore,
        headstore,
        collection.resolved_root_id(),
        collection.version_id(),
        doc_cid,
        signing_config,
        prepared_inputs,
    )
    .await
    .map(Some)
    .map_err(|error| match error {
        crate::block::builder::BuildError::NeedsInput(request) => {
            query::error::QueryError::NeedsInput(request)
        }
        error => query::error::QueryError::execution(format!(
            "failed to write collection block for branchable mutation on collection {}: {}",
            collection_name, error
        )),
    })?;

    Ok(written)
}

/// Run best-effort head reclamation only after the owning mutation has
/// returned from its engine commit. Opening a nested prune transaction from a
/// retry closure would repeat side effects on every attempt and can conflict
/// with the mutation snapshot.
pub(crate) async fn prune_heads_after_commit<S: storage::corekv::Store + 'static>(
    db: &crate::database::DB<S>,
    collection: &Collection,
    doc_short_ids: impl IntoIterator<Item = u64>,
) {
    for doc_short_id in doc_short_ids {
        db.maybe_prune_document_heads(doc_short_id).await;
    }
    if collection.schema().is_branchable {
        db.maybe_prune_collection_heads(collection.resolved_root_id())
            .await;
    }
}

/// Persist a local UPDATE: apply CRDT field deltas to the authoritative store
/// (the counter RMW, #1021) and then write the document + maintain indexes. These
/// are bundled so no mutator can write a document blob without first advancing the
/// CRDT accumulation store — the single-store invariant is enforced by
/// construction, not by each mutator remembering to call the counter helper.
/// When requested, embeddings use the final field values before persistence.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_local_update(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &mut Document,
    doc_short_id: u64,
    index_manager: &IndexManager,
    modified_fields: &mut rapidhash::RapidHashSet<String>,
    embedding_config: Option<&crate::search::EmbeddingClientConfig>,
    prepared_inputs: Option<&query::prepared::PreparedInputMemo>,
) -> query::error::Result<()> {
    collection
        .validate_write_definition(datastore, true)
        .await
        .map_err(crate::error::commit_query_error)?;

    apply_local_counter_deltas(datastore, collection, doc, doc_short_id).await?;
    if let Some(config) = embedding_config {
        let generated = if let Some(inputs) = prepared_inputs {
            crate::search::embedding::set_embedding_from_inputs(
                &collection.schema().vector_embeddings,
                doc,
                false,
                modified_fields,
                config,
                inputs,
            )?
        } else {
            crate::search::set_embedding(
                &collection.schema().vector_embeddings,
                doc,
                false,
                Some(modified_fields),
                config,
            )
            .await
            .map_err(|e| query::error::QueryError::execution(format!("embedding error: {e}")))?
        };
        modified_fields.extend(generated);
    }
    collection
        .update_with_indexes_parts(
            datastore,
            doc,
            doc_short_id,
            index_manager,
            Some(modified_fields),
        )
        .await
        .map_err(|e| match e {
            crate::error::Error::DocumentNotFound(id) => {
                query::error::QueryError::document_not_found(id)
            }
            other => crate::error::index_write_query_error("update", other),
        })
}

/// Persist a local CREATE: write the document + maintain indexes, then seed the
/// CRDT accumulation store for counter fields (#1021). Bundled for the same
/// by-construction reason as `write_local_update`.
pub(crate) async fn write_local_create(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
    doc_short_id: u64,
    index_manager: &IndexManager,
) -> query::error::Result<()> {
    collection
        .validate_write_definition(datastore, true)
        .await
        .map_err(crate::error::commit_query_error)?;

    collection
        .create_with_indexes(datastore, doc, doc_short_id, index_manager)
        .await
        .map_err(|e| crate::error::index_write_query_error("create", e))?;
    init_counter_stores_on_create(datastore, collection, doc).await
}

/// Register the identity of a freshly created document (Go `save()` isAdd):
/// duplicate-check the derived DocID against the mapping, persist the
/// short-ID <-> DocID mapping, and record block ownership for the genesis
/// composite, field, and encryption blocks. Returns the parsed DocID.
pub(crate) async fn register_created_doc(
    acp: Option<&Arc<dyn acp::DocumentACP>>,
    systemstore: &NamespaceView,
    datastore: &NamespaceView,
    collection: &Collection,
    doc_short_id: u64,
    block_result: &BlockResult,
) -> query::error::Result<DocID> {
    let doc_id_str = &block_result.doc_id;

    if let Some(existing_id) = crate::docid::map::get_doc_id(systemstore, doc_short_id)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?
    {
        return Err(query::error::QueryError::execution(format!(
            "Document with ID {existing_id} already exists"
        )));
    }

    let existing = crate::docid::map::get_doc_ref(systemstore, doc_id_str)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
    if let Some(doc_ref) = existing {
        let is_deleted = collection
            .is_deleted(datastore, doc_ref.doc_short_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        if is_deleted {
            return Err(query::error::QueryError::execution(format!(
                "a document with the given ID has been deleted. DocID: {doc_id_str}"
            )));
        }
        return Err(query::error::QueryError::execution(format!(
            "Document with ID {} already exists",
            doc_id_str
        )));
    }

    crate::docid::map::set_doc_id_mapping(
        systemstore,
        collection.resolved_root_id(),
        doc_short_id,
        doc_id_str,
    )
    .await
    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    if let (Some(provider), Some(policy), Some(creator)) = (
        acp,
        collection.schema().policy.as_ref(),
        defra_core::signing::get_broadcast_creator_did(),
    ) {
        if let Some(scoped) =
            provider.bind_transaction(systemstore.sibling(storage::namespace::Namespace::Acpstore))
        {
            let creator = identity::Did::new(&creator)
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            scoped
                .register_doc_object(&creator, &policy.id, &policy.resource_name, doc_id_str)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        }
    }

    register_block_doc_id_mappings(systemstore, block_result, doc_id_str).await?;
    crate::event::arrivals::record(systemstore, collection.resolved_root_id(), doc_id_str)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    DocID::from_string(doc_id_str)
        .map_err(|e| query::error::QueryError::execution(format!("invalid derived DocID: {}", e)))
}

/// Record block ownership (`/d/b/{cid}/{docID}`) for every block produced by
/// a mutation: the composite, each field block, and each encryption block.
pub(crate) async fn register_block_doc_id_mappings(
    systemstore: &NamespaceView,
    block_result: &BlockResult,
    doc_id: &str,
) -> query::error::Result<()> {
    let mut cids = Vec::with_capacity(1 + block_result.field_cids.len());
    cids.push(block_result.cid);
    cids.extend(block_result.field_cids.iter().copied());
    cids.extend(block_result.encryption_cids.iter().copied());
    for cid in cids {
        crate::docid::map::set_block_doc_id_mapping(systemstore, &cid.to_string(), doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
    }
    Ok(())
}

async fn apply_local_counter_deltas(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &mut Document,
    doc_short_id: u64,
) -> query::error::Result<()> {
    let schema_version_id = collection.version_id().to_string();

    // Collect the counter fields that carry a local increment, with their kind.
    let mut counter_fields: Vec<(String, NumericKind, bool, NormalValue)> = Vec::new();
    for field in &collection.schema().fields {
        if !field.crdt_type.is_counter() {
            continue;
        }
        let Some(delta) = doc.get_counter_delta(&field.name) else {
            continue;
        };
        let Some(kind) = numeric_kind_from_field_kind(&field.kind) else {
            continue;
        };
        counter_fields.push((
            field.name.clone(),
            kind,
            field.crdt_type.allows_decrement(),
            delta.clone(),
        ));
    }

    if counter_fields.is_empty() {
        return Ok(());
    }

    let doc_id = doc
        .id()
        .cloned()
        .ok_or_else(|| query::error::QueryError::execution("counter update requires a doc ID"))?;
    let doc_id_str = doc_id.to_string();
    let doc_id_bytes = doc_id_str.as_bytes().to_vec();

    // Freshly read committed doc (inside this write txn) to seed the store the
    // first time it is touched (init-if-absent). A first update of a doc with no
    // committed value seeds 0.
    let committed = collection
        .get_with_datastore(datastore, doc_short_id, &doc_id)
        .await
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

    // The CRDT operates on a mutable `ReaderWriter`; `NamespaceView` is one, but
    // it is shared by `&` here. Take an owned clone to obtain a mutable handle —
    // the clone writes through to the same underlying transaction.
    let mut rw = datastore.clone();

    for (field_name, kind, allow_decrement, delta) in counter_fields {
        let counter = Counter::new(
            schema_version_id.clone(),
            &doc_id_bytes,
            field_name.clone(),
            allow_decrement,
            kind,
        )
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        // Init-if-absent: seed the accumulation store from the committed blob the
        // first time only. Never overwrites a present (authoritative) store.
        let committed_value = committed.as_ref().and_then(|d| d.get(&field_name));
        match (kind, committed_value) {
            (NumericKind::Int64, Some(NormalValue::Int(v))) => {
                counter
                    .reconcile_int64(&mut rw, *v)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float64, Some(NormalValue::Float64(v))) => {
                counter
                    .reconcile_float64(&mut rw, *v)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, Some(NormalValue::Float32(v))) => {
                counter
                    .reconcile_float32(&mut rw, *v)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, Some(NormalValue::Float64(v))) => {
                counter
                    .reconcile_float32(&mut rw, *v as f32)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, Some(NormalValue::Int(v))) => {
                counter
                    .reconcile_float32(&mut rw, *v as f32)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float64, Some(NormalValue::Int(v))) => {
                counter
                    .reconcile_float64(&mut rw, *v as f64)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Int64, None) => {
                counter
                    .reconcile_int64(&mut rw, 0)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float64, None) => {
                counter
                    .reconcile_float64(&mut rw, 0.0)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            (NumericKind::Float32, None) => {
                counter
                    .reconcile_float32(&mut rw, 0.0)
                    .await
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
            }
            _ => {}
        }

        let counter_delta = build_local_counter_delta(
            &doc_id_bytes,
            &field_name,
            &schema_version_id,
            kind,
            &delta,
        )?;

        let ctx = Context {
            doc_id: CrdtDocId::new(&doc_id_str)
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            schema_version: schema_version_id.clone(),
            is_create: false,
        };

        crdt::traits::ReplicatedData::merge(&counter, &mut rw, &ctx, &counter_delta)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        // Mirror the resulting authoritative store value into the blob, overriding
        // the absolute value the query-plan layer pre-computed from a possibly
        // stale read.
        let bytes = ValueReader::value(&counter, &rw)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
        if let Some(value) = decode_counter_value(&bytes, kind) {
            doc.set(field_name.clone(), value);
        }
    }

    Ok(())
}

/// Initialize the CRDT accumulation store for counter fields on document
/// creation so the store is authoritative from creation (matching the
/// single-store invariant). The created value is absolute (no delta recorded on
/// create), so the store is seeded directly to it via init-if-absent.
async fn init_counter_stores_on_create(
    datastore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> query::error::Result<()> {
    let schema_version_id = collection.version_id().to_string();

    let mut counter_fields: Vec<(String, NumericKind, bool, NormalValue)> = Vec::new();
    for field in &collection.schema().fields {
        if !field.crdt_type.is_counter() {
            continue;
        }
        let Some(value) = doc.get(&field.name) else {
            continue;
        };
        let Some(kind) = numeric_kind_from_field_kind(&field.kind) else {
            continue;
        };
        counter_fields.push((
            field.name.clone(),
            kind,
            field.crdt_type.allows_decrement(),
            value.clone(),
        ));
    }

    if counter_fields.is_empty() {
        return Ok(());
    }

    let doc_id = doc
        .id()
        .cloned()
        .ok_or_else(|| query::error::QueryError::execution("counter create requires a doc ID"))?;
    let doc_id_bytes = doc_id.to_string().into_bytes();

    let mut rw = datastore.clone();
    for (field_name, kind, allow_decrement, value) in counter_fields {
        let counter = Counter::new(
            schema_version_id.clone(),
            &doc_id_bytes,
            field_name.clone(),
            allow_decrement,
            kind,
        )
        .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        match (kind, &value) {
            (NumericKind::Int64, NormalValue::Int(v)) => counter
                .reconcile_int64(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float64, NormalValue::Float64(v)) => counter
                .reconcile_float64(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Float32(v)) => counter
                .reconcile_float32(&mut rw, *v)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Float64(v)) => counter
                .reconcile_float32(&mut rw, *v as f32)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float32, NormalValue::Int(v)) => counter
                .reconcile_float32(&mut rw, *v as f32)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            (NumericKind::Float64, NormalValue::Int(v)) => counter
                .reconcile_float64(&mut rw, *v as f64)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?,
            _ => {}
        }
    }

    Ok(())
}

fn numeric_kind_from_field_kind(kind: &FieldKind) -> Option<NumericKind> {
    match kind.as_scalar().map(ScalarKind::base_kind) {
        Some(ScalarKind::Int) => Some(NumericKind::Int64),
        Some(ScalarKind::Float32) => Some(NumericKind::Float32),
        Some(ScalarKind::Float64) => Some(NumericKind::Float64),
        _ => None,
    }
}

fn build_local_counter_delta(
    doc_id_bytes: &[u8],
    field_name: &str,
    schema_version_id: &str,
    kind: NumericKind,
    delta: &NormalValue,
) -> query::error::Result<CounterDelta> {
    // priority/nonce are irrelevant to the accumulated value (counter merge is
    // unconditional commutative addition); use stable placeholders.
    match kind {
        NumericKind::Int64 => {
            let inc = match delta {
                NormalValue::Int(v) => *v,
                other => {
                    return Err(query::error::QueryError::execution(format!(
                        "counter Int64 field got non-int delta: {other:?}"
                    )));
                }
            };
            CounterDelta::new_int64(
                doc_id_bytes.to_vec(),
                field_name.to_string(),
                1,
                0,
                schema_version_id.to_string(),
                inc,
            )
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
        }
        NumericKind::Float64 => {
            let inc = match delta {
                NormalValue::Float64(v) => *v,
                NormalValue::Float32(v) => *v as f64,
                NormalValue::Int(v) => *v as f64,
                other => {
                    return Err(query::error::QueryError::execution(format!(
                        "counter Float64 field got non-numeric delta: {other:?}"
                    )));
                }
            };
            CounterDelta::new_float64(
                doc_id_bytes.to_vec(),
                field_name.to_string(),
                1,
                0,
                schema_version_id.to_string(),
                inc,
            )
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
        }
        NumericKind::Float32 => {
            let inc = match delta {
                NormalValue::Float32(v) => *v,
                NormalValue::Float64(v) => *v as f32,
                NormalValue::Int(v) => *v as f32,
                other => {
                    return Err(query::error::QueryError::execution(format!(
                        "counter Float32 field got non-numeric delta: {other:?}"
                    )));
                }
            };
            CounterDelta::new_float32(
                doc_id_bytes.to_vec(),
                field_name.to_string(),
                1,
                0,
                schema_version_id.to_string(),
                inc,
            )
            .map_err(|e| query::error::QueryError::execution(e.to_string()))
        }
        other => Err(query::error::QueryError::execution(format!(
            "unsupported counter NumericKind {other:?}"
        ))),
    }
}

fn decode_counter_value(bytes: &[u8], kind: NumericKind) -> Option<NormalValue> {
    match kind {
        NumericKind::Int64 if bytes.len() == 8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(NormalValue::Int(i64::from_be_bytes(arr)))
        }
        NumericKind::Float64 if bytes.len() == 8 => {
            let arr: [u8; 8] = bytes.try_into().ok()?;
            Some(NormalValue::Float64(f64::from_be_bytes(arr)))
        }
        NumericKind::Float32 if bytes.len() == 4 => {
            let arr: [u8; 4] = bytes.try_into().ok()?;
            Some(NormalValue::Float32(f32::from_be_bytes(arr)))
        }
        _ => None,
    }
}

pub(crate) fn ensure_collection_is_active<S: Store>(
    db: &DB<S>,
    collection_name: &str,
    collection: &Collection,
) -> query::error::Result<()> {
    let is_active = db
        .find_collection_by_id(collection.collection_id())
        .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
        .is_some();

    if is_active {
        Ok(())
    } else {
        Err(query::error::QueryError::collection_not_found(
            collection_name,
        ))
    }
}

impl<S: Store + 'static> AutoCommitMutator<S> {
    /// The collection's read guard, then its definition: resolved after the
    /// guard, so a patch or an index committed under the write guard is the
    /// definition this write uses.
    pub(super) async fn guarded_collection(
        &self,
        collection_name: &str,
    ) -> query::error::Result<((), Collection)> {
        let collection = self.get_collection_or_err(collection_name)?;
        ensure_collection_is_active(&self.db, collection_name, &collection)?;
        Ok(((), collection))
    }

    /// Get collection from DB cache or return a not-found error.
    pub(crate) fn get_collection_or_err(
        &self,
        collection_name: &str,
    ) -> query::error::Result<Collection> {
        self.db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))
    }
}

/// Local ownership disappears in the same commit as the deletion marker.
pub(crate) async fn unregister_deleted_doc(
    provider: Option<&Arc<dyn acp::DocumentACP>>,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc_id: &DocID,
) -> query::error::Result<()> {
    if let (Some(provider), Some(policy)) = (provider, collection.schema().policy.as_ref()) {
        if let Some(scoped) =
            provider.bind_transaction(systemstore.sibling(storage::namespace::Namespace::Acpstore))
        {
            scoped
                .unregister_doc_object(&policy.id, &policy.resource_name, &doc_id.to_string())
                .await
                .map_err(|error| query::error::QueryError::execution(error.to_string()))?;
        }
    }
    Ok(())
}
