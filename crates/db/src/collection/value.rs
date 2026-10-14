//! Materialized document puts and field operands. Block CBOR stays unchanged.
use super::*;
use document::stored::{StoredDocument, StoredField, ValueKind};
use rapidhash::RapidHashMap;
use storage::namespace::Namespace;

pub(crate) struct FieldOrder {
    pub(crate) priority: u64,
    pub(crate) raw_value: Option<Vec<u8>>,
}
type Priorities = RapidHashMap<String, FieldOrder>;

fn field_value(
    field: &schema::FieldDescription,
    value: &NormalValue,
    priority: u64,
) -> Result<StoredField> {
    if !field.crdt_type.is_counter() {
        return StoredField::lww(field.name.clone(), priority, value).map_err(Error::from);
    }
    let (kind, value) = match field.kind.as_scalar().map(ScalarKind::base_kind) {
        Some(ScalarKind::Int) => (
            ValueKind::Int64,
            value.as_int().map(|v| v.to_be_bytes().to_vec()),
        ),
        Some(ScalarKind::Float32) => (
            ValueKind::Float32,
            value
                .as_float32()
                .or_else(|| value.as_float64().map(|v| v as f32))
                .or_else(|| value.as_int().map(|v| v as f32))
                .map(|v| v.to_be_bytes().to_vec()),
        ),
        Some(ScalarKind::Float64) => (
            ValueKind::Float64,
            value
                .as_float64()
                .or_else(|| value.as_float32().map(f64::from))
                .or_else(|| value.as_int().map(|v| v as f64))
                .map(|v| v.to_be_bytes().to_vec()),
        ),
        _ => {
            return Err(Error::InvalidDocument(format!(
                "unsupported counter field {}",
                field.name
            )))
        }
    };
    let value = value.ok_or_else(|| {
        Error::InvalidDocument(format!("invalid counter value for {}", field.name))
    })?;
    Ok(StoredField {
        name: field.name.clone(),
        kind,
        priority,
        value,
    })
}

impl Collection {
    pub(crate) async fn put_document_value(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
        doc: &Document,
        creating: bool,
        priorities: Option<&Priorities>,
    ) -> Result<()> {
        let Some(ids) = self.part_ids(datastore).await? else {
            // Low-level collection fixtures may bypass definition installation.
            // Such values retain whole-value validation and never receive operands.
            return datastore
                .set(&self.doc_key(doc_short_id), &doc.to_cbor()?)
                .await
                .map_err(Error::Storage);
        };
        let heads = if creating {
            None
        } else {
            Some(
                crate::block::builder::DocHeadsSnapshot::load(
                    &datastore.sibling(Namespace::Headstore),
                    doc_short_id,
                )
                .await
                .map_err(Error::InvalidDocument)?,
            )
        };
        let mut stored = StoredDocument::default();
        for field in &self.def.fields {
            let value = doc.get(&field.name).unwrap_or(&NormalValue::Null);
            if field.crdt_type.is_counter() && value.is_nil() {
                continue;
            }
            let id = ids.get(&field.name).ok_or_else(|| {
                Error::InvalidDocument(format!("missing part ID for {}", field.name))
            })?;
            let priority = priorities
                .and_then(|values| values.get(&field.name).map(|order| order.priority))
                .unwrap_or_else(|| {
                    heads
                        .as_ref()
                        .map(|heads| heads.field_max_priority(&field.name))
                        .unwrap_or(1)
                });
            let mut stored_field = field_value(field, value, priority)?;
            if stored_field.kind == ValueKind::Lww {
                if let Some(raw) = priorities
                    .and_then(|values| values.get(&field.name))
                    .and_then(|order| order.raw_value.as_ref())
                {
                    stored_field.value.clone_from(raw);
                }
            }
            stored.fields.insert(*id, stored_field);
        }
        let bytes = stored
            .encode()
            .ok_or_else(|| Error::InvalidDocument("invalid local document value".into()))?;
        datastore
            .set(&self.doc_key(doc_short_id), &bytes)
            .await
            .map_err(Error::Storage)
    }

    /// Materialize CRDT-resolved values through operands when the schema is
    /// unchanged. Integer mirror differences are exact modulo 2^64. Floating
    /// point subtraction cannot reconstruct an exact delta, so those repairs
    /// retain a validated put instead of silently changing rounding.
    pub(crate) async fn merge_resolved_document_value(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
        doc: &Document,
        priorities: &Priorities,
    ) -> Result<bool> {
        if self.load_version(datastore, doc_short_id).await?.as_deref() != Some(self.version_id()) {
            return Ok(false);
        }
        let Some(ids) = self.part_ids(datastore).await? else {
            return Ok(false);
        };
        let counter_parts: Vec<u32> = self
            .def
            .fields
            .iter()
            .filter(|field| field.crdt_type.is_counter())
            .filter_map(|field| ids.get(&field.name).copied())
            .collect();
        let key = self.doc_key(doc_short_id);
        let Some(bytes) = datastore
            .get_parts(&key, &counter_parts)
            .await
            .map_err(Error::Storage)?
        else {
            return Ok(false);
        };
        let Some(stored) = StoredDocument::decode(&bytes) else {
            return Ok(false);
        };
        let mut parts = StoredDocument::default();
        for field in &self.def.fields {
            let order = priorities.get(&field.name);
            if !field.crdt_type.is_counter() && order.is_none() {
                continue;
            }
            let Some(id) = ids.get(&field.name) else {
                return Ok(false);
            };
            let old = stored.fields.get(id);
            let value = doc.get(&field.name).unwrap_or(&NormalValue::Null);
            if field.crdt_type.is_counter() && value.is_nil() {
                if old.is_some() {
                    return Ok(false);
                }
                continue;
            }
            let priority = order
                .map(|order| order.priority)
                .or_else(|| old.map(|field| field.priority))
                .unwrap_or(0);
            let mut resolved = field_value(field, value, priority)?;
            if let Some(old) = old {
                if old.name != resolved.name || old.kind != resolved.kind {
                    return Ok(false);
                }
            }
            match resolved.kind {
                ValueKind::Lww => {
                    if let Some(raw) = order.and_then(|order| order.raw_value.as_ref()) {
                        resolved.value.clone_from(raw);
                    }
                }
                ValueKind::Int64 => {
                    let target =
                        i64::from_be_bytes(resolved.value.as_slice().try_into().map_err(|_| {
                            Error::InvalidDocument("invalid integer mirror".into())
                        })?);
                    let previous = old
                        .map(|old| {
                            i64::from_be_bytes(
                                old.value.as_slice().try_into().expect("decoded integer"),
                            )
                        })
                        .unwrap_or(0);
                    let delta = target.wrapping_sub(previous);
                    if delta == 0 && old.is_some() {
                        continue;
                    }
                    resolved.value = delta.to_be_bytes().to_vec();
                }
                ValueKind::Float32 | ValueKind::Float64 => {
                    if let Some(old) = old {
                        if old.value == resolved.value {
                            continue;
                        }
                        return Ok(false);
                    }
                }
            }
            parts.fields.insert(*id, resolved);
        }
        if !parts.fields.is_empty() {
            let operand = storage::merge::document_parts(&parts).ok_or_else(|| {
                Error::InvalidDocument("invalid resolved document operand".into())
            })?;
            datastore
                .merge(&key, &operand)
                .await
                .map_err(Error::Storage)?;
        }
        Ok(true)
    }

    pub(crate) async fn merge_local_document_value(
        &self,
        datastore: &NamespaceView,
        doc_short_id: u64,
        doc: &Document,
        modified: &rapidhash::RapidHashSet<String>,
    ) -> Result<()> {
        let key = self.doc_key(doc_short_id);
        let ids = self.part_ids(datastore).await?;
        let bytes = datastore
            .get_parts(&key, &[])
            .await
            .map_err(Error::Storage)?;
        let stored = bytes.as_deref().and_then(StoredDocument::decode);
        let heads = crate::block::builder::DocHeadsSnapshot::load(
            &datastore.sibling(Namespace::Headstore),
            doc_short_id,
        )
        .await
        .map_err(Error::InvalidDocument)?;
        let priorities: Priorities = modified
            .iter()
            .map(|name| {
                Ok((
                    name.clone(),
                    FieldOrder {
                        priority: heads.field_max_priority(name).checked_add(1).ok_or_else(
                            || Error::InvalidDocument("field priority overflow".into()),
                        )?,
                        raw_value: None,
                    },
                ))
            })
            .collect::<Result<_>>()?;
        let (Some(ids), Some(stored)) = (ids, stored) else {
            return self
                .put_document_value(datastore, doc_short_id, doc, false, Some(&priorities))
                .await;
        };
        if self.load_version(datastore, doc_short_id).await?.as_deref() != Some(self.version_id()) {
            return self
                .put_document_value(datastore, doc_short_id, doc, false, Some(&priorities))
                .await;
        }
        let mut parts = StoredDocument::default();
        for field in &self.def.fields {
            if !modified.contains(&field.name) {
                continue;
            }
            let value = if field.crdt_type.is_counter() {
                let Some(delta) = doc.get_counter_delta(&field.name) else {
                    // Absolute counter replacement is structural, not an increment.
                    return self
                        .put_document_value(datastore, doc_short_id, doc, false, Some(&priorities))
                        .await;
                };
                delta
            } else {
                doc.get(&field.name).unwrap_or(&NormalValue::Null)
            };
            let id = ids.get(&field.name).ok_or_else(|| {
                Error::InvalidDocument(format!("missing part ID for {}", field.name))
            })?;
            parts.fields.insert(
                *id,
                field_value(field, value, priorities[&field.name].priority)?,
            );
        }
        // A standalone field-block merge may have advanced the accumulator
        // before its composite materialized the document. Repair that gap with
        // a validated put; applying only the new delta to a stale mirror loses
        // the previously received increment.
        let mut projected = stored;
        projected.apply(parts.clone()).ok_or_else(|| {
            Error::InvalidDocument(
                "document part identity changed without a schema transition".into(),
            )
        })?;
        for field in &self.def.fields {
            if !field.crdt_type.is_counter() || !modified.contains(&field.name) {
                continue;
            }
            let value = doc.get(&field.name).unwrap_or(&NormalValue::Null);
            if value.is_nil()
                || projected
                    .fields
                    .get(&ids[&field.name])
                    .map(|part| &part.value)
                    != Some(&field_value(field, value, priorities[&field.name].priority)?.value)
            {
                return self
                    .put_document_value(datastore, doc_short_id, doc, false, Some(&priorities))
                    .await;
            }
        }
        if !parts.fields.is_empty() {
            let operand = storage::merge::document_parts(&parts)
                .ok_or_else(|| Error::InvalidDocument("invalid document operand".into()))?;
            datastore
                .merge(&key, &operand)
                .await
                .map_err(Error::Storage)?;
        }
        Ok(())
    }
}
