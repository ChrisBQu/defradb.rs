//! Local document values with stable field IDs and CRDT ordering metadata.
//!
//! This format is separate from the canonical CBOR used for blocks and DocIDs.
//! Empty LWW bytes are a tombstone; their priority must survive materialization.
use std::collections::BTreeMap;

use crate::encoding::{cbor_to_normal_value, normal_value_to_cbor};
use crate::{Document, Error, NormalValue, Result};

const MAGIC: &[u8; 4] = b"DFV\x01";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueKind {
    Lww = 0,
    Int64 = 1,
    Float32 = 2,
    Float64 = 3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredField {
    pub name: String,
    pub kind: ValueKind,
    pub priority: u64,
    /// CBOR for LWW values; big-endian numbers for counters.
    pub value: Vec<u8>,
}

impl StoredField {
    pub fn lww(name: String, priority: u64, value: &NormalValue) -> Result<Self> {
        let mut bytes = Vec::new();
        if !value.is_nil() {
            ciborium::into_writer(&normal_value_to_cbor(value)?, &mut bytes)
                .map_err(|error| Error::CborEncode(error.to_string()))?;
        }
        Ok(Self {
            name,
            kind: ValueKind::Lww,
            priority,
            value: bytes,
        })
    }

    pub fn materialize(&self) -> Option<NormalValue> {
        match self.kind {
            ValueKind::Lww if self.value.is_empty() => Some(NormalValue::Null),
            ValueKind::Lww => {
                let mut reader = self.value.as_slice();
                let value = ciborium::from_reader(&mut reader).ok()?;
                if !reader.is_empty() {
                    return None;
                }
                cbor_to_normal_value(value).ok()
            }
            ValueKind::Int64 => Some(NormalValue::Int(i64::from_be_bytes(
                self.value.as_slice().try_into().ok()?,
            ))),
            ValueKind::Float32 => Some(NormalValue::Float64(f64::from(f32::from_be_bytes(
                self.value.as_slice().try_into().ok()?,
            )))),
            ValueKind::Float64 => Some(NormalValue::Float64(f64::from_be_bytes(
                self.value.as_slice().try_into().ok()?,
            ))),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StoredDocument {
    pub fields: BTreeMap<u32, StoredField>,
}

impl StoredDocument {
    pub fn encode(&self) -> Option<Vec<u8>> {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&u32::try_from(self.fields.len()).ok()?.to_be_bytes());
        let mut names = Vec::with_capacity(self.fields.len());
        for (id, field) in &self.fields {
            if field.name.is_empty() || field.materialize().is_none() {
                return None;
            }
            names.push(field.name.as_str());
            bytes.extend_from_slice(&id.to_be_bytes());
            bytes.push(field.kind as u8);
            bytes.extend_from_slice(&field.priority.to_be_bytes());
            for value in [field.name.as_bytes(), field.value.as_slice()] {
                bytes.extend_from_slice(&u32::try_from(value.len()).ok()?.to_be_bytes());
                bytes.extend_from_slice(value);
            }
        }
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return None;
        }
        Some(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        Self::decode_inner(bytes, true)
    }

    /// Whether `bytes` are in this format rather than a whole CBOR value
    /// written before it existed. Checks the leading tag only.
    pub fn is_field_format(bytes: &[u8]) -> bool {
        bytes.starts_with(MAGIC)
    }

    fn decode_inner(bytes: &[u8], validate_values: bool) -> Option<Self> {
        let mut input = bytes.strip_prefix(MAGIC)?;
        let count = u32::from_be_bytes(take(&mut input, 4)?.try_into().ok()?) as usize;
        // Every field has at least 21 bytes of header. Never allocate from an
        // unchecked on-disk count, and never accept duplicate IDs or names.
        if count > input.len() / 21 {
            return None;
        }
        let mut fields = BTreeMap::new();
        for _ in 0..count {
            let id = u32::from_be_bytes(take(&mut input, 4)?.try_into().ok()?);
            let kind = match take(&mut input, 1)?[0] {
                0 => ValueKind::Lww,
                1 => ValueKind::Int64,
                2 => ValueKind::Float32,
                3 => ValueKind::Float64,
                _ => return None,
            };
            let priority = u64::from_be_bytes(take(&mut input, 8)?.try_into().ok()?);
            let name = std::str::from_utf8(take_sized(&mut input)?)
                .ok()?
                .to_owned();
            let value = take_sized(&mut input)?.to_vec();
            let field = StoredField {
                name,
                kind,
                priority,
                value,
            };
            if field.name.is_empty()
                || (validate_values && field.materialize().is_none())
                || fields.insert(id, field).is_some()
            {
                return None;
            }
        }
        let mut names: Vec<_> = fields.values().map(|field| field.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return None;
        }
        input.is_empty().then_some(Self { fields })
    }

    pub fn materialize(&self) -> Option<Document> {
        let mut doc = Document::new();
        for field in self.fields.values() {
            let value = field.materialize()?;
            if !value.is_nil() {
                doc.set(&field.name, value);
            }
        }
        doc.clean();
        Some(doc)
    }

    /// Apply one operand in commit order. Callers put a fresh value instead
    /// when the schema version changes; IDs and names cannot be repurposed.
    pub fn apply(&mut self, operand: Self) -> Option<()> {
        for (id, incoming) in operand.fields {
            let Some(current) = self.fields.get_mut(&id) else {
                if self
                    .fields
                    .values()
                    .any(|field| field.name == incoming.name)
                {
                    return None;
                }
                self.fields.insert(id, incoming);
                continue;
            };
            if current.kind != incoming.kind || current.name != incoming.name {
                return None;
            }
            current.value = match incoming.kind {
                ValueKind::Lww => {
                    if (incoming.priority, &incoming.value) <= (current.priority, &current.value) {
                        continue;
                    }
                    incoming.value
                }
                ValueKind::Int64 => i64::from_be_bytes(current.value.as_slice().try_into().ok()?)
                    .wrapping_add(i64::from_be_bytes(
                        incoming.value.as_slice().try_into().ok()?,
                    ))
                    .to_be_bytes()
                    .to_vec(),
                ValueKind::Float32 => {
                    (f32::from_be_bytes(current.value.as_slice().try_into().ok()?)
                        + f32::from_be_bytes(incoming.value.as_slice().try_into().ok()?))
                    .to_be_bytes()
                    .to_vec()
                }
                ValueKind::Float64 => {
                    (f64::from_be_bytes(current.value.as_slice().try_into().ok()?)
                        + f64::from_be_bytes(incoming.value.as_slice().try_into().ok()?))
                    .to_be_bytes()
                    .to_vec()
                }
            };
            current.priority = current.priority.max(incoming.priority);
        }
        Some(())
    }
}

fn take<'a>(input: &mut &'a [u8], length: usize) -> Option<&'a [u8]> {
    let (head, tail) = input.split_at_checked(length)?;
    *input = tail;
    Some(head)
}
fn take_sized<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
    let length = u32::from_be_bytes(take(input, 4)?.try_into().ok()?) as usize;
    take(input, length)
}

impl Document {
    /// Decode a local stored value without changing the wire CBOR decoder.
    pub fn from_storage_cbor(bytes: &[u8]) -> Result<Self> {
        if bytes.starts_with(MAGIC) {
            StoredDocument::decode_inner(bytes, false)
                .and_then(|value| value.materialize())
                .ok_or_else(|| Error::CborDecode("invalid local document value".into()))
        } else {
            Self::from_cbor(bytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_storage_cbor_materializes_a_stored_document() {
        let mut stored = StoredDocument::default();
        stored.fields.insert(
            1,
            StoredField::lww("name".to_owned(), 1, &NormalValue::String("Ada".to_owned())).unwrap(),
        );
        let bytes = stored.encode().unwrap();

        let document = Document::from_storage_cbor(&bytes).unwrap();
        assert_eq!(
            document.get("name").and_then(NormalValue::as_str),
            Some("Ada")
        );
    }

    #[test]
    fn from_storage_cbor_rejects_invalid_field_values() {
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&1_u32.to_be_bytes()); // field count
        bytes.extend_from_slice(&1_u32.to_be_bytes()); // field ID
        bytes.push(ValueKind::Lww as u8);
        bytes.extend_from_slice(&1_u64.to_be_bytes()); // priority
        bytes.extend_from_slice(&4_u32.to_be_bytes());
        bytes.extend_from_slice(b"name");
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.push(0xff); // invalid CBOR

        assert!(StoredDocument::decode(&bytes).is_none());
        assert!(Document::from_storage_cbor(&bytes).is_err());
    }

    #[test]
    fn stored_document_rejects_duplicate_field_names() {
        let mut stored = StoredDocument::default();
        for id in [1, 2] {
            stored.fields.insert(
                id,
                StoredField::lww("same".to_owned(), 1, &NormalValue::Null).unwrap(),
            );
        }
        assert!(stored.encode().is_none());

        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&2_u32.to_be_bytes()); // field count
        for id in [1_u32, 2] {
            bytes.extend_from_slice(&id.to_be_bytes());
            bytes.push(ValueKind::Lww as u8);
            bytes.extend_from_slice(&1_u64.to_be_bytes()); // priority
            bytes.extend_from_slice(&4_u32.to_be_bytes());
            bytes.extend_from_slice(b"same");
            bytes.extend_from_slice(&0_u32.to_be_bytes()); // null value
        }
        assert!(StoredDocument::decode(&bytes).is_none());
    }
}
