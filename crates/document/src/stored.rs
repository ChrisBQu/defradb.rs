//! Local document values with stable field IDs and CRDT ordering metadata.
//!
//! This format is separate from the canonical CBOR used for blocks and DocIDs.
//! Empty LWW bytes are a tombstone; their priority must survive materialization.
use std::collections::{BTreeMap, BTreeSet};

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
            ValueKind::Float32 => Some(NormalValue::Float32(f32::from_be_bytes(
                self.value.as_slice().try_into().ok()?,
            ))),
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
        let mut names = BTreeSet::new();
        for (id, field) in &self.fields {
            if field.name.is_empty() || !names.insert(&field.name) || field.materialize().is_none()
            {
                return None;
            }
            bytes.extend_from_slice(&id.to_be_bytes());
            bytes.push(field.kind as u8);
            bytes.extend_from_slice(&field.priority.to_be_bytes());
            for value in [field.name.as_bytes(), field.value.as_slice()] {
                bytes.extend_from_slice(&u32::try_from(value.len()).ok()?.to_be_bytes());
                bytes.extend_from_slice(value);
            }
        }
        Some(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let mut input = bytes.strip_prefix(MAGIC)?;
        let count = u32::from_be_bytes(take(&mut input, 4)?.try_into().ok()?) as usize;
        // Every field has at least 21 bytes of header. Never allocate from an
        // unchecked on-disk count, and never accept duplicate IDs or names.
        if count > input.len() / 21 {
            return None;
        }
        let mut fields = BTreeMap::new();
        let mut names = BTreeSet::new();
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
                || !names.insert(field.name.clone())
                || field.materialize().is_none()
                || fields.insert(id, field).is_some()
            {
                return None;
            }
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
