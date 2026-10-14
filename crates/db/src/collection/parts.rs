//! Persistent local IDs for Regolith's projected document reads.
//!
//! Schema field IDs are CIDs, not `u32`s. These IDs never cross the wire and
//! are never recycled, including when a schema version is removed and restored.
use std::collections::BTreeMap;

use super::{Collection, Error, NamespaceView, Result};
use schema::CollectionVersion;
use storage::corekv::Store;
use storage::namespace::Namespace;

pub(crate) type PartIds = BTreeMap<String, u32>;

fn key(version: &str) -> Vec<u8> {
    format!("/parts/{version}").into_bytes()
}

fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Storage(storage::Error::Other(format!(
        "invalid document part IDs: {error}"
    )))
}

pub(crate) async fn prepare<S: Store>(
    store: &S,
    systemstore: &NamespaceView,
    schema: &CollectionVersion,
) -> Result<()> {
    let key = key(&schema.version_id);
    let mut ids: PartIds = match systemstore.get(&key).await.map_err(Error::Storage)? {
        Some(bytes) => serde_json::from_slice(&bytes).map_err(invalid)?,
        None => BTreeMap::new(),
    };
    let mut missing: Vec<_> = schema
        .fields
        .iter()
        .filter(|field| !ids.contains_key(&field.name))
        .map(|field| field.name.clone())
        .collect();
    missing.sort();
    missing.dedup();
    if missing.is_empty() {
        return Ok(());
    }
    let reserved = store
        .allocate(b"s/seq/document-part", missing.len() as u64)
        .await
        .map_err(Error::Storage)?;
    if reserved.end - 1 > u64::from(u32::MAX) {
        return Err(invalid("part ID space exhausted"));
    }
    for (name, id) in missing.into_iter().zip(reserved) {
        ids.insert(name, id as u32);
    }
    systemstore
        .set(&key, &serde_json::to_vec(&ids).map_err(invalid)?)
        .await
        .map_err(Error::Storage)
}

impl Collection {
    pub(crate) async fn part_ids(&self, datastore: &NamespaceView) -> Result<Option<PartIds>> {
        let system = datastore.sibling(Namespace::Systemstore);
        system
            .get(&key(self.version_id()))
            .await
            .map_err(Error::Storage)?
            .map(|bytes| serde_json::from_slice(&bytes).map_err(invalid))
            .transpose()
    }
}
