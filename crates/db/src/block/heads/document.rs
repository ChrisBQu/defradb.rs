//! Document heads use the same parent/child markers as collection heads.
//! A marker may arrive before its parent; retain it until both can be reclaimed.
use async_trait::async_trait;
use cid::Cid;
use rapidhash::RapidHashSet;
use storage::corekv::{Error, IterOptions, Iterator, KvPair, Reader, Result};

/// Convert a document head prefix to its supersede-marker prefix.
pub fn marker_prefix(head_prefix: &[u8]) -> Vec<u8> {
    let mut result = b"/ds/".to_vec();
    result.extend_from_slice(
        head_prefix
            .strip_prefix(b"/d/")
            .expect("document head prefix"),
    );
    result
}

/// Durable publication identity, separate from CRDT application: a standalone
/// counter can be applied before its composite publishes the field head.
/// This guard survives head reclamation so replay cannot resurrect a parent.
pub fn publication_key(head_key: &[u8]) -> Vec<u8> {
    let mut key = b"/r/".to_vec();
    key.extend_from_slice(head_key.strip_prefix(b"/d/").expect("document head key"));
    key
}

pub async fn publish(
    writer: &datastore::NamespaceView,
    head: &storage::keys::headstore::HeadstoreDocKey,
    priority: &[u8],
    parents: &[Cid],
) -> Result<()> {
    use storage::corekv::Key;
    let key = head.bytes();
    let recorded = publication_key(&key);
    if writer.has(&recorded).await? {
        return Ok(());
    }
    for parent in parents {
        let parent_key = storage::keys::headstore::HeadstoreDocKey::new(
            head.doc_short_id,
            &head.field_id,
            *parent,
        );
        supersede(writer, &parent_key.bytes(), head.cid).await?;
    }
    writer.set(&key, priority).await?;
    writer.set(&recorded, &[]).await
}

/// Each writer names its own child, so sibling writers never overwrite a parent.
pub async fn supersede(
    writer: &datastore::NamespaceView,
    parent_key: &[u8],
    child: Cid,
) -> Result<()> {
    let mut marker = marker_prefix(parent_key);
    marker.push(b'/');
    marker.extend_from_slice(child.to_string().as_bytes());
    writer.set(&marker, &[]).await
}

/// Iterate live heads in one snapshot, excluding parents named by markers.
/// This scan is advisory: choosing parents may fork the DAG, never a business decision.
pub async fn iterator<R: Reader + ?Sized>(
    reader: &R,
    options: IterOptions,
) -> Result<Box<dyn Iterator>> {
    let prefix = options
        .prefix()
        .filter(|prefix| prefix.starts_with(b"/d/"))
        .ok_or_else(|| Error::Iterator("document head iterator requires a /d/ prefix".into()))?;
    let mut markers = reader
        .iterator(
            IterOptions::new()
                .with_prefix(marker_prefix(prefix))
                .with_keys_only(true),
        )
        .await?;
    let mut superseded = RapidHashSet::default();
    while let Some(pair) = markers.next().await? {
        let Some(last) = pair.key.iter().rposition(|byte| *byte == b'/') else {
            continue;
        };
        if last < 4 {
            continue;
        }
        let mut head = b"/d/".to_vec();
        head.extend_from_slice(&pair.key[4..last]);
        superseded.insert(head);
    }
    markers.close().await?;
    Ok(Box::new(LiveHeads {
        inner: reader.iterator(options).await?,
        superseded,
        pending: None,
    }))
}

struct LiveHeads {
    inner: Box<dyn Iterator>,
    superseded: RapidHashSet<Vec<u8>>,
    pending: Option<KvPair>,
}
impl storage::corekv::private::Sealed for LiveHeads {}
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Iterator for LiveHeads {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        if let Some(pair) = self.pending.take() {
            return Ok(Some(pair));
        }
        while let Some(pair) = self.inner.next().await? {
            if !self.superseded.contains(&pair.key) {
                return Ok(Some(pair));
            }
        }
        Ok(None)
    }
    async fn close(&mut self) -> Result<()> {
        self.pending = None;
        self.inner.close().await
    }
    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.pending = None;
        if !self.inner.seek(key).await? {
            return Ok(false);
        }
        self.pending = self.next().await?;
        Ok(self.pending.is_some())
    }
    async fn reset(&mut self) -> Result<()> {
        self.pending = None;
        self.inner.reset().await
    }
    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }
}
