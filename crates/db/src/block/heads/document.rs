//! Document heads use the same parent/child markers as collection heads.
//!
//! A head key is `/d/{doc}/{field}/{cid}`, a marker `/ds/{doc}/{field}/{parent}/{child}`
//! and a publication guard `/r/{doc}/{field}/{cid}`. A marker may arrive before
//! its parent; it is kept until its parent is published and reclaimed.
use async_trait::async_trait;
use cid::Cid;
use storage::corekv::{Error, IterOptions, Iterator, Key, KvPair, Reader, Result};
use storage::keys::headstore::HeadstoreDocKey;

use super::walk::HeadWalk;
use super::{PruneOutcome, RECLAIM_MAX_KEYS};

const HEADS: &[u8] = b"/d/";
const MARKERS: &[u8] = b"/ds/";
const PUBLISHED: &[u8] = b"/r/";

/// The same document-head name under another family's prefix, or `None` for
/// a key outside the head family.
fn renamed(head_family_key: &[u8], family: &[u8]) -> Option<Vec<u8>> {
    let name = head_family_key.strip_prefix(HEADS)?;
    Some([family, name].concat())
}

/// The markers of every head of one document.
pub fn document_markers(doc_short_id: u64) -> Vec<u8> {
    let prefix = HeadstoreDocKey::document_prefix(doc_short_id);
    [MARKERS, &prefix[HEADS.len()..]].concat()
}

/// The publication guards of every head of one document.
pub fn document_publications(doc_short_id: u64) -> Vec<u8> {
    let prefix = HeadstoreDocKey::document_prefix(doc_short_id);
    [PUBLISHED, &prefix[HEADS.len()..]].concat()
}

/// Durable publication identity, separate from CRDT application: a standalone
/// counter can be applied before its composite publishes the field head.
/// This guard survives head reclamation so replay cannot resurrect a parent.
pub fn publication_key(head: &HeadstoreDocKey) -> Vec<u8> {
    [PUBLISHED, &head.bytes()[HEADS.len()..]].concat()
}

pub async fn publish(
    writer: &datastore::NamespaceView,
    head: &HeadstoreDocKey,
    priority: &[u8],
    parents: &[Cid],
) -> Result<()> {
    let recorded = publication_key(head);
    if writer.has(&recorded).await? {
        return Ok(());
    }
    for parent in parents {
        let parent_key = HeadstoreDocKey::new(head.doc_short_id, &head.field_id, *parent);
        supersede(writer, &parent_key.bytes(), head.cid).await?;
    }
    writer.set(&head.bytes(), priority).await?;
    writer.set(&recorded, &[]).await
}

/// Each writer names its own child, so sibling writers never overwrite a parent.
/// `parent_key` is the stored head key, byte for byte.
pub async fn supersede(
    writer: &datastore::NamespaceView,
    parent_key: &[u8],
    child: Cid,
) -> Result<()> {
    let mut marker = renamed(parent_key, MARKERS)
        .ok_or_else(|| Error::Other("a document head marker needs a /d/ head key".into()))?;
    marker.push(b'/');
    marker.extend_from_slice(child.to_string().as_bytes());
    writer.set(&marker, &[]).await
}

/// Reclaim the document's superseded heads in the caller's transaction.
///
/// Every write that publishes a document head calls this once, so a
/// document's backlog is what its latest writes superseded. Bounded by
/// [`RECLAIM_MAX_KEYS`]; [`super::reclaim`] says why it never conflicts.
pub async fn reclaim(writer: &datastore::NamespaceView, doc_short_id: u64) -> Result<PruneOutcome> {
    super::reclaim(
        writer,
        HeadstoreDocKey::document_prefix(doc_short_id),
        document_markers(doc_short_id),
        (HEADS.len(), MARKERS.len()),
        Some(PUBLISHED),
        RECLAIM_MAX_KEYS,
    )
    .await
}

/// Iterate live heads in one snapshot, excluding parents named by markers.
///
/// The heads and the markers of the prefix are walked side by side, so the
/// iterator holds one marker at a time however long the history. This scan is
/// advisory: choosing parents may fork the DAG, never a business decision.
pub async fn iterator<R>(reader: &R, options: IterOptions) -> Result<Box<dyn Iterator>>
where
    R: Reader + ?Sized,
{
    let marker_prefix = options
        .prefix()
        .and_then(|prefix| renamed(prefix, MARKERS))
        .ok_or_else(|| Error::Iterator("document head iterator requires a /d/ prefix".into()))?;
    let reverse = options.reverse();
    let markers = reader
        .iterator(
            IterOptions::new()
                .with_prefix(marker_prefix)
                .with_keys_only(true)
                .with_reverse(reverse),
        )
        .await?;
    let heads = reader.iterator(options).await?;
    Ok(Box::new(LiveHeads {
        walk: HeadWalk::new(heads, markers, HEADS.len(), MARKERS.len(), reverse),
        reverse,
        pending: None,
    }))
}

struct LiveHeads {
    walk: HeadWalk,
    reverse: bool,
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
        while let Some(head) = self.walk.next().await? {
            if !head.superseded {
                return Ok(Some(head.pair));
            }
        }
        Ok(None)
    }
    async fn close(&mut self) -> Result<()> {
        self.pending = None;
        self.walk.close().await
    }
    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.pending = None;
        // A head's markers sort right after its name: forward, seek the
        // markers to the name; in reverse, to just past every marker of it.
        let marker_key = match renamed(key, MARKERS) {
            Some(mut marker) if self.reverse => {
                marker.push(b'/' + 1);
                marker
            }
            Some(marker) => marker,
            None => MARKERS.to_vec(),
        };
        if !self.walk.seek(key, &marker_key).await? {
            return Ok(false);
        }
        self.pending = self.next().await?;
        Ok(self.pending.is_some())
    }
    async fn reset(&mut self) -> Result<()> {
        self.pending = None;
        self.walk.reset().await
    }
    fn is_valid(&self) -> bool {
        self.walk.is_valid()
    }
}
