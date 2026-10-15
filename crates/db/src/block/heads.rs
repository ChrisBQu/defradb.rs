//! The collection head set: the tips of a branchable collection's DAG.
//!
//! A head is a stored block that nothing else names as a parent. Maintaining
//! that set by deleting a parent's head key when a child appears is the obvious
//! encoding and it is the one this module replaces: two writers appending
//! concurrently observe the same parent and both issue the same delete, which
//! is a write-write conflict the engine refuses at every isolation level. The
//! set stops forming siblings, which is a change to the data structure and not
//! to its speed.
//!
//! Here a writer only ever writes keys that name its own block: its head key,
//! and one `/cs/{collection}/{parent}/{child}` marker per parent. Two writers
//! therefore cannot write the same key, and the head set becomes a query: a
//! stored head key is live exactly when no marker names it.
//!
//! `proofs/tla/HeadSet.tla` checks both strategies, with
//! `MC_HeadSet_Red_EagerDelete.cfg` failing on the delete and
//! `MC_HeadSet_Green.cfg` passing on this one. `proofs/lean/HeadSet/Core.lean`
//! carries the algebra: `derived_writeSets_disjoint` for why nothing collides,
//! and `applyDerived_parents_not_head` for why the answer is unchanged.
//!
//! # Reclamation
//!
//! Markers are garbage once their parent's head key goes, which is inherent:
//! every conflict-free set carries tombstones. Every writer that appends to a
//! head set reclaims what it finds superseded at its snapshot, in its own
//! transaction ([`reclaim`]), so the backlog of a set is what the writes since
//! the last append left, never its history. The deletes are blind writes
//! inside a commutative prefix: two writers reclaiming the same group both
//! commit, because a delete of a key a newer delete already removed is no
//! change. Nothing writes a superseded head key or its markers again (a
//! document head is published once, `document::publication_key`; a collection
//! block is merged once, behind the merge handler's merged-block guard), so a
//! reclaiming writer conflicts with no one.
//!
//! The deletes leave tombstones in the two families. Regolith's
//! deletion-density compaction rewrites a range once most of what it would
//! read is dead and retires the tombstones, which is what bounds the bytes a
//! head scan walks. A compaction filter cannot do this reclamation: whether a
//! head key or a marker is garbage depends on another key (a marker naming
//! the head, a head key naming the marker's parent), and a filter sees one
//! entry at a time.

use cid::Cid;
use datastore::NamespaceView;
use storage::corekv::{Reader, Result};

pub mod document;
mod walk;
use storage::corekv::IterOptions;
use storage::keys::headstore::{HeadstoreColKey, HeadstoreColSuperseded};
use walk::HeadWalk;

/// Keys one reclamation may delete. A write supersedes about one head per
/// field it touches, so this is many writes' worth: the steady backlog of a
/// set clears in one pass, and a backlog left from before inline reclamation
/// clears over a few writes instead of in one oversized transaction.
pub(crate) const RECLAIM_MAX_KEYS: usize = 512;

/// What one pass over the head prefix found.
#[derive(Debug, Default)]
pub struct CollectionHeads {
    /// The live tips, ascending by CID text.
    pub live: Vec<Cid>,
    /// The highest priority among the live tips, or 0 when there are none.
    pub max_priority: u64,
    /// Head keys that a marker superseded, still stored.
    pub superseded: usize,
}

/// The collection's live heads.
///
/// Walks the head prefix and the marker prefix together. Both are sorted by
/// the CID text they carry, so one pass over each answers the whole question
/// and neither range is ever held in memory.
pub async fn live_collection_heads<R>(
    reader: &R,
    collection_short_id: u32,
) -> Result<CollectionHeads>
where
    R: Reader + ?Sized,
{
    let head_prefix = HeadstoreColKey::collection_prefix(collection_short_id);
    let marker_prefix = HeadstoreColSuperseded::collection_prefix(collection_short_id);
    let head_name_at = head_prefix.len();
    let parent_name_at = marker_prefix.len();
    let heads = reader
        .iterator(IterOptions::new().with_prefix(head_prefix))
        .await?;
    let markers = reader
        .iterator(
            IterOptions::new()
                .with_prefix(marker_prefix)
                .with_keys_only(true),
        )
        .await?;
    let mut walk = HeadWalk::new(heads, markers, head_name_at, parent_name_at, false);
    let mut found = CollectionHeads::default();
    while let Some(head) = walk.next().await? {
        if head.superseded {
            found.superseded += 1;
            continue;
        }
        let Some(cid) = std::str::from_utf8(&head.pair.key[head_name_at..])
            .ok()
            .and_then(|text| text.parse::<Cid>().ok())
        else {
            continue;
        };
        found.max_priority = found
            .max_priority
            .max(crate::block::builder::decode_priority_varint(
                &head.pair.value,
            ));
        found.live.push(cid);
    }
    walk.close().await?;
    Ok(found)
}

/// Record that `child` superseded each of `parents`.
///
/// Every key written names `child`, so this is safe to run concurrently with
/// any other writer: `HeadSet.derived_writeSets_disjoint`.
pub async fn record_supersedes(
    headstore: &NamespaceView,
    collection_short_id: u32,
    parents: &[Cid],
    child: Cid,
) -> Result<()> {
    for parent in parents {
        let marker = HeadstoreColSuperseded::new(collection_short_id, *parent, child);
        headstore
            .set(&storage::corekv::Key::bytes(&marker), &[])
            .await?;
    }
    Ok(())
}

/// What one reclamation removed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PruneOutcome {
    /// Head keys removed. Each came with its markers.
    pub heads_removed: usize,
    /// Marker keys removed with their head.
    pub markers_removed: usize,
    /// Markers removed whose head key had already gone.
    pub orphans_removed: usize,
    /// True when the pass stopped on its key budget rather than on the end of
    /// the range, so a caller can see that more is left rather than read the
    /// result as "clean".
    pub more_remaining: bool,
}

/// Reclaim the collection's superseded head keys, each with its markers, in
/// the caller's transaction. Every branchable append calls this.
pub async fn prune_superseded_heads(
    headstore: &NamespaceView,
    collection_short_id: u32,
    max_keys: usize,
) -> Result<PruneOutcome> {
    let head_prefix = HeadstoreColKey::collection_prefix(collection_short_id);
    let marker_prefix = HeadstoreColSuperseded::collection_prefix(collection_short_id);
    let names = (head_prefix.len(), marker_prefix.len());
    reclaim(headstore, head_prefix, marker_prefix, names, None, max_keys).await
}

/// Delete superseded head keys, each together with the markers against it,
/// with blind deletes in the caller's transaction.
///
/// A head key and its markers go in one transaction, so no reader can observe
/// a head whose markers were already removed. Deleting them separately would
/// resurrect the head, which is why `max_keys` bounds a *pass* and not a
/// single head's key group: a group wider than the budget still leaves whole,
/// on a pass of its own.
///
/// A marker whose parent has no head key is not necessarily garbage: a block
/// replicated ahead of its parent records the marker first, and the parent's
/// head key arrives afterwards. When the family records publication
/// (`publication`, the prefix of the publication keys, named like the heads),
/// a parent that was published and has no head key was reclaimed, and its
/// late markers are deleted too. Without that record they stay.
///
/// Nothing here is read with validation: the scans lie inside the families'
/// commutative prefixes and the publication check reads no parts, so the only
/// effect on the caller's commit is its own blind deletes.
pub(crate) async fn reclaim(
    headstore: &NamespaceView,
    head_prefix: Vec<u8>,
    marker_prefix: Vec<u8>,
    (head_name_at, parent_name_at): (usize, usize),
    publication: Option<&[u8]>,
    max_keys: usize,
) -> Result<PruneOutcome> {
    let heads = headstore
        .iterator(IterOptions::new().with_prefix(head_prefix))
        .await?;
    let markers = headstore
        .iterator(
            IterOptions::new()
                .with_prefix(marker_prefix)
                .with_keys_only(true),
        )
        .await?;
    let mut walk = HeadWalk::new(heads, markers, head_name_at, parent_name_at, false);
    if publication.is_some() {
        walk = walk.collect_orphans(max_keys);
    }

    // Collected rather than deleted in place: deleting from under an iterator
    // that is rebuilt from its last key would skip entries. The budget is what
    // keeps this bounded.
    let mut doomed: Vec<Vec<u8>> = Vec::new();
    let mut outcome = PruneOutcome::default();
    while let Some(head) = walk.next().await? {
        if !head.superseded {
            continue;
        }
        // Every marker against this head, so the group leaves together.
        let mut group = Vec::new();
        walk.take_markers(&head.pair.key, &mut group).await?;
        group.push(head.pair.key);
        // A group always leaves whole, so one larger than the whole budget
        // still goes on an otherwise empty pass. Deferring it instead would
        // defer it forever: every later pass would meet the same group first
        // and stop in the same place.
        if !doomed.is_empty() && doomed.len() + group.len() > max_keys {
            outcome.more_remaining = true;
            break;
        }
        outcome.heads_removed += 1;
        outcome.markers_removed += group.len() - 1;
        doomed.extend(group);
    }
    if !outcome.more_remaining {
        walk.finish().await?;
    }
    let orphans = walk.take_orphans();
    walk.close().await?;

    if let Some(publication) = publication {
        let mut published = Vec::with_capacity(publication.len() + 64);
        for marker in orphans {
            if doomed.len() >= max_keys {
                outcome.more_remaining = true;
                break;
            }
            let Some(parent) = marker
                .get(parent_name_at..)
                .and_then(|rest| Some(&rest[..rest.iter().rposition(|byte| *byte == b'/')?]))
            else {
                continue;
            };
            published.clear();
            published.extend_from_slice(publication);
            published.extend_from_slice(parent);
            if headstore.get_parts(&published, &[]).await?.is_some() {
                outcome.orphans_removed += 1;
                doomed.push(marker);
            }
        }
    }
    for key in doomed {
        headstore.delete(&key).await?;
    }
    Ok(outcome)
}
