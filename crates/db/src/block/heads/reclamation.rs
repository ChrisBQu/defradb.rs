//! One reclamation's deletes: whole superseded groups under a key budget, then
//! the orphan markers it may drop, deleted together in the caller's
//! transaction. Both a fresh scan ([`super::reclaim`]) and a write that already
//! walked its heads (`document::Reclaimable`) go through here, so they make the
//! same deletes by the same rules.

use datastore::NamespaceView;
use storage::corekv::{KvPair, Result};

use super::walk::HeadWalk;
use super::PruneOutcome;

/// The keys one reclamation deletes, gathered under its budget.
#[derive(Debug, Default)]
pub(crate) struct Reclamation {
    doomed: Vec<Vec<u8>>,
    outcome: PruneOutcome,
}

impl Reclamation {
    /// Walk to the end, or until the budget is spent when `stop_when_full`,
    /// handing each live head to `live` and gathering each superseded head
    /// with every marker that names it.
    pub(crate) async fn gather(
        walk: &mut HeadWalk,
        max_keys: usize,
        stop_when_full: bool,
        mut live: impl FnMut(KvPair),
    ) -> Result<Self> {
        let mut this = Self::default();
        while let Some(head) = walk.next().await? {
            if !head.superseded {
                live(head.pair);
                continue;
            }
            // Taken even past the budget: left in place, the walk would pass
            // them as markers that name no stored head.
            let mut group = Vec::new();
            walk.take_markers(&head.pair.key, &mut group).await?;
            group.push(head.pair.key);
            if !this.offer(group, max_keys) && stop_when_full {
                break;
            }
        }
        if !this.outcome.more_remaining {
            walk.finish().await?;
        }
        Ok(this)
    }

    /// Take one superseded head's group (its key and the markers against it)
    /// unless the budget is spent.
    ///
    /// A group always leaves whole, so one larger than the whole budget still
    /// goes on an otherwise empty pass. Deferring it instead would defer it
    /// forever: every later pass would meet the same group first and stop in
    /// the same place.
    pub(crate) fn offer(&mut self, group: Vec<Vec<u8>>, max_keys: usize) -> bool {
        if self.outcome.more_remaining {
            return false;
        }
        if !self.doomed.is_empty() && self.doomed.len() + group.len() > max_keys {
            self.outcome.more_remaining = true;
            return false;
        }
        self.outcome.heads_removed += 1;
        self.outcome.markers_removed += group.len().saturating_sub(1);
        self.doomed.extend(group);
        true
    }

    /// Delete what was gathered, together with each orphan marker whose
    /// parent was published (`publication`, the prefix of the publication
    /// keys, named like the heads) and so was reclaimed. Without a
    /// publication record orphans stay: their parent may still arrive.
    pub(crate) async fn finish(
        mut self,
        headstore: &NamespaceView,
        orphans: Vec<Vec<u8>>,
        publication: Option<&[u8]>,
        parent_name_at: usize,
        max_keys: usize,
    ) -> Result<PruneOutcome> {
        if let Some(publication) = publication {
            let mut published = Vec::with_capacity(publication.len() + 64);
            for marker in orphans {
                if self.doomed.len() >= max_keys {
                    self.outcome.more_remaining = true;
                    break;
                }
                let Some(parent) = marker_parent(&marker, parent_name_at) else {
                    continue;
                };
                published.clear();
                published.extend_from_slice(publication);
                published.extend_from_slice(parent);
                if headstore.get_parts(&published, &[]).await?.is_some() {
                    self.outcome.orphans_removed += 1;
                    self.doomed.push(marker);
                }
            }
        }
        for key in &self.doomed {
            headstore.delete(key).await?;
        }
        Ok(self.outcome)
    }
}

/// The superseded head's name inside a marker key: what follows the family
/// prefix, up to the last `/`.
pub(crate) fn marker_parent(marker: &[u8], parent_name_at: usize) -> Option<&[u8]> {
    let rest = marker.get(parent_name_at..)?;
    Some(&rest[..rest.iter().rposition(|byte| *byte == b'/')?])
}
