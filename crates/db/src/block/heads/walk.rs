//! One pass over a head family and its marker family together.
//!
//! A head key names a block; a marker key names the head it supersedes and
//! then, after a final `/`, the child that superseded it. Both families sort by
//! the head they name: every byte of a head name after the family prefix is a
//! uvarint short ID (prefix-free), a field name or a CID text, and the last two
//! sort above `/`, so the markers of a smaller head sort before those of a
//! larger one. Walking the two ranges side by side answers "is this head
//! superseded" holding one marker at a time, whatever the history.

use std::cmp::Ordering;

use storage::corekv::{Iterator, KvPair, Result};

/// The heads and markers of one prefix, walked in the same direction.
pub(crate) struct HeadWalk {
    heads: Box<dyn Iterator>,
    markers: Box<dyn Iterator>,
    /// Bytes in front of the head name in a head key.
    head_name_at: usize,
    /// Bytes in front of the superseded head's name in a marker key.
    parent_name_at: usize,
    reverse: bool,
    /// The marker the walk stands on, read and not yet passed.
    marker: Option<KvPair>,
    /// Markers passed because no stored head carries their name, kept only
    /// when a reclamation asked for them, up to its room.
    orphans: Option<(Vec<Vec<u8>>, usize)>,
}

/// One stored head key and whether a marker supersedes it.
pub(crate) struct WalkedHead {
    pub(crate) pair: KvPair,
    pub(crate) superseded: bool,
}

impl HeadWalk {
    pub(crate) fn new(
        heads: Box<dyn Iterator>,
        markers: Box<dyn Iterator>,
        head_name_at: usize,
        parent_name_at: usize,
        reverse: bool,
    ) -> Self {
        Self {
            heads,
            markers,
            head_name_at,
            parent_name_at,
            reverse,
            marker: None,
            orphans: None,
        }
    }

    /// Keep up to `room` markers whose head has no stored key.
    pub(crate) fn collect_orphans(mut self, room: usize) -> Self {
        self.orphans = Some((Vec::new(), room));
        self
    }

    /// The markers kept by [`Self::collect_orphans`].
    pub(crate) fn take_orphans(&mut self) -> Vec<Vec<u8>> {
        self.orphans
            .as_mut()
            .map(|(orphans, _)| std::mem::take(orphans))
            .unwrap_or_default()
    }

    /// The next stored head key and whether a marker supersedes it.
    pub(crate) async fn next(&mut self) -> Result<Option<WalkedHead>> {
        let Some(pair) = self.heads.next().await? else {
            return Ok(None);
        };
        let Some(name) = pair.key.get(self.head_name_at..) else {
            return Ok(Some(WalkedHead {
                pair,
                superseded: false,
            }));
        };
        let superseded = loop {
            match self.compare(name).await? {
                // Markers of heads before this one name no stored key.
                Some(Ordering::Less) => self.pass_orphan(),
                Some(Ordering::Equal) => break true,
                Some(Ordering::Greater) | None => break false,
            }
        };
        Ok(Some(WalkedHead { pair, superseded }))
    }

    /// Move every marker naming `head` into `group`. Call right after
    /// [`Self::next`] returned `head` as superseded.
    pub(crate) async fn take_markers(
        &mut self,
        head: &[u8],
        group: &mut Vec<Vec<u8>>,
    ) -> Result<()> {
        let Some(name) = head.get(self.head_name_at..) else {
            return Ok(());
        };
        while self.compare(name).await? == Some(Ordering::Equal) {
            if let Some(marker) = self.marker.take() {
                group.push(marker.key);
            }
        }
        Ok(())
    }

    /// Pass every marker left once the heads ran out: none names a stored head.
    pub(crate) async fn finish(&mut self) -> Result<()> {
        while self
            .orphans
            .as_ref()
            .is_some_and(|(orphans, room)| orphans.len() < *room)
            && self.current().await?.is_some()
        {
            self.pass_orphan();
        }
        Ok(())
    }

    pub(crate) async fn seek(&mut self, head_key: &[u8], marker_key: &[u8]) -> Result<bool> {
        self.marker = None;
        self.markers.seek(marker_key).await?;
        self.heads.seek(head_key).await
    }

    pub(crate) async fn reset(&mut self) -> Result<()> {
        self.marker = None;
        self.heads.reset().await?;
        self.markers.reset().await
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        self.marker = None;
        self.heads.close().await?;
        self.markers.close().await
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.heads.is_valid()
    }

    /// Where the current marker's head sorts against `name`, in walk order.
    async fn compare(&mut self, name: &[u8]) -> Result<Option<Ordering>> {
        let parent_name_at = self.parent_name_at;
        let reverse = self.reverse;
        Ok(self.current().await?.map(|marker| {
            let order = parent_name(&marker.key, parent_name_at).cmp(name);
            if reverse {
                order.reverse()
            } else {
                order
            }
        }))
    }

    /// The marker the walk stands on, reading the next well-formed one if it
    /// stands on none.
    async fn current(&mut self) -> Result<Option<&KvPair>> {
        while self.marker.is_none() {
            let Some(marker) = self.markers.next().await? else {
                return Ok(None);
            };
            if marker.key.len() > self.parent_name_at
                && marker.key[self.parent_name_at..].contains(&b'/')
            {
                self.marker = Some(marker);
            }
        }
        Ok(self.marker.as_ref())
    }

    fn pass_orphan(&mut self) {
        let marker = self.marker.take();
        if let (Some((orphans, room)), Some(marker)) = (self.orphans.as_mut(), marker) {
            if orphans.len() < *room {
                orphans.push(marker.key);
            }
        }
    }
}

/// The superseded head's name inside a well-formed marker key.
fn parent_name(marker: &[u8], at: usize) -> &[u8] {
    let rest = &marker[at..];
    let end = rest
        .iter()
        .rposition(|byte| *byte == b'/')
        .unwrap_or(rest.len());
    &rest[..end]
}
