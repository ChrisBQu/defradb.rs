//! The sender scope of a document head, and which head of a scope is current.
//!
//! A peer announces heads of a document (or a collection) one after another,
//! each with a monotonic priority. Within one sender scope (the announcing
//! peer, the collection and the document) a newer head carries every older
//! one forward, so the receiver keeps one obligation per scope: the current
//! head. A head with no sender or no priority belongs to no scope and is
//! independent of every other. The in-memory pending registry and the
//! durable registrations decide with the one rule here.

use cid::Cid;

/// The announcing peer, collection and document of a head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Scope {
    source_peer: String,
    collection_id: String,
    doc_id: String,
}

/// A head's place in its scope: by priority, then by CID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct HeadVersion {
    pub(crate) priority: u64,
    pub(crate) cid: Cid,
}

/// What a head is to its scope's current head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeDecision {
    /// It belongs to no scope.
    Independent,
    /// It is the current head, or the scope has none.
    Current,
    /// A newer or equal head is current.
    CoveredByCurrent,
    /// It is newer than the current head, which it retires.
    Supersedes(Cid),
}

impl Scope {
    /// The scope and version of the head `root`, or `None` when it has no
    /// sender or no priority.
    pub(crate) fn of(
        root: Cid,
        source_peer: Option<&str>,
        collection_id: &str,
        doc_id: &str,
        head_priority: Option<u64>,
    ) -> Option<(Self, HeadVersion)> {
        Some((
            Self {
                source_peer: source_peer?.to_owned(),
                collection_id: collection_id.to_owned(),
                doc_id: doc_id.to_owned(),
            },
            HeadVersion {
                priority: head_priority?,
                cid: root,
            },
        ))
    }

    /// The scope's key under `prefix`, each part length-prefixed so no part
    /// can run into the next.
    pub(crate) fn key(&self, prefix: &[u8]) -> Vec<u8> {
        let parts = [&self.source_peer, &self.collection_id, &self.doc_id];
        let mut key = Vec::with_capacity(
            prefix.len() + parts.iter().map(|part| part.len() + 9).sum::<usize>(),
        );
        key.extend_from_slice(prefix);
        for part in parts {
            key.extend_from_slice(format!("{}:", part.len()).as_bytes());
            key.extend_from_slice(part.as_bytes());
        }
        key
    }
}

impl HeadVersion {
    /// The priority as 8 big-endian bytes, then the CID's bytes.
    pub(crate) fn to_bytes(self) -> Vec<u8> {
        let mut bytes = self.priority.to_be_bytes().to_vec();
        bytes.extend_from_slice(&self.cid.to_bytes());
        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (priority, cid) = bytes.split_first_chunk::<8>()?;
        Some(Self {
            priority: u64::from_be_bytes(*priority),
            cid: Cid::try_from(cid).ok()?,
        })
    }
}

impl ScopeDecision {
    /// The decision for a head at `version` when the scope's current head is
    /// `current`.
    pub(crate) fn of(version: HeadVersion, current: Option<HeadVersion>) -> Self {
        match current {
            None => Self::Current,
            Some(current) if current.cid == version.cid => Self::Current,
            Some(current) if version <= current => Self::CoveredByCurrent,
            Some(current) => Self::Supersedes(current.cid),
        }
    }

    /// Whether the scope has a current head and `version` is it or newer.
    pub(crate) fn refreshes_or_supersedes(
        version: HeadVersion,
        current: Option<HeadVersion>,
    ) -> bool {
        current.is_some_and(|current| current.cid == version.cid || version > current)
    }

    /// Whether another head of the scope, newer or equal, is current.
    pub(crate) fn covered(version: HeadVersion, current: Option<HeadVersion>) -> bool {
        matches!(Self::of(version, current), Self::CoveredByCurrent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash_codetable::{Code, MultihashDigest};

    fn cid(seed: &[u8]) -> Cid {
        Cid::new_v1(0x55, Code::Sha2_256.digest(seed))
    }

    fn version(priority: u64, seed: &[u8]) -> HeadVersion {
        HeadVersion {
            priority,
            cid: cid(seed),
        }
    }

    #[test]
    fn a_head_with_no_sender_or_no_priority_has_no_scope() {
        assert!(Scope::of(cid(b"a"), None, "c", "d", Some(1)).is_none());
        assert!(Scope::of(cid(b"a"), Some("p"), "c", "d", None).is_none());
        assert!(Scope::of(cid(b"a"), Some("p"), "c", "d", Some(1)).is_some());
    }

    #[test]
    fn the_newest_head_of_a_scope_is_current() {
        let old = version(1, b"old");
        let new = version(2, b"new");
        assert_eq!(ScopeDecision::of(new, None), ScopeDecision::Current);
        assert_eq!(ScopeDecision::of(new, Some(new)), ScopeDecision::Current);
        assert_eq!(
            ScopeDecision::of(new, Some(old)),
            ScopeDecision::Supersedes(old.cid)
        );
        assert_eq!(
            ScopeDecision::of(old, Some(new)),
            ScopeDecision::CoveredByCurrent
        );
        assert!(ScopeDecision::covered(old, Some(new)));
        assert!(!ScopeDecision::covered(new, Some(new)));
        assert!(ScopeDecision::refreshes_or_supersedes(new, Some(new)));
        assert!(ScopeDecision::refreshes_or_supersedes(new, Some(old)));
        assert!(!ScopeDecision::refreshes_or_supersedes(old, Some(new)));
        assert!(!ScopeDecision::refreshes_or_supersedes(new, None));
    }

    #[test]
    fn scope_keys_do_not_run_parts_together() {
        let (left, _) = Scope::of(cid(b"a"), Some("ab"), "c", "d", Some(1)).unwrap();
        let (right, _) = Scope::of(cid(b"a"), Some("a"), "bc", "d", Some(1)).unwrap();
        assert_ne!(left.key(b"/scope/"), right.key(b"/scope/"));
        assert!(left.key(b"/scope/").starts_with(b"/scope/"));
    }

    #[test]
    fn a_head_version_round_trips_through_its_bytes() {
        let head = version(u64::MAX - 1, b"head");
        assert_eq!(HeadVersion::from_bytes(&head.to_bytes()), Some(head));
        assert_eq!(HeadVersion::from_bytes(b"short"), None);
    }
}
