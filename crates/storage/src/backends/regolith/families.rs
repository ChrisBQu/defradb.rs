//! The families of the store's keys that regolith is told about, declared
//! once.
//!
//! A family is the bytes its keys start with (`h/d/` for document heads),
//! the parts after them that name the one entity a key belongs to (the
//! document's short ID), each ended by a separator, and the class regolith
//! validates the family's keys by. The key classifier (`classifier`) takes
//! the class from here, and a commutative set's length from the entity; the
//! prefix extractor (`prefix`) builds its prefix filters from the entity.
//! Both read this one table, so they cannot disagree on what one head set,
//! one document or one collection is.
//!
//! Where an entity ends is decided by the bytes before its end, and the end
//! is a separator, so every key that starts with an entity's prefix has that
//! same entity. Both readers rely on it: a commutative set of `len` bytes
//! holds every key that shares them, and a prefix seek may pass over a table
//! whose filter lacks the entity its prefix names. A key cut short, or
//! malformed, has no entity.

use crate::keys::doc_id_index::doc_short_id_len;
use crate::keys::utils::{uvarint_len, SEPARATOR};

/// How regolith validates the keys of a family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Class {
    /// As any key is validated.
    Ordinary,
    /// One entity's keys are one set whose concurrent writers leave the
    /// union: a head set, or the markers against one.
    CommutativeSet,
    /// The rest of the key is the content identifier of its value.
    ContentAddressed,
    /// Written only by the commit-ordered append of the arrival journal.
    Log,
}

/// One part of a key that names its entity.
#[derive(Clone, Copy, Debug)]
enum Part {
    /// A document short ID: an ordered uvarint, never zero.
    DocShortId,
    /// Any ordered uvarint: a collection's or an index's short ID.
    Uvarint,
    /// A number in decimal text: a collection's short ID in a head key.
    Digits,
    /// Any text but a separator, at least one byte.
    Text,
    /// These bytes exactly.
    Exactly(&'static [u8]),
}

impl Part {
    /// How many bytes of `rest` this part takes, or `None` when `rest` does
    /// not start with all of it.
    fn len(self, rest: &[u8]) -> Option<usize> {
        match self {
            Self::DocShortId => doc_short_id_len(rest),
            Self::Uvarint => uvarint_len(rest),
            Self::Digits => {
                text_len(rest).filter(|&len| rest[..len].iter().all(u8::is_ascii_digit))
            }
            Self::Text => text_len(rest),
            Self::Exactly(bytes) => rest.starts_with(bytes).then_some(bytes.len()),
        }
    }
}

/// The length of the text `rest` starts with, up to its separator: at least
/// one byte, and `None` when no separator ends it.
fn text_len(rest: &[u8]) -> Option<usize> {
    rest.iter()
        .position(|&byte| byte == SEPARATOR)
        .filter(|&len| len > 0)
}

/// A family of keys.
#[derive(Debug)]
pub(super) struct Family {
    /// The bytes every key of the family starts with, namespace included.
    start: &'static [u8],
    /// The parts after `start` that name a key's entity, each ended by a
    /// separator. Empty for a family nothing reads by entity.
    entity: &'static [Part],
    /// How regolith validates the family's keys.
    class: Class,
}

impl Family {
    /// How regolith validates this family's keys.
    pub(super) fn class(&self) -> Class {
        self.class
    }

    /// The bytes of `key` after this family's start.
    pub(super) fn rest<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        key.get(self.start.len()..).unwrap_or_default()
    }

    /// The length of `key`'s entity prefix: the family's start and every
    /// part of the entity with the separator that ends it. `None` for a
    /// family with no entity, and for a key cut short or malformed.
    pub(super) fn entity_len(&self, key: &[u8]) -> Option<usize> {
        if self.entity.is_empty() {
            return None;
        }
        self.entity.iter().try_fold(self.start.len(), |end, part| {
            let end = end + part.len(key.get(end..)?)?;
            (key.get(end) == Some(&SEPARATOR)).then_some(end + 1)
        })
    }
}

const fn family(start: &'static [u8], entity: &'static [Part], class: Class) -> Family {
    Family {
        start,
        entity,
        class,
    }
}

/// Every family, in the order a key is matched against them: the first
/// family whose start the key starts with is the key's family.
///
/// Where one start begins another, the longer comes first: `bm` before `b`,
/// and `d/d/`, `d/del/` and `d/v/` before `d/`. The order keeps an entity
/// stable as a key grows: a key still shorter than one of the longer starts
/// falls to the shorter family, which names no entity in so few bytes (`b`
/// names none, and `d/` needs six).
static FAMILIES: [Family; 15] = [
    // Block merge markers: decision keys, not blocks.
    family(b"bm", &[], Class::Ordinary),
    // Blocks, keyed by the CID of their bytes (`keys::BlockstoreKey`).
    family(b"b", &[], Class::ContentAddressed),
    // The arrival journal: one collection's head, rows and once keys.
    family(b"s/arrival/", &[], Class::Log),
    // A document's head keys (`keys::HeadstoreDocKey`), and the markers
    // against them (`db::block::heads::document`).
    family(b"h/d/", &[Part::DocShortId], Class::CommutativeSet),
    family(b"h/ds/", &[Part::DocShortId], Class::CommutativeSet),
    // A collection's head keys, and the markers against them
    // (`keys::HeadstoreColKey`, `keys::HeadstoreColSuperseded`).
    family(b"h/c/", &[Part::Digits], Class::CommutativeSet),
    family(b"h/cs/", &[Part::Digits], Class::CommutativeSet),
    // A document's commit priority index (`keys::HeadstorePriorityKey`), and
    // the guards that record its heads' publication.
    family(b"h/p/", &[Part::DocShortId], Class::Ordinary),
    family(b"h/r/", &[Part::DocShortId], Class::Ordinary),
    // A collection's document values, deletion markers and schema versions
    // (`keys::doc_key`, `keys::deleted_doc_key`, a collection's version key).
    family(b"d/d/", &[Part::Text], Class::Ordinary),
    family(b"d/del/", &[Part::Text], Class::Ordinary),
    family(b"d/v/", &[Part::Text], Class::Ordinary),
    // One index's entries (`keys::IndexDataStoreKey`), and one collection's
    // field values of one instance (`keys::DataStoreKey`).
    family(b"d/", &[Part::Uvarint, Part::Uvarint], Class::Ordinary),
    // One ACP object's relationship tuples, under their policy
    // (`zanzibar::Relationship::storage_key`), and in the earlier ACP store
    // (`acp::RelationTuple::storage_key`).
    family(
        b"a/zanzibar/",
        &[Part::Text, Part::Exactly(b"rel"), Part::Text, Part::Text],
        Class::Ordinary,
    ),
    family(b"a/acp/", &[Part::Text, Part::Text], Class::Ordinary),
];

/// The family `key` belongs to, or `None` for a key of no declared family.
pub(super) fn family_of(key: &[u8]) -> Option<&'static Family> {
    FAMILIES.iter().find(|family| key.starts_with(family.start))
}

/// Keys of every family and of none, cut short or malformed as often as
/// whole: what property tests of the table's readers walk.
#[cfg(test)]
pub(super) fn keys() -> impl proptest::strategy::Strategy<Value = Vec<u8>> {
    use proptest::prelude::*;

    let starts: Vec<Vec<u8>> = FAMILIES
        .iter()
        .flat_map(|family| (0..=family.start.len()).map(|len| family.start[..len].to_vec()))
        .collect();
    let pieces: Vec<Vec<u8>> = [
        b"/".as_slice(),
        b"rel/",
        b"7/",
        b"12/",
        b"\x01/",
        b"\xf0\x2f/",
        b"\xf9\x00\x01/",
        b"\x00/",
        b"\xf9\x00\x00/",
        b"users/",
        b"bae-1/",
        b"x",
    ]
    .iter()
    .map(|piece| piece.to_vec())
    .collect();
    let piece = prop_oneof![
        proptest::sample::select(pieces),
        any::<u8>().prop_map(|byte| vec![byte]),
    ];
    (
        proptest::sample::select(starts),
        proptest::collection::vec(piece, 0..8),
    )
        .prop_map(|(mut key, pieces)| {
            for piece in pieces {
                key.extend(piece);
            }
            key
        })
}

#[cfg(test)]
mod tests;
