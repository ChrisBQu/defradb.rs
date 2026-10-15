//! A document's head set: every field's heads and the composite's (`C`)
//! together, under one key, so an update reads all of them with one point read.
//!
//! A name is `{field}/{cid text}`, the bytes a head key has after its
//! document prefix, so [`iterator`] rebuilds the head keys of the earlier
//! layout from the set and its callers read the live heads unchanged: live
//! heads only, in head-key order, with seek and reverse.
//!
//! A head is published once, behind its publication guard (`h/r/`), which
//! survives the head being superseded: it is what tells a parent that was
//! published from one whose block has not arrived.

use async_trait::async_trait;
use cid::Cid;
use storage::corekv::{Error, IterOptions, Iterator, Key, KvPair, Reader, Result};
use storage::keys::doc_id_index::decode_doc_short_id_prefix;
use storage::keys::headstore::{HeadstoreDocKey, HeadstoreDocSetKey};
use storage::merge::head_set::{Set, Write};
use std::collections::VecDeque;

use super::{merge_set, read_set};
use crate::block::builder::encode_priority_varint;

const HEADS: &[u8] = b"/d/";
const PUBLISHED: &[u8] = b"/r/";

/// The name of a head in its document's set.
pub fn name(field: &str, cid: &Cid) -> Vec<u8> {
    format!("{field}/{cid}").into_bytes()
}

/// The field and CID a head's name holds.
pub fn parse_name(name: &[u8]) -> Option<(&str, Cid)> {
    let (field, cid) = std::str::from_utf8(name).ok()?.rsplit_once('/')?;
    Some((field, cid.parse().ok()?))
}

/// The CIDs of a set's live heads.
pub fn live_cids(set: &Set) -> Vec<Cid> {
    set.live()
        .filter_map(|(name, _)| parse_name(name).map(|(_, cid)| cid))
        .collect()
}

/// The key of a document's head set.
pub fn set_key(doc_short_id: u64) -> Vec<u8> {
    HeadstoreDocSetKey::new(doc_short_id).bytes()
}

/// The document's head set: one point read, never validated.
pub async fn load<R>(reader: &R, doc_short_id: u64) -> Result<Set>
where
    R: Reader + ?Sized,
{
    read_set(reader, &set_key(doc_short_id)).await
}

/// Merge a write into the document's set. One per set per transaction.
pub async fn commit(
    writer: &datastore::NamespaceView,
    doc_short_id: u64,
    write: &Write,
) -> Result<()> {
    merge_set(writer, &set_key(doc_short_id), write).await
}

/// The publication guards of every head of one document.
pub fn document_publications(doc_short_id: u64) -> Vec<u8> {
    let prefix = HeadstoreDocKey::document_prefix(doc_short_id);
    [PUBLISHED, &prefix[HEADS.len()..]].concat()
}

/// Durable publication identity, separate from CRDT application: a standalone
/// counter can be applied before its composite publishes the field head.
/// This guard survives the head being superseded so replay cannot resurrect a
/// parent.
pub fn publication_key(head: &HeadstoreDocKey) -> Vec<u8> {
    [PUBLISHED, &head.bytes()[HEADS.len()..]].concat()
}

/// Publish a replicated head once, into `write`: it becomes a head at
/// `priority` and each of `parents` stops being one. `set` is the document's
/// set as the caller read it. A parent live in `set` or with a publication
/// guard is superseded; any other has not arrived, and is held ahead until it
/// does. The guard reads are validated.
pub async fn publish(
    writer: &datastore::NamespaceView,
    set: &Set,
    write: &mut Write,
    head: &HeadstoreDocKey,
    priority: u64,
    parents: &[Cid],
) -> Result<()> {
    let recorded = publication_key(head);
    if writer.has(&recorded).await? {
        return Ok(());
    }
    for parent in parents {
        let name = name(&head.field_id, parent);
        let parent_head = HeadstoreDocKey::new(head.doc_short_id, &head.field_id, *parent);
        if set.is_live(&name) || writer.has(&publication_key(&parent_head)).await? {
            write.supersede(&name);
        } else {
            write.supersede_ahead(&name);
        }
    }
    write.add(&name(&head.field_id, &head.cid), priority);
    writer.set(&recorded, &[]).await
}

/// The live heads of the documents `options` names, as head keys with their
/// priority as the value: one document's set is one point read, and a prefix
/// that names no single document walks the sets, one at a time.
pub async fn iterator<R>(reader: &R, options: IterOptions) -> Result<Box<dyn Iterator>>
where
    R: Reader + ?Sized,
{
    let prefix = options
        .prefix()
        .filter(|prefix| prefix.starts_with(HEADS))
        .ok_or_else(|| Error::Iterator("document head iterator requires a /d/ prefix".into()))?
        .to_vec();
    let reverse = options.reverse();
    let one_document = decode_doc_short_id_prefix(&prefix[HEADS.len()..])
        .ok()
        .filter(|(rest, _)| rest.first() == Some(&b'/'))
        .map(|(_, doc_short_id)| doc_short_id);
    let source = match one_document {
        Some(doc_short_id) => Source::One(doc_short_id, load(reader, doc_short_id).await?),
        None => Source::Many(
            reader
                .iterator(
                    IterOptions::new()
                        .with_prefix(HeadstoreDocSetKey::family_prefix())
                        .with_reverse(reverse),
                )
                .await?,
        ),
    };
    Ok(Box::new(LiveHeads {
        source,
        prefix,
        start: options.start().map(<[u8]>::to_vec),
        end: options.end().map(<[u8]>::to_vec),
        reverse,
        bound: None,
        buffer: VecDeque::new(),
        armed: true,
    }))
}

enum Source {
    One(u64, Set),
    Many(Box<dyn Iterator>),
}

struct LiveHeads {
    source: Source,
    prefix: Vec<u8>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    reverse: bool,
    /// Where a seek landed: keys before it (after it, in reverse) are skipped.
    bound: Option<Vec<u8>>,
    buffer: VecDeque<KvPair>,
    /// A single document's set not yet loaded into the buffer.
    armed: bool,
}

impl LiveHeads {
    fn admits(&self, key: &[u8]) -> bool {
        key.starts_with(&self.prefix)
            && self.start.as_deref().is_none_or(|start| key >= start)
            && self.end.as_deref().is_none_or(|end| key < end)
            && self.bound.as_deref().is_none_or(|bound| {
                if self.reverse {
                    key <= bound
                } else {
                    key >= bound
                }
            })
    }

    fn load(&mut self, doc_short_id: u64, set: &Set) {
        let document = HeadstoreDocKey::document_prefix(doc_short_id);
        let mut pairs: Vec<KvPair> = set
            .live()
            .map(|(name, priority)| {
                KvPair::new(
                    [document.as_slice(), name].concat(),
                    encode_priority_varint(priority),
                )
            })
            .filter(|pair| self.admits(&pair.key))
            .collect();
        if self.reverse {
            pairs.reverse();
        }
        self.buffer.extend(pairs);
    }

    /// Refill the buffer from the next set; false when the sets are done.
    async fn refill(&mut self) -> Result<bool> {
        let mut loaded = None;
        match &mut self.source {
            Source::One(doc_short_id, set) => {
                if !std::mem::take(&mut self.armed) {
                    return Ok(false);
                }
                loaded = Some((*doc_short_id, set.clone()));
            }
            Source::Many(sets) => {
                if let Some(pair) = sets.next().await? {
                    let doc = HeadstoreDocSetKey::parse(&pair.key).ok_or_else(|| {
                        Error::Iterator("a head set key is malformed".into())
                    })?;
                    let set = Set::read(&pair.value)
                        .ok_or_else(|| Error::Iterator("a head set is malformed".into()))?;
                    loaded = Some((doc.doc_short_id, set));
                }
            }
        }
        let Some((doc_short_id, set)) = loaded else {
            return Ok(false);
        };
        self.load(doc_short_id, &set);
        Ok(true)
    }
}

impl storage::corekv::private::Sealed for LiveHeads {}
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Iterator for LiveHeads {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        while self.buffer.is_empty() {
            if !self.refill().await? {
                return Ok(None);
            }
        }
        Ok(self.buffer.pop_front())
    }
    async fn close(&mut self) -> Result<()> {
        self.buffer.clear();
        self.armed = false;
        match &mut self.source {
            Source::One(..) => Ok(()),
            Source::Many(sets) => sets.close().await,
        }
    }
    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.buffer.clear();
        self.bound = Some(key.to_vec());
        match &mut self.source {
            Source::One(..) => self.armed = true,
            Source::Many(sets) => {
                let doc = key
                    .strip_prefix(HEADS)
                    .and_then(|rest| decode_doc_short_id_prefix(rest).ok())
                    .map(|(_, doc_short_id)| HeadstoreDocSetKey::new(doc_short_id).bytes());
                match doc {
                    Some(set_key) => sets.seek(&set_key).await?,
                    None => {
                        sets.reset().await?;
                        false
                    }
                };
            }
        }
        Ok(self.next().await?.is_some_and(|pair| {
            self.buffer.push_front(pair);
            true
        }))
    }
    async fn reset(&mut self) -> Result<()> {
        self.buffer.clear();
        self.bound = None;
        match &mut self.source {
            Source::One(..) => self.armed = true,
            Source::Many(sets) => sets.reset().await?,
        }
        Ok(())
    }
    fn is_valid(&self) -> bool {
        match &self.source {
            Source::One(..) => true,
            Source::Many(sets) => sets.is_valid(),
        }
    }
}
