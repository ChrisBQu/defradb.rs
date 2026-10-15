//! The key classes against the house rules' reads and writes, one family at
//! a time: the class each key gets, then what that class does to commits
//! through the adapter.

use std::str::FromStr;

use proptest::prelude::*;
use regolith::{KeyClass, KeyClassifier};

use super::DefraKeyClassifier;
use crate::corekv::{Error, IterOptions, Key, Store};
use crate::keys::{encode_doc_short_id, HeadstoreColKey, HeadstoreColSuperseded, HeadstoreDocKey};
use crate::{RegolithStore, RegolithStoreOptions};

const CID: &str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";

fn class(key: &[u8]) -> KeyClass {
    DefraKeyClassifier.classify(key)
}

/// A key of the headstore as the store writes it, behind the namespace byte.
fn headstore(key: &[u8]) -> Vec<u8> {
    [b"h".as_slice(), key].concat()
}

/// One document's heads, and the markers against them, are each one set,
/// whatever the width of the document's short ID.
#[test]
fn a_documents_heads_and_markers_are_each_one_commutative_set() {
    let cid = cid::Cid::from_str(CID).unwrap();
    for id in [1u64, 47, 239, 240, 287, 2287, 2288, u64::MAX] {
        let heads = headstore(&HeadstoreDocKey::document_prefix(id));
        let head = headstore(&HeadstoreDocKey::new(id, "field", cid).bytes());
        assert_eq!(class(&head), KeyClass::CommutativePrefix { len: heads.len() });
        let markers = [b"h/ds/".as_slice(), &encode_doc_short_id(id), b"/"].concat();
        for set in [heads, markers] {
            for suffix in [b"".as_slice(), b"field/parent/child", b"\xff\0/"] {
                assert_eq!(
                    class(&[set.as_slice(), suffix].concat()),
                    KeyClass::CommutativePrefix { len: set.len() },
                    "{set:?}"
                );
            }
        }
    }
}

/// One collection's heads, and the markers against them, are each one set.
#[test]
fn a_collections_heads_and_markers_are_each_one_commutative_set() {
    let cid = cid::Cid::from_str(CID).unwrap();
    for id in [0u32, 7, 42, u32::MAX] {
        let heads = headstore(&HeadstoreColKey::collection_prefix(id));
        let markers = headstore(&HeadstoreColSuperseded::collection_prefix(id));
        let head = headstore(&HeadstoreColKey::new(id, cid).bytes());
        let marker = headstore(&HeadstoreColSuperseded::new(id, cid, cid).bytes());
        assert_eq!(class(&head), KeyClass::CommutativePrefix { len: heads.len() });
        assert_eq!(class(&marker), KeyClass::CommutativePrefix { len: markers.len() });
    }
}

/// A head key cut short or malformed names no set, and stays ordinary.
#[test]
fn malformed_head_keys_stay_ordinary() {
    for key in [
        b"h/d/".as_slice(),
        b"h/d/\x00/field/cid",
        b"h/d/\xf0",
        b"h/d/\x01",
        b"h/d/\x01x/",
        b"h/ds/",
        b"h/ds/\x01",
        b"h/c/",
        b"h/c//cid",
        b"h/c/1x/cid",
        b"h/c/1",
        b"h/cs/a/parent/child",
        b"h/d",
        b"h/different/",
    ] {
        assert_eq!(class(key), KeyClass::Ordinary, "{key:?}");
    }
}

/// Blocks are content addressed, the arrival journal is a log, and every
/// decision key is ordinary: one winner by design, or a blind write.
#[test]
fn every_other_family_has_its_declared_class() {
    assert_eq!(class(b"b\x01\x55\x00\x03abc"), KeyClass::ContentAddressed);
    for key in [
        b"s/arrival/7/head".as_slice(),
        b"s/arrival/7/row/00000000000000000001",
        b"s/arrival/7/doc/bae-1",
    ] {
        assert_eq!(class(key), KeyClass::Log, "{key:?}");
    }
    for key in [
        b"".as_slice(),
        b"b",
        b"b\x01invalid",
        b"bm\x01\x55\x00\x03abc",
        b"s/arrival",
        b"h/p/\x01/0000000000000001/cid",
        b"h/p/applied",
        b"h/r/\x01/field/cid",
        b"h/f/users/email/cid",
        b"h/g/users/cid",
        b"d/d/collection/\x01",
        b"d/del/collection/\x01",
        b"d/v/collection/\x01",
        b"d/\x01/\x02/value",
        b"s/d/p/doc",
        b"s/seq/doc",
        b"a/zanzibar/7/rel/users/bae-1/owner/subject",
        b"a/zanzibar/counter",
        b"e\x01\x55\x00\x03abc",
        b"unknown",
    ] {
        assert_eq!(class(key), KeyClass::Ordinary, "{key:?}");
    }
}

/// A head set's keys and a malformed tail: any of them is fine, since only
/// the set's prefix decides the class.
fn suffixes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(
        prop_oneof![Just(b'/'), Just(0u8), Just(0xf0u8), any::<u8>()],
        0..16,
    )
}

proptest! {
    /// regolith's rule for a commutative prefix of `len` bytes: every key
    /// that shares those bytes is in the same one.
    #[test]
    fn a_commutative_set_holds_every_key_that_shares_its_prefix(
        id in 1u64..,
        collection in any::<u32>(),
        mine in suffixes(),
        theirs in suffixes(),
    ) {
        let document = encode_doc_short_id(id);
        let collection = collection.to_string();
        for set in [
            [b"h/d/".as_slice(), &document, b"/"].concat(),
            [b"h/ds/".as_slice(), &document, b"/"].concat(),
            [b"h/c/".as_slice(), collection.as_bytes(), b"/"].concat(),
            [b"h/cs/".as_slice(), collection.as_bytes(), b"/"].concat(),
        ] {
            let expected = KeyClass::CommutativePrefix { len: set.len() };
            prop_assert_eq!(class(&[set.as_slice(), &mine].concat()), expected);
            prop_assert_eq!(class(&[set.as_slice(), &theirs].concat()), expected);
        }
    }
}

/// Seeds `keys`, then lets two transactions each scan `prefix` and delete
/// `doomed` inside what they scanned, as two writers reclaiming the same
/// superseded head do. Returns whether the second commit conflicted.
async fn second_reclaim_conflicts(keys: &[&[u8]], prefix: &[u8], doomed: &[u8]) -> bool {
    let store = RegolithStore::in_memory().unwrap();
    let mut seed = store.new_txn(false).await.unwrap();
    for key in keys {
        seed.set(key, b"").await.unwrap();
    }
    seed.commit().await.unwrap();
    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();
    for txn in [&mut first, &mut second] {
        let mut iter = txn
            .iterator(IterOptions::new().with_prefix(prefix.to_vec()))
            .await
            .unwrap();
        let mut scanned = 0;
        while iter.next().await.unwrap().is_some() {
            scanned += 1;
        }
        iter.close().await.unwrap();
        drop(iter);
        assert_eq!(scanned, keys.len(), "{prefix:?}");
        txn.delete(doomed).await.unwrap();
    }
    first.commit().await.unwrap();
    match second.commit().await {
        Ok(()) => false,
        Err(Error::TxnConflictDetails(_)) => true,
        Err(error) => panic!("{prefix:?}: {error}"),
    }
}

/// Two writers of one document's (or one collection's) head set reclaim the
/// same superseded head at once, each having scanned that set: both commit,
/// because the set is commutative.
#[tokio::test]
async fn one_head_set_commutes() {
    let heads: &[&[u8]] = &[b"h/d/\x01/field/a", b"h/d/\x01/field/b"];
    assert!(!second_reclaim_conflicts(heads, b"h/d/\x01/", heads[0]).await);
    let markers: &[&[u8]] = &[b"h/ds/\x01/field/a/x", b"h/ds/\x01/field/b/y"];
    assert!(!second_reclaim_conflicts(markers, b"h/ds/\x01/", markers[0]).await);
    let heads: &[&[u8]] = &[b"h/c/1/a", b"h/c/1/b"];
    assert!(!second_reclaim_conflicts(heads, b"h/c/1/", heads[0]).await);
    let markers: &[&[u8]] = &[b"h/cs/1/a/x", b"h/cs/1/b/y"];
    assert!(!second_reclaim_conflicts(markers, b"h/cs/1/", markers[0]).await);
}

/// A scan across two documents' (or two collections') head sets is no head
/// set: a delete inside it is validated as a read, so the second of two such
/// reclaims conflicts. While a whole family counted as one set, both
/// committed.
#[tokio::test]
async fn a_scan_across_head_sets_is_validated() {
    let heads: &[&[u8]] = &[b"h/d/\x01/field/a", b"h/d/\x02/field/b"];
    assert!(second_reclaim_conflicts(heads, b"h/d/", heads[0]).await);
    let markers: &[&[u8]] = &[b"h/ds/\x01/field/a/x", b"h/ds/\x02/field/b/y"];
    assert!(second_reclaim_conflicts(markers, b"h/ds/", markers[0]).await);
    let heads: &[&[u8]] = &[b"h/c/1/a", b"h/c/2/b"];
    assert!(second_reclaim_conflicts(heads, b"h/c/", heads[0]).await);
    let markers: &[&[u8]] = &[b"h/cs/1/a/x", b"h/cs/2/b/y"];
    assert!(second_reclaim_conflicts(markers, b"h/cs/", markers[0]).await);
}

async fn same_block_writes(options: RegolithStoreOptions) {
    let dir = tempfile::tempdir().unwrap();
    let store = RegolithStore::open_with_options(dir.path(), options).unwrap();
    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();
    for txn in [&mut first, &mut second] {
        assert!(txn.get(b"b\x01\x55\x00\x03abc").await.unwrap().is_none());
        txn.set(b"b\x01\x55\x00\x03abc", b"same block")
            .await
            .unwrap();
    }
    first.commit().await.unwrap();
    second.commit().await.unwrap();
}

/// Two creates of one block both commit, sealed at rest or not.
#[tokio::test]
async fn identical_blocks_commit_with_engine_encryption() {
    same_block_writes(RegolithStoreOptions::default().with_encryption_key([42; 32])).await;
    same_block_writes(RegolithStoreOptions::memory()).await;
}

/// Decision keys have one winner.
#[tokio::test]
async fn ordinary_decision_keys_have_one_winner() {
    let store = RegolithStore::in_memory().unwrap();
    for key in [
        b"bm\x01\x55\x00\x03abc".as_slice(),
        b"h/p/applied",
        b"h/r/\x01/field/cid",
        b"s/d/p/doc",
        b"a/zanzibar/owner",
        b"d/del/doc",
    ] {
        let mut first = store.new_txn(false).await.unwrap();
        let mut second = store.new_txn(false).await.unwrap();
        for txn in [&mut first, &mut second] {
            assert!(txn.get(key).await.unwrap().is_none());
            txn.set(key, b"present").await.unwrap();
        }
        first.commit().await.unwrap();
        assert!(
            matches!(second.commit().await, Err(Error::TxnConflictDetails(_))),
            "{key:?}"
        );
    }
}

/// A block's key names its bytes: two different values under one key are
/// refused, not merged.
#[tokio::test]
async fn concurrent_different_block_bytes_are_rejected() {
    let store = RegolithStore::in_memory().unwrap();
    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();
    first.set(b"b\x01\x55\x00\x03abc", b"first").await.unwrap();
    second
        .set(b"b\x01\x55\x00\x03abc", b"second")
        .await
        .unwrap();
    first.commit().await.unwrap();
    assert!(matches!(second.commit().await, Err(Error::Backend(_))));
    let read = store.new_txn(true).await.unwrap();
    assert_eq!(
        read.get(b"b\x01\x55\x00\x03abc").await.unwrap().as_deref(),
        Some(b"first".as_slice())
    );
}

/// A decision taken on a block being present conflicts with its deletion.
#[tokio::test]
async fn deleting_a_block_invalidates_a_presence_decision() {
    let store = RegolithStore::in_memory().unwrap();
    let mut seed = store.new_txn(false).await.unwrap();
    seed.set(b"b\x01\x55\x00\x03abc", b"block").await.unwrap();
    seed.commit().await.unwrap();
    let mut reader = store.new_txn(false).await.unwrap();
    assert!(reader.get(b"b\x01\x55\x00\x03abc").await.unwrap().is_some());
    reader.set(b"decision", b"block exists").await.unwrap();
    let mut delete = store.new_txn(false).await.unwrap();
    delete.delete(b"b\x01\x55\x00\x03abc").await.unwrap();
    delete.commit().await.unwrap();
    assert!(matches!(
        reader.commit().await,
        Err(Error::TxnConflictDetails(_))
    ));
}
