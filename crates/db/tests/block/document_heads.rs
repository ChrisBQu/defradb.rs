use cid::Cid;
use db::block::heads::document::{commit, iterator, load, publish};
use storage::corekv::{IterOptions, Key};
use storage::keys::headstore::HeadstoreDocKey;
use storage::merge::head_set::Write;
use storage::RegolithStore;

const DOC: u64 = 47; // The binary short ID contains a slash.
fn cid(value: &[u8]) -> Cid {
    defra_core::block::generate_cid_from_bytes(value).unwrap()
}
fn head(cid: Cid) -> HeadstoreDocKey {
    HeadstoreDocKey::new(DOC, "name", cid)
}
/// Publish `cid` over `parents` in `txn`, as a replicated block does.
async fn publish_head(txn: &db::DbTxn<RegolithStore>, cid: Cid, priority: u64, parents: &[Cid]) {
    let view = txn.headstore().unwrap();
    let set = load(&view, DOC).await.unwrap();
    let mut write = Write::default();
    publish(&view, &set, &mut write, &head(cid), priority, parents)
        .await
        .unwrap();
    commit(&view, DOC, &write).await.unwrap();
}
async fn live(view: &datastore::NamespaceView) -> Vec<Cid> {
    let mut iter = iterator(
        view,
        IterOptions::new().with_prefix(HeadstoreDocKey::document_prefix(DOC)),
    )
    .await
    .unwrap();
    let mut result = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        result.push(HeadstoreDocKey::parse(&pair.key).unwrap().cid);
    }
    iter.close().await.unwrap();
    result.sort_by_cached_key(Cid::to_string);
    result
}
async fn ahead(view: &datastore::NamespaceView) -> usize {
    load(view, DOC).await.unwrap().ahead().count()
}

/// Two writers that both saw the parent each publish a child: both commit, a
/// snapshot taken before keeps the parent, and the parent is no head after.
#[tokio::test]
async fn overlapping_document_heads_commit_and_snapshots_are_preserved() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, left, right) = (cid(b"parent"), cid(b"left"), cid(b"right"));
    let seed = db.new_txn(false).await.unwrap();
    publish_head(&seed, parent, 1, &[]).await;
    seed.commit().await.unwrap();
    let old = db.new_txn(true).await.unwrap();
    let a = db.new_txn(false).await.unwrap();
    let b = db.new_txn(false).await.unwrap();
    for (txn, child) in [(&a, left), (&b, right)] {
        assert_eq!(live(&txn.headstore().unwrap()).await, vec![parent]);
        publish_head(txn, child, 2, &[parent]).await;
    }
    b.commit().await.unwrap();
    a.commit().await.unwrap();
    let mut expected = vec![left, right];
    expected.sort_by_cached_key(Cid::to_string);
    assert_eq!(live(&old.headstore().unwrap()).await, vec![parent]);
    let after = db.new_txn(true).await.unwrap();
    let view = after.headstore().unwrap();
    assert_eq!(live(&view).await, expected);
    assert_eq!(ahead(&view).await, 0);
}

/// A child that arrives before its parent hides the parent when it arrives,
/// and leaves nothing behind.
#[tokio::test]
async fn a_child_before_its_parent_hides_the_late_parent() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, child) = (cid(b"late parent"), cid(b"early child"));
    let txn = db.new_txn(false).await.unwrap();
    publish_head(&txn, child, 2, &[parent]).await;
    txn.commit().await.unwrap();
    let txn = db.new_txn(true).await.unwrap();
    assert_eq!(live(&txn.headstore().unwrap()).await, vec![child]);
    assert_eq!(ahead(&txn.headstore().unwrap()).await, 1);
    txn.discard().unwrap();
    let txn = db.new_txn(false).await.unwrap();
    publish_head(&txn, parent, 1, &[]).await;
    txn.commit().await.unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    assert_eq!(live(&view).await, vec![child]);
    assert_eq!(ahead(&view).await, 0);
}

#[tokio::test]
async fn replay_of_a_superseded_parent_cannot_resurrect_it() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, child) = (cid(b"published parent"), cid(b"published child"));
    let txn = db.new_txn(false).await.unwrap();
    publish_head(&txn, parent, 1, &[]).await;
    publish_head(&txn, child, 2, &[parent]).await;
    txn.commit().await.unwrap();
    // A replay may already own an older snapshot when the child commits.
    let replay = db.new_txn(false).await.unwrap();
    publish_head(&replay, parent, 1, &[]).await;
    replay.commit().await.unwrap();
    let replay = db.new_txn(false).await.unwrap();
    publish_head(&replay, parent, 1, &[]).await;
    replay.commit().await.unwrap();
    let txn = db.new_txn(true).await.unwrap();
    assert_eq!(live(&txn.headstore().unwrap()).await, vec![child]);
}

/// A writer that supersedes a published parent concurrently with the write
/// that superseded it first: a fork never conflicts, and the head set never
/// holds the parent or a name ahead.
#[tokio::test]
async fn a_late_sibling_of_a_superseded_parent_forks_without_conflict() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, left, right) = (
        cid(b"shared parent"),
        cid(b"left child"),
        cid(b"right child"),
    );
    let seed = db.new_txn(false).await.unwrap();
    publish_head(&seed, parent, 1, &[]).await;
    seed.commit().await.unwrap();

    let late = db.new_txn(false).await.unwrap();
    let early = db.new_txn(false).await.unwrap();
    publish_head(&early, left, 2, &[parent]).await;
    early.commit().await.unwrap();
    publish_head(&late, right, 2, &[parent]).await;
    late.commit().await.expect("a fork never conflicts");

    let mut expected = vec![left, right];
    expected.sort_by_cached_key(Cid::to_string);
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    assert_eq!(live(&view).await, expected);
    assert_eq!(ahead(&view).await, 0);
}

/// The view over a head set answers the same live heads in either direction
/// and after a seek, for every pattern of superseded heads across fields.
#[tokio::test]
async fn live_heads_agree_forward_reverse_and_after_seek() {
    let fields = ["C", "name", "score"];
    let heads: Vec<HeadstoreDocKey> = fields
        .iter()
        .flat_map(|field| {
            (0..2u8).map(move |i| HeadstoreDocKey::new(DOC, *field, cid(&[i, field.len() as u8])))
        })
        .collect();
    for pattern in 0u32..(1 << heads.len()) {
        let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
        let txn = db.new_txn(false).await.unwrap();
        {
            let view = txn.headstore().unwrap();
            let mut write = Write::default();
            for (index, head) in heads.iter().enumerate() {
                let name = db::block::heads::document::name(&head.field_id, &head.cid);
                write.add(&name, 1);
                if pattern & (1 << index) != 0 {
                    write.supersede(&name);
                }
            }
            commit(&view, DOC, &write).await.unwrap();
        }
        txn.commit().await.unwrap();

        let mut expected: Vec<Vec<u8>> = heads
            .iter()
            .enumerate()
            .filter(|(index, _)| pattern & (1 << index) == 0)
            .map(|(_, head)| head.bytes())
            .collect();
        expected.sort();
        let txn = db.new_txn(true).await.unwrap();
        let view = txn.headstore().unwrap();
        for reverse in [false, true] {
            let options = IterOptions::new()
                .with_prefix(HeadstoreDocKey::document_prefix(DOC))
                .with_reverse(reverse);
            let mut iter = iterator(&view, options.clone()).await.unwrap();
            let mut walked = Vec::new();
            while let Some(pair) = iter.next().await.unwrap() {
                walked.push(pair.key);
            }
            if reverse {
                walked.reverse();
            }
            assert_eq!(walked, expected, "pattern {pattern:#08b} reverse {reverse}");

            // Seek to every stored head: the walk resumes on the first live
            // head at or past it, in the iteration's direction.
            for target in &heads {
                let target = target.bytes();
                let mut iter = iterator(&view, options.clone()).await.unwrap();
                let found = iter.seek(&target).await.unwrap();
                let got = iter.next().await.unwrap().map(|pair| pair.key);
                let want = if reverse {
                    expected.iter().rev().find(|key| **key <= target).cloned()
                } else {
                    expected.iter().find(|key| **key >= target).cloned()
                };
                assert_eq!(
                    found,
                    want.is_some(),
                    "pattern {pattern:#08b} reverse {reverse}"
                );
                assert_eq!(got, want, "pattern {pattern:#08b} reverse {reverse}");
            }
        }
    }
}

/// The names stored in every document's head set, and the head keys of the
/// earlier layout still stored (none).
async fn stored(db: &db::DB<RegolithStore>) -> (usize, usize) {
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    let (mut names, mut legacy) = (0, 0);
    let mut sets = view
        .iterator(IterOptions::new().with_prefix(b"/hd/".to_vec()))
        .await
        .unwrap();
    while let Some(pair) = sets.next().await.unwrap() {
        let set = storage::merge::head_set::Set::read(&pair.value).unwrap();
        names += set.live().count() + set.ahead().count();
    }
    sets.close().await.unwrap();
    for prefix in [b"/d/".as_slice(), b"/ds/"] {
        let mut iter = view
            .iterator(IterOptions::new().with_prefix(prefix.to_vec()))
            .await
            .unwrap();
        while iter.next().await.unwrap().is_some() {
            legacy += 1;
        }
        iter.close().await.unwrap();
    }
    (names, legacy)
}

/// Autocommit and explicit-transaction updates both keep a document's set to
/// its live heads, however many times it is written.
#[tokio::test]
async fn every_write_path_keeps_a_documents_stored_heads_to_its_live_heads() {
    use query::mutator::DocMutator;
    const WRITES: i64 = 24;
    let db = std::sync::Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    db.create_collection(schema::CollectionVersion::new(
        "Rewritten",
        "rewritten-v1",
        "col-rewritten",
        vec![
            schema::FieldDescription::new("1", "_docID", schema::FieldKind::doc_id()),
            schema::FieldDescription::new("2", "value", schema::FieldKind::int()),
        ],
    ))
    .await
    .unwrap();
    let autocommit = db::AutoCommitMutator::new(db.clone());
    let doc_id = autocommit
        .create(
            "Rewritten",
            document::Document::from_json_str(r#"{"value": 0}"#).unwrap(),
        )
        .await
        .unwrap()
        .doc_id;
    let modified = || rapidhash::RapidHashSet::from_iter(["value".to_string()]);
    for value in 1..=WRITES {
        let mut doc = autocommit
            .get_for_update("Rewritten", &doc_id)
            .await
            .unwrap()
            .unwrap();
        doc.set("value", document::NormalValue::Int(value));
        autocommit
            .update("Rewritten", doc, modified())
            .await
            .unwrap();
    }
    // One composite head and one head for the field.
    assert_eq!(stored(&db).await, (2, 0));

    for value in WRITES + 1..=2 * WRITES {
        let explicit = db::DbDocMutator::new(db.clone(), db.new_txn(false).await.unwrap());
        let mut doc = explicit
            .get_for_update("Rewritten", &doc_id)
            .await
            .unwrap()
            .unwrap();
        doc.set("value", document::NormalValue::Int(value));
        explicit.update("Rewritten", doc, modified()).await.unwrap();
        explicit.take_txn().await.unwrap().commit().await.unwrap();
    }
    assert_eq!(stored(&db).await, (2, 0));
}

/// Two updates of one document, each of a different field, both commit: their
/// head set writes are blind merges, and the live set is both new composite
/// heads and both new field heads.
#[tokio::test]
async fn concurrent_updates_of_one_document_both_commit() {
    use query::mutator::DocMutator;
    let db = std::sync::Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    db.create_collection(schema::CollectionVersion::new(
        "Pair",
        "pair-v1",
        "col-pair",
        vec![
            schema::FieldDescription::new("1", "_docID", schema::FieldKind::doc_id()),
            schema::FieldDescription::new("2", "left", schema::FieldKind::int()),
            schema::FieldDescription::new("3", "right", schema::FieldKind::int()),
        ],
    ))
    .await
    .unwrap();
    let doc_id = db::AutoCommitMutator::new(db.clone())
        .create(
            "Pair",
            document::Document::from_json_str(r#"{"left": 0, "right": 0}"#).unwrap(),
        )
        .await
        .unwrap()
        .doc_id;
    // Read outside the writers: `get_for_update` validates every field, which
    // would make the writers conflict on the document value, not on the heads.
    let reader = db::DbDocMutator::new(db.clone(), db.new_txn(true).await.unwrap());
    let current = reader
        .get_for_update("Pair", &doc_id)
        .await
        .unwrap()
        .unwrap();
    let mut writers = Vec::new();
    for (field, value) in [("left", 1), ("right", 2)] {
        let writer = db::DbDocMutator::new(db.clone(), db.new_txn(false).await.unwrap());
        let mut doc = current.clone();
        doc.set(field, document::NormalValue::Int(value));
        writer
            .update(
                "Pair",
                doc,
                rapidhash::RapidHashSet::from_iter([field.to_string()]),
            )
            .await
            .unwrap();
        writers.push(writer);
    }
    for writer in writers {
        writer.take_txn().await.unwrap().commit().await.unwrap();
    }
    // Two composite heads, and one new head for each updated field.
    assert_eq!(stored(&db).await, (4, 0));
}

/// Holding a parent ahead is a decision that the parent is unpublished: it
/// reads the parent's guard validated, so it cannot commit once the parent was
/// published underneath it. The retry then supersedes the published parent, and
/// no name is left ahead.
#[tokio::test]
async fn holding_a_parent_ahead_conflicts_with_its_publication() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, child) = (cid(b"racing parent"), cid(b"racing child"));
    let child_txn = db.new_txn(false).await.unwrap();
    publish_head(&child_txn, child, 2, &[parent]).await;
    let parent_txn = db.new_txn(false).await.unwrap();
    publish_head(&parent_txn, parent, 1, &[]).await;
    parent_txn.commit().await.unwrap();
    assert!(child_txn.commit().await.is_err());

    let retry = db.new_txn(false).await.unwrap();
    publish_head(&retry, child, 2, &[parent]).await;
    retry.commit().await.unwrap();
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    assert_eq!(live(&view).await, vec![child]);
    assert_eq!(ahead(&view).await, 0);
}
