use cid::Cid;
use db::block::heads::document::{document_markers, iterator, publish, reclaim, supersede};
use db::block::heads::PruneOutcome;
use storage::corekv::{IterOptions, Key};
use storage::keys::headstore::HeadstoreDocKey;
use storage::RegolithStore;

const DOC: u64 = 47; // The binary short ID contains a slash.
fn cid(value: &[u8]) -> Cid {
    defra_core::block::generate_cid_from_bytes(value).unwrap()
}
fn key(cid: Cid) -> Vec<u8> {
    HeadstoreDocKey::new(DOC, "name", cid).bytes()
}
/// Run one reclamation in a transaction of its own.
async fn prune(db: &db::DB<RegolithStore>) -> PruneOutcome {
    let txn = db.new_txn(false).await.unwrap();
    let outcome = reclaim(&txn.headstore().unwrap(), DOC).await.unwrap();
    txn.commit().await.unwrap();
    outcome
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

#[tokio::test]
async fn overlapping_document_heads_commit_and_pruning_preserves_snapshots() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, left, right) = (cid(b"parent"), cid(b"left"), cid(b"right"));
    let seed = db.new_txn(false).await.unwrap();
    seed.headstore()
        .unwrap()
        .set(&key(parent), &[1])
        .await
        .unwrap();
    seed.commit().await.unwrap();
    let old = db.new_txn(true).await.unwrap();
    let a = db.new_txn(false).await.unwrap();
    let b = db.new_txn(false).await.unwrap();
    for (txn, child) in [(&a, left), (&b, right)] {
        let view = txn.headstore().unwrap();
        assert_eq!(live(&view).await, vec![parent]);
        supersede(&view, &key(parent), child).await.unwrap();
        view.set(&key(child), &[2]).await.unwrap();
    }
    b.commit().await.unwrap();
    a.commit().await.unwrap();
    let before_prune = db.new_txn(true).await.unwrap();
    let mut expected = vec![left, right];
    expected.sort_by_cached_key(Cid::to_string);
    assert_eq!(live(&before_prune.headstore().unwrap()).await, expected);
    let outcome = prune(&db).await;
    assert_eq!((outcome.heads_removed, outcome.markers_removed), (1, 2));
    assert_eq!(live(&old.headstore().unwrap()).await, vec![parent]);
    assert_eq!(live(&before_prune.headstore().unwrap()).await, expected);
    let after = db.new_txn(true).await.unwrap();
    assert_eq!(live(&after.headstore().unwrap()).await, expected);
    assert!(after
        .headstore()
        .unwrap()
        .get(&key(parent))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn marker_before_parent_survives_prune_and_hides_late_parent() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, child) = (cid(b"late parent"), cid(b"early child"));
    let txn = db.new_txn(false).await.unwrap();
    {
        let view = txn.headstore().unwrap();
        supersede(&view, &key(parent), child).await.unwrap();
        view.set(&key(child), &[2]).await.unwrap();
    }
    txn.commit().await.unwrap();
    assert_eq!(prune(&db).await, Default::default());
    let txn = db.new_txn(false).await.unwrap();
    txn.headstore()
        .unwrap()
        .set(&key(parent), &[1])
        .await
        .unwrap();
    txn.commit().await.unwrap();
    let txn = db.new_txn(true).await.unwrap();
    assert_eq!(live(&txn.headstore().unwrap()).await, vec![child]);
    txn.discard().unwrap();
    assert_eq!(prune(&db).await.heads_removed, 1);
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    assert_eq!(live(&view).await, vec![child]);
    let mut iter = view
        .iterator(IterOptions::new().with_prefix(document_markers(DOC)))
        .await
        .unwrap();
    assert!(iter.next().await.unwrap().is_none());
    iter.close().await.unwrap();
}

#[tokio::test]
async fn replay_after_pruning_cannot_resurrect_a_published_parent() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, child) = (cid(b"published parent"), cid(b"published child"));
    let txn = db.new_txn(false).await.unwrap();
    {
        let view = txn.headstore().unwrap();
        publish(&view, &HeadstoreDocKey::new(DOC, "name", parent), &[1], &[])
            .await
            .unwrap();
        publish(
            &view,
            &HeadstoreDocKey::new(DOC, "name", child),
            &[2],
            &[parent],
        )
        .await
        .unwrap();
    }
    txn.commit().await.unwrap();
    // A replay may already own an older snapshot when reclamation commits.
    let replay = db.new_txn(false).await.unwrap();
    assert_eq!(prune(&db).await.heads_removed, 1);
    publish(
        &replay.headstore().unwrap(),
        &HeadstoreDocKey::new(DOC, "name", parent),
        &[1],
        &[],
    )
    .await
    .unwrap();
    replay.commit().await.unwrap();
    let replay = db.new_txn(false).await.unwrap();
    publish(
        &replay.headstore().unwrap(),
        &HeadstoreDocKey::new(DOC, "name", parent),
        &[1],
        &[],
    )
    .await
    .unwrap();
    replay.commit().await.unwrap();
    let txn = db.new_txn(true).await.unwrap();
    assert_eq!(live(&txn.headstore().unwrap()).await, vec![child]);
    assert!(txn
        .headstore()
        .unwrap()
        .get(&key(parent))
        .await
        .unwrap()
        .is_none());
}

/// Two writers that reclaim the same superseded group in their own
/// transactions both commit: the deletes are blind, and a delete of a key a
/// newer delete removed is no change.
#[tokio::test]
async fn concurrent_reclamations_of_one_group_both_commit() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, child) = (cid(b"reclaimed parent"), cid(b"current child"));
    let seed = db.new_txn(false).await.unwrap();
    {
        let view = seed.headstore().unwrap();
        view.set(&key(parent), &[1]).await.unwrap();
        supersede(&view, &key(parent), child).await.unwrap();
        view.set(&key(child), &[2]).await.unwrap();
    }
    seed.commit().await.unwrap();

    let first = db.new_txn(false).await.unwrap();
    let second = db.new_txn(false).await.unwrap();
    for txn in [&first, &second] {
        let outcome = reclaim(&txn.headstore().unwrap(), DOC).await.unwrap();
        assert_eq!((outcome.heads_removed, outcome.markers_removed), (1, 1));
    }
    first.commit().await.unwrap();
    second
        .commit()
        .await
        .expect("a second reclamation of the same group must not conflict");
    let txn = db.new_txn(true).await.unwrap();
    assert_eq!(live(&txn.headstore().unwrap()).await, vec![child]);
}

/// A writer that superseded a published parent concurrently with the write
/// that reclaimed it leaves a marker whose parent has no head key. The parent's
/// publication guard says it was reclaimed, so the next reclamation removes
/// the marker; the head set never changes.
#[tokio::test]
async fn a_late_marker_of_a_reclaimed_parent_is_reclaimed() {
    let db = db::DB::new(RegolithStore::in_memory().unwrap()).unwrap();
    let (parent, left, right) = (
        cid(b"shared parent"),
        cid(b"left child"),
        cid(b"right child"),
    );
    let head = |cid| HeadstoreDocKey::new(DOC, "name", cid);
    let seed = db.new_txn(false).await.unwrap();
    publish(&seed.headstore().unwrap(), &head(parent), &[1], &[])
        .await
        .unwrap();
    seed.commit().await.unwrap();

    let late = db.new_txn(false).await.unwrap();
    let early = db.new_txn(false).await.unwrap();
    {
        let view = early.headstore().unwrap();
        publish(&view, &head(left), &[2], &[parent]).await.unwrap();
        assert_eq!(reclaim(&view, DOC).await.unwrap().heads_removed, 1);
    }
    early.commit().await.unwrap();
    publish(&late.headstore().unwrap(), &head(right), &[2], &[parent])
        .await
        .unwrap();
    late.commit().await.expect("a fork never conflicts");

    let mut expected = vec![left, right];
    expected.sort_by_cached_key(Cid::to_string);
    let txn = db.new_txn(true).await.unwrap();
    assert_eq!(live(&txn.headstore().unwrap()).await, expected);
    txn.discard().unwrap();

    let outcome = prune(&db).await;
    assert_eq!(outcome.orphans_removed, 1);
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    assert_eq!(live(&view).await, expected);
    let mut markers = view
        .iterator(IterOptions::new().with_prefix(document_markers(DOC)))
        .await
        .unwrap();
    assert!(markers.next().await.unwrap().is_none());
    markers.close().await.unwrap();
}

/// The walk over heads and markers answers the same set in either direction
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
            for (index, head) in heads.iter().enumerate() {
                view.set(&head.bytes(), &[1]).await.unwrap();
                if pattern & (1 << index) != 0 {
                    supersede(&view, &head.bytes(), cid(b"superseder"))
                        .await
                        .unwrap();
                }
            }
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

async fn stored(db: &db::DB<RegolithStore>, prefix: &[u8]) -> usize {
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    let mut iter = view
        .iterator(IterOptions::new().with_prefix(prefix.to_vec()))
        .await
        .unwrap();
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    iter.close().await.unwrap();
    count
}

/// Autocommit and explicit-transaction updates both reclaim in their own
/// transaction, so a document keeps exactly its live heads stored, however
/// many times it is written.
#[tokio::test]
async fn every_write_path_keeps_a_documents_stored_heads_to_its_live_heads() {
    use query::mutator::DocMutator;
    const WRITES: i64 = 24;
    let db = std::sync::Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    db.create_collection(
        &defra_core::RequestContext::default(),
        schema::CollectionVersion::new(
            "Rewritten",
            "rewritten-v1",
            "col-rewritten",
            vec![
                schema::FieldDescription::new("1", "_docID", schema::FieldKind::doc_id()),
                schema::FieldDescription::new("2", "value", schema::FieldKind::int()),
            ],
        ),
    )
    .await
    .unwrap();
    let autocommit = db::AutoCommitMutator::new(db.clone());
    let doc_id = autocommit
        .create(
            &defra_core::RequestContext::default(),
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
            .update(
                &defra_core::RequestContext::default(),
                "Rewritten",
                doc,
                modified(),
            )
            .await
            .unwrap();
    }
    // One composite head and one head for the field.
    assert_eq!(stored(&db, b"/d/").await, 2);
    assert_eq!(stored(&db, b"/ds/").await, 0);

    for value in WRITES + 1..=2 * WRITES {
        let explicit = db::DbDocMutator::new(db.clone(), db.new_txn(false).await.unwrap());
        let mut doc = explicit
            .get_for_update("Rewritten", &doc_id)
            .await
            .unwrap()
            .unwrap();
        doc.set("value", document::NormalValue::Int(value));
        explicit
            .update(
                &defra_core::RequestContext::default(),
                "Rewritten",
                doc,
                modified(),
            )
            .await
            .unwrap();
        explicit.take_txn().await.unwrap().commit().await.unwrap();
    }
    assert_eq!(stored(&db, b"/d/").await, 2);
    assert_eq!(stored(&db, b"/ds/").await, 0);
}
