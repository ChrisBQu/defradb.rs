use cid::Cid;
use db::block::heads::document::{iterator, marker_prefix, publish, supersede};
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
    let outcome = db.prune_document_heads(DOC).await.unwrap();
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
    assert_eq!(
        db.prune_document_heads(DOC).await.unwrap(),
        Default::default()
    );
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
    assert_eq!(db.prune_document_heads(DOC).await.unwrap().heads_removed, 1);
    let txn = db.new_txn(true).await.unwrap();
    let view = txn.headstore().unwrap();
    assert_eq!(live(&view).await, vec![child]);
    let mut iter = view
        .iterator(
            IterOptions::new().with_prefix(marker_prefix(&HeadstoreDocKey::document_prefix(DOC))),
        )
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
    assert_eq!(db.prune_document_heads(DOC).await.unwrap().heads_removed, 1);
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
