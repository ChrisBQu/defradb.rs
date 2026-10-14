//! Caller key-policy contract, exercised through the actual storage adapter.
use storage::corekv::{Error, Store};
use storage::{RegolithStore, RegolithStoreOptions};

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

#[tokio::test]
async fn identical_blocks_commit_with_engine_encryption() {
    same_block_writes(RegolithStoreOptions::default().with_encryption_key([42; 32])).await;
    same_block_writes(RegolithStoreOptions::memory()).await;
}

#[tokio::test]
async fn ordinary_decision_keys_have_one_winner() {
    let store = RegolithStore::in_memory().unwrap();
    for key in [
        b"bm\x01\x55\x00\x03abc".as_slice(),
        b"h/p/applied",
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
            matches!(second.commit().await, Err(Error::TxnConflict)),
            "{key:?}"
        );
    }
}

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
    assert!(matches!(reader.commit().await, Err(Error::TxnConflict)));
}
