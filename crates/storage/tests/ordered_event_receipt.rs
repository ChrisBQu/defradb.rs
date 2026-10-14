use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use storage::backends::regolith::RegolithStore;
use storage::corekv::{Store, Writer};

#[tokio::test]
async fn eventful_empty_commit_is_rejected_instead_of_reusing_old_receipt() {
    let store = RegolithStore::in_memory().expect("store");
    let mut first = store.new_txn(false).await.expect("first txn");
    first.set(b"key", b"value").await.expect("write");
    first.commit().await.expect("first commit");

    let published = Arc::new(AtomicUsize::new(0));
    let mut empty = store.new_txn(false).await.expect("empty txn");
    let published_by_callback = Arc::clone(&published);
    empty
        .on_success_ordered(Arc::new(move || {
            published_by_callback.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("register event");

    assert!(empty.commit().await.is_err());
    assert_eq!(published.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn accepted_identical_blind_write_keeps_its_event_callback() {
    let store = RegolithStore::in_memory().expect("store");
    let mut seed = store.new_txn(false).await.expect("seed txn");
    seed.set(b"key", b"initial").await.expect("seed write");
    seed.commit().await.expect("seed commit");

    let mut delayed = store.new_txn(false).await.expect("delayed txn");
    delayed.set(b"key", b"same").await.expect("delayed write");
    let published = Arc::new(AtomicUsize::new(0));
    let published_by_callback = Arc::clone(&published);
    delayed
        .on_success_ordered(Arc::new(move || {
            published_by_callback.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("register event");

    let mut concurrent = store.new_txn(false).await.expect("concurrent txn");
    concurrent
        .set(b"key", b"same")
        .await
        .expect("concurrent write");
    concurrent.commit().await.expect("concurrent commit");

    delayed
        .commit()
        .await
        .expect("identical blind write is accepted");
    assert_eq!(published.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_ordered_hook_suppresses_events_and_releases_later_commits() {
    let store = RegolithStore::in_memory().expect("store");
    let published = Arc::new(AtomicUsize::new(0));

    let mut first = store.new_txn(false).await.expect("first txn");
    first.set(b"first", b"value").await.expect("first write");
    let published_by_first = Arc::clone(&published);
    first
        .on_success_ordered(Arc::new(move || {
            published_by_first.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("first event");
    first
        .on_success_ordered_async(Arc::new(|| {
            Box::pin(async { Err("external postcommit hook failed".to_string()) })
        }))
        .expect("failing hook");
    assert!(first.commit().await.is_err());
    assert_eq!(published.load(Ordering::SeqCst), 0);

    let mut second = store.new_txn(false).await.expect("second txn");
    second.set(b"second", b"value").await.expect("second write");
    let published_by_second = Arc::clone(&published);
    second
        .on_success_ordered(Arc::new(move || {
            published_by_second.fetch_add(1, Ordering::SeqCst);
        }))
        .expect("second event");
    second.commit().await.expect("second commit");
    assert_eq!(published.load(Ordering::SeqCst), 1);
}
