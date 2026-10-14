use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use storage::corekv::{Error, Store, Writer};
use storage::{RegolithStore, RegolithStoreOptions};

#[derive(Default)]
struct CallerListener(AtomicU64);

impl regolith::EventListener for CallerListener {
    fn on_conflict(&self, _: &regolith::Conflict) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn engine_counters_and_conflict_reasons_preserve_caller_instrumentation() {
    let engine = Arc::new(regolith::Statistics::default());
    let listener = Arc::new(CallerListener::default());
    let mut options = RegolithStoreOptions::memory();
    options.engine = options
        .engine
        .statistics(Some(engine.clone()))
        .add_listener(listener.clone());
    let store = RegolithStore::open_with_options("telemetry", options).unwrap();
    let stats = store.transaction_stats_handle().unwrap();
    let mut seed = store.new_txn(false).await.unwrap();
    seed.set(b"ordinary", b"before").await.unwrap();
    seed.commit().await.unwrap();
    let mut reader = store.new_txn(false).await.unwrap();
    reader.get(b"ordinary").await.unwrap();
    reader.set(b"effect", b"yes").await.unwrap();
    let mut delete = store.new_txn(false).await.unwrap();
    delete.delete(b"ordinary").await.unwrap();
    delete.commit().await.unwrap();
    assert!(matches!(reader.commit().await, Err(Error::TxnConflict)));

    let snapshot = stats.snapshot();
    assert_eq!(
        snapshot.commits,
        engine.get_ticker(regolith::Ticker::CommitCount)
    );
    assert_eq!(snapshot.conflicts, 1);
    assert_eq!(snapshot.conflict_reasons.get("read/delete"), Some(&1));
    assert_eq!(snapshot.engine["regolith.commit.conflicts_after_delete"], 1);
    assert_eq!(listener.0.load(Ordering::Relaxed), 1);
    assert_eq!(snapshot.engine.len(), 8);

    // A read-only adapter transaction owns a snapshot, not a native commit.
    store.new_txn(true).await.unwrap().commit().await.unwrap();
    assert_eq!(stats.snapshot().commits, snapshot.commits);
    let other = RegolithStore::in_memory().unwrap();
    assert_eq!(
        other
            .transaction_stats_handle()
            .unwrap()
            .snapshot()
            .conflicts,
        0
    );
}

#[tokio::test]
async fn accepted_blind_merges_are_visible_in_policy_tickers() {
    let store = RegolithStore::in_memory().unwrap();
    let mut seed = store.new_txn(false).await.unwrap();
    seed.set(b"counter", &0_i64.to_be_bytes()).await.unwrap();
    seed.commit().await.unwrap();
    let mut first = store.new_txn(false).await.unwrap();
    let mut second = store.new_txn(false).await.unwrap();
    let delta = storage::merge::counter_i64(1);
    Writer::merge(&mut first, b"counter", &delta).await.unwrap();
    Writer::merge(&mut second, b"counter", &delta)
        .await
        .unwrap();
    first.commit().await.unwrap();
    second.commit().await.unwrap();
    let stats = store.transaction_stats_handle().unwrap().snapshot();
    assert_eq!(stats.conflicts, 0);
    assert_eq!(stats.engine["regolith.policy.blind_merges_commuted"], 1);
}
