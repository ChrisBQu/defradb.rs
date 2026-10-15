use crate::common::schema::test_collection;
use async_trait::async_trait;
use db::{DbCollectionProvider, LensedAutoCommitFetcher, DB};
use events::{Bus, ChannelBus, EventName};
use query::{DocFetcher, QueryExecutor, QueryRequest};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use storage::corekv::{RetryAttempt, RetryAttemptError, RetryAttemptFn, RetryAttemptFuture};
use storage::{RegolithStore, Result, Store, Txn};

#[derive(Clone)]
struct ConflictStore {
    inner: RegolithStore,
    attempts: Arc<AtomicUsize>,
    raced: Arc<AtomicBool>,
    attempt_ids: Arc<Mutex<Vec<Vec<String>>>>,
}

impl storage::corekv::private::Sealed for ConflictStore {}

#[async_trait]
impl Store for ConflictStore {
    async fn allocate(&self, key: &[u8], count: u64) -> Result<std::ops::Range<u64>> {
        self.inner.allocate(key, count).await
    }
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        self.inner.new_txn(readonly).await
    }
    async fn transact_retry(
        &self,
        options: storage::corekv::RetryOptions,
        attempt: RetryAttempt,
    ) -> std::result::Result<storage::corekv::RetryAttemptValue, RetryAttemptError> {
        struct InjectConflict {
            store: RegolithStore,
            attempts: Arc<AtomicUsize>,
            raced: Arc<AtomicBool>,
            attempt_ids: Arc<Mutex<Vec<Vec<String>>>>,
            inner: RetryAttempt,
        }
        impl RetryAttemptFn for InjectConflict {
            fn run<'a>(
                &'a mut self,
                txn: &'a mut dyn storage::corekv::RetryTxn,
            ) -> RetryAttemptFuture<'a> {
                Box::pin(async move {
                    if !self.raced.swap(true, Ordering::SeqCst) {
                        txn.get(b"batch-conflict").await?;
                        txn.set(b"batch-conflict", b"loser").await?;
                        let mut winner = self.store.new_txn(false).await?;
                        winner.set(b"batch-conflict", b"winner").await?;
                        winner.commit().await?;
                    }
                    let result = self.inner.run(txn).await?;
                    self.attempts.fetch_add(1, Ordering::SeqCst);
                    if let Some(value) = result.downcast_ref::<serde_json::Value>() {
                        let mut ids = Vec::new();
                        collect_doc_ids(value, &mut ids);
                        self.attempt_ids.lock().unwrap().push(ids);
                    }
                    Ok(result)
                })
            }
        }
        self.inner
            .transact_retry(
                options,
                Box::new(InjectConflict {
                    store: self.inner.clone(),
                    attempts: self.attempts.clone(),
                    raced: self.raced.clone(),
                    attempt_ids: self.attempt_ids.clone(),
                    inner: attempt,
                }),
            )
            .await
    }
    async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

fn collect_doc_ids(value: &serde_json::Value, ids: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(id) = map.get("_docID").and_then(serde_json::Value::as_str) {
                ids.push(id.to_owned());
            }
            for child in map.values() {
                collect_doc_ids(child, ids);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_doc_ids(child, ids);
            }
        }
        _ => {}
    }
}

#[tokio::test]
async fn implicit_batch_replays_atomically_inside_engine_attempt() {
    let inner = RegolithStore::in_memory().unwrap();
    let store = ConflictStore {
        inner: inner.clone(),
        attempts: Arc::new(AtomicUsize::new(0)),
        raced: Arc::new(AtomicBool::new(false)),
        attempt_ids: Arc::new(Mutex::new(Vec::new())),
    };
    let bus: Arc<dyn Bus> = Arc::new(ChannelBus::new());
    let mut db = DB::new(store.clone()).unwrap();
    db.set_event_bus(bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_collection()).await.unwrap();

    let runner = query::QueryRunner::with_provider(
        LensedAutoCommitFetcher::new(db.clone()),
        DbCollectionProvider::new_arc(db.clone()),
    )
    .with_mutator(Arc::new(db::write::autocommit::AutoCommitMutator::new(
        db.clone(),
    )));
    let request = QueryRequest::new(
        r#"mutation {
            first: add_TestDoc(input: {x: 1}) { _docID }
            second: add_TestDoc(input: {x: 2}) { _docID }
        }"#,
    );
    let prepared = runner.prepare_request(&request).await.unwrap();
    assert!(
        prepared.is_some(),
        "a top-level batch must be request-prepared"
    );
    let mut events = bus.subscribe(&[EventName::Update]);
    let response = runner.execute_prepared(request, prepared).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(store.attempts.load(Ordering::SeqCst), 2);
    let attempt_ids = store.attempt_ids.lock().unwrap();
    assert_eq!(attempt_ids.len(), 2);
    assert_eq!(attempt_ids[0], attempt_ids[1]);
    let mut returned = attempt_ids[1].clone();
    assert_eq!(returned.len(), 2);
    drop(attempt_ids);
    let mut doc_events = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .expect("winner event arrives")
            .expect("subscription remains open");
        doc_events.push(event.as_update().unwrap().doc_id.clone());
    }
    doc_events.sort();
    returned.sort();
    assert_eq!(doc_events, returned);
    assert!(
        events.try_recv().is_err(),
        "losing attempt must publish no events"
    );

    let docs = LensedAutoCommitFetcher::new(db)
        .get_all("TestDoc")
        .await
        .unwrap();
    assert_eq!(docs.len(), 2, "the whole batch commits atomically");
}
