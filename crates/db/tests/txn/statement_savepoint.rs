use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use db::{DbCollectionProvider, DbTransactionRegistry, LensedAutoCommitFetcher, DB};
use events::{Bus, ChannelBus, EventName};
use query::{QueryExecutor, QueryRequest};
use storage::corekv::{
    private::Sealed, AppendLog, AsyncTxnCallback, IterOptions, Iterator, Reader,
    Result as StorageResult, Store, Txn, TxnCallback, Writer,
};
use storage::RegolithStore;

#[derive(Default)]
struct WriteGate {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    block_keys: Mutex<Vec<Vec<u8>>>,
}

struct PauseAfterSetStore {
    inner: RegolithStore,
    gate: Arc<WriteGate>,
}

impl Sealed for PauseAfterSetStore {}

#[async_trait]
impl Store for PauseAfterSetStore {
    async fn allocate(&self, key: &[u8], count: u64) -> StorageResult<std::ops::Range<u64>> {
        self.inner.allocate(key, count).await
    }

    async fn new_txn(&self, readonly: bool) -> StorageResult<Box<dyn Txn>> {
        Ok(Box::new(PauseAfterSetTxn {
            inner: self.inner.new_txn(readonly).await?,
            gate: Arc::clone(&self.gate),
        }))
    }

    async fn transact_retry(
        &self,
        options: storage::corekv::RetryOptions,
        attempt: storage::corekv::RetryAttempt,
    ) -> std::result::Result<storage::corekv::RetryAttemptValue, storage::corekv::RetryAttemptError>
    {
        self.inner.transact_retry(options, attempt).await
    }

    async fn close(&self) -> StorageResult<()> {
        self.inner.close().await
    }
}

struct PauseAfterSetTxn {
    inner: Box<dyn Txn>,
    gate: Arc<WriteGate>,
}

impl Sealed for PauseAfterSetTxn {}

#[async_trait]
impl Reader for PauseAfterSetTxn {
    async fn get(&self, key: &[u8]) -> StorageResult<Option<bytes::Bytes>> {
        self.inner.get(key).await
    }

    async fn get_parts(&self, key: &[u8], parts: &[u32]) -> StorageResult<Option<bytes::Bytes>> {
        self.inner.get_parts(key, parts).await
    }

    async fn has(&self, key: &[u8]) -> StorageResult<bool> {
        self.inner.has(key).await
    }

    async fn get_size(&self, key: &[u8]) -> StorageResult<Option<usize>> {
        self.inner.get_size(key).await
    }

    async fn iterator(&self, opts: IterOptions) -> StorageResult<Box<dyn Iterator>> {
        self.inner.iterator(opts).await
    }
}

#[async_trait]
impl Writer for PauseAfterSetTxn {
    async fn merge(&mut self, key: &[u8], operand: &[u8]) -> StorageResult<()> {
        self.inner.merge(key, operand).await
    }

    async fn append(
        &mut self,
        log: &AppendLog,
        entry: &[u8],
        once_key: Option<&[u8]>,
    ) -> StorageResult<()> {
        self.inner.append(log, entry, once_key).await
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> StorageResult<()> {
        self.inner.set(key, value).await?;
        if key.first() == Some(&storage::namespace::Namespace::Blockstore.prefix())
            && cid::Cid::try_from(&key[1..]).is_ok()
        {
            self.gate.block_keys.lock().unwrap().push(key.to_vec());
        }
        if self.gate.armed.swap(false, Ordering::SeqCst) {
            self.gate.entered.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> StorageResult<()> {
        self.inner.delete(key).await
    }
}

#[async_trait]
impl Txn for PauseAfterSetTxn {
    fn set_savepoint(&mut self) -> StorageResult<()> {
        self.inner.set_savepoint()
    }

    fn rollback_to_savepoint(&mut self) -> StorageResult<()> {
        self.inner.rollback_to_savepoint()
    }

    fn release_savepoint(&mut self) -> StorageResult<()> {
        self.inner.release_savepoint()
    }

    async fn commit(self: Box<Self>) -> StorageResult<()> {
        self.inner.commit().await
    }

    fn discard(self: Box<Self>) {
        self.inner.discard()
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.inner.on_success(callback)
    }

    fn on_success_ordered(
        &mut self,
        callback: storage::corekv::OrderedTxnCallback,
    ) -> StorageResult<()> {
        self.inner.on_success_ordered(callback)
    }

    fn on_success_ordered_async(
        &mut self,
        callback: storage::corekv::AsyncOrderedTxnCallback,
    ) -> StorageResult<()> {
        self.inner.on_success_ordered_async(callback)
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_success_async(callback)
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.inner.on_error(callback)
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_error_async(callback)
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.inner.on_discard(callback)
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.inner.on_discard_async(callback)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.inner.is_readonly()
    }

    fn callback_count(&self) -> usize {
        self.inner.callback_count()
    }
}

#[tokio::test]
async fn cancelling_a_statement_rolls_back_only_that_statement_before_commit() {
    let inner = RegolithStore::in_memory().unwrap();
    let gate = Arc::new(WriteGate::default());
    let db = Arc::new(
        DB::new(PauseAfterSetStore {
            inner,
            gate: Arc::clone(&gate),
        })
        .unwrap(),
    );
    for schema in query::parse_sdl("type Users { name: String }").unwrap() {
        db.create_collection(schema).await.unwrap();
    }
    let registry = Arc::new(DbTransactionRegistry::new(Arc::clone(&db)));
    let runner = Arc::new(query::QueryRunner::with_arc_registry_and_provider(
        LensedAutoCommitFetcher::new(Arc::clone(&db)),
        DbCollectionProvider::new_arc(Arc::clone(&db)),
        Arc::clone(&registry),
    ));

    let handle = runner.begin_txn(false).await.unwrap();
    let first = runner
        .execute_in_txn(
            QueryRequest::new(r#"mutation { add_Users(input: {name: "kept"}) { _docID } }"#),
            &handle,
        )
        .await;
    assert!(!first.has_errors(), "{:?}", first.errors);

    gate.armed.store(true, Ordering::SeqCst);
    let runner_for_task = Arc::clone(&runner);
    let handle_for_task = handle.clone();
    let mutation = tokio::spawn(async move {
        runner_for_task
            .execute_in_txn(
                QueryRequest::new(
                    r#"mutation { add_Users(input: {name: "cancelled"}) { _docID } }"#,
                ),
                &handle_for_task,
            )
            .await
    });
    gate.entered.notified().await;
    mutation.abort();
    assert!(mutation.await.unwrap_err().is_cancelled());

    runner.commit_txn(&handle).await.unwrap();
    let response = runner
        .execute(QueryRequest::new("{ Users { name } }"))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(
        response.data.unwrap()["Users"],
        serde_json::json!([{ "name": "kept" }])
    );
}

#[tokio::test]
async fn timing_out_a_statement_rolls_back_only_that_statement_before_commit() {
    let bus: Arc<dyn Bus> = Arc::new(ChannelBus::new());
    let inner = RegolithStore::in_memory().unwrap();
    let gate = Arc::new(WriteGate::default());
    let mut db = DB::new(PauseAfterSetStore {
        inner,
        gate: Arc::clone(&gate),
    })
    .unwrap();
    db.set_event_bus(bus.clone());
    let db = Arc::new(db);
    for schema in query::parse_sdl("type Users { name: String }").unwrap() {
        db.create_collection(schema).await.unwrap();
    }
    let registry = Arc::new(DbTransactionRegistry::new(Arc::clone(&db)));
    let runner = Arc::new(
        query::QueryRunner::with_arc_registry_and_provider(
            LensedAutoCommitFetcher::new(Arc::clone(&db)),
            DbCollectionProvider::new_arc(Arc::clone(&db)),
            Arc::clone(&registry),
        )
        .with_query_timeout(1),
    );

    let handle = runner.begin_txn(false).await.unwrap();
    let mut events = bus.subscribe(&[EventName::Update]);
    let prior = runner
        .execute_in_txn(
            QueryRequest::new(r#"mutation { add_Users(input: {name: "kept"}) { _docID } }"#),
            &handle,
        )
        .await;
    assert!(!prior.has_errors(), "{:?}", prior.errors);

    gate.armed.store(true, Ordering::SeqCst);
    let runner_for_task = Arc::clone(&runner);
    let handle_for_task = handle.clone();
    let mutation = tokio::spawn(async move {
        runner_for_task
            .execute_in_txn(
                QueryRequest::new(
                    r#"mutation { add_Users(input: {name: "timed out"}) { _docID } }"#,
                ),
                &handle_for_task,
            )
            .await
    });
    gate.entered.notified().await;
    let timed_out = mutation.await.unwrap();
    assert!(timed_out.has_errors(), "expected query timeout");
    assert!(timed_out.errors[0].message.contains("timed out"));

    runner.commit_txn(&handle).await.unwrap();
    let response = runner
        .execute(QueryRequest::new("{ Users { name } }"))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(
        response.data.unwrap()["Users"],
        serde_json::json!([{ "name": "kept" }])
    );
    tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
        .await
        .expect("prior statement event remains queued")
        .expect("event bus stays open");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), events.recv())
            .await
            .is_err(),
        "timed-out statement must not publish a buffered document event"
    );
}

#[tokio::test]
async fn failed_partial_batch_keeps_prior_statements_and_discards_its_own_writes() {
    let bus: Arc<dyn Bus> = Arc::new(ChannelBus::new());
    let inner = RegolithStore::in_memory().unwrap();
    let gate = Arc::new(WriteGate::default());
    let mut db = DB::new(PauseAfterSetStore {
        inner: inner.clone(),
        gate: Arc::clone(&gate),
    })
    .unwrap();
    db.set_event_bus(bus.clone());
    let db = Arc::new(db);
    for schema in query::parse_sdl("type Users { email: String @index(unique: true) }").unwrap() {
        db.create_collection(schema).await.unwrap();
    }
    let after_schema = gate.block_keys.lock().unwrap().len();
    let registry = Arc::new(DbTransactionRegistry::new(Arc::clone(&db)));
    let runner = Arc::new(query::QueryRunner::with_arc_registry_and_provider(
        LensedAutoCommitFetcher::new(Arc::clone(&db)),
        DbCollectionProvider::new_arc(Arc::clone(&db)),
        Arc::clone(&registry),
    ));

    let handle = runner.begin_txn(false).await.unwrap();
    let mut events = bus.subscribe(&[EventName::Update]);
    let prior = runner
        .execute_in_txn(
            QueryRequest::new(r#"mutation { add_Users(input: {email: "prior"}) { _docID } }"#),
            &handle,
        )
        .await;
    assert!(!prior.has_errors(), "{:?}", prior.errors);
    let prior_block_keys = gate.block_keys.lock().unwrap()[after_schema..].to_vec();
    assert!(
        !prior_block_keys.is_empty(),
        "the prior successful statement must write blobstore blocks"
    );

    let failed_statement_start = gate.block_keys.lock().unwrap().len();
    let failed = runner
        .execute_in_txn(
            QueryRequest::new(
                r#"mutation {
                    first: add_Users(input: {email: "duplicate"}) { _docID }
                    second: add_Users(input: {email: "duplicate"}) { _docID }
                }"#,
            ),
            &handle,
        )
        .await;
    assert!(
        failed.has_errors(),
        "expected the second unique insert to fail"
    );
    let prior_key_set: HashSet<_> = prior_block_keys.iter().cloned().collect();
    let failed_block_keys: HashSet<_> = {
        let recorded = gate.block_keys.lock().unwrap();
        recorded[failed_statement_start..]
            .iter()
            .filter(|key| !prior_key_set.contains(*key))
            .cloned()
            .collect()
    };
    assert!(
        !failed_block_keys.is_empty(),
        "the failed statement must have written distinct blobstore blocks before its unique-index error"
    );
    runner.commit_txn(&handle).await.unwrap();
    let inspect = inner.new_txn(true).await.unwrap();
    for key in &prior_block_keys {
        assert!(
            inspect.has(key).await.unwrap(),
            "blob from the prior successful statement must survive commit"
        );
    }
    for key in &failed_block_keys {
        assert!(
            !inspect.has(key).await.unwrap(),
            "blob written by the failed statement must be absent after commit"
        );
    }
    inspect.discard();
    let response = runner
        .execute(QueryRequest::new("{ Users { email } }"))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(
        response.data.unwrap()["Users"],
        serde_json::json!([{ "email": "prior" }])
    );
    tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
        .await
        .expect("prior statement event remains queued")
        .expect("event bus stays open");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), events.recv())
            .await
            .is_err(),
        "failed statement must not publish a buffered document event"
    );
}
