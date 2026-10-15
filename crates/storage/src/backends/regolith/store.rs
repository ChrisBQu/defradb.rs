//! The regolith-backed store.
//!
//! regolith validates its own transactions, so there is no conflict
//! tracker or read-set bookkeeping here: the engine decides whether a
//! transaction may land. Snapshot capture and native commit use the engine
//! directly, so independent commits can join its commit groups.

#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use regolith::{OptimisticTransactionDb, TxnOptions};

use super::background_errors::{BackgroundErrorListener, ConflictListener};
use super::config::RegolithStoreOptions;
use super::event_order::{Event, EventOrder};
use super::transaction::RegolithTxn;
use crate::backends::shared::TransactionStatsHandle;
use crate::corekv::{
    Dropable, Error, Result, RetryAttempt, RetryAttemptError, RetryAttemptIO, RetryAttemptValue,
    RetryCallbacks, RetryOptions, RetryTxn, Store, Txn,
};

/// Key-value store backed by regolith.
///
/// Cloning hands back another handle on the same database, not another
/// database: the close flag and the in-flight count are shared, so
/// closing through one handle closes it for all of them.
#[derive(Clone)]
pub struct RegolithStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    db: Arc<OptimisticTransactionDb>,
    options: RegolithStoreOptions,
    closed: AtomicBool,
    active_txns: Arc<AtomicUsize>,
    stats: TransactionStatsHandle,
    event_order: Arc<EventOrder>,
    path: PathBuf,
    /// The mounted OPFS environment, kept so `persist` can reach it.
    ///
    /// Where the browser refuses synchronous access handles the database is
    /// resident in linear memory, and only `OpfsEnv::persist` writes it back.
    /// Dropping the handle at mount would leave no way to ask.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    opfs: Option<Arc<regolith::env::opfs::OpfsEnv>>,
}

impl RegolithStore {
    /// Open at `path` with the profile for this target.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, RegolithStoreOptions::default())
    }

    /// Open at `path` with explicit options.
    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        options: RegolithStoreOptions,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    Error::Backend(format!(
                        "failed to create directory '{}': {error}",
                        parent.display()
                    ))
                })?;
            }
        }
        let engine_stats = options.engine.get_statistics().cloned().unwrap_or_default();
        let stats = TransactionStatsHandle::from_engine(engine_stats.clone());
        let engine = options
            .engine
            .clone()
            .statistics(Some(engine_stats))
            .add_listener(Arc::new(BackgroundErrorListener))
            .add_listener(Arc::new(ConflictListener(stats.clone())))
            .merge_operator(Some(Arc::new(crate::merge::DefraMergeOperator)));
        let db = OptimisticTransactionDb::open(&path, engine)
            .map_err(|error| Error::Backend(format!("failed to open regolith: {error}")))?
            .with_isolation(options.isolation)
            .with_policy(Arc::new(super::classifier::DefraKeyClassifier));
        Ok(Self {
            inner: Arc::new(StoreInner {
                db: Arc::new(db),
                options,
                closed: AtomicBool::new(false),
                active_txns: Arc::new(AtomicUsize::new(0)),
                stats,
                event_order: Arc::default(),
                path,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                opfs: None,
            }),
        })
    }

    /// An in-memory database. Nothing reaches a filesystem, so this is
    /// the store for a test or a deliberately ephemeral node.
    pub fn in_memory() -> Result<Self> {
        Self::open_with_options("regolith-memory", RegolithStoreOptions::memory())
    }

    /// Open a store persisted in the browser's origin-private filesystem.
    ///
    /// `wasm32-unknown-unknown` has no filesystem of its own, so the engine
    /// takes one: this mounts OPFS on `db_name` and installs it before
    /// opening. The mount is asynchronous because acquiring the OPFS root is,
    /// which is why this cannot be a plain `open`.
    ///
    /// The mount probes for synchronous access handles and falls back to a
    /// resident mirror when the browser refuses them, which it does outside a
    /// Worker. The fallback keeps the database in linear memory, so it is
    /// bounded rather than unlimited.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub async fn open_opfs(db_name: &str) -> Result<Self> {
        Self::open_opfs_with(db_name, false).await
    }

    /// [`open_opfs`](Self::open_opfs), optionally refusing the mirror.
    ///
    /// With `require_sync_handles`, a mount that cannot take synchronous
    /// access handles fails instead of falling back to the resident mirror.
    /// The probe cannot tell "not in a Worker" from "another Worker still
    /// holds these files", so without this a Worker that takes over a
    /// database from one that has not yet released it silently mounts an
    /// empty mirror beside the real data. Failing lets the caller retry.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub async fn open_opfs_with(db_name: &str, require_sync_handles: bool) -> Result<Self> {
        use std::sync::Arc;

        let mut mount_options = regolith::env::opfs::OpfsOptions::default();
        if require_sync_handles {
            mount_options.force_mode = Some(regolith::env::opfs::OpfsMode::Sah);
        }
        let env = Arc::new(
            regolith::env::opfs::OpfsEnv::mount(db_name, mount_options)
                .await
                .map_err(|error| Error::Backend(format!("failed to mount OPFS: {error}")))?,
        );

        let mut options = RegolithStoreOptions::wasm();
        // OPFS reports no durable sync, and the engine refuses `Immediate`
        // against an environment that cannot honour it rather than letting a
        // caller believe a synced write survived. That refusal is right: on
        // this target durability comes from `persist`, which writes the
        // database back to OPFS, not from an fsync that does nothing.
        if !env.as_env().capabilities().durable_sync {
            options.engine = options
                .engine
                .durability(regolith::DurabilityMode::Eventual);
        }
        options.engine = options.engine.env(env.as_env());
        let store = Self::open_with_options(db_name, options)?;
        // Safe to reach in: nothing else holds this store yet.
        let mut inner = Arc::try_unwrap(store.inner)
            .map_err(|_| Error::Backend("store handle escaped during open".to_string()))?;
        inner.opfs = Some(env);
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Write everything buffered in linear memory back to OPFS.
    ///
    /// A no-op when the browser granted synchronous access handles, because
    /// those write through. In the mirror fallback this is what makes the
    /// database survive the tab.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub async fn persist(&self) -> Result<()> {
        let Some(env) = self.inner.opfs.as_ref() else {
            return Ok(());
        };
        self.inner
            .db
            .db()
            .flush()
            .map_err(|error| Error::Backend(format!("flush before persist failed: {error}")))?;
        env.persist()
            .await
            .map_err(|error| Error::Backend(format!("OPFS persist failed: {error}")))
    }

    /// Where the database lives.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Wait for in-flight transactions to finish, up to the configured
    /// timeout.
    ///
    /// On wasm there is nothing to wait for: the runtime is
    /// single-threaded, so any transaction still counted here belongs to
    /// this task's own call stack and spinning would deadlock rather than
    /// let it finish. Report it instead.
    #[cfg(not(target_arch = "wasm32"))]
    async fn await_quiescence(&self) -> Result<()> {
        let deadline = web_time::Instant::now() + self.inner.options.close_timeout;
        let mut backoff = std::time::Duration::from_millis(1);
        while self.inner.active_txns.load(Ordering::Acquire) > 0 {
            if web_time::Instant::now() >= deadline {
                return Err(self.in_flight_error());
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
        }
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    async fn await_quiescence(&self) -> Result<()> {
        if self.inner.active_txns.load(Ordering::Acquire) > 0 {
            return Err(self.in_flight_error());
        }
        Ok(())
    }

    fn in_flight_error(&self) -> Error {
        Error::Backend(format!(
            "closed with {} transaction(s) still in flight",
            self.inner.active_txns.load(Ordering::Acquire)
        ))
    }

    fn ensure_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::DBClosed);
        }
        Ok(())
    }
}

impl crate::corekv::private::Sealed for RegolithStore {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Store for RegolithStore {
    async fn allocate(&self, key: &[u8], count: u64) -> Result<std::ops::Range<u64>> {
        self.ensure_open()?;
        super::blocking(|| self.inner.db.db().allocate(key, count))
            .map_err(|error| Error::Backend(error.to_string()))
    }

    fn transaction_stats_handle(&self) -> Option<TransactionStatsHandle> {
        Some(self.inner.stats.clone())
    }

    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        self.ensure_open()?;
        self.inner.active_txns.fetch_add(1, Ordering::AcqRel);
        // Closing between the check and the count would leak the count,
        // so re-check and hand it back rather than leaving `close`
        // waiting on a transaction that was never handed out.
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.active_txns.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::DBClosed);
        }
        let txn = RegolithTxn::new(
            &self.inner.db,
            readonly,
            self.inner.options.isolation,
            Arc::clone(&self.inner.active_txns),
            Arc::clone(&self.inner.event_order),
        );
        match txn {
            Ok(txn) => Ok(Box::new(txn)),
            Err(error) => {
                self.inner.active_txns.fetch_sub(1, Ordering::AcqRel);
                Err(error)
            }
        }
    }

    async fn transact_retry(
        &self,
        options: RetryOptions,
        mut attempt: RetryAttempt,
    ) -> std::result::Result<RetryAttemptValue, RetryAttemptError> {
        self.ensure_open()?;
        self.inner.active_txns.fetch_add(1, Ordering::AcqRel);
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.active_txns.fetch_sub(1, Ordering::AcqRel);
            return Err(RetryAttemptError::Storage(Error::DBClosed));
        }

        struct ActiveTxnGuard(Arc<AtomicUsize>);
        impl Drop for ActiveTxnGuard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::AcqRel);
            }
        }
        let _active = ActiveTxnGuard(Arc::clone(&self.inner.active_txns));
        let queue_db = Arc::clone(&self.inner.db);
        let policy = regolith::RetryPolicy::new(options.max_attempts.max(1));
        let mut event_token = None;
        let mut committed_callbacks = RetryCallbacks::default();
        let outcome = {
            let mut queue = queue_db.db().io_queue();
            let queue_id = queue.id();
            let future = queue_db.transact_async(queue_id, &policy, async |txn, lost| {
                if lost.is_some() {
                    telemetry::record_retry_attempt(telemetry::RetryLayer::Engine);
                }
                committed_callbacks = RetryCallbacks::default();
                let mut retry_txn = RegolithRetryTxn {
                    previous_conflict: lost.map(|reason| crate::corekv::TransactionConflict {
                        access: reason.mine(),
                        winning_write: reason.theirs(),
                        observed_seq: reason.observed_seq(),
                        latest_seq: reason.latest_seq(),
                    }),
                    txn,
                    db: Arc::clone(&self.inner.db),
                    isolation: self.inner.options.isolation,
                    attempt_io_active: false,
                    has_writes: false,
                    callbacks: RetryCallbacks::default(),
                };
                let mut value = attempt.run(&mut retry_txn).await;
                if value.is_ok() && retry_txn.attempt_io_active {
                    value = Err(RetryAttemptError::Storage(Error::Other(
                        "retry attempt returned with scoped IO still active".into(),
                    )));
                }
                if value.is_ok()
                    && retry_txn.callbacks.has_ordered_callbacks()
                    && !retry_txn.has_writes
                {
                    value = Err(RetryAttemptError::Storage(Error::Other(
                        "cannot publish ordered callbacks from a retry attempt without writes"
                            .into(),
                    )));
                }
                if value.is_ok() {
                    if retry_txn.callbacks.has_ordered_callbacks() && event_token.is_none() {
                        event_token = Some(self.inner.event_order.begin());
                    }
                    committed_callbacks = std::mem::take(&mut retry_txn.callbacks);
                }
                value
            });
            // Regolith drives this future on its own I/O queue. Tokio's
            // cooperative budget cannot be replenished by its scheduler while
            // this synchronous bridge owns the thread; an exhausted budget
            // would park an otherwise ready storage operation forever.
            #[cfg(not(target_arch = "wasm32"))]
            {
                super::blocking(|| queue.block_on(tokio::task::unconstrained(future)))
            }
            #[cfg(target_arch = "wasm32")]
            {
                drive_browser_queue(&mut queue, future).await
            }
        };
        match outcome {
            Ok((value, receipt)) => {
                let (callbacks, async_callbacks, ordered_callbacks, ordered_async_callbacks) =
                    committed_callbacks.into_parts();
                for callback in async_callbacks {
                    let callback = callback.lock().unwrap().take();
                    if let Some(callback) = callback {
                        callback().await;
                    }
                }
                for callback in callbacks {
                    let callback = callback.lock().unwrap().take();
                    if let Some(callback) = callback {
                        callback();
                    }
                }
                let mut post_commit_error = None;
                for callback in ordered_async_callbacks {
                    let callback = callback.lock().unwrap().take();
                    if let Some(callback) = callback {
                        if let Err(error) = callback().await {
                            post_commit_error.get_or_insert(error);
                        }
                    }
                }
                let events: Vec<Event> = ordered_callbacks
                    .into_iter()
                    .map(|callback| {
                        Arc::new(move || {
                            let callback = callback.lock().unwrap().take();
                            if let Some(callback) = callback {
                                callback();
                            }
                        }) as Event
                    })
                    .collect();
                let events = if post_commit_error.is_some() {
                    Vec::new()
                } else {
                    events
                };
                let has_events = !events.is_empty();
                if event_token.is_some_and(|token| !token.finish(receipt.seq(), events))
                    && has_events
                {
                    return Err(RetryAttemptError::PostCommit(Error::Other(
                        "ordered retry receipt did not advance".into(),
                    )));
                }
                if let Some(error) = post_commit_error {
                    return Err(RetryAttemptError::PostCommit(Error::Other(error)));
                }
                Ok(value)
            }
            Err(regolith::TransactError::Closure(error)) => Err(error),
            Err(regolith::TransactError::Exhausted(conflict)) => Err(RetryAttemptError::Exhausted(
                crate::corekv::TransactionConflict {
                    access: conflict.mine(),
                    winning_write: conflict.theirs(),
                    observed_seq: conflict.observed_seq(),
                    latest_seq: conflict.latest_seq(),
                },
            )),
            Err(regolith::TransactError::Engine(error)) => Err(RetryAttemptError::Storage(
                super::transaction::map_txn_error(error),
            )),
            Err(_other) => Err(RetryAttemptError::Storage(Error::Backend(
                "unknown regolith transaction retry failure".into(),
            ))),
        }
    }

    async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.await_quiescence().await?;
        // On OPFS the database is resident in linear memory and only `persist`
        // writes it back, so closing without it discards everything since the
        // last explicit call. A no-op on every other environment.
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        self.persist().await?;
        self.inner
            .db
            .db()
            .close()
            .map_err(|error| Error::Backend(format!("failed to close regolith: {error}")))
    }
}

struct RegolithRetryTxn<'a> {
    previous_conflict: Option<crate::corekv::TransactionConflict>,
    txn: &'a mut regolith::Transaction,
    db: Arc<OptimisticTransactionDb>,
    isolation: regolith::IsolationLevel,
    attempt_io_active: bool,
    has_writes: bool,
    callbacks: RetryCallbacks,
}

impl RegolithRetryTxn<'_> {
    fn ensure_no_active_attempt_io(&self) -> Result<()> {
        if self.attempt_io_active {
            return Err(Error::Other(
                "raw retry operations are unavailable while scoped IO is active".into(),
            ));
        }
        Ok(())
    }
}

impl crate::corekv::private::Sealed for RegolithRetryTxn<'_> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl RetryTxn for RegolithRetryTxn<'_> {
    fn previous_conflict(&self) -> Option<crate::corekv::TransactionConflict> {
        self.previous_conflict
    }

    async fn get(&self, key: &[u8]) -> Result<Option<bytes::Bytes>> {
        self.ensure_no_active_attempt_io()?;
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let txn: &regolith::Transaction = self.txn;
        regolith::ready(|| txn.get_slice(key))
            .await
            .map(|value| value.map(bytes::Bytes::from_owner))
            .map_err(super::transaction::map_txn_error)
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.ensure_no_active_attempt_io()?;
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let txn: &regolith::Transaction = self.txn;
        regolith::ready(|| txn.get_slice(key))
            .await
            .map(|value| value.is_some())
            .map_err(super::transaction::map_txn_error)
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_no_active_attempt_io()?;
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        regolith::ready(|| self.txn.put(key, value))
            .await
            .map_err(super::transaction::map_txn_error)?;
        self.has_writes = true;
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.ensure_no_active_attempt_io()?;
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        regolith::ready(|| self.txn.delete(key))
            .await
            .map_err(super::transaction::map_txn_error)?;
        self.has_writes = true;
        Ok(())
    }

    fn begin_attempt_io(&mut self) -> Result<Box<dyn RetryAttemptIO>> {
        if self.attempt_io_active {
            return Err(Error::Other("scoped retry IO is already active".into()));
        }
        let placeholder = self.db.begin(&TxnOptions::new().isolation(self.isolation));
        let actual = Box::new(std::mem::replace(self.txn, placeholder));
        self.attempt_io_active = true;
        Ok(Box::new(RegolithTxn::scoped_attempt(actual)))
    }

    fn finish_attempt_io(
        &mut self,
        io: Box<dyn RetryAttemptIO>,
        callbacks: RetryCallbacks,
    ) -> Result<()> {
        let io = io
            .into_any()
            .downcast::<RegolithTxn>()
            .map_err(|_| Error::Other("retry IO belongs to a different backend".into()))?;
        let (actual, has_writes) = io.into_scoped_engine_txn()?;
        self.has_writes |= has_writes;
        *self.txn = *actual;
        self.callbacks = callbacks;
        self.attempt_io_active = false;
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Dropable for RegolithStore {
    async fn drop_all(&self) -> Result<()> {
        super::blocking(|| {
            self.ensure_open()?;
            self.inner
                .db
                .db()
                .drop_all()
                .map_err(|error| Error::Backend(format!("drop_all failed: {error}")))
        })
    }
}

#[cfg(test)]
mod retry_cold_io_tests {
    use super::*;

    struct CooperativeBudgetAttempt;
    impl crate::corekv::RetryAttemptFn for CooperativeBudgetAttempt {
        fn run<'a>(
            &'a mut self,
            _retry: &'a mut dyn RetryTxn,
        ) -> crate::corekv::RetryAttemptFuture<'a> {
            Box::pin(async move {
                // More ready operations than Tokio's per-poll budget. The
                // engine queue must not wait for the suspended outer runtime
                // scheduler to replenish that budget.
                for _ in 0..1024 {
                    tokio::task::consume_budget().await;
                }
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    #[tokio::test]
    async fn retry_queue_does_not_depend_on_outer_tokio_cooperative_budget() {
        let store = RegolithStore::in_memory().unwrap();
        store
            .transact_retry(RetryOptions::new(1), Box::new(CooperativeBudgetAttempt))
            .await
            .unwrap();
    }

    struct ColdReadAttempt;
    impl crate::corekv::RetryAttemptFn for ColdReadAttempt {
        fn run<'a>(
            &'a mut self,
            retry: &'a mut dyn RetryTxn,
        ) -> crate::corekv::RetryAttemptFuture<'a> {
            Box::pin(async move {
                let mut io = retry.begin_attempt_io()?;
                assert_eq!(io.get(b"cold/00").await?.unwrap().len(), 16384);
                assert!(io.has(b"cold/01").await?);
                assert_eq!(io.get_size(b"cold/02").await?, Some(16384));
                assert_eq!(io.get_parts(b"cold/03", &[]).await?.unwrap().len(), 16384);
                for validated in [false, true] {
                    let mut options =
                        crate::corekv::IterOptions::new().with_prefix(b"cold/".to_vec());
                    if validated {
                        options = options.with_validation(true);
                    }
                    let mut iter = io.iterator(options).await?;
                    let mut count = 0;
                    while let Some(pair) = iter.next().await? {
                        assert_eq!(pair.value.len(), 16384);
                        count += 1;
                    }
                    assert_eq!(count, 24);
                    iter.close().await?;
                    drop(iter);
                }
                io.set(b"cold-result", b"complete").await?;
                retry.finish_attempt_io(io, RetryCallbacks::default())?;
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    #[tokio::test]
    async fn scoped_retry_io_reads_cold_sst_points_and_scans() {
        let directory = tempfile::tempdir().unwrap();
        let store = RegolithStore::open(directory.path()).unwrap();
        let mut txn = store.new_txn(false).await.unwrap();
        for n in 0..24 {
            txn.set(format!("cold/{n:02}").as_bytes(), &vec![n; 16384])
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
        store.inner.db.db().flush().unwrap();
        store.close().await.unwrap();
        drop(store);
        let store = RegolithStore::open(directory.path()).unwrap();
        store
            .transact_retry(RetryOptions::new(2), Box::new(ColdReadAttempt))
            .await
            .unwrap();
        let txn = store.new_txn(true).await.unwrap();
        assert_eq!(
            txn.get(b"cold-result").await.unwrap().as_deref(),
            Some(b"complete".as_slice())
        );
        txn.discard();
        store.close().await.unwrap();
    }
}

#[cfg(test)]
mod retry_read_tests {
    use super::*;

    /// A compare-and-set whose condition finds nothing to change, while
    /// another writer updates the key it read before it commits.
    struct ConditionalNoop {
        store: RegolithStore,
    }
    impl crate::corekv::RetryAttemptFn for ConditionalNoop {
        fn run<'a>(
            &'a mut self,
            retry: &'a mut dyn RetryTxn,
        ) -> crate::corekv::RetryAttemptFuture<'a> {
            Box::pin(async move {
                assert_eq!(retry.get(b"flag").await?.as_deref(), Some(&b"old"[..]));
                assert!(retry.has(b"flag").await?);
                let other = self.store.inner.db.begin(&TxnOptions::new());
                other.put(b"flag", b"new").unwrap();
                other.commit().unwrap();
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    #[tokio::test]
    async fn a_write_free_attempt_and_an_update_of_what_it_read_both_commit() {
        let store = RegolithStore::in_memory().unwrap();
        let mut seed = store.new_txn(false).await.unwrap();
        seed.set(b"flag", b"old").await.unwrap();
        seed.commit().await.unwrap();
        store
            .transact_retry(
                RetryOptions::new(1),
                Box::new(ConditionalNoop {
                    store: store.clone(),
                }),
            )
            .await
            .unwrap();
        let read = store.new_txn(true).await.unwrap();
        assert_eq!(
            read.get(b"flag").await.unwrap().as_deref(),
            Some(&b"new"[..])
        );
        read.discard();
    }
}

#[cfg(test)]
mod scan_tests {
    use super::*;
    use crate::corekv::IterOptions;
    use regolith::{PerfContext, PerfLevel};

    async fn scan_lookups(txn: &dyn Txn, rows: usize) -> u64 {
        PerfContext::reset();
        let mut iter = txn
            .iterator(IterOptions::new().with_prefix(b"row/".to_vec()))
            .await
            .unwrap();
        let mut count = 0;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        iter.close().await.unwrap();
        assert_eq!(count, rows);
        PerfContext::capture().block_cache_lookup_count
    }

    /// A scan of a writing transaction walks the tables once, as a snapshot
    /// scan does, rather than seeking them again for every page.
    #[tokio::test]
    async fn an_unvalidated_scan_of_a_writing_transaction_reads_each_block_once() {
        let directory = tempfile::tempdir().unwrap();
        let store = RegolithStore::open(directory.path()).unwrap();
        let rows = 3 * super::super::iterator::PAGE_ENTRIES + 7;
        let mut seed = store.new_txn(false).await.unwrap();
        for row in 0..rows {
            seed.set(format!("row/{row:06}").as_bytes(), &[7; 64])
                .await
                .unwrap();
        }
        seed.commit().await.unwrap();
        store.inner.db.db().flush().unwrap();

        let level = PerfContext::set_level(PerfLevel::EnableCount);
        let snapshot = store.new_txn(true).await.unwrap();
        let snapshot_lookups = scan_lookups(snapshot.as_ref(), rows).await;
        snapshot.discard();
        let mut writing = store.new_txn(false).await.unwrap();
        writing.set(b"other", b"own").await.unwrap();
        let writing_lookups = scan_lookups(writing.as_ref(), rows).await;
        writing.discard();
        PerfContext::set_level(level);

        assert!(snapshot_lookups > 0);
        assert_eq!(writing_lookups, snapshot_lookups);
        store.close().await.unwrap();
    }
}

#[cfg(test)]
mod retry_order_tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::sync::Notify;

    struct OrderedAttempt {
        events: Arc<Mutex<Vec<u8>>>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }
    impl crate::corekv::RetryAttemptFn for OrderedAttempt {
        fn run<'a>(
            &'a mut self,
            retry: &'a mut dyn RetryTxn,
        ) -> crate::corekv::RetryAttemptFuture<'a> {
            Box::pin(async move {
                let mut io = retry.begin_attempt_io()?;
                io.set(b"ordered-retry", b"durable").await?;
                let mut callbacks = RetryCallbacks::default();
                let entered = self.entered.clone();
                let release = self.release.clone();
                callbacks.push_ordered_async(Box::new(move || {
                    Box::pin(async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                }));
                let events = self.events.clone();
                callbacks.push_ordered(Box::new(move || events.lock().unwrap().push(1)));
                retry.finish_attempt_io(io, callbacks)?;
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    #[tokio::test]
    async fn regular_commit_waits_for_earlier_retry_postcommit_hook() {
        let store = RegolithStore::in_memory().unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let attempt = OrderedAttempt {
            events: events.clone(),
            entered: entered.clone(),
            release: release.clone(),
        };
        let retry_store = store.clone();
        let retry_task = tokio::spawn(async move {
            retry_store
                .transact_retry(RetryOptions::new(2), Box::new(attempt))
                .await
                .unwrap();
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let mut regular = store.new_txn(false).await.unwrap();
        regular.set(b"ordered-regular", b"durable").await.unwrap();
        let out = events.clone();
        regular
            .on_success_ordered(Arc::new(move || out.lock().unwrap().push(2)))
            .unwrap();
        regular.commit().await.unwrap();
        assert!(events.lock().unwrap().is_empty());
        release.notify_one();
        retry_task.await.unwrap();
        assert_eq!(*events.lock().unwrap(), vec![1, 2]);
    }
}

/// Browser transactions yield to the event loop instead of parking a thread.
#[cfg(target_arch = "wasm32")]
async fn drive_browser_queue<F: std::future::Future>(
    queue: &mut regolith::IoQueue,
    future: F,
) -> F::Output {
    use std::{future::Future, pin::Pin, task::Poll};
    let mut future = std::pin::pin!(future);
    type Timer = (web_time::Instant, Pin<Box<dyn Future<Output = ()>>>);
    let mut timer: Option<Timer> = None;
    std::future::poll_fn(|cx| {
        if let Poll::Ready(result) = future.as_mut().poll(cx) {
            return Poll::Ready(result);
        }
        let progress = queue.poll(regolith::IoBudget::ALL);
        if progress.completed > 0 {
            cx.waker().wake_by_ref();
        }
        queue.idle_waker(cx.waker());
        if let Some(delay) = progress.wake_in {
            let deadline = web_time::Instant::now() + delay;
            if timer
                .as_ref()
                .is_none_or(|(current, _)| deadline < *current)
            {
                timer = Some((deadline, Box::pin(n0_future::time::sleep(delay))));
            }
            if let Some((_, sleep)) = timer.as_mut() {
                if sleep.as_mut().poll(cx).is_ready() {
                    timer = None;
                    cx.waker().wake_by_ref();
                }
            }
        } else {
            timer = None;
        }
        Poll::Pending
    })
    .await
}
