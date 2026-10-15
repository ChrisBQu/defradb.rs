//! The regolith-backed store.
//!
//! regolith validates its own transactions, so there is no conflict
//! tracker or read-set bookkeeping here: the engine decides whether a
//! transaction may land. Snapshot capture and native commit use the engine
//! directly, so independent commits can join its commit groups.

#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use regolith::{OptimisticTransactionDb, TxnOptions};

use super::config::RegolithStoreOptions;
use super::event_order::{Binding, Event, EventOrder};
use super::hooks::StoreHooks;
use super::listener::StoreListener;
use super::slots::TxnSlots;
use super::transaction::RegolithTxn;
use crate::backends::shared::TransactionStatsHandle;
use crate::corekv::{
    AsyncOrderedRetryCallback, AsyncTxnCallback, Dropable, Error, Result, RetryAttempt,
    RetryAttemptError, RetryAttemptIO, RetryAttemptValue, RetryCallbacks, RetryOptions, RetryTxn,
    Store, Txn, TxnCallback,
};

/// Key-value store backed by regolith.
///
/// Cloning hands back another handle on the same database, not another
/// database: the in-flight slots are shared, so closing through one handle
/// closes it for all of them.
#[derive(Clone)]
pub struct RegolithStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    db: Arc<OptimisticTransactionDb>,
    options: RegolithStoreOptions,
    slots: Arc<TxnSlots>,
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
            .engine_for_open()
            .statistics(Some(engine_stats))
            .add_listener(Arc::new(StoreListener(stats.clone())))
            .transaction_hooks(Arc::new(StoreHooks));
        #[cfg(not(target_arch = "wasm32"))]
        let db = super::rewrite::open(&path, engine, &options)?;
        #[cfg(target_arch = "wasm32")]
        let db = OptimisticTransactionDb::open(&path, engine)
            .map_err(|error| Error::Backend(format!("failed to open regolith: {error}")))?;
        let db = db
            .with_isolation(options.isolation)
            .with_policy(Arc::new(super::classifier::DefraKeyClassifier));
        Ok(Self {
            inner: Arc::new(StoreInner {
                db: Arc::new(db),
                options,
                slots: TxnSlots::new(),
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

    fn ensure_open(&self) -> Result<()> {
        if self.inner.slots.is_closing() {
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
        let slot = self.inner.slots.enter()?;
        let txn = RegolithTxn::new(
            &self.inner.db,
            readonly,
            self.inner.options.isolation,
            slot,
            Arc::clone(&self.inner.event_order),
        )?;
        Ok(Box::new(txn))
    }

    async fn transact_retry(
        &self,
        options: RetryOptions,
        mut attempt: RetryAttempt,
    ) -> std::result::Result<RetryAttemptValue, RetryAttemptError> {
        let _slot = self.inner.slots.enter()?;
        let queue_db = Arc::clone(&self.inner.db);
        let policy = regolith::RetryPolicy::new(options.max_attempts.max(1));
        // The work the latest attempt registered. regolith runs exactly one
        // outcome per attempt: an attempt that lost already let go of its
        // place in the stream through its own `on_abort`, and its callbacks
        // are dropped here when the next one starts.
        let mut latest: Option<AttemptWork> = None;
        let outcome = {
            let mut queue = queue_db.db().io_queue();
            let queue_id = queue.id();
            let future = queue_db.transact_async(queue_id, &policy, async |txn, lost| {
                if lost.is_some() {
                    telemetry::record_retry_attempt(telemetry::RetryLayer::Engine);
                }
                if let Some(previous) = latest.take() {
                    previous.lost();
                }
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
                    callbacks: RetryCallbacks::default(),
                };
                let mut value = attempt.run(&mut retry_txn).await;
                if value.is_ok() && retry_txn.attempt_io_active {
                    value = Err(RetryAttemptError::Storage(Error::Other(
                        "retry attempt returned with scoped IO still active".into(),
                    )));
                }
                if value.is_ok() {
                    let callbacks = std::mem::take(&mut retry_txn.callbacks);
                    latest = Some(AttemptWork::bind(
                        &self.inner.event_order,
                        retry_txn.txn,
                        callbacks,
                    ));
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
        let committed = outcome.is_ok();
        if let Some(latest) = latest {
            if committed {
                latest.committed().await;
            } else {
                latest.lost();
            }
        }
        match outcome {
            Ok((value, _receipt)) => Ok(value),
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
        if !self.inner.slots.begin_close() {
            return Ok(());
        }
        // A transaction still open, a commit still running and its
        // post-commit work each hold a slot; the last to end opens the latch.
        self.inner
            .slots
            .quiesce(self.inner.options.close_timeout)
            .await?;
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

/// What one retry attempt registered for after its commit: its ordered
/// events and hooks bound to the stream, and its other success callbacks.
struct AttemptWork {
    binding: Option<Binding>,
    hooks: Vec<AsyncOrderedRetryCallback>,
    sync: Vec<TxnCallback>,
    asynchronous: Vec<AsyncTxnCallback>,
}

impl AttemptWork {
    /// Binds the attempt's ordered work to `txn`, which regolith commits next.
    fn bind(
        order: &Arc<EventOrder>,
        txn: &mut regolith::Transaction,
        callbacks: RetryCallbacks,
    ) -> Self {
        let parts = callbacks.into_parts();
        let events: Vec<Event> = parts
            .ordered
            .into_iter()
            .map(|callback| callback as Event)
            .collect();
        let hooks = parts.ordered_async;
        let binding = (!events.is_empty() || !hooks.is_empty())
            .then(|| order.bind(txn, events, !hooks.is_empty()));
        Self {
            binding,
            hooks,
            sync: parts.sync,
            asynchronous: parts.asynchronous,
        }
    }

    /// The attempt did not commit: nothing it registered runs.
    fn lost(self) {
        if let Some(binding) = self.binding {
            binding.hand_over(false);
        }
    }

    /// The attempt committed and is durable: its hooks run (a failure is
    /// reported, never returned), its events go out in receipt order, and
    /// its success callbacks run.
    async fn committed(self) {
        let running = self.binding.and_then(|binding| binding.hand_over(true));
        if let Some(running) = running {
            for hook in self.hooks {
                super::post_commit::run_hook(hook()).await;
            }
            running.done();
        }
        for callback in self.sync {
            callback();
        }
        for callback in self.asynchronous {
            callback().await;
        }
    }
}

struct RegolithRetryTxn<'a> {
    previous_conflict: Option<crate::corekv::TransactionConflict>,
    txn: &'a mut regolith::Transaction,
    db: Arc<OptimisticTransactionDb>,
    isolation: regolith::IsolationLevel,
    attempt_io_active: bool,
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
        let actual = io.into_scoped_engine_txn()?;
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
mod close_tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    #[derive(Default)]
    struct CountingWaker(AtomicUsize);
    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The transaction that ends last wakes `close`; nothing polls a clock.
    #[tokio::test]
    async fn close_is_woken_by_the_last_transaction_that_ends() {
        let store = RegolithStore::in_memory().unwrap();
        let reader = store.new_txn(true).await.unwrap();
        let mut writer = store.new_txn(false).await.unwrap();
        writer.set(b"key", b"value").await.unwrap();
        let counter = Arc::new(CountingWaker::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut cx = Context::from_waker(&waker);
        let mut close = Box::pin(store.close());
        assert!(close.as_mut().poll(&mut cx).is_pending());
        assert!(matches!(store.new_txn(false).await, Err(Error::DBClosed)));

        drop(reader);
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
        writer.commit().await.unwrap();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(close.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    }

    #[tokio::test]
    async fn close_reports_a_transaction_still_open_at_its_deadline() {
        let mut options = RegolithStoreOptions::memory();
        options.close_timeout = std::time::Duration::from_millis(20);
        let store = RegolithStore::open_with_options("close-deadline", options).unwrap();
        let held = store.new_txn(false).await.unwrap();
        let error = store.close().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("1 transaction(s) still in flight"));
        drop(held);
    }
}

#[cfg(test)]
mod listener_tests {
    use super::*;
    use telemetry::StorageEvent;

    /// A store's flushes and table files reach the listener it opens with.
    #[tokio::test]
    async fn a_flush_and_its_table_file_are_counted() {
        let directory = tempfile::tempdir().unwrap();
        let store = RegolithStore::open(directory.path()).unwrap();
        let flushes = telemetry::storage_event_count(StorageEvent::Flush);
        let tables = telemetry::storage_event_count(StorageEvent::TableFileCreated);
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"row", b"value").await.unwrap();
        txn.commit().await.unwrap();
        store.inner.db.db().flush().unwrap();
        assert!(telemetry::storage_event_count(StorageEvent::Flush) > flushes);
        assert!(telemetry::storage_event_count(StorageEvent::TableFileCreated) > tables);
        store.close().await.unwrap();
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
mod post_commit_tests {
    use super::*;
    use crate::corekv::{
        AppendLog, AsyncOrderedTxnCallback, OrderedRetryCallback, OrderedTxnCallback,
        RetryAttemptFn, RetryAttemptFuture, Writer,
    };
    use regolith::sync::{Atom, Event as Signal};
    use telemetry::StorageEvent;

    /// What the ordered events of a test saw, in dispatch order.
    #[derive(Clone, Default)]
    struct Out(Arc<Atom<Vec<u64>>>);

    impl Out {
        fn push(&self, value: u64) {
            self.0.rcu(|values| {
                let mut next = values.clone();
                next.push(value);
                next
            });
        }

        fn event(&self, value: u64) -> OrderedTxnCallback {
            let out = self.clone();
            Arc::new(move || out.push(value))
        }

        fn retry_event(&self, value: u64) -> OrderedRetryCallback {
            let out = self.clone();
            Box::new(move || out.push(value))
        }

        fn get(&self) -> Vec<u64> {
            self.0.load().clone()
        }
    }

    fn failing_hook() -> AsyncOrderedTxnCallback {
        Arc::new(|| Box::pin(async { Err("the registration was refused".to_string()) }))
    }

    async fn read(store: &RegolithStore, key: &[u8]) -> Option<Vec<u8>> {
        let txn = store.new_txn(true).await.unwrap();
        let value = txn.get(key).await.unwrap().map(|value| value.to_vec());
        txn.discard();
        value
    }

    #[tokio::test]
    async fn a_failed_hook_after_a_durable_commit_is_reported_and_the_commit_stands() {
        let store = RegolithStore::in_memory().unwrap();
        let out = Out::default();
        let before = telemetry::storage_event_count(StorageEvent::PostCommitHookFailed);
        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"doc", b"v").await.unwrap();
        txn.on_success_ordered_async(failing_hook()).unwrap();
        txn.on_success_ordered(out.event(1)).unwrap();
        txn.commit().await.unwrap();
        assert_eq!(out.get(), vec![1]);
        assert!(telemetry::storage_event_count(StorageEvent::PostCommitHookFailed) > before);
        assert_eq!(read(&store, b"doc").await.as_deref(), Some(&b"v"[..]));
    }

    /// An attempt that writes `key` (when given) and registers `events`, and
    /// a hook that fails when `failing`.
    struct EventfulAttempt {
        key: Option<&'static [u8]>,
        out: Out,
        events: Vec<u64>,
        failing: bool,
    }

    impl RetryAttemptFn for EventfulAttempt {
        fn run<'a>(&'a mut self, retry: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
            Box::pin(async move {
                let mut io = retry.begin_attempt_io()?;
                if let Some(key) = self.key {
                    io.set(key, b"v").await?;
                }
                let mut callbacks = RetryCallbacks::default();
                if self.failing {
                    callbacks.push_ordered_async(Box::new(|| {
                        Box::pin(async { Err("the registration was refused".to_string()) })
                    }));
                }
                for value in &self.events {
                    callbacks.push_ordered(self.out.retry_event(*value));
                }
                retry.finish_attempt_io(io, callbacks)?;
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    #[tokio::test]
    async fn a_failed_hook_after_a_durable_retry_commit_is_reported_and_the_commit_stands() {
        let store = RegolithStore::in_memory().unwrap();
        let out = Out::default();
        let before = telemetry::storage_event_count(StorageEvent::PostCommitHookFailed);
        let attempt = EventfulAttempt {
            key: Some(b"doc"),
            out: out.clone(),
            events: vec![2],
            failing: true,
        };
        store
            .transact_retry(RetryOptions::new(2), Box::new(attempt))
            .await
            .unwrap();
        assert_eq!(out.get(), vec![2]);
        assert!(telemetry::storage_event_count(StorageEvent::PostCommitHookFailed) > before);
        assert_eq!(read(&store, b"doc").await.as_deref(), Some(&b"v"[..]));
    }

    #[tokio::test]
    async fn an_eventful_transaction_that_writes_nothing_dispatches_its_events() {
        let store = RegolithStore::in_memory().unwrap();
        let out = Out::default();
        let mut txn = store.new_txn(false).await.unwrap();
        assert!(txn.get(b"absent").await.unwrap().is_none());
        txn.on_success_ordered(out.event(1)).unwrap();
        txn.commit().await.unwrap();
        let attempt = EventfulAttempt {
            key: None,
            out: out.clone(),
            events: vec![2],
            failing: false,
        };
        store
            .transact_retry(RetryOptions::new(1), Box::new(attempt))
            .await
            .unwrap();
        assert_eq!(out.get(), vec![1, 2]);
    }

    #[tokio::test]
    async fn an_eventful_transaction_that_only_appends_dispatches_its_events() {
        let store = RegolithStore::in_memory().unwrap();
        let out = Out::default();
        let log = AppendLog::new(
            b"s/arrival/c1/head".to_vec(),
            b"s/arrival/c1/row/".to_vec(),
        );
        for value in [1, 2] {
            // The second append names a once key that already holds a
            // position, so regolith appends nothing for it.
            let mut txn = store.new_txn(false).await.unwrap();
            txn.append(&log, b"doc-1", Some(b"s/arrival/c1/doc/doc-1"))
                .await
                .unwrap();
            txn.on_success_ordered(out.event(value)).unwrap();
            txn.commit().await.unwrap();
        }
        assert_eq!(out.get(), vec![1, 2]);
    }

    /// An attempt that writes and whose hook holds the stream until
    /// `release` is set.
    struct HoldingAttempt {
        entered: Arc<Signal>,
        release: Arc<Signal>,
    }

    impl RetryAttemptFn for HoldingAttempt {
        fn run<'a>(&'a mut self, retry: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
            Box::pin(async move {
                let mut io = retry.begin_attempt_io()?;
                io.set(b"held", b"v").await?;
                let mut callbacks = RetryCallbacks::default();
                let entered = Arc::clone(&self.entered);
                let release = Arc::clone(&self.release);
                callbacks.push_ordered_async(Box::new(move || {
                    Box::pin(async move {
                        entered.set();
                        release.wait().await;
                        Ok(())
                    })
                }));
                retry.finish_attempt_io(io, callbacks)?;
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    /// A commit's panicking event goes out while another commit's thread
    /// drains the stream; that thread must not panic.
    #[tokio::test]
    async fn a_panicking_event_does_not_reach_the_commit_that_drains_it() {
        let store = RegolithStore::in_memory().unwrap();
        let before = telemetry::storage_event_count(StorageEvent::PostCommitEventPanicked);
        let entered = Arc::new(Signal::new());
        let release = Arc::new(Signal::new());
        let attempt = HoldingAttempt {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        };
        let holder = store.clone();
        let held = tokio::spawn(async move {
            holder
                .transact_retry(RetryOptions::new(1), Box::new(attempt))
                .await
                .map(|_| ())
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), entered.wait())
            .await
            .unwrap();

        let mut txn = store.new_txn(false).await.unwrap();
        txn.set(b"doc", b"v").await.unwrap();
        txn.on_success_ordered(Arc::new(|| panic!("a subscriber panicked")))
            .unwrap();
        txn.commit().await.unwrap();

        release.set();
        held.await.unwrap().unwrap();
        assert!(telemetry::storage_event_count(StorageEvent::PostCommitEventPanicked) > before);
    }
}

#[cfg(test)]
mod discard_tests {
    use super::*;
    use crate::corekv::Writer;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn counting(count: &Arc<AtomicUsize>) -> crate::corekv::TxnCallback {
        let count = Arc::clone(count);
        Box::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
        })
    }

    /// A transaction dropped without a commit or a discard ends without
    /// committing, so its discard callbacks run, once.
    #[tokio::test]
    async fn a_dropped_transaction_runs_its_discard_callbacks_once() {
        let store = RegolithStore::in_memory().unwrap();
        let discarded = Arc::new(AtomicUsize::new(0));
        let mut dropped = store.new_txn(false).await.unwrap();
        dropped.set(b"key", b"value").await.unwrap();
        dropped.on_discard(counting(&discarded));
        drop(dropped);
        assert_eq!(discarded.load(Ordering::SeqCst), 1);

        let mut explicit = store.new_txn(false).await.unwrap();
        explicit.on_discard(counting(&discarded));
        explicit.discard();
        assert_eq!(discarded.load(Ordering::SeqCst), 2);

        let mut committed = store.new_txn(false).await.unwrap();
        committed.set(b"key", b"value").await.unwrap();
        committed.on_discard(counting(&discarded));
        committed.commit().await.unwrap();
        assert_eq!(discarded.load(Ordering::SeqCst), 2);
    }
}

#[cfg(test)]
mod hooks_tests {
    use super::*;
    use crate::corekv::Writer;
    use telemetry::StorageEvent;

    /// Every transaction's one outcome reaches the store's hooks.
    #[tokio::test]
    async fn commits_rollbacks_and_conflicts_are_counted_by_kind() {
        let store = RegolithStore::in_memory().unwrap();
        let count = telemetry::storage_event_count;
        let (commits, rollbacks, conflicts) = (
            count(StorageEvent::Commit),
            count(StorageEvent::AbortRollback),
            count(StorageEvent::AbortConflict),
        );
        let mut committed = store.new_txn(false).await.unwrap();
        committed.set(b"key", b"one").await.unwrap();
        committed.commit().await.unwrap();
        assert!(count(StorageEvent::Commit) > commits);

        store.new_txn(false).await.unwrap().discard();
        assert!(count(StorageEvent::AbortRollback) > rollbacks);

        let mut loser = store.new_txn(false).await.unwrap();
        assert!(loser.get(b"key").await.unwrap().is_some());
        loser.set(b"key", b"loser").await.unwrap();
        let mut winner = store.new_txn(false).await.unwrap();
        winner.set(b"key", b"winner").await.unwrap();
        winner.commit().await.unwrap();
        assert!(loser.commit().await.is_err());
        assert!(count(StorageEvent::AbortConflict) > conflicts);
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
