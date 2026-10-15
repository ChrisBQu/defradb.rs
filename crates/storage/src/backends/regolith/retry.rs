//! The engine-owned retry: one call runs an attempt in a transaction regolith
//! owns and commits, and runs it again after each conflict, up to the
//! policy's budget.
//!
//! An attempt sees a [`RegolithRetryTxn`]: the reads and writes of the
//! engine transaction, and nothing that could commit it or let it escape.
//! What the attempt registered for after its end ([`PostCommit`]) is bound to
//! the commit-receipt stream before regolith commits it; an attempt that
//! loses lets go of its place and runs its abort callbacks, and only the
//! winner's commit callbacks run.

#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

use std::sync::Arc;

use async_trait::async_trait;
use regolith::{OptimisticTransactionDb, TxnOptions};

use super::event_order::EventOrder;
use super::post_commit::PostCommit;
use super::transaction::RegolithTxn;
use crate::corekv::{
    Error, Result, RetryAttempt, RetryAttemptError, RetryAttemptIO, RetryAttemptValue,
    RetryOptions, RetryTxn, TxnCallbacks,
};

/// Runs `attempt` in engine transactions of `db` until one commits, an
/// attempt fails, or the policy's attempts are spent.
pub(super) async fn transact(
    db: &Arc<OptimisticTransactionDb>,
    isolation: regolith::IsolationLevel,
    event_order: &Arc<EventOrder>,
    options: RetryOptions,
    mut attempt: RetryAttempt,
) -> std::result::Result<RetryAttemptValue, RetryAttemptError> {
    let queue_db = Arc::clone(db);
    let policy = regolith::RetryPolicy::new(options.max_attempts.max(1));
    // The work the latest attempt registered. regolith runs exactly one
    // outcome per attempt: an attempt that lost already let go of its
    // place in the stream through its own `on_abort`, and its abort
    // callbacks run here when the next one starts.
    let mut latest: Option<PostCommit> = None;
    let outcome = {
        let mut queue = queue_db.db().io_queue();
        let queue_id = queue.id();
        let future = queue_db.transact_async(queue_id, &policy, async |txn, lost| {
            if lost.is_some() {
                telemetry::record_retry_attempt(telemetry::RetryLayer::Engine);
            }
            if let Some(previous) = latest.take() {
                previous.failed().await;
            }
            let mut retry_txn = RegolithRetryTxn {
                previous_conflict: lost.map(crate::corekv::TransactionConflict::from),
                txn,
                db: Arc::clone(db),
                isolation,
                attempt_io_active: false,
                callbacks: TxnCallbacks::default(),
            };
            let mut value = attempt.run(&mut retry_txn).await;
            if value.is_ok() && retry_txn.attempt_io_active {
                value = Err(RetryAttemptError::Storage(Error::Other(
                    "retry attempt returned with scoped IO still active".into(),
                )));
            }
            if value.is_ok() {
                let callbacks = std::mem::take(&mut retry_txn.callbacks);
                latest = Some(PostCommit::bind(event_order, retry_txn.txn, callbacks));
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
            latest.failed().await;
        }
    }
    match outcome {
        Ok((value, _receipt)) => Ok(value),
        Err(regolith::TransactError::Closure(error)) => Err(error),
        Err(regolith::TransactError::Exhausted(conflict)) => Err(RetryAttemptError::Exhausted(
            crate::corekv::TransactionConflict::from(&conflict),
        )),
        Err(regolith::TransactError::Engine(error)) => Err(RetryAttemptError::Storage(
            super::transaction::map_txn_error(error),
        )),
        Err(_other) => Err(RetryAttemptError::Storage(Error::Backend(
            "unknown regolith transaction retry failure".into(),
        ))),
    }
}

struct RegolithRetryTxn<'a> {
    previous_conflict: Option<crate::corekv::TransactionConflict>,
    txn: &'a mut regolith::Transaction,
    db: Arc<OptimisticTransactionDb>,
    isolation: regolith::IsolationLevel,
    attempt_io_active: bool,
    callbacks: TxnCallbacks,
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
        callbacks: TxnCallbacks,
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

#[cfg(test)]
mod retry_cold_io_tests {
    use super::*;
    use crate::backends::RegolithStore;
    use crate::corekv::Store;

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
                retry.finish_attempt_io(io, TxnCallbacks::default())?;
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
        store.engine().db().flush().unwrap();
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
    use crate::backends::RegolithStore;
    use crate::corekv::{OnAbort, OnCommit, Store, TxnCallback};
    use std::sync::atomic::{AtomicUsize, Ordering};

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
                let other = self.store.engine().begin(&TxnOptions::new());
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
    fn counting(count: &Arc<AtomicUsize>) -> TxnCallback {
        let count = Arc::clone(count);
        Box::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
        })
    }

    /// An attempt that writes after a read another writer changes before
    /// its first run commits, and the callbacks each run registers.
    struct LosesOnce {
        store: RegolithStore,
        runs: usize,
        committed: Arc<AtomicUsize>,
        aborted: Arc<AtomicUsize>,
    }
    impl crate::corekv::RetryAttemptFn for LosesOnce {
        fn run<'a>(
            &'a mut self,
            retry: &'a mut dyn RetryTxn,
        ) -> crate::corekv::RetryAttemptFuture<'a> {
            Box::pin(async move {
                self.runs += 1;
                let mut io = retry.begin_attempt_io()?;
                assert!(io.get(b"flag").await?.is_some());
                io.set(b"out", b"v").await?;
                if self.runs == 1 {
                    let other = self.store.engine().begin(&TxnOptions::new());
                    other.put(b"flag", b"changed").unwrap();
                    other.commit().unwrap();
                }
                let mut callbacks = TxnCallbacks::default();
                callbacks.on_commit(OnCommit::Sync(counting(&self.committed)))?;
                callbacks.on_abort(OnAbort::Sync(counting(&self.aborted)));
                retry.finish_attempt_io(io, callbacks)?;
                Ok(Box::new(()) as RetryAttemptValue)
            })
        }
    }

    /// Each attempt ends once: the one that lost runs its abort callbacks,
    /// the one that won its commit callbacks.
    #[tokio::test]
    async fn an_attempt_that_loses_runs_its_abort_callbacks_and_the_winner_its_commit_ones() {
        let store = RegolithStore::in_memory().unwrap();
        let mut seed = store.new_txn(false).await.unwrap();
        seed.set(b"flag", b"old").await.unwrap();
        seed.commit().await.unwrap();
        let (committed, aborted) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let attempt = LosesOnce {
            store: store.clone(),
            runs: 0,
            committed: Arc::clone(&committed),
            aborted: Arc::clone(&aborted),
        };
        store
            .transact_retry(RetryOptions::new(2), Box::new(attempt))
            .await
            .unwrap();
        assert_eq!(committed.load(Ordering::SeqCst), 1);
        assert_eq!(aborted.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod retry_order_tests {
    use super::*;
    use crate::backends::RegolithStore;
    use crate::corekv::{OnCommit, Store};
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
                let mut callbacks = TxnCallbacks::default();
                let entered = self.entered.clone();
                let release = self.release.clone();
                callbacks.on_commit(OnCommit::OrderedAsync(Box::new(move || {
                    Box::pin(async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                })))?;
                let events = self.events.clone();
                callbacks.on_commit(OnCommit::Ordered(Box::new(move || {
                    events.lock().unwrap().push(1)
                })))?;
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
            .on_commit(OnCommit::Ordered(Box::new(move || {
                out.lock().unwrap().push(2)
            })))
            .unwrap();
        regular.commit().await.unwrap();
        assert!(events.lock().unwrap().is_empty());
        release.notify_one();
        retry_task.await.unwrap();
        assert_eq!(*events.lock().unwrap(), vec![1, 2]);
    }
}
