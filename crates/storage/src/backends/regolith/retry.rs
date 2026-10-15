//! The engine-owned retry: one call runs an attempt in a transaction regolith
//! owns and commits, and runs it again after each conflict, up to the
//! policy's budget.
//!
//! An attempt sees a [`RegolithRetryTxn`]: the reads and writes of the
//! engine transaction, and nothing that could commit it or let it escape.
//! What the attempt registered for after its commit ([`AttemptWork`]) is
//! bound to the commit-receipt stream before regolith commits it; an attempt
//! that loses lets go of its place, and only the winner's work runs.

#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

use std::sync::Arc;

use async_trait::async_trait;
use regolith::{OptimisticTransactionDb, TxnOptions};

use super::event_order::{Binding, Event, EventOrder};
use super::transaction::RegolithTxn;
use crate::corekv::{
    AsyncOrderedRetryCallback, AsyncTxnCallback, Error, Result, RetryAttempt, RetryAttemptError,
    RetryAttemptIO, RetryAttemptValue, RetryCallbacks, RetryOptions, RetryTxn, TxnCallback,
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
                db: Arc::clone(db),
                isolation: isolation,
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
                    event_order,
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
    use crate::corekv::Store;

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
}

#[cfg(test)]
mod retry_order_tests {
    use super::*;
    use crate::backends::RegolithStore;
    use crate::corekv::Store;
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
