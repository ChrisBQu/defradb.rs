//! A commit's post-commit work, and what failed of it after the commit was
//! durable.
//!
//! What a transaction or a retry attempt registered ([`TxnCallbacks`]) is
//! bound to the commit-receipt stream before regolith commits it
//! ([`PostCommit::bind`]), and runs once the outcome is known: the commit
//! callbacks after a commit, the abort callbacks after a failed one.
//!
//! A durable commit's outcome stands. A hook that fails, panics or is
//! cancelled, an event that panics, and a regolith callback that panics
//! after its outcome are each reported here, as one warn line and one count
//! of `defradb.storage.events` by kind, and never turned into a failed write
//! or raised in another caller.

use std::sync::Arc;

use telemetry::StorageEvent;

use super::event_order::{Binding, EventOrder};
use crate::corekv::{CommitHook, Committed, Ended, TxnCallbacks};

/// What one commit registered for after its outcome: its events and hooks,
/// bound to the stream, and its other callbacks.
pub(super) struct PostCommit {
    binding: Option<Binding>,
    hooks: Vec<CommitHook>,
    committed: Ended,
    aborted: Ended,
}

impl PostCommit {
    /// Binds the ordered work of `callbacks` to `txn`, which regolith
    /// commits next.
    pub(super) fn bind(
        order: &Arc<EventOrder>,
        txn: &mut regolith::Transaction,
        callbacks: TxnCallbacks,
    ) -> Self {
        let (
            Committed {
                events,
                hooks,
                then,
            },
            aborted,
        ) = callbacks.into_ends();
        let binding = (!events.is_empty() || !hooks.is_empty())
            .then(|| order.bind(txn, events, !hooks.is_empty()));
        Self {
            binding,
            hooks,
            committed: then,
            aborted,
        }
    }

    /// The commit is durable: its hooks run (a failure is reported, never
    /// returned), its events go out in receipt order, and its other commit
    /// callbacks run.
    pub(super) async fn committed(self, pool: Option<&workers::Pool>) {
        #[cfg(target_arch = "wasm32")]
        let _ = pool;

        // With no ordered work there is no publication wait to outlive its
        // caller, so keep the ordinary callback path in place.
        if self.binding.is_none() {
            self.committed.run().await;
            return;
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Once durable, an ordered publication may wait behind another
            // commit. Keep this future owned by a worker if its caller drops
            // the completion while it waits.
            let owner = Arc::new(regolith::sync::ArrayQueue::new(1));
            let _ = owner.push(self);
            let task_owner = Arc::clone(&owner);
            let completion = workers::spawn(pool, "storage::post_commit", move || async move {
                let work = task_owner.pop();
                if let Some(work) = work {
                    let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(
                        work.committed_ordered(),
                    ))
                    .await;
                    if let Err(panic) = outcome {
                        report(
                            StorageEvent::PostCommitCallbackPanicked,
                            &super::event_order::panic_message(panic.as_ref()),
                        );
                    }
                }
            });
            match completion {
                Ok(completion) => {
                    if let Err(error) = completion.await {
                        tracing::error!(%error, "durable post-commit owner did not finish");
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "could not start durable post-commit owner; running inline");
                    let work = owner.pop();
                    if let Some(work) = work {
                        let outcome = futures::FutureExt::catch_unwind(
                            std::panic::AssertUnwindSafe(work.committed_ordered()),
                        )
                        .await;
                        if let Err(panic) = outcome {
                            report(
                                StorageEvent::PostCommitCallbackPanicked,
                                &super::event_order::panic_message(panic.as_ref()),
                            );
                        }
                    }
                }
            }
        }

        #[cfg(target_arch = "wasm32")]
        {
            if workers::on_worker() {
                // On the page worker the completion itself is polled by the
                // owner's task, which keeps ordered callbacks alive if its
                // original caller is cancelled.
                match workers::spawn_local(self.committed_ordered()) {
                    Ok(completion) => {
                        if let Err(error) = completion.await {
                            tracing::error!(%error, "durable post-commit owner did not finish");
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "could not start durable post-commit owner")
                    }
                }
            } else {
                self.committed_ordered().await;
            }
        }
    }

    async fn committed_ordered(self) {
        let publication = self.binding.as_ref().map(Binding::publication);
        let running = self.binding.and_then(|binding| binding.hand_over(true));
        if let Some(running) = running {
            for hook in self.hooks {
                run_hook(hook()).await;
            }
            running.done();
        }
        if let Some(publication) = publication {
            publication.wait().await;
        }
        self.committed.run().await;
    }

    /// The commit failed: nothing it registered for after a commit runs, and
    /// its abort callbacks do.
    pub(super) async fn failed(self) {
        if let Some(binding) = self.binding {
            binding.hand_over(false);
        }
        self.aborted.run().await;
    }
}

pub(super) fn report(kind: StorageEvent, detail: &dyn std::fmt::Display) {
    tracing::warn!(
        kind = kind.label(),
        %detail,
        "Post-commit work failed after its commit was durable; the commit stands"
    );
    telemetry::record_storage_event(kind, 0);
}

/// Runs one hook to its end. A failure or a panic in it is reported, never
/// returned: its commit is durable by now.
pub(super) async fn run_hook<F>(hook: F)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    #[cfg(not(target_arch = "wasm32"))]
    let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(hook)).await;
    // A browser build aborts on a panic, so there is nothing to catch.
    #[cfg(target_arch = "wasm32")]
    let outcome: Result<Result<(), String>, Box<dyn std::any::Any + Send>> = Ok(hook.await);
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => report(StorageEvent::PostCommitHookFailed, &error),
        Err(panic) => report(
            StorageEvent::PostCommitHookPanicked,
            &super::event_order::panic_message(panic.as_ref()),
        ),
    }
}

#[cfg(test)]
mod post_commit_tests {
    use std::sync::Arc;

    use crate::backends::RegolithStore;
    use crate::corekv::{
        AppendLog, CommitHook, OnCommit, RetryAttemptFn, RetryAttemptFuture, RetryAttemptValue,
        RetryOptions, RetryTxn, Store, TxnCallback, TxnCallbacks, Writer,
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

        fn event(&self, value: u64) -> TxnCallback {
            let out = self.clone();
            Box::new(move || out.push(value))
        }

        fn get(&self) -> Vec<u64> {
            self.0.load().clone()
        }
    }

    fn failing_hook() -> CommitHook {
        Box::new(|| Box::pin(async { Err("the registration was refused".to_string()) }))
    }

    #[tokio::test]
    async fn commit_completion_waits_until_its_ordered_events_are_visible() {
        let db =
            regolith::OptimisticTransactionDb::open("commit-events", regolith::Options::memory())
                .unwrap();
        let order = Arc::new(super::EventOrder::default());
        let out = Out::default();
        let mut earlier = db.begin(&regolith::TxnOptions::new());
        earlier.put(b"earlier", b"v").unwrap();
        let held = order.bind(&mut earlier, vec![out.event(1)], false);
        earlier.commit().unwrap();

        let mut later = db.begin(&regolith::TxnOptions::new());
        later.put(b"later", b"v").unwrap();
        let mut callbacks = TxnCallbacks::default();
        callbacks
            .on_commit(OnCommit::Ordered(out.event(2)))
            .unwrap();
        let work = super::PostCommit::bind(&order, &mut later, callbacks);
        later.commit().unwrap();
        let mut completion = std::pin::pin!(work.committed_ordered());
        assert!(futures::poll!(&mut completion).is_pending());
        assert!(out.get().is_empty());
        held.hand_over(true);
        completion.await;
        assert_eq!(out.get(), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_late_native_outcome_wakes_publication_after_its_owner_was_cancelled() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct Wakes(AtomicUsize);
        impl futures::task::ArcWake for Wakes {
            fn wake_by_ref(wakes: &Arc<Self>) {
                wakes.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let db = regolith::OptimisticTransactionDb::open(
            "cancelled-publication",
            regolith::Options::memory(),
        )
        .unwrap();
        let order = Arc::new(super::EventOrder::default());
        let out = Out::default();
        let mut detached = db.begin(&regolith::TxnOptions::new());
        detached.put(b"detached", b"v").unwrap();
        let cancelled = order.bind(&mut detached, vec![out.event(2)], false);
        drop(cancelled);

        let mut later = db.begin(&regolith::TxnOptions::new());
        later.put(b"later", b"v").unwrap();
        let mut callbacks = TxnCallbacks::default();
        callbacks
            .on_commit(OnCommit::Ordered(out.event(1)))
            .unwrap();
        let work = super::PostCommit::bind(&order, &mut later, callbacks);
        later.commit().unwrap();
        let mut completion = std::pin::pin!(work.committed_ordered());
        let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
        let waker = futures::task::waker(Arc::clone(&wakes));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(std::future::Future::poll(completion.as_mut(), &mut context).is_pending());
        let before = wakes.0.load(Ordering::SeqCst);
        detached.commit().unwrap();
        assert!(wakes.0.load(Ordering::SeqCst) > before);
        completion.await;
        assert_eq!(out.get(), vec![1, 2]);
    }

    #[tokio::test]
    async fn cancellation_while_waiting_for_publication_keeps_committed_callbacks_owned() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        let pool =
            workers::Pool::start(crate::with_worker_io(workers::Config::unplaced(2))).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let store = RegolithStore::open_with_options(
            dir.path(),
            crate::RegolithStoreOptions::default().placed_on(&pool),
        )
        .unwrap();
        let out = Out::default();
        let first_entered = Arc::new(Signal::new());
        let release_first = Arc::new(Signal::new());

        let first_store = store.clone();
        let first_out = out.clone();
        let first_entered_task = Arc::clone(&first_entered);
        let first_release_task = Arc::clone(&release_first);
        let first = tokio::spawn(async move {
            let mut txn = first_store.new_txn(false).await.unwrap();
            txn.set(b"first", b"durable").await.unwrap();
            txn.on_commit(OnCommit::OrderedAsync(Box::new(move || {
                Box::pin(async move {
                    first_entered_task.set();
                    first_release_task.wait().await;
                    Ok(())
                })
            })))
            .unwrap();
            txn.on_commit(OnCommit::Ordered(first_out.event(1)))
                .unwrap();
            txn.commit().await.unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), first_entered.wait())
            .await
            .unwrap();

        let callbacks = Arc::new(AtomicUsize::new(0));
        let second_entered = Arc::new(Signal::new());
        let second_done = Arc::new(Signal::new());
        let second_store = store.clone();
        let second_out = out.clone();
        let second_callbacks = Arc::clone(&callbacks);
        let second_entered_task = Arc::clone(&second_entered);
        let second_done_task = Arc::clone(&second_done);
        let second = tokio::spawn(async move {
            let mut txn = second_store.new_txn(false).await.unwrap();
            txn.set(b"second", b"durable").await.unwrap();
            txn.on_commit(OnCommit::OrderedAsync(Box::new(move || {
                Box::pin(async move {
                    second_entered_task.set();
                    Ok(())
                })
            })))
            .unwrap();
            txn.on_commit(OnCommit::Ordered(second_out.event(2)))
                .unwrap();
            txn.on_commit(OnCommit::Sync(Box::new(move || {
                second_callbacks.fetch_add(1, Ordering::SeqCst);
                second_done_task.set();
            })))
            .unwrap();
            txn.commit().await.unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), second_entered.wait())
            .await
            .unwrap();
        assert!(out.get().is_empty());

        second.abort();
        let _ = second.await;
        release_first.set();
        first.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), second_done.wait())
            .await
            .unwrap();

        assert_eq!(out.get(), vec![1, 2]);
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        workers::block_on(store.close()).unwrap().unwrap();
        pool.shutdown_blocking().unwrap();
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
        txn.on_commit(OnCommit::OrderedAsync(failing_hook()))
            .unwrap();
        txn.on_commit(OnCommit::Ordered(out.event(1))).unwrap();
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
                let mut callbacks = TxnCallbacks::default();
                if self.failing {
                    callbacks.on_commit(OnCommit::OrderedAsync(Box::new(|| {
                        Box::pin(async { Err("the registration was refused".to_string()) })
                    })))?;
                }
                for value in &self.events {
                    callbacks.on_commit(OnCommit::Ordered(self.out.event(*value)))?;
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
        txn.on_commit(OnCommit::Ordered(out.event(1))).unwrap();
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
        let log = AppendLog::new(b"s/arrival/c1/head".to_vec(), b"s/arrival/c1/row/".to_vec());
        for value in [1, 2] {
            // The second append names a once key that already holds a
            // position, so regolith appends nothing for it.
            let mut txn = store.new_txn(false).await.unwrap();
            txn.append(&log, b"doc-1", Some(b"s/arrival/c1/doc/doc-1"))
                .await
                .unwrap();
            txn.on_commit(OnCommit::Ordered(out.event(value))).unwrap();
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
                let mut callbacks = TxnCallbacks::default();
                let entered = Arc::clone(&self.entered);
                let release = Arc::clone(&self.release);
                callbacks.on_commit(OnCommit::OrderedAsync(Box::new(move || {
                    Box::pin(async move {
                        entered.set();
                        release.wait().await;
                        Ok(())
                    })
                })))?;
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
        txn.on_commit(OnCommit::Ordered(Box::new(|| {
            panic!("a subscriber panicked")
        })))
        .unwrap();
        let later = tokio::spawn(async move { txn.commit().await });
        release.set();
        held.await.unwrap().unwrap();
        later.await.unwrap().unwrap();
        assert!(telemetry::storage_event_count(StorageEvent::PostCommitEventPanicked) > before);
    }
}
