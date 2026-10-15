//! Post-commit work that failed after its commit was durable.
//!
//! A durable commit's outcome stands. A hook that fails, panics or is
//! cancelled, an event that panics, and a regolith callback that panics
//! after its outcome are each reported here, as one warn line and one count
//! of `defradb.storage.events` by kind, and never turned into a failed write
//! or raised in another caller.

use telemetry::StorageEvent;

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
        AppendLog, AsyncOrderedTxnCallback, OrderedRetryCallback, OrderedTxnCallback,
        RetryAttemptFn, RetryAttemptFuture, Writer,
    };
    use crate::corekv::{RetryAttemptValue, RetryOptions, Store};
    use crate::corekv::{RetryCallbacks, RetryTxn};
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
        let log = AppendLog::new(b"s/arrival/c1/head".to_vec(), b"s/arrival/c1/row/".to_vec());
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
