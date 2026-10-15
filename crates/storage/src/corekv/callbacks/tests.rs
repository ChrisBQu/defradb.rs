use std::sync::Arc;

use regolith::sync::SegQueue;

use super::*;

/// What the callbacks of a test saw, in the order they ran.
#[derive(Clone, Default)]
struct Seen(Arc<SegQueue<&'static str>>);

impl Seen {
    fn sync(&self, name: &'static str) -> TxnCallback {
        let seen = self.clone();
        Box::new(move || seen.0.push(name))
    }

    fn future(&self, name: &'static str) -> AsyncTxnCallback {
        let seen = self.clone();
        Box::new(move || Box::pin(async move { seen.0.push(name) }))
    }

    fn hook(&self, name: &'static str) -> CommitHook {
        let seen = self.clone();
        Box::new(move || {
            Box::pin(async move {
                seen.0.push(name);
                Ok(())
            })
        })
    }

    fn take(&self) -> Vec<&'static str> {
        std::iter::from_fn(|| self.0.pop()).collect()
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn a_registry_is_shareable_without_a_lock() {
    fn shareable<T>()
    where
        T: Send + Sync,
    {
    }
    shareable::<TxnCallbacks>();
}

#[test]
fn a_registry_that_registers_nothing_allocates_nothing() {
    let callbacks = TxnCallbacks::default();
    assert!(callbacks.entries.is_none());
    assert!(callbacks.is_empty());
    let (committed, aborted) = callbacks.into_ends();
    assert!(committed.events.is_empty() && committed.hooks.is_empty());
    assert!(aborted.sync.is_empty() && aborted.asynchronous.is_empty());
}

/// Each end gets only its own callbacks, every kind in registration order,
/// through a savepoint and an append.
#[tokio::test]
async fn each_end_runs_its_own_callbacks_async_first_in_registration_order() {
    let seen = Seen::default();
    let mut callbacks = TxnCallbacks::default();
    callbacks
        .on_commit(OnCommit::Sync(seen.sync("sync-1")))
        .unwrap();
    callbacks.on_abort(OnAbort::Sync(seen.sync("abort-1")));
    let mark = callbacks.mark();
    callbacks
        .on_commit(OnCommit::Sync(seen.sync("rolled back")))
        .unwrap();
    callbacks.on_abort(OnAbort::Sync(seen.sync("rolled back")));
    callbacks.truncate(mark);
    assert_eq!(callbacks.len(), 2);
    callbacks
        .on_commit(OnCommit::Async(seen.future("async-1")))
        .unwrap();
    callbacks
        .on_commit(OnCommit::Ordered(seen.sync("event-1")))
        .unwrap();
    callbacks
        .on_commit(OnCommit::OrderedAsync(seen.hook("hook-1")))
        .unwrap();
    callbacks.on_abort(OnAbort::Async(seen.future("abort-async-1")));
    let mut more = TxnCallbacks::default();
    more.on_commit(OnCommit::Sync(seen.sync("sync-2"))).unwrap();
    more.on_commit(OnCommit::Async(seen.future("async-2")))
        .unwrap();
    more.on_commit(OnCommit::Ordered(seen.sync("event-2")))
        .unwrap();
    more.on_abort(OnAbort::Sync(seen.sync("abort-2")));
    callbacks.append(more);
    callbacks.append(TxnCallbacks::default());

    let (committed, aborted) = callbacks.into_ends();
    for event in committed.events {
        event();
    }
    for hook in committed.hooks {
        hook().await.unwrap();
    }
    assert_eq!(seen.take(), ["event-1", "event-2", "hook-1"]);
    committed.then.run().await;
    assert_eq!(seen.take(), ["async-1", "async-2", "sync-1", "sync-2"]);
    aborted.run().await;
    assert_eq!(seen.take(), ["abort-async-1", "abort-1", "abort-2"]);
}

/// A read-only transaction never gets a commit receipt, so it refuses what is
/// ordered by one and takes everything else.
#[test]
fn a_read_only_registry_refuses_only_ordered_work() {
    let seen = Seen::default();
    let mut callbacks = TxnCallbacks::new(true);
    assert!(matches!(
        callbacks.on_commit(OnCommit::Ordered(seen.sync("event"))),
        Err(Error::ReadOnlyTxn)
    ));
    assert!(matches!(
        callbacks.on_commit(OnCommit::OrderedAsync(seen.hook("hook"))),
        Err(Error::ReadOnlyTxn)
    ));
    callbacks
        .on_commit(OnCommit::Sync(seen.sync("sync")))
        .unwrap();
    callbacks
        .on_commit(OnCommit::Async(seen.future("async")))
        .unwrap();
    callbacks.on_abort(OnAbort::Sync(seen.sync("abort")));
    assert_eq!(callbacks.len(), 3);
}

/// An end that cannot await spawns its async callbacks before it runs the
/// sync ones, which may wait for them.
#[cfg(not(target_arch = "wasm32"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_end_that_cannot_await_spawns_its_async_callbacks_first() {
    let (started, wait) = std::sync::mpsc::channel();
    let mut callbacks = TxnCallbacks::default();
    callbacks.on_abort(OnAbort::Async(Box::new(move || {
        Box::pin(async move { started.send(()).unwrap() })
    })));
    callbacks.on_abort(OnAbort::Sync(Box::new(move || {
        wait.recv_timeout(std::time::Duration::from_secs(5))
            .expect("the async callback was spawned before the sync one ran");
    })));
    let (_, aborted) = callbacks.into_ends();
    aborted.run_now();
}

#[test]
fn the_helpers_build_each_shape() {
    let seen = Seen::default();
    let recorded = seen.clone();
    make_callback(move || recorded.0.push("sync"))();
    assert_eq!(seen.take(), ["sync"]);
    let _future: AsyncTxnCallback = make_async_callback(|| async {});
}
