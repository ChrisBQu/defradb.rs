//! Callbacks a transaction runs when it ends, and the one registry that holds
//! them.
//!
//! A transaction ends once: it commits, or it ends without committing (its
//! commit fails, or it is discarded or dropped). What was registered with
//! [`Txn::on_commit`](super::Txn::on_commit) runs only after a commit, what
//! was registered with [`Txn::on_abort`](super::Txn::on_abort) only after any
//! other end; each callback runs once, and the other end's are dropped unrun.
//!
//! After a commit, in this order: the hooks ([`OnCommit::OrderedAsync`]) are
//! awaited and the events ([`OnCommit::Ordered`]) handed to the stream that
//! sends every commit's events out in commit-receipt order; then the async
//! callbacks are awaited and the sync ones run. After any other end the
//! async callbacks come first too: awaited after a failed commit, spawned
//! after a discard or a drop, whose caller cannot await. Each kind keeps its
//! registration order.
//!
//! Every callback type is one shape of [`CallOnce`], declared once for every
//! target: `Send` on native targets, not required to be in the
//! single-threaded browser ([`MaybeSend`]).

use std::future::Future;

use defra_core::thread_bounds::MaybeBoxFuture;
use regolith::sync::SegQueue;

use super::errors::{Error, Result};
use super::traits::MaybeSend;

/// A callback that runs once and returns `T`.
pub trait CallOnce<T>: FnOnce() -> T + MaybeSend + 'static {}

impl<F, T> CallOnce<T> for F where F: FnOnce() -> T + MaybeSend + 'static {}

/// A callback run once, synchronously.
pub type TxnCallback = Box<dyn CallOnce<()>>;

/// A callback awaited once.
pub type AsyncTxnCallback = Box<dyn CallOnce<MaybeBoxFuture<'static, ()>>>;

/// A hook awaited once its commit is durable, before that commit's events go
/// out. Its error is reported, never returned: the commit stands.
pub type CommitHook = Box<dyn CallOnce<MaybeBoxFuture<'static, std::result::Result<(), String>>>>;

/// What runs after a commit, and how.
pub enum OnCommit {
    /// Run once the commit is durable.
    Sync(TxnCallback),
    /// Awaited once the commit is durable.
    Async(AsyncTxnCallback),
    /// An event: sent out with every commit's events, in commit-receipt
    /// order.
    Ordered(TxnCallback),
    /// A hook: awaited before this commit's events go out, which holds back
    /// the events of every later commit until it finishes.
    OrderedAsync(CommitHook),
}

impl OnCommit {
    /// Whether it is ordered by the commit's receipt.
    fn is_ordered(&self) -> bool {
        matches!(self, Self::Ordered(_) | Self::OrderedAsync(_))
    }
}

/// What runs after a transaction ends without committing.
pub enum OnAbort {
    /// Run once the transaction has ended.
    Sync(TxnCallback),
    /// Awaited once a failed commit returns, or spawned after a discard or a
    /// drop.
    Async(AsyncTxnCallback),
}

/// The callbacks one transaction registered, in registration order.
///
/// The queue is what makes the registry `Sync` without a lock (an attempt
/// view holding it is shared by reference across awaits), and it is built on
/// the first registration, so a transaction that registers nothing allocates
/// nothing.
#[derive(Default)]
pub struct TxnCallbacks {
    read_only: bool,
    entries: Option<Box<SegQueue<Entry>>>,
}

enum Entry {
    Commit(OnCommit),
    Abort(OnAbort),
}

/// How many callbacks a registry held when a savepoint was set.
#[derive(Clone, Copy, Debug, Default)]
pub struct CallbackMark(usize);

impl TxnCallbacks {
    /// A registry for a transaction that is `read_only` or not. A read-only
    /// one refuses ordered work: it never gets a commit receipt to order it
    /// by.
    pub fn new(read_only: bool) -> Self {
        Self {
            read_only,
            entries: None,
        }
    }

    /// Register `callback` to run once if the transaction commits.
    pub fn on_commit(&mut self, callback: OnCommit) -> Result<()> {
        if self.read_only && callback.is_ordered() {
            return Err(Error::ReadOnlyTxn);
        }
        self.push(Entry::Commit(callback));
        Ok(())
    }

    /// Register `callback` to run once if the transaction ends without
    /// committing.
    pub fn on_abort(&mut self, callback: OnAbort) {
        self.push(Entry::Abort(callback));
    }

    fn push(&mut self, entry: Entry) {
        self.entries.get_or_insert_with(Box::default).push(entry);
    }

    /// The callbacks registered so far, of every kind.
    pub fn len(&self) -> usize {
        self.entries.as_ref().map_or(0, |entries| entries.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Where the registry stands, for a savepoint to return to.
    pub fn mark(&self) -> CallbackMark {
        CallbackMark(self.len())
    }

    /// Drop the callbacks registered after `mark`.
    pub fn truncate(&mut self, mark: CallbackMark) {
        let Some(entries) = &self.entries else {
            return;
        };
        let mut kept = Vec::with_capacity(mark.0.min(entries.len()));
        while kept.len() < mark.0 {
            match entries.pop() {
                Some(entry) => kept.push(entry),
                None => break,
            }
        }
        while entries.pop().is_some() {}
        for entry in kept {
            entries.push(entry);
        }
    }

    /// Register what `other` holds after what this registry holds.
    pub fn append(&mut self, other: Self) {
        let Some(other) = other.entries else {
            return;
        };
        while let Some(entry) = other.pop() {
            self.push(entry);
        }
    }

    /// What runs after a commit, and what runs after any other end.
    pub fn into_ends(self) -> (Committed, Ended) {
        let mut committed = Committed::default();
        let mut aborted = Ended::default();
        let Some(entries) = self.entries else {
            return (committed, aborted);
        };
        while let Some(entry) = entries.pop() {
            match entry {
                Entry::Commit(OnCommit::Sync(callback)) => committed.then.sync.push(callback),
                Entry::Commit(OnCommit::Async(callback)) => {
                    committed.then.asynchronous.push(callback)
                }
                Entry::Commit(OnCommit::Ordered(event)) => committed.events.push(event),
                Entry::Commit(OnCommit::OrderedAsync(hook)) => committed.hooks.push(hook),
                Entry::Abort(OnAbort::Sync(callback)) => aborted.sync.push(callback),
                Entry::Abort(OnAbort::Async(callback)) => aborted.asynchronous.push(callback),
            }
        }
        (committed, aborted)
    }
}

/// What runs after a commit, each kind in registration order.
#[derive(Default)]
pub struct Committed {
    /// Sent out in commit-receipt order.
    pub events: Vec<TxnCallback>,
    /// Awaited before the events go out.
    pub hooks: Vec<CommitHook>,
    /// Run once the hooks finished.
    pub then: Ended,
}

/// The callbacks of one end that nothing orders: the async ones run before
/// the sync ones, each kind in registration order.
#[derive(Default)]
pub struct Ended {
    asynchronous: Vec<AsyncTxnCallback>,
    sync: Vec<TxnCallback>,
}

impl Ended {
    /// Await the async callbacks, then run the sync ones. A panic in one
    /// unwinds to the caller, and the callbacks after it do not run.
    pub async fn run(self) {
        for callback in self.asynchronous {
            callback().await;
        }
        for callback in self.sync {
            callback();
        }
    }

    /// For an end whose caller cannot await, a discard or a drop: the async
    /// callbacks are spawned, one task running them in order, then the sync
    /// ones run. Without a runtime there is nowhere to spawn them, and that
    /// is said out loud rather than dropped quietly.
    pub fn run_now(self) {
        if !self.asynchronous.is_empty() {
            spawn_in_order(self.asynchronous);
        }
        for callback in self.sync {
            callback();
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_in_order(callbacks: Vec<AsyncTxnCallback>) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move {
                for callback in callbacks {
                    callback().await;
                }
            });
        }
        Err(_) => tracing::warn!(
            count = callbacks.len(),
            "async abort callbacks dropped: the transaction ended outside a tokio runtime"
        ),
    }
}

#[cfg(target_arch = "wasm32")]
fn spawn_in_order(callbacks: Vec<AsyncTxnCallback>) {
    tracing::warn!(
        count = callbacks.len(),
        "async abort callbacks dropped: no runtime to spawn them on"
    );
}

/// Helper function to create a simple sync callback from a closure.
///
/// # Example
///
/// ```ignore
/// txn.on_commit(OnCommit::Sync(make_callback(|| println!("Success!"))))?;
/// ```
pub fn make_callback<F>(f: F) -> TxnCallback
where
    F: FnOnce() + MaybeSend + 'static,
{
    Box::new(f)
}

/// Helper function to create an async callback from a closure.
///
/// # Example
///
/// ```ignore
/// txn.on_commit(OnCommit::Async(make_async_callback(|| async {
///     println!("Async success!");
/// })))?;
/// ```
pub fn make_async_callback<F, Fut>(f: F) -> AsyncTxnCallback
where
    F: FnOnce() -> Fut + MaybeSend + 'static,
    Fut: Future<Output = ()> + MaybeSend + 'static,
{
    Box::new(move || Box::pin(f()))
}

#[cfg(test)]
mod tests;
