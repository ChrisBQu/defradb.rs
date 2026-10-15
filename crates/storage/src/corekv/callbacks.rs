//! Callbacks a transaction runs when it ends: the callback types, the
//! bundle of receipt-ordered callbacks a mutation attempt collects, and the
//! success callbacks an engine-owned retry attempt hands its store.

use std::future::Future;
use std::pin::Pin;

use defra_core::thread_bounds::MaybeBoxFuture;
use regolith::sync::SegQueue;

use super::traits::MaybeSend;

/// A callback that runs once and returns `T`: `Send` on native targets, not
/// required to be in the single-threaded browser ([`MaybeSend`]). Each
/// callback type below is one shape of it, declared once for every target.
pub trait CallOnce<T>: FnOnce() -> T + MaybeSend + 'static {}

impl<F, T> CallOnce<T> for F where F: FnOnce() -> T + MaybeSend + 'static {}

/// A callback run synchronously when a transaction ends.
pub type TxnCallback = Box<dyn CallOnce<()>>;

#[cfg(not(target_arch = "wasm32"))]
pub type OrderedTxnCallback = std::sync::Arc<dyn Fn() + Send + Sync + 'static>;

#[cfg(target_arch = "wasm32")]
pub type OrderedTxnCallback = std::sync::Arc<dyn Fn() + 'static>;

#[cfg(not(target_arch = "wasm32"))]
pub type AsyncOrderedTxnCallback = std::sync::Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>
        + Send
        + Sync
        + 'static,
>;

/// Receipt-ordered callbacks collected while evaluating a mutation attempt.
/// A retrying caller can keep the bundle with its attempt and register only
/// the winning attempt on the transaction that receives the commit receipt.
#[derive(Default)]
pub struct OrderedTxnCallbacks {
    sync: Vec<OrderedTxnCallback>,
    asynchronous: Vec<AsyncOrderedTxnCallback>,
}

impl OrderedTxnCallbacks {
    pub fn push_sync(&mut self, callback: OrderedTxnCallback) {
        self.sync.push(callback);
    }

    pub fn push_async(&mut self, callback: AsyncOrderedTxnCallback) {
        self.asynchronous.push(callback);
    }

    pub fn append(&mut self, mut other: Self) {
        self.sync.append(&mut other.sync);
        self.asynchronous.append(&mut other.asynchronous);
    }

    pub fn is_empty(&self) -> bool {
        self.sync.is_empty() && self.asynchronous.is_empty()
    }

    pub fn into_parts(self) -> (Vec<OrderedTxnCallback>, Vec<AsyncOrderedTxnCallback>) {
        (self.sync, self.asynchronous)
    }
}
#[cfg(target_arch = "wasm32")]
pub type AsyncOrderedTxnCallback = std::sync::Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = std::result::Result<(), String>>>> + 'static,
>;

/// A callback awaited when a transaction ends.
pub type AsyncTxnCallback = Box<dyn CallOnce<MaybeBoxFuture<'static, ()>>>;

/// Success callbacks collected by datastore's attempt facade. The store runs
/// them only after the winning native commit has produced its receipt.
///
/// Each kind is a FIFO queue owned by the bundle: a queue is what makes the
/// bundle `Sync` without a lock (the attempt view holding it is shared by
/// reference across awaits), and the queues are built on the first
/// registration, so an attempt that registers nothing allocates nothing.
#[derive(Default)]
pub struct RetryCallbacks {
    queues: Option<Box<RetryQueues>>,
}

#[derive(Default)]
struct RetryQueues {
    sync: SegQueue<TxnCallback>,
    asynchronous: SegQueue<AsyncTxnCallback>,
    ordered: SegQueue<OrderedRetryCallback>,
    ordered_async: SegQueue<AsyncOrderedRetryCallback>,
}

/// The callbacks of a [`RetryCallbacks`] bundle, by kind, in registration
/// order.
pub struct RetryCallbackParts {
    pub sync: Vec<TxnCallback>,
    pub asynchronous: Vec<AsyncTxnCallback>,
    pub ordered: Vec<OrderedRetryCallback>,
    pub ordered_async: Vec<AsyncOrderedRetryCallback>,
}

/// Lengths of retry-success callback queues at a transaction savepoint.
#[derive(Clone, Copy, Debug, Default)]
pub struct RetryCallbackCounts {
    sync: usize,
    asynchronous: usize,
    ordered: usize,
    ordered_async: usize,
}

/// An event a retry attempt registers, dispatched once in commit-receipt
/// order.
pub type OrderedRetryCallback = TxnCallback;

/// A hook a retry attempt registers, awaited once its commit is durable and
/// before its events go out; its error is reported, never returned.
pub type AsyncOrderedRetryCallback =
    Box<dyn CallOnce<MaybeBoxFuture<'static, std::result::Result<(), String>>>>;

/// Keep the first `keep` entries of `queue`, in order, and drop the rest.
pub(crate) fn truncate_queue<T>(queue: &SegQueue<T>, keep: usize)
where
    T: 'static,
{
    let mut retained = Vec::with_capacity(keep.min(queue.len()));
    for _ in 0..keep {
        match queue.pop() {
            Some(entry) => retained.push(entry),
            None => break,
        }
    }
    while queue.pop().is_some() {}
    for entry in retained {
        queue.push(entry);
    }
}

fn drain_queue<T>(queue: SegQueue<T>) -> Vec<T>
where
    T: 'static,
{
    let mut drained = Vec::with_capacity(queue.len());
    while let Some(entry) = queue.pop() {
        drained.push(entry);
    }
    drained
}

impl RetryCallbacks {
    fn queues(&mut self) -> &RetryQueues {
        self.queues.get_or_insert_with(Box::default)
    }

    pub fn counts(&self) -> RetryCallbackCounts {
        let Some(queues) = &self.queues else {
            return RetryCallbackCounts::default();
        };
        RetryCallbackCounts {
            sync: queues.sync.len(),
            asynchronous: queues.asynchronous.len(),
            ordered: queues.ordered.len(),
            ordered_async: queues.ordered_async.len(),
        }
    }

    pub fn truncate(&mut self, counts: RetryCallbackCounts) {
        let Some(queues) = &self.queues else {
            return;
        };
        truncate_queue(&queues.sync, counts.sync);
        truncate_queue(&queues.asynchronous, counts.asynchronous);
        truncate_queue(&queues.ordered, counts.ordered);
        truncate_queue(&queues.ordered_async, counts.ordered_async);
    }

    pub fn has_ordered_callbacks(&self) -> bool {
        self.queues
            .as_ref()
            .is_some_and(|queues| !queues.ordered.is_empty() || !queues.ordered_async.is_empty())
    }

    pub fn append(&mut self, other: Self) {
        let Some(other) = other.queues else {
            return;
        };
        let queues = self.queues();
        for entry in drain_queue(other.sync) {
            queues.sync.push(entry);
        }
        for entry in drain_queue(other.asynchronous) {
            queues.asynchronous.push(entry);
        }
        for entry in drain_queue(other.ordered) {
            queues.ordered.push(entry);
        }
        for entry in drain_queue(other.ordered_async) {
            queues.ordered_async.push(entry);
        }
    }

    pub fn push_sync(&mut self, callback: TxnCallback) {
        self.queues().sync.push(callback);
    }

    pub fn push_async(&mut self, callback: AsyncTxnCallback) {
        self.queues().asynchronous.push(callback);
    }

    pub fn push_ordered(&mut self, callback: OrderedRetryCallback) {
        self.queues().ordered.push(callback);
    }

    pub fn push_ordered_async(&mut self, callback: AsyncOrderedRetryCallback) {
        self.queues().ordered_async.push(callback);
    }

    /// The callbacks by kind, each in registration order.
    pub fn into_parts(self) -> RetryCallbackParts {
        let Some(queues) = self.queues else {
            return RetryCallbackParts {
                sync: Vec::new(),
                asynchronous: Vec::new(),
                ordered: Vec::new(),
                ordered_async: Vec::new(),
            };
        };
        let RetryQueues {
            sync,
            asynchronous,
            ordered,
            ordered_async,
        } = *queues;
        RetryCallbackParts {
            sync: drain_queue(sync),
            asynchronous: drain_queue(asynchronous),
            ordered: drain_queue(ordered),
            ordered_async: drain_queue(ordered_async),
        }
    }
}

/// Helper function to create a simple sync callback from a closure.
///
/// # Example
///
/// ```ignore
/// txn.on_success(make_callback(|| println!("Success!")));
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
/// txn.on_success_async(make_async_callback(|| async {
///     println!("Async success!");
/// }));
/// ```
pub fn make_async_callback<F, Fut>(f: F) -> AsyncTxnCallback
where
    F: FnOnce() -> Fut + MaybeSend + 'static,
    Fut: Future<Output = ()> + MaybeSend + 'static,
{
    Box::new(move || Box::pin(f()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_callback() {
        let _callback = make_callback(|| {
            println!("Test callback");
        });
        // Just ensure it compiles and has the right type
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn retry_callbacks_are_shareable_without_a_lock() {
        fn shareable<T>()
        where
            T: Send + Sync,
        {
        }
        shareable::<RetryCallbacks>();
    }

    #[test]
    fn retry_callbacks_keep_registration_order_through_append_and_truncate() {
        let seen = std::sync::Arc::new(regolith::sync::SegQueue::new());
        let push = |value: u32| -> TxnCallback {
            let seen = std::sync::Arc::clone(&seen);
            Box::new(move || seen.push(value))
        };
        let mut callbacks = RetryCallbacks::default();
        assert!(callbacks.queues.is_none());
        assert!(!callbacks.has_ordered_callbacks());
        callbacks.push_sync(push(1));
        let mark = callbacks.counts();
        callbacks.push_sync(push(2));
        callbacks.push_ordered(Box::new(|| {}));
        assert!(callbacks.has_ordered_callbacks());
        callbacks.truncate(mark);
        assert!(!callbacks.has_ordered_callbacks());
        let mut more = RetryCallbacks::default();
        more.push_sync(push(3));
        callbacks.append(more);
        callbacks.append(RetryCallbacks::default());
        for callback in callbacks.into_parts().sync {
            callback();
        }
        let order: Vec<u32> = std::iter::from_fn(|| seen.pop()).collect();
        assert_eq!(order, vec![1, 3]);
        assert!(RetryCallbacks::default().into_parts().sync.is_empty());
    }

    #[test]
    fn test_make_async_callback() {
        let _callback = make_async_callback(|| async {
            println!("Test async callback");
        });
        // Just ensure it compiles and has the right type
    }
}
