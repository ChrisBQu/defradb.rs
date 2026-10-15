use async_trait::async_trait;
/// Core KV trait definitions matching the Go corekv package.
///
/// This module defines the complete trait hierarchy for key-value storage:
/// - Reader: Read-only operations
/// - Writer: Write operations
/// - ReaderWriter: Combined read-write
/// - Store: Store that can create transactions
/// - Txn: Transaction interface with ACID guarantees
/// - TxnStore: Store that supports transactions
///
/// All traits use async_trait to support asynchronous operations.
///
/// # WASM Compatibility
///
/// For WASM targets, all `Send + Sync` bounds are removed since WASM is
/// single-threaded. The `#[cfg_attr]` pattern is used to conditionally
/// apply `async_trait(?Send)` for WASM builds.
use bytes::Bytes;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;

/// Sealed trait module preventing external implementations of core storage traits.
pub mod private {
    pub trait Sealed {}

    impl Sealed for Box<dyn super::Txn> {}
    impl Sealed for Box<dyn super::super::iterator::Iterator> {}
}

use super::errors::Result;
use super::iterator::Iterator;
use super::types::IterOptions;

// Conditional Send bounds for callbacks based on target architecture
#[cfg(not(target_arch = "wasm32"))]
/// Callback function type for transaction lifecycle events.
///
/// These callbacks are executed synchronously when transaction events occur.
pub type TxnCallback = Box<dyn FnOnce() + Send + 'static>;

#[cfg(target_arch = "wasm32")]
/// Callback function type for transaction lifecycle events (WASM version).
///
/// These callbacks are executed synchronously when transaction events occur.
/// Lifecycle callbacks do not require `Send` in the single-threaded WASM environment.
pub type TxnCallback = Box<dyn FnOnce() + 'static>;

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

#[cfg(not(target_arch = "wasm32"))]
/// Asynchronous callback function type for transaction lifecycle events.
///
/// These callbacks are executed asynchronously when transaction events occur.
pub type AsyncTxnCallback =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

#[cfg(target_arch = "wasm32")]
/// Asynchronous callback function type for transaction lifecycle events (WASM version).
///
/// These callbacks are executed asynchronously when transaction events occur.
/// No Send bound required in single-threaded WASM environment.
pub type AsyncTxnCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>> + 'static>;

/// A retry attempt's restricted transaction view. It deliberately has no
/// commit, discard, iterator, or callback methods: the store owns the attempt
/// lifetime and commit, and the view cannot escape through this API.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RetryTxn: MaybeSendSync + private::Sealed {
    /// Why the previous commit lost, without exposing user key bytes.
    fn previous_conflict(&self) -> Option<super::errors::TransactionConflict>;

    /// A plain validated read, as `Reader::get`: checked at commit when the
    /// attempt writes, so an identical rewrite of the key is not a conflict
    /// and an attempt that ends up writing nothing commits at its snapshot.
    /// Projected and unvalidated reads go through `begin_attempt_io`.
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    /// Whether `key` holds a value, read as `get` reads it.
    async fn has(&self, key: &[u8]) -> Result<bool>;
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Open a sealed, attempt-scoped Reader/Writer capability for datastore
    /// facades. It has no transaction lifecycle methods and cannot commit.
    #[doc(hidden)]
    fn begin_attempt_io(&mut self) -> Result<Box<dyn RetryAttemptIO>> {
        Err(super::errors::Error::Other(
            "backend does not expose scoped retry IO".into(),
        ))
    }

    /// Return the attempt capability after all namespace views and iterators
    /// have been dropped. Backends reject retained references before commit.
    #[doc(hidden)]
    fn finish_attempt_io(
        &mut self,
        _io: Box<dyn RetryAttemptIO>,
        _callbacks: RetryCallbacks,
    ) -> Result<()> {
        Err(super::errors::Error::Other(
            "backend does not accept scoped retry IO".into(),
        ))
    }
}

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

use regolith::sync::SegQueue;
use std::sync::Arc;

#[cfg(not(target_arch = "wasm32"))]
pub type OrderedRetryCallback = Box<dyn FnOnce() + Send + 'static>;
#[cfg(target_arch = "wasm32")]
pub type OrderedRetryCallback = Box<dyn FnOnce() + 'static>;
#[cfg(not(target_arch = "wasm32"))]
pub type AsyncOrderedRetryCallback = Box<
    dyn FnOnce() -> Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>
        + Send
        + 'static,
>;
#[cfg(target_arch = "wasm32")]
pub type AsyncOrderedRetryCallback =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = std::result::Result<(), String>>>> + 'static>;

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

/// Engine-owned Reader/Writer access scoped to one retry attempt. This
/// intentionally omits `Txn`, callbacks, commit, and discard.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait RetryAttemptIO: ReaderWriter + private::Sealed {
    #[doc(hidden)]
    fn as_any(&self) -> &dyn Any;

    #[doc(hidden)]
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Intercepts a write staged by an engine-owned attempt. `value` is `None` for
/// deletes. The interceptor runs before the underlying write is staged.
#[cfg(not(target_arch = "wasm32"))]
pub type RetryWriteInterceptor = Arc<dyn Fn(&[u8], Option<&[u8]>) -> Result<()> + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub type RetryWriteInterceptor = Arc<dyn Fn(&[u8], Option<&[u8]>) -> Result<()>>;

/// Wrap a retry attempt with a safe write interceptor while preserving the
/// backend-owned attempt IO and commit lifecycle. Useful for decorators that
/// inject failures or observe writes without implementing the sealed IO type.
pub fn intercept_retry_writes(
    attempt: RetryAttempt,
    interceptor: RetryWriteInterceptor,
) -> RetryAttempt {
    Box::new(InterceptingRetryAttempt {
        inner: attempt,
        interceptor,
    })
}

struct InterceptingRetryAttempt {
    inner: RetryAttempt,
    interceptor: RetryWriteInterceptor,
}

impl private::Sealed for InterceptingRetryAttempt {}

#[cfg(not(target_arch = "wasm32"))]
impl RetryAttemptFn for InterceptingRetryAttempt {
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        Box::pin(async move {
            let mut wrapped = InterceptingRetryTxn {
                inner: txn,
                interceptor: Arc::clone(&self.interceptor),
            };
            self.inner.run(&mut wrapped).await
        })
    }
}

#[cfg(target_arch = "wasm32")]
impl RetryAttemptFn for InterceptingRetryAttempt {
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        Box::pin(async move {
            let mut wrapped = InterceptingRetryTxn {
                inner: txn,
                interceptor: Arc::clone(&self.interceptor),
            };
            self.inner.run(&mut wrapped).await
        })
    }
}

struct InterceptingRetryTxn<'a> {
    inner: &'a mut dyn RetryTxn,
    interceptor: RetryWriteInterceptor,
}

impl private::Sealed for InterceptingRetryTxn<'_> {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl RetryTxn for InterceptingRetryTxn<'_> {
    fn previous_conflict(&self) -> Option<super::errors::TransactionConflict> {
        self.inner.previous_conflict()
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }
    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.inner.has(key).await
    }
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        (self.interceptor)(key, Some(value))?;
        self.inner.set(key, value).await
    }
    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        (self.interceptor)(key, None)?;
        self.inner.delete(key).await
    }
    fn begin_attempt_io(&mut self) -> Result<Box<dyn RetryAttemptIO>> {
        Ok(Box::new(InterceptingRetryAttemptIO {
            inner: self.inner.begin_attempt_io()?,
            interceptor: Arc::clone(&self.interceptor),
        }))
    }
    fn finish_attempt_io(
        &mut self,
        io: Box<dyn RetryAttemptIO>,
        callbacks: RetryCallbacks,
    ) -> Result<()> {
        self.inner.finish_attempt_io(io, callbacks)
    }
}

struct InterceptingRetryAttemptIO {
    inner: Box<dyn RetryAttemptIO>,
    interceptor: RetryWriteInterceptor,
}

impl private::Sealed for InterceptingRetryAttemptIO {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for InterceptingRetryAttemptIO {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key).await
    }
    async fn get_parts(&self, key: &[u8], parts: &[u32]) -> Result<Option<Bytes>> {
        self.inner.get_parts(key, parts).await
    }
    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.inner.has(key).await
    }
    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        self.inner.get_size(key).await
    }
    async fn iterator(&self, options: IterOptions) -> Result<Box<dyn Iterator>> {
        self.inner.iterator(options).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for InterceptingRetryAttemptIO {
    async fn merge(&mut self, key: &[u8], operand: &[u8]) -> Result<()> {
        (self.interceptor)(key, Some(operand))?;
        self.inner.merge(key, operand).await
    }
    async fn append(
        &mut self,
        log: &super::types::AppendLog,
        entry: &[u8],
        once_key: Option<&[u8]>,
    ) -> Result<()> {
        (self.interceptor)(log.head_key(), Some(entry))?;
        self.inner.append(log, entry, once_key).await
    }
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        (self.interceptor)(key, Some(value))?;
        self.inner.set(key, value).await
    }
    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        (self.interceptor)(key, None)?;
        self.inner.delete(key).await
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl RetryAttemptIO for InterceptingRetryAttemptIO {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self.inner.into_any()
    }
}

#[cfg(target_arch = "wasm32")]
impl RetryAttemptIO for InterceptingRetryAttemptIO {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self.inner.into_any()
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub type RetryAttemptValue = Box<dyn Any + Send>;
#[cfg(target_arch = "wasm32")]
pub type RetryAttemptValue = Box<dyn Any>;

/// Result of an engine-owned retry operation. Storage failures remain typed;
/// domain aborts cross the engine boundary untouched and are never retried.
pub enum RetryAttemptError {
    Storage(super::errors::Error),
    Exhausted(super::errors::TransactionConflict),
    #[cfg(not(target_arch = "wasm32"))]
    Abort(Box<dyn Any + Send>),
    #[cfg(target_arch = "wasm32")]
    Abort(Box<dyn Any>),
}

impl std::fmt::Debug for RetryAttemptError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
            Self::Exhausted(conflict) => {
                formatter.debug_tuple("Exhausted").field(conflict).finish()
            }
            Self::Abort(_) => formatter.write_str("Abort(<typed domain error>)"),
        }
    }
}

impl From<super::errors::Error> for RetryAttemptError {
    fn from(error: super::errors::Error) -> Self {
        Self::Storage(error)
    }
}

impl RetryAttemptError {
    pub fn into_storage(self) -> super::errors::Error {
        match self {
            Self::Storage(error) => error,
            Self::Exhausted(conflict) => super::errors::Error::TxnRetryExhausted(conflict),
            Self::Abort(_) => super::errors::Error::Other(
                "transaction attempt aborted with a domain error".to_string(),
            ),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn downcast_abort<T: Any + Send>(self) -> std::result::Result<T, Self> {
        match self {
            Self::Abort(error) => match error.downcast::<T>() {
                Ok(value) => Ok(*value),
                Err(error) => Err(Self::Abort(error)),
            },
            storage => Err(storage),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn downcast_abort<T: Any>(self) -> std::result::Result<T, Self> {
        match self {
            Self::Abort(error) => match error.downcast::<T>() {
                Ok(value) => Ok(*value),
                Err(error) => Err(Self::Abort(error)),
            },
            storage => Err(storage),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub type RetryAttemptFuture<'a> = Pin<
    Box<dyn Future<Output = std::result::Result<RetryAttemptValue, RetryAttemptError>> + Send + 'a>,
>;
#[cfg(target_arch = "wasm32")]
pub type RetryAttemptFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<RetryAttemptValue, RetryAttemptError>> + 'a>>;
#[cfg(not(target_arch = "wasm32"))]
pub trait RetryAttemptFn: Send {
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>;
}
#[cfg(target_arch = "wasm32")]
pub trait RetryAttemptFn {
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>;
}

#[cfg(not(target_arch = "wasm32"))]
pub type RetryAttempt = Box<dyn RetryAttemptFn>;
#[cfg(target_arch = "wasm32")]
pub type RetryAttempt = Box<dyn RetryAttemptFn>;

#[cfg(not(target_arch = "wasm32"))]
impl<F> RetryAttemptFn for F
where
    F: Send + for<'a> FnMut(&'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>,
{
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        self(txn)
    }
}

#[cfg(target_arch = "wasm32")]
impl<F> RetryAttemptFn for F
where
    F: for<'a> FnMut(&'a mut dyn RetryTxn) -> RetryAttemptFuture<'a>,
{
    fn run<'a>(&'a mut self, txn: &'a mut dyn RetryTxn) -> RetryAttemptFuture<'a> {
        self(txn)
    }
}

/// Number of total transaction attempts, including the first run.
#[derive(Clone, Copy, Debug)]
pub struct RetryOptions {
    pub max_attempts: u32,
}

impl RetryOptions {
    pub fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
        }
    }
}

pub use defra_core::thread_bounds::{MaybeSend, MaybeSendSync, MaybeSync};

/// Reader trait for read-only key-value operations.
///
/// This trait provides the core read operations: get, has, and iterator.
/// All operations are asynchronous to support various backend implementations.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Reader: MaybeSendSync + private::Sealed {
    /// Retrieve the value associated with a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if the key exists
    /// * `Ok(None)` if the key does not exist
    /// * `Err(Error)` if an error occurred
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;

    /// Read a value while validating only the named logical parts at commit.
    /// Part IDs belong to the value's merge operator. An empty list requests
    /// an unvalidated read; it must not drive an authoritative decision.
    /// Backends without projection support conservatively validate the whole value.
    async fn get_parts(&self, key: &[u8], _parts: &[u32]) -> Result<Option<Bytes>> {
        self.get(key).await
    }

    /// Check if a key exists in the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if the key exists
    /// * `Ok(false)` if the key does not exist
    /// * `Err(Error)` if an error occurred
    ///
    /// # Note
    ///
    /// Performance may vary by backend. Some backends may need to fetch the value
    /// to check existence.
    async fn has(&self, key: &[u8]) -> Result<bool>;

    /// Get the size of a value without reading the entire value.
    ///
    /// This is useful for checking the size of large values without incurring
    /// the cost of reading them into memory.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// * `Ok(Some(size))` if the key exists, with size in bytes
    /// * `Ok(None)` if the key does not exist
    /// * `Err(Error)` if an error occurred
    ///
    /// # Note
    ///
    /// Some backends may need to read the entire value to determine its size.
    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>>;

    /// Create an iterator over key-value pairs.
    ///
    /// The iterator can be configured with various options for filtering
    /// and ordering using `IterOptions`.
    ///
    /// # Arguments
    ///
    /// * `opts` - Configuration options for the iterator
    ///
    /// # Returns
    ///
    /// * `Ok(Iterator)` - A new iterator instance
    /// * `Err(Error)` if the iterator could not be created
    ///
    /// # Note
    ///
    /// The caller is responsible for closing the iterator when done.
    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>>;
}

/// Writer trait for write operations.
///
/// This trait provides the core write operations: set and delete.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Writer: MaybeSendSync + private::Sealed {
    /// Apply an operator-defined operand without reading the value first.
    async fn merge(&mut self, _key: &[u8], _operand: &[u8]) -> Result<()> {
        Err(super::errors::Error::Other(
            "value merges are unsupported by this store".into(),
        ))
    }

    /// Queue an entry whose position becomes visible only after commit.
    async fn append(
        &mut self,
        _log: &super::types::AppendLog,
        _entry: &[u8],
        _once_key: Option<&[u8]>,
    ) -> Result<()> {
        Err(super::errors::Error::Other(
            "commit-ordered append is unsupported by this store".into(),
        ))
    }

    /// Store a key-value pair.
    ///
    /// If the key already exists, its value is overwritten.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store (must not be empty)
    /// * `value` - The value to store
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error::EmptyKey)` if the key is empty
    /// * `Err(Error::ReadOnlyTxn)` if called on a read-only transaction
    /// * `Err(Error)` for other errors
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key from the store.
    ///
    /// If the key doesn't exist, this is a no-op (not an error).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error::EmptyKey)` if the key is empty
    /// * `Err(Error::ReadOnlyTxn)` if called on a read-only transaction
    /// * `Err(Error)` for other errors
    async fn delete(&mut self, key: &[u8]) -> Result<()>;
}

/// ReaderWriter trait combining read and write operations.
///
/// This is a marker trait that combines Reader and Writer.
/// Most stores and transactions implement this trait.
pub trait ReaderWriter: Reader + Writer {}

/// Blanket implementation: any type that implements both Reader and Writer
/// automatically implements ReaderWriter.
impl<T> ReaderWriter for T where T: Reader + Writer {}

/// Store trait for key-value stores that support transactions.
///
/// This trait defines the basic store interface with transaction support.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Store: MaybeSendSync + private::Sealed {
    /// Reserve IDs outside a transaction. Reservations survive aborted mutations.
    /// Store wrappers must forward this capability; database schema and document
    /// creation depend on it, so omission must fail at compile time.
    async fn allocate(&self, key: &[u8], count: u64) -> Result<std::ops::Range<u64>>;

    /// Transaction diagnostics for stores that keep them.
    fn transaction_stats_handle(&self) -> Option<crate::backends::TransactionStatsHandle> {
        None
    }

    /// Create a new transaction.
    ///
    /// # Arguments
    ///
    /// * `readonly` - If true, creates a read-only transaction that cannot
    ///   perform writes. Read-only transactions may have better performance.
    ///
    /// # Returns
    ///
    /// * `Ok(Txn)` - A new transaction instance
    /// * `Err(Error::DBClosed)` if the store is closed
    /// * `Err(Error)` for other errors
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>>;

    /// Run an attempt in a transaction owned and committed by the store.
    /// Only commit conflicts are retried; an error returned by the operation
    /// is returned immediately. The restricted view cannot commit or escape.
    async fn transact_retry(
        &self,
        options: RetryOptions,
        attempt: RetryAttempt,
    ) -> std::result::Result<RetryAttemptValue, RetryAttemptError>;

    /// Close the store and release resources.
    ///
    /// After closing, no further operations can be performed on this store.
    /// Attempting to use a closed store will return `Error::DBClosed`.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error)` if an error occurred during close
    async fn close(&self) -> Result<()>;
}

/// Dropable trait for stores that support bulk deletion.
///
/// This is an optional interface implemented by some stores. It provides a
/// convenient and efficient way of deleting all data in a store.
///
/// # Note
///
/// Concurrent writes will be blocked during `drop_all()`. Depending on the
/// underlying store implementation, concurrent reads may or may not be blocked.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Dropable: Store + private::Sealed {
    /// Delete all data stored in the store.
    ///
    /// This is a destructive operation that removes all key-value pairs.
    /// Use with caution in production environments.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error::DBClosed)` if the store is closed
    /// * `Err(Error)` for other errors
    async fn drop_all(&self) -> Result<()>;
}

/// Transaction trait with ACID guarantees and callback support.
///
/// Transactions provide atomicity, consistency, isolation, and durability (ACID).
/// All operations within a transaction are isolated from other transactions until
/// commit is called.
///
/// # Lifecycle Callbacks
///
/// Transactions support registering callbacks for lifecycle events:
/// - `on_success`: Called when commit succeeds
/// - `on_error`: Called when commit fails
/// - `on_discard`: Called when transaction is discarded
///
/// Each callback type has both sync and async variants.
///
/// # Callback Panic Handling
///
/// Transaction callbacks run directly. In dev/test profiles, a callback panic
/// unwinds through the transaction API. In release builds this workspace uses
/// `panic = "abort"`, so a panicking callback aborts the process.
///
/// # Example
///
/// ```ignore
/// let mut txn = store.new_txn(false).await?;
///
/// txn.on_success(Box::new(|| println!("Transaction committed!")));
///
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?; // Callbacks execute here
/// ```
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Txn: ReaderWriter + private::Sealed {
    /// Commit the transaction, making all changes permanent.
    ///
    /// On success, all on_success callbacks are executed. On failure, all
    /// on_error callbacks are executed.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the transaction committed successfully
    /// * `Err(Error::TxnConflict)` if a conflict occurred (retriable)
    /// * `Err(Error::DiscardedTxn)` if the transaction was already discarded
    /// * `Err(Error)` for other errors
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are not recovered here. In dev/test profiles the panic
    /// unwinds through `commit()`. In release builds this workspace uses
    /// `panic = "abort"`, so a panicking callback aborts the process.
    ///
    /// # Note
    ///
    /// After calling commit (success or failure), the transaction cannot be reused.
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Save the current buffered writes and lifecycle callback queues.
    /// Backends without statement savepoint support fail explicitly.
    fn set_savepoint(&mut self) -> Result<()> {
        Err(crate::Error::Other(
            "transaction savepoints are unsupported by this backend".into(),
        ))
    }

    /// Roll back writes and callbacks staged since the latest savepoint.
    fn rollback_to_savepoint(&mut self) -> Result<()> {
        Err(crate::Error::Other(
            "transaction savepoints are unsupported by this backend".into(),
        ))
    }

    /// Release the latest savepoint, retaining its writes and callbacks.
    fn release_savepoint(&mut self) -> Result<()> {
        Err(crate::Error::Other(
            "transaction savepoints are unsupported by this backend".into(),
        ))
    }

    /// Register a synchronous event to dispatch in commit receipt order.
    ///
    /// The transaction must contain a write that obtains a commit receipt.
    /// Regolith rejects eventful write-free and append-only transactions:
    /// an append can be deduplicated by its once-key and reuse its snapshot
    /// receipt, which cannot establish event order.
    /// Backends without receipt sequencing retain ordinary success semantics.
    fn on_success_ordered(&mut self, callback: OrderedTxnCallback) -> Result<()> {
        let callback = callback.clone();
        self.on_success(Box::new(move || callback()));
        Ok(())
    }

    /// Register an async post-commit hook that must complete before ordered events become visible.
    fn on_success_ordered_async(&mut self, _callback: AsyncOrderedTxnCallback) -> Result<()> {
        Err(crate::Error::Other(
            "ordered async callbacks are unsupported by this store".into(),
        ))
    }

    /// Register a bundle collected from a winning transaction attempt.
    fn register_ordered_callbacks(&mut self, callbacks: OrderedTxnCallbacks) -> Result<()> {
        let (sync, asynchronous) = callbacks.into_parts();
        for callback in sync {
            self.on_success_ordered(callback)?;
        }
        for callback in asynchronous {
            self.on_success_ordered_async(callback)?;
        }
        Ok(())
    }

    /// Discard the transaction, rolling back all changes.
    ///
    /// All on_discard callbacks are executed. After calling discard, the
    /// transaction cannot be used for any further operations.
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are not recovered here. In dev/test profiles the panic
    /// unwinds through `discard()`. In release builds this workspace uses
    /// `panic = "abort"`, so a panicking callback aborts the process.
    ///
    /// # Note
    ///
    /// This method consumes self, ensuring the transaction cannot be used after discard.
    fn discard(self: Box<Self>);

    /// Register a synchronous callback to be called on successful commit.
    ///
    /// Multiple callbacks can be registered and will be executed in order until
    /// one panics or the list completes.
    fn on_success(&mut self, callback: TxnCallback);

    /// Register an asynchronous callback to be called on successful commit.
    ///
    /// Multiple callbacks can be registered and will be executed sequentially
    /// in registration order until one panics or the list completes.
    fn on_success_async(&mut self, callback: AsyncTxnCallback);

    /// Register a synchronous callback to be called on commit error.
    ///
    /// Multiple callbacks can be registered and will be executed in order until
    /// one panics or the list completes.
    fn on_error(&mut self, callback: TxnCallback);

    /// Register an asynchronous callback to be called on commit error.
    ///
    /// Multiple callbacks can be registered and will be executed sequentially
    /// in registration order until one panics or the list completes.
    fn on_error_async(&mut self, callback: AsyncTxnCallback);

    /// Register a synchronous callback to be called on discard.
    ///
    /// Multiple callbacks can be registered and will be executed in order until
    /// one panics or the list completes.
    fn on_discard(&mut self, callback: TxnCallback);

    /// Register an asynchronous callback to be called on discard.
    ///
    /// Multiple callbacks can be registered and will be executed sequentially
    /// in registration order until one panics or the list completes.
    ///
    /// # Fire-and-Forget Warning
    ///
    /// Unlike `on_success_async` and `on_error_async` which are awaited during commit,
    /// async discard callbacks are **spawned as background tasks** and may not complete
    /// if the process exits before they finish. This matches Go DefraDB semantics where
    /// `discard()` is a synchronous operation.
    ///
    /// If you need completion guarantees for async cleanup:
    /// - Use `on_discard` (synchronous) instead
    /// - Use `commit()` with `on_success_async` when possible
    /// - Implement your own synchronization using `tokio::task::JoinSet`,
    ///   `tokio_util::task::TaskTracker`, or third-party crates like `awaitgroup`
    fn on_discard_async(&mut self, callback: AsyncTxnCallback);

    /// Downcast to concrete type (for internal use in tests)
    fn as_any(&self) -> &dyn std::any::Any;

    /// Downcast to concrete type (for internal use in tests)
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Get the total number of callbacks registered on this transaction.
    ///
    /// This returns the sum of all callback types (success, error, discard)
    /// including both sync and async variants.
    ///
    /// Useful for monitoring callback accumulation in long-running transactions.
    fn callback_count(&self) -> usize;
}

/// TxnStore trait for stores that support transactions.
///
/// This is a marker trait combining Store with the ability to create transactions.
/// Most production stores implement this trait.
pub trait TxnStore: Store {}

/// Blanket implementation: any Store automatically implements TxnStore.
impl<T> TxnStore for T where T: Store {}

/// Blanket implementation of Reader for Box<dyn Txn>.
///
/// This allows boxed transactions to be used where Reader is required.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for Box<dyn Txn> {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        (**self).get(key).await
    }

    async fn get_parts(&self, key: &[u8], parts: &[u32]) -> Result<Option<Bytes>> {
        (**self).get_parts(key, parts).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        (**self).has(key).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        (**self).get_size(key).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        (**self).iterator(opts).await
    }
}

/// Blanket implementation of Writer for Box<dyn Txn>.
///
/// This allows boxed transactions to be used where Writer is required.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for Box<dyn Txn> {
    async fn merge(&mut self, key: &[u8], operand: &[u8]) -> Result<()> {
        (**self).merge(key, operand).await
    }

    async fn append(
        &mut self,
        log: &super::types::AppendLog,
        entry: &[u8],
        once_key: Option<&[u8]>,
    ) -> Result<()> {
        (**self).append(log, entry, once_key).await
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        (**self).set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        (**self).delete(key).await
    }
}

/// Helper function to create a simple sync callback from a closure.
///
/// # Example
///
/// ```ignore
/// txn.on_success(make_callback(|| println!("Success!")));
/// ```
#[cfg(not(target_arch = "wasm32"))]
pub fn make_callback<F>(f: F) -> TxnCallback
where
    F: FnOnce() + Send + 'static,
{
    Box::new(f)
}

/// Helper function to create a simple sync callback from a closure (WASM version).
#[cfg(target_arch = "wasm32")]
pub fn make_callback<F>(f: F) -> TxnCallback
where
    F: FnOnce() + 'static,
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
#[cfg(not(target_arch = "wasm32"))]
pub fn make_async_callback<F, Fut>(f: F) -> AsyncTxnCallback
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || Box::pin(f()))
}

/// Helper function to create an async callback from a closure (WASM version).
#[cfg(target_arch = "wasm32")]
pub fn make_async_callback<F, Fut>(f: F) -> AsyncTxnCallback
where
    F: FnOnce() -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
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
