use crate::error::{Error, Result};
use crate::multistore::{NamespaceView, RootView, SharedTxn};
/// Transaction wrapper for DefraDB matching Go's internal/datastore/txn.go.
///
/// BasicTxn wraps a corekv transaction and provides:
/// - Multistore access via namespace views
/// - Transaction ID
/// - Lifecycle callbacks (on_success, on_error, on_discard)
use async_trait::async_trait;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use storage::corekv::{
    AsyncOrderedRetryCallback, AsyncTxnCallback, IterOptions, Iterator, OrderedRetryCallback,
    OrderedTxnCallback, Reader, RetryAttemptIO, RetryCallbackCounts, RetryCallbacks, Store, Txn,
    TxnCallback, Writer,
};

use storage::namespace::Namespace;

/// Asynchronous callback for transaction events.
#[cfg(not(target_arch = "wasm32"))]
pub type AsyncCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;
#[cfg(target_arch = "wasm32")]
pub type AsyncCallback = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()>>>>;

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnState {
    Active,
    Committed,
    Discarded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxnCallbackPhase {
    Success,
    Error,
    Discard,
}

enum RegisteredCallback {
    Sync {
        phase: TxnCallbackPhase,
        callback: TxnCallback,
    },
    Async {
        phase: TxnCallbackPhase,
        callback: AsyncCallback,
    },
}

/// BasicTxn wraps a corekv transaction with DefraDB-specific functionality.
///
/// This matches Go's BasicTxn in internal/datastore/txn.go, providing:
/// - Access to all namespaced stores (datastore, blockstore, etc.)
/// - Transaction ID for tracking
/// - Lifecycle callbacks
pub struct BasicTxn {
    shared_txn: Arc<SharedTxn>,
    id: u64,
    readonly: bool,
    state: TxnState,
    callbacks: Vec<RegisteredCallback>,
    savepoints: Vec<BasicTxnSavepoint>,
    ordered_callbacks: Vec<OrderedTxnCallback>,
    ordered_async_callbacks: Vec<storage::corekv::AsyncOrderedTxnCallback>,
    retry_callbacks: RetryCallbacks,
}

/// Internal Txn adapter used only to reuse the existing namespace view graph
/// inside an engine-owned retry. Its lifecycle methods never commit the
/// transaction; only `RetryTxn::finish_attempt_io` can return it to the engine.
struct AttemptScopedTxn {
    io: Option<Box<dyn RetryAttemptIO>>,
    callbacks: RetryCallbacks,
}

impl AttemptScopedTxn {
    fn new(io: Box<dyn RetryAttemptIO>) -> Self {
        Self {
            io: Some(io),
            callbacks: RetryCallbacks::default(),
        }
    }

    fn take(&mut self) -> storage::corekv::Result<(Box<dyn RetryAttemptIO>, RetryCallbacks)> {
        let io = self.io.take().ok_or(storage::corekv::Error::DiscardedTxn)?;
        Ok((io, std::mem::take(&mut self.callbacks)))
    }

    fn io(&self) -> storage::corekv::Result<&dyn RetryAttemptIO> {
        self.io
            .as_deref()
            .ok_or(storage::corekv::Error::DiscardedTxn)
    }

    fn io_mut(&mut self) -> storage::corekv::Result<&mut (dyn RetryAttemptIO + 'static)> {
        self.io
            .as_deref_mut()
            .ok_or(storage::corekv::Error::DiscardedTxn)
    }
}

impl storage::corekv::private::Sealed for AttemptScopedTxn {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for AttemptScopedTxn {
    async fn get(&self, key: &[u8]) -> storage::corekv::Result<Option<Bytes>> {
        self.io()?.get(key).await
    }

    async fn get_parts(&self, key: &[u8], parts: &[u32]) -> storage::corekv::Result<Option<Bytes>> {
        self.io()?.get_parts(key, parts).await
    }

    async fn has(&self, key: &[u8]) -> storage::corekv::Result<bool> {
        self.io()?.has(key).await
    }

    async fn has_for_update(&self, key: &[u8]) -> storage::corekv::Result<bool> {
        self.io()?.has_for_update(key).await
    }

    async fn get_size(&self, key: &[u8]) -> storage::corekv::Result<Option<usize>> {
        self.io()?.get_size(key).await
    }

    async fn iterator(&self, options: IterOptions) -> storage::corekv::Result<Box<dyn Iterator>> {
        self.io()?.iterator(options).await
    }

    async fn collection_head_entries(
        &self,
        head_prefix: &[u8],
        marker_prefix: &[u8],
    ) -> storage::corekv::Result<Option<storage::stores::headstore::CollectionHeadEntries>> {
        self.io()?
            .collection_head_entries(head_prefix, marker_prefix)
            .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for AttemptScopedTxn {
    async fn merge(&mut self, key: &[u8], operand: &[u8]) -> storage::corekv::Result<()> {
        self.io_mut()?.merge(key, operand).await
    }

    async fn append(
        &mut self,
        log: &storage::corekv::AppendLog,
        entry: &[u8],
        once_key: Option<&[u8]>,
    ) -> storage::corekv::Result<()> {
        self.io_mut()?.append(log, entry, once_key).await
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> storage::corekv::Result<()> {
        self.io_mut()?.set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> storage::corekv::Result<()> {
        self.io_mut()?.delete(key).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Txn for AttemptScopedTxn {
    async fn commit(self: Box<Self>) -> storage::corekv::Result<()> {
        Err(storage::corekv::Error::Other(
            "attempt-scoped transaction cannot commit independently".into(),
        ))
    }

    fn discard(self: Box<Self>) {}

    fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.push_sync(callback);
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.push_async(callback);
    }

    fn on_success_ordered(&mut self, callback: OrderedTxnCallback) -> storage::corekv::Result<()> {
        self.callbacks.push_ordered(Box::new(move || callback()));
        Ok(())
    }

    fn on_success_ordered_async(&mut self, callback: storage::corekv::AsyncOrderedTxnCallback) -> storage::corekv::Result<()> {
        self.callbacks.push_ordered_async(Box::new(move || callback()));
        Ok(())
    }

    fn on_error(&mut self, _callback: TxnCallback) {}
    fn on_error_async(&mut self, _callback: AsyncTxnCallback) {}
    fn on_discard(&mut self, _callback: TxnCallback) {}
    fn on_discard_async(&mut self, _callback: AsyncTxnCallback) {}

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        false
    }

    fn callback_count(&self) -> usize {
        0
    }

}

#[derive(Clone, Copy)]
struct BasicTxnSavepoint {
    callbacks: usize,
    ordered_callbacks: usize,
    ordered_async_callbacks: usize,
    retry_callbacks: RetryCallbackCounts,
}

impl BasicTxn {
    /// Create a new BasicTxn from a store.
    pub async fn new<S: Store>(
        store: &S,
        id: u64,
        readonly: bool,
    ) -> storage::corekv::Result<Self> {
        let txn = store.new_txn(readonly).await?;
        Ok(Self::from_txn(txn, id, readonly))
    }

    /// Create a BasicTxn from an existing corekv transaction.
    pub fn from_txn(txn: Box<dyn Txn>, id: u64, readonly: bool) -> Self {
        Self {
            shared_txn: SharedTxn::new(txn),
            id,
            readonly,
            state: TxnState::Active,
            callbacks: Vec::new(),
            savepoints: Vec::new(),
            ordered_callbacks: Vec::new(),
            ordered_async_callbacks: Vec::new(),
            retry_callbacks: RetryCallbacks::default(),

        }
    }

    pub fn set_savepoint(&mut self) -> Result<()> {
        if self.state != TxnState::Active || self.readonly {
            return Err(Error::Storage(if self.readonly {
                storage::corekv::Error::ReadOnlyTxn
            } else {
                storage::corekv::Error::DiscardedTxn
            }));
        }
        self.shared_txn.set_savepoint().map_err(Error::Storage)?;
        self.savepoints.push(BasicTxnSavepoint {
            callbacks: self.callbacks.len(),
            ordered_callbacks: self.ordered_callbacks.len(),
            ordered_async_callbacks: self.ordered_async_callbacks.len(),
            retry_callbacks: self.retry_callbacks.counts(),
        });
        Ok(())
    }

    pub fn rollback_to_savepoint(&mut self) -> Result<()> {
        let state = *self.savepoints.last().ok_or_else(|| {
            Error::Storage(storage::corekv::Error::Other(
                "transaction has no active savepoint".into(),
            ))
        })?;
        self.shared_txn
            .rollback_to_savepoint()
            .map_err(Error::Storage)?;
        self.savepoints.pop();
        self.callbacks.truncate(state.callbacks);
        self.ordered_callbacks.truncate(state.ordered_callbacks);
        self.ordered_async_callbacks
            .truncate(state.ordered_async_callbacks);
        self.retry_callbacks.truncate(state.retry_callbacks);
        Ok(())
    }

    pub fn release_savepoint(&mut self) -> Result<()> {
        if self.savepoints.is_empty() {
            return Err(Error::Storage(storage::corekv::Error::Other(
                "transaction has no active savepoint".into(),
            )));
        }
        self.shared_txn
            .release_savepoint()
            .map_err(Error::Storage)?;
        self.savepoints.pop();
        Ok(())
    }

    /// Create the namespace-capable facade for one engine-owned retry attempt.
    pub fn from_retry_attempt_io(io: Box<dyn RetryAttemptIO>, id: u64) -> Self {
        Self::from_txn(Box::new(AttemptScopedTxn::new(io)), id, false)
    }

    /// Drop all namespace views and recover the restricted IO capability plus
    /// callbacks for the store to attach to the winning commit.
    pub fn into_retry_attempt(
        self,
    ) -> storage::corekv::Result<(Box<dyn RetryAttemptIO>, RetryCallbacks)> {
        let Self {
            shared_txn,
            state,
            callbacks,
            mut retry_callbacks,
            ordered_callbacks,
            ordered_async_callbacks,
            ..
        } = self;
        if state != TxnState::Active {
            return Err(storage::corekv::Error::DiscardedTxn);
        }
        let mut txn = SharedTxn::try_into_txn(shared_txn).map_err(|_| {
            storage::corekv::Error::Other(
                "retry attempt retained namespace views or iterators".into(),
            )
        })?;
        let scoped = txn
            .as_any_mut()
            .downcast_mut::<AttemptScopedTxn>()
            .ok_or_else(|| {
                storage::corekv::Error::Other("transaction is not retry-scoped".into())
            })?;
        let (io, mut success_callbacks) = scoped.take()?;
        success_callbacks.append(std::mem::take(&mut retry_callbacks));
        for callback in ordered_callbacks { success_callbacks.push_ordered(Box::new(move || callback())); }
        for callback in ordered_async_callbacks { success_callbacks.push_ordered_async(Box::new(move || callback())); }
        drop(txn);

        for callback in callbacks {
            match callback {
                RegisteredCallback::Sync {
                    phase: TxnCallbackPhase::Success,
                    callback,
                } => success_callbacks.push_sync(callback),
                RegisteredCallback::Async {
                    phase: TxnCallbackPhase::Success,
                    callback,
                } => success_callbacks.push_async(callback),
                _ => {}
            }
        }
        Ok((io, success_callbacks))
    }

    /// Get the transaction ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Check if this is a read-only transaction.
    pub fn is_readonly(&self) -> bool {
        self.readonly
    }

    /// Get the blockstore (namespace 'b').
    pub fn blockstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Blockstore)
    }

    /// Get the datastore (namespace 'd').
    pub fn datastore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Datastore)
    }

    /// Get the encstore (namespace 'e').
    pub fn encstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Encstore)
    }

    /// Get the headstore (namespace 'h').
    pub fn headstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Headstore)
    }

    /// Get the peerstore (namespace 'p').
    pub fn peerstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Peerstore)
    }

    /// Get the systemstore (namespace 's').
    pub fn systemstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Systemstore)
    }

    /// Get the access-control policy store (namespace 'a').
    pub fn acpstore(&self) -> NamespaceView {
        NamespaceView::new(self.shared_txn.clone(), Namespace::Acpstore)
    }

    /// Get the rootstore (no namespace prefix).
    pub fn rootstore(&self) -> RootView {
        RootView::new(self.shared_txn.clone())
    }

    /// Register a callback to be called on successful commit.
    pub fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.push(RegisteredCallback::Sync {
            phase: TxnCallbackPhase::Success,
            callback,
        });
    }

    /// Register a post-commit event for receipt-ordered dispatch.
    pub fn on_success_ordered(&mut self, callback: OrderedTxnCallback) -> Result<()> {
        if self.readonly {
            return Err(Error::Storage(storage::Error::ReadOnlyTxn));
        }
        self.ordered_callbacks.push(callback);
        Ok(())
    }

    pub fn on_success_ordered_async(
        &mut self,
        callback: storage::corekv::AsyncOrderedTxnCallback,
    ) -> Result<()> {
        if self.readonly {
            return Err(Error::Storage(storage::Error::ReadOnlyTxn));
        }
        self.ordered_async_callbacks.push(callback);
        Ok(())
    }

    /// Move callbacks collected by a mutation attempt into an owned bundle.
    pub fn take_ordered_callbacks(&mut self) -> storage::corekv::OrderedTxnCallbacks {
        let mut callbacks = storage::corekv::OrderedTxnCallbacks::default();
        for callback in self.ordered_callbacks.drain(..) {
            callbacks.push_sync(callback);
        }
        for callback in self.ordered_async_callbacks.drain(..) {
            callbacks.push_async(callback);
        }
        callbacks
    }

    /// Attach callbacks from a winning attempt to this transaction.
    pub fn register_ordered_callbacks(
        &mut self,
        callbacks: storage::corekv::OrderedTxnCallbacks,
    ) -> Result<()> {
        if self.readonly {
            return Err(Error::Storage(storage::Error::ReadOnlyTxn));
        }
        let (sync, asynchronous) = callbacks.into_parts();
        self.ordered_callbacks.extend(sync);
        self.ordered_async_callbacks.extend(asynchronous);
        Ok(())
    }

    /// Register a callback to be called on commit error.
    pub fn on_error(&mut self, callback: TxnCallback) {
        self.callbacks.push(RegisteredCallback::Sync {
            phase: TxnCallbackPhase::Error,
            callback,
        });
    }

    /// Register a callback to be called on discard.
    pub fn on_discard(&mut self, callback: TxnCallback) {
        self.callbacks.push(RegisteredCallback::Sync {
            phase: TxnCallbackPhase::Discard,
            callback,
        });
    }

    /// Register an async callback to be called on successful commit.
    pub fn on_success_async(&mut self, callback: AsyncCallback) {
        self.callbacks.push(RegisteredCallback::Async {
            phase: TxnCallbackPhase::Success,
            callback,
        });
    }

    pub fn on_retry_success_ordered(&mut self, callback: OrderedRetryCallback) {
        self.retry_callbacks.push_ordered(callback);
    }

    pub fn on_retry_success_ordered_async(&mut self, callback: AsyncOrderedRetryCallback) {
        self.retry_callbacks.push_ordered_async(callback);
    }

    /// Register an async callback to be called on commit error.
    pub fn on_error_async(&mut self, callback: AsyncCallback) {
        self.callbacks.push(RegisteredCallback::Async {
            phase: TxnCallbackPhase::Error,
            callback,
        });
    }

    /// Register an async callback to be called on discard.
    pub fn on_discard_async(&mut self, callback: AsyncCallback) {
        self.callbacks.push(RegisteredCallback::Async {
            phase: TxnCallbackPhase::Discard,
            callback,
        });
    }

    fn split_callbacks(
        callbacks: Vec<RegisteredCallback>,
        phase: TxnCallbackPhase,
    ) -> (
        Vec<TxnCallback>,
        Vec<AsyncCallback>,
        Vec<RegisteredCallback>,
    ) {
        let mut retained = Vec::with_capacity(callbacks.len());
        let mut sync_callbacks = Vec::new();
        let mut async_callbacks = Vec::new();

        for callback in callbacks {
            match callback {
                RegisteredCallback::Sync {
                    phase: callback_phase,
                    callback,
                } if callback_phase == phase => {
                    sync_callbacks.push(callback);
                }
                RegisteredCallback::Async {
                    phase: callback_phase,
                    callback,
                } if callback_phase == phase => {
                    async_callbacks.push(callback);
                }
                callback => retained.push(callback),
            }
        }

        (sync_callbacks, async_callbacks, retained)
    }

    /// Commit the transaction.
    ///
    /// On success, all on_success callbacks are executed.
    /// On error, all on_error callbacks are executed.
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are not recovered here. In dev/test profiles the panic
    /// unwinds through `commit()`. In release builds this workspace uses
    /// `panic = "abort"`, so a panicking callback aborts the process.
    pub async fn commit(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(match self.state {
                TxnState::Committed => Error::TxnAlreadyCommitted,
                TxnState::Discarded => Error::TxnAlreadyDiscarded,
                TxnState::Active => unreachable!(),
            });
        }

        // Extract the underlying transaction
        // The SharedTxn holds it in an Arc<RwLock<Box<dyn Txn>>>
        // We need to get ownership to call commit
        let callbacks = std::mem::take(&mut self.callbacks);
        let ordered_callbacks = std::mem::take(&mut self.ordered_callbacks);
        let ordered_async_callbacks = std::mem::take(&mut self.ordered_async_callbacks);
        let shared = Arc::try_unwrap(self.shared_txn).map_err(|_| {
            Error::Storage(storage::corekv::Error::Other(
                "Cannot commit: transaction still has references".into(),
            ))
        })?;

        let mut txn = shared.into_txn();
        for callback in ordered_callbacks {
            txn.on_success_ordered(callback).map_err(Error::Storage)?;
        }
        for callback in ordered_async_callbacks {
            txn.on_success_ordered_async(callback)
                .map_err(Error::Storage)?;
        }
        let result = txn.commit().await;

        let (sync_fns, async_fns, retained_callbacks) = if result.is_ok() {
            self.state = TxnState::Committed;
            Self::split_callbacks(callbacks, TxnCallbackPhase::Success)
        } else {
            Self::split_callbacks(callbacks, TxnCallbackPhase::Error)
        };
        self.callbacks = retained_callbacks;

        // Callbacks run directly so the runtime's panic strategy stays explicit:
        // unwind propagates in dev/test, and release aborts the process.
        for callback in async_fns {
            callback().await;
        }

        for callback in sync_fns {
            callback();
        }

        result.map_err(Error::Storage)
    }

    /// Discard the transaction.
    ///
    /// All on_discard callbacks are executed on success.
    /// Returns an error if the transaction is not active or still has references.
    ///
    /// # Callback Panic Handling
    ///
    /// Callback panics are not recovered here. In dev/test profiles the panic
    /// unwinds through `discard()`. In release builds this workspace uses
    /// `panic = "abort"`, so a panicking callback aborts the process.
    ///
    /// # Async Callback Warning
    ///
    /// Async discard callbacks are spawned as background tasks and may not complete
    /// if the process exits before they finish. For completion guarantees, use
    /// sync callbacks or prefer `commit()` when async cleanup is critical.
    pub fn discard(mut self) -> Result<()> {
        if self.state != TxnState::Active {
            return Err(match self.state {
                TxnState::Committed => Error::TxnAlreadyCommitted,
                TxnState::Discarded => Error::TxnAlreadyDiscarded,
                TxnState::Active => unreachable!(),
            });
        }

        // Extract and discard the underlying transaction
        let callbacks = std::mem::take(&mut self.callbacks);
        let shared = Arc::try_unwrap(self.shared_txn).map_err(|_| Error::TxnStillInUse)?;

        let txn = shared.into_txn();
        txn.discard();

        self.state = TxnState::Discarded;

        // Async discard callbacks are spawned and run directly. On native
        // targets a panic in the task follows the runtime panic strategy.
        let txn_id = self.id;
        let (discard_fns, discard_async_fns, retained_callbacks) =
            Self::split_callbacks(callbacks, TxnCallbackPhase::Discard);
        self.callbacks = retained_callbacks;
        if !discard_async_fns.is_empty() {
            let callback_count = discard_async_fns.len();
            tracing::debug!(
                txn_id = txn_id,
                count = callback_count,
                "Spawning async discard callbacks in background"
            );
            for callback in discard_async_fns {
                #[cfg(not(target_arch = "wasm32"))]
                tokio::spawn(async move {
                    callback().await;
                });
                // On wasm32, async callbacks cannot be spawned or awaited from
                // a sync context. Since discard callbacks are fire-and-forget,
                // we drop them. Use sync callbacks for critical cleanup on wasm32.
                #[cfg(target_arch = "wasm32")]
                {
                    let _ = callback;
                }
            }
        }

        for callback in discard_fns {
            callback();
        }

        Ok(())
    }
}
