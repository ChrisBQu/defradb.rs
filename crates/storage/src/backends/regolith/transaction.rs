//! A regolith-backed transaction.
//!
//! regolith owns the write buffer, read set, and commit validation. A bounded
//! native commit publishes writes without an adapter publication lock. Head
//! rows and supersession markers are read through engine snapshots.
//!
//! A read-only transaction does not begin one at all: it pins a snapshot,
//! which is the same view without the commit-time validation nothing will
//! ever ask for.

#![cfg_attr(target_arch = "wasm32", allow(clippy::arc_with_non_send_sync))]

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use regolith::{
    IsolationLevel, OptimisticTransactionDb, Transaction, TransactionError, TxnOptions,
};

use super::event_order::{Event, EventOrder};
use super::handle::Handle;
use super::iterator::RegolithIterator;
use super::slots::TxnSlot;
use crate::backends::shared::{CallbackCounts, CallbackManager};
use crate::corekv::{
    AsyncOrderedTxnCallback, AsyncTxnCallback, Error, IterOptions, Iterator, OrderedTxnCallback,
    Reader, Result, Txn, TxnCallback, Writer,
};

/// Transaction over a regolith store.
pub struct RegolithTxn {
    handle: Option<Arc<Handle>>,
    /// This transaction's place in its store's in-flight count, held until
    /// the transaction and its post-commit work are done. A read-only open
    /// and a retry attempt's view count against no store.
    _slot: Option<TxnSlot>,
    callbacks: CallbackManager,
    readonly: bool,
    event_order: Arc<EventOrder>,
    ordered_events: Vec<OrderedTxnCallback>,
    ordered_async_callbacks: Vec<AsyncOrderedTxnCallback>,
    savepoints: Vec<SavepointState>,
    scoped_attempt: bool,
}

#[derive(Clone)]
struct SavepointState {
    callbacks: CallbackCounts,
    ordered_events: usize,
    ordered_async_callbacks: usize,
}

impl RegolithTxn {
    /// Open a snapshot of an existing on-disk database without modifying files,
    /// with the engine options, at-rest keys included, a writable store of it
    /// uses. This never rewrites a legacy store.
    pub fn open_read_only(
        path: impl AsRef<std::path::Path>,
        options: super::RegolithStoreOptions,
    ) -> Result<Self> {
        let db = regolith::Db::open_read_only(path.as_ref(), options.engine_for_open()).map_err(
            |error| Error::Backend(format!("failed to open regolith read-only: {error}")),
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        super::rewrite::check_read_only(path.as_ref(), &db, &options)?;
        Ok(Self {
            handle: Some(Arc::new(Handle::ReadOnly(db.snapshot()))),
            _slot: None,
            callbacks: CallbackManager::default(),
            readonly: true,
            savepoints: Vec::new(),
            event_order: Arc::new(EventOrder::default()),
            ordered_events: Vec::new(),
            ordered_async_callbacks: Vec::new(),
            scoped_attempt: false,
        })
    }

    pub(super) fn new(
        db: &Arc<OptimisticTransactionDb>,
        readonly: bool,
        isolation: IsolationLevel,
        slot: TxnSlot,
        event_order: Arc<EventOrder>,
    ) -> Result<Self> {
        super::blocking(|| {
            let handle = if readonly {
                Handle::ReadOnly(db.db().snapshot())
            } else {
                Handle::Writable(Box::new(db.begin(&TxnOptions::new().isolation(isolation))))
            };
            Ok(Self {
                handle: Some(Arc::new(handle)),
                _slot: Some(slot),
                callbacks: CallbackManager::default(),
                readonly,
                savepoints: Vec::new(),
                event_order,
                ordered_events: Vec::new(),
                ordered_async_callbacks: Vec::new(),
                scoped_attempt: false,
            })
        })
    }

    pub(super) fn scoped_attempt(txn: Box<Transaction>) -> Self {
        Self {
            handle: Some(Arc::new(Handle::Writable(txn))),
            _slot: None,
            callbacks: CallbackManager::default(),
            readonly: false,
            event_order: Arc::new(EventOrder::default()),
            ordered_events: Vec::new(),
            ordered_async_callbacks: Vec::new(),
            savepoints: Vec::new(),
            scoped_attempt: true,
        }
    }

    // Ownership arrives boxed from the sealed attempt IO capability.
    #[allow(clippy::boxed_local)]
    pub(super) fn into_scoped_engine_txn(mut self: Box<Self>) -> Result<Box<Transaction>> {
        if !self.scoped_attempt {
            return Err(Error::Other("not a scoped retry transaction".into()));
        }
        let handle = self.handle.take().ok_or(Error::DiscardedTxn)?;
        let handle = Arc::try_unwrap(handle).map_err(|_| {
            Error::Other("retry transaction still has retained views or iterators".into())
        })?;
        let Handle::Writable(txn) = handle else {
            return Err(Error::ReadOnlyTxn);
        };
        Ok(txn)
    }

    fn handle(&self) -> Result<&Arc<Handle>> {
        self.handle.as_ref().ok_or(Error::DiscardedTxn)
    }

    fn writable(&self) -> Result<&Transaction> {
        match self.handle()?.as_ref() {
            Handle::Writable(txn) => Ok(txn),
            Handle::ReadOnly(_) => Err(Error::ReadOnlyTxn),
        }
    }

    fn writable_mut(&mut self) -> Result<&mut Transaction> {
        let handle = self.handle.as_mut().ok_or(Error::DiscardedTxn)?;
        let handle = Arc::get_mut(handle).ok_or_else(|| {
            Error::Other("cannot savepoint while transaction iterators are active".into())
        })?;
        match handle {
            Handle::Writable(txn) => Ok(txn),
            Handle::ReadOnly(_) => Err(Error::ReadOnlyTxn),
        }
    }
}

impl Drop for RegolithTxn {
    /// A transaction dropped without a commit or a discard ended without
    /// committing: regolith rolls its writes back, and its discard
    /// callbacks run here, once. Not while unwinding, where a callback that
    /// panicked too would abort the process.
    fn drop(&mut self) {
        if self.handle.take().is_some() && !std::thread::panicking() {
            self.callbacks.run_discard();
        }
    }
}

pub(super) fn map_txn_error(error: TransactionError) -> Error {
    match error {
        TransactionError::Conflict(conflict) => {
            Error::TxnConflictDetails(crate::corekv::TransactionConflict {
                access: conflict.mine(),
                winning_write: conflict.theirs(),
                observed_seq: conflict.observed_seq(),
                latest_seq: conflict.latest_seq(),
            })
        }
        // Lock admission failure is not an optimistic validation conflict.
        // Keep it transient and typed as a backend failure so outer callers
        // do not count it as another storage-conflict retry.
        busy @ TransactionError::Busy(_) => Error::Backend(busy.to_string()),
        other => Error::Backend(other.to_string()),
    }
}

impl crate::corekv::private::Sealed for RegolithTxn {}

impl crate::corekv::RetryAttemptIO for RegolithTxn {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Reader for RegolithTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        // `get_slice` borrows the bytes the engine already holds and
        // `Bytes::from_owner` adopts that borrow, so a read copies
        // nothing on the way to the caller.
        let slice = match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .get_slice(key)
                .map_err(|error| Error::Backend(error.to_string()))?,
            Handle::Writable(txn) => regolith::ready(|| txn.get_slice(key))
                .await
                .map_err(map_txn_error)?,
        };
        Ok(slice.map(Bytes::from_owner))
    }

    async fn get_parts(&self, key: &[u8], parts: &[u32]) -> Result<Option<Bytes>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let slice = match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .get_slice(key)
                .map_err(|error| Error::Backend(error.to_string()))?,
            Handle::Writable(txn) => regolith::ready(|| txn.get_parts(key, parts))
                .await
                .map_err(map_txn_error)?,
        };
        Ok(slice.map(Bytes::from_owner))
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .has(key)
                .map_err(|error| Error::Backend(error.to_string())),
            // A writing transaction has to consult its own buffer, which
            // `has` on the snapshot underneath would miss.
            Handle::Writable(txn) => Ok(regolith::ready(|| txn.get_slice(key))
                .await
                .map_err(map_txn_error)?
                .is_some()),
        }
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        match self.handle()?.as_ref() {
            Handle::ReadOnly(snapshot) => snapshot
                .get_size(key)
                .map_err(|error| Error::Backend(error.to_string())),
            Handle::Writable(txn) => Ok(regolith::ready(|| txn.get_slice(key))
                .await
                .map_err(map_txn_error)?
                .map(|v| v.len())),
        }
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        // The iterator holds the same handle, so it keeps the snapshot or
        // the transaction alive without borrowing this one. Scanning a
        // transaction that is still being written to is the expensive
        // shape, because the merged stream is rebuilt per page: open a
        // read-only transaction where the scan does not need to see
        // uncommitted writes.
        let handle = Arc::clone(self.handle()?);
        Ok(Box::new(RegolithIterator::open(handle, &opts)?))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Writer for RegolithTxn {
    async fn merge(&mut self, key: &[u8], operand: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let txn = self.writable()?;
        regolith::ready(|| txn.merge(key, operand))
            .await
            .map_err(map_txn_error)?;
        Ok(())
    }

    async fn append(
        &mut self,
        log: &crate::corekv::AppendLog,
        entry: &[u8],
        once_key: Option<&[u8]>,
    ) -> Result<()> {
        let layout: Arc<dyn regolith::LogLayout> = log.keys().clone();
        let txn = self.writable()?;
        regolith::ready(|| txn.append(&layout, entry, once_key))
            .await
            .map_err(map_txn_error)?;
        Ok(())
    }

    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let txn = self.writable()?;
        regolith::ready(|| txn.put(key, value))
            .await
            .map_err(map_txn_error)?;
        Ok(())
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        let txn = self.writable()?;
        regolith::ready(|| txn.delete(key))
            .await
            .map_err(map_txn_error)?;
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Txn for RegolithTxn {
    async fn commit(mut self: Box<Self>) -> Result<()> {
        if self.scoped_attempt {
            return Err(Error::Other(
                "scoped retry transaction cannot commit independently".into(),
            ));
        }
        let handle = self.handle.take().ok_or(Error::DiscardedTxn)?;
        // regolith's `commit` consumes the transaction, so this has to be
        // its only owner. An iterator opened from it holds the same `Arc`,
        // so a caller that commits without closing one gets told, rather
        // than a commit that silently does not happen.
        let Ok(handle) = Arc::try_unwrap(handle) else {
            self.callbacks.run_error().await;
            return Err(Error::Other(
                "cannot commit while an iterator opened from this transaction is still open"
                    .to_string(),
            ));
        };
        let Handle::Writable(mut txn) = handle else {
            // A snapshot has nothing to commit and cannot carry ordered work.
            self.callbacks.run_success().await;
            return Ok(());
        };
        let events: Vec<Event> = std::mem::take(&mut self.ordered_events)
            .into_iter()
            .map(|callback| Box::new(move || callback()) as Event)
            .collect();
        let hooks = std::mem::take(&mut self.ordered_async_callbacks);
        let binding = (!events.is_empty() || !hooks.is_empty())
            .then(|| self.event_order.bind(&mut txn, events, !hooks.is_empty()));
        let outcome = super::blocking(|| txn.commit()).map_err(map_txn_error);
        let running = binding.and_then(|binding| binding.hand_over(outcome.is_ok()));
        match outcome {
            // Durable from here on: what follows can be reported, never
            // turned into a failed write.
            Ok(_) => {
                if let Some(running) = running {
                    for hook in hooks {
                        super::post_commit::run_hook(hook()).await;
                    }
                    running.done();
                }
                self.callbacks.run_success().await;
                Ok(())
            }
            Err(error) => {
                self.callbacks.run_error().await;
                Err(error)
            }
        }
    }

    fn set_savepoint(&mut self) -> Result<()> {
        let state = SavepointState {
            callbacks: self.callbacks.counts(),
            ordered_events: self.ordered_events.len(),
            ordered_async_callbacks: self.ordered_async_callbacks.len(),
        };
        self.writable_mut()?.set_savepoint();
        self.savepoints.push(state);
        Ok(())
    }

    fn rollback_to_savepoint(&mut self) -> Result<()> {
        let state = self
            .savepoints
            .last()
            .cloned()
            .ok_or_else(|| Error::Other("transaction has no active savepoint".into()))?;
        self.writable_mut()?
            .rollback_to_savepoint()
            .map_err(map_txn_error)?;
        self.savepoints.pop();
        self.callbacks.truncate(state.callbacks);
        self.ordered_events.truncate(state.ordered_events);
        self.ordered_async_callbacks
            .truncate(state.ordered_async_callbacks);
        Ok(())
    }

    fn release_savepoint(&mut self) -> Result<()> {
        self.savepoints
            .last()
            .ok_or_else(|| Error::Other("transaction has no active savepoint".into()))?;
        self.writable_mut()?
            .release_savepoint()
            .map_err(map_txn_error)?;
        self.savepoints.pop();
        Ok(())
    }

    fn discard(mut self: Box<Self>) {
        // An iterator still open keeps the transaction alive; regolith
        // rolls it back when that last handle drops.
        if let Some(Ok(Handle::Writable(txn))) = self.handle.take().map(Arc::try_unwrap) {
            txn.rollback();
        }
        self.callbacks.run_discard();
    }

    fn on_success(&mut self, callback: TxnCallback) {
        self.callbacks.on_success(callback);
    }

    fn on_success_ordered(&mut self, callback: crate::corekv::OrderedTxnCallback) -> Result<()> {
        if self.readonly {
            return Err(Error::ReadOnlyTxn);
        }
        self.ordered_events.push(callback);
        Ok(())
    }

    fn on_success_ordered_async(&mut self, callback: AsyncOrderedTxnCallback) -> Result<()> {
        if self.readonly {
            return Err(Error::ReadOnlyTxn);
        }
        self.ordered_async_callbacks.push(callback);
        Ok(())
    }

    fn on_success_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.on_success_async(callback);
    }

    fn on_error(&mut self, callback: TxnCallback) {
        self.callbacks.on_error(callback);
    }

    fn on_error_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.on_error_async(callback);
    }

    fn on_discard(&mut self, callback: TxnCallback) {
        self.callbacks.on_discard(callback);
    }

    fn on_discard_async(&mut self, callback: AsyncTxnCallback) {
        self.callbacks.on_discard_async(callback);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.readonly
    }

    fn callback_count(&self) -> usize {
        self.callbacks.total()
    }
}

#[cfg(test)]
mod append_tests {
    use crate::backends::RegolithStore;
    use crate::corekv::{AppendLog, Store, Writer};

    /// A pending append holds the layout its log already shares, not a copy.
    #[tokio::test]
    async fn an_append_shares_its_log_layout() {
        let store = RegolithStore::in_memory().unwrap();
        let log = AppendLog::new(b"s/arrival/c1/head".to_vec(), b"s/arrival/c1/row/".to_vec());
        let mut txn = store.new_txn(false).await.unwrap();
        txn.append(&log, b"doc-1", Some(b"s/arrival/c1/doc/doc-1"))
            .await
            .unwrap();
        assert_eq!(std::sync::Arc::strong_count(log.keys()), 2);
        txn.discard();
    }
}
