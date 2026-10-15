//! Transaction registry trait and no-op implementation.

use async_trait::async_trait;
use defra_core::RequestContext;
use storage::corekv::MaybeSendSync;

use crate::error::TransactionError;

use super::handle::TransactionHandle;
use super::result::GetTransactionResult;

/// Registry for managing active transactions.
///
/// The database layer implements this to track transactions that can be
/// used by the query executor. A transaction belongs to the identity of the
/// context that began it, and only a request carrying that identity reaches
/// it again.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait TransactionRegistry: MaybeSendSync + 'static {
    /// Begin a new transaction.
    ///
    /// Returns a handle that can be used with `get()`, `commit()`, and `rollback()`.
    /// Registration and returning the handle must not be separated by an await.
    async fn begin(
        &self,
        context: &RequestContext,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError>;

    /// Begin the short-lived read-only transaction used for a standalone query.
    ///
    /// Registries that need to defer read side effects until the snapshot has
    /// closed can override this separately from user-managed transactions.
    async fn begin_implicit_read(
        &self,
        context: &RequestContext,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin(context, true).await
    }

    /// Get an existing transaction by handle.
    ///
    /// Returns `Found(context)` if the transaction exists,
    /// `NotFound` if it doesn't exist or has been committed/rolled back,
    /// or `LockPoisoned` if the registry lock is poisoned.
    fn get(&self, context: &RequestContext, handle: &TransactionHandle) -> GetTransactionResult;

    /// Commit a transaction.
    ///
    /// After commit, the handle is no longer valid for `get()`.
    async fn commit(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError>;

    /// Rollback a transaction.
    ///
    /// After rollback, the handle is no longer valid for `get()`.
    async fn rollback(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError>;

    /// Synchronously remove an abandoned entry without committing its writes.
    /// Must be idempotent and work without an async runtime. Existing context
    /// borrowers may delay resource release, but cannot finalize through this handle.
    fn abandon(&self, handle: &TransactionHandle);

    /// Close a standalone query's implicit read transaction.
    ///
    /// `apply_read_effects` is false when query execution failed. The default
    /// implementation simply rolls the snapshot back.
    async fn finish_implicit_read(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
        _apply_read_effects: bool,
    ) -> std::result::Result<(), TransactionError> {
        self.rollback(context, handle).await
    }
}

/// A no-op transaction registry that doesn't support transactions.
///
/// This is used when transactions aren't needed or available.
#[derive(Debug, Clone, Default)]
pub struct NoOpTransactionRegistry;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl TransactionRegistry for NoOpTransactionRegistry {
    fn abandon(&self, _handle: &TransactionHandle) {}

    async fn begin(
        &self,
        _context: &RequestContext,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    fn get(&self, _context: &RequestContext, _handle: &TransactionHandle) -> GetTransactionResult {
        GetTransactionResult::NotFound
    }

    async fn commit(
        &self,
        _context: &RequestContext,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    async fn rollback(
        &self,
        _context: &RequestContext,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }
}
