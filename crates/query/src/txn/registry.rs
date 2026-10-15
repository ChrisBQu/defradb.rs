//! Transaction registry trait and no-op implementation.

use std::time::Duration;

use async_trait::async_trait;
use storage::corekv::{MaybeSend, MaybeSendSync};

use crate::error::TransactionError;

use super::handle::TransactionHandle;
use super::turn::TxnJob;

/// Registry for managing active transactions.
///
/// The database layer implements this. A transaction lives where the registry
/// keeps it, and every request for it runs there, one at a time, in the
/// order the requests arrived ([`run`](Self::run)); nothing outside the
/// registry ever holds the transaction.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait TransactionRegistry: MaybeSendSync + 'static {
    /// Begin a new transaction.
    ///
    /// Returns a handle that names it to [`run`](Self::run),
    /// [`commit`](Self::commit) and [`rollback`](Self::rollback).
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError>;

    /// Runs `job` on the transaction `handle` names, where it lives: after
    /// every request for it that arrived earlier and before every one that
    /// arrives later. A job still running after `timeout` is stopped, its
    /// statement undone, and the call fails with an execution error. A
    /// handle the caller does not own is not found.
    async fn run<T>(
        &self,
        handle: &TransactionHandle,
        timeout: Option<Duration>,
        job: TxnJob<T>,
    ) -> std::result::Result<T, TransactionError>
    where
        T: MaybeSend + 'static,
    {
        let _ = (handle, timeout, job);
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    /// Runs `job` against a read-only snapshot of its own, which no other
    /// request sees and nothing registers: a standalone query's implicit
    /// read. `timeout` stops it as [`run`](Self::run) does.
    async fn read<T>(
        &self,
        timeout: Option<Duration>,
        job: TxnJob<T>,
    ) -> std::result::Result<T, TransactionError>
    where
        T: MaybeSend + 'static,
    {
        let _ = (timeout, job);
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    /// Commit a transaction, after every request for it that arrived earlier.
    ///
    /// After commit, the handle names nothing.
    async fn commit(&self, handle: &TransactionHandle)
        -> std::result::Result<(), TransactionError>;

    /// Rollback a transaction, after every request for it that arrived
    /// earlier.
    ///
    /// After rollback, the handle names nothing.
    async fn rollback(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError>;

    /// Let go of a transaction without committing its writes, at once and
    /// without waiting: no later request finds it, and it ends after the
    /// requests already queued for it. Must be idempotent and work without
    /// an async runtime (a guard's destructor calls it).
    fn abandon(&self, handle: &TransactionHandle);
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
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    async fn commit(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }

    async fn rollback(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported(
            "transactions are not supported in this configuration",
        ))
    }
}
