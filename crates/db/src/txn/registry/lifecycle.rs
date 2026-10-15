//! Transaction begin, requests, commit, rollback and abandon, each a message
//! to the worker that owns the transaction.

use super::*;
use async_trait::async_trait;
use query::txn::{TransactionHandle, TransactionRegistry, TxnJob};
use storage::corekv::MaybeSend;

/// Runs `job` until it finishes or `timeout` passes, when the job is dropped
/// and the request fails.
async fn within<F: std::future::Future>(
    timeout: Option<Duration>,
    job: F,
) -> std::result::Result<F::Output, TransactionError> {
    query::runner::within_deadline(timeout, job)
        .await
        .map_err(TransactionError::execution)
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    fn abandon(&self, handle: &TransactionHandle) {
        if let (Ok(owners), Some(id)) = (self.owners(), TxnId::parse(handle)) {
            owners.abandon(id);
        }
    }

    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_as(&RequestContext::ambient(), readonly).await
    }

    async fn begin_as(
        &self,
        context: &RequestContext,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let db = self.db.clone();
        let broadcaster = self.broadcaster.clone();
        let opened = self
            .owners()
            .map_err(|error| TransactionError::execution(error.to_string()))?
            .begin(caller(context), move |id| async move {
                let mut txn = db.new_txn(readonly).await?;
                txn.make_explicit();
                OwnedTxn::new(id, txn, db, broadcaster)
            })
            .await
            .map_err(|error| TransactionError::execution(error.to_string()))?;
        let id = opened
            .map_err(|error| TransactionError::execution(format!("storage error: {}", error)))?;
        Ok(TransactionHandle::new(id.to_string()))
    }

    async fn run<T>(
        &self,
        handle: &TransactionHandle,
        timeout: Option<Duration>,
        job: TxnJob<T>,
    ) -> std::result::Result<T, TransactionError>
    where
        T: MaybeSend + 'static,
    {
        self.run_as(&RequestContext::ambient(), handle, timeout, job)
            .await
    }

    async fn run_as<T>(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
        timeout: Option<Duration>,
        job: TxnJob<T>,
    ) -> std::result::Result<T, TransactionError>
    where
        T: MaybeSend + 'static,
    {
        let id = TxnId::parse(handle).ok_or_else(|| {
            TransactionError::not_found(format!("transaction '{handle}' not found"))
        })?;
        let scope = context.clone();
        self.owners()
            .map_err(|error| unavailable(error, handle))?
            .run(id, &caller(context), move |owned| {
                Box::pin(async move {
                    let ran = scope.mirror(within(timeout, job(owned))).await;
                    owned.after_request();
                    ran
                })
            })
            .await
            .map_err(|error| unavailable(error, handle))?
    }

    async fn read<T>(
        &self,
        timeout: Option<Duration>,
        job: TxnJob<T>,
    ) -> std::result::Result<T, TransactionError>
    where
        T: MaybeSend + 'static,
    {
        self.read_as(&RequestContext::ambient(), timeout, job).await
    }

    async fn read_as<T>(
        &self,
        context: &RequestContext,
        timeout: Option<Duration>,
        job: TxnJob<T>,
    ) -> std::result::Result<T, TransactionError>
    where
        T: MaybeSend + 'static,
    {
        let db = self.db.clone();
        let scope = context.clone();
        self.owners()
            .map_err(|error| TransactionError::execution(error.to_string()))?
            .run_anywhere("txn.read", move || async move {
                scope.mirror(async move {
                    let txn = db.new_txn(true).await.map_err(|error| {
                        TransactionError::execution(format!("storage error: {}", error))
                    })?;
                    // A snapshot of its own, never registered: no ID names it.
                    let mut owned = OwnedTxn::new(TxnId::unnamed(), txn, db, None)
                        .map_err(|error| TransactionError::execution(error.to_string()))?;
                    within(timeout, job(&mut owned)).await
                })
                .await
            })
            .await
            .map_err(|error| TransactionError::execution(error.to_string()))?
    }

    async fn commit(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.commit_as(&RequestContext::ambient(), handle).await
    }

    async fn commit_as(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let id = TxnId::parse(handle).ok_or_else(|| {
            TransactionError::not_found(format!("transaction '{handle}' not found"))
        })?;
        let label = handle.to_string();
        self.owners()
            .map_err(|error| unavailable(error, handle))?
            .finish(id, &caller(context), move |owned| {
                Box::pin(async move {
                    owned.commit().await.map_err(|error| {
                        TransactionError::execution(format!(
                            "commit error for transaction '{label}': {error}"
                        ))
                    })
                })
            })
            .await
            .map_err(|error| unavailable(error, handle))?
    }

    async fn rollback(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.rollback_as(&RequestContext::ambient(), handle).await
    }

    async fn rollback_as(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let id = TxnId::parse(handle).ok_or_else(|| {
            TransactionError::not_found(format!("transaction '{handle}' not found"))
        })?;
        let label = handle.to_string();
        self.owners()
            .map_err(|error| unavailable(error, handle))?
            .finish(id, &caller(context), move |owned| {
                Box::pin(async move {
                    owned.rollback().map_err(|error| {
                        TransactionError::execution(format!(
                            "rollback error for transaction '{label}': {error}"
                        ))
                    })
                })
            })
            .await
            .map_err(|error| unavailable(error, handle))?
    }
}
