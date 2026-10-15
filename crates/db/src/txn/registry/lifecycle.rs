//! Transaction begin, requests, commit, rollback and abandon, each a message
//! to the worker that owns the transaction.

use super::*;
use async_trait::async_trait;
use query::txn::{TransactionHandle, TransactionRegistry, TxnJob};
use storage::corekv::MaybeSend;
use workers::Target;

/// Runs `job` until it finishes or `timeout` passes on the worker's clock:
/// `None` then, and the job is dropped.
async fn within<F>(timeout: Option<Duration>, job: F) -> Option<F::Output>
where
    F: std::future::Future,
{
    let Some(sleep) = timeout.and_then(|limit| workers::sleep(limit).ok()) else {
        return Some(job.await);
    };
    match futures::future::select(Box::pin(job), sleep).await {
        futures::future::Either::Left((output, _)) => Some(output),
        futures::future::Either::Right(_) => None,
    }
}

fn timed_out(timeout: Option<Duration>) -> TransactionError {
    TransactionError::execution(format!(
        "query execution timed out after {} seconds",
        timeout.map_or(0, |limit| limit.as_secs())
    ))
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
        let db = self.db.clone();
        let broadcaster = self.broadcaster.clone();
        let opened = self
            .owners()
            .map_err(|error| TransactionError::execution(error.to_string()))?
            .begin(caller(), move |id| async move {
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
        let id = TxnId::parse(handle).ok_or_else(|| {
            TransactionError::not_found(format!("transaction '{handle}' not found"))
        })?;
        let caller = caller();
        let scope = caller.clone();
        self.owners()
            .map_err(|error| unavailable(error, handle))?
            .run(id, &caller, move |owned| {
                Box::pin(defra_core::current_identity::with_scoped_identity(
                    scope,
                    async move {
                        let ran = within(timeout, job(owned)).await;
                        owned.after_request();
                        ran.ok_or_else(|| timed_out(timeout))
                    },
                ))
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
        let pool = self
            .owners()
            .map_err(|error| TransactionError::execution(error.to_string()))?
            .pool()
            .clone();
        let db = self.db.clone();
        let scope = caller();
        let done = pool
            .submit(Target::LeastLoaded, move || {
                defra_core::current_identity::with_scoped_identity(scope, async move {
                    let txn = db.new_txn(true).await.map_err(|error| {
                        TransactionError::execution(format!("storage error: {}", error))
                    })?;
                    // A snapshot of its own, never registered: no ID names it.
                    let mut owned = OwnedTxn::new(TxnId::unnamed(), txn, db, None)
                        .map_err(|error| TransactionError::execution(error.to_string()))?;
                    within(timeout, job(&mut owned))
                        .await
                        .ok_or_else(|| timed_out(timeout))
                })
            })
            .await
            .map_err(|error| TransactionError::execution(error.to_string()))?;
        done.await
            .map_err(|error| TransactionError::execution(error.to_string()))?
    }

    async fn commit(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let id = TxnId::parse(handle).ok_or_else(|| {
            TransactionError::not_found(format!("transaction '{handle}' not found"))
        })?;
        let label = handle.to_string();
        self.owners()
            .map_err(|error| unavailable(error, handle))?
            .finish(id, &caller(), move |owned| {
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
        let id = TxnId::parse(handle).ok_or_else(|| {
            TransactionError::not_found(format!("transaction '{handle}' not found"))
        })?;
        let label = handle.to_string();
        self.owners()
            .map_err(|error| unavailable(error, handle))?
            .finish(id, &caller(), move |owned| {
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
