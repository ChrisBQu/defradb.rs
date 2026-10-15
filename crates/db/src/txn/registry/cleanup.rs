//! The idle-transaction sweep: a timer on each worker that ends the
//! transactions it owns once no request has used them for the idle limit.
//! The sweep's ask is a message queued behind the requests already there, so
//! a request that arrived first keeps its transaction. Subject of
//! proofs/tla/TxnRegistry.tla.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Ends every transaction no request has used for longer than
    /// `max_idle_age`, each after the requests queued for it before this, and
    /// says how many ended.
    ///
    /// Owning `TransactionGuard`s clean up on drop without waiting for a sweep.
    pub async fn cleanup_stale_transactions(
        &self,
        max_idle_age: Duration,
    ) -> Result<CleanupResult> {
        let cleaned = self
            .owners()
            .map_err(|error| Error::Other(error.to_string()))?
            .expire_idle(max_idle_age)
            .await
            .map_err(|error| Error::Other(error.to_string()))?;
        Ok(CleanupResult {
            cleaned,
            failed: Vec::new(),
        })
    }

    /// Starts the periodic sweep on every worker: every `sweep_interval`, each
    /// worker ends the transactions it owns that no request has used for
    /// longer than `max_idle_age`. Runs until the returned handle is stopped,
    /// or the registry or its pool goes.
    pub fn start_stale_transaction_cleanup(
        &self,
        max_idle_age: Duration,
        sweep_interval: Duration,
    ) -> Sweep {
        if sweep_interval.is_zero() {
            tracing::error!("transaction cleanup sweep interval must be above zero; no sweep runs");
            return Sweep::stopped();
        }
        match self
            .owners()
            .and_then(|owners| owners.sweep(max_idle_age, sweep_interval))
        {
            Ok(sweep) => sweep,
            Err(error) => {
                tracing::error!(%error, "the idle transaction sweep could not start");
                Sweep::stopped()
            }
        }
    }
}
