//! Transaction begin, commit and rollback.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    fn take_owned_ctx(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<RemovedTransaction<S>, TransactionError> {
        self.transactions
            .get(handle.as_str())
            .and_then(|slot| slot.ctx())
            .filter(|ctx| ctx.is_owned_by(context))
            .and_then(|_| self.take_registered(handle))
            .ok_or_else(|| {
                TransactionError::not_found(format!("transaction '{}' not found", handle))
            })
    }

    async fn begin_with_options(
        &self,
        context: &RequestContext,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let txn_id = self.id_counter.fetch_add(1, Ordering::SeqCst).to_string();

        let mut db_txn = self
            .db
            .new_txn(readonly)
            .await
            .map_err(|e| TransactionError::execution(format!("storage error: {}", e)))?;
        db_txn.make_explicit();
        // Owed registrations are this transaction's own writes. Remote ACP
        // effects are registered as ordered callbacks with the mutation itself.
        let acp_registration_projection = db_txn.acp_registration_projection();

        // Transaction-scoped collection caching: collections are loaded lazily
        // from the SystemStore on first access within the transaction. Once loaded,
        // the collection metadata is cached for the transaction's duration.
        let lens_store = Arc::new(TxnLensStore::new(self.db.lens_store().clone()));
        let fetcher = Arc::new(LensedDocFetcher::new(self.db.clone(), db_txn, lens_store));
        let ctx = Arc::new(DbTransactionContext::new_with_broadcaster(
            self.db.clone(),
            txn_id.clone(),
            readonly,
            context.identity().map(str::to_owned),
            fetcher,
            acp_registration_projection,
            self.broadcaster.clone(),
        ));

        // Registration and returning the handle must be one poll: cancellation
        // before the caller can construct its guard must not orphan an entry.
        self.transactions
            .insert(txn_id.clone(), Arc::new(TransactionSlot::new(ctx)));

        Ok(TransactionHandle::new(txn_id))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> TransactionRegistry for DbTransactionRegistry<S> {
    fn abandon(&self, handle: &TransactionHandle) {
        drop(self.take_registered(handle));
    }

    async fn begin(
        &self,
        context: &RequestContext,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_with_options(context, readonly).await
    }

    async fn begin_implicit_read(
        &self,
        context: &RequestContext,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.begin_with_options(context, true).await
    }

    fn get(&self, context: &RequestContext, handle: &TransactionHandle) -> GetTransactionResult {
        match self
            .transactions
            .get(handle.as_str())
            .and_then(|slot| slot.ctx())
        {
            Some(ctx) if ctx.is_owned_by(context) && ctx.touch() => {
                GetTransactionResult::Found(ctx as Arc<dyn TransactionContext>)
            }
            _ => GetTransactionResult::NotFound,
        }
    }

    async fn commit(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self.take_owned_ctx(context, handle)?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;
        ctx.recover_pending_statement_sync()
            .map_err(TransactionError::execution)?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_commit().await.map_err(|error| {
            TransactionError::execution(format!("commit error for transaction '{handle}': {error}"))
        })
    }

    async fn rollback(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self.take_owned_ctx(context, handle)?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;
        ctx.recover_pending_statement_sync()
            .map_err(TransactionError::execution)?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed (double commit/rollback?)",
                handle
            ))
        })?;

        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "rollback error for transaction '{}': {}",
                handle, e
            ))
        })
    }

    async fn finish_implicit_read(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
        _apply_read_effects: bool,
    ) -> std::result::Result<(), TransactionError> {
        let ctx = self.take_owned_ctx(context, handle)?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;
        ctx.recover_pending_statement_sync()
            .map_err(TransactionError::execution)?;

        let txn = ctx.take_txn().await.ok_or_else(|| {
            TransactionError::already_finalized(format!(
                "transaction '{}' was already consumed",
                handle
            ))
        })?;
        txn.force_discard().map_err(|e| {
            TransactionError::execution(format!(
                "failed to close implicit read transaction '{}': {}",
                handle, e
            ))
        })?;

        Ok(())
    }
}
