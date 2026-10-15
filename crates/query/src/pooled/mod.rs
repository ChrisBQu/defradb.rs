//! The boundary between the network edge and the worker pool for GraphQL:
//! a [`QueryExecutor`] that runs each request on the worker its routing
//! names ([`route`]) and hands the response back. Every boundary that
//! executes GraphQL (HTTP, Postgres wire, the embedded API, the C FFI)
//! holds the database's executor through one of these, so each request
//! crosses into the pool once and its result crosses back once.
//!
//! What a request does stays as the inner executor does it; only where it
//! runs changes:
//! - a write naming one document runs on that document's worker, one such
//!   task per document at a time;
//! - every request of an explicit transaction runs on that transaction's
//!   worker, one at a time, in arrival order;
//! - reads and everything else run on the least-loaded worker;
//! - preparing a request's remote inputs (embeddings, keys) is network
//!   work: it runs where the caller is, before the request enters the pool.

mod route;

use std::sync::Arc;

use async_trait::async_trait;

pub use route::document_key;

use crate::error::{QueryError, Result, TransactionError};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse};
use crate::prepared::{PreparedInputRequest, PreparedMutations};
use crate::txn::TransactionHandle;

/// A [`QueryExecutor`] whose requests run on `pool`.
pub struct PooledExecutor {
    inner: Arc<dyn QueryExecutor>,
    pool: workers::Pool,
}

impl PooledExecutor {
    /// Runs `inner`'s requests on `pool`.
    pub fn new(inner: Arc<dyn QueryExecutor>, pool: workers::Pool) -> Self {
        Self { inner, pool }
    }

    /// `inner` as the boundary holds it: on `pool` when the database has one,
    /// as it is otherwise (a test, a one-shot tool).
    pub fn wrap(inner: Arc<dyn QueryExecutor>, pool: Option<&workers::Pool>) -> Arc<dyn QueryExecutor> {
        match pool {
            Some(pool) => Arc::new(Self::new(inner, pool.clone())),
            None => inner,
        }
    }

    /// Runs `call` on `inner` on the worker `target` names, and hands its
    /// output back; why it has none when the pool could not run it.
    async fn on_pool<T, F, Fut>(&self, target: workers::Target, call: F) -> Result<T>
    where
        F: FnOnce(Arc<dyn QueryExecutor>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let done = self
            .pool
            .submit(target, move || call(inner))
            .await
            .map_err(unavailable)?;
        done.await.map_err(unavailable)
    }
}

/// A request the pool could not run: it was shutting down, or the task
/// panicked.
fn unavailable(error: workers::Error) -> QueryError {
    QueryError::execution(format!("the request did not run: {error}"))
}

fn txn_unavailable(error: QueryError) -> TransactionError {
    TransactionError::execution(error.to_string())
}

#[async_trait]
impl QueryExecutor for PooledExecutor {
    async fn prepare_request(
        &self,
        request: &QueryRequest,
    ) -> Result<Option<Arc<PreparedMutations>>> {
        self.inner.prepare_request(request).await
    }

    async fn execute_prepared(
        &self,
        request: QueryRequest,
        prepared: Option<Arc<PreparedMutations>>,
    ) -> QueryResponse {
        let target = route::request_target(&request);
        self.on_pool(target, move |inner| async move {
            inner.execute_prepared(request, prepared).await
        })
        .await
        .unwrap_or_else(|error| QueryResponse::error(error.to_string()))
    }

    async fn resolve_retry_input(
        &self,
        request: &QueryRequest,
        prepared: Option<Arc<PreparedMutations>>,
        input: &PreparedInputRequest,
    ) -> Result<Option<Arc<PreparedMutations>>> {
        self.inner
            .resolve_retry_input(request, prepared, input)
            .await
    }

    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        let target = route::request_target(&request);
        self.on_pool(target, move |inner| async move { inner.execute(request).await })
            .await
            .unwrap_or_else(|error| QueryResponse::error(error.to_string()))
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        let target = route::transaction_target(handle);
        let handle = handle.clone();
        self.on_pool(target, move |inner| async move {
            inner.execute_in_txn(request, &handle).await
        })
        .await
        .unwrap_or_else(|error| QueryResponse::error(error.to_string()))
    }

    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.on_pool(workers::Target::LeastLoaded, move |inner| async move {
            inner.begin_txn(readonly).await
        })
        .await
        .map_err(txn_unavailable)?
    }

    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let target = route::transaction_target(handle);
        let handle = handle.clone();
        self.on_pool(target, move |inner| async move {
            inner.commit_txn(&handle).await
        })
        .await
        .map_err(txn_unavailable)?
    }

    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        let target = route::transaction_target(handle);
        let handle = handle.clone();
        self.on_pool(target, move |inner| async move {
            inner.rollback_txn(&handle).await
        })
        .await
        .map_err(txn_unavailable)?
    }

    fn abandon_txn(&self, handle: &TransactionHandle) {
        self.inner.abandon_txn(handle);
    }

    async fn schema(&self) -> Result<String> {
        self.on_pool(workers::Target::LeastLoaded, |inner| async move {
            inner.schema().await
        })
        .await?
    }
}

#[cfg(test)]
mod tests;
