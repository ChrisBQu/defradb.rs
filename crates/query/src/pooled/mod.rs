//! The boundary between the network edge and the worker pool for GraphQL:
//! a [`QueryExecutor`] that runs each request on the worker its routing
//! names and hands the response back. Every boundary that
//! executes GraphQL (HTTP, Postgres wire, the embedded API, the C FFI)
//! holds the database's executor through one of these, so each request
//! crosses into the pool once and its result crosses back once.
//!
//! What a request does stays as the inner executor does it; only where it
//! runs changes:
//! - a write naming one document runs on that document's worker, one such
//!   task per document at a time;
//! - reads and everything else run on the least-loaded worker;
//! - an explicit transaction's requests go to the inner executor as they
//!   come: its transaction registry sends each to the worker that owns the
//!   transaction, so routing them here would add a hop;
//! - preparing a request's remote inputs (embeddings, keys) is network
//!   work: it runs where the caller is, before the request enters the pool.
//!
//! A request carries its [`RequestContext`] into the pool, and the
//! transaction calls that take one pass it through to the inner executor.

mod route;

use std::sync::Arc;

use async_trait::async_trait;

pub use route::document_key;

use defra_core::RequestContext;

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
    pub fn wrap(
        inner: Arc<dyn QueryExecutor>,
        pool: Option<&workers::Pool>,
    ) -> Arc<dyn QueryExecutor> {
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

/// `request` as it leaves its caller's thread for the pool: with the context
/// its caller set up taken along (see [`RequestContext::or_ambient`]; outside
/// tests this is `request` itself).
fn leaving_the_caller(mut request: QueryRequest) -> QueryRequest {
    request.context = std::mem::take(&mut request.context).or_ambient();
    request
}

/// A request the pool could not run: it was shutting down, or the task
/// panicked.
fn unavailable(error: workers::Error) -> QueryError {
    QueryError::execution(format!("the request did not run: {error}"))
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
        let request = leaving_the_caller(request);
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
        let request = leaving_the_caller(request);
        self.on_pool(
            target,
            move |inner| async move { inner.execute(request).await },
        )
        .await
        .unwrap_or_else(|error| QueryResponse::error(error.to_string()))
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        self.inner.execute_in_txn(request, handle).await
    }

    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.inner.begin_txn(readonly).await
    }

    async fn begin_txn_as(
        &self,
        context: &RequestContext,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.inner.begin_txn_as(context, readonly).await
    }

    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.inner.commit_txn(handle).await
    }

    async fn commit_txn_as(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.inner.commit_txn_as(context, handle).await
    }

    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.inner.rollback_txn(handle).await
    }

    async fn rollback_txn_as(
        &self,
        context: &RequestContext,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.inner.rollback_txn_as(context, handle).await
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
