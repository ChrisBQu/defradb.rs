//! The REST routes whose work is the data path run on the worker pool.
//!
//! [`OnPool`] is the server's outermost route layer: for a route under one of
//! [`ON_POOL`]'s prefixes, the whole route (the auth middleware, the body's
//! extraction, the handler and the storage reads and writes it makes) runs as
//! one task on the database's pool, and the response comes back to the edge.
//! A request that names one document (`/collections/{name}/document/{docID}`)
//! runs on that document's worker, where every write to it runs; every other
//! request runs on the least-loaded worker.
//!
//! Every other route stays on the network edge: the explicit transaction
//! routes (their requests go to the worker that owns the transaction), the
//! P2P and document ACP routes (they reach peers and remote ACP providers
//! through the network), event and GraphQL subscription streams, and the
//! routes with no storage behind them. GraphQL requests reach the pool
//! through the query executor.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::extract::{MatchedPath, Request};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use tower::{Layer, Service, ServiceExt};

/// The route prefixes, under each API prefix (`go_paths::API_PREFIXES`),
/// whose requests run on the pool.
pub const ON_POOL: [&str; 9] = [
    "/collections",
    "/index",
    "/backup",
    "/views",
    "/view",
    "/block",
    "/lens",
    "/acp/node",
    "/actions",
];

/// Whether the route `path` (as matched) runs on the pool.
pub fn runs_on_pool(path: &str) -> bool {
    crate::go_paths::API_PREFIXES
        .iter()
        .filter_map(|api| path.strip_prefix(api))
        .any(|route| {
            route == "/schema"
                || ON_POOL.iter().any(|prefix| {
                    route
                        .strip_prefix(prefix)
                        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
                })
        })
}

/// The worker a request runs on: its document's, when its path names one
/// (`/collections/{name}/document/{docID}`, keyed as a GraphQL write to it
/// is).
pub fn route_target(path: &str) -> workers::Target {
    let mut segments = path.split('/').skip_while(|segment| *segment != "collections");
    match (segments.nth(1), segments.next(), segments.next()) {
        (Some(collection), Some("document"), Some(doc_id))
            if !collection.is_empty() && !doc_id.is_empty() =>
        {
            workers::Target::Key(query::pooled::document_key(collection, doc_id))
        }
        _ => workers::Target::LeastLoaded,
    }
}

/// The layer that runs data-path routes on `pool`.
#[derive(Clone)]
pub struct OnPool {
    pool: workers::Pool,
}

impl OnPool {
    /// Runs the data-path routes of the router it is laid over on `pool`.
    pub fn new(pool: workers::Pool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for OnPool {
    type Service = OnPoolService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OnPoolService {
            inner,
            pool: self.pool.clone(),
        }
    }
}

/// [`OnPool`] over one route.
#[derive(Clone)]
pub struct OnPoolService<S> {
    inner: S,
    pool: workers::Pool,
}

impl<S> Service<Request> for OnPoolService<S>
where
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let path = request
            .extensions()
            .get::<MatchedPath>()
            .map(|matched| matched.as_str().to_owned());
        let inner = self.inner.clone();
        let Some(path) = path.filter(|path| runs_on_pool(path)) else {
            return Box::pin(inner.oneshot(request));
        };
        let target = route_target(request.uri().path());
        let pool = self.pool.clone();
        Box::pin(async move {
            match workers::run_on(Some(&pool), target, move || inner.oneshot(request)).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(route = %path, %error, "a request did not run on the worker pool");
                    Ok((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "the node is shutting down; try again",
                    )
                        .into_response())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_path_routes_run_on_the_pool_and_network_routes_do_not() {
        for path in [
            "/api/v0/collections",
            "/api/v0/collections/{name}/document/{docID}",
            "/api/v0/index",
            "/api/v0/backup/export",
            "/api/v0/view/refresh",
            "/api/v0/acp/node/status",
            "/api/v0/schema",
            "/api/v1/collections",
            "/api/collections",
        ] {
            assert!(runs_on_pool(path), "{path}");
        }
        for path in [
            "/api/v0/graphql",
            "/api/v0/tx/{id}",
            "/api/v0/p2p/replicators",
            "/api/v0/acp/policy",
            "/api/v0/events",
            "/api/v0/collectionsx",
            "/health-check",
        ] {
            assert!(!runs_on_pool(path), "{path}");
        }
    }

    #[test]
    fn a_request_naming_one_document_goes_to_its_worker() {
        assert_eq!(
            route_target("/api/v0/collections/User/document/bae-1"),
            workers::Target::Key(query::pooled::document_key("User", "bae-1"))
        );
        assert_eq!(
            route_target("/api/v0/collections/User"),
            workers::Target::LeastLoaded
        );
    }
}
