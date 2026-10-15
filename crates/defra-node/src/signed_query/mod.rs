//! Queries the node runs as its own signing identity.
//!
//! The signing config and batch session travel in the request's context.
//! The acting identity is a thread-local, so a signed query runs pinned to
//! one thread of the network runtime ([`Edge::spawn_pinned`]) with it set
//! on it, and it keeps running if its caller stops waiting.

use std::sync::Arc;

use defra_core::signing::SigningConfig;
use edge::{Edge, EdgeError};
use query::{QueryExecutor, QueryRequest, QueryResponse, TransactionHandle};

#[cfg(test)]
mod tests;

pub(super) async fn execute_with_signing_context(
    edge: &Edge,
    executor: Arc<dyn QueryExecutor>,
    mut request: QueryRequest,
    txn_handle: Option<TransactionHandle>,
    signing_config: SigningConfig,
    node_did: String,
) -> QueryResponse {
    let batch_session = Some(signing_config.public_key_hex.clone());
    request.context = request
        .context
        .with_signing(Some(signing_config))
        .with_batch_session(batch_session);
    edge.spawn_pinned(move || {
        let identity_guard = defra_core::current_identity::scoped_current_identity(Some(node_did));
        async move {
            let _identity_guard = identity_guard;
            match txn_handle {
                Some(txn_handle) => executor.execute_in_txn(request, &txn_handle).await,
                None => super::execute_autocommit_request(executor.as_ref(), request).await,
            }
        }
    })
    .await
    .unwrap_or_else(|error| match error {
        EdgeError::Closed => QueryResponse::error("the node is shutting down"),
        error => QueryResponse::error(format!("query execution task failed: {error}")),
    })
}

pub(super) fn unavailable_node_signer_response(node_did: &str) -> QueryResponse {
    QueryResponse::error(format!(
        "configured node signing identity {node_did} is unavailable"
    ))
}
