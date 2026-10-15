//! Queries the node runs as its own signing identity.
//!
//! The signing config, batch session key and acting identity are
//! thread-locals, so a signed query runs pinned to one thread of the
//! network runtime ([`Edge::spawn_pinned`]) with them set on it, and it
//! keeps running if its caller stops waiting.

use std::sync::Arc;

use defra_core::signing::SigningConfig;
use edge::{Edge, EdgeError};
use query::{QueryExecutor, QueryRequest, QueryResponse, TransactionHandle};

#[cfg(test)]
mod tests;

pub(super) async fn execute_with_signing_context(
    edge: &Edge,
    executor: Arc<dyn QueryExecutor>,
    request: QueryRequest,
    txn_handle: Option<TransactionHandle>,
    signing_config: SigningConfig,
    node_did: String,
) -> QueryResponse {
    let batch_session_key = Some(signing_config.public_key_hex.clone());
    edge.spawn_pinned(move || {
        let signing_guard = ThreadSigningContextGuard::install(signing_config, batch_session_key);
        let identity_guard = defra_core::current_identity::scoped_current_identity(Some(node_did));
        async move {
            let _signing_guard = signing_guard;
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

struct ThreadSigningContextGuard {
    previous_signing_config: Option<SigningConfig>,
    previous_batch_session_key: Option<String>,
}

impl ThreadSigningContextGuard {
    fn install(signing_config: SigningConfig, batch_session_key: Option<String>) -> Self {
        let previous_signing_config = defra_core::signing::get_signing_config();
        let previous_batch_session_key = defra_core::batch_signing::get_batch_session_key();
        defra_core::signing::set_signing_config(Some(signing_config));
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        Self {
            previous_signing_config,
            previous_batch_session_key,
        }
    }
}

impl Drop for ThreadSigningContextGuard {
    fn drop(&mut self) {
        defra_core::signing::set_signing_config(self.previous_signing_config.take());
        defra_core::batch_signing::set_batch_session_key(self.previous_batch_session_key.take());
    }
}
