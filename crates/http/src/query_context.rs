//! Query execution with thread-local signing and DAC bypass context.
//!
//! HTTP handlers must set `signing_config` and `dac_bypass` thread-locals
//! before executing queries to match FFI behavior. A task on a
//! multi-threaded runtime can move between OS threads at `.await` points,
//! so a query that needs them runs pinned to one thread of the network
//! runtime ([`Edge::spawn_pinned`]), the thread-locals set on it first.

use std::future::Future;
use std::sync::Arc;

use defra_core::signing::SigningConfig;
use edge::Edge;
use query::executor::{QueryExecutor, QueryRequest, QueryResponse};
use query::txn::TransactionHandle;

use crate::identity_extractor::ExtractIdentity;
use crate::router::AppState;

/// Execute a GraphQL query with signing config and DAC bypass context.
pub async fn execute_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
) -> QueryResponse {
    let signing_config = resolve_signing_config(state, identity);

    let dac_bypass = resolve_dac_bypass(state, identity).await;

    // Fast path: when there is nothing to put on the thread-locals, skip
    // spawn_blocking entirely.
    if signing_config.is_none() && state.nac.is_none() && !dac_bypass {
        return execute_prepared_request(state, request).await;
    }
    let executor = state.executor.clone();
    let acting_did = identity.did().map(|d| d.as_str().to_string());
    run_pinned_with_context(
        &state.edge,
        signing_config,
        dac_bypass,
        acting_did,
        move || async move { execute_prepared_with_executor(executor.as_ref(), request).await },
    )
    .await
}

async fn execute_prepared_request(state: &AppState, request: QueryRequest) -> QueryResponse {
    execute_prepared_with_executor(state.executor.as_ref(), request).await
}

async fn execute_prepared_with_executor(
    executor: &dyn QueryExecutor,
    request: QueryRequest,
) -> QueryResponse {
    let prepared = match executor.prepare_request(&request).await {
        Ok(prepared) => prepared,
        Err(error) => {
            return QueryResponse::error(query::executor::QueryResponseError::from_query_error(
                error,
            ))
        }
    };
    execute_prepared_request_resolving_inputs(executor, request, prepared).await
}

async fn execute_prepared_request_resolving_inputs(
    executor: &dyn QueryExecutor,
    request: QueryRequest,
    mut prepared: Option<Arc<query::prepared::PreparedMutations>>,
) -> QueryResponse {
    loop {
        let response = executor
            .execute_prepared(request.clone(), prepared.clone())
            .await;
        if let Some(input) = response.retry_input() {
            match executor
                .resolve_retry_input(&request, prepared.clone(), input)
                .await
            {
                Ok(Some(updated)) => {
                    prepared = Some(updated);
                    continue;
                }
                Ok(None) => {
                    return QueryResponse::error(
                        "retry input resolution returned no preparation capsule",
                    )
                }
                Err(error) => {
                    return QueryResponse::error(
                        query::executor::QueryResponseError::from_query_error(error),
                    )
                }
            }
        }
        return response;
    }
}

/// Execute a GraphQL query within a transaction with signing config and DAC bypass context.
pub async fn execute_in_txn_with_context(
    state: &AppState,
    identity: &ExtractIdentity,
    request: QueryRequest,
    txn_handle: TransactionHandle,
) -> QueryResponse {
    let signing_config = resolve_signing_config(state, identity);

    let dac_bypass = resolve_dac_bypass(state, identity).await;

    // Fast path: see the note in `execute_with_context`.
    if signing_config.is_none() && state.nac.is_none() && !dac_bypass {
        return state.executor.execute_in_txn(request, &txn_handle).await;
    }

    let executor = state.executor.clone();
    let acting_did = identity.did().map(|d| d.as_str().to_string());
    run_pinned_with_context(
        &state.edge,
        signing_config,
        dac_bypass,
        acting_did,
        move || async move { executor.execute_in_txn(request, &txn_handle).await },
    )
    .await
}

/// Execute a query pinned to one thread with pre-resolved context values.
///
/// Used by SSE streams where signing config and DAC bypass are resolved once
/// at subscription setup time, then reused for each per-event query.
pub async fn execute_with_resolved_context(
    edge: &Edge,
    executor: Arc<dyn QueryExecutor>,
    request: QueryRequest,
    signing_config: Option<SigningConfig>,
    dac_bypass: bool,
    acting_did: Option<String>,
) -> QueryResponse {
    run_pinned_with_context(
        edge,
        signing_config,
        dac_bypass,
        acting_did,
        move || async move { executor.execute(request).await },
    )
    .await
}

/// Runs the query `run` builds on one thread of the network runtime, with
/// the request's signing config, batch session key, DAC bypass, acting
/// identity and bearer token set on that thread first.
async fn run_pinned_with_context<R, Fut>(
    edge: &Edge,
    signing_config: Option<SigningConfig>,
    dac_bypass: bool,
    acting_did: Option<String>,
    run: R,
) -> QueryResponse
where
    R: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = QueryResponse> + 'static,
{
    let batch_session_key = signing_config.as_ref().map(|s| s.public_key_hex.clone());
    let bearer = acting_did
        .as_deref()
        .and_then(defra_core::signing::get_request_bearer_token);
    edge.spawn_pinned(move || {
        let identity_guard =
            defra_core::current_identity::scoped_current_identity(acting_did.clone());
        defra_core::signing::set_signing_config(signing_config);
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        defra_core::dac_bypass::set_dac_bypass(dac_bypass);
        async move {
            let _identity_guard = identity_guard;
            defra_core::signing::scope_request_bearer_token(acting_did, bearer, run()).await
        }
    })
    .await
    .unwrap_or_else(|error| QueryResponse::error(format!("query execution task failed: {error}")))
}

/// Resolve the signing config for a request identity.
///
/// - If identity has a DID, look up its signing config.
/// - If no DID (anonymous), fall back to the node identity DID.
pub(crate) fn resolve_signing_config(
    state: &AppState,
    identity: &ExtractIdentity,
) -> Option<SigningConfig> {
    defra_core::signing::resolve_signing_config_with_flag(
        identity.did().map(|d| d.as_str()),
        state.node_identity_did.as_deref(),
        state.signing_enabled,
    )
}

/// Resolve whether DAC bypass should be enabled for this request.
///
/// - If NAC is configured and the identity has `DacBypass` permission, bypass.
/// - Otherwise, no bypass.
pub(crate) async fn resolve_dac_bypass(state: &AppState, identity: &ExtractIdentity) -> bool {
    let Some(nac) = &state.nac else {
        return false;
    };

    let status = nac.get_status().await;
    acp::nac::should_bypass_dac(status, identity.did(), |d, p| async move {
        nac.check_permission(d, p).await.unwrap_or(false)
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use query::error::{Result, TransactionError};

    use super::*;
    use crate::router::AppStateBuilder;

    #[derive(Default)]
    struct ConflictExecutor {
        preparations: AtomicUsize,
        auto_commit_attempts: AtomicUsize,
        explicit_txn_attempts: AtomicUsize,
        input_on_second: bool,
        input_resolutions: AtomicUsize,
        engine_managed_retry: bool,
    }

    #[async_trait]
    impl QueryExecutor for ConflictExecutor {
        async fn prepare_request(
            &self,
            _request: &QueryRequest,
        ) -> Result<Option<Arc<query::prepared::PreparedMutations>>> {
            self.preparations.fetch_add(1, Ordering::SeqCst);
            Ok(Some(Arc::new(query::prepared::PreparedMutations {
                engine_managed_retry: self.engine_managed_retry,
                ..Default::default()
            })))
        }

        fn abandon_txn(&self, _handle: &TransactionHandle) {}

        async fn execute(&self, _request: QueryRequest) -> QueryResponse {
            assert_eq!(self.preparations.load(Ordering::SeqCst), 1);
            let attempt = self.auto_commit_attempts.fetch_add(1, Ordering::SeqCst);
            if self.input_on_second {
                match attempt {
                    0 => QueryResponse::error(query::QueryResponseError::from_query_error(
                        query::error::QueryError::NeedsInput(
                            query::prepared::PreparedInputRequest::new(
                                query::prepared::PreparedInputKind::Embedding,
                                br#"{"model":"m","input":"source"}"#.as_slice(),
                            ),
                        ),
                    )),
                    1 => QueryResponse::transaction_conflict("transaction conflict"),
                    _ => QueryResponse::success(serde_json::json!({"_docID": "doc"})),
                }
            } else if attempt < 2 {
                QueryResponse::transaction_conflict("transaction conflict")
            } else {
                QueryResponse::success(serde_json::json!({"_docID": "doc"}))
            }
        }

        async fn resolve_retry_input(
            &self,
            _request: &QueryRequest,
            prepared: Option<Arc<query::prepared::PreparedMutations>>,
            input: &query::prepared::PreparedInputRequest,
        ) -> Result<Option<Arc<query::prepared::PreparedMutations>>> {
            self.input_resolutions.fetch_add(1, Ordering::SeqCst);
            let Some(prepared) = prepared else {
                return Ok(None);
            };
            if prepared.inputs.get(input).is_none() {
                prepared.inputs.insert(input.clone(), b"[1.0]".as_slice());
            }
            Ok(Some(prepared))
        }

        async fn execute_in_txn(
            &self,
            _request: QueryRequest,
            _handle: &TransactionHandle,
        ) -> QueryResponse {
            self.explicit_txn_attempts.fetch_add(1, Ordering::SeqCst);
            QueryResponse::transaction_conflict("transaction conflict")
        }

        async fn begin_txn(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn commit_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn rollback_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn schema(&self) -> Result<String> {
            Ok(String::new())
        }
    }

    #[tokio::test]
    async fn auto_commit_returns_business_conflict_without_replaying_request() {
        let executor = Arc::new(ConflictExecutor::default());
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge()).build();

        let response = execute_with_context(
            &state,
            &ExtractIdentity::anonymous(),
            QueryRequest::new("mutation { create_User(input: {}) { _docID } }"),
        )
        .await;

        assert_eq!(executor.preparations.load(Ordering::SeqCst), 1);
        assert_eq!(executor.auto_commit_attempts.load(Ordering::SeqCst), 1);
        assert!(response.is_transaction_conflict());
    }

    #[tokio::test]
    async fn engine_managed_batch_conflict_is_not_replayed_by_http() {
        let executor = Arc::new(ConflictExecutor {
            engine_managed_retry: true,
            ..Default::default()
        });
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge()).build();

        let response = execute_with_context(
            &state,
            &ExtractIdentity::anonymous(),
            QueryRequest::new("mutation { first: create_User(input: {}) { _docID } second: create_User(input: {}) { _docID } }"),
        )
        .await;

        assert!(response.is_transaction_conflict());
        assert_eq!(executor.preparations.load(Ordering::SeqCst), 1);
        assert_eq!(executor.auto_commit_attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn auto_commit_resolves_input_then_returns_conflict_without_replaying_it() {
        let executor = Arc::new(ConflictExecutor {
            input_on_second: true,
            ..Default::default()
        });
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge()).build();

        let response = execute_with_context(
            &state,
            &ExtractIdentity::anonymous(),
            QueryRequest::new("mutation { update_User(input: {}) { _docID } }"),
        )
        .await;

        assert_eq!(executor.preparations.load(Ordering::SeqCst), 1);
        assert_eq!(executor.auto_commit_attempts.load(Ordering::SeqCst), 2);
        assert_eq!(executor.input_resolutions.load(Ordering::SeqCst), 1);
        assert!(response.is_transaction_conflict());
    }

    #[tokio::test]
    async fn explicit_transaction_execution_is_not_retried() {
        let executor = Arc::new(ConflictExecutor::default());
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge()).build();

        let response = execute_in_txn_with_context(
            &state,
            &ExtractIdentity::anonymous(),
            QueryRequest::new("mutation { update_User(input: {}) { _docID } }"),
            TransactionHandle::new("txn".to_string()),
        )
        .await;

        assert_eq!(executor.explicit_txn_attempts.load(Ordering::SeqCst), 1);
        assert!(response.is_transaction_conflict());
    }

    const OWNER_DID: &str = "did:key:z6MkTestNodeOwner";

    /// Records the `dac_bypass` thread-local as observed by the executor, which
    /// is the only place the fast path's effect is visible.
    #[derive(Default)]
    struct DacBypassRecorder {
        observed: std::sync::atomic::AtomicBool,
        bearer: std::sync::Mutex<Option<String>>,
    }

    #[async_trait]
    impl QueryExecutor for DacBypassRecorder {
        fn abandon_txn(&self, _handle: &TransactionHandle) {}

        async fn execute(&self, _request: QueryRequest) -> QueryResponse {
            self.observed
                .store(defra_core::dac_bypass::get_dac_bypass(), Ordering::SeqCst);
            *self.bearer.lock().unwrap() = defra_core::signing::get_request_bearer_token(OWNER_DID);
            QueryResponse::success(serde_json::json!({}))
        }

        async fn execute_in_txn(
            &self,
            _request: QueryRequest,
            _handle: &TransactionHandle,
        ) -> QueryResponse {
            self.observed
                .store(defra_core::dac_bypass::get_dac_bypass(), Ordering::SeqCst);
            *self.bearer.lock().unwrap() = defra_core::signing::get_request_bearer_token(OWNER_DID);
            QueryResponse::success(serde_json::json!({}))
        }

        async fn begin_txn(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn commit_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn rollback_txn(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Err(TransactionError::not_supported("test executor"))
        }

        async fn schema(&self) -> Result<String> {
            Ok(String::new())
        }
    }

    fn owner_identity() -> ExtractIdentity {
        ExtractIdentity::from_did(Some(identity::Did::new_unchecked(OWNER_DID.to_string())))
    }

    #[tokio::test]
    async fn node_owner_is_not_promoted_to_nac_dac_bypass() {
        let executor = Arc::new(DacBypassRecorder::default());
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge())
            .with_node_identity_did(OWNER_DID.to_string())
            .with_signing_enabled(false)
            .build();

        execute_with_context(
            &state,
            &owner_identity(),
            QueryRequest::new("{ __typename }"),
        )
        .await;

        assert!(!executor.observed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_stranger_gets_no_dac_bypass() {
        let executor = Arc::new(DacBypassRecorder::default());
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge())
            .with_node_identity_did(OWNER_DID.to_string())
            .with_signing_enabled(false)
            .build();

        execute_with_context(
            &state,
            &ExtractIdentity::from_did(Some(identity::Did::new_unchecked(
                "did:key:z6MkSomeoneElse".to_string(),
            ))),
            QueryRequest::new("{ __typename }"),
        )
        .await;

        assert!(!executor.observed.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn bearer_scope_crosses_each_blocking_execution_boundary() {
        let executor = Arc::new(DacBypassRecorder::default());
        let state = AppStateBuilder::new(executor.clone(), edge::testing::edge())
            .with_signing_enabled(false)
            .with_nac(Arc::new(crate::MockNodeAcpOperations::new()))
            .build();
        for path in 0..3 {
            *executor.bearer.lock().unwrap() = None;
            defra_core::signing::scope_request_bearer_token(
                Some(OWNER_DID.to_string()),
                Some("request-token".to_string()),
                async {
                    let request = QueryRequest::new("{ __typename }");
                    match path {
                        0 => execute_with_context(&state, &owner_identity(), request).await,
                        1 => {
                            execute_in_txn_with_context(
                                &state,
                                &owner_identity(),
                                request,
                                TransactionHandle::new("txn".into()),
                            )
                            .await
                        }
                        _ => {
                            execute_with_resolved_context(
                                &state.edge,
                                executor.clone(),
                                request,
                                None,
                                false,
                                Some(OWNER_DID.to_string()),
                            )
                            .await
                        }
                    }
                },
            )
            .await;
            assert_eq!(
                executor.bearer.lock().unwrap().as_deref(),
                Some("request-token")
            );
        }
    }
}
