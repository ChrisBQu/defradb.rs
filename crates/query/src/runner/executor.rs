//! QueryExecutor trait implementation for QueryRunner.

use async_trait::async_trait;
use rapidhash::RapidHashMap;
use serde_json::Value as JsonValue;
use std::future::Future;
use std::sync::Arc;
use tracing::{instrument, warn};

use acp::nac::NodePermission;
use identity::Did;

use crate::error::{Result, TransactionError};
use crate::executor::{GqlWarning, QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::query_parse::{parse_request_with_limits, validate_parsed_operation, ParsedOperation};
use crate::txn::{
    GetTransactionResult, TransactionContext, TransactionHandle, TransactionRegistry,
};

use super::{DocFetcher, QueryRunner};

struct StatementRollbackGuard {
    transaction: Arc<dyn TransactionContext>,
    armed: bool,
}

impl StatementRollbackGuard {
    fn new(transaction: Arc<dyn TransactionContext>) -> Self {
        Self {
            transaction,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StatementRollbackGuard {
    fn drop(&mut self) {
        if self.armed {
            self.transaction.mark_statement_rollback_pending();
        }
    }
}

/// Await a future with an optional timeout (native only, WASM always awaits
/// directly). The deadline is a timer of the worker pool's deadline queue:
/// the calling worker's, or the fallback pool's off the pool.
#[cfg(not(target_arch = "wasm32"))]
async fn await_with_timeout<T, F: Future<Output = Result<T>>>(
    future: F,
    timeout_secs: u64,
) -> Result<T> {
    if timeout_secs > 0 {
        let timeout = std::time::Duration::from_secs(timeout_secs);
        match workers::timeout(timeout, future).await {
            Ok(r) => r,
            Err(workers::Error::TimedOut) => Err(crate::error::QueryError::execution(format!(
                "query execution timed out after {} seconds",
                timeout_secs
            ))),
            Err(error) => Err(crate::error::QueryError::execution(format!(
                "the query's deadline could not be kept: {error}"
            ))),
        }
    } else {
        future.await
    }
}

#[cfg(target_arch = "wasm32")]
async fn await_with_timeout<T, F: Future<Output = Result<T>>>(
    future: F,
    _timeout_secs: u64,
) -> Result<T> {
    future.await
}

/// Map a parsed operation to the required NAC permission.
fn permission_for_operation(parsed: &ParsedOperation) -> NodePermission {
    match parsed {
        ParsedOperation::Query { .. } => NodePermission::DocumentRead,
        ParsedOperation::Subscription { .. } => NodePermission::DocumentRead,
        ParsedOperation::Introspection { .. } => NodePermission::DocumentRead,
        ParsedOperation::Mutation { mutations, .. } => {
            if mutations
                .iter()
                .any(|m| m.mutation_type == crate::mapper::MutationType::Truncate)
            {
                NodePermission::CollectionTruncate
            } else if mutations
                .iter()
                .any(|m| m.mutation_type == crate::mapper::MutationType::Delete)
            {
                NodePermission::DocumentDelete
            } else {
                NodePermission::DocumentUpdate
            }
        }
    }
}

/// Check NAC permission and return a denial response if not authorized.
///
/// Go enforces NAC at the data layer — denied queries return HTTP 200 with
/// empty data (not a GraphQL error). We match that by returning an empty
/// JSON object as data when NAC denies a request.
pub(super) async fn check_nac<F: DocFetcher + 'static, R: crate::txn::TransactionRegistry>(
    runner: &QueryRunner<F, R>,
    identity: &Option<Did>,
    parsed: &ParsedOperation,
) -> Option<QueryResponse> {
    let did = match identity {
        Some(d) => d.clone(),
        None => Did::wildcard(),
    };
    let permission = permission_for_operation(parsed);
    if !runner.nac.check_permission(&did, permission).await {
        return Some(QueryResponse::success(serde_json::json!({})));
    }
    None
}

/// Convert JSON variables from request format to parser format.
/// Variables in requests are `Option<JsonValue>` (a JSON object), but the
/// parser expects `Option<RapidHashMap<String, JsonValue>>`.
pub(super) fn convert_variables(
    variables: &Option<JsonValue>,
) -> Option<RapidHashMap<String, JsonValue>> {
    variables.as_ref().and_then(|v| {
        if let JsonValue::Object(map) = v {
            Some(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        } else {
            None // Non-object variables are ignored (invalid per GraphQL spec)
        }
    })
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryExecutor for QueryRunner<F, R> {
    async fn prepare_request(
        &self,
        request: &QueryRequest,
    ) -> Result<Option<std::sync::Arc<crate::prepared::PreparedMutations>>> {
        await_with_timeout(self.prepare_mutations(request), self.query_timeout).await
    }

    async fn execute_prepared(
        &self,
        request: QueryRequest,
        prepared: Option<std::sync::Arc<crate::prepared::PreparedMutations>>,
    ) -> QueryResponse {
        // Convert variables from JSON to RapidHashMap format for the parser
        let variables = convert_variables(&request.variables);

        // First, parse the request to determine if it's a query or mutation
        let parsed = match parse_request_with_limits(
            &request.query,
            variables.as_ref(),
            request.operation_name.as_deref(),
            self.query_limits,
        ) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                        extensions: None,
                        retry_input: None,
                    }],
                    extensions: None,
                };
            }
        };

        if matches!(&parsed, ParsedOperation::Query { .. }) {
            match self.registry.begin_implicit_read().await {
                Ok(handle) => {
                    let response = self.execute_in_txn(request.clone(), &handle).await;
                    let apply_read_effects = response.errors.is_empty();
                    if let Err(error) = self
                        .registry
                        .finish_implicit_read(&handle, apply_read_effects)
                        .await
                    {
                        if apply_read_effects {
                            return QueryResponse::error(format!(
                                "failed to finalize implicit read transaction: {}",
                                error
                            ));
                        }
                        warn!(
                            txn_id = %handle,
                            error = %error,
                            "Failed to close implicit read-only transaction after query execution"
                        );
                    }
                    return response;
                }
                Err(TransactionError::NotSupported(_)) => {}
                Err(error) => {
                    return QueryResponse::error(format!(
                        "failed to create implicit read-only transaction: {}",
                        error
                    ));
                }
            }
        }

        // Validate that all referenced collections exist before execution
        if let Err(e) = validate_parsed_operation(&parsed, self.effective_provider().as_ref()).await
        {
            return QueryResponse {
                data: None,
                errors: vec![QueryResponseError {
                    message: e.to_string(),
                    path: None,
                    locations: None,
                    extensions: None,
                    retry_input: None,
                }],
                extensions: None,
            };
        }

        // NAC check uses the raw request identity (not default fallback).
        // The default_identity is for document ACP, not node access control.
        if let Some(denial) = check_nac(self, &request.identity, &parsed).await {
            return denial;
        }

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);
        let creator_did = identity.as_ref().map(ToString::to_string);
        let acting_identity = creator_did
            .clone()
            .or_else(defra_core::current_identity::get_effective_identity);

        let mut warnings: Vec<GqlWarning> = Vec::new();
        let scoped_runner = self.scoped_runner(self.acp.clone());

        // Route to appropriate handler based on operation type
        // Pass identity and variables through for ACP permission checks and variable substitution
        let execution = Box::pin(async {
            match parsed {
                ParsedOperation::Query {
                    mut selects,
                    explain,
                    exhaustive,
                } => {
                    if exhaustive {
                        for s in &mut selects {
                            s.exhaustive = true;
                        }
                    }
                    if let Some(explain_type) = explain {
                        self.explain_query_with_identity_and_vars(
                            &request.query,
                            identity,
                            explain_type,
                            variables.as_ref(),
                        )
                        .await
                    } else {
                        self.execute_selects_internal(
                            selects,
                            self.fetcher.as_ref(),
                            identity,
                            &mut warnings,
                        )
                        .await
                    }
                }
                ParsedOperation::Mutation {
                    mutations, explain, ..
                } => {
                    if let Some(explain_type) = explain {
                        // Return mutation plan instead of executing
                        self.explain_mutation_with_identity(&request.query, identity, explain_type)
                            .await
                    } else {
                        // Use pre-parsed mutations to avoid redundant re-parsing
                        match self.mutator.as_ref() {
                            Some(mutator) => {
                                let mutator = prepared
                                    .as_ref()
                                    .and_then(|prepared| {
                                        mutator.with_prepared_inputs(prepared.inputs.clone())
                                    })
                                    .unwrap_or_else(|| mutator.clone());
                                scoped_runner
                                    .execute_parsed_mutations(
                                        mutations,
                                        mutator,
                                        identity,
                                        None,
                                        prepared.as_deref(),
                                    )
                                    .await
                            }
                            None => Err(crate::error::QueryError::execution(
                                "mutations require a mutator; call with_mutator() first",
                            )),
                        }
                    }
                }
                ParsedOperation::Subscription { .. } => {
                    // Subscriptions require SSE transport - they cannot be executed via regular request/response
                    Err(crate::error::QueryError::parse(
                        "Subscriptions must be executed via Server-Sent Events (SSE). \
                         Send the request with Accept: text/event-stream header.",
                    ))
                }
                ParsedOperation::Introspection { query } => {
                    // Introspection queries are executed against the GraphQL schema
                    self.execute_introspection(&query).await
                }
            }
        });

        // The primary QueryExecutor path parses mutations once and therefore
        // does not pass through execute_mutation_internal_with_vars. Scope the
        // resolved request identity here as well so HTTP/CLI auto-commit and
        // transactional broadcasts carry the same Creator DID as direct
        // mutation entry points.
        let result = defra_core::current_identity::with_scoped_identity(
            acting_identity,
            defra_core::signing::scope_broadcast_creator_did(
                creator_did,
                await_with_timeout(execution, self.query_timeout),
            ),
        )
        .await;

        match result {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
                extensions: None,
            }
            .with_warnings(warnings),
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    error = %e,
                    "Query execution failed"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError::from_query_error(e)],
                    extensions: None,
                }
                .with_warnings(warnings)
            }
        }
    }

    async fn resolve_retry_input(
        &self,
        _request: &QueryRequest,
        prepared: Option<std::sync::Arc<crate::prepared::PreparedMutations>>,
        input: &crate::prepared::PreparedInputRequest,
    ) -> Result<Option<std::sync::Arc<crate::prepared::PreparedMutations>>> {
        let Some(prepared) = prepared else {
            return Err(crate::error::QueryError::execution(
                "retry input has no request preparation capsule",
            ));
        };
        prepared
            .inputs
            .answer_wave(input, |wave| self.resolve_wave(wave))
            .await?;
        Ok(Some(prepared))
    }

    #[instrument(name = "query.execute_request", skip(self, request), fields(query_len = request.query.len() as i64))]
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        let mut prepared = match self.prepare_request(&request).await {
            Ok(prepared) => prepared,
            Err(error) => return QueryResponse::error(QueryResponseError::from_query_error(error)),
        };
        loop {
            let response = self
                .execute_prepared(request.clone(), prepared.clone())
                .await;
            let Some(input) = response.retry_input().cloned() else {
                return response;
            };
            match self
                .resolve_retry_input(&request, prepared.clone(), &input)
                .await
            {
                Ok(Some(updated)) => prepared = Some(updated),
                Ok(None) => {
                    return QueryResponse::error(
                        "retry input resolution returned no preparation capsule",
                    )
                }
                Err(error) => {
                    return QueryResponse::error(QueryResponseError::from_query_error(error))
                }
            }
        }
    }

    #[instrument(
        name = "query.execute_in_txn",
        skip(self, request),
        fields(query_len = request.query.len() as i64, txn_id = %handle)
    )]
    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        let txn_ctx = match self.registry.get(handle) {
            GetTransactionResult::Found(ctx) => ctx,
            GetTransactionResult::NotFound => {
                return QueryResponse::error(format!(
                    "transaction '{}' not found or has been committed/rolled back",
                    handle
                ));
            }
            GetTransactionResult::LockPoisoned => {
                return QueryResponse::error(format!(
                    "transaction registry lock poisoned - system may be in corrupted state (transaction '{}')",
                    handle
                ));
            }
        };
        let action_lock = txn_ctx.action_lock();
        let _action_guard = match action_lock.as_ref() {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };

        if let Err(error) = txn_ctx.recover_pending_statement() {
            return QueryResponse::error(format!(
                "failed to roll back cancelled transaction statement: {error}"
            ));
        }

        // Convert variables from JSON to RapidHashMap format for the parser
        let variables = convert_variables(&request.variables);

        // Parse the request to determine if it's a query or mutation
        let parsed = match parse_request_with_limits(
            &request.query,
            variables.as_ref(),
            request.operation_name.as_deref(),
            self.query_limits,
        ) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                        extensions: None,
                        retry_input: None,
                    }],
                    extensions: None,
                };
            }
        };

        let txn_provider = txn_ctx.collection_provider();

        // Validate that all referenced collections exist before execution.
        // Use the transaction-scoped provider if available so uncommitted schemas are visible.
        {
            let validation_provider: &dyn crate::fetcher::CollectionProvider =
                if let Some(ref p) = txn_provider {
                    p.as_ref()
                } else {
                    self.collection_provider.as_ref()
                };

            if let Err(e) = validate_parsed_operation(&parsed, validation_provider).await {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                        extensions: None,
                        retry_input: None,
                    }],
                    extensions: None,
                };
            }
        }

        let nac_identity = request.identity.clone().unwrap_or_else(Did::wildcard);
        match txn_ctx
            .doc_fetcher()
            .check_node_permission(&nac_identity, permission_for_operation(&parsed))
            .await
        {
            Ok(Some(false)) => return QueryResponse::success(serde_json::json!({})),
            Ok(Some(true)) => {}
            Ok(None) => {
                if let Some(denial) = check_nac(self, &request.identity, &parsed).await {
                    return denial;
                }
            }
            Err(error) => return QueryResponse::error(error.to_string()),
        }

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        let mut warnings: Vec<GqlWarning> = Vec::new();

        let scoped_acp = match txn_ctx.doc_fetcher().scoped_acp(self.acp.clone()).await {
            Ok(provider) => provider,
            Err(error) => return QueryResponse::error(error.to_string()),
        };
        let mut scoped_runner = self.scoped_runner(scoped_acp);
        if !scoped_runner.acp.supports_transactions() {
            // Even transaction-context implementations without an ACP
            // projection must never fall back to mutating an external provider
            // before the storage transaction commits. The mutator's ordered
            // hook registration will fail closed if it cannot retain hooks.
            scoped_runner.acp_registration_projection = Some(
                txn_ctx
                    .acp_registration_projection()
                    .unwrap_or_else(crate::txn::AcpRegistrationProjection::detached_shared),
            );
        }

        let statement_guard = if !txn_ctx.is_readonly()
            && matches!(&parsed, ParsedOperation::Mutation { explain: None, .. })
        {
            if let Err(error) = txn_ctx.set_statement_savepoint() {
                return QueryResponse::error(format!(
                    "failed to establish transaction statement savepoint: {error}"
                ));
            }
            Some(StatementRollbackGuard::new(Arc::clone(&txn_ctx)))
        } else {
            None
        };

        // Route to appropriate handler based on operation type
        let execution = async {
            match parsed {
                ParsedOperation::Query {
                    mut selects,
                    explain,
                    exhaustive,
                } => {
                    if exhaustive {
                        for s in &mut selects {
                            s.exhaustive = true;
                        }
                    }
                    let fetcher = txn_ctx.doc_fetcher();
                    if let Some(explain_type) = explain {
                        self.explain_query_with_identity_and_vars(
                            &request.query,
                            identity,
                            explain_type,
                            variables.as_ref(),
                        )
                        .await
                    } else {
                        scoped_runner
                            .execute_selects_internal(
                                selects,
                                fetcher.as_ref(),
                                identity,
                                &mut warnings,
                            )
                            .await
                    }
                }
                ParsedOperation::Mutation { explain, .. } => {
                    if let Some(explain_type) = explain {
                        // Return mutation plan instead of executing
                        self.explain_mutation_with_identity(&request.query, identity, explain_type)
                            .await
                    } else {
                        // Check if this is a read-only transaction
                        if txn_ctx.is_readonly() {
                            return Err(crate::error::QueryError::execution(
                                "cannot execute mutation in read-only transaction",
                            ));
                        }

                        // Get the transaction-scoped mutator
                        let mutator = match txn_ctx.doc_mutator() {
                            Some(m) => m,
                            None => {
                                return Err(crate::error::QueryError::execution(
                                    "mutations not supported in this transaction context",
                                ));
                            }
                        };

                        mutator.set_document_acp(self.acp.clone());
                        let txn_fetcher = txn_ctx.doc_fetcher();
                        // A remote input the statement needs (an embedding)
                        // is never fetched inside it: the statement ends, is
                        // rolled back to its savepoint, the input is fetched
                        // with nothing of the transaction in flight, and the
                        // statement runs again reading it from the memo.
                        let inputs = crate::prepared::PreparedInputMemo::default();
                        loop {
                            let attempt = mutator
                                .with_prepared_inputs(inputs.clone())
                                .unwrap_or_else(|| mutator.clone());
                            match scoped_runner
                                .execute_mutation_internal_with_vars(
                                    &request.query,
                                    attempt,
                                    identity.clone(),
                                    variables.as_ref(),
                                    Some(Arc::clone(&txn_fetcher)),
                                )
                                .await
                            {
                                Err(crate::error::QueryError::NeedsInput(input)) => {
                                    txn_ctx
                                        .finish_statement_savepoint(false)
                                        .map_err(crate::error::QueryError::execution)?;
                                    inputs
                                        .answer_wave(&input, |wave| {
                                            mutator.resolve_prepared_inputs(wave)
                                        })
                                        .await?;
                                    txn_ctx
                                        .set_statement_savepoint()
                                        .map_err(crate::error::QueryError::execution)?;
                                }
                                result => break result,
                            }
                        }
                    }
                }
                ParsedOperation::Subscription { .. } => {
                    // Subscriptions require SSE transport
                    Err(crate::error::QueryError::parse(
                        "Subscriptions must be executed via Server-Sent Events (SSE). \
                         Send the request with Accept: text/event-stream header.",
                    ))
                }
                ParsedOperation::Introspection { query } => {
                    // Introspection queries are executed against the GraphQL schema
                    self.execute_introspection(&query).await
                }
            }
        };

        // Collection schema reads retain their transaction-scoped provider;
        // ACP projection is passed explicitly through the scoped runner.
        let result = if let Some(provider) = txn_provider {
            super::scope_txn_collection_provider(
                provider,
                await_with_timeout(execution, self.query_timeout),
            )
            .await
        } else {
            await_with_timeout(execution, self.query_timeout).await
        };

        let response = match result {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
                extensions: None,
            }
            .with_warnings(warnings),
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    txn_id = %handle,
                    error = %e,
                    "Query execution failed in transaction"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError::from_query_error(e)],
                    extensions: None,
                }
                .with_warnings(warnings)
            }
        };

        if let Some(mut guard) = statement_guard {
            if let Err(error) = txn_ctx.finish_statement_savepoint(response.errors.is_empty()) {
                return QueryResponse::error(format!(
                    "failed to finish transaction statement savepoint: {error}"
                ));
            }
            guard.disarm();
        }

        response
    }

    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.registry.begin(readonly).await
    }

    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.commit(handle).await
    }

    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.rollback(handle).await
    }

    fn abandon_txn(&self, handle: &TransactionHandle) {
        self.registry.abandon(handle);
    }

    async fn schema(&self) -> Result<String> {
        let collections = self.collections_map().await?;
        let mut schema_str = String::new();
        for collection in collections.values() {
            schema_str.push_str(&format!("type {} {{\n", collection.name));
            for field in &collection.fields {
                let gql_type = field.kind.graphql_type_name();
                schema_str.push_str(&format!("  {}: {}\n", field.name, gql_type));
            }
            schema_str.push_str("}\n\n");
        }
        Ok(schema_str)
    }
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// One wave of a request's external inputs, each by the resolver of its
    /// kind: the runner's own concurrently, the mutator's as one batch.
    async fn resolve_wave(
        &self,
        wave: Vec<crate::prepared::PreparedInputRequest>,
    ) -> Result<Vec<std::sync::Arc<[u8]>>> {
        let (own, delegated): (Vec<_>, Vec<_>) =
            wave.iter().enumerate().partition(|(_, request)| {
                matches!(
                    request.kind(),
                    crate::prepared::PreparedInputKind::AcpDecision
                        | crate::prepared::PreparedInputKind::WriteValidation
                        | crate::prepared::PreparedInputKind::ReadValidation
                        | crate::prepared::PreparedInputKind::EncryptedSearch
                )
            });
        let own_answers =
            futures::future::try_join_all(own.iter().map(|(_, request)| async move {
                match request.kind() {
                    crate::prepared::PreparedInputKind::AcpDecision => {
                        self.resolve_acp_decision_input(request).await
                    }
                    crate::prepared::PreparedInputKind::WriteValidation => {
                        self.resolve_write_validation_input(request).await
                    }
                    crate::prepared::PreparedInputKind::ReadValidation => {
                        self.resolve_read_validation_input(request).await
                    }
                    _ => self.resolve_encrypted_search_input(request).await,
                }
            }));
        let delegated_answers = async {
            if delegated.is_empty() {
                return Ok(Vec::new());
            }
            let mutator = self.mutator.as_ref().ok_or_else(|| {
                crate::error::QueryError::execution("retry input has no document mutator")
            })?;
            mutator
                .resolve_prepared_inputs(
                    delegated
                        .iter()
                        .map(|(_, request)| (*request).clone())
                        .collect(),
                )
                .await
        };
        let (own_answers, delegated_answers) =
            futures::future::try_join(own_answers, delegated_answers).await?;
        let mut answers: Vec<Option<std::sync::Arc<[u8]>>> = vec![None; wave.len()];
        for ((index, _), answer) in own
            .iter()
            .chain(delegated.iter())
            .zip(own_answers.into_iter().chain(delegated_answers))
        {
            answers[*index] = Some(answer);
        }
        answers
            .into_iter()
            .map(|answer| {
                answer.ok_or_else(|| {
                    crate::error::QueryError::execution("an external input went unanswered")
                })
            })
            .collect()
    }

    async fn resolve_write_validation_input(
        &self,
        input: &crate::prepared::PreparedInputRequest,
    ) -> Result<std::sync::Arc<[u8]>> {
        let request: crate::access_hooks::write::WriteValidationInput =
            serde_json::from_slice(input.exact_request()).map_err(|_| {
                crate::error::QueryError::execution("invalid prepared write validation request")
            })?;
        let validator = self.write_validator.as_ref().ok_or_else(|| {
            crate::error::QueryError::execution("write validator changed during retry preparation")
        })?;
        if std::sync::Arc::as_ptr(validator) as *const () as usize != request.validator {
            return Err(crate::error::QueryError::execution(
                "write validator changed during retry preparation",
            ));
        }
        let kind = crate::mapper::MutationType::from_prefix(&request.kind)
            .ok_or_else(|| crate::error::QueryError::execution("invalid prepared mutation kind"))?;
        let identity = request
            .identity
            .as_deref()
            .map(Did::new)
            .transpose()
            .map_err(|_| {
                crate::error::QueryError::execution("invalid identity in write validation request")
            })?;
        validator
            .validate_write(&crate::access_hooks::WriteRequest {
                identity: identity.as_ref(),
                collection: &request.collection,
                kind,
                doc_ids: &request.doc_ids,
                create_input: &request.create_input,
                update_input: &request.update_input,
            })
            .await
            .map_err(crate::error::QueryError::execution)?;
        Ok(std::sync::Arc::from(b"ok".as_slice()))
    }

    async fn resolve_acp_decision_input(
        &self,
        input: &crate::prepared::PreparedInputRequest,
    ) -> Result<std::sync::Arc<[u8]>> {
        use acp::{DocumentPermission, Identity};
        use serde_json::Value;

        let fields: Vec<Value> = serde_json::from_slice(input.exact_request())
            .map_err(|_| crate::error::QueryError::execution("invalid prepared ACP request"))?;
        let field = |index: usize| -> Result<&str> {
            fields
                .get(index)
                .and_then(Value::as_str)
                .ok_or_else(|| crate::error::QueryError::execution("invalid prepared ACP request"))
        };
        let did = if field(0)? == "anonymous" {
            None
        } else {
            Some(Did::new(field(0)?).map_err(|_| {
                crate::error::QueryError::execution("invalid identity in prepared ACP request")
            })?)
        };
        let bearer = fields.get(5).and_then(Value::as_str).map(str::to_owned);
        let bearer_did = did.as_ref().map(ToString::to_string);
        let acp_identity = did.map(Identity::from).unwrap_or(Identity::Anonymous);
        let response = defra_core::signing::scope_request_bearer_token(bearer_did, bearer, async {
            let response = if field(1)? == "registered" {
                let registered = self
                    .acp
                    .is_doc_registered(field(2)?, field(3)?, field(4)?)
                    .await
                    .map_err(|error| crate::error::QueryError::execution(error.to_string()))?;
                serde_json::to_vec(&registered)
            } else if field(4)? == "writer" {
                let decision = self
                    .acp
                    .create_access_decision(
                        &acp_identity,
                        field(1)?,
                        field(2)?,
                        field(3)?,
                        "writer",
                    )
                    .await
                    .map_err(|error| {
                        crate::error::QueryError::permission_denied(format!(
                            "create authorization failed: {error}"
                        ))
                    })?;
                serde_json::to_vec(&decision)
            } else {
                let permission = match field(1)? {
                    "read" => DocumentPermission::Read,
                    "update" => DocumentPermission::Update,
                    "delete" => DocumentPermission::Delete,
                    _ => {
                        return Err(crate::error::QueryError::execution(
                            "invalid prepared ACP permission",
                        ))
                    }
                };
                let allowed = self
                    .acp
                    .check_doc_access(&acp_identity, permission, field(2)?, field(3)?, field(4)?)
                    .await
                    .map_err(|error| crate::error::QueryError::execution(error.to_string()))?;
                serde_json::to_vec(&allowed)
            }
            .map_err(|error| crate::error::QueryError::execution(error.to_string()))?;
            Ok::<_, crate::error::QueryError>(response)
        })
        .await?;
        Ok(std::sync::Arc::from(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use async_trait::async_trait;
    use document::Document;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::fetcher::{DocFetcher, FetchByIdsResult};
    use crate::test_utils::{MockFetcher, MockTxnRegistry};
    use crate::txn::{GetTransactionResult, TransactionContext, TransactionRegistry};

    #[test]
    fn truncate_uses_collection_truncate_permission() {
        let parsed = ParsedOperation::Mutation {
            mutations: vec![crate::mapper::Mutation::truncate("Users")],
            explain: None,
        };

        assert_eq!(
            permission_for_operation(&parsed),
            NodePermission::CollectionTruncate
        );
    }

    struct ChangingFetcher {
        call_count: std::sync::atomic::AtomicUsize,
    }

    impl ChangingFetcher {
        fn new() -> Self {
            Self {
                call_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn make_doc(&self, name: String) -> Document {
            let mut doc = Document::new();
            doc.set_id(document::DocID::new_v0_from_seed(&name));
            doc.set("name", serde_json::Value::String(name));
            doc
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for ChangingFetcher {
        /// A mock has no storage, so a document's short id is its 1-based position
        /// in the collection, mirroring how the real allocator hands them out.
        async fn stream_by_doc_short_ids(
            &self,
            collection_name: &str,
            doc_short_ids: &[u64],
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            let all = self
                .get_all_with_deleted(collection_name, show_deleted)
                .await?;
            let picked = doc_short_ids
                .iter()
                .filter_map(|id| all.get(id.checked_sub(1)? as usize).cloned())
                .collect();
            Ok(Box::new(crate::doc_stream::VecStream::new(picked)))
        }
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            let call = self
                .call_count
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            Ok(vec![self.make_doc(format!("auto-{call}"))])
        }

        /// In-memory mock: there is no storage to stream from.
        async fn stream_all_with_deleted(
            &self,
            collection_name: &str,
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            Ok(Box::new(crate::doc_stream::VecStream::new(
                self.get_all_with_deleted(collection_name, show_deleted)
                    .await?,
            )))
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.get_all(collection_name).await?;
            Ok(FetchByIdsResult::all_found(docs))
        }

        async fn get_by_field_value(
            &self,
            collection_name: &str,
            _field_name: &str,
            _value: &str,
        ) -> Result<Vec<Document>> {
            self.get_all(collection_name).await
        }
    }

    fn make_users_doc(name: &str) -> Document {
        let mut doc = Document::new();
        doc.set_id(document::DocID::new_v0_from_seed(name));
        doc.set("name", serde_json::Value::String(name.to_string()));
        doc
    }

    struct SerialOnlyFetcher {
        in_flight: AtomicUsize,
    }

    impl SerialOnlyFetcher {
        fn new() -> Self {
            Self {
                in_flight: AtomicUsize::new(0),
            }
        }
    }

    struct InFlightGuard<'a>(&'a AtomicUsize);

    impl Drop for InFlightGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for SerialOnlyFetcher {
        /// A mock has no storage, so a document's short id is its 1-based position
        /// in the collection, mirroring how the real allocator hands them out.
        async fn stream_by_doc_short_ids(
            &self,
            collection_name: &str,
            doc_short_ids: &[u64],
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            let all = self
                .get_all_with_deleted(collection_name, show_deleted)
                .await?;
            let picked = doc_short_ids
                .iter()
                .filter_map(|id| all.get(id.checked_sub(1)? as usize).cloned())
                .collect();
            Ok(Box::new(crate::doc_stream::VecStream::new(picked)))
        }
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            let active = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            let _guard = InFlightGuard(&self.in_flight);
            if active > 1 {
                return Err(crate::error::QueryError::execution(
                    "transaction action overlapped",
                ));
            }

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(vec![make_users_doc("serial")])
        }

        /// In-memory mock: there is no storage to stream from.
        async fn stream_all_with_deleted(
            &self,
            collection_name: &str,
            show_deleted: bool,
        ) -> Result<Box<dyn crate::doc_stream::DocStream>> {
            Ok(Box::new(crate::doc_stream::VecStream::new(
                self.get_all_with_deleted(collection_name, show_deleted)
                    .await?,
            )))
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.get_all(collection_name).await?;
            Ok(FetchByIdsResult::all_found(docs))
        }

        async fn get_by_field_value(
            &self,
            collection_name: &str,
            _field_name: &str,
            _value: &str,
        ) -> Result<Vec<Document>> {
            self.get_all(collection_name).await
        }
    }

    struct SerialTxnContext {
        id: String,
        fetcher: Arc<dyn DocFetcher>,
        action_lock: Arc<async_lock::Mutex<()>>,
    }

    impl TransactionContext for SerialTxnContext {
        fn id(&self) -> &str {
            &self.id
        }

        fn is_readonly(&self) -> bool {
            true
        }

        fn doc_fetcher(&self) -> Arc<dyn DocFetcher> {
            self.fetcher.clone()
        }

        fn action_lock(&self) -> Option<Arc<async_lock::Mutex<()>>> {
            Some(self.action_lock.clone())
        }
    }

    struct SerialTxnRegistry {
        ctx: Arc<SerialTxnContext>,
    }

    impl SerialTxnRegistry {
        fn new(fetcher: SerialOnlyFetcher) -> Self {
            Self {
                ctx: Arc::new(SerialTxnContext {
                    id: "serial-txn".to_string(),
                    fetcher: Arc::new(fetcher),
                    action_lock: Arc::new(async_lock::Mutex::new(())),
                }),
            }
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl TransactionRegistry for SerialTxnRegistry {
        fn abandon(&self, _handle: &TransactionHandle) {}

        async fn begin(
            &self,
            _readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            Ok(TransactionHandle::new(self.ctx.id.clone()))
        }

        fn get(&self, handle: &TransactionHandle) -> GetTransactionResult {
            if handle.as_str() == self.ctx.id {
                GetTransactionResult::Found(self.ctx.clone())
            } else {
                GetTransactionResult::NotFound
            }
        }

        async fn commit(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Ok(())
        }

        async fn rollback(
            &self,
            _handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn execute_in_txn_serializes_concurrent_actions_on_same_handle() {
        let collections = crate::parse_sdl("type Users { name: String }").expect("schema");
        let txn_fetcher = SerialOnlyFetcher::new();

        let runner = QueryRunner::with_registry(
            MockFetcher::new(),
            collections,
            SerialTxnRegistry::new(txn_fetcher),
        );
        let handle = runner.begin_txn(true).await.expect("begin txn");

        let (first, second) = tokio::join!(
            runner.execute_in_txn(QueryRequest::new("query { Users { name } }"), &handle),
            runner.execute_in_txn(QueryRequest::new("query { Users { name } }"), &handle)
        );

        assert!(
            !first.has_errors(),
            "first transaction action failed: {:?}",
            first.errors
        );
        assert!(
            !second.has_errors(),
            "second transaction action failed: {:?}",
            second.errors
        );
    }

    #[tokio::test]
    async fn execute_uses_implicit_read_txn_when_registry_is_available() {
        let collections = crate::parse_sdl("type Users { name: String }").expect("schema");

        let txn_fetcher = MockFetcher::new();
        txn_fetcher.add_doc("Users", make_users_doc("txn-snapshot"));

        let runner = QueryRunner::with_registry(
            ChangingFetcher::new(),
            collections,
            MockTxnRegistry::new(txn_fetcher),
        );

        let response = runner
            .execute(QueryRequest::new(
                "{ first: Users { name } second: Users { name } }",
            ))
            .await;

        assert!(
            !response.has_errors(),
            "unexpected errors: {:?}",
            response.errors
        );
        assert_eq!(
            response.data,
            Some(json!({
                "first": [{"name": "txn-snapshot"}],
                "second": [{"name": "txn-snapshot"}],
            }))
        );
    }
}
