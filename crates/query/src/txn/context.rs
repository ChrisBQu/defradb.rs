//! Transaction context trait.

use acp::error::{Error as AcpError, Result as AcpResult};
use acp::DocumentACP;
use acp::DocumentPermission;
use acp::Identity;
use defra_core::thread_bounds::MaybeBoxFuture;
use futures::FutureExt;
use identity::Did;
use rapidhash::RapidHashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use crate::fetcher::CollectionProvider;
use crate::fetcher::DocFetcher;
use crate::mutator::DocMutator;

#[cfg(not(target_arch = "wasm32"))]
tokio::task_local! {
    static TXN_DEFERRED_ACP_MUTATIONS: Arc<DeferredAcpMutations>;
}

#[cfg(target_arch = "wasm32")]
thread_local! {
    static TXN_DEFERRED_ACP_MUTATIONS: std::cell::RefCell<Option<Arc<DeferredAcpMutations>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(not(target_arch = "wasm32"))]
type DeferredAcpHook = Box<dyn FnOnce() -> MaybeBoxFuture<'static, Result<(), String>> + Send>;
#[cfg(target_arch = "wasm32")]
type DeferredAcpHook = Box<dyn FnOnce() -> MaybeBoxFuture<'static, Result<(), String>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProjectedDocRegistration {
    Registered { owner: Did },
    Unregistered,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct DocRegistrationKey {
    policy_id: String,
    resource_name: String,
    doc_id: String,
}

struct DeferredAcpHookEntry {
    description: &'static str,
    callback: DeferredAcpHook,
}

#[derive(Default)]
struct DeferredAcpState {
    projected_registrations: RapidHashMap<DocRegistrationKey, ProjectedDocRegistration>,
    hooks: Vec<DeferredAcpHookEntry>,
    savepoints: Vec<DeferredAcpSavepoint>,
}

struct DeferredAcpSavepoint {
    projected_registrations: RapidHashMap<DocRegistrationKey, ProjectedDocRegistration>,
    hook_count: usize,
}

/// Deferred ACP mutations and their transaction-local registration projection.
///
/// Explicit database transactions use this to:
/// - buffer ACP writes until commit succeeds
/// - expose projected registration state to permission checks within the txn
#[derive(Default)]
pub struct DeferredAcpMutations {
    state: std::sync::Mutex<DeferredAcpState>,
}

impl DeferredAcpMutations {
    /// Create an empty deferred ACP state container.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_statement_savepoint(&self) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let savepoint = DeferredAcpSavepoint {
            projected_registrations: state.projected_registrations.clone(),
            hook_count: state.hooks.len(),
        };
        state.savepoints.push(savepoint);
        Ok(())
    }

    pub fn rollback_statement_savepoint(&self) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let savepoint = state
            .savepoints
            .pop()
            .ok_or_else(|| "deferred ACP state has no active statement savepoint".to_string())?;
        state.projected_registrations = savepoint.projected_registrations;
        state.hooks.truncate(savepoint.hook_count);
        Ok(())
    }

    pub fn release_statement_savepoint(&self) -> Result<(), String> {
        let mut state = self.lock_state()?;
        state
            .savepoints
            .pop()
            .map(|_| ())
            .ok_or_else(|| "deferred ACP state has no active statement savepoint".to_string())
    }

    fn doc_key(policy_id: &str, resource_name: &str, doc_id: &str) -> DocRegistrationKey {
        DocRegistrationKey {
            policy_id: policy_id.to_string(),
            resource_name: resource_name.to_string(),
            doc_id: doc_id.to_string(),
        }
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, DeferredAcpState>, String> {
        self.state
            .lock()
            .map_err(|_| "deferred ACP mutation state lock poisoned".to_string())
    }

    fn push_hook(
        &self,
        description: &'static str,
        callback: DeferredAcpHook,
    ) -> Result<(), String> {
        let mut state = self.lock_state()?;
        state.hooks.push(DeferredAcpHookEntry {
            description,
            callback,
        });
        Ok(())
    }

    /// Return the projected registration state for a document within the current transaction.
    fn projected_registration(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> AcpResult<Option<ProjectedDocRegistration>> {
        let state = self.lock_state().map_err(AcpError::Storage)?;
        Ok(state
            .projected_registrations
            .get(&Self::doc_key(policy_id, resource_name, doc_id))
            .cloned())
    }

    /// Schedule a document registration to run after the storage transaction commits.
    pub fn schedule_register_doc_object(
        &self,
        acp: Arc<dyn DocumentACP>,
        identity: Did,
        policy_id: String,
        resource_name: String,
        doc_id: String,
        request_bearer_token: Option<String>,
    ) -> Result<(), String> {
        {
            let mut state = self.lock_state()?;
            state.projected_registrations.insert(
                Self::doc_key(&policy_id, &resource_name, &doc_id),
                ProjectedDocRegistration::Registered {
                    owner: identity.clone(),
                },
            );
        }

        let doc_id_for_log = doc_id.clone();
        let policy_id_for_log = policy_id.clone();
        self.push_hook(
            "register_doc_object",
            Box::new(move || {
                Box::pin(async move {
                    defra_core::signing::scope_request_bearer_token(
                        Some(identity.to_string()),
                        request_bearer_token,
                        async {
                            acp.register_doc_object(&identity, &policy_id, &resource_name, &doc_id)
                                .await
                                .map_err(|e| {
                                    format!(
                                        "failed to register ACP document '{}' for policy '{}': {}",
                                        doc_id_for_log, policy_id_for_log, e
                                    )
                                })
                        },
                    )
                    .await
                })
            }),
        )
    }

    /// Schedule a document unregistration to run after the storage transaction commits.
    pub fn schedule_unregister_doc_object(
        &self,
        acp: Arc<dyn DocumentACP>,
        policy_id: String,
        resource_name: String,
        doc_id: String,
        caller_identity: Option<Did>,
        request_bearer_token: Option<String>,
    ) -> Result<(), String> {
        {
            let mut state = self.lock_state()?;
            state.projected_registrations.insert(
                Self::doc_key(&policy_id, &resource_name, &doc_id),
                ProjectedDocRegistration::Unregistered,
            );
        }

        let doc_id_for_log = doc_id.clone();
        let policy_id_for_log = policy_id.clone();
        self.push_hook(
            "unregister_doc_object",
            Box::new(move || {
                Box::pin(async move {
                    defra_core::signing::scope_request_bearer_token(
                        caller_identity.map(|did| did.to_string()),
                        request_bearer_token,
                        async {
                            if let Err(err) = acp
                                .unregister_doc_object(&policy_id, &resource_name, &doc_id)
                                .await
                            {
                                tracing::warn!(
                                    doc_id = %doc_id_for_log,
                                    policy_id = %policy_id_for_log,
                                    error = %err,
                                    "Deferred ACP unregister failed after transaction commit"
                                );
                            }

                            Ok(())
                        },
                    )
                    .await
                })
            }),
        )
    }

    /// Run all deferred ACP hooks in registration order.
    pub async fn run_all_logged(&self) {
        let hooks = match self.lock_state() {
            Ok(mut state) => std::mem::take(&mut state.hooks),
            Err(err) => {
                tracing::error!(error = %err, "Failed to drain deferred ACP hooks");
                return;
            }
        };

        for hook in hooks {
            let description = hook.description;
            let result = AssertUnwindSafe((hook.callback)()).catch_unwind().await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(
                        hook = description,
                        error = %err,
                        "Deferred ACP hook failed after transaction commit"
                    );
                }
                Err(panic) => {
                    tracing::error!(
                        hook = description,
                        panic = ?panic,
                        "Deferred ACP hook panicked after transaction commit"
                    );
                }
            }
        }
    }
}

/// Execute a future with the given deferred ACP mutation projection in scope.
pub async fn scope_deferred_acp_mutations<Fut, T>(
    mutations: Arc<DeferredAcpMutations>,
    future: Fut,
) -> T
where
    Fut: Future<Output = T>,
{
    #[cfg(not(target_arch = "wasm32"))]
    return TXN_DEFERRED_ACP_MUTATIONS.scope(mutations, future).await;

    #[cfg(target_arch = "wasm32")]
    defra_core::wasm_task_local::scope(&TXN_DEFERRED_ACP_MUTATIONS, mutations, future).await
}

/// Return the current transaction's deferred ACP projection, if any.
pub fn current_deferred_acp_mutations() -> Option<Arc<DeferredAcpMutations>> {
    #[cfg(not(target_arch = "wasm32"))]
    return TXN_DEFERRED_ACP_MUTATIONS.try_with(Clone::clone).ok();

    #[cfg(target_arch = "wasm32")]
    defra_core::wasm_task_local::try_with(&TXN_DEFERRED_ACP_MUTATIONS, Clone::clone)
}

/// Check document registration while honoring any ACP mutations buffered in the current txn.
pub async fn is_doc_registered_with_overlay(
    acp: &dyn DocumentACP,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
) -> AcpResult<bool> {
    if let Some(mutations) = current_deferred_acp_mutations() {
        if let Some(projected) =
            mutations.projected_registration(policy_id, resource_name, doc_id)?
        {
            return Ok(matches!(
                projected,
                ProjectedDocRegistration::Registered { .. }
            ));
        }
    }

    acp.is_doc_registered(policy_id, resource_name, doc_id)
        .await
}

/// Check document access while honoring any ACP mutations buffered in the current txn.
pub async fn check_doc_access_with_overlay(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
) -> AcpResult<bool> {
    if let Some(mutations) = current_deferred_acp_mutations() {
        if let Some(projected) =
            mutations.projected_registration(policy_id, resource_name, doc_id)?
        {
            return Ok(match projected {
                ProjectedDocRegistration::Unregistered => acp.unregistered_documents_are_public(),
                ProjectedDocRegistration::Registered { owner } => {
                    matches!(identity, Identity::Authenticated(did) if did == &owner)
                }
            });
        }
    }

    acp.check_doc_access(identity, permission, policy_id, resource_name, doc_id)
        .await
}

/// Resolve a document permission through the request memo when running inside
/// an engine retry attempt. A cache miss exits the attempt so the caller can
/// ask the external ACP provider and re-enter with the exact answer.
pub async fn check_doc_access_with_prepared_inputs(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
    inputs: Option<&crate::prepared::PreparedInputMemo>,
) -> crate::error::Result<bool> {
    // Transaction-bound providers read the attempt's ACP namespace directly so
    // those reads participate in OCC and see earlier writes in the same batch.
    // The retry memo is only for external providers without transaction scope.
    let Some(inputs) = inputs.filter(|_| !acp.supports_transactions()) else {
        return check_doc_access_with_overlay(
            acp,
            identity,
            permission,
            policy_id,
            resource_name,
            doc_id,
        )
        .await
        .map_err(|error| crate::error::QueryError::execution(error.to_string()));
    };
    let bearer = identity
        .did()
        .and_then(|did| defra_core::signing::get_request_bearer_token(did.as_str()));
    let exact_request = serde_json::to_vec(&(
        identity.did().map(Did::as_str).unwrap_or("anonymous"),
        permission.as_str(),
        policy_id,
        resource_name,
        doc_id,
        bearer,
    ))
    .map_err(|error| crate::error::QueryError::execution(error.to_string()))?;
    let request = crate::prepared::PreparedInputRequest::new(
        crate::prepared::PreparedInputKind::AcpDecision,
        exact_request,
    );
    let Some(response) = inputs.get(&request) else {
        return Err(crate::error::QueryError::NeedsInput(request));
    };
    serde_json::from_slice(&response).map_err(|error| {
        crate::error::QueryError::execution(format!("cached ACP response is invalid: {error}"))
    })
}

/// Resolve the shared document/branchable read rule through the request memo.
/// Registration is a separate ACP input because unregistered documents may be
/// public, and branchable collections fall back to collection-level access.
#[allow(clippy::too_many_arguments)]
pub async fn check_doc_read_access_with_prepared_inputs(
    acp: &dyn DocumentACP,
    identity: &Identity,
    policy_id: &str,
    resource_name: &str,
    collection_id: &str,
    is_branchable: bool,
    doc_id: &str,
    inputs: Option<&crate::prepared::PreparedInputMemo>,
) -> crate::error::Result<bool> {
    use acp::read_access::DocAccess;

    let Some(inputs) = inputs.filter(|_| !acp.supports_transactions()) else {
        let checker = crate::txn::OverlayChecker { acp, identity };
        return acp::read_access::check_doc_read_access(
            &checker,
            policy_id,
            resource_name,
            collection_id,
            is_branchable,
            doc_id,
        )
        .await
        .map_err(|error| crate::error::QueryError::execution(error.to_string()));
    };

    async fn access_for_object(
        acp: &dyn DocumentACP,
        identity: &Identity,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
        inputs: &crate::prepared::PreparedInputMemo,
    ) -> crate::error::Result<DocAccess> {
        let bearer = identity
            .did()
            .and_then(|did| defra_core::signing::get_request_bearer_token(did.as_str()));
        let request_bytes = serde_json::to_vec(&(
            identity.did().map(Did::as_str).unwrap_or("anonymous"),
            "registered",
            policy_id,
            resource_name,
            object_id,
            bearer,
        ))
        .map_err(|error| crate::error::QueryError::execution(error.to_string()))?;
        let request = crate::prepared::PreparedInputRequest::new(
            crate::prepared::PreparedInputKind::AcpDecision,
            request_bytes,
        );
        let Some(response) = inputs.get(&request) else {
            return Err(crate::error::QueryError::NeedsInput(request));
        };
        let registered = serde_json::from_slice::<bool>(&response)
            .map_err(|error| crate::error::QueryError::execution(error.to_string()))?;
        if !registered {
            return Ok(DocAccess {
                has_access: acp.unregistered_documents_are_public(),
                explicit: false,
            });
        }
        let allowed = check_doc_access_with_prepared_inputs(
            acp,
            identity,
            DocumentPermission::Read,
            policy_id,
            resource_name,
            object_id,
            Some(inputs),
        )
        .await?;
        Ok(DocAccess {
            has_access: allowed,
            explicit: true,
        })
    }

    if identity.is_authenticated() && defra_core::dac_bypass::get_dac_bypass() {
        return Ok(true);
    }
    if !doc_id.is_empty() {
        let doc_access =
            access_for_object(acp, identity, policy_id, resource_name, doc_id, inputs).await?;
        if doc_access.explicit || !is_branchable {
            return Ok(doc_access.has_access);
        }
    }
    if is_branchable {
        let collection_access = access_for_object(
            acp,
            identity,
            policy_id,
            resource_name,
            collection_id,
            inputs,
        )
        .await?;
        if !collection_access.has_access {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Transaction context that provides storage access within a transaction.
///
/// This is implemented by the database layer to provide transaction-scoped
/// document fetching and mutation.
pub trait TransactionContext: MaybeSendSync {
    /// Get the transaction ID.
    fn id(&self) -> &str;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Get a document fetcher scoped to this transaction.
    fn doc_fetcher(&self) -> Arc<dyn DocFetcher>;

    /// Get a document mutator scoped to this transaction.
    ///
    /// Returns `None` if this is a read-only transaction or if mutators
    /// are not supported by this context implementation.
    ///
    /// The mutator shares the same underlying transaction as the fetcher,
    /// so all read and write operations are within the same transaction context.
    fn doc_mutator(&self) -> Option<Arc<dyn DocMutator>> {
        None
    }

    /// Get a collection provider scoped to this transaction.
    ///
    /// Returns `None` by default. Implementations that support transaction-scoped
    /// schema resolution should override this to return a provider that reads
    /// from the transaction's uncommitted state (falling back to the process-wide cache).
    fn collection_provider(&self) -> Option<Arc<dyn CollectionProvider>> {
        None
    }

    /// Get deferred ACP mutations for this transaction.
    ///
    /// Implementations with transactional ACP buffering should override this to
    /// return a shared state object that can be used both for commit-time hooks
    /// and for transaction-local ACP projections.
    fn deferred_acp_mutations(&self) -> Option<Arc<DeferredAcpMutations>> {
        None
    }

    /// Get the mutex that serializes top-level actions on this transaction.
    ///
    /// Implementations that share one underlying storage transaction across
    /// multiple public calls should override this so callers can prevent
    /// concurrent use of that handle.
    fn action_lock(&self) -> Option<Arc<async_lock::Mutex<()>>> {
        None
    }

    /// Start an atomicity boundary around one explicit mutation statement.
    /// Writable implementations must override this; the default fails closed.
    fn set_statement_savepoint(&self) -> Result<(), String> {
        Err("transaction context does not support statement savepoints".into())
    }

    fn finish_statement_savepoint(&self, _success: bool) -> Result<(), String> {
        Err("transaction context does not support statement savepoints".into())
    }

    fn recover_pending_statement(&self) -> Result<(), String> {
        Ok(())
    }

    fn mark_statement_rollback_pending(&self) {}

    /// Check if the transaction is still active (not yet committed or rolled back).
    ///
    /// Returns `true` if the transaction can still be used for queries.
    /// Returns `false` if the transaction has been consumed via commit/rollback.
    ///
    /// # Implementation Note
    ///
    /// Concrete implementations SHOULD override this method if they track
    /// consumption state. The default returns `true`, which is appropriate
    /// for implementations that don't track state or where checking state
    /// synchronously isn't feasible (e.g., when state is behind an async mutex).
    fn is_active(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prepared_branchable_read_exits_before_external_acp_and_reuses_registration() {
        let acp = NoopDocumentAcp(true, false);
        let identity = Identity::Anonymous;
        let memo = crate::prepared::PreparedInputMemo::default();
        let result = check_doc_read_access_with_prepared_inputs(
            &acp,
            &identity,
            "policy",
            "User",
            "collection-id",
            true,
            "doc-1",
            Some(&memo),
        )
        .await;
        let Err(crate::error::QueryError::NeedsInput(request)) = result else {
            panic!("expected prepared ACP registration miss, got {result:?}");
        };
        assert_eq!(
            request.kind(),
            crate::prepared::PreparedInputKind::AcpDecision
        );
        let key = crate::prepared::PreparedInputRequest::new(
            crate::prepared::PreparedInputKind::AcpDecision,
            serde_json::to_vec(&(
                "anonymous",
                "registered",
                "policy",
                "User",
                "doc-1",
                None::<String>,
            ))
            .unwrap(),
        );
        assert_eq!(request, key);
        memo.insert(key, Arc::<[u8]>::from(b"false".as_slice()));
        let collection_key = crate::prepared::PreparedInputRequest::new(
            crate::prepared::PreparedInputKind::AcpDecision,
            serde_json::to_vec(&(
                "anonymous",
                "registered",
                "policy",
                "User",
                "collection-id",
                None::<String>,
            ))
            .unwrap(),
        );
        memo.insert(collection_key, Arc::<[u8]>::from(b"false".as_slice()));

        // Unregistered docs in this fixture are public. The branchable rule
        // therefore needs no collection fallback or in-attempt ACP call.
        assert!(check_doc_read_access_with_prepared_inputs(
            &acp,
            &identity,
            "policy",
            "User",
            "collection-id",
            true,
            "doc-1",
            Some(&memo),
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn transactional_acp_reads_stay_live_instead_of_using_retry_memo() {
        let acp = NoopDocumentAcp(false, true);
        let memo = crate::prepared::PreparedInputMemo::default();
        let allowed = check_doc_read_access_with_prepared_inputs(
            &acp,
            &Identity::Anonymous,
            "policy",
            "User",
            "collection-id",
            false,
            "doc-1",
            Some(&memo),
        )
        .await
        .expect("transactional provider must execute its live read");
        assert!(!allowed);
        assert!(memo.is_empty());
    }

    #[tokio::test]
    async fn projected_registration_reports_registered_and_owner_only_access() {
        let mutations = Arc::new(DeferredAcpMutations::new());
        let owner: Did = "did:key:z6MksQ6mYqY1YxR4KQ2NW6y6x8vJrNs2LEd8w5pYxHw3rroA"
            .parse()
            .expect("owner did");
        let stranger: Did = "did:key:z6MkwQm1zZ1p9raM2AfU4vX9y1Aq5PEYB9H9n1tFoZT3zh7T"
            .parse()
            .expect("stranger did");

        mutations
            .lock_state()
            .expect("state lock")
            .projected_registrations
            .insert(
                DeferredAcpMutations::doc_key("policy", "User", "doc-1"),
                ProjectedDocRegistration::Registered {
                    owner: owner.clone(),
                },
            );

        let access = scope_deferred_acp_mutations(
            mutations.clone(),
            check_doc_access_with_overlay(
                &NoopDocumentAcp(true, false),
                &Identity::Authenticated(owner),
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1",
            ),
        )
        .await
        .expect("owner access");
        assert!(access);

        let access = scope_deferred_acp_mutations(
            mutations,
            check_doc_access_with_overlay(
                &NoopDocumentAcp(true, false),
                &Identity::Authenticated(stranger),
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1",
            ),
        )
        .await
        .expect("stranger access");
        assert!(!access);
    }

    #[tokio::test]
    async fn projected_unregistration_reports_public_access() {
        let mutations = Arc::new(DeferredAcpMutations::new());
        mutations
            .lock_state()
            .expect("state lock")
            .projected_registrations
            .insert(
                DeferredAcpMutations::doc_key("policy", "User", "doc-1"),
                ProjectedDocRegistration::Unregistered,
            );

        let access = scope_deferred_acp_mutations(
            mutations.clone(),
            check_doc_access_with_overlay(
                &NoopDocumentAcp(true, false),
                &Identity::Anonymous,
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1",
            ),
        )
        .await
        .expect("anonymous access");
        assert!(access);

        let registered = scope_deferred_acp_mutations(
            mutations,
            is_doc_registered_with_overlay(
                &NoopDocumentAcp(true, false),
                "policy",
                "User",
                "doc-1",
            ),
        )
        .await
        .expect("registration");
        assert!(!registered);
    }

    #[tokio::test]
    async fn private_unregistration_denies_overlay_and_shared_reads() {
        use acp::read_access::{check_doc_read_access, ObjectAccessChecker};
        let acp = NoopDocumentAcp(false, false);
        let checker = crate::txn::OverlayChecker {
            acp: &acp,
            identity: &Identity::Anonymous,
        };
        let absent = checker
            .object_access("policy", "User", "doc-1")
            .await
            .unwrap();
        assert!(!absent.has_access);
        assert!(!absent.explicit);
        let mutations = Arc::new(DeferredAcpMutations::new());
        mutations
            .lock_state()
            .unwrap()
            .projected_registrations
            .insert(
                DeferredAcpMutations::doc_key("policy", "User", "doc-1"),
                ProjectedDocRegistration::Unregistered,
            );
        scope_deferred_acp_mutations(mutations, async {
            assert!(
                !is_doc_registered_with_overlay(&acp, "policy", "User", "doc-1")
                    .await
                    .unwrap()
            );
            assert!(!check_doc_access_with_overlay(
                &acp,
                &Identity::Anonymous,
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1"
            )
            .await
            .unwrap());
            assert!(!check_doc_read_access(
                &checker,
                "policy",
                "User",
                "collection",
                false,
                "doc-1"
            )
            .await
            .unwrap());
        })
        .await;
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl DocumentACP for NoopDocumentAcp {
        fn supports_transactions(&self) -> bool {
            self.1
        }

        fn unregistered_documents_are_public(&self) -> bool {
            self.0
        }

        async fn register_doc_object(
            &self,
            _identity: &Did,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<()> {
            Ok(())
        }

        async fn is_doc_registered(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn get_doc_owner(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<Option<Did>> {
            Ok(None)
        }

        async fn check_doc_access(
            &self,
            _identity: &Identity,
            _permission: DocumentPermission,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn add_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn delete_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn unregister_doc_object(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<()> {
            Ok(())
        }
    }

    struct NoopDocumentAcp(bool, bool);
}
