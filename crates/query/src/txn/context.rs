//! Transaction context trait.

use acp::error::{Error as AcpError, Result as AcpResult};
use acp::DocumentACP;
use acp::DocumentPermission;
use acp::Identity;
use identity::Did;
use rapidhash::RapidHashMap;
use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use crate::fetcher::CollectionProvider;
use crate::fetcher::DocFetcher;
use crate::mutator::DocMutator;

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

#[derive(Default)]
struct AcpRegistrationState {
    projected_registrations: RapidHashMap<DocRegistrationKey, ProjectedDocRegistration>,
    savepoints: Vec<AcpRegistrationSavepoint>,
}

struct AcpRegistrationSavepoint {
    projected_registrations: RapidHashMap<DocRegistrationKey, ProjectedDocRegistration>,
}

/// Transaction or request scoped projection of external ACP registration writes.
///
/// External ACP providers cannot participate in the storage transaction. This
/// projection preserves read-your-own-registration behavior while the ordered
/// post-commit hooks remain attached to the transaction that owns the document
/// writes. It does not make remote ACP updates atomic with storage.
#[derive(Default)]
pub struct AcpRegistrationProjection {
    state: std::sync::Mutex<AcpRegistrationState>,
}

/// Temporarily restore a captured request JWT while an ordered ACP hook runs.
pub struct RequestBearerTokenGuard {
    did: String,
    previous_token: Option<String>,
}

impl RequestBearerTokenGuard {
    pub fn new(did: &str, token: Option<String>) -> Option<Self> {
        let token = token?;
        let previous_token = defra_core::signing::get_request_bearer_token(did);
        defra_core::signing::set_request_bearer_token(did, token);
        Some(Self {
            did: did.to_string(),
            previous_token,
        })
    }
}

impl Drop for RequestBearerTokenGuard {
    fn drop(&mut self) {
        if let Some(previous_token) = self.previous_token.take() {
            defra_core::signing::set_request_bearer_token(&self.did, previous_token);
        } else {
            defra_core::signing::clear_request_bearer_token(&self.did);
        }
    }
}

impl AcpRegistrationProjection {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, AcpRegistrationState>, String> {
        self.state
            .lock()
            .map_err(|_| "ACP registration projection lock poisoned".to_string())
    }

    pub fn set_statement_savepoint(&self) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let savepoint = AcpRegistrationSavepoint {
            projected_registrations: state.projected_registrations.clone(),
        };
        state.savepoints.push(savepoint);
        Ok(())
    }

    pub fn rollback_statement_savepoint(&self) -> Result<(), String> {
        let mut state = self.lock_state()?;
        let savepoint = state
            .savepoints
            .pop()
            .ok_or_else(|| "ACP projection has no active statement savepoint".to_string())?;
        state.projected_registrations = savepoint.projected_registrations;
        Ok(())
    }

    pub fn release_statement_savepoint(&self) -> Result<(), String> {
        let mut state = self.lock_state()?;
        state
            .savepoints
            .pop()
            .map(|_| ())
            .ok_or_else(|| "ACP projection has no active statement savepoint".to_string())
    }

    fn doc_key(policy_id: &str, resource_name: &str, doc_id: &str) -> DocRegistrationKey {
        DocRegistrationKey {
            policy_id: policy_id.to_string(),
            resource_name: resource_name.to_string(),
            doc_id: doc_id.to_string(),
        }
    }

    fn projected_registration(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> AcpResult<Option<ProjectedDocRegistration>> {
        let state = self.state.lock().map_err(|_| {
            AcpError::Storage("ACP registration projection lock poisoned".to_string())
        })?;
        Ok(state
            .projected_registrations
            .get(&Self::doc_key(policy_id, resource_name, doc_id))
            .cloned())
    }

    pub fn project_registration(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        owner: Did,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "ACP registration projection lock poisoned".to_string())?;
        state.projected_registrations.insert(
            Self::doc_key(policy_id, resource_name, doc_id),
            ProjectedDocRegistration::Registered { owner },
        );
        Ok(())
    }

    pub fn project_unregistration(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "ACP registration projection lock poisoned".to_string())?;
        state.projected_registrations.insert(
            Self::doc_key(policy_id, resource_name, doc_id),
            ProjectedDocRegistration::Unregistered,
        );
        Ok(())
    }
}

/// Check document registration using an explicit request/transaction projection.
pub async fn is_doc_registered_with_projection(
    acp: &dyn DocumentACP,
    projection: Option<&AcpRegistrationProjection>,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
) -> AcpResult<bool> {
    if let Some(projection) = projection {
        if let Some(projected) =
            projection.projected_registration(policy_id, resource_name, doc_id)?
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

/// Resolve registration state from attempt-local projection, the live
/// transaction provider, or an exact request memo for external providers.
pub async fn is_doc_registered_with_prepared_inputs(
    acp: &dyn DocumentACP,
    projection: Option<&AcpRegistrationProjection>,
    identity: &Identity,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
    inputs: Option<&crate::prepared::PreparedInputMemo>,
) -> crate::error::Result<bool> {
    if let Some(projection) = projection {
        if let Some(projected) = projection
            .projected_registration(policy_id, resource_name, doc_id)
            .map_err(|error| crate::error::QueryError::execution(error.to_string()))?
        {
            return Ok(matches!(
                projected,
                ProjectedDocRegistration::Registered { .. }
            ));
        }
    }
    if acp.supports_transactions() || inputs.is_none() {
        return is_doc_registered_with_projection(
            acp,
            projection,
            policy_id,
            resource_name,
            doc_id,
        )
        .await
        .map_err(|error| crate::error::QueryError::execution(error.to_string()));
    }
    let bearer = identity
        .did()
        .and_then(|did| defra_core::signing::get_request_bearer_token(did.as_str()));
    let exact_request = serde_json::to_vec(&(
        identity.did().map(Did::as_str).unwrap_or("anonymous"),
        "registered",
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
    let Some(response) = inputs.expect("checked above").get(&request) else {
        return Err(crate::error::QueryError::NeedsInput(request));
    };
    serde_json::from_slice(&response).map_err(|error| {
        crate::error::QueryError::execution(format!("cached ACP response is invalid: {error}"))
    })
}

/// Check document access using an explicit request/transaction projection.
pub async fn check_doc_access_with_projection(
    acp: &dyn DocumentACP,
    projection: Option<&AcpRegistrationProjection>,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
) -> AcpResult<bool> {
    if let Some(projection) = projection {
        if let Some(projected) =
            projection.projected_registration(policy_id, resource_name, doc_id)?
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
#[allow(clippy::too_many_arguments)]
pub async fn check_doc_access_with_prepared_inputs(
    acp: &dyn DocumentACP,
    projection: Option<&AcpRegistrationProjection>,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
    inputs: Option<&crate::prepared::PreparedInputMemo>,
) -> crate::error::Result<bool> {
    // Projected registrations override remote state, while transaction-capable
    // providers stay live so their reads participate in OCC.
    if let Some(projected) = projection
        .map(|projection| projection.projected_registration(policy_id, resource_name, doc_id))
        .transpose()
        .map_err(|error| crate::error::QueryError::execution(error.to_string()))?
        .flatten()
    {
        return Ok(match projected {
            ProjectedDocRegistration::Unregistered => acp.unregistered_documents_are_public(),
            ProjectedDocRegistration::Registered { owner } => {
                matches!(identity, Identity::Authenticated(did) if did == &owner)
            }
        });
    }
    if acp.supports_transactions() || inputs.is_none() {
        return check_doc_access_with_projection(
            acp,
            projection,
            identity,
            permission,
            policy_id,
            resource_name,
            doc_id,
        )
        .await
        .map_err(|error| crate::error::QueryError::execution(error.to_string()));
    }
    let inputs = inputs.expect("checked above");
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

/// Resolve shared object/branchable read permissions from the request memo while
/// retaining live transaction reads and attempt-local projected registrations.
#[allow(clippy::too_many_arguments)]
pub async fn check_doc_read_access_with_prepared_inputs(
    acp: &dyn DocumentACP,
    projection: Option<&AcpRegistrationProjection>,
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
        let checker = crate::txn::OverlayChecker {
            acp,
            projection,
            identity,
        };
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
        projection: Option<&AcpRegistrationProjection>,
        identity: &Identity,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
        inputs: &crate::prepared::PreparedInputMemo,
    ) -> crate::error::Result<DocAccess> {
        if let Some(projected) = projection
            .map(|projection| {
                projection.projected_registration(policy_id, resource_name, object_id)
            })
            .transpose()
            .map_err(|error| crate::error::QueryError::execution(error.to_string()))?
            .flatten()
        {
            let (has_access, explicit) = match projected {
                ProjectedDocRegistration::Unregistered => {
                    (acp.unregistered_documents_are_public(), false)
                }
                ProjectedDocRegistration::Registered { owner } => (
                    matches!(identity, Identity::Authenticated(did) if did == &owner),
                    true,
                ),
            };
            return Ok(DocAccess {
                has_access,
                explicit,
            });
        }

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
            projection,
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
        let object = access_for_object(
            acp,
            projection,
            identity,
            policy_id,
            resource_name,
            doc_id,
            inputs,
        )
        .await?;
        if object.explicit || !is_branchable {
            return Ok(object.has_access);
        }
    }
    if is_branchable {
        let collection = access_for_object(
            acp,
            projection,
            identity,
            policy_id,
            resource_name,
            collection_id,
            inputs,
        )
        .await?;
        if !collection.has_access {
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

    /// Get transaction-scoped projection for external ACP registration writes.
    fn acp_registration_projection(&self) -> Option<Arc<AcpRegistrationProjection>> {
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
    async fn projected_registration_reports_registered_and_owner_only_access() {
        let projection = AcpRegistrationProjection::new();
        let owner: Did = "did:key:z6MksQ6mYqY1YxR4KQ2NW6y6x8vJrNs2LEd8w5pYxHw3rroA"
            .parse()
            .unwrap();
        let stranger: Did = "did:key:z6MkwQm1zZ1p9raM2AfU4vX9y1Aq5PEYB9H9n1tFoZT3zh7T"
            .parse()
            .unwrap();
        projection
            .project_registration("policy", "User", "doc-1", owner.clone())
            .unwrap();

        assert!(check_doc_access_with_projection(
            &NoopDocumentAcp(true),
            Some(&projection),
            &Identity::Authenticated(owner),
            DocumentPermission::Read,
            "policy",
            "User",
            "doc-1",
        )
        .await
        .unwrap());
        assert!(!check_doc_access_with_projection(
            &NoopDocumentAcp(true),
            Some(&projection),
            &Identity::Authenticated(stranger),
            DocumentPermission::Read,
            "policy",
            "User",
            "doc-1",
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn projected_unregistration_reports_public_access() {
        let projection = AcpRegistrationProjection::new();
        projection
            .project_unregistration("policy", "User", "doc-1")
            .unwrap();

        assert!(check_doc_access_with_projection(
            &NoopDocumentAcp(true),
            Some(&projection),
            &Identity::Anonymous,
            DocumentPermission::Read,
            "policy",
            "User",
            "doc-1",
        )
        .await
        .unwrap());
        assert!(!is_doc_registered_with_projection(
            &NoopDocumentAcp(true),
            Some(&projection),
            "policy",
            "User",
            "doc-1",
        )
        .await
        .unwrap());
    }

    #[tokio::test]
    async fn private_unregistration_denies_overlay_and_shared_reads() {
        use acp::read_access::{check_doc_read_access, ObjectAccessChecker};
        let acp = NoopDocumentAcp(false);
        let projection = AcpRegistrationProjection::new();
        projection
            .project_unregistration("policy", "User", "doc-1")
            .unwrap();
        let checker = crate::txn::OverlayChecker {
            acp: &acp,
            projection: Some(&projection),
            identity: &Identity::Anonymous,
        };
        let absent = checker
            .object_access("policy", "User", "doc-1")
            .await
            .unwrap();
        assert!(!absent.has_access);
        assert!(!absent.explicit);
        assert!(
            !check_doc_read_access(&checker, "policy", "User", "collection", false, "doc-1",)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn projected_unregistered_branchable_doc_falls_back_to_collection_policy() {
        let projection = AcpRegistrationProjection::new();
        projection
            .project_unregistration("policy", "User", "doc-1")
            .unwrap();
        let memo = crate::prepared::PreparedInputMemo::default();
        let request_bytes = serde_json::to_vec(&(
            "anonymous",
            "registered",
            "policy",
            "User",
            "collection-1",
            None::<String>,
        ))
        .unwrap();
        let request = crate::prepared::PreparedInputRequest::new(
            crate::prepared::PreparedInputKind::AcpDecision,
            request_bytes,
        );
        assert!(memo.insert(request, std::sync::Arc::<[u8]>::from(b"true".as_slice())));
        let permission_request = crate::prepared::PreparedInputRequest::new(
            crate::prepared::PreparedInputKind::AcpDecision,
            serde_json::to_vec(&(
                "anonymous",
                "read",
                "policy",
                "User",
                "collection-1",
                None::<String>,
            ))
            .unwrap(),
        );
        assert!(memo.insert(
            permission_request,
            std::sync::Arc::<[u8]>::from(b"false".as_slice()),
        ));

        let allowed = check_doc_read_access_with_prepared_inputs(
            &NoopDocumentAcp(true),
            Some(&projection),
            &Identity::Anonymous,
            "policy",
            "User",
            "collection-1",
            true,
            "doc-1",
            Some(&memo),
        )
        .await
        .unwrap();
        assert!(
            !allowed,
            "a public unregistered object still falls back to its branchable collection"
        );
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl DocumentACP for NoopDocumentAcp {
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

    struct NoopDocumentAcp(bool);
}
