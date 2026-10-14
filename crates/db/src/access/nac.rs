use crate::database::DB;
use crate::error::{Error, Result};
use acp::nac::NodePermission;
use identity::Did;
use storage::corekv::Store;

impl<S: Store> DB<S> {
    pub(crate) async fn check_node_access_in_txn(
        &self,
        view: datastore::NamespaceView,
        permission: NodePermission,
    ) -> Result<()> {
        let Some(nac) = self.nac_manager() else {
            return Ok(());
        };
        let identity = defra_core::current_identity::get_effective_identity()
            .and_then(|value| Did::new(value).ok());
        if self
            .node_did()
            .is_some_and(|owner| identity.as_ref() == Some(&owner))
        {
            return Ok(());
        }
        let identity = identity.unwrap_or_else(Did::wildcard);
        if nac
            .check_permission_in_txn(view, &identity, permission)
            .await
            .map_err(|error| Error::Acp(error.to_string()))?
        {
            Ok(())
        } else {
            Err(Error::NotAuthorized {
                permission: permission.as_str().to_string(),
            })
        }
    }

    /// Check NAC permission before proceeding with a node-level operation.
    /// Matches Go's `db.checkNodeAccess()` pattern.
    ///
    /// Returns `Ok(())` when NAC is not configured or not enabled (all
    /// operations allowed), or when the identity holds the permission.
    /// Returns `Error::NotAuthorized` when the identity lacks it.
    pub async fn check_node_access(
        &self,
        identity: Option<&Did>,
        permission: NodePermission,
    ) -> Result<()> {
        let nac = match self.nac_manager() {
            Some(nac) => nac,
            None => return Ok(()),
        };
        if !nac.is_enabled().await {
            return Ok(());
        }
        // Resolve the acting identity: explicit param wins, otherwise fall back
        // to the ambient request identity. The task_local scope (set by the HTTP
        // middleware on the multithreaded REST path) takes priority over the
        // thread_local (set on the pinned query/FFI path).
        let resolved: Option<Did> = match identity {
            Some(d) => Some(d.clone()),
            None => defra_core::current_identity::get_effective_identity()
                .and_then(|s| Did::new(s).ok()),
        };
        // Node's own identity always has access (mirrors Go db_nac.go).
        if let Some(node) = self.node_did() {
            if resolved.as_ref() == Some(&node) {
                return Ok(());
            }
        }
        let did = resolved.unwrap_or_else(Did::wildcard);
        let allowed = nac
            .check_permission(&did, permission)
            .await
            .map_err(|e| Error::Acp(e.to_string()))?;
        if allowed {
            Ok(())
        } else {
            Err(Error::NotAuthorized {
                permission: permission.as_str().to_string(),
            })
        }
    }
}
