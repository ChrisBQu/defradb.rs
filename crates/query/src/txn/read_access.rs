//! Txn-overlay ACP checker for the shared read-access rule.

use acp::read_access::{DocAccess, ObjectAccessChecker};
use acp::{DocumentACP, DocumentPermission, Identity};
use async_trait::async_trait;

use super::context::{
    check_doc_access_with_projection, is_doc_registered_with_projection, AcpRegistrationProjection,
};

pub struct OverlayChecker<'a> {
    pub acp: &'a dyn DocumentACP,
    pub projection: Option<&'a AcpRegistrationProjection>,
    pub identity: &'a Identity,
    /// The request's DAC bypass (`RequestContext::dac_bypass`).
    pub dac_bypass: bool,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ObjectAccessChecker for OverlayChecker<'_> {
    async fn object_access(
        &self,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
    ) -> acp::Result<DocAccess> {
        // NAC `bypass-dac` privilege grants full access, exactly as DirectChecker
        // and Go's checkDocAccess (canDACBypass is the first check on every path).
        if self.identity.is_authenticated() && self.dac_bypass {
            return Ok(DocAccess {
                has_access: true,
                explicit: true,
            });
        }

        if !is_doc_registered_with_projection(
            self.acp,
            self.projection,
            policy_id,
            resource_name,
            object_id,
        )
        .await?
        {
            return Ok(DocAccess {
                has_access: self.acp.unregistered_documents_are_public(),
                explicit: false,
            });
        }

        let has_access = check_doc_access_with_projection(
            self.acp,
            self.projection,
            self.identity,
            DocumentPermission::Read,
            policy_id,
            resource_name,
            object_id,
        )
        .await?;

        Ok(DocAccess {
            has_access,
            explicit: true,
        })
    }
}
