//! NAC (Node Access Control) permission checking for FFI functions.
//!
//! Provides a reusable helper that FFI functions call before executing
//! their operations. When NAC is not configured or disabled, all
//! operations pass through. When enabled, the caller's identity is
//! checked against the required permission.

use std::ffi::c_char;
use std::sync::Arc;

use acp::nac::{NacStatus, NodePermission};
use edge::Edge;

use crate::state::{FfiNacManager, NODES};
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// Check NAC permission for an FFI operation.
///
/// Returns `Ok(())` if authorized, `Err(FfiResult)` if denied.
///
/// - NAC not configured or disabled -> always Ok
/// - NAC enabled + no identity -> Err (not authorized)
/// - NAC enabled + identity lacks permission -> Err (not authorized)
/// - NAC enabled + identity has permission -> Ok
#[allow(clippy::not_unsafe_ptr_arg_deref)]
pub fn check_nac_permission(
    rt: &Edge,
    nac_manager: &Arc<FfiNacManager>,
    identity_did: *const c_char,
    permission: NodePermission,
) -> Result<(), FfiResult> {
    // SAFETY: `identity_did` is either null or a valid C string from the FFI caller.
    let identity_str = unsafe { c_str_to_string(identity_did) };

    // One trip to the runtime decides. Check runtime NAC status directly
    // rather than is_enabled(), because is_enabled() also requires
    // config.enabled which is a startup flag. FFI nodes start with
    // config.enabled=false and enable NAC at runtime via enable_nac().
    // An empty identity is checked as the wildcard; a malformed one is
    // denied. A call the runtime cannot run fails with its reason.
    let allowed = rt
        .block_on(async {
            if nac_manager.status().await != NacStatus::Enabled {
                return true;
            }
            let did = match identity_str {
                Some(s) if !s.is_empty() => match identity::Did::new(&s) {
                    Ok(d) => d,
                    Err(_) => return false,
                },
                _ => identity::Did::wildcard(),
            };
            nac_manager
                .check_permission(&did, permission)
                .await
                .unwrap_or(false)
        })
        .map_err(|error| FfiResult::error(error.to_string()))?;

    if allowed {
        Ok(())
    } else {
        Err(FfiResult::error(format!(
            "not authorized to perform operation. Permission: {}",
            permission.as_str()
        )))
    }
}

/// Convenience: extract NAC manager from node and check permission.
///
/// Returns `Ok(())` if authorized, `Err(FfiResult)` if the node handle
/// is invalid or the caller is not authorized.
pub fn check_nac_for_node(
    rt: &Edge,
    node_ptr: usize,
    identity_did: *const c_char,
    permission: NodePermission,
) -> Result<(), FfiResult> {
    let nac_manager = NODES
        .get(node_ptr, |state| state.nac_manager.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))?;

    check_nac_permission(rt, &nac_manager, identity_did, permission)
}
