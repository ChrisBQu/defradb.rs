//! P2P calls for an app on a phone, which Go's cbindings have no
//! counterpart for.

use acp::nac::NodePermission;

use super::{admit, drive, json, ok, run};
use crate::ffi_entry;
use crate::types::DefraResult;

/// Answers with the best address to share with another peer so it can dial
/// this node, as a JSON string, or JSON `null` while there is none yet (the
/// node runs without P2P, or its transport has no dialable address).
#[no_mangle]
pub extern "C" fn defra_mobile_shareable_address(
    node_ptr: usize,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        match admit(node_ptr, identity_ptr, NodePermission::P2pPeerInfo) {
            Ok(Some(ops)) => json(drive(ops, None, |ops| async move { ops.shareable_address().await })),
            Ok(None) => DefraResult::success("null"),
            Err(error) => error,
        }
    }
}

/// Answers with the node's live P2P sync status as JSON: its outbound push
/// backlog, the DAGs it is still fetching, and its retry and overload
/// counters.
#[no_mangle]
pub extern "C" fn defra_mobile_sync_status(node_ptr: usize, identity_ptr: usize) -> DefraResult {
    ffi_entry! {
        json(run(node_ptr, identity_ptr, NodePermission::P2pPeerInfo, None, |ops| async move {
            ops.sync_status().await
        }))
    }
}

/// Tells the node its network may have changed (Wi-Fi to cellular, a new
/// address, waking up), so the iroh transport looks for new paths to its
/// peers at once instead of waiting to notice. With libp2p it does nothing.
#[no_mangle]
pub extern "C" fn defra_mobile_notify_network_change(
    node_ptr: usize,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        ok(run(node_ptr, identity_ptr, NodePermission::P2pPeerConnect, None, |ops| async move {
            ops.notify_network_change().await
        }))
    }
}
