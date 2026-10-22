use std::ffi::c_char;

use acp::nac::NodePermission;

use super::{json, ok, run};
use crate::ffi_entry;
use crate::state::NODES;
use crate::target::{go_string, split_comma_separated};
use crate::types::DefraResult;

/// Connects to the comma-separated peer multiaddrs `peer_addresses`.
///
/// # Safety
///
/// `peer_addresses` must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_p2p_connect(
    node_ptr: usize,
    peer_addresses: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let addresses = split_comma_separated(&go_string(peer_addresses));
        ok(run(node_ptr, identity_ptr, NodePermission::P2pPeerConnect, None, |ops| async move {
            for address in &addresses {
                ops.connect_peer(address).await?;
            }
            Ok(())
        }))
    }
}

/// Disconnects from the comma-separated peer multiaddrs `peer_addresses`.
///
/// # Safety
///
/// `peer_addresses` must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_p2p_disconnect(
    node_ptr: usize,
    peer_addresses: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let addresses = split_comma_separated(&go_string(peer_addresses));
        ok(run(node_ptr, identity_ptr, NodePermission::P2pPeerDisconnect, None, |ops| async move {
            for address in &addresses {
                ops.disconnect_peer(address).await?;
            }
            Ok(())
        }))
    }
}

/// Answers with the addresses other peers reach this node at, as a JSON
/// array: for libp2p, each listen address with this node's peer ID.
#[no_mangle]
pub extern "C" fn defra_p2p_info(node_ptr: usize, identity_ptr: usize) -> DefraResult {
    ffi_entry! {
        let kind = NODES
            .get(node_ptr, |state| state.p2p.as_ref().map(|p2p| p2p.system.kind()))
            .flatten();
        json(run(node_ptr, identity_ptr, NodePermission::P2pPeerInfo, None, |ops| async move {
            let addresses = ops.listen_addresses().await?;
            match kind {
                #[cfg(feature = "libp2p")]
                Some(embedded::TransportKind::Libp2p) => {
                    let peer_id = ops.local_peer_id().await?;
                    Ok(addresses
                        .into_iter()
                        .map(|address| format!("{}/p2p/{}", address, peer_id))
                        .collect())
                }
                _ => Ok(addresses),
            }
        }))
    }
}

/// Answers with the peers the node is connected to as a JSON array.
#[no_mangle]
pub extern "C" fn defra_p2p_peer_list(node_ptr: usize, identity_ptr: usize) -> DefraResult {
    ffi_entry! {
        json(run(node_ptr, identity_ptr, NodePermission::P2pPeerActive, None, |ops| async move {
            ops.connected_peers().await
        }))
    }
}
