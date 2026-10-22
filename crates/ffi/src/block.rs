//! Block signature verification.

use std::ffi::c_char;

use acp::nac::NodePermission;

use crate::request::Request;
use crate::state::NODES;
use crate::target::go_string;
use crate::types::DefraResult;
use crate::{ffi_async, ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};

/// What a verified block answers with.
const VERIFIED: &str = "Block's signature verified.";

/// Verifies that block `cid` was signed by `public_key`, of type `key_type`
/// ("secp256k1", the default for null or empty, "secp256r1" or "ed25519"),
/// on a node or in a transaction.
///
/// # Safety
///
/// Every string must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_block_verify(
    node_ptr: usize,
    key_type: *const c_char,
    public_key: *const c_char,
    cid: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::SignatureVerify));
        let crypto_key_type = match go_string(key_type).as_str() {
            "" | "secp256k1" => crypto::KeyType::Secp256k1,
            "secp256r1" => crypto::KeyType::Secp256r1,
            "ed25519" => crypto::KeyType::Ed25519,
            other => return DefraResult::error(format!("unsupported key type: {}", other)),
        };
        let public_key = go_string(public_key);
        let cid = go_string(cid);
        let context = request.context();
        let caller_identity: acp::Identity = try_ffi!(request.did()).into();
        let (database, registry, document_acp) = try_ffi!(NODES
            .get(request.node(), |state| {
                (
                    state.database.clone(),
                    state.txn_registry.clone(),
                    state.document_acp.clone(),
                )
            })
            .ok_or_else(|| DefraResult::error(ERR_INVALID_NODE_HANDLE)));

        match request.target.txn_id() {
            None => ffi_async!(request.node(), {
                db::block::verify::verify_block_signature_as(
                    &context,
                    &database,
                    document_acp.as_ref(),
                    &cid,
                    &public_key,
                    crypto_key_type,
                    &caller_identity,
                )
                .await?;
                Ok(VERIFIED.to_string())
            }),
            Some(txn_id) => {
                let txn_id = txn_id.to_string();
                ffi_async!(caller, {
                    registry
                        .verify_block_signature_in_txn_as(
                            &context,
                            &txn_id,
                            document_acp,
                            &cid,
                            &public_key,
                            crypto_key_type,
                            &caller_identity,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(VERIFIED.to_string())
                })
            }
        }
    }
}
