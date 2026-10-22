use std::ffi::c_char;

use acp::nac::NodePermission;

use super::policy::validate_collection_policy;
use crate::request::Request;
use crate::state::NODES;
use crate::target::go_string;
use crate::types::DefraResult;
use crate::{ffi_async, ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};

/// Adds the collections an SDL document defines, on a node or in a
/// transaction, and answers with the created collection versions as JSON.
///
/// # Safety
///
/// `sdl` must be null or a valid null-terminated UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn defra_collection_add(
    node_ptr: usize,
    sdl: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::CollectionPatch));
        let sdl = go_string(sdl);
        let creator = try_ffi!(request.did());
        let context = request.context();
        let (database, policy_store, document_acp) = try_ffi!(NODES
            .get(request.node(), |state| {
                (
                    state.database.clone(),
                    state.policy_store.clone(),
                    state.document_acp.clone(),
                )
            })
            .ok_or_else(|| DefraResult::error(ERR_INVALID_NODE_HANDLE)));

        let created = match request.target.txn_id() {
            None => ffi_async!(request.node(), {
                // Known collection names let the SDL refer to collections
                // that already exist.
                let known_types: rapidhash::RapidHashSet<String> =
                    database.list_collections().unwrap_or_default().into_iter().collect();
                let collections = query::parse_sdl_with_known_types(&sdl, known_types)
                    .map_err(|e| format!("failed to parse schema: {}", e))?;
                schema::definition_validation::validate_new_collections(&collections)
                    .map_err(|e| format!("failed to validate schema: {}", e))?;
                for collection in &collections {
                    if let Some(ref policy) = collection.policy {
                        validate_collection_policy(policy, &policy_store)?;
                    }
                }
                let created = database
                    .create_collections_atomic_with_acp_registration_as(
                        &context,
                        collections,
                        document_acp,
                        creator,
                    )
                    .await
                    .map_err(|e| format!("failed to create collection: {}", e))?;
                serde_json::to_string(&created)
                    .map_err(|e| format!("failed to serialize result: {}", e))
            }),
            Some(txn_id) => {
                let txn_id = txn_id.to_string();
                let registry = try_ffi!(request.txn_registry());
                ffi_async!(caller, {
                    let known_types: rapidhash::RapidHashSet<String> = registry
                        .get_collections_in_txn_as(&context, &txn_id)
                        .await
                        .map_err(|e| format!("failed to get collections in txn: {}", e))?
                        .into_iter()
                        .map(|col| col.name)
                        .collect();
                    let collections = query::parse_sdl_with_known_types(&sdl, known_types)
                        .map_err(|e| format!("failed to parse schema: {}", e))?;
                    schema::definition_validation::validate_new_collections(&collections)
                        .map_err(|e| format!("failed to validate schema: {}", e))?;
                    for collection in &collections {
                        if let Some(ref policy) = collection.policy {
                            validate_collection_policy(policy, &policy_store)?;
                        }
                    }
                    let created = registry
                        .add_schema_in_txn_with_acp_as(
                            &context,
                            &txn_id,
                            &sdl,
                            Some(document_acp),
                            creator,
                        )
                        .await
                        .map_err(|e| format!("failed to create collection in txn: {}", e))?;
                    serde_json::to_string(&created)
                        .map_err(|e| format!("failed to serialize result: {}", e))
                })
            }
        };
        created
    }
}
