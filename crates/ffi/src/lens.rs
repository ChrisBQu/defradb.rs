//! Lenses: WASM transforms, and the migrations between collection versions
//! they make.

use std::ffi::c_char;

use acp::nac::NodePermission;
use lens::LensConfig;

use crate::request::Request;
use crate::target::go_string;
use crate::types::DefraResult;
use crate::{ffi_async, ffi_entry, try_ffi};

/// A lens configuration, Go's `model.Lens`: `{"Lenses": [...]}`.
fn parse_lens(config: &str) -> Result<LensConfig, String> {
    serde_json::from_str(config).map_err(|e| format!("failed to parse lens config: {}", e))
}

/// Adds a lens, on a node or in a transaction, and answers with its ID.
///
/// # Safety
///
/// `cfg` must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_lens_add(
    node_ptr: usize,
    identity_ptr: usize,
    cfg: *const c_char,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::LensCreate));
        let cfg = go_string(cfg);
        let context = request.context();
        match request.target.txn_id() {
            None => {
                let database = try_ffi!(request.database());
                ffi_async!(request.node(), {
                    let config = parse_lens(&cfg)?;
                    database
                        .add_lens(config)
                        .await
                        .map(|id| id.to_string())
                        .map_err(|e| format!("failed to add lens: {}", e))
                })
            }
            Some(txn_id) => {
                let txn_id = txn_id.to_string();
                let registry = try_ffi!(request.txn_registry());
                ffi_async!(caller, {
                    let config = parse_lens(&cfg)?;
                    registry
                        .add_lens_in_txn_as(&context, &txn_id, config)
                        .await
                        .map(|id| id.to_string())
                        .map_err(|e| format!("failed to add lens: {}", e))
                })
            }
        }
    }
}

/// Answers with the node's lenses as JSON, on a node or in a transaction.
#[no_mangle]
pub extern "C" fn defra_lens_list(node_ptr: usize, identity_ptr: usize) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::LensList));
        let context = request.context();
        match request.target.txn_id() {
            None => {
                let database = try_ffi!(request.database());
                ffi_async!(request.node(), {
                    let lenses = database
                        .lens_store()
                        .list()
                        .await
                        .map_err(|e| format!("failed to list lenses: {}", e))?;
                    serde_json::to_string(&lenses)
                        .map_err(|e| format!("failed to serialize lenses: {}", e))
                })
            }
            Some(txn_id) => {
                let txn_id = txn_id.to_string();
                let registry = try_ffi!(request.txn_registry());
                ffi_async!(caller, {
                    let lenses = registry
                        .list_lenses_in_txn_as(&context, &txn_id)
                        .await
                        .map_err(|e| format!("failed to list lenses: {}", e))?;
                    serde_json::to_string(&lenses)
                        .map_err(|e| format!("failed to serialize lenses: {}", e))
                })
            }
        }
    }
}

/// Sets the lens `cfg` as the migration from collection version `src` to
/// `dst`, on a node or in a transaction, and answers with the lens ID.
///
/// # Safety
///
/// `src`, `dst` and `cfg` must be null or valid null-terminated strings.
#[no_mangle]
pub unsafe extern "C" fn defra_lens_set(
    node_ptr: usize,
    identity_ptr: usize,
    src: *const c_char,
    dst: *const c_char,
    cfg: *const c_char,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::MigrationSet));
        let (src, dst, cfg) = (go_string(src), go_string(dst), go_string(cfg));
        let context = request.context();
        let migration = move || {
            let mut config = parse_lens(&cfg)?;
            config.source_schema_version_id = src;
            config.destination_schema_version_id = dst;
            Ok::<_, String>(config)
        };
        match request.target.txn_id() {
            None => {
                let database = try_ffi!(request.database());
                ffi_async!(request.node(), {
                    database
                        .set_migration_as(&context, migration()?)
                        .await
                        .map(|id| id.to_string())
                        .map_err(|e| format!("failed to set migration: {}", e))
                })
            }
            Some(txn_id) => {
                let txn_id = txn_id.to_string();
                let registry = try_ffi!(request.txn_registry());
                ffi_async!(caller, {
                    registry
                        .set_migration_in_txn_as(&context, &txn_id, migration()?)
                        .await
                        .map(|id| id.to_string())
                        .map_err(|e| format!("failed to set migration: {}", e))
                })
            }
        }
    }
}
