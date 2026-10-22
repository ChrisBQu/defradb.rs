use std::ffi::c_int;

use crate::ffi_entry;
use crate::helpers::get_node_runner;
use crate::state::{TxnState, TXNS};
use crate::target::{resolve_node, ERR_INVALID_HANDLE};
use crate::types::{DefraNewTxnResult, DefraResult};
use crate::{ffi_async_ok, try_ffi};

/// Creates a database transaction on node `node_ptr`.
///
/// The returned handle stands in for the node in any call that takes a node
/// or a transaction, which then runs in the transaction. It is released by
/// [`defra_transaction_commit`] or [`defra_transaction_discard`].
#[no_mangle]
pub extern "C" fn defra_transaction_create(
    node_ptr: usize,
    is_read_only: c_int,
) -> DefraNewTxnResult {
    ffi_entry! {
        let node = match resolve_node(node_ptr) {
            Ok(node) => node,
            Err(_) => return DefraNewTxnResult::error(ERR_INVALID_HANDLE),
        };
        let runner = match get_node_runner(node) {
            Ok(runner) => runner,
            Err(_) => return DefraNewTxnResult::error(ERR_INVALID_HANDLE),
        };

        let result = crate::call::wait(async move {
            runner
                .begin_txn(is_read_only != 0)
                .await
                .map(|handle| handle.to_string())
                .map_err(|e| format!("failed to begin transaction: {}", e))
        });

        match result {
            Ok(id) => DefraNewTxnResult::success(TXNS.insert(TxnState { node, id })),
            Err(e) => DefraNewTxnResult::error(e),
        }
    }
}

/// Commits the transaction and releases its handle, whether or not the
/// commit succeeds.
#[no_mangle]
pub extern "C" fn defra_transaction_commit(txn_ptr: usize) -> DefraResult {
    ffi_entry! {
        let txn = try_ffi!(TXNS.remove(txn_ptr).ok_or_else(|| DefraResult::error(ERR_INVALID_HANDLE)));
        let runner = try_ffi!(get_node_runner(txn.node));

        ffi_async_ok!(caller, {
            let handle: query::txn::TransactionHandle = txn
                .id
                .parse()
                .map_err(|e| format!("invalid transaction ID: {}", e))?;
            runner
                .commit_txn(&handle)
                .await
                .map_err(|e| format!("failed to commit transaction: {}", e))
        })
    }
}

/// Discards the transaction and releases its handle. Discarding a handle
/// that is already released does nothing.
#[no_mangle]
pub extern "C" fn defra_transaction_discard(txn_ptr: usize) {
    ffi_entry! {
        let Some(txn) = TXNS.remove(txn_ptr) else {
            return;
        };
        let Ok(runner) = get_node_runner(txn.node) else {
            return;
        };
        let discarded = crate::call::wait(async move {
            let handle: query::txn::TransactionHandle = txn
                .id
                .parse()
                .map_err(|e| format!("invalid transaction ID: {}", e))?;
            runner
                .rollback_txn(&handle)
                .await
                .map_err(|e| format!("failed to discard transaction: {}", e))
        });
        if let Err(error) = discarded {
            tracing::warn!(%error, "discarding a transaction failed");
        }
    }
}
