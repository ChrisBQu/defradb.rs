use std::ffi::c_char;
use std::future::Future;
use std::sync::Arc;

use edge::{Edge, EdgeError};

use crate::state::NODES;
use crate::types::{c_str_to_string, FfiResult};
use crate::ERR_INVALID_NODE_HANDLE;

/// What a call made before `defra_init` reports.
pub(crate) const ERR_RUNTIME_NOT_INITIALIZED: &str =
    "runtime not initialized - call defra_init() first";

/// Get the network runtime's handle or return an FfiResult error.
pub fn get_rt() -> Result<&'static Edge, FfiResult> {
    crate::runtime::edge().ok_or_else(|| FfiResult::error(ERR_RUNTIME_NOT_INITIALIZED))
}

/// Runs `future`, network work, on the network runtime, blocking the C
/// caller's thread until it completes. A failure of the runtime itself (it is
/// shutting down, or the call panicked) becomes the call's own error.
pub fn block_on<F, T, E>(rt: &Edge, future: F) -> Result<T, E>
where
    F: Future<Output = Result<T, E>>,
    E: From<EdgeError>,
{
    rt.block_on(future)?
}

/// Parse a required C string, returning FfiResult error if null.
///
/// # Safety
/// `ptr` must be null or a valid null-terminated UTF-8 string.
pub unsafe fn require_c_str(ptr: *const c_char, name: &str) -> Result<String, FfiResult> {
    c_str_to_string(ptr).ok_or_else(|| FfiResult::error(format!("{} is null", name)))
}

/// Extract the database handle from node state.
pub fn get_node_database(node_ptr: usize) -> Result<Arc<crate::state::FfiDatabase>, FfiResult> {
    NODES
        .get(node_ptr, |state| state.database.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))
}

/// Extract the query runner from node state.
pub fn get_node_runner(node_ptr: usize) -> Result<Arc<dyn query::QueryExecutor>, FfiResult> {
    NODES
        .get(node_ptr, |state| state.query_runner.clone())
        .ok_or_else(|| FfiResult::error(ERR_INVALID_NODE_HANDLE))
}
