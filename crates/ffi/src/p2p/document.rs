use std::ffi::c_char;

use acp::nac::NodePermission;
use defra_http::router::P2pDocumentRequest;

use super::{json, ok, run, timeout};
use crate::target::{go_string, split_comma_separated};
use crate::types::DefraResult;
use crate::{ffi_entry, try_ffi};

/// The documents a comma-separated list of document IDs names; each one's
/// collection is found from its ID.
fn documents(doc_ids: &str) -> Vec<P2pDocumentRequest> {
    split_comma_separated(doc_ids)
        .into_iter()
        .map(|doc_id| P2pDocumentRequest {
            collection: String::new(),
            doc_id,
        })
        .collect()
}

/// Adds the comma-separated document IDs `doc_ids` to the documents the
/// node replicates.
///
/// # Safety
///
/// `doc_ids` must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_p2p_document_add(
    node_ptr: usize,
    doc_ids: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let docs = documents(&go_string(doc_ids));
        ok(run(node_ptr, identity_ptr, NodePermission::P2pDocumentAdd, None, |ops| async move {
            ops.add_documents(docs).await
        }))
    }
}

/// Removes the comma-separated document IDs `doc_ids` from the documents the
/// node replicates.
///
/// # Safety
///
/// `doc_ids` must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_p2p_document_delete(
    node_ptr: usize,
    doc_ids: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let docs = documents(&go_string(doc_ids));
        ok(run(node_ptr, identity_ptr, NodePermission::P2pDocumentDelete, None, |ops| async move {
            ops.remove_documents(docs).await
        }))
    }
}

/// Answers with the documents the node replicates as JSON.
#[no_mangle]
pub extern "C" fn defra_p2p_document_list(node_ptr: usize, identity_ptr: usize) -> DefraResult {
    ffi_entry! {
        json(run(node_ptr, identity_ptr, NodePermission::P2pDocumentList, None, |ops| async move {
            ops.get_documents().await
        }))
    }
}

/// Synchronizes the comma-separated documents `doc_ids` of collection
/// `collection` from connected peers, within `timeout_str`, a Go duration
/// (null or empty for none).
///
/// # Safety
///
/// Every string must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_p2p_document_sync(
    node_ptr: usize,
    collection: *const c_char,
    doc_ids: *const c_char,
    timeout_str: *const c_char,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let limit = try_ffi!(timeout(timeout_str));
        let collection = go_string(collection);
        let doc_ids = split_comma_separated(&go_string(doc_ids));
        ok(run(node_ptr, identity_ptr, NodePermission::P2pSyncDocuments, limit, |ops| async move {
            ops.sync_documents(&collection, doc_ids, limit).await
        }))
    }
}
