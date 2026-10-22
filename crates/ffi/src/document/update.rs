use std::ffi::c_char;

use acp::nac::NodePermission;

use super::run::{doc_ids, DocumentCall};
use super::{filter_value, Selection, ERR_DOCUMENT_NOT_FOUND};
use crate::target::go_string;
use crate::types::{CollectionOptions, DefraResult};
use crate::{ffi_async, ffi_entry, try_ffi};

/// Applies the JSON `updater` to the document `doc_id` of the collection
/// `opts` selects, answering with no value; or, without a document ID, to
/// every document matching the JSON `filter`, answering with
/// `{"Count": ..., "DocIDs": [...]}`.
///
/// # Safety
///
/// `doc_id`, `filter` and `updater` must be null or valid null-terminated
/// strings; every non-null string in `opts` must be one too.
#[no_mangle]
pub unsafe extern "C" fn defra_document_update(
    node_ptr: usize,
    doc_id: *const c_char,
    filter: *const c_char,
    updater: *const c_char,
    opts: CollectionOptions,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let selection = try_ffi!(Selection::read(doc_id, filter));
        let call = try_ffi!(DocumentCall::begin(node_ptr, &opts, identity_ptr, NodePermission::DocumentUpdate));
        let updater = go_string(updater);
        let target = call.target().clone();
        ffi_async!(target &target, {
            let updater: serde_json::Value = serde_json::from_str(&updater)
                .map_err(|e| format!("invalid updater JSON: {}", e))?;
            let name = call.collection().await?.name;
            let field = format!("update_{}", name);
            match selection {
                Selection::Doc(doc_id) => {
                    let mutation = query::rest::gql::build_update_mutation(&name, &doc_id, &updater)
                        .map_err(|e| e.to_string())?;
                    let updated = doc_ids(&call.run(mutation).await?, &field);
                    if updated.is_empty() {
                        return Err(ERR_DOCUMENT_NOT_FOUND.to_string());
                    }
                    Ok(String::new())
                }
                Selection::Filter(filter) => {
                    let mutation = query::rest::gql::build_filtered_update_mutation(
                        &name,
                        &filter_value(&filter)?,
                        &updater,
                    )
                    .map_err(|e| e.to_string())?;
                    let updated = doc_ids(&call.run(mutation).await?, &field);
                    Ok(serde_json::json!({ "Count": updated.len(), "DocIDs": updated }).to_string())
                }
            }
        })
    }
}
