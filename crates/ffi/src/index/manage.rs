use std::ffi::c_char;

use acp::nac::NodePermission;
use defra_core::{ActionExecution, ActionStatus};
use serde::Serialize;
use storage::corekv::Key;

use crate::request::Request;
use crate::target::{go_string, CollectionArgs};
use crate::types::{CollectionOptions, DefraResult};
use crate::{ffi_async, ffi_entry, try_ffi};

fn visible_indexes(collection: &db::Collection) -> Vec<schema::IndexDescription> {
    collection
        .get_indexes()
        .iter()
        .filter(|index| !is_hidden_auto_relation_index(collection, index))
        .cloned()
        .collect()
}

#[derive(Serialize)]
struct IndexResult {
    #[serde(flatten)]
    description: schema::IndexDescription,
    #[serde(rename = "CollectionName")]
    collection_name: String,
    #[serde(rename = "Execution")]
    execution: ActionExecution,
}

fn index_results(
    collection: &db::Collection,
    actions: &rapidhash::RapidHashMap<u32, ActionExecution>,
) -> Vec<IndexResult> {
    visible_indexes(collection)
        .into_iter()
        .map(|description| {
            let execution =
                actions
                    .get(&description.id)
                    .cloned()
                    .unwrap_or_else(|| ActionExecution {
                        collection_id: collection.collection_id().to_string(),
                        subject: description.id.to_string(),
                        status: ActionStatus::COMPLETED,
                        ..Default::default()
                    });
            IndexResult {
                description,
                collection_name: collection.name().to_string(),
                execution,
            }
        })
        .collect()
}

fn is_hidden_auto_relation_index(
    collection: &db::Collection,
    index: &schema::IndexDescription,
) -> bool {
    if index.unique || index.fields.len() != 1 {
        return false;
    }

    let field_name = &index.fields[0].name;
    let Some(stripped_name) = field_name
        .strip_prefix('_')
        .and_then(|name| name.strip_suffix("ID"))
    else {
        return false;
    };

    let expected_name = format!("{}__{}ID_ASC", collection.name(), stripped_name);
    if index.name != expected_name {
        return false;
    }

    collection.schema().fields.iter().any(|field| {
        field.name == *field_name
            && field.relation_name.is_some()
            && field.is_primary
            && matches!(
                field.kind,
                schema::FieldKind::Scalar(schema::ScalarKind::DocID)
            )
    })
}

/// Deletes the index `index_name` from the collection `opts.name` names.
///
/// # Safety
///
/// `index_name` must be null or a valid null-terminated string; every
/// non-null string in `opts` must be one too.
#[no_mangle]
pub unsafe extern "C" fn defra_index_delete(
    node_ptr: usize,
    index_name: *const c_char,
    opts: CollectionOptions,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::IndexDelete));
        let node_ptr = try_ffi!(request.require_node("index delete"));
        let collection_name_str = CollectionArgs::read(&opts).name.unwrap_or_default();
        let index_name_str = go_string(index_name);
        let database = try_ffi!(request.database());

        ffi_async!(node_ptr, {
            // Get the collection
            let collection = database
                .get_collection(&collection_name_str)
                .map_err(|e| format!("failed to get collection: {}", e))?
                .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;
            let collection_id = collection.collection_id().to_string();
            let index_id = collection
                .get_index(&index_name_str)
                .map(|index| index.id)
                .ok_or_else(|| {
                    format!(
                        "index with name doesn't exists. Name: {}",
                        index_name_str
                    )
                })?;

            // Create a transaction
            let txn = database
                .new_txn(false)
                .await
                .map_err(|e| format!("failed to create transaction: {}", e))?;

            // Do all datastore operations in a scope to delete references before commit
            {
                let datastore = txn
                    .datastore()
                    .map_err(|e| format!("failed to get datastore: {}", e))?;

                // Create the index manager
                let mut index_manager = db::index::IndexManager::from_collection(
                    collection.schema().resolved_root_id(),
                    collection.schema(),
                )
                .map_err(|e| format!("failed to create index manager: {}", e))?;

                // Delete the index
                let dropped = index_manager
                    .delete_index(&datastore, &index_name_str)
                    .await
                    .map_err(|e| format!("failed to delete index: {}", e))?;

                if !dropped {
                    return Err(format!(
                        "index with name doesn't exists. Name: {}",
                        index_name_str
                    ));
                }

                // Update the collection schema to remove the index
                let mut updated_schema = collection.schema().clone();
                updated_schema
                    .indexes
                    .retain(|idx| idx.name != index_name_str);

                // Save the updated schema at /collection/id/{version_id}
                let collection_key =
                    storage::keys::systemstore::CollectionKey::new(&updated_schema.version_id);
                let schema_data = serde_json::to_vec(&updated_schema)
                    .map_err(|e| format!("failed to serialize schema: {}", e))?;

                let systemstore = txn
                    .systemstore()
                    .map_err(|e| format!("failed to get systemstore: {}", e))?;

                systemstore
                    .set(&collection_key.bytes(), &schema_data)
                    .await
                    .map_err(|e| format!("failed to save schema: {}", e))?;

                // Update the name → version_id mapping at /collection/name/{name}
                let name_key = storage::keys::systemstore::CollectionNameKey::new(&collection_name_str);
                systemstore
                    .set(&name_key.bytes(), updated_schema.version_id.as_bytes())
                    .await
                    .map_err(|e| format!("failed to save name mapping: {}", e))?;
            }

            // Commit the transaction (datastore reference is now dropped)
            txn.commit()
                .await
                .map_err(|e| format!("failed to commit: {}", e))?;

            database
                .clear_action(
                    &collection_id,
                    defra_core::Action::BACKFILL_INDEX,
                    &index_id.to_string(),
                )
                .await
                .map_err(|e| format!("failed to clear index backfill state: {}", e))?;

            // Reload the collection cache
            database
                .reload_cache()
                .await
                .map_err(|e| format!("failed to reload cache: {}", e))?;

            Ok("{}".to_string())
        })
    }
}

/// Answers with the indexes of the collection `opts.name` names as a JSON
/// array, or, when it names none, with every collection's indexes as a JSON
/// object keyed by collection name.
///
/// # Safety
///
/// Every non-null string in `opts` must be a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_index_list(
    node_ptr: usize,
    opts: CollectionOptions,
    identity_ptr: usize,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::IndexList));
        let node_ptr = try_ffi!(request.require_node("index list"));
        let database = try_ffi!(request.database());
        let context = request.context();

        match CollectionArgs::read(&opts).name {
            Some(collection_name_str) => ffi_async!(node_ptr, {
            // Get the collection
            let collection = database
                .get_collection(&collection_name_str)
                .map_err(|e| format!("failed to get collection: {}", e))?
                .ok_or_else(|| format!("collection '{}' not found", collection_name_str))?;

            let actions = database
                .list_index_actions_as(&context, collection.collection_id())
                .await
                .map_err(|e| format!("failed to list index actions: {}", e))?;
            let indexes = index_results(&collection, &actions);

            // Return JSON array
            let json = serde_json::to_string(&indexes)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
            }),
            None => ffi_async!(node_ptr, {
            // Get all collection names
            let names = database
                .list_collections()
                .map_err(|e| format!("failed to list collections: {}", e))?;

            // Build a map of collection name -> indexes
            let mut all_indexes: rapidhash::RapidHashMap<String, Vec<IndexResult>> =
                rapidhash::RapidHashMap::default();

            for name in names {
                match database.get_collection(&name) {
                    Ok(Some(collection)) => {
                        let actions = database
                            .list_index_actions_as(&context, collection.collection_id())
                            .await
                            .map_err(|e| format!("failed to list index actions: {}", e))?;
                        let indexes = index_results(&collection, &actions);
                        if !indexes.is_empty() {
                            all_indexes.insert(name, indexes);
                        }
                    }
                    Ok(None) => continue,
                    Err(e) => {
                        return Err(format!("failed to get collection '{}': {}", name, e));
                    }
                }
            }

            // Return JSON object
            let json = serde_json::to_string(&all_indexes)
                .map_err(|e| format!("failed to serialize result: {}", e))?;

            Ok(json)
            }),
        }
    }
}
