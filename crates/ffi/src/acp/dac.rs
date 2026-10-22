use std::ffi::c_char;

use acp::nac::NodePermission;
use acp::StorePolicyOptions;

use crate::helpers::get_rt;
use crate::policy_yaml::{
    check_duplicate_yaml_keys, parse_policy_yaml, validate_policy_expressions,
};
use crate::request::Request;
use crate::state::NODES;
use crate::target::go_string;
use crate::types::DefraResult;
use crate::{ffi_entry, try_ffi, ERR_INVALID_NODE_HANDLE};

async fn publish_latest_document_update(
    database: &crate::state::FfiDatabase,
    event_bus: &dyn events::Bus,
    collection_id: &str,
    doc_id: &str,
) -> Result<(), String> {
    let result = db::block::reader::read_latest_composite_block(database, doc_id).await?;
    event_bus.publish(events::Message::update(events::Update::new(
        doc_id.to_string(),
        result.cid,
        collection_id.to_string(),
        result.block,
        false,
        false,
    )));
    Ok(())
}

/// Resolve managing relations for a given relation within a policy resource.
///
/// Loads the policy YAML, finds the resource, validates the relation exists,
/// and returns the list of managing relations.
pub(crate) fn resolve_managing_relations(
    policy_store: &crate::state::PolicyStore,
    policy_id: &str,
    resource_name: &str,
    relation: &str,
) -> Result<Vec<String>, String> {
    let mut managing_relations: Vec<String> = Vec::new();
    if let Some(policy_yaml) = policy_store.get_policy(policy_id) {
        if let Ok(parsed) = crate::policy_yaml::parse_policy_yaml(&policy_yaml) {
            if let Some(resource) = parsed.find_resource(resource_name) {
                if !resource.has_relation(relation) {
                    return Err(format!(
                        "relation '{}' not found in policy resource '{}'",
                        relation, resource_name
                    ));
                }
                managing_relations = resource
                    .get_managers_for_relation(relation)
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect();
            }
        }
    }
    Ok(managing_relations)
}

/// Add a DAC policy.
///
/// Accepts a policy definition in YAML or JSON format.
/// Returns a JSON object with the policy ID:
/// ```json
/// { "PolicyID": "sha256_hash_of_policy" }
/// ```
///
/// # Safety
///
/// `policy` must be a valid null-terminated UTF-8 string containing
/// the policy definition in YAML or JSON format.
/// Adds the document access-control policy `policy` (YAML), created by the
/// identity `identity_ptr` names, and answers with `{"PolicyID": ...}`.
///
/// # Safety
///
/// `policy` must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_acp_dac_policy_add(
    node_ptr: usize,
    identity_ptr: usize,
    policy: *const c_char,
) -> DefraResult {
    ffi_entry! {
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::DacPolicyAdd));
        let node_ptr = try_ffi!(request.require_node("policy add"));

        let policy_str = go_string(policy);

        let identity_str = request.identity.clone().unwrap_or_default();

        if identity_str.is_empty() {
            return DefraResult::error("policy creator can not be empty");
        }

        // Check for empty/null policy data
        if policy_str.is_empty() {
            return DefraResult::error("policy data can not be empty");
        }

        // Step 1: Check for duplicate YAML map keys (Go rejects these via YAMLToJSONStrict)
        if let Err(e) = check_duplicate_yaml_keys(&policy_str) {
            return DefraResult::error(e);
        }

        // Step 2: Parse the policy YAML
        let parsed = match parse_policy_yaml(&policy_str) {
            Ok(p) => p,
            Err(e) => return DefraResult::error(e),
        };

        // Step 3: Validate name is present (Go's BasicRequirement)
        if parsed.name.is_empty() {
            return DefraResult::error("name required");
        }

        // Step 4: Validate permission expressions
        if let Err(e) = validate_policy_expressions(&parsed) {
            return DefraResult::error(e);
        }

        // Step 5: Store policy - route through Vera when configured, else local
        #[cfg(feature = "vera")]
        let sh_acp = match NODES.get(node_ptr, |state| state.vera_acp.clone()) {
            Some(opt) => opt,
            None => return DefraResult::error(ERR_INVALID_NODE_HANDLE),
        };

        #[cfg(feature = "vera")]
        if let Some(sh_acp) = sh_acp {
            // Vera mode: submit MsgCreatePolicy transaction, network work.
            let result = crate::helpers::block_on(try_ffi!(get_rt()), async {
                let policy_id = sh_acp
                    .add_policy(&identity_str, &policy_str)
                    .await
                    .map_err(|e| format!("Vera create policy failed: {}", e))?;
                Ok::<String, String>(policy_id)
            });
            return match result {
                Ok(policy_id) => {
                    // Cache policy on all live FFI nodes so multi-node Vera tests
                    // can validate schemas that reference a policy created by another node.
                    NODES.for_each(|state| {
                        state.policy_store.store_policy(&policy_id, &policy_str);
                    });
                    DefraResult::success(serde_json::json!({ "PolicyID": policy_id }).to_string())
                }
                Err(e) => DefraResult::error(e),
            };
        }

        {
            // Local mode: persist the parsed policy into the same Zanzibar store
            // used by document ACP so custom relations and permissions are enforced.
            let (policy_store, local_zanzibar_store) = match NODES.get(node_ptr, |state| {
                (state.policy_store.clone(), state.local_zanzibar_store.clone())
            }) {
                Some(tuple) => tuple,
                None => return DefraResult::error(ERR_INVALID_NODE_HANDLE),
            };

            let Some(local_zanzibar_store) = local_zanzibar_store else {
                return DefraResult::error("local ACP backend is not available");
            };

            let counter_store = std::sync::Arc::clone(&local_zanzibar_store);
            let counter = match crate::call::on_node(node_ptr, move || async move {
                counter_store
                    .next_policy_counter()
                    .await
                    .map_err(|e| e.to_string())
            }) {
                Ok(counter) => counter,
                Err(e) => {
                    return DefraResult::error(format!("failed to advance policy counter: {}", e))
                }
            };

            let policy = match acp::policy_yaml::build_policy(&parsed, counter) {
                Ok(policy) => policy,
                Err(e) => return DefraResult::error(format!("invalid policy: {}", e)),
            };
            let policy_id = policy.id.clone();
            policy_store.store_policy(&policy_id, &policy_str);

            let options = StorePolicyOptions::new()
                .with_validation()
                .with_dpi_enforcement();

            let stored_id = policy_id.clone();
            let result = crate::call::on_node(node_ptr, move || async move {
                local_zanzibar_store
                    .store_policy_with_options(&policy, &options)
                    .await
                    .map_err(|e| format!("failed to store policy: {}", e))?;
                Ok::<String, String>(
                    serde_json::json!({
                        "PolicyID": stored_id
                    })
                    .to_string(),
                )
            });

            match result {
                Ok(json) => DefraResult::success(json),
                Err(e) => {
                    policy_store.remove_policy(&policy_id);
                    DefraResult::error(e)
                }
            }
        }
    }
}

/// Add a DAC actor relationship (share document access with target).
///
/// The requestor must be the document owner. Relation can be:
/// - "reader" - read access
/// - "updater" - read + update access
/// - "deleter" - read + delete access
///
/// Returns JSON with success status:
/// ```json
/// { "added": true }  // or false if already exists
/// ```
///
/// # Safety
///
/// All string parameters must be valid null-terminated UTF-8 strings.
/// Adds the relation `relation` between `actor` and document `doc_id` of
/// collection `collection`, as the identity `identity_ptr` names, and
/// answers with the result as JSON.
///
/// # Safety
///
/// Every string must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_acp_dac_relationship_add(
    node_ptr: usize,
    identity_ptr: usize,
    collection: *const c_char,
    doc_id: *const c_char,
    relation: *const c_char,
    actor: *const c_char,
) -> DefraResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::DacRelationAdd));
        let node_ptr = try_ffi!(request.require_node("document relationship changes"));

        let requestor_str = request.identity.clone().unwrap_or_default();
        let target_str = go_string(actor);
        let collection_id_str = go_string(collection);
        let doc_id_str = go_string(doc_id);
        let relation_str = go_string(relation);

        // Go checks collection name first (via GetCollectionByName)
        if collection_id_str.is_empty() {
            return DefraResult::error("collection name can't be empty");
        }

        // Then validates remaining required arguments (matches Go bridge.go validation)
        if requestor_str.is_empty()
            || target_str.is_empty()
            || doc_id_str.is_empty()
            || relation_str.is_empty()
        {
            return DefraResult::error(
                "missing a required argument needed to add doc actor relationship.",
            );
        }

        // Validate node handle and get database, document_acp, event_bus, and policy_store
        let (database, document_acp, event_bus, policy_store) = match NODES.get(node_ptr, |state| {
            (
                state.database.clone(),
                state.document_acp.clone(),
                state.event_bus.clone(),
                state.policy_store.clone(),
            )
        }) {
            Some(tuple) => tuple,
            None => return DefraResult::error(ERR_INVALID_NODE_HANDLE),
        };

        // Resolve collection name -> policy resource_name and policy_id
        let (resource_name, policy_id, collection_id) =
            match database.get_collection(&collection_id_str) {
            Ok(Some(col)) => match col.schema().policy {
                Some(ref policy) => (
                    policy.resource_name.clone(),
                    policy.id.clone(),
                    col.collection_id().to_string(),
                ),
                None => {
                    return DefraResult::error("operation requires ACP, but collection has no policy");
                }
            },
            Ok(None) => {
                return DefraResult::error(format!("collection '{}' does not exist", collection_id_str));
            }
            Err(e) => {
                return DefraResult::error(format!(
                    "failed to get collection '{}': {}",
                    collection_id_str, e
                ));
            }
        };

        // Owner relation is immutable - cannot be added or removed
        if relation_str == "owner" {
            return DefraResult::error("OPERATION_FORBIDDEN: cannot add owner relation");
        }

        let managing_relations = match resolve_managing_relations(
            &policy_store,
            &policy_id,
            &resource_name,
            &relation_str,
        ) {
            Ok(r) => r,
            Err(e) => return DefraResult::error(e),
        };

        let result = crate::helpers::block_on(rt, async {
            let requestor = identity::Did::new(&requestor_str)
                .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
            let target = if target_str == "*" {
                identity::Did::wildcard()
            } else {
                identity::Did::new(&target_str)
                    .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?
            };

            let added = document_acp
                .add_actor_relationship(
                    &requestor,
                    &target,
                    &policy_id,
                    &resource_name,
                    &doc_id_str,
                    &relation_str,
                    &managing_relations,
                )
                .await
                .map_err(|e| e.to_string())?;

            if added && doc_id_str != collection_id {
                publish_latest_document_update(
                    &database,
                    event_bus.as_ref(),
                    &collection_id,
                    &doc_id_str,
                )
                .await?;
            }

            // Local ACP relationships are node-local (matches Go): a grant is not
            // propagated to peers. Cross-node access control is Vera's role.
            let json = serde_json::json!({ "added": added }).to_string();
            Ok::<String, String>(json)
        });

        match result {
            Ok(json) => DefraResult::success(json),
            Err(e) => DefraResult::error(e),
        }
    }
}

/// Delete a DAC actor relationship (revoke document access from target).
///
/// The requestor must be the document owner.
///
/// Returns JSON with success status:
/// ```json
/// { "deleted": true }  // or false if didn't exist
/// ```
///
/// # Safety
///
/// All string parameters must be valid null-terminated UTF-8 strings.
/// Deletes the relation `relation` between `actor` and document `doc_id` of
/// collection `collection`, as the identity `identity_ptr` names, and
/// answers with the result as JSON.
///
/// # Safety
///
/// Every string must be null or a valid null-terminated string.
#[no_mangle]
pub unsafe extern "C" fn defra_acp_dac_relationship_delete(
    node_ptr: usize,
    identity_ptr: usize,
    collection: *const c_char,
    doc_id: *const c_char,
    relation: *const c_char,
    actor: *const c_char,
) -> DefraResult {
    ffi_entry! {
        let rt = try_ffi!(get_rt());
        let request = try_ffi!(Request::begin(node_ptr, identity_ptr, NodePermission::DacRelationDelete));
        let node_ptr = try_ffi!(request.require_node("document relationship changes"));

        let requestor_str = request.identity.clone().unwrap_or_default();
        let target_str = go_string(actor);
        let collection_id_str = go_string(collection);
        let doc_id_str = go_string(doc_id);
        let relation_str = go_string(relation);

        // Go checks collection name first (via GetCollectionByName)
        if collection_id_str.is_empty() {
            return DefraResult::error("collection name can't be empty");
        }

        // Then validates remaining required arguments (matches Go bridge.go validation)
        if requestor_str.is_empty()
            || target_str.is_empty()
            || doc_id_str.is_empty()
            || relation_str.is_empty()
        {
            return DefraResult::error(
                "missing a required argument needed to delete doc actor relationship.",
            );
        }

        // Validate node handle and get database, document_acp, and policy_store
        let (database, document_acp, policy_store) = match NODES.get(node_ptr, |state| {
            (
                state.database.clone(),
                state.document_acp.clone(),
                state.policy_store.clone(),
            )
        }) {
            Some(tuple) => tuple,
            None => return DefraResult::error(ERR_INVALID_NODE_HANDLE),
        };

        // Resolve collection name -> policy resource_name and policy_id
        let (resource_name, policy_id) = match database.get_collection(&collection_id_str) {
            Ok(Some(col)) => match col.schema().policy {
                Some(ref policy) => (policy.resource_name.clone(), policy.id.clone()),
                None => {
                    return DefraResult::error("operation requires ACP, but collection has no policy");
                }
            },
            Ok(None) => {
                return DefraResult::error(format!("collection '{}' does not exist", collection_id_str));
            }
            Err(e) => {
                return DefraResult::error(format!(
                    "failed to get collection '{}': {}",
                    collection_id_str, e
                ));
            }
        };

        // Owner relation is immutable - cannot be added or removed
        if relation_str == "owner" {
            return DefraResult::error("OPERATION_FORBIDDEN: cannot delete owner relation");
        }

        let managing_relations = match resolve_managing_relations(
            &policy_store,
            &policy_id,
            &resource_name,
            &relation_str,
        ) {
            Ok(r) => r,
            Err(e) => return DefraResult::error(e),
        };

        let result = crate::helpers::block_on(rt, async {
            let requestor = identity::Did::new(&requestor_str)
                .map_err(|e| format!("invalid requestor DID '{}': {}", requestor_str, e))?;
            let target = if target_str == "*" {
                identity::Did::wildcard()
            } else {
                identity::Did::new(&target_str)
                    .map_err(|e| format!("invalid target DID '{}': {}", target_str, e))?
            };

            let deleted = document_acp
                .delete_actor_relationship(
                    &requestor,
                    &target,
                    &policy_id,
                    &resource_name,
                    &doc_id_str,
                    &relation_str,
                    &managing_relations,
                )
                .await
                .map_err(|e| e.to_string())?;

            // Local ACP relationships are node-local (matches Go): a revoke is not
            // propagated to peers. Cross-node access control is Vera's role.
            let json = serde_json::json!({ "deleted": deleted }).to_string();
            Ok::<String, String>(json)
        });

        match result {
            Ok(json) => DefraResult::success(json),
            Err(e) => DefraResult::error(e),
        }
    }
}
