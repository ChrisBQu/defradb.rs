use std::sync::Arc;

use crate::policy_yaml;
use crate::state::PolicyStore;

/// Validate that a collection's policy references a valid, well-formed policy.
pub(crate) fn validate_collection_policy(
    policy: &schema::PolicyDescription,
    store: &Arc<PolicyStore>,
) -> Result<(), String> {
    let policy_yaml = store
        .get_policy(&policy.id)
        .ok_or("policyID specified does not exist with acp")?;

    let parsed = policy_yaml::parse_policy_yaml(&policy_yaml)
        .map_err(|e| format!("failed to parse policy: {}", e))?;

    let resource = parsed
        .find_resource(&policy.resource_name)
        .ok_or("resource does not exist on the specified policy")?;

    for required in &["read", "update", "delete"] {
        if !resource.has_permission(required) {
            return Err("resource is missing required permission on policy.".to_string());
        }
    }

    Ok(())
}
