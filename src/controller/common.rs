//! Shared controller helpers.
//!
//! Utilities used by both ValkeyCluster and ValkeyUpgrade controllers.

use kube::{Api, Resource, api::PatchParams};
use serde::de::DeserializeOwned;

use crate::controller::context::FIELD_MANAGER;
use crate::controller::error::Error;

/// Add a finalizer to a resource.
pub async fn add_finalizer<T>(api: &Api<T>, name: &str, finalizer: &str) -> Result<(), Error>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <T as Resource>::DynamicType: Default,
{
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [finalizer]
        }
    });
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER),
        &kube::api::Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Remove a finalizer from a resource.
pub async fn remove_finalizer<T>(api: &Api<T>, name: &str) -> Result<(), Error>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <T as Resource>::DynamicType: Default,
{
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": null
        }
    });
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER),
        &kube::api::Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Extract pod name from an address (e.g., "pod-0.service.ns:6379" -> "pod-0").
pub fn extract_pod_name(address: &str) -> String {
    address
        .split(':')
        .next()
        .and_then(|h| h.split('.').next())
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown")
        .to_string()
}
