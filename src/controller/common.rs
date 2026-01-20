//! Shared controller helpers.
//!
//! Utilities used by both ValkeyCluster and ValkeyUpgrade controllers.

use kube::{Api, Resource, ResourceExt, api::PatchParams};
use serde::de::DeserializeOwned;

use crate::controller::error::Error;

/// Add a finalizer to a resource.
pub async fn add_finalizer<T>(api: &Api<T>, name: &str, finalizer: &str) -> Result<(), Error>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <T as Resource>::DynamicType: Default,
{
    // Get current resource to check existing finalizers
    let resource = api.get(name).await?;
    let mut finalizers = resource.finalizers().to_vec();

    // Only add if not already present
    if !finalizers.contains(&finalizer.to_string()) {
        finalizers.push(finalizer.to_string());

        let patch = serde_json::json!({
            "metadata": {
                "finalizers": finalizers
            }
        });
        api.patch(
            name,
            &PatchParams::default(),
            &kube::api::Patch::Merge(&patch),
        )
        .await?;
    }
    Ok(())
}

/// Remove a specific finalizer from a resource.
pub async fn remove_finalizer<T>(api: &Api<T>, name: &str, finalizer: &str) -> Result<(), Error>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <T as Resource>::DynamicType: Default,
{
    // Get current resource to check existing finalizers
    let resource = match api.get(name).await {
        Ok(r) => r,
        Err(kube::Error::Api(e)) if e.code == 404 => {
            // Resource already deleted, nothing to do
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let mut finalizers = resource.finalizers().to_vec();

    // Only patch if the finalizer exists
    if let Some(pos) = finalizers.iter().position(|f| f == finalizer) {
        finalizers.remove(pos);

        let patch = serde_json::json!({
            "metadata": {
                "finalizers": finalizers
            }
        });
        api.patch(
            name,
            &PatchParams::default(),
            &kube::api::Patch::Merge(&patch),
        )
        .await?;
    }
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
