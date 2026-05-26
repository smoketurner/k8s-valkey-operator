//! Shared controller helpers.
//!
//! Utilities used by both ValkeyCluster and ValkeyUpgrade controllers.

use kube::{Api, Resource, ResourceExt, api::PatchParams};
use serde::de::DeserializeOwned;

use crate::controller::context::FIELD_MANAGER;
use crate::controller::error::Error;

/// Add a finalizer to a resource using optimistic concurrency (resourceVersion check).
///
/// Uses JSON Merge Patch with resourceVersion for optimistic concurrency. If the resource
/// was modified between our GET and PATCH, Kubernetes returns 409 Conflict and the
/// controller retries on the next reconciliation.
pub async fn add_finalizer<T>(api: &Api<T>, name: &str, finalizer: &str) -> Result<(), Error>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <T as Resource>::DynamicType: Default,
{
    let resource = api.get(name).await?;
    let mut finalizers = resource.finalizers().to_vec();

    if finalizers.contains(&finalizer.to_string()) {
        return Ok(());
    }

    finalizers.push(finalizer.to_string());
    let resource_version = resource
        .resource_version()
        .ok_or_else(|| Error::MissingField("metadata.resourceVersion".to_string()))?;

    let patch = serde_json::json!({
        "metadata": {
            "resourceVersion": resource_version,
            "finalizers": finalizers
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

/// Remove a specific finalizer from a resource using optimistic concurrency.
///
/// Uses JSON Merge Patch with resourceVersion for optimistic concurrency. If the resource
/// was modified between our GET and PATCH, Kubernetes returns 409 Conflict and the
/// controller retries on the next reconciliation.
pub async fn remove_finalizer<T>(api: &Api<T>, name: &str, finalizer: &str) -> Result<(), Error>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug,
    <T as Resource>::DynamicType: Default,
{
    let resource = match api.get(name).await {
        Ok(r) => r,
        Err(kube::Error::Api(e)) if e.code == 404 => {
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    let mut finalizers = resource.finalizers().to_vec();

    let Some(pos) = finalizers.iter().position(|f| f == finalizer) else {
        return Ok(());
    };
    finalizers.remove(pos);

    let resource_version = resource
        .resource_version()
        .ok_or_else(|| Error::MissingField("metadata.resourceVersion".to_string()))?;

    let patch = serde_json::json!({
        "metadata": {
            "resourceVersion": resource_version,
            "finalizers": finalizers
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
