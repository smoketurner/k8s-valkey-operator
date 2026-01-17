//! Watch-based condition waiting utilities.
//!
//! Provides functions to wait for resources to reach specific states
//! using Kubernetes watch API for efficiency.

use futures::StreamExt;
use kube::Resource;
use kube::api::Api;
use kube::runtime::watcher::{self, Event};
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::timeout;

use valkey_operator::crd::{ClusterPhase, ValkeyCluster};

/// Error type for wait operations.
#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("Timeout waiting for condition after {0:?}")]
    Timeout(Duration),

    #[error("Resource was deleted while waiting")]
    ResourceDeleted,

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("Watcher error: {0}")]
    WatcherError(#[from] watcher::Error),

    #[error("Watch stream ended unexpectedly")]
    StreamEnded,
}

/// Wait for a resource to satisfy a condition.
///
/// # Arguments
/// * `api` - API client for the resource type
/// * `name` - Name of the resource to watch
/// * `condition` - Closure that returns true when the condition is met
/// * `timeout_duration` - Maximum time to wait
///
/// # Returns
/// The resource when the condition is satisfied, or an error on timeout.
pub async fn wait_for_condition<K, F>(
    api: &Api<K>,
    name: &str,
    condition: F,
    timeout_duration: Duration,
) -> Result<K, WaitError>
where
    K: Resource + Clone + Debug + Send + 'static,
    K: serde::de::DeserializeOwned,
    <K as Resource>::DynamicType: Default,
    F: Fn(&K) -> bool,
{
    let watch_future = async {
        let field_selector = format!("metadata.name={name}");
        let mut stream = watcher::watcher(
            api.clone(),
            watcher::Config::default().fields(&field_selector),
        )
        .boxed();

        // First, check the current state
        if let Ok(resource) = api.get(name).await
            && condition(&resource)
        {
            return Ok(resource);
        }

        // Watch for changes
        while let Some(event) = stream.next().await {
            match event {
                Ok(Event::Apply(resource)) | Ok(Event::InitApply(resource)) => {
                    if condition(&resource) {
                        return Ok(resource);
                    }
                }
                Ok(Event::Delete(_)) => {
                    return Err(WaitError::ResourceDeleted);
                }
                Ok(Event::Init) | Ok(Event::InitDone) => {
                    // Continue watching
                }
                Err(e) => {
                    return Err(WaitError::WatcherError(e));
                }
            }
        }

        Err(WaitError::StreamEnded)
    };

    timeout(timeout_duration, watch_future)
        .await
        .map_err(|_| WaitError::Timeout(timeout_duration))?
}

/// Wait for a resource to exist.
pub async fn wait_for_resource<K>(
    api: &Api<K>,
    name: &str,
    timeout_duration: Duration,
) -> Result<K, WaitError>
where
    K: Resource + Clone + Debug + Send + 'static,
    K: serde::de::DeserializeOwned,
    <K as Resource>::DynamicType: Default,
{
    wait_for_condition(api, name, |_| true, timeout_duration).await
}

/// Wait for a resource to be deleted.
pub async fn wait_for_deletion<K>(
    api: &Api<K>,
    name: &str,
    timeout_duration: Duration,
) -> Result<(), WaitError>
where
    K: Resource + Clone + Debug + Send + 'static,
    K: serde::de::DeserializeOwned,
    <K as Resource>::DynamicType: Default,
{
    let wait_future = async {
        // First check if already deleted
        match api.get(name).await {
            Err(kube::Error::Api(e)) if e.code == 404 => {
                return Ok(());
            }
            Err(e) => return Err(WaitError::KubeError(e)),
            Ok(_) => {}
        }

        // Watch for deletion
        let field_selector = format!("metadata.name={name}");
        let mut stream = watcher::watcher(
            api.clone(),
            watcher::Config::default().fields(&field_selector),
        )
        .boxed();

        while let Some(event) = stream.next().await {
            match event {
                Ok(Event::Delete(_)) => {
                    return Ok(());
                }
                Ok(_) => continue,
                Err(e) => {
                    // Watcher errors don't provide 404 details like kube::Error::Api.
                    // The initial api.get() check above handles the already-deleted case.
                    return Err(WaitError::WatcherError(e));
                }
            }
        }

        Err(WaitError::StreamEnded)
    };

    timeout(timeout_duration, wait_future)
        .await
        .map_err(|_| WaitError::Timeout(timeout_duration))?
}

/// Convenience function to wait with default timeout (30 seconds).
pub async fn wait_for<K, F>(api: &Api<K>, name: &str, condition: F) -> Result<K, WaitError>
where
    K: Resource + Clone + Debug + Send + 'static,
    K: serde::de::DeserializeOwned,
    <K as Resource>::DynamicType: Default,
    F: Fn(&K) -> bool,
{
    wait_for_condition(api, name, condition, Duration::from_secs(30)).await
}

// ============================================================
// ValkeyCluster-specific wait helpers
// ============================================================

/// Check if a ValkeyCluster is in a specific phase.
pub fn is_phase(resource: &ValkeyCluster, phase: ClusterPhase) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.phase == phase)
        .unwrap_or(false)
}

/// Check if a ValkeyCluster has the expected ready replicas.
pub fn has_ready_replicas(resource: &ValkeyCluster, count: i32) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.ready_replicas >= count)
        .unwrap_or(false)
}

/// Check if a ValkeyCluster is operational (Running phase with all replicas ready).
///
/// This verifies:
/// 1. The operator has observed the current generation (processed the spec)
/// 2. The phase is Running
/// 3. The ready replicas match the expected count
///
/// For scale-down operations, this ensures we wait until the scaling is complete
/// rather than returning immediately when ready_replicas > expected.
pub fn is_operational(resource: &ValkeyCluster) -> bool {
    let total_pods =
        valkey_operator::crd::total_pods(resource.spec.masters, resource.spec.replicas_per_master);

    // First check that the operator has observed the current generation
    let gen_observed = match (resource.metadata.generation, &resource.status) {
        (Some(current_gen), Some(status)) => status.observed_generation == Some(current_gen),
        _ => false,
    };

    if !gen_observed {
        return false;
    }

    // Then check operational status
    // Use == instead of >= to handle scale-down correctly
    resource
        .status
        .as_ref()
        .map(|s| s.phase == ClusterPhase::Running && s.ready_replicas == total_pods)
        .unwrap_or(false)
}

/// Check if a ValkeyCluster's generation has been observed.
pub fn generation_observed(resource: &ValkeyCluster) -> bool {
    match (resource.metadata.generation, &resource.status) {
        (Some(current_gen), Some(status)) => status.observed_generation == Some(current_gen),
        _ => false,
    }
}

/// Wait for a ValkeyCluster to reach a specific phase.
pub async fn wait_for_phase(
    api: &Api<ValkeyCluster>,
    name: &str,
    phase: ClusterPhase,
    timeout_duration: Duration,
) -> Result<ValkeyCluster, WaitError> {
    wait_for_condition(api, name, |r| is_phase(r, phase), timeout_duration).await
}

/// Wait for a ValkeyCluster to be operational (Running with all replicas ready).
pub async fn wait_for_operational(
    api: &Api<ValkeyCluster>,
    name: &str,
    timeout_duration: Duration,
) -> Result<ValkeyCluster, WaitError> {
    wait_for_condition(api, name, is_operational, timeout_duration).await
}

/// Wait for a ValkeyCluster to have its generation observed.
pub async fn wait_for_generation_observed(
    api: &Api<ValkeyCluster>,
    name: &str,
    timeout_duration: Duration,
) -> Result<ValkeyCluster, WaitError> {
    wait_for_condition(api, name, generation_observed, timeout_duration).await
}
