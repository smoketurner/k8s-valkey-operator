//! Watch-based condition waiting utilities.
//!
//! Provides functions to wait for resources to reach specific states
//! using Kubernetes watch API for efficiency.

use futures::StreamExt;
use kube::Resource;
use kube::api::{Api, ListParams};
use kube::runtime::watcher::{self, Event};
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::timeout;

use my_operator::crd::{MyResource, Phase};

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
    let params = ListParams::default().fields(&format!("metadata.name={}", name));

    let watch_future = async {
        let mut stream = watcher::watcher(
            api.clone(),
            watcher::Config::default().fields(&format!("metadata.name={}", name)),
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
        let mut stream = watcher::watcher(
            api.clone(),
            watcher::Config::default().fields(&format!("metadata.name={}", name)),
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
// MyResource-specific wait helpers
// ============================================================

/// Check if a MyResource is in a specific phase.
pub fn is_phase(resource: &MyResource, phase: Phase) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.phase == phase)
        .unwrap_or(false)
}

/// Check if a MyResource has the expected ready replicas.
pub fn has_ready_replicas(resource: &MyResource, count: i32) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.ready_replicas >= count)
        .unwrap_or(false)
}

/// Check if a MyResource is operational (Running phase with all replicas ready).
pub fn is_operational(resource: &MyResource) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.phase == Phase::Running && s.ready_replicas >= resource.spec.replicas)
        .unwrap_or(false)
}

/// Check if a MyResource's generation has been observed.
pub fn generation_observed(resource: &MyResource) -> bool {
    match (resource.metadata.generation, &resource.status) {
        (Some(current_gen), Some(status)) => status.observed_generation == Some(current_gen),
        _ => false,
    }
}

/// Wait for a MyResource to reach a specific phase.
pub async fn wait_for_phase(
    api: &Api<MyResource>,
    name: &str,
    phase: Phase,
    timeout_duration: Duration,
) -> Result<MyResource, WaitError> {
    wait_for_condition(api, name, |r| is_phase(r, phase), timeout_duration).await
}

/// Wait for a MyResource to be operational (Running with all replicas ready).
pub async fn wait_for_operational(
    api: &Api<MyResource>,
    name: &str,
    timeout_duration: Duration,
) -> Result<MyResource, WaitError> {
    wait_for_condition(api, name, is_operational, timeout_duration).await
}

/// Wait for a MyResource to have its generation observed.
pub async fn wait_for_generation_observed(
    api: &Api<MyResource>,
    name: &str,
    timeout_duration: Duration,
) -> Result<MyResource, WaitError> {
    wait_for_condition(api, name, generation_observed, timeout_duration).await
}
