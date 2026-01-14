//! my-operator library crate
//!
//! This module exports the controller, CRD definitions, and resource generators.

pub mod controller;
pub mod crd;
pub mod health;
pub mod resources;
pub mod webhooks;

pub use health::HealthState;
pub use webhooks::{
    WEBHOOK_CERT_PATH, WEBHOOK_KEY_PATH, WEBHOOK_PORT, WebhookError, run_webhook_server,
};

use std::sync::Arc;

use futures::{Stream, StreamExt};
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::{Controller, WatchStreamExt, metadata_watcher, predicates, reflector, watcher};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use tracing::{debug, error, info};

use controller::{context::Context, reconciler::reconcile};
use crd::MyResource;

/// Create namespaced or cluster-wide API based on scope
pub fn scoped_api<T>(client: Client, namespace: Option<&str>) -> Api<T>
where
    T: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <T as Resource>::DynamicType: Default,
    T: Clone + DeserializeOwned + std::fmt::Debug,
{
    match namespace {
        Some(ns) => Api::namespaced(client, ns),
        None => Api::all(client),
    }
}

/// Create the default watcher configuration for all controllers.
///
/// This ensures consistent behavior across all controllers:
/// - `any_semantic()`: More reliable resource discovery in test environments
fn default_watcher_config() -> WatcherConfig {
    WatcherConfig::default().any_semantic()
}

/// Create a filtered stream for a resource type with standard optimizations.
///
/// This creates a reflector-backed stream that:
/// - Maintains an in-memory cache via reflector
/// - Uses automatic retry with exponential backoff on errors
/// - Converts watch events to objects (Added/Modified only)
/// - Filters out status-only updates via generation predicate
///
/// Returns the reflector store (for cache lookups) and the filtered stream.
fn create_filtered_stream<K>(
    api: Api<K>,
    watcher_config: WatcherConfig,
) -> (
    reflector::Store<K>,
    impl Stream<Item = Result<K, watcher::Error>>,
)
where
    K: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
{
    let (reader, writer) = reflector::store();
    let stream = reflector(writer, watcher(api, watcher_config))
        .default_backoff()
        .applied_objects()
        .predicate_filter(predicates::generation);
    (reader, stream)
}

/// Run the operator controller (cluster-wide).
///
/// This is the main controller loop that watches MyResource resources
/// and reconciles them. It can be called from main.rs or spawned as a
/// background task during integration tests.
///
/// If health_state is provided, metrics will be recorded for reconciliations.
pub async fn run_controller(client: Client, health_state: Option<Arc<HealthState>>) {
    run_controller_scoped(client, health_state, None).await
}

/// Run the operator controller with optional namespace scoping.
///
/// When `namespace` is `Some(ns)`, only watches resources in that namespace.
/// When `namespace` is `None`, watches resources cluster-wide.
///
/// Use the scoped version for integration tests to enable parallel test execution.
pub async fn run_controller_scoped(
    client: Client,
    health_state: Option<Arc<HealthState>>,
    namespace: Option<&str>,
) {
    let scope_msg = namespace.unwrap_or("cluster-wide");
    info!(
        "Starting controller for MyResource resources (scope: {})",
        scope_msg
    );

    // Mark as ready once we start the controller
    if let Some(ref state) = health_state {
        state.set_ready(true).await;
    }

    let ctx = Arc::new(Context::new(client.clone(), health_state));

    // Set up APIs for the controller (namespaced or cluster-wide)
    let myresources: Api<MyResource> = scoped_api(client.clone(), namespace);
    let deployments: Api<Deployment> = scoped_api(client.clone(), namespace);
    let services: Api<Service> = scoped_api(client.clone(), namespace);
    let configmaps: Api<ConfigMap> = scoped_api(client.clone(), namespace);

    // Use consistent watcher configuration across all controllers
    let watcher_config = default_watcher_config();

    // Create filtered stream with standard optimizations (reflector, backoff, generation predicate)
    let (reader, resource_stream) = create_filtered_stream(myresources, watcher_config.clone());

    // Create and run the controller using for_stream with the pre-filtered stream
    // Memory optimization: Use metadata_watcher for owned resources where we only need to know
    // they exist/changed (Services, ConfigMaps). Keep full watcher for Deployment
    // since we read .status.readyReplicas. metadata_watcher returns PartialObjectMeta which only
    // contains TypeMeta + ObjectMeta, reducing memory and IO.
    Controller::for_stream(resource_stream, reader)
        .owns(deployments, watcher_config.clone())
        .owns_stream(metadata_watcher(services, watcher_config.clone()).touched_objects())
        .owns_stream(metadata_watcher(configmaps, watcher_config).touched_objects())
        .run(reconcile, controller::reconciler::error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    debug!("Reconciled: {}", obj.name);
                }
                Err(e) => {
                    // ObjectNotFound/NotFound errors are expected after deletion when
                    // related watch events trigger reconciliation for a deleted object.
                    // Log these at debug level instead of error.
                    let is_not_found = match &e {
                        kube::runtime::controller::Error::ObjectNotFound(_) => true,
                        kube::runtime::controller::Error::ReconcilerFailed(err, _) => {
                            err.is_not_found()
                        }
                        _ => false,
                    };
                    if is_not_found {
                        debug!("Object no longer exists (likely deleted): {:?}", e);
                    } else {
                        error!("Reconciliation error: {:?}", e);
                    }
                }
            }
        })
        .await;

    // This should never complete in normal operation
    error!("Controller stream ended unexpectedly");
}
