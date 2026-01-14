//! Resource assertion helpers for integration tests.
//!
//! Provides convenient assertion functions for verifying resource state.

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::Client;
use kube::api::Api;

use my_operator::crd::{MyResource, Phase};

/// Assert that a MyResource exists and has the expected phase.
pub async fn assert_myresource_phase(
    client: Client,
    namespace: &str,
    name: &str,
    expected_phase: Phase,
) {
    let api: Api<MyResource> = Api::namespaced(client, namespace);
    let resource = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get MyResource {}/{}: {}", namespace, name, e));

    let actual_phase = resource
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or_default();

    assert_eq!(
        actual_phase, expected_phase,
        "MyResource {}/{} phase mismatch: expected {:?}, got {:?}",
        namespace, name, expected_phase, actual_phase
    );
}

/// Assert that a MyResource is in the Ready phase.
pub async fn assert_myresource_ready(client: Client, namespace: &str, name: &str) {
    assert_myresource_phase(client, namespace, name, Phase::Running).await;
}

/// Assert that a Deployment exists with expected replica count.
pub async fn assert_deployment_replicas(
    client: Client,
    namespace: &str,
    name: &str,
    expected_replicas: i32,
) {
    let api: Api<Deployment> = Api::namespaced(client, namespace);
    let deployment = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get Deployment {}/{}: {}", namespace, name, e));

    let actual_replicas = deployment
        .spec
        .as_ref()
        .and_then(|s| s.replicas)
        .unwrap_or(0);

    assert_eq!(
        actual_replicas, expected_replicas,
        "Deployment {}/{} replicas mismatch: expected {}, got {}",
        namespace, name, expected_replicas, actual_replicas
    );
}

/// Assert that a Deployment has all replicas ready.
pub async fn assert_deployment_ready(client: Client, namespace: &str, name: &str) {
    let api: Api<Deployment> = Api::namespaced(client, namespace);
    let deployment = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get Deployment {}/{}: {}", namespace, name, e));

    let desired = deployment
        .spec
        .as_ref()
        .and_then(|s| s.replicas)
        .unwrap_or(1);

    let ready = deployment
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);

    assert_eq!(
        ready, desired,
        "Deployment {}/{} not ready: {}/{} replicas ready",
        namespace, name, ready, desired
    );
}

/// Assert that a ConfigMap exists with expected data key.
pub async fn assert_configmap_has_key(client: Client, namespace: &str, name: &str, key: &str) {
    let api: Api<ConfigMap> = Api::namespaced(client, namespace);
    let configmap = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get ConfigMap {}/{}: {}", namespace, name, e));

    let has_key = configmap.data.as_ref().is_some_and(|d| d.contains_key(key));

    assert!(
        has_key,
        "ConfigMap {}/{} missing expected key: {}",
        namespace, name, key
    );
}

/// Assert that a ConfigMap has a specific value for a key.
pub async fn assert_configmap_value(
    client: Client,
    namespace: &str,
    name: &str,
    key: &str,
    expected_value: &str,
) {
    let api: Api<ConfigMap> = Api::namespaced(client, namespace);
    let configmap = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get ConfigMap {}/{}: {}", namespace, name, e));

    let actual_value = configmap
        .data
        .as_ref()
        .and_then(|d| d.get(key))
        .map(|s| s.as_str());

    assert_eq!(
        actual_value,
        Some(expected_value),
        "ConfigMap {}/{} key {} value mismatch: expected {:?}, got {:?}",
        namespace,
        name,
        key,
        expected_value,
        actual_value
    );
}

/// Assert that a Service exists.
pub async fn assert_service_exists(client: Client, namespace: &str, name: &str) {
    let api: Api<Service> = Api::namespaced(client, namespace);
    api.get(name)
        .await
        .unwrap_or_else(|e| panic!("Service {}/{} should exist: {}", namespace, name, e));
}

/// Assert that a resource has the expected owner reference.
pub fn assert_has_owner_reference<K>(resource: &K, owner_name: &str, owner_kind: &str)
where
    K: kube::Resource,
{
    let owner_refs = resource
        .meta()
        .owner_references
        .as_ref()
        .expect("Resource should have owner references");

    let has_owner = owner_refs
        .iter()
        .any(|r| r.name == owner_name && r.kind == owner_kind);

    assert!(
        has_owner,
        "Resource should have owner reference to {} {}",
        owner_kind, owner_name
    );
}

/// Assert that a resource has a specific label.
pub fn assert_has_label<K>(resource: &K, key: &str, expected_value: &str)
where
    K: kube::Resource,
{
    let labels = resource
        .meta()
        .labels
        .as_ref()
        .expect("Resource should have labels");

    let actual_value = labels.get(key);

    assert_eq!(
        actual_value,
        Some(&expected_value.to_string()),
        "Resource label {} mismatch: expected {:?}, got {:?}",
        key,
        expected_value,
        actual_value
    );
}
