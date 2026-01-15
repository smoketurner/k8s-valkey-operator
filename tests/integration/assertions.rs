//! Resource assertion helpers for integration tests.
//!
//! Provides convenient assertion functions for verifying resource state.

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::Client;
use kube::api::Api;

use valkey_operator::crd::{ValkeyCluster, ClusterPhase};

/// Assert that a ValkeyCluster exists and has the expected phase.
pub async fn assert_valkeycluster_phase(
    client: Client,
    namespace: &str,
    name: &str,
    expected_phase: ClusterPhase,
) {
    let api: Api<ValkeyCluster> = Api::namespaced(client, namespace);
    let resource = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get ValkeyCluster {}/{}: {}", namespace, name, e));

    let actual_phase = resource
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or_default();

    assert_eq!(
        actual_phase, expected_phase,
        "ValkeyCluster {}/{} phase mismatch: expected {:?}, got {:?}",
        namespace, name, expected_phase, actual_phase
    );
}

/// Assert that a ValkeyCluster is in the Running phase.
pub async fn assert_valkeycluster_ready(client: Client, namespace: &str, name: &str) {
    assert_valkeycluster_phase(client, namespace, name, ClusterPhase::Running).await;
}

/// Assert that a StatefulSet exists with expected replica count.
pub async fn assert_statefulset_replicas(
    client: Client,
    namespace: &str,
    name: &str,
    expected_replicas: i32,
) {
    let api: Api<StatefulSet> = Api::namespaced(client, namespace);
    let statefulset = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get StatefulSet {}/{}: {}", namespace, name, e));

    let actual_replicas = statefulset
        .spec
        .as_ref()
        .and_then(|s| s.replicas)
        .unwrap_or(0);

    assert_eq!(
        actual_replicas, expected_replicas,
        "StatefulSet {}/{} replicas mismatch: expected {}, got {}",
        namespace, name, expected_replicas, actual_replicas
    );
}

/// Assert that a StatefulSet has all replicas ready.
pub async fn assert_statefulset_ready(client: Client, namespace: &str, name: &str) {
    let api: Api<StatefulSet> = Api::namespaced(client, namespace);
    let statefulset = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get StatefulSet {}/{}: {}", namespace, name, e));

    let desired = statefulset
        .spec
        .as_ref()
        .and_then(|s| s.replicas)
        .unwrap_or(1);

    let ready = statefulset
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);

    assert_eq!(
        ready, desired,
        "StatefulSet {}/{} not ready: {}/{} replicas ready",
        namespace, name, ready, desired
    );
}

/// Assert that a Service exists.
pub async fn assert_service_exists(client: Client, namespace: &str, name: &str) {
    let api: Api<Service> = Api::namespaced(client, namespace);
    api.get(name)
        .await
        .unwrap_or_else(|e| panic!("Service {}/{} should exist: {}", namespace, name, e));
}

/// Assert that a headless Service exists (ClusterIP: None).
pub async fn assert_headless_service_exists(client: Client, namespace: &str, name: &str) {
    let api: Api<Service> = Api::namespaced(client, namespace);
    let service = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Service {}/{} should exist: {}", namespace, name, e));

    let cluster_ip = service
        .spec
        .as_ref()
        .and_then(|s| s.cluster_ip.as_ref())
        .map(|s| s.as_str());

    assert_eq!(
        cluster_ip,
        Some("None"),
        "Service {}/{} should be headless (ClusterIP: None)",
        namespace, name
    );
}

/// Assert that a Secret exists with expected keys.
pub async fn assert_secret_has_keys(client: Client, namespace: &str, name: &str, keys: &[&str]) {
    let api: Api<Secret> = Api::namespaced(client, namespace);
    let secret = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get Secret {}/{}: {}", namespace, name, e));

    let data = secret.data.as_ref();
    for key in keys {
        let has_key = data.is_some_and(|d| d.contains_key(*key));
        assert!(
            has_key,
            "Secret {}/{} missing expected key: {}",
            namespace, name, key
        );
    }
}

/// Assert that a PodDisruptionBudget exists.
pub async fn assert_pdb_exists(client: Client, namespace: &str, name: &str) {
    let api: Api<PodDisruptionBudget> = Api::namespaced(client, namespace);
    api.get(name)
        .await
        .unwrap_or_else(|e| panic!("PDB {}/{} should exist: {}", namespace, name, e));
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

/// Assert that a ValkeyCluster has the expected number of ready nodes.
pub async fn assert_valkeycluster_ready_nodes(
    client: Client,
    namespace: &str,
    name: &str,
    expected_ready: i32,
) {
    let api: Api<ValkeyCluster> = Api::namespaced(client, namespace);
    let resource = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get ValkeyCluster {}/{}: {}", namespace, name, e));

    let actual_ready = resource
        .status
        .as_ref()
        .map(|s| s.ready_replicas)
        .unwrap_or(0);

    assert_eq!(
        actual_ready, expected_ready,
        "ValkeyCluster {}/{} ready nodes mismatch: expected {}, got {}",
        namespace, name, expected_ready, actual_ready
    );
}

/// Assert that a ValkeyCluster has all slots assigned.
pub async fn assert_valkeycluster_slots_assigned(client: Client, namespace: &str, name: &str) {
    let api: Api<ValkeyCluster> = Api::namespaced(client, namespace);
    let resource = api
        .get(name)
        .await
        .unwrap_or_else(|e| panic!("Failed to get ValkeyCluster {}/{}: {}", namespace, name, e));

    let assigned_slots = resource
        .status
        .as_ref()
        .map(|s| s.assigned_slots.as_str())
        .unwrap_or("0/16384");

    assert_eq!(
        assigned_slots, "16384/16384",
        "ValkeyCluster {}/{} should have all 16384 slots assigned, got {}",
        namespace, name, assigned_slots
    );
}
