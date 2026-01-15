//! Functional tests for ValkeyCluster lifecycle operations.
//!
//! These tests verify the core functionality of the operator including:
//! - Resource creation and owned resource generation
//! - Spec updates and reconciliation
//! - Resource deletion and cleanup

use std::time::Duration;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{Api, DeleteParams, PostParams};

use valkey_operator::crd::{ClusterPhase, ValkeyCluster};

use crate::assertions::{
    assert_has_owner_reference, assert_headless_service_exists,
    assert_service_exists, assert_statefulset_replicas, assert_valkeycluster_phase,
};
use crate::namespace::TestNamespace;
use crate::wait::{wait_for_condition, wait_for_operational, wait_for_phase};
use crate::{init_test, wait};

/// Short timeout for quick operations.
pub const SHORT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for most operations.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Long timeout for cluster initialization.
pub const LONG_TIMEOUT: Duration = Duration::from_secs(180);

/// Helper to create a test ValkeyCluster with the spec structure.
fn test_resource(name: &str, masters: i32, replicas_per_master: i32) -> ValkeyCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "valkey-operator.smoketurner.com/v1alpha1",
        "kind": "ValkeyCluster",
        "metadata": {
            "name": name
        },
        "spec": {
            "masters": masters,
            "replicasPerMaster": replicas_per_master,
            "tls": {
                "issuerRef": {
                    "name": "selfsigned-issuer"
                }
            },
            "auth": {
                "secretRef": {
                    "name": "valkey-auth"
                }
            }
        }
    }))
    .expect("Failed to create test resource")
}

/// Test that creating a ValkeyCluster creates the expected StatefulSet.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_statefulset() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-statefulset").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters and 0 replicas per master = 3 total pods
    let resource = test_resource("test-sts", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for resource to start creating
    wait_for_phase(
        &api,
        "test-sts",
        ClusterPhase::Creating,
        SHORT_TIMEOUT,
    )
    .await
    .expect("Resource should reach Creating phase");

    // Wait for statefulset to exist
    wait::wait_for_resource(&sts_api, "test-sts", SHORT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Verify statefulset properties (3 masters + 0 replicas = 3 pods)
    assert_statefulset_replicas(client.clone(), &ns_name, "test-sts", 3).await;

    // Verify owner reference
    let statefulset = sts_api.get("test-sts").await.unwrap();
    assert_has_owner_reference(&statefulset, "test-sts", "ValkeyCluster");
}

/// Test that creating a ValkeyCluster creates the expected Services.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_services() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-services").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-svc", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for headless service to exist
    wait::wait_for_resource(&svc_api, "test-svc-headless", SHORT_TIMEOUT)
        .await
        .expect("Headless service should be created");

    // Wait for client service to exist
    wait::wait_for_resource(&svc_api, "test-svc", SHORT_TIMEOUT)
        .await
        .expect("Client service should be created");

    // Verify headless service
    assert_headless_service_exists(client.clone(), &ns_name, "test-svc-headless").await;

    // Verify client service
    assert_service_exists(client.clone(), &ns_name, "test-svc").await;

    // Verify owner reference on headless service
    let headless_svc = svc_api.get("test-svc-headless").await.unwrap();
    assert_has_owner_reference(&headless_svc, "test-svc", "ValkeyCluster");
}

/// Test that creating a ValkeyCluster creates a PodDisruptionBudget.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_pdb() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-pdb").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-pdb", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for PDB to exist
    wait::wait_for_resource(&pdb_api, "test-pdb", SHORT_TIMEOUT)
        .await
        .expect("PDB should be created");

    // Verify owner reference
    let pdb = pdb_api.get("test-pdb").await.unwrap();
    assert_has_owner_reference(&pdb, "test-pdb", "ValkeyCluster");
}

/// Test that a ValkeyCluster transitions through phases correctly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_phase_transitions() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "phase-transitions").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-phases", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Should transition to Creating
    wait_for_phase(
        &api,
        "test-phases",
        ClusterPhase::Creating,
        SHORT_TIMEOUT,
    )
    .await
    .expect("Resource should reach Creating phase");

    // Should transition to Initializing
    wait_for_phase(
        &api,
        "test-phases",
        ClusterPhase::Initializing,
        LONG_TIMEOUT,
    )
    .await
    .expect("Resource should reach Initializing phase");

    // Should transition to AssigningSlots
    wait_for_phase(
        &api,
        "test-phases",
        ClusterPhase::AssigningSlots,
        LONG_TIMEOUT,
    )
    .await
    .expect("Resource should reach AssigningSlots phase");

    // Should eventually become Running
    wait_for_operational(&api, "test-phases", LONG_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Verify final state
    assert_valkeycluster_phase(client.clone(), &ns_name, "test-phases", ClusterPhase::Running)
        .await;
}

/// Test that a ValkeyCluster reaches Running phase when pods are ready.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resource_becomes_running() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "becomes-running").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-running", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for resource to become operational (Running with replicas ready)
    wait_for_operational(&api, "test-running", LONG_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Verify final state
    assert_valkeycluster_phase(client.clone(), &ns_name, "test-running", ClusterPhase::Running)
        .await;

    // Verify cluster status fields
    let resource = api.get("test-running").await.expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(status.ready_replicas, 3, "Should have 3 ready replicas");
}

/// Test that deleting a ValkeyCluster triggers cleanup.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resource_deletion() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "deletion-test").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-delete", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for StatefulSet to be created
    wait::wait_for_resource(&sts_api, "test-delete", DEFAULT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Delete the resource
    api.delete("test-delete", &DeleteParams::default())
        .await
        .expect("Failed to delete ValkeyCluster");

    // Wait for deletion to complete
    wait::wait_for_deletion(&api, "test-delete", LONG_TIMEOUT)
        .await
        .expect("ValkeyCluster should be deleted");

    // Verify owned resources are also deleted (via owner references)
    tokio::time::sleep(Duration::from_secs(5)).await;

    let statefulset_exists = sts_api.get("test-delete").await.is_ok();
    assert!(
        !statefulset_exists,
        "StatefulSet should be garbage collected after ValkeyCluster deletion"
    );
}

/// Test that validation rejects invalid master counts.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_validation_rejects_invalid_masters() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "validation-test").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Try to create resource with invalid masters count (2 < minimum of 3)
    let invalid_resource = test_resource("invalid-masters", 2, 0);
    let result = api.create(&PostParams::default(), &invalid_resource).await;

    // The resource might be created but should transition to Failed phase
    // or be rejected by webhook validation
    if result.is_ok() {
        // If created, wait for it to reach Failed phase
        let resource = wait_for_condition(
            &api,
            "invalid-masters",
            |r| {
                r.status
                    .as_ref()
                    .map(|s| s.phase == ClusterPhase::Failed)
                    .unwrap_or(false)
            },
            SHORT_TIMEOUT,
        )
        .await
        .expect("Resource should reach Failed phase");

        assert_eq!(
            resource.status.as_ref().map(|s| s.phase),
            Some(ClusterPhase::Failed),
            "Resource with invalid masters should be in Failed phase"
        );
    }
    // If rejected by webhook, the test passes (validation worked)
}

/// Test that observed_generation is updated after reconciliation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_observed_generation_updated() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "observed-gen").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-gen", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for generation to be observed
    let resource = wait_for_condition(
        &api,
        "test-gen",
        |r| {
            let current_gen = r.metadata.generation;
            let observed_gen = r.status.as_ref().and_then(|s| s.observed_generation);
            current_gen.is_some() && current_gen == observed_gen
        },
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("observed_generation should be updated");

    let observed = resource.status.unwrap().observed_generation.unwrap();
    let generation = resource.metadata.generation.unwrap();
    assert_eq!(observed, generation, "observed_generation should match generation");
}

/// Test that connection endpoint is populated in status.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_connection_endpoint_populated() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "conn-endpoint").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-conn", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for connection endpoint to be populated
    let resource = wait_for_condition(
        &api,
        "test-conn",
        |r| {
            r.status
                .as_ref()
                .and_then(|s| s.connection_endpoint.as_ref())
                .is_some()
        },
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("connection_endpoint should be populated");

    let endpoint = resource.status.unwrap().connection_endpoint.unwrap();
    assert!(
        endpoint.contains("test-conn"),
        "connection_endpoint should reference the cluster name"
    );
}
