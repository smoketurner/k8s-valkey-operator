//! Functional tests for ValkeyCluster lifecycle operations.
//!
//! These tests verify the core functionality of the operator including:
//! - Resource creation and owned resource generation
//! - Spec updates and reconciliation
//! - Resource deletion and cleanup

use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::api::{Api, Patch, PatchParams, PostParams};

use valkey_operator::crd::{ClusterPhase, ValkeyCluster};

use crate::assertions::{
    assert_configmap_has_key, assert_deployment_replicas, assert_has_owner_reference,
    assert_service_exists, assert_valkeycluster_phase,
};
use crate::namespace::TestNamespace;
use crate::wait::{wait_for_condition, wait_for_operational, wait_for_phase};
use crate::{init_test, wait};

/// Helper to create a test ValkeyCluster with the new spec structure.
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
                    "name": "test-issuer"
                }
            },
            "auth": {
                "secretRef": {
                    "name": "test-secret"
                }
            }
        }
    }))
    .expect("Failed to create test resource")
}

/// Test that creating a ValkeyCluster creates the expected Deployment.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_deployment() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-deployment").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters and 1 replica per master = 6 total pods
    let resource = test_resource("test-deploy", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for resource to start creating
    wait_for_phase(
        &api,
        "test-deploy",
        ClusterPhase::Creating,
        Duration::from_secs(30),
    )
    .await
    .expect("Resource should reach Creating phase");

    // Wait for deployment to exist
    wait::wait_for_resource(&deploy_api, "test-deploy", Duration::from_secs(30))
        .await
        .expect("Deployment should be created");

    // Verify deployment properties (3 masters + 3 replicas = 6 pods)
    assert_deployment_replicas(client.clone(), &ns_name, "test-deploy", 6).await;

    // Verify owner reference
    let deployment = deploy_api.get("test-deploy").await.unwrap();
    assert_has_owner_reference(&deployment, "test-deploy", "ValkeyCluster");
}

/// Test that creating a ValkeyCluster creates the expected ConfigMap.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_configmap() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-configmap").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-cm", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for configmap to exist
    wait::wait_for_resource(&cm_api, "test-cm", Duration::from_secs(30))
        .await
        .expect("ConfigMap should be created");

    // Verify configmap has expected keys (masters, replicas_per_master, total_pods)
    assert_configmap_has_key(client.clone(), &ns_name, "test-cm", "masters").await;
    assert_configmap_has_key(client.clone(), &ns_name, "test-cm", "replicas_per_master").await;
    assert_configmap_has_key(client.clone(), &ns_name, "test-cm", "total_pods").await;

    // Verify owner reference
    let configmap = cm_api.get("test-cm").await.unwrap();
    assert_has_owner_reference(&configmap, "test-cm", "ValkeyCluster");
}

/// Test that creating a ValkeyCluster creates the expected Service.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_service() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-service").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-svc", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for service to exist
    wait::wait_for_resource(&svc_api, "test-svc", Duration::from_secs(30))
        .await
        .expect("Service should be created");

    // Verify service exists
    assert_service_exists(client.clone(), &ns_name, "test-svc").await;

    // Verify owner reference
    let service = svc_api.get("test-svc").await.unwrap();
    assert_has_owner_reference(&service, "test-svc", "ValkeyCluster");
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
    let resource = test_resource("test-running", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for resource to become operational (Running with replicas ready)
    wait_for_operational(&api, "test-running", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Verify final state
    assert_valkeycluster_phase(client.clone(), &ns_name, "test-running", ClusterPhase::Running)
        .await;
}

/// Test that updating a ValkeyCluster spec triggers reconciliation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_spec_update() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "spec-update").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters
    let resource = test_resource("test-update", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for initial reconciliation
    wait_for_operational(&api, "test-update", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Verify initial ConfigMap
    let configmap = cm_api
        .get("test-update")
        .await
        .expect("ConfigMap should exist");
    let masters = configmap
        .data
        .as_ref()
        .and_then(|d| d.get("masters"))
        .expect("ConfigMap should have masters key");
    assert_eq!(masters, "3", "Initial masters should be 3");

    // Update to 6 masters (scale up)
    let patch = serde_json::json!({
        "spec": {
            "masters": 6
        }
    });
    api.patch(
        "test-update",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to patch ValkeyCluster");

    // Wait for the update to be processed
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify ConfigMap was updated with new masters count
    let configmap = cm_api
        .get("test-update")
        .await
        .expect("ConfigMap should exist");
    let masters = configmap
        .data
        .as_ref()
        .and_then(|d| d.get("masters"))
        .expect("ConfigMap should have masters key");

    assert_eq!(masters, "6", "ConfigMap should have updated masters count");
}

/// Test that deleting a ValkeyCluster triggers cleanup.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resource_deletion() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "deletion-test").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-delete", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for resource to be ready
    wait_for_operational(&api, "test-delete", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Delete the resource
    api.delete("test-delete", &Default::default())
        .await
        .expect("Failed to delete ValkeyCluster");

    // Wait for deletion to complete
    wait::wait_for_deletion(&api, "test-delete", Duration::from_secs(60))
        .await
        .expect("ValkeyCluster should be deleted");

    // Verify owned resources are also deleted (via owner references)
    tokio::time::sleep(Duration::from_secs(5)).await;

    let deployment_exists = deploy_api.get("test-delete").await.is_ok();
    assert!(
        !deployment_exists,
        "Deployment should be garbage collected after ValkeyCluster deletion"
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
    let invalid_resource = test_resource("invalid-masters", 2, 1);
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
            Duration::from_secs(30),
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
