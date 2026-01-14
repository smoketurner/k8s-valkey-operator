//! Functional tests for MyResource lifecycle operations.
//!
//! These tests verify the core functionality of the operator including:
//! - Resource creation and owned resource generation
//! - Spec updates and reconciliation
//! - Resource deletion and cleanup

use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::api::{Api, Patch, PatchParams, PostParams};

use my_operator::crd::{MyResource, Phase};

use crate::assertions::{
    assert_configmap_has_key, assert_deployment_replicas, assert_has_owner_reference,
    assert_myresource_phase, assert_service_exists,
};
use crate::namespace::TestNamespace;
use crate::wait::{wait_for_condition, wait_for_operational, wait_for_phase};
use crate::{init_test, wait};

/// Helper to create a test MyResource.
fn test_resource(name: &str, replicas: i32, message: &str) -> MyResource {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "myoperator.example.com/v1alpha1",
        "kind": "MyResource",
        "metadata": {
            "name": name
        },
        "spec": {
            "replicas": replicas,
            "message": message
        }
    }))
    .expect("Failed to create test resource")
}

/// Test that creating a MyResource creates the expected Deployment.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_deployment() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-deployment").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-deploy", 1, "test message");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for resource to start creating
    wait_for_phase(
        &api,
        "test-deploy",
        Phase::Creating,
        Duration::from_secs(30),
    )
    .await
    .expect("Resource should reach Creating phase");

    // Wait for deployment to exist
    wait::wait_for_resource(&deploy_api, "test-deploy", Duration::from_secs(30))
        .await
        .expect("Deployment should be created");

    // Verify deployment properties
    assert_deployment_replicas(client.clone(), &ns_name, "test-deploy", 1).await;

    // Verify owner reference
    let deployment = deploy_api.get("test-deploy").await.unwrap();
    assert_has_owner_reference(&deployment, "test-deploy", "MyResource");
}

/// Test that creating a MyResource creates the expected ConfigMap.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_configmap() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-configmap").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-cm", 1, "hello world");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for configmap to exist
    wait::wait_for_resource(&cm_api, "test-cm", Duration::from_secs(30))
        .await
        .expect("ConfigMap should be created");

    // Verify configmap has expected keys
    assert_configmap_has_key(client.clone(), &ns_name, "test-cm", "message").await;

    // Verify owner reference
    let configmap = cm_api.get("test-cm").await.unwrap();
    assert_has_owner_reference(&configmap, "test-cm", "MyResource");
}

/// Test that creating a MyResource creates the expected Service.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_service() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "creates-service").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-svc", 1, "service test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for service to exist
    wait::wait_for_resource(&svc_api, "test-svc", Duration::from_secs(30))
        .await
        .expect("Service should be created");

    // Verify service exists
    assert_service_exists(client.clone(), &ns_name, "test-svc").await;

    // Verify owner reference
    let service = svc_api.get("test-svc").await.unwrap();
    assert_has_owner_reference(&service, "test-svc", "MyResource");
}

/// Test that a MyResource reaches Running phase when pods are ready.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resource_becomes_running() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "becomes-running").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-running", 1, "running test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for resource to become operational (Running with replicas ready)
    wait_for_operational(&api, "test-running", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Verify final state
    assert_myresource_phase(client.clone(), &ns_name, "test-running", Phase::Running).await;
}

/// Test that updating a MyResource spec triggers reconciliation.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_spec_update() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "spec-update").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-update", 1, "original message");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for initial reconciliation
    wait_for_operational(&api, "test-update", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Update the message
    let patch = serde_json::json!({
        "spec": {
            "message": "updated message"
        }
    });
    api.patch(
        "test-update",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to patch MyResource");

    // Wait for the update to be processed
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Verify ConfigMap was updated with new message
    let configmap = cm_api
        .get("test-update")
        .await
        .expect("ConfigMap should exist");
    let message = configmap
        .data
        .as_ref()
        .and_then(|d| d.get("message"))
        .expect("ConfigMap should have message key");

    assert_eq!(
        message, "updated message",
        "ConfigMap should have updated message"
    );
}

/// Test that deleting a MyResource triggers cleanup.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resource_deletion() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "deletion-test").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("test-delete", 1, "deletion test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for resource to be ready
    wait_for_operational(&api, "test-delete", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Delete the resource
    api.delete("test-delete", &Default::default())
        .await
        .expect("Failed to delete MyResource");

    // Wait for deletion to complete
    wait::wait_for_deletion(&api, "test-delete", Duration::from_secs(60))
        .await
        .expect("MyResource should be deleted");

    // Verify owned resources are also deleted (via owner references)
    tokio::time::sleep(Duration::from_secs(5)).await;

    let deployment_exists = deploy_api.get("test-delete").await.is_ok();
    assert!(
        !deployment_exists,
        "Deployment should be garbage collected after MyResource deletion"
    );
}

/// Test that validation rejects invalid replica counts.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_validation_rejects_invalid_replicas() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "validation-test").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);

    // Try to create resource with invalid replicas (0)
    let invalid_resource = test_resource("invalid-replicas", 0, "should fail");
    let result = api.create(&PostParams::default(), &invalid_resource).await;

    // The resource might be created but should transition to Failed phase
    // or be rejected by webhook validation
    if result.is_ok() {
        // If created, wait for it to reach Failed phase
        let resource = wait_for_condition(
            &api,
            "invalid-replicas",
            |r| {
                r.status
                    .as_ref()
                    .map(|s| s.phase == Phase::Failed)
                    .unwrap_or(false)
            },
            Duration::from_secs(30),
        )
        .await
        .expect("Resource should reach Failed phase");

        assert_eq!(
            resource.status.as_ref().map(|s| s.phase),
            Some(Phase::Failed),
            "Resource with invalid replicas should be in Failed phase"
        );
    }
    // If rejected by webhook, the test passes (validation worked)
}
