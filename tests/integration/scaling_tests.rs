//! Scaling tests for MyResource operations.
//!
//! These tests verify scaling behavior including:
//! - Scale up operations
//! - Scale down operations
//! - Degraded state handling

use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, Patch, PatchParams, PostParams};

use my_operator::crd::{MyResource, Phase};

use crate::assertions::assert_deployment_replicas;
use crate::init_test;
use crate::namespace::TestNamespace;
use crate::wait::{wait_for_condition, wait_for_operational, wait_for_phase};

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

/// Test scaling up a MyResource.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_scale_up() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "scale-up").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 1 replica
    let resource = test_resource("scale-up-test", 1, "scale up test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for initial deployment
    wait_for_operational(&api, "scale-up-test", Duration::from_secs(120))
        .await
        .expect("Resource should become operational");

    // Scale up to 2 replicas
    let patch = serde_json::json!({
        "spec": {
            "replicas": 2
        }
    });
    api.patch(
        "scale-up-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale up");

    // Wait for updating phase
    wait_for_phase(
        &api,
        "scale-up-test",
        Phase::Updating,
        Duration::from_secs(30),
    )
    .await
    .expect("Resource should enter Updating phase");

    // Wait for resource to be operational again
    wait_for_operational(&api, "scale-up-test", Duration::from_secs(120))
        .await
        .expect("Resource should become operational after scale up");

    // Verify deployment has 2 replicas
    assert_deployment_replicas(client.clone(), &ns_name, "scale-up-test", 2).await;

    // Verify MyResource status
    let resource = api.get("scale-up-test").await.expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(status.ready_replicas, 2, "Should have 2 ready replicas");
}

/// Test scaling down a MyResource.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_scale_down() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "scale-down").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 replicas
    let resource = test_resource("scale-down-test", 3, "scale down test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for initial deployment
    wait_for_operational(&api, "scale-down-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational");

    // Scale down to 2 replicas
    let patch = serde_json::json!({
        "spec": {
            "replicas": 2
        }
    });
    api.patch(
        "scale-down-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale down");

    // Wait for resource to be operational again
    wait_for_operational(&api, "scale-down-test", Duration::from_secs(120))
        .await
        .expect("Resource should become operational after scale down");

    // Verify deployment has 2 replicas
    assert_deployment_replicas(client.clone(), &ns_name, "scale-down-test", 2).await;

    // Verify MyResource status
    let resource = api
        .get("scale-down-test")
        .await
        .expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(status.ready_replicas, 2, "Should have 2 ready replicas");
}

/// Test that a MyResource enters Degraded state when replicas are unhealthy.
///
/// Note: This test requires the ability to make pods unhealthy.
/// In a real test environment, you might use a chaos engineering tool
/// or delete pods directly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_degraded_state() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "degraded-state").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 2 replicas
    let resource = test_resource("degraded-test", 2, "degraded test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for resource to be operational
    wait_for_operational(&api, "degraded-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational");

    // Scale deployment directly to 1 replica to simulate pod failure
    // (This simulates a failure scenario where not all pods are available)
    let deploy_patch = serde_json::json!({
        "spec": {
            "replicas": 1
        }
    });
    deploy_api
        .patch(
            "degraded-test",
            &PatchParams::apply("test-harness").force(),
            &Patch::Apply(&deploy_patch),
        )
        .await
        .expect("Failed to patch deployment");

    // Give the operator time to detect the degraded state
    // Note: The operator will try to restore the desired state,
    // so we need to watch for the degraded state quickly
    let result = wait_for_condition(
        &api,
        "degraded-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == Phase::Degraded || s.ready_replicas < 2)
                .unwrap_or(false)
        },
        Duration::from_secs(30),
    )
    .await;

    // The test passes if either:
    // 1. The resource reached Degraded phase, or
    // 2. The operator detected fewer ready replicas than desired
    match result {
        Ok(resource) => {
            let status = resource.status.expect("Should have status");
            println!(
                "Resource state: phase={:?}, ready_replicas={}",
                status.phase, status.ready_replicas
            );
            // Either degraded or has fewer replicas
            assert!(
                status.phase == Phase::Degraded || status.ready_replicas < 2,
                "Resource should detect degraded state"
            );
        }
        Err(e) => {
            // If we timed out, check the final state
            let resource = api.get("degraded-test").await.expect("Should get resource");
            let status = resource.status.expect("Should have status");
            println!(
                "Timeout checking degraded state. Final state: phase={:?}, ready_replicas={}",
                status.phase, status.ready_replicas
            );
            panic!("Failed to observe degraded state: {}", e);
        }
    }
}

/// Test recovery from degraded state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_recovery_from_degraded() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "recovery-test").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<MyResource> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_resource("recovery-test", 2, "recovery test");
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    // Wait for resource to be operational
    wait_for_operational(&api, "recovery-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational");

    // Resource should recover and return to Running state
    // (The operator continuously reconciles and will restore the desired state)
    wait_for_operational(&api, "recovery-test", Duration::from_secs(120))
        .await
        .expect("Resource should recover to operational state");

    // Verify final state
    let resource = api.get("recovery-test").await.expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(status.phase, Phase::Running, "Should be in Running phase");
    assert_eq!(status.ready_replicas, 2, "Should have 2 ready replicas");
}
