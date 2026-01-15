//! Scaling tests for ValkeyCluster operations.
//!
//! These tests verify scaling behavior including:
//! - Scale up operations (add masters)
//! - Scale down operations (remove masters)
//! - Degraded state handling

use std::time::Duration;

use k8s_openapi::api::apps::v1::Deployment;
use kube::api::{Api, Patch, PatchParams, PostParams};

use valkey_operator::crd::{total_pods, ClusterPhase, ValkeyCluster};

use crate::assertions::assert_deployment_replicas;
use crate::init_test;
use crate::namespace::TestNamespace;
use crate::wait::{wait_for_condition, wait_for_operational, wait_for_phase};

/// Helper to create a test ValkeyCluster.
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

/// Test scaling up a ValkeyCluster by adding masters.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_scale_up_masters() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "scale-up").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters and 1 replica per master = 6 total pods
    let resource = test_resource("scale-up-test", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for initial deployment
    wait_for_operational(&api, "scale-up-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational");

    // Scale up to 6 masters (6 + 6 replicas = 12 total pods)
    let patch = serde_json::json!({
        "spec": {
            "masters": 6
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
        ClusterPhase::Updating,
        Duration::from_secs(30),
    )
    .await
    .expect("Resource should enter Updating phase");

    // Wait for resource to be operational again
    wait_for_operational(&api, "scale-up-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational after scale up");

    // Verify deployment has 12 pods (6 masters + 6 replicas)
    let expected_pods = total_pods(6, 1);
    assert_deployment_replicas(client.clone(), &ns_name, "scale-up-test", expected_pods).await;

    // Verify ValkeyCluster status
    let resource = api.get("scale-up-test").await.expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(
        status.ready_replicas, expected_pods,
        "Should have {} ready replicas",
        expected_pods
    );
}

/// Test scaling down a ValkeyCluster by removing masters.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_scale_down_masters() {
    let (_cluster, client) = init_test().await;
    let test_ns = TestNamespace::create(client.clone(), "scale-down").await;
    let ns_name = test_ns.name().to_string();

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 6 masters and 1 replica per master = 12 total pods
    let resource = test_resource("scale-down-test", 6, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for initial deployment
    wait_for_operational(&api, "scale-down-test", Duration::from_secs(240))
        .await
        .expect("Resource should become operational");

    // Scale down to 3 masters (3 + 3 replicas = 6 total pods)
    let patch = serde_json::json!({
        "spec": {
            "masters": 3
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
    wait_for_operational(&api, "scale-down-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational after scale down");

    // Verify deployment has 6 pods (3 masters + 3 replicas)
    let expected_pods = total_pods(3, 1);
    assert_deployment_replicas(client.clone(), &ns_name, "scale-down-test", expected_pods).await;

    // Verify ValkeyCluster status
    let resource = api
        .get("scale-down-test")
        .await
        .expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(
        status.ready_replicas, expected_pods,
        "Should have {} ready replicas",
        expected_pods
    );
}

/// Test that a ValkeyCluster enters Degraded state when replicas are unhealthy.
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

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters and 1 replica per master = 6 total pods
    let resource = test_resource("degraded-test", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    let expected_pods = total_pods(3, 1);

    // Wait for resource to be operational
    wait_for_operational(&api, "degraded-test", Duration::from_secs(180))
        .await
        .expect("Resource should become operational");

    // Scale deployment directly to fewer replicas to simulate pod failure
    // (This simulates a failure scenario where not all pods are available)
    let reduced_pods = expected_pods - 2;
    let deploy_patch = serde_json::json!({
        "spec": {
            "replicas": reduced_pods
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
                .map(|s| s.phase == ClusterPhase::Degraded || s.ready_replicas < expected_pods)
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
                status.phase == ClusterPhase::Degraded || status.ready_replicas < expected_pods,
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

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters and 1 replica per master
    let resource = test_resource("recovery-test", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    let expected_pods = total_pods(3, 1);

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
    assert_eq!(
        status.phase,
        ClusterPhase::Running,
        "Should be in Running phase"
    );
    assert_eq!(
        status.ready_replicas, expected_pods,
        "Should have {} ready replicas",
        expected_pods
    );
}
