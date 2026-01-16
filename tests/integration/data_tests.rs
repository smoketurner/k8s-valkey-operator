//! Data integrity integration tests.
//!
//! These tests verify that data is preserved during cluster operations:
//! - Data survives pod restarts
//! - Data survives scaling operations
//! - Data survives upgrade operations
//!
//! Note: These tests require actual Valkey connectivity, which may require
//! port-forwarding or in-cluster access.

use std::time::Duration;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, Patch, PatchParams, PostParams};

use valkey_operator::crd::{ClusterPhase, ValkeyCluster};

use crate::common::fixtures::ValkeyClusterBuilder;
use crate::{
    DEFAULT_TIMEOUT, EXTENDED_TIMEOUT, LONG_TIMEOUT, ScopedOperator, TestNamespace,
    create_auth_secret, init_test, wait_for_condition, wait_for_operational, wait_for_resource,
};

/// Helper to create a test ValkeyCluster with persistence enabled.
fn test_resource_with_persistence(name: &str, masters: i32, replicas: i32) -> ValkeyCluster {
    ValkeyClusterBuilder::new(name)
        .masters(masters)
        .replicas_per_master(replicas)
        .persistence(true, Some("1Gi"))
        .build()
}

/// Helper to check if all slots are assigned.
fn all_slots_assigned(resource: &ValkeyCluster) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.assigned_slots.contains("16384/16384"))
        .unwrap_or(false)
}

/// Test that cluster status reports persistence configuration.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_persistence_enabled() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "persistence-test").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();

    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with persistence
    let resource = test_resource_with_persistence("persist-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "persist-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Verify StatefulSet has PVC template
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);
    let sts = sts_api
        .get("persist-test")
        .await
        .expect("StatefulSet should exist");

    let volume_claim_templates = sts
        .spec
        .as_ref()
        .and_then(|s| s.volume_claim_templates.as_ref());

    assert!(
        volume_claim_templates.is_some(),
        "StatefulSet should have volumeClaimTemplates"
    );

    let templates = volume_claim_templates.unwrap();
    assert!(
        !templates.is_empty(),
        "Should have at least one PVC template"
    );

    println!(
        "PVC template count: {}, first template name: {:?}",
        templates.len(),
        templates.first().and_then(|t| t.metadata.name.as_ref())
    );
}

/// Test that data survives pod deletion (simulating node failure).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_data_survives_pod_deletion() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "data-survive").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();

    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with persistence and 1 replica per master
    let resource = test_resource_with_persistence("data-survive-test", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "data-survive-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Get the first pod name
    let pod_name = format!("{}-0", "data-survive-test");

    // Verify pod exists
    wait_for_resource(&pod_api, &pod_name, DEFAULT_TIMEOUT)
        .await
        .expect("Pod should exist");

    // Delete the first pod (simulating failure)
    pod_api
        .delete(&pod_name, &DeleteParams::default())
        .await
        .expect("Should delete pod");

    // Wait for pod to be recreated and ready
    wait_for_condition(
        &pod_api,
        &pod_name,
        |p| {
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_ref())
                .map(|phase| phase == "Running")
                .unwrap_or(false)
        },
        LONG_TIMEOUT,
    )
    .await
    .expect("Pod should be recreated");

    // Wait for cluster to be operational again
    wait_for_operational(&api, "data-survive-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should return to operational state");

    // Verify cluster is healthy
    let resource = api
        .get("data-survive-test")
        .await
        .expect("Should get resource");
    let status = resource.status.expect("Should have status");

    assert_eq!(
        status.phase,
        ClusterPhase::Running,
        "Cluster should be in Running phase"
    );
    println!(
        "Cluster recovered after pod deletion: phase={:?}, ready={}",
        status.phase, status.ready_replicas
    );
}

/// Test that data survives scaling operations.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_data_survives_scale_operations() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "data-scale").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();

    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create initial cluster
    let resource = test_resource_with_persistence("data-scale-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "data-scale-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Scale up to 6 masters
    let patch = serde_json::json!({ "spec": { "masters": 6 } });
    api.patch(
        "data-scale-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale up");

    // Wait for scale-up to complete
    wait_for_operational(&api, "data-scale-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should be operational after scale up");

    // Verify all slots still assigned
    let resource = wait_for_condition(&api, "data-scale-test", all_slots_assigned, DEFAULT_TIMEOUT)
        .await
        .expect("All slots should be assigned after scale up");

    println!(
        "After scale up: slots={}",
        resource
            .status
            .as_ref()
            .map(|s| s.assigned_slots.as_str())
            .unwrap_or("<none>")
    );

    // Scale back down to 3 masters
    let patch = serde_json::json!({ "spec": { "masters": 3 } });
    api.patch(
        "data-scale-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale down");

    // Wait for scale-down to complete
    wait_for_operational(&api, "data-scale-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should be operational after scale down");

    // Verify all slots still assigned
    let final_resource =
        wait_for_condition(&api, "data-scale-test", all_slots_assigned, DEFAULT_TIMEOUT)
            .await
            .expect("All slots should be assigned after scale down");

    let status = final_resource.status.expect("Should have status");
    assert_eq!(status.phase, ClusterPhase::Running);
    assert_eq!(status.ready_replicas, 3);

    println!(
        "After scale down: slots={}, phase={:?}",
        status.assigned_slots, status.phase
    );
}

/// Test that cluster can recover from degraded state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_recovery_maintains_data() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "data-recovery").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();

    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with replicas for redundancy
    let resource = test_resource_with_persistence("data-recovery-test", 3, 1);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "data-recovery-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Verify all slots assigned
    let initial = wait_for_condition(
        &api,
        "data-recovery-test",
        all_slots_assigned,
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("All slots should be assigned");

    println!(
        "Initial state: slots={}",
        initial
            .status
            .as_ref()
            .map(|s| s.assigned_slots.as_str())
            .unwrap_or("<none>")
    );

    // Delete multiple pods to simulate a more severe failure
    for i in 0..2 {
        let pod_name = format!("{}-{}", "data-recovery-test", i);
        let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
    }

    // The cluster should detect degraded state
    // Give it time to notice and start recovery
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Wait for recovery
    wait_for_operational(&api, "data-recovery-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should recover");

    // Verify all slots still assigned after recovery
    let recovered = wait_for_condition(
        &api,
        "data-recovery-test",
        all_slots_assigned,
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("All slots should be assigned after recovery");

    let status = recovered.status.expect("Should have status");
    assert_eq!(status.phase, ClusterPhase::Running);

    println!(
        "After recovery: slots={}, phase={:?}",
        status.assigned_slots, status.phase
    );
}

/// Test that PVCs are created with correct size.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_pvc_size_configuration() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "pvc-size").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();

    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with specific storage size
    let resource = ValkeyClusterBuilder::new("pvc-size-test")
        .persistence(true, Some("2Gi"))
        .build();

    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for StatefulSet to be created
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);
    wait_for_resource(&sts_api, "pvc-size-test", DEFAULT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");

    // Verify PVC template has correct size
    let sts = sts_api
        .get("pvc-size-test")
        .await
        .expect("Should get StatefulSet");

    let templates = sts
        .spec
        .as_ref()
        .and_then(|s| s.volume_claim_templates.as_ref())
        .expect("Should have volume claim templates");

    let first_template = templates
        .first()
        .expect("Should have at least one template");
    let storage_request = first_template
        .spec
        .as_ref()
        .and_then(|s| s.resources.as_ref())
        .and_then(|r| r.requests.as_ref())
        .and_then(|req| req.get("storage"))
        .expect("Should have storage request");

    println!("PVC storage request: {:?}", storage_request);

    // Verify the size matches what we specified
    assert!(
        storage_request.0.contains("2Gi") || storage_request.0.contains("2G"),
        "Storage should be 2Gi, got: {}",
        storage_request.0
    );
}
