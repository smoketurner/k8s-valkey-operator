//! Slot management integration tests.
//!
//! These tests verify that the Valkey cluster slot distribution and migration
//! works correctly during scaling operations.
//!
//! Key scenarios:
//! - Initial slot distribution across masters
//! - Slot rebalancing during scale-up
//! - Slot migration during scale-down
//! - No data loss during rebalancing operations

use kube::api::{Api, Patch, PatchParams, PostParams};

use valkey_operator::crd::{ClusterPhase, ValkeyCluster, total_pods};
use valkey_operator::slots::TOTAL_SLOTS;

use crate::assertions::assert_valkeycluster_phase;
use crate::{
    DEFAULT_TIMEOUT, EXTENDED_TIMEOUT, LONG_TIMEOUT, SHORT_TIMEOUT, ScopedOperator, TestNamespace,
    create_auth_secret, init_test, test_cluster_with_config, wait_for_condition,
    wait_for_operational, wait_for_phase,
};

/// Helper to check if all slots are assigned (16384/16384).
fn all_slots_assigned(resource: &ValkeyCluster) -> bool {
    resource
        .status
        .as_ref()
        .map(|s| s.assigned_slots.contains("16384/16384"))
        .unwrap_or(false)
}

/// Test that initial slot distribution assigns all 16384 slots to masters.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_initial_slot_distribution() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "slot-init").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create a 3-master cluster
    let resource = test_cluster_with_config("slot-init-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "slot-init-test", LONG_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Verify all slots are assigned
    let resource = wait_for_condition(&api, "slot-init-test", all_slots_assigned, LONG_TIMEOUT)
        .await
        .expect("All slots should be assigned");

    let status = resource.status.expect("Should have status");

    // Verify format is "16384/16384"
    assert!(
        status.assigned_slots.contains(&TOTAL_SLOTS.to_string()),
        "Should have all {} slots assigned, got: {}",
        TOTAL_SLOTS,
        status.assigned_slots
    );
}

/// Test that scaling up redistributes slots to new masters.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_slot_rebalancing_on_scale_up() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "slot-scale-up").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create a 3-master cluster
    let resource = test_cluster_with_config("slot-rebalance-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational with all slots assigned
    wait_for_operational(&api, "slot-rebalance-test", LONG_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Verify initial slot distribution
    let initial_resource = wait_for_condition(
        &api,
        "slot-rebalance-test",
        all_slots_assigned,
        LONG_TIMEOUT,
    )
    .await
    .expect("Initial slots should be assigned");

    println!(
        "Initial state: {}",
        initial_resource
            .status
            .as_ref()
            .map(|s| s.assigned_slots.as_str())
            .unwrap_or("<none>")
    );

    // Scale up to 6 masters
    let patch = serde_json::json!({
        "spec": {
            "masters": 6
        }
    });
    api.patch(
        "slot-rebalance-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale up");

    // Should transition through Updating/Resharding phases
    let updating_or_resharding = wait_for_condition(
        &api,
        "slot-rebalance-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == ClusterPhase::Updating || s.phase == ClusterPhase::Resharding)
                .unwrap_or(false)
        },
        SHORT_TIMEOUT,
    )
    .await;

    if updating_or_resharding.is_ok() {
        println!("Cluster entered Updating/Resharding phase during scale-up");
    }

    // Wait for cluster to be operational again
    wait_for_operational(&api, "slot-rebalance-test", EXTENDED_TIMEOUT)
        .await
        .expect("Resource should become operational after scale up");

    // Verify all slots are still assigned after rebalancing
    let final_resource = wait_for_condition(
        &api,
        "slot-rebalance-test",
        all_slots_assigned,
        LONG_TIMEOUT,
    )
    .await
    .expect("All slots should still be assigned after rebalancing");

    let status = final_resource.status.expect("Should have status");
    println!("Final state: {}", status.assigned_slots);

    // Verify the cluster has 6 replicas now
    let expected_pods = total_pods(6, 0);
    assert_eq!(
        status.ready_replicas, expected_pods,
        "Should have {} ready replicas",
        expected_pods
    );
}

/// Test that scaling down migrates slots before removing nodes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_slot_migration_on_scale_down() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "slot-scale-down").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create a 6-master cluster (larger to scale down)
    let resource = test_cluster_with_config("slot-migrate-test", 6, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "slot-migrate-test", EXTENDED_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Verify initial slot distribution
    let initial_resource =
        wait_for_condition(&api, "slot-migrate-test", all_slots_assigned, LONG_TIMEOUT)
            .await
            .expect("Initial slots should be assigned");

    println!(
        "Initial state: {}",
        initial_resource
            .status
            .as_ref()
            .map(|s| s.assigned_slots.as_str())
            .unwrap_or("<none>")
    );

    // Scale down to 3 masters
    let patch = serde_json::json!({
        "spec": {
            "masters": 3
        }
    });
    api.patch(
        "slot-migrate-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale down");

    // Should transition through Updating/Resharding phases
    wait_for_phase(
        &api,
        "slot-migrate-test",
        ClusterPhase::Updating,
        SHORT_TIMEOUT,
    )
    .await
    .ok(); // May be fast enough to skip directly to Resharding

    // Wait for cluster to be operational again
    wait_for_operational(&api, "slot-migrate-test", EXTENDED_TIMEOUT)
        .await
        .expect("Resource should become operational after scale down");

    // Verify all slots are still assigned after migration
    let final_resource =
        wait_for_condition(&api, "slot-migrate-test", all_slots_assigned, LONG_TIMEOUT)
            .await
            .expect("All slots should still be assigned after migration");

    let status = final_resource.status.expect("Should have status");
    println!("Final state: {}", status.assigned_slots);

    // Verify the cluster has 3 replicas now
    let expected_pods = total_pods(3, 0);
    assert_eq!(
        status.ready_replicas, expected_pods,
        "Should have {} ready replicas",
        expected_pods
    );
}

/// Test that cluster remains healthy during multiple scaling operations.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_slot_stability_during_multiple_scales() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "slot-multi-scale").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create a 3-master cluster
    let resource = test_cluster_with_config("multi-scale-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for initial operational state
    wait_for_operational(&api, "multi-scale-test", LONG_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Scale up to 4 masters
    let patch = serde_json::json!({ "spec": { "masters": 4 } });
    api.patch(
        "multi-scale-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale to 4");

    wait_for_operational(&api, "multi-scale-test", EXTENDED_TIMEOUT)
        .await
        .expect("Should be operational after scale to 4");

    // Scale up to 5 masters
    let patch = serde_json::json!({ "spec": { "masters": 5 } });
    api.patch(
        "multi-scale-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale to 5");

    wait_for_operational(&api, "multi-scale-test", EXTENDED_TIMEOUT)
        .await
        .expect("Should be operational after scale to 5");

    // Scale back down to 3 masters
    let patch = serde_json::json!({ "spec": { "masters": 3 } });
    api.patch(
        "multi-scale-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale to 3");

    wait_for_operational(&api, "multi-scale-test", EXTENDED_TIMEOUT)
        .await
        .expect("Should be operational after scale back to 3");

    // Wait for the final state to be stable with exact replica count
    // This ensures the scale-down has fully completed
    let final_resource = wait_for_condition(
        &api,
        "multi-scale-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| {
                    s.phase == ClusterPhase::Running
                        && s.ready_replicas == total_pods(3, 0)
                        && s.assigned_slots.contains("16384/16384")
                })
                .unwrap_or(false)
        },
        EXTENDED_TIMEOUT,
    )
    .await
    .expect("Cluster should stabilize in Running phase with correct replica count");

    let status = final_resource.status.expect("Should have status");

    assert_eq!(status.phase, ClusterPhase::Running, "Should be Running");
    assert_eq!(
        status.ready_replicas,
        total_pods(3, 0),
        "Should have 3 pods"
    );
    assert!(
        status.assigned_slots.contains("16384/16384"),
        "All slots should be assigned"
    );
}

/// Test that the Resharding phase is detected during slot operations.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resharding_phase_detection() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "resharding-detect").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create a 3-master cluster
    let resource = test_cluster_with_config("reshard-detect-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for operational state
    wait_for_operational(&api, "reshard-detect-test", LONG_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Scale up to trigger resharding
    let patch = serde_json::json!({ "spec": { "masters": 6 } });
    api.patch(
        "reshard-detect-test",
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await
    .expect("Failed to scale up");

    // Try to detect the Resharding phase (may be fast, so this is best-effort)
    let resharding_detected = wait_for_phase(
        &api,
        "reshard-detect-test",
        ClusterPhase::Resharding,
        DEFAULT_TIMEOUT,
    )
    .await;

    if resharding_detected.is_ok() {
        println!("Successfully detected Resharding phase");
        assert_valkeycluster_phase(
            client.clone(),
            &ns_name,
            "reshard-detect-test",
            ClusterPhase::Resharding,
        )
        .await;
    } else {
        // If we missed the Resharding phase, at least verify it completes
        println!("Resharding phase was too fast to detect, verifying completion");
    }

    // Wait for completion
    wait_for_operational(&api, "reshard-detect-test", EXTENDED_TIMEOUT)
        .await
        .expect("Resource should become operational");

    // Final verification
    assert_valkeycluster_phase(
        client.clone(),
        &ns_name,
        "reshard-detect-test",
        ClusterPhase::Running,
    )
    .await;
}
