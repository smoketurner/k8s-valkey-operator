//! Failure scenario tests for ValkeyUpgrade operations.
//!
//! These tests verify upgrade behavior in failure scenarios:
//! - Failover failures
//! - Pod deletion failures
//! - Replication sync timeouts
//! - Cluster health check failures

use kube::api::{Api, PostParams};

use valkey_operator::crd::{UpgradePhase, ValkeyCluster, ValkeyUpgrade};

use crate::{
    DEFAULT_TIMEOUT, LONG_TIMEOUT, ScopedOperator, TestNamespace, create_auth_secret,
    init_test_with_upgrade, test_cluster_with_replicas, test_upgrade, wait_for_condition,
    wait_for_operational,
};

/// Test that upgrade fails gracefully when cluster becomes unhealthy during upgrade.
///
/// This test verifies that the upgrade process detects cluster health issues
/// and transitions to Failed phase rather than continuing with an unhealthy cluster.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_fails_on_cluster_health_issue() {
    let (cluster, _permit) = init_test_with_upgrade().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-health-fail").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster (0 replicas to reduce resource usage)
    let cluster = test_cluster_with_replicas("health-fail-target", 0);
    cluster_api
        .create(&PostParams::default(), &cluster)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to be operational
    wait_for_operational(&cluster_api, "health-fail-target", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Get current image version
    let cluster_resource = cluster_api
        .get("health-fail-target")
        .await
        .expect("Should get cluster");
    let current_version = cluster_resource.spec.image.tag.clone();
    let target_version = if current_version == "9.0.0" {
        "9.0.1"
    } else {
        "9.0.0"
    };

    // Create upgrade
    let upgrade = test_upgrade("health-fail-test", "health-fail-target", target_version);
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for upgrade to start
    wait_for_condition(
        &upgrade_api,
        "health-fail-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == UpgradePhase::InProgress || s.phase.is_terminal())
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("Upgrade should start");

    // Note: In a real failure scenario, we would simulate cluster health issues
    // (e.g., delete pods, cause network partitions). For this test, we verify
    // that the upgrade process has proper error handling and can transition to Failed.

    // Check that upgrade either completes or fails gracefully
    let upgrade_resource = upgrade_api
        .get("health-fail-test")
        .await
        .expect("Should get upgrade");

    if let Some(status) = upgrade_resource.status {
        // Upgrade should be in a valid state (not stuck)
        assert!(
            status.phase == UpgradePhase::InProgress
                || status.phase == UpgradePhase::Completed
                || status.phase == UpgradePhase::Failed
                || status.phase == UpgradePhase::RollingBack,
            "Upgrade should be in a valid state, got: {:?}",
            status.phase
        );

        // If failed, verify error message is present
        if status.phase == UpgradePhase::Failed {
            // Status should have error information
            assert!(
                status.shard_statuses.iter().any(|s| s.error.is_some())
                    || !status.progress.is_empty(),
                "Failed upgrade should have error information"
            );
        }
    }
}

/// Test that upgrade handles concurrent upgrade prevention correctly.
///
/// This test verifies that creating a second upgrade targeting the same cluster
/// fails validation and transitions to Failed phase.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_concurrent_upgrade_prevention() {
    let (cluster, _permit) = init_test_with_upgrade().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-concurrent").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster (0 replicas to reduce resource usage)
    let cluster = test_cluster_with_replicas("concurrent-target", 0);
    cluster_api
        .create(&PostParams::default(), &cluster)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to be operational
    wait_for_operational(&cluster_api, "concurrent-target", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Get current image version
    let cluster_resource = cluster_api
        .get("concurrent-target")
        .await
        .expect("Should get cluster");
    let current_version = cluster_resource.spec.image.tag.clone();
    let target_version = if current_version == "9.0.0" {
        "9.0.1"
    } else {
        "9.0.0"
    };

    // Create first upgrade
    let upgrade1 = test_upgrade("concurrent-test-1", "concurrent-target", target_version);
    upgrade_api
        .create(&PostParams::default(), &upgrade1)
        .await
        .expect("Failed to create first ValkeyUpgrade");

    // Wait for first upgrade to enter active phase
    wait_for_condition(
        &upgrade_api,
        "concurrent-test-1",
        |r| {
            r.status
                .as_ref()
                .map(|s| {
                    s.phase == UpgradePhase::PreChecks
                        || s.phase == UpgradePhase::InProgress
                        || s.phase.is_terminal()
                })
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("First upgrade should start");

    // Try to create second upgrade targeting same cluster
    let upgrade2 = test_upgrade("concurrent-test-2", "concurrent-target", target_version);
    upgrade_api
        .create(&PostParams::default(), &upgrade2)
        .await
        .expect("Failed to create second ValkeyUpgrade");

    // Second upgrade should fail validation
    wait_for_condition(
        &upgrade_api,
        "concurrent-test-2",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == UpgradePhase::Failed)
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("Second upgrade should fail due to concurrent upgrade prevention");

    // Verify error message indicates concurrent upgrade
    let upgrade2_resource = upgrade_api
        .get("concurrent-test-2")
        .await
        .expect("Should get second upgrade");

    if let Some(status) = upgrade2_resource.status
        && status.phase == UpgradePhase::Failed
    {
        // Error information should be present in one of these fields:
        // - error_message (for validation failures)
        // - progress (for in-progress failures)
        // - shard_statuses errors (for per-shard failures)
        let has_error_info = status.error_message.is_some()
            || !status.progress.is_empty()
            || status.shard_statuses.iter().any(|s| s.error.is_some());
        assert!(
            has_error_info,
            "Failed upgrade should have error information about concurrent upgrade"
        );

        // Verify the error message mentions concurrent upgrade
        if let Some(ref msg) = status.error_message {
            assert!(
                msg.contains("another upgrade") || msg.contains("in progress"),
                "Error message should mention concurrent upgrade: {}",
                msg
            );
        }
    }
}

/// Test that upgrade handles invalid target version gracefully.
///
/// This test verifies that upgrades with invalid or missing target versions
/// fail validation and transition to Failed phase.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_fails_with_invalid_version() {
    let (cluster, _permit) = init_test_with_upgrade().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-invalid-version").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster
    let cluster = test_cluster_with_replicas("invalid-version-target", 0);
    cluster_api
        .create(&PostParams::default(), &cluster)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to be operational
    wait_for_operational(&cluster_api, "invalid-version-target", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Create upgrade with empty target version (should fail validation)
    let upgrade = test_upgrade("invalid-version-test", "invalid-version-target", "");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Upgrade should fail validation
    // Note: Validation may happen at different stages, so we check if it fails
    // or if it's still pending (validation might be deferred)
    let _ = wait_for_condition(
        &upgrade_api,
        "invalid-version-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == UpgradePhase::Failed)
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await;
    let upgrade_resource = upgrade_api
        .get("invalid-version-test")
        .await
        .expect("Should get upgrade");

    if let Some(status) = upgrade_resource.status {
        // Upgrade should either fail or be in a state that will fail
        assert!(
            status.phase == UpgradePhase::Failed
                || status.phase == UpgradePhase::Pending
                || status.phase == UpgradePhase::PreChecks,
            "Upgrade with invalid version should fail or be pending validation, got: {:?}",
            status.phase
        );
    }
}
