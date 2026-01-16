//! Upgrade tests for ValkeyUpgrade operations.
//!
//! These tests verify upgrade behavior including:
//! - Upgrade resource creation
//! - Phase transitions during upgrade
//! - Validation of invalid upgrades
//! - Upgrade completion

use std::time::Duration;

use kube::ResourceExt;
use kube::api::{Api, DeleteParams, PostParams};

use valkey_operator::crd::{UpgradePhase, ValkeyCluster, ValkeyUpgrade};

use crate::fixtures::create_auth_secret;
use crate::{
    ScopedOperator, SharedTestCluster, TestNamespace, ensure_cluster_crd_installed,
    ensure_upgrade_crd_installed, wait_for_condition, wait_for_operational,
};

/// Long timeout for upgrade operations.
const LONG_TIMEOUT: Duration = Duration::from_secs(180);

/// Short timeout for quick checks.
const SHORT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for most operations.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Initialize tracing and ensure CRD is installed
async fn init_test() -> std::sync::Arc<SharedTestCluster> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,kube=warn,valkey_operator=debug")
        .with_test_writer()
        .try_init();

    let cluster = SharedTestCluster::get()
        .await
        .expect("Failed to get cluster");

    ensure_cluster_crd_installed(&cluster)
        .await
        .expect("Failed to install ValkeyCluster CRD");

    ensure_upgrade_crd_installed(&cluster)
        .await
        .expect("Failed to install ValkeyUpgrade CRD");

    cluster
}

/// Helper to create a test ValkeyCluster.
fn test_cluster(name: &str) -> ValkeyCluster {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "valkey-operator.smoketurner.com/v1alpha1",
        "kind": "ValkeyCluster",
        "metadata": {
            "name": name
        },
        "spec": {
            "masters": 3,
            "replicasPerMaster": 0,
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
    .expect("Failed to create test cluster")
}

/// Helper to create a ValkeyUpgrade resource.
fn test_upgrade(name: &str, cluster_name: &str, target_version: &str) -> ValkeyUpgrade {
    serde_json::from_value(serde_json::json!({
        "apiVersion": "valkey-operator.smoketurner.com/v1alpha1",
        "kind": "ValkeyUpgrade",
        "metadata": {
            "name": name
        },
        "spec": {
            "clusterRef": {
                "name": cluster_name
            },
            "targetVersion": target_version
        }
    }))
    .expect("Failed to create test upgrade")
}

/// Test that creating a ValkeyUpgrade fails when target cluster doesn't exist.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_fails_without_cluster() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-no-cluster").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create upgrade without a target cluster
    let upgrade = test_upgrade("upgrade-no-cluster", "nonexistent-cluster", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for upgrade to reach Failed phase (cluster not found)
    let result = wait_for_condition(
        &upgrade_api,
        "upgrade-no-cluster",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == UpgradePhase::Failed)
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await;

    assert!(
        result.is_ok(),
        "Upgrade should fail when target cluster doesn't exist"
    );
}

/// Test that creating a ValkeyUpgrade transitions through phases.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_phase_transitions() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-phases").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // First create the target cluster
    let cluster = test_cluster("upgrade-target");
    cluster_api
        .create(&PostParams::default(), &cluster)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to be operational
    wait_for_operational(&cluster_api, "upgrade-target", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Create upgrade
    let upgrade = test_upgrade("upgrade-phases-test", "upgrade-target", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for upgrade to enter PreChecks phase
    let result = wait_for_condition(
        &upgrade_api,
        "upgrade-phases-test",
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
    .await;

    assert!(
        result.is_ok(),
        "Upgrade should transition from Pending phase"
    );
}

/// Test ValkeyUpgrade resource creation and status initialization.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_status_initialization() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-status").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // First create the target cluster
    let cluster = test_cluster("status-target");
    cluster_api
        .create(&PostParams::default(), &cluster)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to be operational
    wait_for_operational(&cluster_api, "status-target", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Create upgrade
    let upgrade = test_upgrade("status-test", "status-target", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for status to be populated
    let result = wait_for_condition(
        &upgrade_api,
        "status-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.total_shards > 0 || s.phase.is_terminal())
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await;

    assert!(
        result.is_ok(),
        "Upgrade should have status initialized with shard count"
    );

    // Verify status fields
    let upgrade_resource = upgrade_api
        .get("status-test")
        .await
        .expect("Should get upgrade");
    if let Some(status) = upgrade_resource.status {
        // Total shards should match cluster masters (if not failed)
        if status.phase != UpgradePhase::Failed {
            assert_eq!(
                status.total_shards, 3,
                "Total shards should match cluster masters"
            );
        }
        // Target version should be set
        if let Some(target) = status.target_version {
            assert_eq!(target, "9.0.1", "Target version should be set in status");
        }
    }
}

/// Test that ValkeyUpgrade observed_generation is updated.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_observed_generation() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-gen").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create upgrade (it will fail since no cluster exists, but generation should be observed)
    let upgrade = test_upgrade("gen-test", "nonexistent-cluster", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for generation to be observed
    let result = wait_for_condition(
        &upgrade_api,
        "gen-test",
        |r| {
            let current_gen = r.metadata.generation;
            let observed_gen = r.status.as_ref().and_then(|s| s.observed_generation);
            current_gen.is_some() && current_gen == observed_gen
        },
        DEFAULT_TIMEOUT,
    )
    .await;

    assert!(
        result.is_ok(),
        "Upgrade should have observed_generation updated"
    );
}

/// Test that ValkeyUpgrade finalizer is added.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_finalizer_added() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-finalizer").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create upgrade
    let upgrade = test_upgrade("finalizer-test", "nonexistent-cluster", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for finalizer to be added
    let result = wait_for_condition(
        &upgrade_api,
        "finalizer-test",
        |r| {
            r.finalizers()
                .iter()
                .any(|f| f == "valkey-operator.smoketurner.com/upgrade-finalizer")
        },
        SHORT_TIMEOUT,
    )
    .await;

    assert!(result.is_ok(), "Upgrade should have finalizer added");
}

/// Test that deleting a ValkeyUpgrade works correctly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_deletion() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-delete").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // Create upgrade
    let upgrade = test_upgrade("delete-test", "nonexistent-cluster", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for finalizer to be added
    wait_for_condition(
        &upgrade_api,
        "delete-test",
        |r| !r.finalizers().is_empty(),
        SHORT_TIMEOUT,
    )
    .await
    .expect("Upgrade should have finalizer");

    // Delete the upgrade
    upgrade_api
        .delete("delete-test", &DeleteParams::default())
        .await
        .expect("Failed to delete ValkeyUpgrade");

    // Wait for deletion to complete
    crate::wait::wait_for_deletion(&upgrade_api, "delete-test", DEFAULT_TIMEOUT)
        .await
        .expect("ValkeyUpgrade should be deleted");
}

/// Test that ValkeyUpgrade progress is tracked.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_upgrade_progress_tracking() {
    let cluster = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "upgrade-progress").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(client.clone(), &ns_name);

    // First create the target cluster
    let cluster = test_cluster("progress-target");
    cluster_api
        .create(&PostParams::default(), &cluster)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to be operational
    wait_for_operational(&cluster_api, "progress-target", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Create upgrade
    let upgrade = test_upgrade("progress-test", "progress-target", "9.0.1");
    upgrade_api
        .create(&PostParams::default(), &upgrade)
        .await
        .expect("Failed to create ValkeyUpgrade");

    // Wait for progress to be set
    let result = wait_for_condition(
        &upgrade_api,
        "progress-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| !s.progress.is_empty() || s.phase.is_terminal())
                .unwrap_or(false)
        },
        DEFAULT_TIMEOUT,
    )
    .await;

    assert!(
        result.is_ok(),
        "Upgrade should have progress tracking initialized"
    );

    // Verify progress format
    let upgrade_resource = upgrade_api
        .get("progress-test")
        .await
        .expect("Should get upgrade");
    if let Some(status) = upgrade_resource.status
        && status.phase != UpgradePhase::Failed
        && !status.progress.is_empty()
    {
        // Progress should be in "X/Y shards" format
        assert!(
            status.progress.contains("/"),
            "Progress should contain shard count"
        );
    }
}
