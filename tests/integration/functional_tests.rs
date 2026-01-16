//! Functional tests for ValkeyCluster lifecycle operations.
//!
//! These tests verify the core functionality of the operator including:
//! - Resource creation and owned resource generation
//! - Spec updates and reconciliation
//! - Resource deletion and cleanup

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{Api, DeleteParams, PostParams};

use valkey_operator::crd::{ClusterPhase, ValkeyCluster};

use crate::assertions::{
    assert_has_owner_reference, assert_headless_service_exists, assert_service_exists,
    assert_statefulset_replicas, assert_valkeycluster_phase,
};
use crate::{
    DEFAULT_TIMEOUT, LONG_TIMEOUT, SHORT_TIMEOUT, ScopedOperator, TestNamespace,
    create_auth_secret, init_test, test_cluster_with_config, wait, wait_for_condition,
    wait_for_operational, wait_for_phase,
};

/// Test that creating a ValkeyCluster creates all expected owned resources.
/// This consolidated test verifies StatefulSet, Services, and PDB creation in one test
/// to reduce resource usage while maintaining test coverage.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_creates_owned_resources() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "creates-resources").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &ns_name);
    let pdb_api: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), &ns_name);

    // Create resource with 3 masters and 0 replicas per master = 3 total pods
    let resource = test_cluster_with_config("test-resources", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for resource to start creating
    wait_for_phase(
        &api,
        "test-resources",
        ClusterPhase::Creating,
        SHORT_TIMEOUT,
    )
    .await
    .expect("Resource should reach Creating phase");

    // Verify StatefulSet is created
    wait::wait_for_resource(&sts_api, "test-resources", SHORT_TIMEOUT)
        .await
        .expect("StatefulSet should be created");
    assert_statefulset_replicas(client.clone(), &ns_name, "test-resources", 3).await;
    let statefulset = sts_api.get("test-resources").await.unwrap();
    assert_has_owner_reference(&statefulset, "test-resources", "ValkeyCluster");

    // Verify Services are created
    wait::wait_for_resource(&svc_api, "test-resources-headless", SHORT_TIMEOUT)
        .await
        .expect("Headless service should be created");
    wait::wait_for_resource(&svc_api, "test-resources", SHORT_TIMEOUT)
        .await
        .expect("Client service should be created");
    assert_headless_service_exists(client.clone(), &ns_name, "test-resources-headless").await;
    assert_service_exists(client.clone(), &ns_name, "test-resources").await;
    let headless_svc = svc_api.get("test-resources-headless").await.unwrap();
    assert_has_owner_reference(&headless_svc, "test-resources", "ValkeyCluster");

    // Verify PDB is created
    wait::wait_for_resource(&pdb_api, "test-resources", SHORT_TIMEOUT)
        .await
        .expect("PDB should be created");
    let pdb = pdb_api.get("test-resources").await.unwrap();
    assert_has_owner_reference(&pdb, "test-resources", "ValkeyCluster");
}

/// Test that a ValkeyCluster transitions through phases correctly and reaches Running.
/// This consolidated test verifies phase progression and final Running state in one test.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_phase_transitions_to_running() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "phase-transitions").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_cluster_with_config("test-phases", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Should transition to Creating
    wait_for_phase(&api, "test-phases", ClusterPhase::Creating, SHORT_TIMEOUT)
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
    assert_valkeycluster_phase(
        client.clone(),
        &ns_name,
        "test-phases",
        ClusterPhase::Running,
    )
    .await;

    // Verify cluster status fields
    let resource = api.get("test-phases").await.expect("Should get resource");
    let status = resource.status.expect("Should have status");
    assert_eq!(status.ready_replicas, 3, "Should have 3 ready replicas");
}

/// Test that deleting a ValkeyCluster triggers cleanup.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_resource_deletion() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "deletion-test").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_cluster_with_config("test-delete", 3, 0);
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
    wait::wait_for_deletion(&sts_api, "test-delete", DEFAULT_TIMEOUT)
        .await
        .expect("StatefulSet should be garbage collected after ValkeyCluster deletion");
}

/// Test that validation rejects invalid master counts.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_validation_rejects_invalid_masters() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "validation-test").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Try to create resource with invalid masters count (2 < minimum of 3)
    let invalid_resource = test_cluster_with_config("invalid-masters", 2, 0);
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
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "observed-gen").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_cluster_with_config("test-gen", 3, 0);
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
    assert_eq!(
        observed, generation,
        "observed_generation should match generation"
    );
}

/// Test that connection endpoint is populated in status.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_connection_endpoint_populated() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "conn-endpoint").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);

    // Create resource
    let resource = test_cluster_with_config("test-conn", 3, 0);
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
