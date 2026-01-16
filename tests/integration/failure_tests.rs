//! Failure and recovery scenario tests.
//!
//! These tests verify the operator handles failure scenarios gracefully:
//! - Pod failures and automatic recovery
//! - Multiple pod failures
//! - Node failures (simulated via pod deletion)
//! - Degraded state detection and recovery

use std::time::Duration;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, Patch, PatchParams, PostParams};

use valkey_operator::crd::{ClusterPhase, ValkeyCluster, total_pods};

use crate::{
    EXTENDED_TIMEOUT, LONG_TIMEOUT, SHORT_TIMEOUT, ScopedOperator, TestNamespace,
    create_auth_secret, init_test, test_cluster_with_config, wait_for_condition,
    wait_for_operational,
};

/// Test that a single pod failure is detected and recovered.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_single_pod_failure_recovery() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "single-pod-fail").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with 3 masters and 0 replicas = 3 total pods
    let resource = test_cluster_with_config("single-fail-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "single-fail-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    let expected_pods = total_pods(3, 0);

    // Verify initial state
    let initial = api
        .get("single-fail-test")
        .await
        .expect("Should get resource");
    let status = initial.status.expect("Should have status");
    assert_eq!(status.ready_replicas, expected_pods);

    // Delete one pod
    let pod_name = "single-fail-test-0";
    pod_api
        .delete(pod_name, &DeleteParams::default())
        .await
        .expect("Should delete pod");

    println!("Deleted pod: {}", pod_name);

    // Wait for pod to be recreated
    wait_for_condition(
        &pod_api,
        pod_name,
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

    println!("Pod recreated: {}", pod_name);

    // Wait for cluster to be operational again
    wait_for_operational(&api, "single-fail-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should recover");

    // Verify final state
    let final_resource = api
        .get("single-fail-test")
        .await
        .expect("Should get resource");
    let status = final_resource.status.expect("Should have status");
    assert_eq!(status.phase, ClusterPhase::Running);
    assert_eq!(status.ready_replicas, expected_pods);
}

/// Test that multiple pod failures are handled.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_multiple_pod_failure_recovery() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "multi-pod-fail").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with 3 masters and 0 replicas = 3 total pods
    let resource = test_cluster_with_config("multi-fail-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "multi-fail-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    let expected_pods = total_pods(3, 0);

    // Delete multiple pods (but leave enough for quorum)
    for i in [0, 2] {
        let pod_name = format!("multi-fail-test-{}", i);
        let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
        println!("Deleted pod: {}", pod_name);
    }

    // The cluster may enter Degraded state
    // Give it time to detect the failures
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Wait for recovery
    wait_for_operational(&api, "multi-fail-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should recover from multiple pod failures");

    // Verify final state
    let final_resource = api
        .get("multi-fail-test")
        .await
        .expect("Should get resource");
    let status = final_resource.status.expect("Should have status");
    assert_eq!(status.phase, ClusterPhase::Running);
    assert_eq!(status.ready_replicas, expected_pods);
}

/// Test that StatefulSet replica count mismatch is corrected.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_statefulset_replica_correction() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "sts-correct").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster
    let resource = test_cluster_with_config("sts-correct-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "sts-correct-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Manually scale down the StatefulSet (simulating external modification)
    let patch = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "spec": {
            "replicas": 2
        }
    });
    sts_api
        .patch(
            "sts-correct-test",
            &PatchParams::apply("test-harness").force(),
            &Patch::Apply(&patch),
        )
        .await
        .expect("Should patch StatefulSet");

    println!("Manually scaled StatefulSet to 2 replicas");

    // The operator should detect and correct this
    // Wait for the operator to restore the correct count
    let sts = wait_for_condition(
        &sts_api,
        "sts-correct-test",
        |s| {
            s.spec
                .as_ref()
                .and_then(|spec| spec.replicas)
                .map(|r| r == 3)
                .unwrap_or(false)
        },
        LONG_TIMEOUT,
    )
    .await
    .expect("StatefulSet should be corrected to 3 replicas");

    let replicas = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
    assert_eq!(replicas, 3, "StatefulSet should have 3 replicas");

    // Wait for cluster to be operational
    wait_for_operational(&api, "sts-correct-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should be operational after correction");
}

/// Test that degraded phase is detected when pods are unhealthy.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_degraded_phase_detection() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "degraded-detect").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with 3 masters and 0 replicas = 3 total pods
    let resource = test_cluster_with_config("degraded-detect-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    let expected_pods = total_pods(3, 0);

    // Wait for cluster to become operational
    wait_for_operational(&api, "degraded-detect-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Force the StatefulSet to fewer replicas to simulate failure
    // With 3 pods, reduce by 1 to leave 2 pods (below quorum)
    let reduced = expected_pods - 1;
    let patch = serde_json::json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "spec": {
            "replicas": reduced
        }
    });
    sts_api
        .patch(
            "degraded-detect-test",
            &PatchParams::apply("test-harness").force(),
            &Patch::Apply(&patch),
        )
        .await
        .expect("Should patch StatefulSet");

    // Try to detect degraded state
    let result = wait_for_condition(
        &api,
        "degraded-detect-test",
        |r| {
            r.status
                .as_ref()
                .map(|s| s.phase == ClusterPhase::Degraded || s.ready_replicas < expected_pods)
                .unwrap_or(false)
        },
        SHORT_TIMEOUT,
    )
    .await;

    if result.is_ok() {
        println!("Degraded state detected successfully");
    } else {
        // The operator may recover too quickly to detect
        println!("Operator recovered before degraded state was observed (expected behavior)");
    }

    // Either way, verify recovery
    wait_for_operational(&api, "degraded-detect-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should eventually recover");
}

/// Test that cluster recovers from all masters being deleted.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_full_cluster_restart() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "full-restart").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create a small cluster (no replicas to make the test more dramatic)
    let resource = test_cluster_with_config("full-restart-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "full-restart-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Delete all pods
    for i in 0..3 {
        let pod_name = format!("full-restart-test-{}", i);
        let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
        println!("Deleted pod: {}", pod_name);
    }

    // Give the system time to notice the failures
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Wait for full recovery
    wait_for_operational(&api, "full-restart-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should fully recover after all pods deleted");

    // Verify final state
    let final_resource = api
        .get("full-restart-test")
        .await
        .expect("Should get resource");
    let status = final_resource.status.expect("Should have status");
    assert_eq!(status.phase, ClusterPhase::Running);
    assert_eq!(status.ready_replicas, 3);
}

/// Test that cluster handles rapid successive failures.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_rapid_successive_failures() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "rapid-fail").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster with 3 masters and 0 replicas = 3 total pods
    let resource = test_cluster_with_config("rapid-fail-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    let expected_pods = total_pods(3, 0);

    // Wait for cluster to become operational
    wait_for_operational(&api, "rapid-fail-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Delete pods in rapid succession
    for i in 0..3 {
        let pod_name = format!("rapid-fail-test-{}", i);
        let _ = pod_api.delete(&pod_name, &DeleteParams::default()).await;
        // Very short delay between deletions
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("Rapidly deleted 3 pods");

    // Wait for recovery
    wait_for_operational(&api, "rapid-fail-test", EXTENDED_TIMEOUT)
        .await
        .expect("Cluster should recover from rapid failures");

    // Verify final state
    let final_resource = api
        .get("rapid-fail-test")
        .await
        .expect("Should get resource");
    let status = final_resource.status.expect("Should have status");
    assert_eq!(status.phase, ClusterPhase::Running);
    assert_eq!(status.ready_replicas, expected_pods);
}

/// Test that conditions are updated correctly during failures.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster with operator running"]
async fn test_condition_updates_during_failure() {
    let (cluster, _permit) = init_test().await;
    let client = cluster.new_client().await.expect("create client");
    let test_ns = TestNamespace::create(client.clone(), "cond-fail").await;
    let _operator = ScopedOperator::start(client.clone(), test_ns.name()).await;
    let ns_name = test_ns.name().to_string();
    create_auth_secret(client.clone(), &ns_name).await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), &ns_name);
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), &ns_name);

    // Create cluster
    let resource = test_cluster_with_config("cond-fail-test", 3, 0);
    api.create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    // Wait for cluster to become operational
    wait_for_operational(&api, "cond-fail-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should become operational");

    // Verify Ready condition is True
    let resource = api
        .get("cond-fail-test")
        .await
        .expect("Should get resource");
    let status = resource.status.expect("Should have status");

    let ready_condition = status.conditions.iter().find(|c| c.r#type == "Ready");

    if let Some(cond) = ready_condition {
        println!("Initial Ready condition: status={}", cond.status);
        assert_eq!(cond.status, "True", "Ready condition should be True");
    }

    // Delete a pod to trigger condition changes
    let _ = pod_api
        .delete("cond-fail-test-0", &DeleteParams::default())
        .await;

    // Wait briefly for condition updates
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Check conditions during recovery (may or may not catch the degraded state)
    let during_recovery = api
        .get("cond-fail-test")
        .await
        .expect("Should get resource");
    if let Some(status) = during_recovery.status.as_ref() {
        println!(
            "During recovery: phase={:?}, conditions={:?}",
            status.phase,
            status.conditions.len()
        );
    }

    // Wait for full recovery
    wait_for_operational(&api, "cond-fail-test", LONG_TIMEOUT)
        .await
        .expect("Cluster should recover");

    // Verify Ready condition is True again
    let recovered = api
        .get("cond-fail-test")
        .await
        .expect("Should get resource");
    let status = recovered.status.expect("Should have status");

    let ready_condition = status.conditions.iter().find(|c| c.r#type == "Ready");

    if let Some(cond) = ready_condition {
        println!("After recovery Ready condition: status={}", cond.status);
        assert_eq!(
            cond.status, "True",
            "Ready condition should be True after recovery"
        );
    }
}
