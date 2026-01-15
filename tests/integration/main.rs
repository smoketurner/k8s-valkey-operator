// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice,
    dead_code,
    unused_variables
)]

//! Integration test harness for valkey-operator.
//!
//! These tests require a running Kubernetes cluster (e.g., kind, minikube).
//! Tests are marked with #[ignore] and must be run explicitly:
//!
//! ```bash
//! # Run all integration tests
//! cargo test --test integration -- --ignored
//!
//! # Run specific test
//! cargo test --test integration test_valkeycluster_lifecycle -- --ignored
//! ```
//!
//! The CRD must be installed before running tests: `make install-crd`

mod assertions;
mod cluster;
mod functional_tests;
mod namespace;
mod operator;
mod scaling_tests;
mod upgrade_tests;
mod wait;

use cluster::SharedTestCluster;
use std::sync::Arc;

/// Initialize the test cluster and return the shared cluster and a fresh client.
///
/// This function should be called at the start of each test to get
/// access to the shared test cluster.
pub async fn init_test() -> (Arc<SharedTestCluster>, kube::Client) {
    let cluster = SharedTestCluster::get().await;
    let client = cluster.new_client().await;
    (cluster, client)
}

/// Basic connectivity test to verify the test harness works.
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_cluster_connectivity() {
    let (_cluster, client) = init_test().await;

    // Verify we can list namespaces
    use kube::api::{Api, ListParams};
    let namespaces: Api<k8s_openapi::api::core::v1::Namespace> = Api::all(client);
    let ns_list = namespaces
        .list(&ListParams::default())
        .await
        .expect("Failed to list namespaces");

    assert!(
        !ns_list.items.is_empty(),
        "Should have at least one namespace"
    );
}

/// Test that the ValkeyCluster CRD is installed.
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_crd_installed() {
    let (_cluster, client) = init_test().await;

    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube::api::Api;

    let crds: Api<CustomResourceDefinition> = Api::all(client);
    let crd = crds
        .get("valkeyclusters.valkey-operator.smoketurner.com")
        .await
        .expect("CRD should be installed. Run `make install-crd` first.");

    assert_eq!(
        crd.spec.group, "valkey-operator.smoketurner.com",
        "CRD group should match"
    );
}

/// Test creating and deleting a ValkeyCluster in an isolated namespace.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_valkeycluster_lifecycle() {
    use kube::api::{Api, PostParams};
    use valkey_operator::crd::ValkeyCluster;

    let (_cluster, client) = init_test().await;
    let test_ns = namespace::TestNamespace::create(client.clone(), "lifecycle-test").await;

    let api: Api<ValkeyCluster> = Api::namespaced(client.clone(), test_ns.name());

    // Create a ValkeyCluster with the new spec structure
    let resource = serde_json::from_value(serde_json::json!({
        "apiVersion": "valkey-operator.smoketurner.com/v1alpha1",
        "kind": "ValkeyCluster",
        "metadata": {
            "name": "test-resource"
        },
        "spec": {
            "masters": 3,
            "replicasPerMaster": 1,
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
    .expect("Failed to parse ValkeyCluster");

    let created = api
        .create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create ValkeyCluster");

    assert_eq!(created.metadata.name, Some("test-resource".to_string()));
    assert_eq!(created.spec.masters, 3);
    assert_eq!(created.spec.replicas_per_master, 1);

    // Clean up happens automatically when test_ns is dropped
}
