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

//! Integration test harness for my-operator.
//!
//! These tests require a running Kubernetes cluster (e.g., kind, minikube).
//! Tests are marked with #[ignore] and must be run explicitly:
//!
//! ```bash
//! # Run all integration tests
//! cargo test --test integration -- --ignored
//!
//! # Run specific test
//! cargo test --test integration test_myresource_lifecycle -- --ignored
//! ```
//!
//! The CRD must be installed before running tests: `make install-crd`

mod assertions;
mod cluster;
mod functional_tests;
mod namespace;
mod operator;
mod scaling_tests;
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

/// Test that the MyResource CRD is installed.
#[tokio::test]
#[ignore = "requires Kubernetes cluster"]
async fn test_crd_installed() {
    let (_cluster, client) = init_test().await;

    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube::api::Api;

    let crds: Api<CustomResourceDefinition> = Api::all(client);
    let crd = crds
        .get("myresources.myoperator.example.com")
        .await
        .expect("CRD should be installed. Run `make install-crd` first.");

    assert_eq!(
        crd.spec.group, "myoperator.example.com",
        "CRD group should match"
    );
}

/// Test creating and deleting a MyResource in an isolated namespace.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Kubernetes cluster"]
async fn test_myresource_lifecycle() {
    use kube::api::{Api, PostParams};
    use my_operator::crd::MyResource;

    let (_cluster, client) = init_test().await;
    let test_ns = namespace::TestNamespace::create(client.clone(), "lifecycle-test").await;

    let api: Api<MyResource> = Api::namespaced(client.clone(), test_ns.name());

    // Create a MyResource
    let resource = serde_json::from_value(serde_json::json!({
        "apiVersion": "myoperator.example.com/v1alpha1",
        "kind": "MyResource",
        "metadata": {
            "name": "test-resource"
        },
        "spec": {
            "replicas": 1,
            "message": "Hello from integration test"
        }
    }))
    .expect("Failed to parse MyResource");

    let created = api
        .create(&PostParams::default(), &resource)
        .await
        .expect("Failed to create MyResource");

    assert_eq!(created.metadata.name, Some("test-resource".to_string()));
    assert_eq!(created.spec.replicas, 1);

    // Clean up happens automatically when test_ns is dropped
}
