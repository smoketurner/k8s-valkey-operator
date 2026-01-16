//! Common test fixtures and helpers.
//!
//! This module provides shared utilities for integration tests.

use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, PostParams};

use crate::common::fixtures::{ValkeyClusterBuilder, ValkeyUpgradeBuilder};
use crate::{SharedTestCluster, ensure_cluster_crd_installed, ensure_upgrade_crd_installed};

// ============================================================
// Timeout Constants
// ============================================================

/// Short timeout for quick operations.
pub const SHORT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for most operations.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// Long timeout for cluster operations.
pub const LONG_TIMEOUT: Duration = Duration::from_secs(180);

/// Extended timeout for large cluster operations.
pub const EXTENDED_TIMEOUT: Duration = Duration::from_secs(240);

/// Extended timeout for upgrade operations.
pub const UPGRADE_TIMEOUT: Duration = Duration::from_secs(300);

// ============================================================
// Test Initialization
// ============================================================

/// Initialize tracing and ensure ValkeyCluster CRD is installed.
///
/// Returns the shared test cluster instance.
pub async fn init_test() -> std::sync::Arc<SharedTestCluster> {
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

    cluster
}

/// Initialize tracing and ensure both CRDs are installed.
///
/// Returns the shared test cluster instance.
pub async fn init_test_with_upgrade() -> std::sync::Arc<SharedTestCluster> {
    let cluster = init_test().await;

    ensure_upgrade_crd_installed(&cluster)
        .await
        .expect("Failed to install ValkeyUpgrade CRD");

    cluster
}

// ============================================================
// ValkeyCluster Builders
// ============================================================

/// Create a minimal ValkeyCluster for testing (3 masters, 0 replicas).
pub fn test_cluster(name: &str) -> valkey_operator::crd::ValkeyCluster {
    ValkeyClusterBuilder::new(name).build()
}

/// Create a ValkeyCluster with specified replicas per master (3 masters).
pub fn test_cluster_with_replicas(
    name: &str,
    replicas_per_master: i32,
) -> valkey_operator::crd::ValkeyCluster {
    ValkeyClusterBuilder::new(name)
        .replicas_per_master(replicas_per_master)
        .build()
}

/// Create a ValkeyCluster with custom master and replica counts.
pub fn test_cluster_with_config(
    name: &str,
    masters: i32,
    replicas_per_master: i32,
) -> valkey_operator::crd::ValkeyCluster {
    ValkeyClusterBuilder::new(name)
        .masters(masters)
        .replicas_per_master(replicas_per_master)
        .build()
}

// ============================================================
// ValkeyUpgrade Builders
// ============================================================

/// Create a ValkeyUpgrade resource.
pub fn test_upgrade(
    name: &str,
    cluster_name: &str,
    target_version: &str,
) -> valkey_operator::crd::ValkeyUpgrade {
    ValkeyUpgradeBuilder::new(name)
        .cluster(cluster_name)
        .target_version(target_version)
        .build()
}

// ============================================================
// Auth Secret Helpers
// ============================================================

/// Create the default auth secret used by test ValkeyCluster resources.
///
/// Most tests reference a secret named "valkey-auth" for authentication.
/// This function creates that secret in the specified namespace.
pub async fn create_auth_secret(client: kube::Client, namespace: &str) {
    create_auth_secret_with_name(client, namespace, "valkey-auth").await;
}

/// Create an auth secret with a custom name.
pub async fn create_auth_secret_with_name(client: kube::Client, namespace: &str, name: &str) {
    let secrets: Api<Secret> = Api::namespaced(client, namespace);

    let secret: Secret = serde_json::from_value(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name
        },
        "type": "Opaque",
        "stringData": {
            "password": "test-password-123"
        }
    }))
    .expect("Failed to create secret");

    // Ignore errors if secret already exists
    let _ = secrets.create(&PostParams::default(), &secret).await;
}
