//! Common test fixtures and helpers.
//!
//! This module provides shared utilities for integration tests.

use k8s_openapi::api::core::v1::Secret;
use kube::api::{Api, PostParams};

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
