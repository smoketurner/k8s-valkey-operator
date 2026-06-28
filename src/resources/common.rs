//! Common resource generation utilities.
//!
//! Provides functions for creating standard Kubernetes resource metadata
//! including labels, annotations, and owner references.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use std::collections::BTreeMap;

use crate::crd::ValkeyCluster;

/// Standard labels applied to all managed resources.
///
/// These labels follow Kubernetes recommended labeling conventions:
/// - `app.kubernetes.io/name`: The cluster name
/// - `app.kubernetes.io/managed-by`: "valkey-operator"
/// - `app.kubernetes.io/component`: "valkeycluster"
/// - `app.kubernetes.io/instance`: The cluster name (for Helm compatibility)
///
/// User-defined labels from the spec are merged in first, but cannot override
/// operator-managed standard labels. This protects the pod selector identity
/// (which always uses the cluster name) from being broken by user input.
pub fn standard_labels(resource: &ValkeyCluster) -> BTreeMap<String, String> {
    let name = resource.name_any();

    // Start with user-defined labels so operator labels can overwrite them.
    let mut labels: BTreeMap<String, String> = resource
        .spec
        .labels
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    labels.insert("app.kubernetes.io/name".to_string(), name.clone());
    labels.insert("app.kubernetes.io/instance".to_string(), name);
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "valkey-operator".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "valkeycluster".to_string(),
    );

    labels
}

/// Standard annotations applied to managed resources.
///
/// Merges user-defined annotations from the spec.
pub fn standard_annotations(resource: &ValkeyCluster) -> BTreeMap<String, String> {
    let mut annotations = BTreeMap::new();

    // Merge user-defined annotations
    for (key, value) in &resource.spec.annotations {
        annotations.insert(key.clone(), value.clone());
    }

    annotations
}

/// Create owner reference for a ValkeyCluster.
///
/// This ensures that all created resources are garbage collected
/// when the ValkeyCluster is deleted.
///
/// # Panics
/// This function will return an invalid owner reference if the resource
/// has no UID.
pub fn owner_reference(resource: &ValkeyCluster) -> OwnerReference {
    OwnerReference {
        api_version: "valkey-operator.smoketurner.com/v1alpha1".to_string(),
        kind: "ValkeyCluster".to_string(),
        name: resource.name_any(),
        uid: resource.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Pod selector labels (subset of standard labels used for service selectors).
pub fn pod_selector_labels(resource: &ValkeyCluster) -> BTreeMap<String, String> {
    let name = resource.name_any();
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), name.clone());
    labels.insert("app.kubernetes.io/instance".to_string(), name);
    labels
}

/// Generate the headless service name for a ValkeyCluster.
pub fn headless_service_name(resource: &ValkeyCluster) -> String {
    format!("{}-headless", resource.name_any())
}

/// Generate the client service name for a ValkeyCluster.
pub fn client_service_name(resource: &ValkeyCluster) -> String {
    resource.name_any()
}

/// Generate the read service name for a ValkeyCluster.
pub fn read_service_name(resource: &ValkeyCluster) -> String {
    format!("{}-read", resource.name_any())
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
mod tests {
    use super::*;
    use crate::crd::{AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyClusterSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn test_resource(name: &str) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "test-issuer".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-secret".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_standard_labels() {
        let resource = test_resource("my-cluster");
        let labels = standard_labels(&resource);

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"valkey-operator".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"valkeycluster".to_string())
        );
    }

    #[test]
    fn test_standard_labels_user_cannot_override_operator_labels() {
        // User attempts to override every operator-managed label.
        let mut resource = test_resource("my-cluster");
        resource
            .spec
            .labels
            .insert("app.kubernetes.io/name".to_string(), "evil".to_string());
        resource.spec.labels.insert(
            "app.kubernetes.io/instance".to_string(),
            "evil-instance".to_string(),
        );
        resource.spec.labels.insert(
            "app.kubernetes.io/managed-by".to_string(),
            "imposter".to_string(),
        );
        resource.spec.labels.insert(
            "app.kubernetes.io/component".to_string(),
            "not-a-cluster".to_string(),
        );

        let labels = standard_labels(&resource);

        // Operator labels must win — otherwise the selector/template label
        // mismatch breaks StatefulSet creation (see issue #51).
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"valkey-operator".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"valkeycluster".to_string())
        );
    }

    #[test]
    fn test_standard_labels_user_labels_preserved() {
        // User-defined non-reserved labels must coexist with operator labels.
        let mut resource = test_resource("my-cluster");
        resource
            .spec
            .labels
            .insert("team".to_string(), "platform".to_string());
        resource
            .spec
            .labels
            .insert("environment".to_string(), "production".to_string());

        let labels = standard_labels(&resource);

        assert_eq!(labels.get("team"), Some(&"platform".to_string()));
        assert_eq!(labels.get("environment"), Some(&"production".to_string()));
        // Operator labels still present alongside user labels.
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
    }

    #[test]
    fn test_owner_reference() {
        let resource = test_resource("my-cluster");
        let owner_ref = owner_reference(&resource);

        assert_eq!(owner_ref.name, "my-cluster");
        assert_eq!(owner_ref.kind, "ValkeyCluster");
        assert_eq!(
            owner_ref.api_version,
            "valkey-operator.smoketurner.com/v1alpha1"
        );
        assert_eq!(owner_ref.controller, Some(true));
    }

    #[test]
    fn test_service_names() {
        let resource = test_resource("my-cluster");

        assert_eq!(headless_service_name(&resource), "my-cluster-headless");
        assert_eq!(client_service_name(&resource), "my-cluster");
    }
}
