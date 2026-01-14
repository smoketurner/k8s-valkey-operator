//! Test fixtures and builder patterns for ValkeyCluster.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;
use valkey_operator::crd::{
    AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyCluster, ValkeyClusterSpec,
};

/// Builder for creating ValkeyCluster test fixtures.
///
/// # Example
/// ```
/// let resource = ValkeyClusterBuilder::new("test-resource")
///     .namespace("test-ns")
///     .masters(3)
///     .replicas_per_master(1)
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct ValkeyClusterBuilder {
    name: String,
    namespace: Option<String>,
    masters: i32,
    replicas_per_master: i32,
    issuer_name: String,
    secret_name: String,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    generation: Option<i64>,
    uid: Option<String>,
}

impl ValkeyClusterBuilder {
    /// Create a new builder with the given resource name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            masters: 3,
            replicas_per_master: 1,
            issuer_name: "test-issuer".to_string(),
            secret_name: "test-secret".to_string(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            generation: None,
            uid: None,
        }
    }

    /// Set the namespace for the resource.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the number of masters.
    pub fn masters(mut self, masters: i32) -> Self {
        self.masters = masters;
        self
    }

    /// Set the number of replicas per master.
    pub fn replicas_per_master(mut self, replicas: i32) -> Self {
        self.replicas_per_master = replicas;
        self
    }

    /// Set the cert-manager issuer name.
    pub fn issuer_name(mut self, name: impl Into<String>) -> Self {
        self.issuer_name = name.into();
        self
    }

    /// Set the auth secret name.
    pub fn secret_name(mut self, name: impl Into<String>) -> Self {
        self.secret_name = name.into();
        self
    }

    /// Add a label to the resource.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add multiple labels to the resource.
    pub fn labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels.extend(labels);
        self
    }

    /// Add an annotation to the resource.
    pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Set the generation (for testing status updates).
    pub fn generation(mut self, generation: i64) -> Self {
        self.generation = Some(generation);
        self
    }

    /// Set the UID (for testing owner references).
    pub fn uid(mut self, uid: impl Into<String>) -> Self {
        self.uid = Some(uid.into());
        self
    }

    /// Build the ValkeyCluster.
    pub fn build(self) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: self.namespace,
                labels: if self.labels.is_empty() {
                    None
                } else {
                    Some(self.labels)
                },
                annotations: if self.annotations.is_empty() {
                    None
                } else {
                    Some(self.annotations)
                },
                generation: self.generation,
                uid: self.uid,
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters: self.masters,
                replicas_per_master: self.replicas_per_master,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: self.issuer_name,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: self.secret_name,
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
            status: None,
        }
    }
}

impl Default for ValkeyClusterBuilder {
    fn default() -> Self {
        Self::new("test-resource")
    }
}

/// Create a minimal ValkeyCluster for testing.
pub fn minimal_resource(name: &str) -> ValkeyCluster {
    ValkeyClusterBuilder::new(name).build()
}

/// Create a ValkeyCluster with common test defaults.
pub fn test_resource(name: &str, namespace: &str) -> ValkeyCluster {
    ValkeyClusterBuilder::new(name)
        .namespace(namespace)
        .masters(3)
        .replicas_per_master(1)
        .generation(1)
        .uid(format!("test-uid-{}", name))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let resource = ValkeyClusterBuilder::new("test").build();
        assert_eq!(resource.metadata.name, Some("test".to_string()));
        assert_eq!(resource.spec.masters, 3);
        assert_eq!(resource.spec.replicas_per_master, 1);
    }

    #[test]
    fn test_builder_with_options() {
        let resource = ValkeyClusterBuilder::new("test")
            .namespace("my-ns")
            .masters(6)
            .replicas_per_master(2)
            .label("app", "test")
            .build();

        assert_eq!(resource.metadata.namespace, Some("my-ns".to_string()));
        assert_eq!(resource.spec.masters, 6);
        assert_eq!(resource.spec.replicas_per_master, 2);
        assert!(resource.metadata.labels.is_some());
    }
}
