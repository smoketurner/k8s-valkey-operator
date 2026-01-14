//! Test fixtures and builder patterns for ValkeyCluster.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use valkey_operator::crd::{ValkeyCluster, ValkeyClusterSpec};
use std::collections::BTreeMap;

/// Builder for creating ValkeyCluster test fixtures.
///
/// # Example
/// ```
/// let resource = ValkeyClusterBuilder::new("test-resource")
///     .namespace("test-ns")
///     .replicas(3)
///     .message("Hello, World!")
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct ValkeyClusterBuilder {
    name: String,
    namespace: Option<String>,
    replicas: i32,
    message: String,
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
            replicas: 1,
            message: "Hello from ValkeyCluster".to_string(),
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

    /// Set the number of replicas.
    pub fn replicas(mut self, replicas: i32) -> Self {
        self.replicas = replicas;
        self
    }

    /// Set the message field.
    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
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
                replicas: self.replicas,
                message: self.message,
                labels: BTreeMap::new(),
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
        .replicas(1)
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
        assert_eq!(resource.spec.replicas, 1);
    }

    #[test]
    fn test_builder_with_options() {
        let resource = ValkeyClusterBuilder::new("test")
            .namespace("my-ns")
            .replicas(3)
            .message("Custom message")
            .label("app", "test")
            .build();

        assert_eq!(resource.metadata.namespace, Some("my-ns".to_string()));
        assert_eq!(resource.spec.replicas, 3);
        assert_eq!(resource.spec.message, "Custom message");
        assert!(resource.metadata.labels.is_some());
    }
}
