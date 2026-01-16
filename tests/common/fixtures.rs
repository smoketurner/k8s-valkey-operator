//! Test fixtures and builder patterns for ValkeyCluster and ValkeyUpgrade.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;
use valkey_operator::crd::{
    AuthSpec, ClusterReference, IssuerRef, PersistenceSpec, ResourceLimitsSpec, ResourceRequirementsSpec, ResourceSpec, SecretKeyRef, TlsSpec, ValkeyCluster,
    ValkeyClusterSpec, ValkeyUpgrade, ValkeyUpgradeSpec,
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
    persistence_enabled: bool,
    persistence_size: Option<String>,
}

impl ValkeyClusterBuilder {
    /// Create a new builder with the given resource name.
    ///
    /// Defaults match integration test requirements:
    /// - 3 masters, 0 replicas per master
    /// - TLS issuer: "selfsigned-issuer"
    /// - Auth secret: "valkey-auth"
    /// - Persistence: disabled
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            masters: 3,
            replicas_per_master: 0,
            issuer_name: String::from("selfsigned-issuer"),
            secret_name: String::from("valkey-auth"),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            generation: None,
            uid: None,
            persistence_enabled: false,
            persistence_size: None,
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
    #[allow(dead_code)] // May be used by tests
    pub fn issuer_name(mut self, name: impl Into<String>) -> Self {
        self.issuer_name = name.into();
        self
    }

    /// Set the auth secret name.
    #[allow(dead_code)] // May be used by tests
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
    #[allow(dead_code)] // May be used by tests
    pub fn labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels.extend(labels);
        self
    }

    /// Add an annotation to the resource.
    #[allow(dead_code)] // May be used by tests
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

    /// Enable persistence with optional size.
    pub fn persistence(mut self, enabled: bool, size: Option<impl Into<String>>) -> Self {
        self.persistence_enabled = enabled;
        self.persistence_size = size.map(|s| s.into());
        self
    }

    /// Build the ValkeyCluster.
    pub fn build(self) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: self.namespace,
                labels: (!self.labels.is_empty()).then_some(self.labels),
                annotations: (!self.annotations.is_empty()).then_some(self.annotations),
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
                        kind: "ClusterIssuer".to_string(),
                        group: "cert-manager.io".to_string(),
                    },
                    duration: "2160h".to_string(),
                    renew_before: "360h".to_string(),
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: self.secret_name,
                        key: "password".to_string(),
                    },
                },
                persistence: if self.persistence_enabled {
                    PersistenceSpec {
                        enabled: true,
                        size: self.persistence_size.unwrap_or_else(|| "1Gi".to_string()),
                        ..Default::default()
                    }
                } else {
                    PersistenceSpec {
                        enabled: false,
                        ..Default::default()
                    }
                },
                // Use minimal resources for integration tests to avoid resource exhaustion
                resources: ResourceRequirementsSpec {
                    requests: ResourceSpec {
                        cpu: "50m".to_string(),
                        memory: "64Mi".to_string(),
                    },
                    limits: ResourceLimitsSpec {
                        cpu: "200m".to_string(),
                        memory: "256Mi".to_string(),
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
#[allow(dead_code)] // May be used by integration tests
pub fn minimal_resource(name: &str) -> ValkeyCluster {
    ValkeyClusterBuilder::new(name).build()
}

/// Create a ValkeyCluster with common test defaults.
#[allow(dead_code)] // May be used by integration tests
pub fn test_resource(name: &str, namespace: &str) -> ValkeyCluster {
    ValkeyClusterBuilder::new(name)
        .namespace(namespace)
        .masters(3)
        .replicas_per_master(1)
        .generation(1)
        .uid(format!("test-uid-{name}"))
        .build()
}

// ============================================================================
// ValkeyUpgrade Builder
// ============================================================================

/// Builder for creating ValkeyUpgrade test fixtures.
///
/// # Example
/// ```
/// let upgrade = ValkeyUpgradeBuilder::new("test-upgrade")
///     .cluster("my-cluster")
///     .target_version("9.0.1")
///     .build();
/// ```
pub struct ValkeyUpgradeBuilder {
    name: String,
    namespace: Option<String>,
    cluster_name: String,
    cluster_namespace: Option<String>,
    target_version: String,
    replication_sync_timeout: Option<u64>,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
}

impl ValkeyUpgradeBuilder {
    /// Create a new builder with the given upgrade name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            namespace: None,
            cluster_name: String::new(),
            cluster_namespace: None,
            target_version: String::new(),
            replication_sync_timeout: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        }
    }

    /// Set the namespace for the upgrade resource.
    #[allow(dead_code)] // May be used by tests
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Set the target cluster name (required).
    pub fn cluster(mut self, cluster_name: impl Into<String>) -> Self {
        self.cluster_name = cluster_name.into();
        self
    }

    /// Set the target cluster namespace (optional, defaults to same namespace as upgrade).
    #[allow(dead_code)] // May be used by tests
    pub fn cluster_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.cluster_namespace = Some(namespace.into());
        self
    }

    /// Set the target Valkey version (required).
    pub fn target_version(mut self, version: impl Into<String>) -> Self {
        self.target_version = version.into();
        self
    }

    /// Set the replication sync timeout in seconds.
    #[allow(dead_code)] // May be used by tests
    pub fn replication_sync_timeout(mut self, timeout: u64) -> Self {
        self.replication_sync_timeout = Some(timeout);
        self
    }

    /// Add a label to the resource.
    #[allow(dead_code)] // May be used by tests
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add multiple labels to the resource.
    #[allow(dead_code)] // May be used by tests
    pub fn labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels.extend(labels);
        self
    }

    /// Add an annotation to the resource.
    #[allow(dead_code)] // May be used by tests
    pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Build the ValkeyUpgrade.
    pub fn build(self) -> ValkeyUpgrade {
        ValkeyUpgrade {
            metadata: ObjectMeta {
                name: Some(self.name),
                namespace: self.namespace,
                labels: (!self.labels.is_empty()).then_some(self.labels),
                annotations: (!self.annotations.is_empty()).then_some(self.annotations),
                ..Default::default()
            },
            spec: ValkeyUpgradeSpec {
                cluster_ref: ClusterReference {
                    name: self.cluster_name,
                    namespace: self.cluster_namespace,
                },
                target_version: self.target_version,
                replication_sync_timeout_seconds: self.replication_sync_timeout.unwrap_or(300),
            },
            status: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let resource = ValkeyClusterBuilder::new("test").build();
        assert_eq!(resource.metadata.name.as_deref(), Some("test"));
        assert_eq!(resource.spec.masters, 3);
        assert_eq!(resource.spec.replicas_per_master, 0);
        assert_eq!(resource.spec.tls.issuer_ref.name, "selfsigned-issuer");
        assert_eq!(resource.spec.auth.secret_ref.name, "valkey-auth");
        assert!(!resource.spec.persistence.enabled);
    }

    #[test]
    fn test_builder_with_options() {
        let resource = ValkeyClusterBuilder::new("test")
            .namespace("my-ns")
            .masters(6)
            .replicas_per_master(2)
            .label("app", "test")
            .build();

        assert_eq!(resource.metadata.namespace.as_deref(), Some("my-ns"));
        assert_eq!(resource.spec.masters, 6);
        assert_eq!(resource.spec.replicas_per_master, 2);
        assert!(resource.metadata.labels.is_some());
    }

    #[test]
    fn test_builder_with_persistence() {
        let resource = ValkeyClusterBuilder::new("test")
            .persistence(true, Some("2Gi"))
            .build();

        assert!(resource.spec.persistence.enabled);
        assert_eq!(resource.spec.persistence.size, "2Gi");
    }
}
