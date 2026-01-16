//! StatefulSet generation for Valkey clusters.
//!
//! Creates a StatefulSet with proper configuration for Valkey cluster operation:
//! - Stable network identity via headless service
//! - Persistent storage for data durability
//! - TLS certificate mounts
//! - Two-phase probe strategy
//! - Pod anti-affinity for high availability
//! - Graceful shutdown with BGSAVE

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy};
use k8s_openapi::api::core::v1::{
    Capabilities, Container, ContainerPort, EmptyDirVolumeSource, EnvVar, EnvVarSource, ExecAction,
    Lifecycle, LifecycleHandler, LocalObjectReference, ObjectFieldSelector, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PodSecurityContext, PodSpec, PodTemplateSpec, Probe,
    ResourceRequirements, SecretKeySelector, SecurityContext, Toleration, Volume, VolumeMount,
    VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::ResourceExt;
use std::collections::BTreeMap;

use crate::crd::{ValkeyCluster, total_pods};
use crate::resources::common::{
    headless_service_name, owner_reference, pod_selector_labels, standard_annotations,
    standard_labels,
};

/// Valkey client port
const CLIENT_PORT: i32 = 6379;
/// Valkey cluster bus port (client port + 10000)
const CLUSTER_BUS_PORT: i32 = 16379;
/// Valkey user ID in the official container image
const VALKEY_USER_ID: i64 = 999;
/// Default termination grace period in seconds (allows time for BGSAVE)
const TERMINATION_GRACE_PERIOD: i64 = 60;

/// Generate a StatefulSet for a ValkeyCluster.
///
/// The StatefulSet provides:
/// - Stable network identity (pod-0, pod-1, etc.)
/// - Persistent storage via PVC templates
/// - Ordered, graceful deployment and scaling
/// - Pod anti-affinity to spread masters across nodes
pub fn generate_statefulset(resource: &ValkeyCluster) -> StatefulSet {
    let name = resource.name_any();
    let namespace = resource.namespace();
    let labels = standard_labels(resource);
    let annotations = standard_annotations(resource);
    let replicas = total_pods(resource.spec.masters, resource.spec.replicas_per_master);

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: namespace.clone(),
            labels: Some(labels.clone()),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations.clone())
            },
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(replicas),
            service_name: Some(headless_service_name(resource)),
            selector: LabelSelector {
                match_labels: Some(pod_selector_labels(resource)),
                ..Default::default()
            },
            // Parallel pod management for faster cluster startup
            pod_management_policy: Some("Parallel".to_string()),
            // OnDelete strategy for operator-controlled rolling updates
            update_strategy: Some(StatefulSetUpdateStrategy {
                type_: Some("OnDelete".to_string()),
                ..Default::default()
            }),
            template: generate_pod_template(resource, &labels, &annotations),
            volume_claim_templates: if resource.spec.persistence.enabled {
                Some(vec![generate_pvc_template(resource)])
            } else {
                None
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate the pod template for the StatefulSet.
fn generate_pod_template(
    resource: &ValkeyCluster,
    labels: &BTreeMap<String, String>,
    annotations: &BTreeMap<String, String>,
) -> PodTemplateSpec {
    // Convert custom Toleration to k8s Toleration
    let tolerations = convert_tolerations(&resource.spec.scheduling.tolerations);

    // Convert pull_secrets to LocalObjectReference
    let image_pull_secrets = convert_pull_secrets(&resource.spec.image.pull_secrets);

    // Convert node_selector to Option
    let node_selector = if resource.spec.scheduling.node_selector.is_empty() {
        None
    } else {
        Some(resource.spec.scheduling.node_selector.clone())
    };

    PodTemplateSpec {
        metadata: Some(ObjectMeta {
            labels: Some(labels.clone()),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations.clone())
            },
            ..Default::default()
        }),
        spec: Some(PodSpec {
            termination_grace_period_seconds: Some(TERMINATION_GRACE_PERIOD),
            security_context: Some(generate_pod_security_context()),
            affinity: Some(generate_affinity(resource)),
            containers: vec![generate_valkey_container(resource)],
            volumes: Some(generate_volumes(resource)),
            // Apply scheduling constraints from spec
            node_selector,
            tolerations,
            image_pull_secrets,
            // Use the default service account - Valkey pods don't need K8s API access
            ..Default::default()
        }),
    }
}

/// Convert CRD tolerations to k8s-openapi Tolerations.
fn convert_tolerations(crd_tolerations: &[crate::crd::Toleration]) -> Option<Vec<Toleration>> {
    if crd_tolerations.is_empty() {
        return None;
    }

    Some(
        crd_tolerations
            .iter()
            .map(|t| Toleration {
                key: t.key.clone(),
                operator: t.operator.clone(),
                value: t.value.clone(),
                effect: t.effect.clone(),
                toleration_seconds: t.toleration_seconds,
            })
            .collect(),
    )
}

/// Convert pull_secrets list to LocalObjectReference list.
fn convert_pull_secrets(pull_secrets: &[String]) -> Option<Vec<LocalObjectReference>> {
    if pull_secrets.is_empty() {
        return None;
    }

    Some(
        pull_secrets
            .iter()
            .map(|name| LocalObjectReference { name: name.clone() })
            .collect(),
    )
}

/// Generate pod security context.
fn generate_pod_security_context() -> PodSecurityContext {
    PodSecurityContext {
        run_as_non_root: Some(true),
        run_as_user: Some(VALKEY_USER_ID),
        fs_group: Some(VALKEY_USER_ID),
        // Use RuntimeDefault seccomp profile
        seccomp_profile: Some(k8s_openapi::api::core::v1::SeccompProfile {
            type_: "RuntimeDefault".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate pod affinity rules to spread masters across nodes.
fn generate_affinity(resource: &ValkeyCluster) -> k8s_openapi::api::core::v1::Affinity {
    let name = resource.name_any();
    let topology_key = resource.spec.scheduling.topology_key.clone();

    // Only add anti-affinity if spread_masters is enabled
    if !resource.spec.scheduling.spread_masters {
        return k8s_openapi::api::core::v1::Affinity::default();
    }

    // Use preferred anti-affinity (soft constraint) to spread pods
    // This allows scheduling to proceed even if not all nodes are unique
    k8s_openapi::api::core::v1::Affinity {
        pod_anti_affinity: Some(k8s_openapi::api::core::v1::PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                k8s_openapi::api::core::v1::WeightedPodAffinityTerm {
                    weight: 100,
                    pod_affinity_term: k8s_openapi::api::core::v1::PodAffinityTerm {
                        label_selector: Some(LabelSelector {
                            match_labels: Some({
                                let mut labels = BTreeMap::new();
                                labels.insert("app.kubernetes.io/name".to_string(), name);
                                labels
                            }),
                            ..Default::default()
                        }),
                        topology_key,
                        ..Default::default()
                    },
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate the main Valkey container.
fn generate_valkey_container(resource: &ValkeyCluster) -> Container {
    Container {
        name: "valkey".to_string(),
        image: Some(format!(
            "{}:{}",
            resource.spec.image.repository, resource.spec.image.tag
        )),
        image_pull_policy: Some(resource.spec.image.pull_policy.clone()),
        ports: Some(vec![
            ContainerPort {
                container_port: CLIENT_PORT,
                name: Some("client".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            },
            ContainerPort {
                container_port: CLUSTER_BUS_PORT,
                name: Some("cluster-bus".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            },
        ]),
        env: Some(generate_env_vars(resource)),
        resources: Some(generate_resource_requirements(resource)),
        volume_mounts: Some(generate_volume_mounts(resource)),
        security_context: Some(generate_container_security_context()),
        // Two-phase probe strategy
        startup_probe: Some(generate_startup_probe()),
        liveness_probe: Some(generate_liveness_probe()),
        readiness_probe: Some(generate_readiness_probe()),
        lifecycle: Some(generate_lifecycle()),
        ..Default::default()
    }
}

/// Generate environment variables for Valkey configuration.
///
/// Uses VALKEY_EXTRA_FLAGS for cluster configuration instead of ConfigMap.
fn generate_env_vars(resource: &ValkeyCluster) -> Vec<EnvVar> {
    let name = resource.name_any();
    let namespace = resource
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let headless_svc = headless_service_name(resource);

    let mut env = vec![
        // Pod information via downward API
        EnvVar {
            name: "POD_NAME".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "metadata.name".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "POD_IP".to_string(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: "status.podIP".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
        EnvVar {
            name: "NAMESPACE".to_string(),
            value: Some(namespace.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "CLUSTER_NAME".to_string(),
            value: Some(name.clone()),
            ..Default::default()
        },
        EnvVar {
            name: "HEADLESS_SERVICE".to_string(),
            value: Some(headless_svc.clone()),
            ..Default::default()
        },
        // Authentication password from secret
        EnvVar {
            name: "VALKEY_PASSWORD".to_string(),
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: resource.spec.auth.secret_ref.name.clone(),
                    key: resource.spec.auth.secret_ref.key.clone(),
                    optional: Some(false),
                }),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];

    // Build VALKEY_EXTRA_FLAGS for cluster configuration
    let extra_flags = build_valkey_extra_flags(resource, &namespace, &headless_svc);
    env.push(EnvVar {
        name: "VALKEY_EXTRA_FLAGS".to_string(),
        value: Some(extra_flags),
        ..Default::default()
    });

    env
}

/// Build the VALKEY_EXTRA_FLAGS environment variable value.
fn build_valkey_extra_flags(
    _resource: &ValkeyCluster,
    namespace: &str,
    headless_svc: &str,
) -> String {
    let mut flags = vec![
        // Cluster mode
        "--cluster-enabled yes".to_string(),
        "--cluster-config-file /data/nodes.conf".to_string(),
        "--cluster-node-timeout 5000".to_string(),
        // Don't require full slot coverage (allows partial operation during scaling)
        "--cluster-require-full-coverage no".to_string(),
        // Persistence
        "--appendonly yes".to_string(),
        "--appendfsync everysec".to_string(),
        // Bind to all interfaces
        "--bind 0.0.0.0".to_string(),
        // Authentication
        "--requirepass $(VALKEY_PASSWORD)".to_string(),
        "--masterauth $(VALKEY_PASSWORD)".to_string(),
    ];

    // TLS configuration
    flags.push("--tls-port 6379".to_string());
    flags.push("--port 0".to_string()); // Disable non-TLS port
    flags.push("--tls-cluster yes".to_string());
    flags.push("--tls-replication yes".to_string());
    flags.push("--tls-cert-file /etc/valkey/certs/tls.crt".to_string());
    flags.push("--tls-key-file /etc/valkey/certs/tls.key".to_string());
    flags.push("--tls-ca-cert-file /etc/valkey/certs/ca.crt".to_string());
    // Allow clients without TLS certs (server-only TLS)
    flags.push("--tls-auth-clients optional".to_string());

    // Announce the cluster-accessible hostname
    // Format: $(POD_NAME).{headless_svc}.{namespace}.svc.cluster.local
    flags.push(format!(
        "--cluster-announce-hostname $(POD_NAME).{}.{}.svc.cluster.local",
        headless_svc, namespace
    ));

    flags.join(" ")
}

/// Generate resource requirements from the spec.
fn generate_resource_requirements(resource: &ValkeyCluster) -> ResourceRequirements {
    ResourceRequirements {
        limits: Some({
            let mut limits = BTreeMap::new();
            limits.insert(
                "cpu".to_string(),
                Quantity(resource.spec.resources.limits.cpu.clone()),
            );
            limits.insert(
                "memory".to_string(),
                Quantity(resource.spec.resources.limits.memory.clone()),
            );
            limits
        }),
        requests: Some({
            let mut requests = BTreeMap::new();
            requests.insert(
                "cpu".to_string(),
                Quantity(resource.spec.resources.requests.cpu.clone()),
            );
            requests.insert(
                "memory".to_string(),
                Quantity(resource.spec.resources.requests.memory.clone()),
            );
            requests
        }),
        ..Default::default()
    }
}

/// Generate container security context.
fn generate_container_security_context() -> SecurityContext {
    SecurityContext {
        allow_privilege_escalation: Some(false),
        read_only_root_filesystem: Some(false), // Valkey needs to write to /data
        run_as_non_root: Some(true),
        run_as_user: Some(VALKEY_USER_ID),
        capabilities: Some(Capabilities {
            drop: Some(vec!["ALL".to_string()]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate startup probe.
///
/// Uses simple PING during startup to allow time for AOF loading.
/// Higher failure threshold to accommodate large datasets.
/// Uses VALKEYCLI_AUTH environment variable for secure password handling.
/// Uses --tls --insecure for localhost connections (skips cert validation).
fn generate_startup_probe() -> Probe {
    Probe {
        exec: Some(ExecAction {
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "VALKEYCLI_AUTH=$VALKEY_PASSWORD valkey-cli --tls --insecure ping".to_string(),
            ]),
        }),
        // 60 failures * 5 seconds = 5 minutes for AOF loading
        failure_threshold: Some(60),
        period_seconds: Some(5),
        timeout_seconds: Some(5),
        ..Default::default()
    }
}

/// Generate liveness probe.
///
/// Uses simple PING to verify the process is responsive.
/// Uses VALKEYCLI_AUTH environment variable for secure password handling.
/// Uses --tls --insecure for localhost connections (skips cert validation).
fn generate_liveness_probe() -> Probe {
    Probe {
        exec: Some(ExecAction {
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "VALKEYCLI_AUTH=$VALKEY_PASSWORD valkey-cli --tls --insecure ping".to_string(),
            ]),
        }),
        initial_delay_seconds: Some(10),
        period_seconds: Some(10),
        timeout_seconds: Some(5),
        failure_threshold: Some(3),
        ..Default::default()
    }
}

/// Generate readiness probe.
///
/// Checks that the cluster state is OK, meaning:
/// - Node is part of the cluster
/// - Cluster is consistent and operational
///   Uses VALKEYCLI_AUTH environment variable for secure password handling.
///   Uses --tls --insecure for localhost connections (skips cert validation).
fn generate_readiness_probe() -> Probe {
    Probe {
        exec: Some(ExecAction {
            command: Some(vec![
                "sh".to_string(),
                "-c".to_string(),
                "VALKEYCLI_AUTH=$VALKEY_PASSWORD valkey-cli --tls --insecure cluster info | grep cluster_state:ok".to_string(),
            ]),
        }),
        initial_delay_seconds: Some(5),
        period_seconds: Some(5),
        timeout_seconds: Some(5),
        failure_threshold: Some(3),
        success_threshold: Some(1),
        ..Default::default()
    }
}

/// Generate lifecycle hooks.
///
/// preStop hook performs BGSAVE before termination to minimize data loss.
/// Uses VALKEYCLI_AUTH environment variable for secure password handling.
/// Uses --tls --insecure for localhost connections (skips cert validation).
fn generate_lifecycle() -> Lifecycle {
    Lifecycle {
        pre_stop: Some(LifecycleHandler {
            exec: Some(ExecAction {
                command: Some(vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "VALKEYCLI_AUTH=$VALKEY_PASSWORD valkey-cli --tls --insecure bgsave && sleep 5"
                        .to_string(),
                ]),
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate volumes for the pod.
fn generate_volumes(resource: &ValkeyCluster) -> Vec<Volume> {
    let name = resource.name_any();

    let mut volumes = vec![
        // TLS certificates from cert-manager
        Volume {
            name: "tls-certs".to_string(),
            secret: Some(k8s_openapi::api::core::v1::SecretVolumeSource {
                secret_name: Some(format!("{}-tls", name)),
                default_mode: Some(0o400),
                ..Default::default()
            }),
            ..Default::default()
        },
    ];

    // If persistence is disabled, use emptyDir for data
    if !resource.spec.persistence.enabled {
        volumes.push(Volume {
            name: "data".to_string(),
            empty_dir: Some(EmptyDirVolumeSource::default()),
            ..Default::default()
        });
    }

    volumes
}

/// Generate volume mounts for the container.
fn generate_volume_mounts(_resource: &ValkeyCluster) -> Vec<VolumeMount> {
    vec![
        VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            ..Default::default()
        },
        VolumeMount {
            name: "tls-certs".to_string(),
            mount_path: "/etc/valkey/certs".to_string(),
            read_only: Some(true),
            ..Default::default()
        },
    ]
}

/// Generate PVC template for the StatefulSet.
fn generate_pvc_template(resource: &ValkeyCluster) -> PersistentVolumeClaim {
    PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("data".to_string()),
            ..Default::default()
        },
        spec: Some(PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            storage_class_name: resource.spec.persistence.storage_class_name.clone(),
            resources: Some(VolumeResourceRequirements {
                requests: Some({
                    let mut requests = BTreeMap::new();
                    requests.insert(
                        "storage".to_string(),
                        Quantity(resource.spec.persistence.size.clone()),
                    );
                    requests
                }),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;
    use crate::crd::{AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyClusterSpec};

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
                },
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_generate_statefulset() {
        let resource = test_resource("my-cluster");
        let sts = generate_statefulset(&resource);

        assert_eq!(sts.metadata.name, Some("my-cluster".to_string()));
        assert_eq!(sts.metadata.namespace, Some("default".to_string()));

        let spec = sts.spec.unwrap();
        assert_eq!(spec.replicas, Some(6)); // 3 masters + 3 replicas
        assert_eq!(spec.service_name, Some("my-cluster-headless".to_string()));
        assert_eq!(spec.pod_management_policy, Some("Parallel".to_string()));
    }

    #[test]
    fn test_generate_statefulset_labels() {
        let resource = test_resource("my-cluster");
        let sts = generate_statefulset(&resource);

        let labels = sts.metadata.labels.unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/managed-by"),
            Some(&"valkey-operator".to_string())
        );
    }

    #[test]
    fn test_valkey_extra_flags() {
        let resource = test_resource("my-cluster");
        let flags = build_valkey_extra_flags(&resource, "default", "my-cluster-headless");

        assert!(flags.contains("--cluster-enabled yes"));
        assert!(flags.contains("--tls-port 6379"));
        assert!(flags.contains("--tls-cluster yes"));
        assert!(flags.contains("--appendonly yes"));
    }

    #[test]
    fn test_pvc_template() {
        let resource = test_resource("my-cluster");
        let pvc = generate_pvc_template(&resource);

        assert_eq!(pvc.metadata.name, Some("data".to_string()));
        let spec = pvc.spec.unwrap();
        assert_eq!(spec.access_modes, Some(vec!["ReadWriteOnce".to_string()]));
    }
}
