//! Service generation for Valkey clusters.
//!
//! Creates two services:
//! - **Headless Service**: For cluster discovery and stable network identity
//! - **Client Service**: For client connections with load balancing

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;

use crate::crd::ValkeyCluster;
use crate::resources::common::{
    client_service_name, headless_service_name, owner_reference, pod_selector_labels,
    standard_annotations, standard_labels,
};

/// Valkey client port
const CLIENT_PORT: i32 = 6379;
/// Valkey cluster bus port
const CLUSTER_BUS_PORT: i32 = 16379;

/// Generate a headless Service for cluster discovery.
///
/// The headless service provides:
/// - DNS records for each pod (pod-0.svc-headless.ns.svc.cluster.local)
/// - No load balancing (direct pod access)
/// - `publishNotReadyAddresses: true` for cluster formation before all pods are ready
pub fn generate_headless_service(resource: &ValkeyCluster) -> Service {
    let name = headless_service_name(resource);
    let namespace = resource.namespace();
    let mut labels = standard_labels(resource);
    labels.insert(
        "app.kubernetes.io/service-type".to_string(),
        "headless".to_string(),
    );
    let annotations = standard_annotations(resource);

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace,
            labels: Some(labels),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            // Headless service (no cluster IP)
            cluster_ip: Some("None".to_string()),
            // Critical: Allow DNS lookups for pods that aren't ready yet
            // This enables CLUSTER MEET during initial cluster formation
            publish_not_ready_addresses: Some(true),
            selector: Some(pod_selector_labels(resource)),
            ports: Some(vec![
                ServicePort {
                    port: CLIENT_PORT,
                    target_port: Some(IntOrString::String("client".to_string())),
                    name: Some("client".to_string()),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                },
                ServicePort {
                    port: CLUSTER_BUS_PORT,
                    target_port: Some(IntOrString::String("cluster-bus".to_string())),
                    name: Some("cluster-bus".to_string()),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate a client Service for external access.
///
/// The client service provides:
/// - Load balancing across all ready pods
/// - Stable endpoint for client connections
/// - Only exposes the client port (not cluster bus)
pub fn generate_client_service(resource: &ValkeyCluster) -> Service {
    let name = client_service_name(resource);
    let namespace = resource.namespace();
    let mut labels = standard_labels(resource);
    labels.insert(
        "app.kubernetes.io/service-type".to_string(),
        "client".to_string(),
    );
    let annotations = standard_annotations(resource);

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace,
            labels: Some(labels),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            selector: Some(pod_selector_labels(resource)),
            ports: Some(vec![ServicePort {
                port: CLIENT_PORT,
                target_port: Some(IntOrString::String("client".to_string())),
                name: Some("client".to_string()),
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
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
    fn test_generate_headless_service() {
        let resource = test_resource("my-cluster");
        let svc = generate_headless_service(&resource);

        assert_eq!(svc.metadata.name, Some("my-cluster-headless".to_string()));
        assert_eq!(svc.metadata.namespace, Some("default".to_string()));

        let spec = svc.spec.unwrap();
        assert_eq!(spec.cluster_ip, Some("None".to_string()));
        assert_eq!(spec.publish_not_ready_addresses, Some(true));

        let ports = spec.ports.unwrap();
        assert_eq!(ports.len(), 2);
        assert!(ports.iter().any(|p| p.name == Some("client".to_string())));
        assert!(
            ports
                .iter()
                .any(|p| p.name == Some("cluster-bus".to_string()))
        );
    }

    #[test]
    fn test_generate_client_service() {
        let resource = test_resource("my-cluster");
        let svc = generate_client_service(&resource);

        assert_eq!(svc.metadata.name, Some("my-cluster".to_string()));
        assert_eq!(svc.metadata.namespace, Some("default".to_string()));

        let spec = svc.spec.unwrap();
        assert_eq!(spec.type_, Some("ClusterIP".to_string()));

        let ports = spec.ports.unwrap();
        assert_eq!(ports.len(), 1);
        assert_eq!(ports.first().unwrap().name, Some("client".to_string()));
        assert_eq!(ports.first().unwrap().port, CLIENT_PORT);
    }

    #[test]
    fn test_headless_service_labels() {
        let resource = test_resource("my-cluster");
        let svc = generate_headless_service(&resource);

        let labels = svc.metadata.labels.unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/service-type"),
            Some(&"headless".to_string())
        );
    }

    #[test]
    fn test_client_service_labels() {
        let resource = test_resource("my-cluster");
        let svc = generate_client_service(&resource);

        let labels = svc.metadata.labels.unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/service-type"),
            Some(&"client".to_string())
        );
    }
}
