//! Certificate resource generation for cert-manager integration.
//!
//! Generates cert-manager Certificate resources to provide TLS certificates
//! for Valkey cluster communication.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::ResourceExt;
use serde::{Deserialize, Serialize};

use crate::crd::ValkeyCluster;

use super::common::{owner_reference, standard_annotations, standard_labels};

// ============================================================================
// cert-manager Certificate types
// ============================================================================

/// cert-manager Certificate resource.
///
/// This is a simplified representation of the cert-manager Certificate CRD
/// for generating TLS certificates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Certificate {
    /// API version for cert-manager Certificate.
    pub api_version: String,

    /// Kind is always "Certificate".
    pub kind: String,

    /// Standard object metadata.
    pub metadata: ObjectMeta,

    /// Certificate specification.
    pub spec: CertificateSpec,
}

/// Specification for a cert-manager Certificate.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateSpec {
    /// Name of the Secret that will contain the certificate.
    pub secret_name: String,

    /// Reference to the issuer responsible for issuing the certificate.
    pub issuer_ref: CertIssuerRef,

    /// DNS names to include in the certificate.
    pub dns_names: Vec<String>,

    /// Requested certificate validity duration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,

    /// How long before expiry to renew the certificate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub renew_before: Option<String>,

    /// Private key configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key: Option<PrivateKeySpec>,

    /// Key usages for the certificate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usages: Option<Vec<String>>,
}

/// Reference to a cert-manager Issuer or ClusterIssuer.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertIssuerRef {
    /// Name of the issuer.
    pub name: String,

    /// Kind of the issuer (Issuer or ClusterIssuer).
    pub kind: String,

    /// Group of the issuer (typically "cert-manager.io").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

/// Private key configuration for certificates.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivateKeySpec {
    /// Algorithm for the private key (RSA, ECDSA, Ed25519).
    pub algorithm: String,

    /// Size of the private key in bits.
    /// For RSA: 2048, 4096, 8192
    /// For ECDSA: 256, 384, 521
    pub size: i32,
}

// ============================================================================
// Certificate generation
// ============================================================================

/// Generate the certificate secret name for a ValkeyCluster.
pub fn certificate_secret_name(resource: &ValkeyCluster) -> String {
    format!("{}-tls", resource.name_any())
}

/// Generate a cert-manager Certificate resource for the ValkeyCluster.
///
/// The certificate includes DNS SANs for:
/// - Wildcard for all pods via headless service: `*.{name}-headless.{ns}.svc.cluster.local`
/// - Client service endpoint: `{name}.{ns}.svc.cluster.local`
///
/// This enables TLS for:
/// - Client connections
/// - Cluster bus communication between nodes
/// - Replication traffic
pub fn generate_certificate(resource: &ValkeyCluster) -> Certificate {
    let name = resource.name_any();
    let namespace = resource
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let secret_name = certificate_secret_name(resource);

    let tls = &resource.spec.tls;
    let issuer_ref = &tls.issuer_ref;

    // Build DNS names for the certificate
    let dns_names = vec![
        // Wildcard for all pod DNS names via headless service
        format!("*.{}-headless.{}.svc.cluster.local", name, namespace),
        // Client service endpoint
        format!("{}.{}.svc.cluster.local", name, namespace),
        // Short names for in-cluster access
        format!("{}-headless.{}.svc", name, namespace),
        format!("{}.{}.svc", name, namespace),
        // Headless service itself
        format!("{}-headless", name),
        // Client service itself
        name.clone(),
    ];

    // Build labels and annotations
    let mut labels = standard_labels(resource);
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "certificate".to_string(),
    );
    let annotations = standard_annotations(resource);

    Certificate {
        api_version: "cert-manager.io/v1".to_string(),
        kind: "Certificate".to_string(),
        metadata: ObjectMeta {
            name: Some(secret_name.clone()),
            namespace: Some(namespace),
            labels: Some(labels),
            annotations: if annotations.is_empty() {
                None
            } else {
                Some(annotations)
            },
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: CertificateSpec {
            secret_name,
            issuer_ref: CertIssuerRef {
                name: issuer_ref.name.clone(),
                kind: issuer_ref.kind.clone(),
                group: Some(issuer_ref.group.clone()),
            },
            dns_names,
            duration: Some(tls.duration.clone()),
            renew_before: Some(tls.renew_before.clone()),
            private_key: Some(PrivateKeySpec {
                algorithm: "ECDSA".to_string(),
                size: 256,
            }),
            usages: Some(vec!["server auth".to_string(), "client auth".to_string()]),
        },
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;
    use crate::crd::{AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyClusterSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn test_resource(name: &str, namespace: &str) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "ca-issuer".to_string(),
                        kind: "ClusterIssuer".to_string(),
                        group: "cert-manager.io".to_string(),
                    },
                    duration: "2160h".to_string(),
                    renew_before: "360h".to_string(),
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-secret".to_string(),
                        key: "password".to_string(),
                    },
                },
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_certificate_secret_name() {
        let resource = test_resource("my-cluster", "default");
        assert_eq!(certificate_secret_name(&resource), "my-cluster-tls");
    }

    #[test]
    fn test_generate_certificate() {
        let resource = test_resource("my-cluster", "production");
        let cert = generate_certificate(&resource);

        // Check metadata
        assert_eq!(cert.metadata.name, Some("my-cluster-tls".to_string()));
        assert_eq!(cert.metadata.namespace, Some("production".to_string()));

        // Check type info
        assert_eq!(cert.api_version, "cert-manager.io/v1");
        assert_eq!(cert.kind, "Certificate");

        // Check owner reference
        let owner_refs = cert.metadata.owner_references.unwrap();
        assert_eq!(owner_refs.len(), 1);
        assert_eq!(owner_refs.first().unwrap().name, "my-cluster");

        // Check labels
        let labels = cert.metadata.labels.unwrap();
        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/component"),
            Some(&"certificate".to_string())
        );

        // Check spec
        assert_eq!(cert.spec.secret_name, "my-cluster-tls");
        assert_eq!(cert.spec.duration, Some("2160h".to_string()));
        assert_eq!(cert.spec.renew_before, Some("360h".to_string()));
        assert_eq!(cert.spec.issuer_ref.name, "ca-issuer");
        assert_eq!(cert.spec.issuer_ref.kind, "ClusterIssuer");
        assert_eq!(
            cert.spec.issuer_ref.group,
            Some("cert-manager.io".to_string())
        );

        // Check DNS names
        assert!(
            cert.spec
                .dns_names
                .contains(&"*.my-cluster-headless.production.svc.cluster.local".to_string())
        );
        assert!(
            cert.spec
                .dns_names
                .contains(&"my-cluster.production.svc.cluster.local".to_string())
        );

        // Check private key
        let private_key = cert.spec.private_key.unwrap();
        assert_eq!(private_key.algorithm, "ECDSA");
        assert_eq!(private_key.size, 256);

        // Check usages
        let usages = cert.spec.usages.unwrap();
        assert!(usages.contains(&"server auth".to_string()));
        assert!(usages.contains(&"client auth".to_string()));
    }

    #[test]
    fn test_certificate_serialization() {
        let resource = test_resource("my-cluster", "default");
        let cert = generate_certificate(&resource);

        // Verify it can be serialized to JSON
        let json = serde_json::to_string_pretty(&cert).unwrap();
        assert!(json.contains("\"apiVersion\": \"cert-manager.io/v1\""));
        assert!(json.contains("\"kind\": \"Certificate\""));
        assert!(json.contains("\"secretName\": \"my-cluster-tls\""));
    }
}
