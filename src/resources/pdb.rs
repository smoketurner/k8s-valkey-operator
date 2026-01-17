//! PodDisruptionBudget generation for Valkey clusters.
//!
//! Creates a PDB to maintain cluster quorum during voluntary disruptions
//! such as node drains or cluster upgrades.

use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::ResourceExt;

use crate::crd::ValkeyCluster;
use crate::resources::common::{owner_reference, pod_selector_labels, standard_labels};

/// Generate a PodDisruptionBudget for a ValkeyCluster.
///
/// The PDB ensures that:
/// - At least a majority of masters remain available
/// - Cluster quorum is maintained during disruptions
/// - Voluntary disruptions (node drains, upgrades) respect cluster health
///
/// For a 3-master cluster, maxUnavailable=1 ensures 2 masters are always available.
/// For a 6-master cluster, maxUnavailable=2 ensures 4 masters are always available.
pub fn generate_pod_disruption_budget(resource: &ValkeyCluster) -> PodDisruptionBudget {
    let name = resource.name_any();
    let namespace = resource.namespace();
    let labels = standard_labels(resource);

    // Calculate maxUnavailable to maintain quorum
    // For Valkey cluster, we need at least half + 1 masters available
    // maxUnavailable = floor(masters / 2)
    let masters = resource.spec.masters;
    let max_unavailable = masters / 2;

    PodDisruptionBudget {
        metadata: ObjectMeta {
            name: Some(name),
            namespace,
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: Some(PodDisruptionBudgetSpec {
            max_unavailable: Some(IntOrString::Int(max_unavailable)),
            selector: Some(LabelSelector {
                match_labels: Some(pod_selector_labels(resource)),
                ..Default::default()
            }),
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

    fn test_resource(name: &str, masters: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters,
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
    fn test_generate_pdb_3_masters() {
        let resource = test_resource("my-cluster", 3);
        let pdb = generate_pod_disruption_budget(&resource);

        assert_eq!(pdb.metadata.name, Some("my-cluster".to_string()));
        assert_eq!(pdb.metadata.namespace, Some("default".to_string()));

        let spec = pdb.spec.unwrap();
        // 3 masters: maxUnavailable = 3 / 2 = 1
        assert_eq!(spec.max_unavailable, Some(IntOrString::Int(1)));
    }

    #[test]
    fn test_generate_pdb_6_masters() {
        let resource = test_resource("my-cluster", 6);
        let pdb = generate_pod_disruption_budget(&resource);

        let spec = pdb.spec.unwrap();
        // 6 masters: maxUnavailable = 6 / 2 = 3
        assert_eq!(spec.max_unavailable, Some(IntOrString::Int(3)));
    }

    #[test]
    fn test_generate_pdb_9_masters() {
        let resource = test_resource("my-cluster", 9);
        let pdb = generate_pod_disruption_budget(&resource);

        let spec = pdb.spec.unwrap();
        // 9 masters: maxUnavailable = 9 / 2 = 4
        assert_eq!(spec.max_unavailable, Some(IntOrString::Int(4)));
    }

    #[test]
    fn test_pdb_selector() {
        let resource = test_resource("my-cluster", 3);
        let pdb = generate_pod_disruption_budget(&resource);

        let spec = pdb.spec.unwrap();
        let selector = spec.selector.unwrap();
        let labels = selector.match_labels.unwrap();

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
    }
}
