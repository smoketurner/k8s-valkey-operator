//! Master and replica count validation policy.
//!
//! Tier 1 (Critical): Always enforced
//!
//! Validates:
//! - Master count is at least MIN_MASTERS (3 for cluster quorum)
//! - Master count does not exceed MAX_MASTERS
//! - Replicas per master is within bounds

use super::{ValidationContext, ValidationResult};
use crate::controller::cluster_validation::{MAX_MASTERS, MAX_REPLICAS_PER_MASTER, MIN_MASTERS};

/// Validate master and replica counts
pub fn validate(ctx: &ValidationContext<'_>) -> ValidationResult {
    let masters = ctx.resource.spec.masters;
    let replicas_per_master = ctx.resource.spec.replicas_per_master;

    if masters < MIN_MASTERS {
        return ValidationResult::denied(
            "InvalidMasters",
            &format!(
                "spec.masters must be at least {} for cluster quorum (got {})",
                MIN_MASTERS, masters
            ),
        );
    }

    if masters > MAX_MASTERS {
        return ValidationResult::denied(
            "InvalidMasters",
            &format!(
                "spec.masters cannot exceed {} (got {})",
                MAX_MASTERS, masters
            ),
        );
    }

    if replicas_per_master < 0 {
        return ValidationResult::denied(
            "InvalidReplicasPerMaster",
            "spec.replicasPerMaster cannot be negative",
        );
    }

    if replicas_per_master > MAX_REPLICAS_PER_MASTER {
        return ValidationResult::denied(
            "InvalidReplicasPerMaster",
            &format!(
                "spec.replicasPerMaster cannot exceed {} (got {})",
                MAX_REPLICAS_PER_MASTER, replicas_per_master
            ),
        );
    }

    ValidationResult::allowed()
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
    use crate::crd::{
        AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyCluster, ValkeyClusterSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_resource(masters: i32, replicas_per_master: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters,
                replicas_per_master,
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
                labels: BTreeMap::new(),
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_valid_masters() {
        let resource = create_resource(3, 1);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_masters_below_minimum() {
        let resource = create_resource(2, 1);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert!(result.reason.unwrap().contains("InvalidMasters"));
    }

    #[test]
    fn test_masters_above_maximum() {
        let resource = create_resource(101, 1);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert!(result.reason.unwrap().contains("InvalidMasters"));
    }

    #[test]
    fn test_masters_at_bounds() {
        // At minimum
        let resource = create_resource(MIN_MASTERS, 1);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);

        // At maximum
        let resource = create_resource(MAX_MASTERS, 1);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);
    }

    #[test]
    fn test_replicas_per_master_above_max() {
        let resource = create_resource(3, 6);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert!(result.reason.unwrap().contains("InvalidReplicasPerMaster"));
    }
}
