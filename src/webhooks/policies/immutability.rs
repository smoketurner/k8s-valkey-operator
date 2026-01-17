//! Immutability validation policy.
//!
//! Tier 2 (Update): Only enforced on UPDATE operations
//!
//! Validates:
//! - Certain fields cannot be changed after creation
//! - Prevents scaling masters to below minimum

use super::{ValidationContext, ValidationResult};
use crate::controller::cluster_validation::MIN_MASTERS;

/// Validate immutability constraints on UPDATE operations
pub fn validate(ctx: &ValidationContext<'_>) -> ValidationResult {
    let old = match ctx.old_resource {
        Some(r) => r,
        None => return ValidationResult::allowed(), // Not an UPDATE
    };

    let new = ctx.resource;

    // Prevent scaling masters below minimum
    if old.spec.masters >= MIN_MASTERS && new.spec.masters < MIN_MASTERS {
        return ValidationResult::denied(
            "InvalidScaleDown",
            &format!(
                "Cannot scale down masters below {} (cluster quorum requirement). Delete the resource instead.",
                MIN_MASTERS
            ),
        );
    }

    // Prevent changing TLS issuer (would require certificate rotation)
    if old.spec.tls.issuer_ref.name != new.spec.tls.issuer_ref.name {
        return ValidationResult::denied(
            "ImmutableField",
            "tls.issuerRef.name cannot be changed after creation. Delete and recreate the cluster.",
        );
    }

    // Prevent changing auth secret reference (would require credential rotation)
    if old.spec.auth.secret_ref.name != new.spec.auth.secret_ref.name {
        return ValidationResult::denied(
            "ImmutableField",
            "auth.secretRef.name cannot be changed after creation. Delete and recreate the cluster.",
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

    fn create_resource(masters: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
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
                labels: BTreeMap::new(),
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_valid_update() {
        let old = create_resource(3);
        let new = create_resource(6);

        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_scale_below_minimum_denied() {
        let old = create_resource(3);
        let new = create_resource(2);

        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert!(result.reason.unwrap().contains("InvalidScaleDown"));
    }

    #[test]
    fn test_create_allows_any() {
        // On CREATE, there's no old_resource
        let new = create_resource(3);

        let ctx = ValidationContext {
            resource: &new,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(result.allowed);
    }
}
