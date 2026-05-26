//! Image change validation policy.
//!
//! Tier 2 (Update): Only enforced on UPDATE operations
//!
//! Validates:
//! - Downgrades are blocked unless `spec.image.allowDowngrade` is true
//!
//! The CRD documents `allowDowngrade` as the gate that controls whether the
//! tag may be lowered. Without enforcement, an operator user could downgrade
//! by simply editing `spec.image.tag`, defeating the protection. This policy
//! delegates to `cluster_validation::validate_image_change` so the rule lives
//! in one place and is shared with the upgrade reconciler (issue #45).

use super::{ValidationContext, ValidationResult};
use crate::controller::cluster_validation::validate_image_change;

/// Validate image changes on UPDATE operations
pub fn validate(ctx: &ValidationContext<'_>) -> ValidationResult {
    let Some(old) = ctx.old_resource else {
        return ValidationResult::allowed();
    };
    let new = ctx.resource;

    match validate_image_change(&old.spec, &new.spec) {
        Ok(()) => ValidationResult::allowed(),
        Err(e) => ValidationResult::denied("DowngradeBlocked", &e.to_string()),
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
    use crate::crd::{
        AuthSpec, ImageSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyCluster, ValkeyClusterSpec,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_resource(tag: &str, allow_downgrade: bool) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                image: ImageSpec {
                    tag: tag.to_string(),
                    allow_downgrade,
                    ..Default::default()
                },
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
    fn test_create_allowed() {
        let new = create_resource("9.0.1-alpine", false);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);
    }

    #[test]
    fn test_upgrade_allowed() {
        let old = create_resource("9.0.0-alpine", false);
        let new = create_resource("9.0.1-alpine", false);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);
    }

    #[test]
    fn test_downgrade_blocked_without_flag() {
        let old = create_resource("9.0.1-alpine", false);
        let new = create_resource("9.0.0-alpine", false);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };
        let result = validate(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("DowngradeBlocked"));
        assert!(
            result
                .message
                .as_deref()
                .unwrap_or_default()
                .contains("allowDowngrade")
        );
    }

    #[test]
    fn test_downgrade_allowed_with_flag() {
        let old = create_resource("9.0.1-alpine", false);
        let new = create_resource("9.0.0-alpine", true);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);
    }

    #[test]
    fn test_no_tag_change_allowed() {
        let old = create_resource("9.0.1-alpine", false);
        let new = create_resource("9.0.1-alpine", false);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);
    }
}
