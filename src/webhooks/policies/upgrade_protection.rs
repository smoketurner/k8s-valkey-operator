//! Upgrade protection validation policy.
//!
//! Tier 2 (Update): Only enforced on UPDATE operations
//!
//! Validates:
//! - Spec changes are blocked when an upgrade is in progress
//! - Prevents users from making changes that could conflict with the upgrade
//!
//! During a ValkeyUpgrade operation, the cluster is in a sensitive state where
//! node restarts and version changes are being coordinated. Allowing spec changes
//! during this time could lead to:
//! - Conflicting pod restarts
//! - Inconsistent cluster state
//! - Potential data loss or cluster unavailability

use super::{ValidationContext, ValidationResult};

/// Annotation key indicating an upgrade is in progress
const UPGRADE_IN_PROGRESS_ANNOTATION: &str = "valkey-operator.smoketurner.com/upgrade-in-progress";

/// Annotation key storing the name of the active upgrade
const UPGRADE_NAME_ANNOTATION: &str = "valkey-operator.smoketurner.com/upgrade-name";

/// Check if an upgrade is in progress for this cluster
fn is_upgrade_in_progress(ctx: &ValidationContext<'_>) -> Option<String> {
    // Check the OLD resource for the annotation, since that's the current state
    let old = ctx.old_resource?;

    let annotations = old.metadata.annotations.as_ref()?;
    let in_progress = annotations.get(UPGRADE_IN_PROGRESS_ANNOTATION)?;

    if in_progress == "true" {
        // Return the upgrade name if available for a better error message
        let upgrade_name = annotations
            .get(UPGRADE_NAME_ANNOTATION)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        Some(upgrade_name)
    } else {
        None
    }
}

/// Check if the spec has changed between old and new resource
fn has_spec_changed(ctx: &ValidationContext<'_>) -> bool {
    let Some(old) = ctx.old_resource else {
        return false;
    };

    let new = ctx.resource;

    // Check for any spec changes that could affect cluster state
    // Compare individual fields rather than complex structs to avoid PartialEq requirements
    old.spec.masters != new.spec.masters
        || old.spec.replicas_per_master != new.spec.replicas_per_master
        || old.spec.image.repository != new.spec.image.repository
        || old.spec.image.tag != new.spec.image.tag
        || old.spec.resources.requests.cpu != new.spec.resources.requests.cpu
        || old.spec.resources.requests.memory != new.spec.resources.requests.memory
        || old.spec.resources.limits.cpu != new.spec.resources.limits.cpu
        || old.spec.resources.limits.memory != new.spec.resources.limits.memory
        || old.spec.persistence.enabled != new.spec.persistence.enabled
        || old.spec.persistence.size != new.spec.persistence.size
        || old.spec.persistence.storage_class_name != new.spec.persistence.storage_class_name
        || old.spec.labels != new.spec.labels
}

/// Validate that spec changes are not made during an active upgrade
pub fn validate(ctx: &ValidationContext<'_>) -> ValidationResult {
    // Only applies to UPDATE operations
    if !ctx.is_update() {
        return ValidationResult::allowed();
    }

    // Check if an upgrade is in progress
    let Some(upgrade_name) = is_upgrade_in_progress(ctx) else {
        return ValidationResult::allowed();
    };

    // Check if the spec has changed
    if !has_spec_changed(ctx) {
        // Metadata-only changes (annotations, labels on metadata) are allowed
        // Status updates are also allowed (handled separately by the API server)
        return ValidationResult::allowed();
    }

    // Spec changed during upgrade - block it
    ValidationResult::denied(
        "UpgradeInProgress",
        &format!(
            "Cannot modify ValkeyCluster spec while upgrade '{}' is in progress. \
             Wait for the upgrade to complete or roll it back first. \
             You can check upgrade status with: kubectl get valkeyupgrade {}",
            upgrade_name, upgrade_name
        ),
    )
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

    fn create_resource_with_upgrade_annotation(masters: i32, upgrade_name: &str) -> ValkeyCluster {
        let mut resource = create_resource(masters);
        let mut annotations = BTreeMap::new();
        annotations.insert(
            UPGRADE_IN_PROGRESS_ANNOTATION.to_string(),
            "true".to_string(),
        );
        annotations.insert(
            UPGRADE_NAME_ANNOTATION.to_string(),
            upgrade_name.to_string(),
        );
        resource.metadata.annotations = Some(annotations);
        resource
    }

    #[test]
    fn test_no_upgrade_allows_changes() {
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
    fn test_upgrade_blocks_spec_changes() {
        let old = create_resource_with_upgrade_annotation(3, "test-upgrade");
        let new = create_resource(6);

        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert_eq!(result.reason.unwrap(), "UpgradeInProgress");
        assert!(result.message.unwrap().contains("test-upgrade"));
    }

    #[test]
    fn test_upgrade_allows_no_spec_changes() {
        // Same spec, just metadata might differ
        let old = create_resource_with_upgrade_annotation(3, "test-upgrade");
        let mut new = create_resource(3);
        // Add different metadata annotation (not spec change)
        let mut annotations = BTreeMap::new();
        annotations.insert("some-other-annotation".to_string(), "value".to_string());
        new.metadata.annotations = Some(annotations);

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
    fn test_create_always_allowed() {
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

    #[test]
    fn test_upgrade_blocks_image_change() {
        let old = create_resource_with_upgrade_annotation(3, "test-upgrade");
        let mut new = create_resource(3);
        new.spec.image.tag = "9.0.1".to_string();

        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
    }

    #[test]
    fn test_upgrade_blocks_replica_change() {
        let old = create_resource_with_upgrade_annotation(3, "test-upgrade");
        let mut new = create_resource(3);
        new.spec.replicas_per_master = 2;

        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
    }
}
