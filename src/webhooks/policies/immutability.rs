//! Immutability validation policy.
//!
//! Tier 2 (Update): Only enforced on UPDATE operations
//!
//! Validates:
//! - Certain fields cannot be changed after creation
//! - Prevents scaling to zero from a running state

use super::{ValidationContext, ValidationResult};

/// Validate immutability constraints on UPDATE operations
pub fn validate(ctx: &ValidationContext<'_>) -> ValidationResult {
    let old = match ctx.old_resource {
        Some(r) => r,
        None => return ValidationResult::allowed(), // Not an UPDATE
    };

    let new = ctx.resource;

    // Prevent scaling from non-zero to zero
    if old.spec.replicas > 0 && new.spec.replicas == 0 {
        return ValidationResult::denied(
            "InvalidScaleDown",
            "Cannot scale down from a running state to 0 replicas. Delete the resource instead.",
        );
    }

    // Example: Add more immutability checks here
    // For instance, preventing changes to certain labels or annotations

    ValidationResult::allowed()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::crd::{MyResource, MyResourceSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_resource(replicas: i32) -> MyResource {
        MyResource {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: MyResourceSpec {
                replicas,
                message: "test".to_string(),
                labels: BTreeMap::new(),
            },
            status: None,
        }
    }

    #[test]
    fn test_valid_update() {
        let old = create_resource(2);
        let new = create_resource(3);

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
    fn test_scale_to_zero_denied() {
        let old = create_resource(2);
        let new = create_resource(0);

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
    fn test_create_allows_zero() {
        // On CREATE, there's no old_resource
        let new = create_resource(0);

        let ctx = ValidationContext {
            resource: &new,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        // Immutability policy allows this (replicas policy will catch it)
        let result = validate(&ctx);
        assert!(result.allowed);
    }
}
