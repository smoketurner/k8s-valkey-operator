//! Replica count validation policy.
//!
//! Tier 1 (Critical): Always enforced
//!
//! Validates:
//! - Replica count is at least MIN_REPLICAS
//! - Replica count does not exceed MAX_REPLICAS

use super::{ValidationContext, ValidationResult};
use crate::controller::validation::{MAX_REPLICAS, MIN_REPLICAS};

/// Validate replica count
pub fn validate(ctx: &ValidationContext<'_>) -> ValidationResult {
    let replicas = ctx.resource.spec.replicas;

    if replicas < MIN_REPLICAS {
        return ValidationResult::denied(
            "InvalidReplicas",
            &format!(
                "spec.replicas must be at least {} (got {})",
                MIN_REPLICAS, replicas
            ),
        );
    }

    if replicas > MAX_REPLICAS {
        return ValidationResult::denied(
            "InvalidReplicas",
            &format!(
                "spec.replicas cannot exceed {} (got {})",
                MAX_REPLICAS, replicas
            ),
        );
    }

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
    fn test_valid_replicas() {
        let resource = create_resource(3);
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
    fn test_replicas_below_minimum() {
        let resource = create_resource(0);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert!(result.reason.unwrap().contains("InvalidReplicas"));
    }

    #[test]
    fn test_replicas_above_maximum() {
        let resource = create_resource(100);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate(&ctx);
        assert!(!result.allowed);
        assert!(result.reason.unwrap().contains("InvalidReplicas"));
    }

    #[test]
    fn test_replicas_at_bounds() {
        // At minimum
        let resource = create_resource(MIN_REPLICAS);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);

        // At maximum
        let resource = create_resource(MAX_REPLICAS);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };
        assert!(validate(&ctx).allowed);
    }
}
