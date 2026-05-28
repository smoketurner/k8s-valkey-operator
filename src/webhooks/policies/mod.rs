//! Validation policies for ValkeyCluster admission webhooks.
//!
//! Numeric bounds (masters, replicasPerMaster) and field immutability
//! (TLS issuer name, auth secret name) live in the CRD schema as OpenAPI
//! constraints and `x-kubernetes-validations` CEL rules. The webhook
//! covers only what the schema can't express:
//! - `image_change`: semver-aware downgrade protection
//! - `upgrade_protection`: blocks spec changes while an upgrade is in flight
//!   (driven by the `upgrade-in-progress` annotation on the cluster)

pub mod image_change;
pub mod upgrade_protection;

use crate::crd::ValkeyCluster;

/// Result of a validation check
#[derive(Debug)]
pub struct ValidationResult {
    /// Whether the validation passed
    pub allowed: bool,
    /// Reason for denial (if not allowed)
    pub reason: Option<String>,
    /// Detailed message (if not allowed)
    pub message: Option<String>,
}

impl ValidationResult {
    /// Create an allowed result
    pub fn allowed() -> Self {
        Self {
            allowed: true,
            reason: None,
            message: None,
        }
    }

    /// Create a denied result
    pub fn denied(reason: &str, message: &str) -> Self {
        Self {
            allowed: false,
            reason: Some(reason.to_string()),
            message: Some(message.to_string()),
        }
    }
}

/// Context for validation
pub struct ValidationContext<'a> {
    /// The resource being validated
    pub resource: &'a ValkeyCluster,
    /// The old resource (for UPDATE operations)
    pub old_resource: Option<&'a ValkeyCluster>,
    /// Whether this is a dry-run request
    pub dry_run: bool,
    /// The namespace of the resource
    pub namespace: Option<&'a str>,
}

impl<'a> ValidationContext<'a> {
    /// Check if this is an UPDATE operation
    pub fn is_update(&self) -> bool {
        self.old_resource.is_some()
    }
}

/// Run all validation policies. Only relevant on UPDATE — CREATE-time
/// shape validation is handled entirely by the CRD schema.
pub fn validate_all(ctx: &ValidationContext<'_>) -> ValidationResult {
    if ctx.is_update() {
        // Upgrade protection runs first: while an upgrade is in flight, no
        // other spec change should be evaluated at all.
        let result = upgrade_protection::validate(ctx);
        if !result.allowed {
            return result;
        }

        let result = image_change::validate(ctx);
        if !result.allowed {
            return result;
        }
    }

    ValidationResult::allowed()
}
