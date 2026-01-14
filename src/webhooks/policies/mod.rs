//! Validation policies for MyResource admission webhooks.
//!
//! Policies are organized into tiers:
//! - Tier 1 (Critical): Always enforced (replica validation)
//! - Tier 2 (Update): Only enforced on UPDATE operations (immutability)

pub mod immutability;
pub mod replicas;

use crate::crd::MyResource;

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
    pub resource: &'a MyResource,
    /// The old resource (for UPDATE operations)
    pub old_resource: Option<&'a MyResource>,
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

/// Run all validation policies
pub fn validate_all(ctx: &ValidationContext<'_>) -> ValidationResult {
    // Tier 1: Critical validations (always enforced)
    let result = replicas::validate(ctx);
    if !result.allowed {
        return result;
    }

    // Tier 2: Update validations (only for UPDATE operations)
    if ctx.is_update() {
        let result = immutability::validate(ctx);
        if !result.allowed {
            return result;
        }
    }

    ValidationResult::allowed()
}
