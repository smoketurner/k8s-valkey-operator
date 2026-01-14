//! Validation logic for MyResource spec changes.
//!
//! This module provides validation for spec changes, including:
//! - Replica count validation
//! - Spec change detection
//! - Immutable field changes

use crate::controller::error::{Error, Result};
use crate::crd::MyResource;

/// Minimum number of replicas
pub const MIN_REPLICAS: i32 = 1;

/// Maximum number of replicas
pub const MAX_REPLICAS: i32 = 10;

/// Validate the resource spec
pub fn validate_spec(resource: &MyResource) -> Result<()> {
    validate_replicas(resource)?;
    validate_message(resource)?;
    Ok(())
}

/// Validate replica count
fn validate_replicas(resource: &MyResource) -> Result<()> {
    let replicas = resource.spec.replicas;

    if replicas < MIN_REPLICAS {
        return Err(Error::Validation(format!(
            "replica count {} is below minimum {}",
            replicas, MIN_REPLICAS
        )));
    }

    if replicas > MAX_REPLICAS {
        return Err(Error::Validation(format!(
            "replica count {} exceeds maximum {}",
            replicas, MAX_REPLICAS
        )));
    }

    Ok(())
}

/// Validate message (basic check)
fn validate_message(resource: &MyResource) -> Result<()> {
    // Message can be empty, but if provided, check reasonable length
    if resource.spec.message.len() > 1024 {
        return Err(Error::Validation(
            "message exceeds maximum length of 1024 characters".to_string(),
        ));
    }
    Ok(())
}

/// Result of comparing old and new spec
#[derive(Debug, Clone, Default)]
pub struct SpecDiff {
    /// Number of replicas changed
    pub replicas_changed: bool,
    /// Scale direction (positive = up, negative = down)
    pub replica_delta: i32,
    /// Message changed
    pub message_changed: bool,
    /// Labels changed
    pub labels_changed: bool,
}

impl SpecDiff {
    /// Check if any changes require an update to managed resources
    pub fn requires_update(&self) -> bool {
        self.replicas_changed || self.message_changed || self.labels_changed
    }

    /// Check if this is a scale-only operation
    pub fn is_scale_only(&self) -> bool {
        self.replicas_changed && !self.message_changed && !self.labels_changed
    }

    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        self.replicas_changed || self.message_changed || self.labels_changed
    }

    /// Check if this is a scale-up operation
    pub fn is_scale_up(&self) -> bool {
        self.replicas_changed && self.replica_delta > 0
    }

    /// Check if this is a scale-down operation
    pub fn is_scale_down(&self) -> bool {
        self.replicas_changed && self.replica_delta < 0
    }
}

/// Validate spec changes between old and new resource specs
pub fn validate_spec_change(old: &MyResource, new: &MyResource) -> Result<SpecDiff> {
    let old_spec = &old.spec;
    let new_spec = &new.spec;

    // First validate the new spec
    validate_spec(new)?;

    // Calculate replica delta
    let replica_delta = new_spec.replicas - old_spec.replicas;

    // Validate scale down
    if replica_delta < 0 {
        validate_scale_down(old, new)?;
    }

    let diff = SpecDiff {
        replicas_changed: old_spec.replicas != new_spec.replicas,
        replica_delta,
        message_changed: old_spec.message != new_spec.message,
        labels_changed: old_spec.labels != new_spec.labels,
    };

    Ok(diff)
}

/// Validate scale down operation
fn validate_scale_down(old: &MyResource, new: &MyResource) -> Result<()> {
    let new_replicas = new.spec.replicas;

    // Ensure we don't go below minimum
    if new_replicas < MIN_REPLICAS {
        return Err(Error::Validation(format!(
            "cannot scale down to {} replicas, minimum is {}",
            new_replicas, MIN_REPLICAS
        )));
    }

    // Warn about aggressive scale down (more than 50% at once)
    let old_replicas = old.spec.replicas;
    let delta = old_replicas - new_replicas;
    if old_replicas > 2 && delta > old_replicas / 2 {
        tracing::warn!(
            old_replicas,
            new_replicas,
            delta,
            "Large scale down detected. Consider scaling down gradually."
        );
    }

    Ok(())
}

/// Check if generation has changed (spec update)
pub fn generation_changed(resource: &MyResource) -> bool {
    let generation = resource.metadata.generation;
    let observed = resource.status.as_ref().and_then(|s| s.observed_generation);

    match (generation, observed) {
        (Some(current_gen), Some(obs)) => current_gen != obs,
        (Some(_), None) => true, // No observed generation, assume changed
        _ => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::crd::{MyResourceSpec, MyResourceStatus};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_test_resource(replicas: i32, message: &str) -> MyResource {
        MyResource {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: MyResourceSpec {
                replicas,
                message: message.to_string(),
                labels: BTreeMap::new(),
            },
            status: Some(MyResourceStatus::default()),
        }
    }

    #[test]
    fn test_validate_replicas_valid() {
        let resource = create_test_resource(3, "test");
        assert!(validate_spec(&resource).is_ok());
    }

    #[test]
    fn test_validate_replicas_too_low() {
        let resource = create_test_resource(0, "test");
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_replicas_too_high() {
        let resource = create_test_resource(100, "test");
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_replicas_at_bounds() {
        let min_resource = create_test_resource(MIN_REPLICAS, "test");
        assert!(validate_spec(&min_resource).is_ok());

        let max_resource = create_test_resource(MAX_REPLICAS, "test");
        assert!(validate_spec(&max_resource).is_ok());
    }

    #[test]
    fn test_spec_diff_replicas_change() {
        let old = create_test_resource(2, "test");
        let new = create_test_resource(3, "test");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, 1);
        assert!(!diff.message_changed);
        assert!(diff.is_scale_only());
        assert!(diff.is_scale_up());
    }

    #[test]
    fn test_spec_diff_message_change() {
        let old = create_test_resource(2, "old message");
        let new = create_test_resource(2, "new message");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(!diff.replicas_changed);
        assert!(diff.message_changed);
        assert!(diff.requires_update());
        assert!(!diff.is_scale_only());
    }

    #[test]
    fn test_spec_diff_no_change() {
        let old = create_test_resource(2, "test");
        let new = create_test_resource(2, "test");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(!diff.has_changes());
    }

    #[test]
    fn test_spec_diff_scale_down() {
        let old = create_test_resource(5, "test");
        let new = create_test_resource(3, "test");

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.replicas_changed);
        assert_eq!(diff.replica_delta, -2);
        assert!(diff.is_scale_down());
    }

    #[test]
    fn test_generation_changed() {
        let mut resource = create_test_resource(2, "test");
        resource.metadata.generation = Some(2);
        resource.status = Some(MyResourceStatus {
            observed_generation: Some(1),
            ..Default::default()
        });

        assert!(generation_changed(&resource));
    }

    #[test]
    fn test_generation_unchanged() {
        let mut resource = create_test_resource(2, "test");
        resource.metadata.generation = Some(1);
        resource.status = Some(MyResourceStatus {
            observed_generation: Some(1),
            ..Default::default()
        });

        assert!(!generation_changed(&resource));
    }
}
