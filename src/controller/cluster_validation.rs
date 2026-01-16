//! Validation logic for ValkeyCluster spec changes.
//!
//! This module provides validation for spec changes, including:
//! - Master count validation (minimum 3 for quorum)
//! - Replicas per master validation
//! - Spec change detection
//! - Immutable field changes

use crate::controller::error::{Error, Result};
use crate::crd::{ValkeyCluster, total_pods};

/// Minimum number of masters (required for cluster quorum)
pub const MIN_MASTERS: i32 = 3;

/// Maximum number of masters
pub const MAX_MASTERS: i32 = 100;

/// Maximum replicas per master
pub const MAX_REPLICAS_PER_MASTER: i32 = 5;

/// Validate the resource spec
pub fn validate_spec(resource: &ValkeyCluster) -> Result<()> {
    validate_masters(resource)?;
    validate_replicas_per_master(resource)?;
    validate_tls(resource)?;
    validate_auth(resource)?;
    Ok(())
}

/// Validate master count
fn validate_masters(resource: &ValkeyCluster) -> Result<()> {
    let masters = resource.spec.masters;

    if masters < MIN_MASTERS {
        return Err(Error::Validation(format!(
            "master count {} is below minimum {} required for cluster quorum",
            masters, MIN_MASTERS
        )));
    }

    if masters > MAX_MASTERS {
        return Err(Error::Validation(format!(
            "master count {} exceeds maximum {}",
            masters, MAX_MASTERS
        )));
    }

    Ok(())
}

/// Validate replicas per master
fn validate_replicas_per_master(resource: &ValkeyCluster) -> Result<()> {
    let replicas = resource.spec.replicas_per_master;

    if replicas < 0 {
        return Err(Error::Validation(
            "replicas per master cannot be negative".to_string(),
        ));
    }

    if replicas > MAX_REPLICAS_PER_MASTER {
        return Err(Error::Validation(format!(
            "replicas per master {} exceeds maximum {}",
            replicas, MAX_REPLICAS_PER_MASTER
        )));
    }

    Ok(())
}

/// Validate TLS configuration (required for security)
fn validate_tls(resource: &ValkeyCluster) -> Result<()> {
    if resource.spec.tls.issuer_ref.name.is_empty() {
        return Err(Error::Validation(
            "tls.issuerRef.name is required (TLS is mandatory for Valkey clusters)".to_string(),
        ));
    }
    Ok(())
}

/// Validate auth configuration (required for security)
fn validate_auth(resource: &ValkeyCluster) -> Result<()> {
    if resource.spec.auth.secret_ref.name.is_empty() {
        return Err(Error::Validation(
            "auth.secretRef.name is required (authentication is mandatory for Valkey clusters)"
                .to_string(),
        ));
    }
    Ok(())
}

/// Result of comparing old and new spec
#[derive(Debug, Clone, Default)]
pub struct SpecDiff {
    /// Number of masters changed
    pub masters_changed: bool,
    /// Master scale direction (positive = up, negative = down)
    pub master_delta: i32,
    /// Replicas per master changed
    pub replicas_per_master_changed: bool,
    /// Labels changed
    pub labels_changed: bool,
    /// Image changed
    pub image_changed: bool,
    /// Resources changed
    pub resources_changed: bool,
}

impl SpecDiff {
    /// Check if any changes require an update to managed resources
    pub fn requires_update(&self) -> bool {
        self.masters_changed
            || self.replicas_per_master_changed
            || self.labels_changed
            || self.image_changed
            || self.resources_changed
    }

    /// Check if this is a scale-only operation
    pub fn is_scale_only(&self) -> bool {
        (self.masters_changed || self.replicas_per_master_changed)
            && !self.labels_changed
            && !self.image_changed
            && !self.resources_changed
    }

    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        self.masters_changed
            || self.replicas_per_master_changed
            || self.labels_changed
            || self.image_changed
            || self.resources_changed
    }

    /// Check if this is a scale-up operation
    pub fn is_scale_up(&self) -> bool {
        self.masters_changed && self.master_delta > 0
    }

    /// Check if this is a scale-down operation
    pub fn is_scale_down(&self) -> bool {
        self.masters_changed && self.master_delta < 0
    }

    /// Calculate total pod delta
    pub fn total_pod_delta(
        &self,
        old_masters: i32,
        old_replicas_per_master: i32,
        new_masters: i32,
        new_replicas_per_master: i32,
    ) -> i32 {
        total_pods(new_masters, new_replicas_per_master)
            - total_pods(old_masters, old_replicas_per_master)
    }
}

/// Validate spec changes between old and new resource specs
pub fn validate_spec_change(old: &ValkeyCluster, new: &ValkeyCluster) -> Result<SpecDiff> {
    let old_spec = &old.spec;
    let new_spec = &new.spec;

    // First validate the new spec
    validate_spec(new)?;

    // Calculate master delta
    let master_delta = new_spec.masters - old_spec.masters;

    // Validate scale down
    if master_delta < 0 {
        validate_scale_down(old, new)?;
    }

    let diff = SpecDiff {
        masters_changed: old_spec.masters != new_spec.masters,
        master_delta,
        replicas_per_master_changed: old_spec.replicas_per_master != new_spec.replicas_per_master,
        labels_changed: old_spec.labels != new_spec.labels,
        image_changed: old_spec.image.repository != new_spec.image.repository
            || old_spec.image.tag != new_spec.image.tag,
        resources_changed: old_spec.resources.requests.cpu != new_spec.resources.requests.cpu
            || old_spec.resources.requests.memory != new_spec.resources.requests.memory
            || old_spec.resources.limits.cpu != new_spec.resources.limits.cpu
            || old_spec.resources.limits.memory != new_spec.resources.limits.memory,
    };

    Ok(diff)
}

/// Validate scale down operation
fn validate_scale_down(old: &ValkeyCluster, new: &ValkeyCluster) -> Result<()> {
    let new_masters = new.spec.masters;

    // Ensure we don't go below minimum
    if new_masters < MIN_MASTERS {
        return Err(Error::Validation(format!(
            "cannot scale down to {} masters, minimum is {}",
            new_masters, MIN_MASTERS
        )));
    }

    // Warn about aggressive scale down (more than 50% at once)
    let old_masters = old.spec.masters;
    let delta = old_masters - new_masters;
    if old_masters > 3 && delta > old_masters / 2 {
        tracing::warn!(
            old_masters,
            new_masters,
            delta,
            "Large scale down detected. Consider scaling down gradually to avoid data loss."
        );
    }

    Ok(())
}

/// Check if generation has changed (spec update)
pub fn generation_changed(resource: &ValkeyCluster) -> bool {
    let generation = resource.metadata.generation;
    let observed = resource.status.as_ref().and_then(|s| s.observed_generation);

    match (generation, observed) {
        (Some(current_gen), Some(obs)) => current_gen != obs,
        (Some(_), None) => true, // No observed generation, assume changed
        _ => false,
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
        AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyClusterSpec, ValkeyClusterStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_test_resource(masters: i32, replicas_per_master: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                generation: Some(1),
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
                },
                labels: BTreeMap::new(),
                ..Default::default()
            },
            status: Some(ValkeyClusterStatus::default()),
        }
    }

    #[test]
    fn test_validate_masters_valid() {
        let resource = create_test_resource(3, 1);
        assert!(validate_spec(&resource).is_ok());
    }

    #[test]
    fn test_validate_masters_too_low() {
        let mut resource = create_test_resource(2, 1);
        resource.spec.masters = 2;
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_masters_too_high() {
        let mut resource = create_test_resource(3, 1);
        resource.spec.masters = 101;
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_masters_at_bounds() {
        let min_resource = create_test_resource(MIN_MASTERS, 1);
        assert!(validate_spec(&min_resource).is_ok());

        let max_resource = create_test_resource(MAX_MASTERS, 1);
        assert!(validate_spec(&max_resource).is_ok());
    }

    #[test]
    fn test_spec_diff_masters_change() {
        let old = create_test_resource(3, 1);
        let new = create_test_resource(6, 1);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.masters_changed);
        assert_eq!(diff.master_delta, 3);
        assert!(!diff.replicas_per_master_changed);
        assert!(diff.is_scale_only());
        assert!(diff.is_scale_up());
    }

    #[test]
    fn test_spec_diff_replicas_per_master_change() {
        let old = create_test_resource(3, 1);
        let new = create_test_resource(3, 2);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(!diff.masters_changed);
        assert!(diff.replicas_per_master_changed);
        assert!(diff.requires_update());
    }

    #[test]
    fn test_spec_diff_no_change() {
        let old = create_test_resource(3, 1);
        let new = create_test_resource(3, 1);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(!diff.has_changes());
    }

    #[test]
    fn test_spec_diff_scale_down() {
        let old = create_test_resource(6, 1);
        let new = create_test_resource(3, 1);

        let diff = validate_spec_change(&old, &new).unwrap();
        assert!(diff.masters_changed);
        assert_eq!(diff.master_delta, -3);
        assert!(diff.is_scale_down());
    }

    #[test]
    fn test_generation_changed() {
        let mut resource = create_test_resource(3, 1);
        resource.metadata.generation = Some(2);
        resource.status = Some(ValkeyClusterStatus {
            observed_generation: Some(1),
            ..Default::default()
        });

        assert!(generation_changed(&resource));
    }

    #[test]
    fn test_generation_unchanged() {
        let mut resource = create_test_resource(3, 1);
        resource.metadata.generation = Some(1);
        resource.status = Some(ValkeyClusterStatus {
            observed_generation: Some(1),
            ..Default::default()
        });

        assert!(!generation_changed(&resource));
    }

    #[test]
    fn test_total_pod_delta() {
        let diff = SpecDiff::default();
        // 3 masters + 3 replicas = 6 pods -> 6 masters + 6 replicas = 12 pods
        let delta = diff.total_pod_delta(3, 1, 6, 1);
        assert_eq!(delta, 6);
    }
}
