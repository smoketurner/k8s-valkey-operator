//! Validation logic for ValkeyCluster spec changes.
//!
//! This module provides validation for spec changes, including:
//! - Master count validation (minimum 3 for quorum)
//! - Replicas per master validation
//! - Spec change detection
//! - Immutable field changes

use crate::controller::error::{Error, Result};
use crate::crd::{ValkeyCluster, ValkeyClusterSpec, total_pods};
use semver::Version;

/// Minimum number of masters (required for cluster quorum)
pub const MIN_MASTERS: i32 = 3;

/// Maximum number of masters
pub const MAX_MASTERS: i32 = 100;

/// Maximum replicas per master
pub const MAX_REPLICAS_PER_MASTER: i32 = 5;

/// Maximum name length to allow room for suffixes like -headless (10 chars)
pub const MAX_NAME_LENGTH: usize = 53;

/// Validate the resource spec
pub fn validate_spec(resource: &ValkeyCluster) -> Result<()> {
    validate_name_length(resource)?;
    validate_masters(resource)?;
    validate_replicas_per_master(resource)?;
    validate_tls(resource)?;
    validate_auth(resource)?;
    validate_resource_quantities(resource)?;
    Ok(())
}

/// Validate the resource name length
fn validate_name_length(resource: &ValkeyCluster) -> Result<()> {
    use kube::ResourceExt;
    let name = resource.name_any();
    if name.len() > MAX_NAME_LENGTH {
        return Err(Error::Validation(format!(
            "name '{}' exceeds maximum length of {} characters (needed for -headless suffix)",
            name, MAX_NAME_LENGTH
        )));
    }
    Ok(())
}

/// Validate resource quantity formats (storage size, CPU, memory)
fn validate_resource_quantities(resource: &ValkeyCluster) -> Result<()> {
    // Validate storage size
    let storage_size = &resource.spec.persistence.size;
    if !is_valid_storage_size(storage_size) {
        return Err(Error::Validation(format!(
            "invalid storage size '{}'. Expected format: <number><unit> where unit is Ki, Mi, Gi, Ti, Pi, or Ei (e.g., 10Gi)",
            storage_size
        )));
    }

    // Validate CPU requests
    let cpu_request = &resource.spec.resources.requests.cpu;
    if !is_valid_cpu(cpu_request) {
        return Err(Error::Validation(format!(
            "invalid CPU request '{}'. Expected format: <number>m or <decimal> (e.g., 100m, 0.5, 1)",
            cpu_request
        )));
    }

    // Validate memory requests
    let memory_request = &resource.spec.resources.requests.memory;
    if !is_valid_memory(memory_request) {
        return Err(Error::Validation(format!(
            "invalid memory request '{}'. Expected format: <number><unit> where unit is Ki, Mi, Gi, or Ti (e.g., 256Mi)",
            memory_request
        )));
    }

    // Validate CPU limits
    let cpu_limit = &resource.spec.resources.limits.cpu;
    if !is_valid_cpu(cpu_limit) {
        return Err(Error::Validation(format!(
            "invalid CPU limit '{}'. Expected format: <number>m or <decimal> (e.g., 100m, 0.5, 1)",
            cpu_limit
        )));
    }

    // Validate memory limits
    let memory_limit = &resource.spec.resources.limits.memory;
    if !is_valid_memory(memory_limit) {
        return Err(Error::Validation(format!(
            "invalid memory limit '{}'. Expected format: <number><unit> where unit is Ki, Mi, Gi, or Ti (e.g., 1Gi)",
            memory_limit
        )));
    }

    Ok(())
}

/// Check if a storage size string is valid
fn is_valid_storage_size(size: &str) -> bool {
    use std::sync::LazyLock;
    // Pattern: ^([0-9]+)(Ki|Mi|Gi|Ti|Pi|Ei)?$
    static STORAGE_RE: LazyLock<Option<regex::Regex>> =
        LazyLock::new(|| regex::Regex::new(r"^([0-9]+)(Ki|Mi|Gi|Ti|Pi|Ei)?$").ok());
    STORAGE_RE.as_ref().is_some_and(|re| re.is_match(size))
}

/// Check if a CPU string is valid
fn is_valid_cpu(cpu: &str) -> bool {
    use std::sync::LazyLock;
    // Pattern: ^([0-9]+m?|[0-9]*\.[0-9]+)$
    static CPU_RE: LazyLock<Option<regex::Regex>> =
        LazyLock::new(|| regex::Regex::new(r"^([0-9]+m?|[0-9]*\.[0-9]+)$").ok());
    CPU_RE.as_ref().is_some_and(|re| re.is_match(cpu))
}

/// Check if a memory string is valid
fn is_valid_memory(memory: &str) -> bool {
    use std::sync::LazyLock;
    // Pattern: ^([0-9]+)(Ki|Mi|Gi|Ti)?$
    static MEMORY_RE: LazyLock<Option<regex::Regex>> =
        LazyLock::new(|| regex::Regex::new(r"^([0-9]+)(Ki|Mi|Gi|Ti)?$").ok());
    MEMORY_RE.as_ref().is_some_and(|re| re.is_match(memory))
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

    // Validate image changes (including downgrade protection)
    validate_image_change(old_spec, new_spec)?;

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

/// Extract the semver version from a tag string.
///
/// Strips common suffixes like "-alpine", "-bookworm", etc. and attempts to
/// parse as semver.
fn extract_version(tag: &str) -> Option<Version> {
    // Strip common suffixes
    let version_part = tag.split('-').next().unwrap_or(tag);

    // Handle tags like "9" (major only) by expanding to "9.0.0"
    let normalized = match version_part.matches('.').count() {
        0 => format!("{}.0.0", version_part),
        1 => format!("{}.0", version_part),
        _ => version_part.to_string(),
    };

    Version::parse(&normalized).ok()
}

/// Check if the new version is a downgrade from the old version.
///
/// Returns true if the new version is lower than the old version.
/// Returns false if versions cannot be parsed (allowing the change).
pub fn is_downgrade(old_tag: &str, new_tag: &str) -> bool {
    match (extract_version(old_tag), extract_version(new_tag)) {
        (Some(old), Some(new)) => new < old,
        _ => false, // Can't determine, allow change
    }
}

/// Validate image change is allowed.
///
/// Validates that:
/// - Downgrade is allowed if `allow_downgrade` is set, otherwise rejected
///
/// Returns Ok(()) if change is allowed, Err with message if not.
pub fn validate_image_change(
    old_spec: &ValkeyClusterSpec,
    new_spec: &ValkeyClusterSpec,
) -> Result<()> {
    if old_spec.image.tag == new_spec.image.tag {
        return Ok(()); // No change
    }

    if is_downgrade(&old_spec.image.tag, &new_spec.image.tag) {
        if !new_spec.image.allow_downgrade {
            return Err(Error::Validation(format!(
                "Downgrade from {} to {} requires spec.image.allowDowngrade=true. \
                 Set this field or use ValkeyUpgrade for controlled version changes.",
                old_spec.image.tag, new_spec.image.tag
            )));
        }
        tracing::info!(
            old_tag = %old_spec.image.tag,
            new_tag = %new_spec.image.tag,
            "Downgrade detected but allowed via allowDowngrade=true"
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
                    ..Default::default()
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

    #[test]
    fn test_extract_version_full() {
        use super::extract_version;
        let v = extract_version("9.0.1-alpine").unwrap();
        assert_eq!(v.major, 9);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 1);
    }

    #[test]
    fn test_extract_version_major_only() {
        use super::extract_version;
        let v = extract_version("9-alpine").unwrap();
        assert_eq!(v.major, 9);
        assert_eq!(v.minor, 0);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn test_extract_version_major_minor() {
        use super::extract_version;
        let v = extract_version("9.1").unwrap();
        assert_eq!(v.major, 9);
        assert_eq!(v.minor, 1);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn test_is_downgrade_true() {
        use super::is_downgrade;
        assert!(is_downgrade("9.0.1-alpine", "8.0.2-alpine"));
        assert!(is_downgrade("9.1.0", "9.0.5"));
        assert!(is_downgrade("10.0.0", "9.5.0"));
    }

    #[test]
    fn test_is_downgrade_false() {
        use super::is_downgrade;
        assert!(!is_downgrade("8.0.2-alpine", "9.0.1-alpine"));
        assert!(!is_downgrade("9.0.0", "9.0.0")); // Same version
        assert!(!is_downgrade("9.0.0", "9.0.1")); // Upgrade
    }

    #[test]
    fn test_validate_image_change_upgrade_allowed() {
        use super::validate_image_change;
        use crate::crd::ImageSpec;

        let old_spec = ValkeyClusterSpec {
            image: ImageSpec {
                tag: "8.0.2-alpine".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let new_spec = ValkeyClusterSpec {
            image: ImageSpec {
                tag: "9.0.1-alpine".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(validate_image_change(&old_spec, &new_spec).is_ok());
    }

    #[test]
    fn test_validate_image_change_downgrade_blocked() {
        use super::validate_image_change;
        use crate::crd::ImageSpec;

        let old_spec = ValkeyClusterSpec {
            image: ImageSpec {
                tag: "9.0.1-alpine".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let new_spec = ValkeyClusterSpec {
            image: ImageSpec {
                tag: "8.0.2-alpine".to_string(),
                allow_downgrade: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let result = validate_image_change(&old_spec, &new_spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("allowDowngrade"));
    }

    #[test]
    fn test_validate_image_change_downgrade_allowed() {
        use super::validate_image_change;
        use crate::crd::ImageSpec;

        let old_spec = ValkeyClusterSpec {
            image: ImageSpec {
                tag: "9.0.1-alpine".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let new_spec = ValkeyClusterSpec {
            image: ImageSpec {
                tag: "8.0.2-alpine".to_string(),
                allow_downgrade: true,
                ..Default::default()
            },
            ..Default::default()
        };

        assert!(validate_image_change(&old_spec, &new_spec).is_ok());
    }
}
