//! Kubernetes Lease-based operation locks for cluster coordination.
//!
//! This module provides distributed locking using Kubernetes Lease resources.
//! Leases have built-in TTL via `spec.leaseDurationSeconds` and automatic
//! cleanup when the holder crashes (lease expires).
//!
//! Benefits over status-based locks:
//! - Built-in TTL prevents stuck locks if operator crashes mid-operation
//! - Designed for distributed coordination (same pattern as leader election)
//! - Clean separation of concerns (lock state not mixed with cluster status)

use jiff::Timestamp;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta, OwnerReference};
use kube::api::{Api, DeleteParams, Patch, PatchParams};
use kube::{Client, Resource, ResourceExt};
use tracing::{debug, info, warn};

use crate::controller::cluster_reconciler::FIELD_MANAGER;
use crate::controller::error::Error;
use crate::crd::ValkeyCluster;

/// Default lease duration in seconds.
/// If the operator crashes, the lock will be released after this time.
const LEASE_DURATION_SECONDS: i32 = 300; // 5 minutes

/// Types of operations that can be performed on a cluster.
/// These are stored as the lease holder identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationType {
    /// Cluster initialization (CLUSTER MEET, slot assignment, replica setup).
    Initializing,
    /// Scaling operation (adding/removing nodes, slot migration).
    Scaling,
    /// Upgrade operation (rolling upgrade with failover).
    Upgrading,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Initializing => write!(f, "initializing"),
            OperationType::Scaling => write!(f, "scaling"),
            OperationType::Upgrading => write!(f, "upgrading"),
        }
    }
}

impl std::str::FromStr for OperationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "initializing" => Ok(OperationType::Initializing),
            "scaling" => Ok(OperationType::Scaling),
            "upgrading" => Ok(OperationType::Upgrading),
            _ => Err(format!("Unknown operation type: {}", s)),
        }
    }
}

/// Operation lock using Kubernetes Lease resources.
///
/// Provides distributed locking with automatic expiration via Lease TTL.
pub struct OperationLock {
    client: Client,
    namespace: String,
    cluster_name: String,
}

impl OperationLock {
    /// Create a new operation lock for a cluster.
    pub fn new(client: Client, namespace: &str, cluster_name: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            cluster_name: cluster_name.to_string(),
        }
    }

    /// Get the lease name for this cluster's operation lock.
    fn lease_name(&self) -> String {
        format!("{}-operation-lock", self.cluster_name)
    }

    /// Convert jiff::Timestamp to MicroTime for Lease API.
    fn timestamp_to_microtime(ts: Timestamp) -> MicroTime {
        MicroTime(ts)
    }

    /// Check if the lease is expired based on renew time and duration.
    fn is_lease_expired(lease: &Lease) -> bool {
        if let Some(spec) = &lease.spec
            && let (Some(renew_time), Some(duration)) =
                (&spec.renew_time, spec.lease_duration_seconds)
        {
            let now = Timestamp::now();
            let elapsed_secs = now.as_second() - renew_time.0.as_second();
            return elapsed_secs > i64::from(duration);
        }
        // No valid spec means expired (or never acquired)
        true
    }

    /// Get the current holder of the lock, if any.
    fn get_holder(lease: &Lease) -> Option<String> {
        lease
            .spec
            .as_ref()
            .and_then(|s| s.holder_identity.as_ref())
            .cloned()
    }

    /// Acquire the operation lock.
    ///
    /// Returns `Ok(())` if the lock was acquired successfully.
    /// Returns `Err(OperationLocked)` if another operation holds the lock.
    /// Returns `Err(...)` for other errors.
    pub async fn acquire(&self, operation: OperationType) -> Result<(), Error> {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lease_name = self.lease_name();

        // Check existing lease
        match api.get(&lease_name).await {
            Ok(lease) => {
                // Check if lease is expired
                if !Self::is_lease_expired(&lease) {
                    let holder = Self::get_holder(&lease).unwrap_or_else(|| "unknown".to_string());
                    // Check if same operation is already holding the lock (allow idempotent acquire)
                    if holder == operation.to_string() {
                        debug!(
                            cluster = %self.cluster_name,
                            operation = %operation,
                            "Operation already holds lock, renewing"
                        );
                        return self.renew(operation).await;
                    }
                    return Err(Error::OperationLocked {
                        current_holder: holder,
                    });
                }
                // Lease expired, we can take over
                debug!(
                    cluster = %self.cluster_name,
                    operation = %operation,
                    "Existing lease expired, acquiring"
                );
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // No lease exists, create new one
                debug!(
                    cluster = %self.cluster_name,
                    operation = %operation,
                    "No existing lease, creating new one"
                );
            }
            Err(e) => return Err(e.into()),
        }

        // Create or update lease
        let now = Self::timestamp_to_microtime(Timestamp::now());
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(lease_name.clone()),
                namespace: Some(self.namespace.clone()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(operation.to_string()),
                lease_duration_seconds: Some(LEASE_DURATION_SECONDS),
                acquire_time: Some(now.clone()),
                renew_time: Some(now),
                lease_transitions: Some(1),
                ..Default::default()
            }),
        };

        api.patch(
            &lease_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&lease),
        )
        .await?;

        info!(
            cluster = %self.cluster_name,
            operation = %operation,
            "Operation lock acquired (lease-based)"
        );

        Ok(())
    }

    /// Acquire the lock with an owner reference to the ValkeyCluster.
    ///
    /// The owner reference ensures the lease is garbage collected when the cluster is deleted.
    pub async fn acquire_with_owner(
        &self,
        operation: OperationType,
        owner: &ValkeyCluster,
    ) -> Result<(), Error> {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lease_name = self.lease_name();

        // Check existing lease first
        match api.get(&lease_name).await {
            Ok(lease) => {
                if !Self::is_lease_expired(&lease) {
                    let holder = Self::get_holder(&lease).unwrap_or_else(|| "unknown".to_string());
                    if holder == operation.to_string() {
                        return self.renew(operation).await;
                    }
                    return Err(Error::OperationLocked {
                        current_holder: holder,
                    });
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(e.into()),
        }

        // Build owner reference
        let owner_ref = OwnerReference {
            api_version: ValkeyCluster::api_version(&()).into_owned(),
            kind: ValkeyCluster::kind(&()).into_owned(),
            name: owner.name_any(),
            uid: owner.uid().unwrap_or_default(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        };

        let now = Self::timestamp_to_microtime(Timestamp::now());
        let lease = Lease {
            metadata: ObjectMeta {
                name: Some(lease_name.clone()),
                namespace: Some(self.namespace.clone()),
                owner_references: Some(vec![owner_ref]),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(operation.to_string()),
                lease_duration_seconds: Some(LEASE_DURATION_SECONDS),
                acquire_time: Some(now.clone()),
                renew_time: Some(now),
                lease_transitions: Some(1),
                ..Default::default()
            }),
        };

        api.patch(
            &lease_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&lease),
        )
        .await?;

        info!(
            cluster = %self.cluster_name,
            operation = %operation,
            "Operation lock acquired with owner reference"
        );

        Ok(())
    }

    /// Renew the lease (call periodically during long operations).
    ///
    /// This updates the renew time to prevent the lease from expiring.
    pub async fn renew(&self, operation: OperationType) -> Result<(), Error> {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lease_name = self.lease_name();

        // Verify we still hold the lock before renewing
        match api.get(&lease_name).await {
            Ok(lease) => {
                let holder = Self::get_holder(&lease);
                if holder.as_deref() != Some(&operation.to_string()) {
                    warn!(
                        cluster = %self.cluster_name,
                        operation = %operation,
                        current_holder = ?holder,
                        "Cannot renew lease - not the current holder"
                    );
                    return Err(Error::OperationLocked {
                        current_holder: holder.unwrap_or_else(|| "unknown".to_string()),
                    });
                }
            }
            Err(e) => return Err(e.into()),
        }

        let now = Self::timestamp_to_microtime(Timestamp::now());
        let patch = serde_json::json!({
            "spec": {
                "renewTime": now,
            }
        });

        api.patch(
            &lease_name,
            &PatchParams::apply(FIELD_MANAGER),
            &Patch::Merge(&patch),
        )
        .await?;

        debug!(
            cluster = %self.cluster_name,
            operation = %operation,
            "Operation lock renewed"
        );

        Ok(())
    }

    /// Release the lock.
    ///
    /// Only releases if the specified operation is the current holder.
    pub async fn release(&self, operation: OperationType) -> Result<(), Error> {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lease_name = self.lease_name();

        // Verify we hold the lock before deleting
        match api.get(&lease_name).await {
            Ok(lease) => {
                let holder = Self::get_holder(&lease);
                if holder.as_deref() != Some(&operation.to_string()) {
                    warn!(
                        cluster = %self.cluster_name,
                        operation = %operation,
                        current_holder = ?holder,
                        "Cannot release lease - not the current holder"
                    );
                    // Don't return error - if we don't hold it, that's fine
                    return Ok(());
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // Already deleted
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        api.delete(&lease_name, &DeleteParams::default()).await?;

        info!(
            cluster = %self.cluster_name,
            operation = %operation,
            "Operation lock released"
        );

        Ok(())
    }

    /// Check if an operation can be started (no active lock or same operation).
    pub async fn check_allowed(&self, operation: OperationType) -> Result<(), Error> {
        let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
        let lease_name = self.lease_name();

        match api.get(&lease_name).await {
            Ok(lease) => {
                if Self::is_lease_expired(&lease) {
                    return Ok(());
                }
                let holder = Self::get_holder(&lease).unwrap_or_else(|| "unknown".to_string());
                if holder == operation.to_string() {
                    // Same operation, allowed
                    return Ok(());
                }
                Err(Error::OperationLocked {
                    current_holder: holder,
                })
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // No lease, allowed
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
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

    #[test]
    fn test_operation_type_display() {
        assert_eq!(OperationType::Initializing.to_string(), "initializing");
        assert_eq!(OperationType::Scaling.to_string(), "scaling");
        assert_eq!(OperationType::Upgrading.to_string(), "upgrading");
    }

    #[test]
    fn test_operation_type_from_str() {
        assert_eq!(
            "initializing".parse::<OperationType>().unwrap(),
            OperationType::Initializing
        );
        assert_eq!(
            "scaling".parse::<OperationType>().unwrap(),
            OperationType::Scaling
        );
        assert_eq!(
            "upgrading".parse::<OperationType>().unwrap(),
            OperationType::Upgrading
        );
        assert!("unknown".parse::<OperationType>().is_err());
    }

    #[test]
    fn test_lease_name() {
        // Cannot test without a client, but verify the format
        let expected = "my-cluster-operation-lock";
        let cluster_name = "my-cluster";
        assert_eq!(format!("{}-operation-lock", cluster_name), expected);
    }

    #[test]
    fn test_is_lease_expired_no_spec() {
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: None,
        };
        assert!(OperationLock::is_lease_expired(&lease));
    }

    #[test]
    fn test_is_lease_expired_no_renew_time() {
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some("scaling".to_string()),
                lease_duration_seconds: Some(300),
                renew_time: None,
                ..Default::default()
            }),
        };
        assert!(OperationLock::is_lease_expired(&lease));
    }

    #[test]
    fn test_is_lease_expired_fresh() {
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some("scaling".to_string()),
                lease_duration_seconds: Some(300),
                renew_time: Some(MicroTime(Timestamp::now())),
                ..Default::default()
            }),
        };
        assert!(!OperationLock::is_lease_expired(&lease));
    }

    #[test]
    fn test_is_lease_expired_old() {
        let old_time = Timestamp::now() - jiff::SignedDuration::from_secs(400);
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some("scaling".to_string()),
                lease_duration_seconds: Some(300),
                renew_time: Some(MicroTime(old_time)),
                ..Default::default()
            }),
        };
        assert!(OperationLock::is_lease_expired(&lease));
    }

    #[test]
    fn test_get_holder() {
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some("scaling".to_string()),
                ..Default::default()
            }),
        };
        assert_eq!(
            OperationLock::get_holder(&lease),
            Some("scaling".to_string())
        );
    }

    #[test]
    fn test_get_holder_none() {
        let lease = Lease {
            metadata: ObjectMeta::default(),
            spec: None,
        };
        assert_eq!(OperationLock::get_holder(&lease), None);
    }
}
