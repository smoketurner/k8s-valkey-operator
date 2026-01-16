//! Operation coordination for preventing concurrent cluster operations.
//!
//! This module provides functions to coordinate between different cluster operations
//! (initialization, scaling, upgrades) to prevent conflicts and race conditions.

use jiff::Timestamp;
use kube::api::{Api, Patch, PatchParams};
use tracing::{debug, info, warn};

use crate::controller::cluster_reconciler::FIELD_MANAGER;
use crate::controller::error::Error;
use crate::crd::ValkeyCluster;

/// Types of operations that can be performed on a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Check if an operation can be started on the cluster.
///
/// Returns Ok(()) if the operation can proceed, or Err with a descriptive error
/// if another operation is in progress.
pub async fn check_operation_allowed(
    api: &Api<ValkeyCluster>,
    cluster_name: &str,
    operation: OperationType,
) -> Result<(), Error> {
    let cluster = api.get(cluster_name).await?;

    if let Some(status) = cluster.status.as_ref()
        && let Some(current_op) = status.current_operation.as_ref()
    {
        if current_op != &operation.to_string() {
            return Err(Error::Validation(format!(
                "Cannot start {} operation: {} operation is already in progress (started at {}). Wait for the current operation to complete before starting a new one.",
                operation,
                current_op,
                status
                    .operation_started_at
                    .as_deref()
                    .unwrap_or("unknown time")
            )));
        }
        // Same operation type - allow it (idempotent)
        debug!(
            cluster = %cluster_name,
            operation = %operation,
            "Operation already in progress, allowing continuation"
        );
    }

    Ok(())
}

/// Start an operation by setting the operation lock in cluster status.
///
/// This should be called at the beginning of an operation to prevent
/// concurrent operations from starting.
pub async fn start_operation(
    api: &Api<ValkeyCluster>,
    cluster_name: &str,
    operation: OperationType,
) -> Result<(), Error> {
    let cluster = api.get(cluster_name).await?;
    let mut status = cluster.status.unwrap_or_default();

    // Check if another operation is in progress
    if let Some(current_op) = status.current_operation.as_ref() {
        if current_op != &operation.to_string() {
            return Err(Error::Validation(format!(
                "Cannot start {} operation: {} operation is already in progress (started at {}). Wait for the current operation to complete.",
                operation,
                current_op,
                status
                    .operation_started_at
                    .as_deref()
                    .unwrap_or("unknown time")
            )));
        }
        // Same operation - update timestamp but don't error
        info!(
            cluster = %cluster_name,
            operation = %operation,
            "Operation already started, updating timestamp"
        );
    }

    status.current_operation = Some(operation.to_string());
    status.operation_started_at = Some(Timestamp::now().to_string());

    let patch = serde_json::json!({
        "status": {
            "currentOperation": status.current_operation,
            "operationStartedAt": status.operation_started_at,
        }
    });

    api.patch_status(
        cluster_name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;

    info!(
        cluster = %cluster_name,
        operation = %operation,
        "Operation lock acquired"
    );

    Ok(())
}

/// Complete an operation by clearing the operation lock.
///
/// This should be called when an operation completes (successfully or with failure).
pub async fn complete_operation(
    api: &Api<ValkeyCluster>,
    cluster_name: &str,
    operation: OperationType,
) -> Result<(), Error> {
    let cluster = api.get(cluster_name).await?;
    let mut status = cluster.status.unwrap_or_default();

    // Only clear if this operation is the current one
    if let Some(current_op) = status.current_operation.as_ref() {
        if current_op == &operation.to_string() {
            status.current_operation = None;
            status.operation_started_at = None;

            let patch = serde_json::json!({
                "status": {
                    "currentOperation": null,
                    "operationStartedAt": null,
                }
            });

            api.patch_status(
                cluster_name,
                &PatchParams::apply(FIELD_MANAGER),
                &Patch::Merge(&patch),
            )
            .await?;

            info!(
                cluster = %cluster_name,
                operation = %operation,
                "Operation lock released"
            );
        } else {
            warn!(
                cluster = %cluster_name,
                operation = %operation,
                current_operation = %current_op,
                "Attempted to complete different operation than is in progress"
            );
        }
    }

    Ok(())
}

/// Check if cluster is in a state that allows operations.
///
/// Some operations require the cluster to be in Running state.
pub fn is_cluster_ready_for_operation(cluster: &ValkeyCluster, operation: OperationType) -> bool {
    let phase = cluster
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(crate::crd::ClusterPhase::Pending);

    match operation {
        OperationType::Initializing => {
            // Initialization can happen from Pending, Creating, Initializing, AssigningSlots
            matches!(
                phase,
                crate::crd::ClusterPhase::Pending
                    | crate::crd::ClusterPhase::Creating
                    | crate::crd::ClusterPhase::Initializing
                    | crate::crd::ClusterPhase::AssigningSlots
            )
        }
        OperationType::Scaling => {
            // Scaling requires cluster to be Running
            phase == crate::crd::ClusterPhase::Running
        }
        OperationType::Upgrading => {
            // Upgrades require cluster to be Running
            phase == crate::crd::ClusterPhase::Running
        }
    }
}
