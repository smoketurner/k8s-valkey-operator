//! Status update logic for ValkeyCluster.

use kube::{
    ResourceExt,
    api::{Patch, PatchParams},
};
use tracing::debug;

use crate::{
    controller::error::Error,
    crd::{ClusterPhase, Condition, ValkeyCluster, ValkeyClusterStatus, total_pods},
    resources::certificate,
    slots::TOTAL_SLOTS,
};

/// Health check result containing cluster state information.
pub struct ClusterHealthStatus {
    /// Whether the cluster is healthy (state=ok, all slots assigned).
    pub is_healthy: bool,
    /// Number of healthy master nodes.
    pub healthy_masters: i32,
    /// Number of healthy replica nodes.
    pub healthy_replicas: i32,
    /// Total slots assigned.
    pub slots_assigned: i32,
    /// Cluster topology if available.
    pub topology: Option<crate::crd::ClusterTopology>,
}

impl From<crate::client::ClusterState> for ClusterHealthStatus {
    fn from(state: crate::client::ClusterState) -> Self {
        ClusterHealthStatus {
            is_healthy: state.is_healthy(0),
            healthy_masters: state.healthy_masters_count(),
            healthy_replicas: state.healthy_replicas_count(),
            slots_assigned: state.cluster_info.slots_assigned,
            topology: state.topology,
        }
    }
}

/// All parameters needed to update ValkeyCluster status in one reconciliation.
///
/// Replaces the 7-positional-parameter `update_status` function.
pub(crate) struct StatusUpdate<'a> {
    pub(crate) api: &'a kube::Api<ValkeyCluster>,
    pub(crate) name: &'a str,
    pub(crate) phase: ClusterPhase,
    pub(crate) ready_replicas: i32,
    pub(crate) error_message: Option<&'a str>,
    pub(crate) health_status: Option<&'a ClusterHealthStatus>,
    /// The generation captured at reconcile start — prevents racing against concurrent spec changes.
    pub(crate) generation: Option<i64>,
}

impl<'a> StatusUpdate<'a> {
    pub(crate) async fn apply(self) -> Result<(), Error> {
        let obj = self.api.get(self.name).await?;

        let generation = self.generation.or(obj.metadata.generation);
        let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

        let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

        let conditions = if self.phase == ClusterPhase::Running {
            vec![Condition::ready(
                true,
                "AllReplicasReady",
                "All replicas are ready",
                generation,
            )]
        } else if self.phase == ClusterPhase::Failed {
            vec![Condition::ready(
                false,
                "ReconciliationFailed",
                self.error_message.unwrap_or("Resource failed"),
                generation,
            )]
        } else {
            vec![Condition::progressing(
                true,
                "Reconciling",
                &format!("Phase: {}", self.phase),
                generation,
            )]
        };

        let connection_endpoint = if self.phase == ClusterPhase::Running {
            Some(format!("valkey://{}.{}.svc:6379", self.name, namespace))
        } else {
            None
        };

        let (assigned_slots, topology, ready_masters) = if let Some(health) = self.health_status {
            (
                format!("{}/{TOTAL_SLOTS}", health.slots_assigned),
                health.topology.clone(),
                health.healthy_masters,
            )
        } else if self.phase == ClusterPhase::Running {
            (
                format!("{TOTAL_SLOTS}/{TOTAL_SLOTS}"),
                None,
                obj.spec.masters,
            )
        } else {
            (format!("0/{TOTAL_SLOTS}"), None, 0)
        };

        let tls_secret_name = certificate::certificate_secret_name(&obj);

        let existing_status = obj.status.as_ref();
        let current_operation = existing_status.and_then(|s| s.current_operation.clone());
        let operation_started_at = existing_status.and_then(|s| s.operation_started_at.clone());
        let connection_secret = Some(obj.spec.auth.secret_ref.name.clone());
        let operation_progress = existing_status.and_then(|s| s.operation_progress.clone());
        let message = existing_status
            .map(|s| s.message.clone())
            .unwrap_or_default();
        let last_phase_transition = existing_status.and_then(|s| s.last_phase_transition.clone());

        let prev_phase = existing_status
            .map(|s| s.phase)
            .unwrap_or(ClusterPhase::Pending);
        let reconcile_count = if self.phase == prev_phase {
            existing_status.map(|s| s.reconcile_count).unwrap_or(0) + 1
        } else {
            0
        };

        let last_phase_transition = if self.phase != prev_phase {
            Some(jiff::Timestamp::now().to_string())
        } else {
            last_phase_transition
        };

        let (recovery_attempts, last_recovery_attempt) = if self.phase != ClusterPhase::Failed {
            (0, None)
        } else {
            (
                existing_status.map(|s| s.recovery_attempts).unwrap_or(0),
                existing_status.and_then(|s| s.last_recovery_attempt.clone()),
            )
        };

        let (last_error, summary) = if self.phase == ClusterPhase::Running {
            let slots = self
                .health_status
                .map(|h| h.slots_assigned)
                .unwrap_or_else(|| i32::from(TOTAL_SLOTS));
            let healthy_replicas = self.health_status.map(|h| h.healthy_replicas).unwrap_or(0);
            (
                None,
                Some(format!(
                    "Healthy: {} masters, {} replicas, {}/{TOTAL_SLOTS} slots",
                    ready_masters, healthy_replicas, slots
                )),
            )
        } else if self.phase == ClusterPhase::Failed {
            let err = self
                .error_message
                .map(|e| e.to_string())
                .or_else(|| existing_status.and_then(|s| s.last_error.clone()));
            (
                err,
                Some(format!(
                    "Failed: {}",
                    self.error_message.unwrap_or("unknown error")
                )),
            )
        } else {
            let err = existing_status.and_then(|s| s.last_error.clone());
            (err, Some(format!("Phase: {}", self.phase)))
        };

        let status = ValkeyClusterStatus {
            phase: self.phase,
            ready_nodes: format!("{}/{}", self.ready_replicas, desired_replicas),
            ready_masters,
            ready_replicas: self.ready_replicas,
            assigned_slots,
            observed_generation: generation,
            conditions,
            topology,
            valkey_version: Some("9".to_string()),
            connection_endpoint,
            connection_secret,
            tls_secret: Some(tls_secret_name),
            current_operation,
            operation_started_at,
            operation_progress,
            message,
            reconcile_count,
            last_phase_transition,
            recovery_attempts,
            last_recovery_attempt,
            last_error,
            summary,
        };

        let patch = serde_json::json!({ "status": status });

        debug!(name = %self.name, phase = %self.phase, "Updating cluster status");

        self.api
            .patch_status(self.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await?;

        Ok(())
    }
}
