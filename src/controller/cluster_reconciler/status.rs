//! Status update logic for ValkeyCluster.

use kube::{
    ResourceExt,
    api::{Patch, PatchParams},
};
use tracing::debug;

use crate::{
    controller::{error::Error, tls_status::TlsObservation},
    crd::{
        ClusterPhase, Condition, ValkeyCluster, ValkeyClusterStatus, new_condition, total_pods,
        tri_state_condition,
    },
    resources::certificate,
    slots::TOTAL_SLOTS,
};

/// Replica replication lag in bytes above which the cluster is considered
/// not in sync. 1 MiB — generously above normal sustained lag under writes,
/// well below values that indicate a real replication problem.
pub const REPLICATION_LAG_TOLERANCE_BYTES: i64 = 1_048_576;

fn is_progressing(phase: ClusterPhase) -> bool {
    matches!(
        phase,
        ClusterPhase::Pending
            | ClusterPhase::Creating
            | ClusterPhase::WaitingForPods
            | ClusterPhase::InitializingCluster
            | ClusterPhase::AssigningSlots
            | ClusterPhase::ConfiguringReplicas
            | ClusterPhase::ScalingUpStatefulSet
            | ClusterPhase::WaitingForNewPods
            | ClusterPhase::AddingNodesToCluster
            | ClusterPhase::RebalancingSlots
            | ClusterPhase::ConfiguringNewReplicas
            | ClusterPhase::EvacuatingSlots
            | ClusterPhase::RemovingNodesFromCluster
            | ClusterPhase::ScalingDownStatefulSet
            | ClusterPhase::VerifyingClusterHealth
    )
}

fn is_degraded(phase: ClusterPhase, ready_pods: i32, desired_pods: i32) -> bool {
    phase == ClusterPhase::Degraded
        || phase == ClusterPhase::Failed
        || (phase == ClusterPhase::Running && ready_pods < desired_pods)
}

fn slots_are_stable(phase: ClusterPhase) -> bool {
    !matches!(
        phase,
        ClusterPhase::RebalancingSlots | ClusterPhase::EvacuatingSlots
    )
}

fn ready_reason(phase: ClusterPhase) -> (&'static str, &'static str) {
    match phase {
        ClusterPhase::Pending | ClusterPhase::Creating => {
            ("Creating", "Cluster resources are being created.")
        }
        ClusterPhase::WaitingForPods | ClusterPhase::WaitingForNewPods => {
            ("WaitingForPods", "Waiting for pods to become running.")
        }
        ClusterPhase::InitializingCluster => ("Initializing", "Cluster nodes are being connected."),
        ClusterPhase::AssigningSlots => (
            "AssigningSlots",
            "Hash slots are being assigned to masters.",
        ),
        ClusterPhase::ConfiguringReplicas | ClusterPhase::ConfiguringNewReplicas => (
            "ConfiguringReplicas",
            "Replica relationships are being configured.",
        ),
        ClusterPhase::ScalingUpStatefulSet
        | ClusterPhase::AddingNodesToCluster
        | ClusterPhase::RebalancingSlots => {
            ("ScaleUpInProgress", "Cluster scale-up is in progress.")
        }
        ClusterPhase::EvacuatingSlots
        | ClusterPhase::RemovingNodesFromCluster
        | ClusterPhase::ScalingDownStatefulSet => {
            ("ScaleDownInProgress", "Cluster scale-down is in progress.")
        }
        ClusterPhase::VerifyingClusterHealth => (
            "VerifyingHealth",
            "Verifying cluster health before transitioning to Running.",
        ),
        ClusterPhase::Degraded => (
            "Degraded",
            "Cluster is degraded: some pods are unavailable.",
        ),
        ClusterPhase::Failed => (
            "ReconciliationFailed",
            "Cluster reconciliation has failed and requires intervention.",
        ),
        ClusterPhase::Deleting => ("Deleting", "Cluster is being deleted."),
        ClusterPhase::Running => (
            "PodsNotReady",
            "Cluster is running but not all pods are ready.",
        ),
    }
}

/// Derive the canonical condition set for a ValkeyCluster.
///
/// Three phase-driven conditions (`Ready`, `Progressing`, `Degraded`), one
/// phase-driven slot-migration signal (`SlotsStable`), and two tri-state
/// observation-driven conditions (`ReplicasInSync`, `TLSReady`). Tri-state
/// conditions use status `"Unknown"` when the operator has no observation
/// for them this reconcile.
///
/// # Example
///
/// ```
/// use valkey_operator::controller::cluster_reconciler::derive_cluster_conditions;
/// use valkey_operator::crd::ClusterPhase;
///
/// let conditions = derive_cluster_conditions(ClusterPhase::Running, 6, 6, Some(1), None, None);
/// assert!(conditions.iter().any(|c| c.type_ == "Ready" && c.status == "True"));
/// assert!(conditions.iter().any(|c| c.type_ == "TLSReady" && c.status == "Unknown"));
/// ```
pub fn derive_cluster_conditions(
    phase: ClusterPhase,
    ready_pods: i32,
    desired_pods: i32,
    generation: Option<i64>,
    health_status: Option<&ClusterHealthStatus>,
    tls: Option<&TlsObservation>,
) -> Vec<Condition> {
    let ready = phase == ClusterPhase::Running && ready_pods == desired_pods && desired_pods > 0;
    let progressing = is_progressing(phase);
    let degraded = is_degraded(phase, ready_pods, desired_pods);
    let slots_stable = slots_are_stable(phase);
    let replicas_in_sync = health_status
        .and_then(|h| h.max_replica_lag)
        .map(|lag| lag < REPLICATION_LAG_TOLERANCE_BYTES);
    let (rr, rm) = if ready {
        (
            "ClusterHealthy",
            "Cluster is running and all pods are ready.",
        )
    } else {
        ready_reason(phase)
    };

    let (tls_ready, tls_reason, tls_message) = match tls {
        Some(obs) => (obs.ready, obs.reason, obs.message.clone()),
        None => (
            None,
            "NotObserved",
            "TLS certificate has not been observed this reconcile.".to_string(),
        ),
    };

    vec![
        new_condition("Ready", ready, rr, rm, generation),
        new_condition(
            "Progressing",
            progressing,
            if progressing {
                "ReconcileInProgress"
            } else {
                "ReconcileComplete"
            },
            if progressing {
                "Operator is reconciling the cluster."
            } else {
                "Cluster has reached a stable state."
            },
            generation,
        ),
        new_condition(
            "Degraded",
            degraded,
            if degraded {
                "ClusterDegraded"
            } else {
                "ClusterHealthy"
            },
            if degraded {
                "Cluster is in a degraded state."
            } else {
                "Cluster is not degraded."
            },
            generation,
        ),
        new_condition(
            "SlotsStable",
            slots_stable,
            if slots_stable {
                "SlotsStable"
            } else {
                "SlotMigrationInProgress"
            },
            if slots_stable {
                "No slot migration is in progress."
            } else {
                "Slot migration is in progress."
            },
            generation,
        ),
        tri_state_condition(
            "ReplicasInSync",
            replicas_in_sync,
            match replicas_in_sync {
                Some(true) => "AllReplicasInSync",
                Some(false) => "ReplicaLagExceedsTolerance",
                None => "NotObserved",
            },
            match replicas_in_sync {
                Some(true) => "All replicas are within the replication-lag tolerance.",
                Some(false) => "At least one replica's replication lag exceeds tolerance.",
                None => "Replica replication lag has not been observed this reconcile.",
            },
            generation,
        ),
        tri_state_condition("TLSReady", tls_ready, tls_reason, &tls_message, generation),
    ]
}

/// Merge a new set of conditions over an existing set, preserving `last_transition_time`
/// for conditions whose `status` value has not changed.
pub fn merge_conditions(existing: &[Condition], new: Vec<Condition>) -> Vec<Condition> {
    let mut merged = Vec::with_capacity(new.len());
    for mut condition in new {
        let old = existing.iter().find(|c| c.type_ == condition.type_);
        if let Some(prev) = old
            && prev.status == condition.status
        {
            condition.last_transition_time = prev.last_transition_time.clone();
        }
        merged.push(condition);
    }
    merged
}

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
    /// Highest observed replica replication lag in bytes, across all replicas
    /// queried during this reconcile. `None` when no replica could be observed
    /// (typically because the cluster is still being created or the operator
    /// cannot reach any pod). Used to derive the `ReplicasInSync` condition.
    pub max_replica_lag: Option<i64>,
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
    pub(crate) tls: Option<&'a TlsObservation>,
    /// The generation captured at reconcile start — prevents racing against concurrent spec changes.
    pub(crate) generation: Option<i64>,
}

impl<'a> StatusUpdate<'a> {
    pub(crate) async fn apply(self) -> Result<(), Error> {
        let obj = self.api.get(self.name).await?;

        let generation = self.generation.or(obj.metadata.generation);
        let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

        let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

        let existing_conditions = obj
            .status
            .as_ref()
            .map(|s| s.conditions.as_slice())
            .unwrap_or(&[]);
        let new_conditions = derive_cluster_conditions(
            self.phase,
            self.ready_replicas,
            desired_replicas,
            generation,
            self.health_status,
            self.tls,
        );
        let conditions = merge_conditions(existing_conditions, new_conditions);

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

        // Label selector for the scale subresource — must match the pods owned by
        // this cluster so HPA can read replica counts. Mirrors pod_selector_labels
        // in src/resources/common.rs.
        let selector = Some(format!(
            "app.kubernetes.io/name={},app.kubernetes.io/instance={}",
            self.name, self.name
        ));

        let status = ValkeyClusterStatus {
            phase: self.phase,
            ready_nodes: format!("{}/{}", self.ready_replicas, desired_replicas),
            ready_masters,
            ready_replicas: self.ready_replicas,
            selector,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn find_condition<'a>(conditions: &'a [Condition], ty: &str) -> &'a Condition {
        conditions
            .iter()
            .find(|c| c.type_ == ty)
            .expect("condition should exist")
    }

    fn assert_true(conditions: &[Condition], ty: &str) {
        assert_eq!(
            find_condition(conditions, ty).status,
            "True",
            "{} should be True",
            ty
        );
    }

    fn assert_false(conditions: &[Condition], ty: &str) {
        assert_eq!(
            find_condition(conditions, ty).status,
            "False",
            "{} should be False",
            ty
        );
    }

    #[test]
    fn test_running_all_ready() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, Some(1), None, None);
        assert_eq!(c.len(), 6);
        assert_true(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_false(&c, "Degraded");
        assert_true(&c, "SlotsStable");
    }

    #[test]
    fn test_running_pods_not_ready() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 4, 6, Some(1), None, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_true(&c, "Degraded");
    }

    #[test]
    fn test_pending() {
        let c = derive_cluster_conditions(ClusterPhase::Pending, 0, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_false(&c, "Degraded");
    }

    #[test]
    fn test_creating() {
        let c = derive_cluster_conditions(ClusterPhase::Creating, 0, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_waiting_for_pods() {
        let c = derive_cluster_conditions(ClusterPhase::WaitingForPods, 0, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_initializing_cluster() {
        let c =
            derive_cluster_conditions(ClusterPhase::InitializingCluster, 6, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_assigning_slots() {
        let c = derive_cluster_conditions(ClusterPhase::AssigningSlots, 6, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_configuring_replicas() {
        let c =
            derive_cluster_conditions(ClusterPhase::ConfiguringReplicas, 6, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_scaling_up_statefulset() {
        let c =
            derive_cluster_conditions(ClusterPhase::ScalingUpStatefulSet, 6, 12, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_waiting_for_new_pods() {
        let c = derive_cluster_conditions(ClusterPhase::WaitingForNewPods, 6, 12, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_adding_nodes_to_cluster() {
        let c =
            derive_cluster_conditions(ClusterPhase::AddingNodesToCluster, 12, 12, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_rebalancing_slots() {
        let c = derive_cluster_conditions(ClusterPhase::RebalancingSlots, 12, 12, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_configuring_new_replicas() {
        let c = derive_cluster_conditions(
            ClusterPhase::ConfiguringNewReplicas,
            12,
            12,
            None,
            None,
            None,
        );
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_evacuating_slots() {
        let c = derive_cluster_conditions(ClusterPhase::EvacuatingSlots, 12, 12, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_removing_nodes_from_cluster() {
        let c = derive_cluster_conditions(
            ClusterPhase::RemovingNodesFromCluster,
            12,
            12,
            None,
            None,
            None,
        );
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_scaling_down_statefulset() {
        let c =
            derive_cluster_conditions(ClusterPhase::ScalingDownStatefulSet, 6, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_verifying_cluster_health() {
        let c =
            derive_cluster_conditions(ClusterPhase::VerifyingClusterHealth, 6, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_degraded() {
        let c = derive_cluster_conditions(ClusterPhase::Degraded, 4, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_true(&c, "Degraded");
    }

    #[test]
    fn test_failed() {
        let c = derive_cluster_conditions(ClusterPhase::Failed, 0, 6, None, None, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_true(&c, "Degraded");
    }

    #[test]
    fn test_deleting() {
        let c = derive_cluster_conditions(ClusterPhase::Deleting, 0, 0, None, None, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
    }

    #[test]
    fn test_running_desired_zero_not_ready() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 0, 0, None, None, None);
        assert_false(&c, "Ready");
    }

    #[test]
    fn test_merge_conditions_preserves_timestamp_when_status_unchanged() {
        let old = vec![new_condition(
            "Ready",
            true,
            "ClusterHealthy",
            "All good",
            Some(1),
        )];
        let old_ts = old.first().unwrap().last_transition_time.clone();

        std::thread::sleep(std::time::Duration::from_millis(5));

        let new = vec![new_condition(
            "Ready",
            true,
            "ClusterHealthy",
            "All good",
            Some(1),
        )];
        let merged = merge_conditions(&old, new);
        assert_eq!(
            merged.first().unwrap().last_transition_time,
            old_ts,
            "timestamp should be preserved when status is unchanged"
        );
    }

    #[test]
    fn test_merge_conditions_updates_timestamp_when_status_flips() {
        let old = vec![new_condition(
            "Ready",
            false,
            "Creating",
            "Not ready yet",
            Some(1),
        )];
        let old_ts = old.first().unwrap().last_transition_time.clone();

        std::thread::sleep(std::time::Duration::from_millis(5));

        let new = vec![new_condition(
            "Ready",
            true,
            "ClusterHealthy",
            "All good",
            Some(1),
        )];
        let merged = merge_conditions(&old, new);
        assert_ne!(
            merged.first().unwrap().last_transition_time,
            old_ts,
            "timestamp should be updated when status flips"
        );
    }

    #[test]
    fn test_merge_conditions_adds_new_condition() {
        let old = vec![new_condition(
            "Ready",
            true,
            "ClusterHealthy",
            "OK",
            Some(1),
        )];
        let new = vec![
            new_condition("Ready", true, "ClusterHealthy", "OK", Some(1)),
            new_condition("Progressing", false, "ReconcileComplete", "Done", Some(1)),
        ];
        let merged = merge_conditions(&old, new);
        assert_eq!(merged.len(), 2);
    }

    fn healthy() -> ClusterHealthStatus {
        ClusterHealthStatus {
            is_healthy: true,
            healthy_masters: 3,
            healthy_replicas: 3,
            slots_assigned: 16384,
            topology: None,
            max_replica_lag: Some(0),
        }
    }

    fn unhealthy_with_lag(lag: i64) -> ClusterHealthStatus {
        ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 1,
            healthy_replicas: 0,
            slots_assigned: 16384,
            topology: None,
            max_replica_lag: Some(lag),
        }
    }

    #[test]
    fn test_slots_stable_during_rebalance_is_false() {
        let c = derive_cluster_conditions(ClusterPhase::RebalancingSlots, 12, 12, None, None, None);
        assert_false(&c, "SlotsStable");
    }

    #[test]
    fn test_slots_stable_during_evacuation_is_false() {
        let c = derive_cluster_conditions(ClusterPhase::EvacuatingSlots, 12, 12, None, None, None);
        assert_false(&c, "SlotsStable");
    }

    #[test]
    fn test_slots_stable_in_steady_state_is_true() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, None, None);
        assert_true(&c, "SlotsStable");
    }

    #[test]
    fn test_replicas_in_sync_unknown_when_no_observation() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, None, None);
        assert_eq!(find_condition(&c, "ReplicasInSync").status, "Unknown");
    }

    #[test]
    fn test_replicas_in_sync_unknown_when_no_lag_observed() {
        let mut h = healthy();
        h.max_replica_lag = None;
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, Some(&h), None);
        assert_eq!(find_condition(&c, "ReplicasInSync").status, "Unknown");
    }

    #[test]
    fn test_replicas_in_sync_true_below_threshold() {
        let h = healthy();
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, Some(&h), None);
        assert_true(&c, "ReplicasInSync");
    }

    #[test]
    fn test_replicas_in_sync_true_just_below_threshold() {
        let mut h = healthy();
        h.max_replica_lag = Some(REPLICATION_LAG_TOLERANCE_BYTES - 1);
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, Some(&h), None);
        assert_true(&c, "ReplicasInSync");
    }

    #[test]
    fn test_replicas_in_sync_false_at_threshold() {
        let mut h = healthy();
        h.max_replica_lag = Some(REPLICATION_LAG_TOLERANCE_BYTES);
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, Some(&h), None);
        assert_false(&c, "ReplicasInSync");
    }

    #[test]
    fn test_replicas_in_sync_false_above_threshold() {
        let h = unhealthy_with_lag(REPLICATION_LAG_TOLERANCE_BYTES * 4);
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, Some(&h), None);
        assert_false(&c, "ReplicasInSync");
    }

    fn tls_observation(ready: Option<bool>, reason: &'static str) -> TlsObservation {
        TlsObservation {
            ready,
            reason,
            message: "test".to_string(),
        }
    }

    #[test]
    fn test_tls_ready_unknown_when_no_observation() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, None, None);
        assert_eq!(find_condition(&c, "TLSReady").status, "Unknown");
    }

    #[test]
    fn test_tls_ready_unknown_when_observation_unknown() {
        let obs = tls_observation(None, "CertificateNotObserved");
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, None, Some(&obs));
        assert_eq!(find_condition(&c, "TLSReady").status, "Unknown");
        assert_eq!(
            find_condition(&c, "TLSReady").reason,
            "CertificateNotObserved"
        );
    }

    #[test]
    fn test_tls_ready_true_when_observation_true() {
        let obs = tls_observation(Some(true), "CertificateReady");
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, None, Some(&obs));
        assert_true(&c, "TLSReady");
    }

    #[test]
    fn test_tls_ready_false_when_observation_false() {
        let obs = tls_observation(Some(false), "CertificateExpired");
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, None, None, Some(&obs));
        assert_false(&c, "TLSReady");
    }
}
