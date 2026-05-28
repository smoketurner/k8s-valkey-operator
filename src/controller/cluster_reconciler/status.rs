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

fn is_cluster_formed(phase: ClusterPhase) -> bool {
    !matches!(
        phase,
        ClusterPhase::Pending
            | ClusterPhase::Creating
            | ClusterPhase::WaitingForPods
            | ClusterPhase::InitializingCluster
    )
}

fn slots_are_assigned(phase: ClusterPhase) -> bool {
    is_cluster_formed(phase)
        && !matches!(
            phase,
            ClusterPhase::AssigningSlots | ClusterPhase::EvacuatingSlots
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

/// Derive the canonical 5-condition set for a ValkeyCluster based on its current phase.
///
/// # Example
///
/// ```
/// use valkey_operator::controller::cluster_reconciler::derive_cluster_conditions;
/// use valkey_operator::crd::ClusterPhase;
///
/// let conditions = derive_cluster_conditions(ClusterPhase::Running, 6, 6, Some(1));
/// assert!(conditions.iter().any(|c| c.r#type == "Ready" && c.status == "True"));
/// ```
pub fn derive_cluster_conditions(
    phase: ClusterPhase,
    ready_pods: i32,
    desired_pods: i32,
    generation: Option<i64>,
) -> Vec<Condition> {
    let ready = phase == ClusterPhase::Running && ready_pods == desired_pods && desired_pods > 0;
    let progressing = is_progressing(phase);
    let degraded = is_degraded(phase, ready_pods, desired_pods);
    let cluster_formed = is_cluster_formed(phase);
    let slots_assigned = slots_are_assigned(phase);
    let (rr, rm) = if ready {
        (
            "ClusterHealthy",
            "Cluster is running and all pods are ready.",
        )
    } else {
        ready_reason(phase)
    };

    vec![
        Condition::new("Ready", ready, rr, rm, generation),
        Condition::new(
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
        Condition::new(
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
        Condition::new(
            "ClusterFormed",
            cluster_formed,
            if cluster_formed {
                "ClusterFormed"
            } else {
                "ClusterNotFormed"
            },
            if cluster_formed {
                "Cluster nodes have been connected via CLUSTER MEET."
            } else {
                "Cluster nodes have not yet been connected."
            },
            generation,
        ),
        Condition::new(
            "SlotsAssigned",
            slots_assigned,
            if slots_assigned {
                "SlotsAssigned"
            } else {
                "SlotsNotAssigned"
            },
            if slots_assigned {
                "All 16384 hash slots have been assigned."
            } else {
                "Hash slots have not yet been fully assigned."
            },
            generation,
        ),
    ]
}

/// Merge a new set of conditions over an existing set, preserving `last_transition_time`
/// for conditions whose `status` value has not changed.
pub fn merge_conditions(existing: &[Condition], new: Vec<Condition>) -> Vec<Condition> {
    let mut merged = Vec::with_capacity(new.len());
    for mut condition in new {
        let old = existing.iter().find(|c| c.r#type == condition.r#type);
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn find_condition<'a>(conditions: &'a [Condition], ty: &str) -> &'a Condition {
        conditions
            .iter()
            .find(|c| c.r#type == ty)
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
        let c = derive_cluster_conditions(ClusterPhase::Running, 6, 6, Some(1));
        assert_eq!(c.len(), 5);
        assert_true(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_false(&c, "Degraded");
        assert_true(&c, "ClusterFormed");
        assert_true(&c, "SlotsAssigned");
    }

    #[test]
    fn test_running_pods_not_ready() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 4, 6, Some(1));
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_true(&c, "Degraded");
    }

    #[test]
    fn test_pending() {
        let c = derive_cluster_conditions(ClusterPhase::Pending, 0, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_false(&c, "Degraded");
        assert_false(&c, "ClusterFormed");
        assert_false(&c, "SlotsAssigned");
    }

    #[test]
    fn test_creating() {
        let c = derive_cluster_conditions(ClusterPhase::Creating, 0, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_false(&c, "ClusterFormed");
        assert_false(&c, "SlotsAssigned");
    }

    #[test]
    fn test_waiting_for_pods() {
        let c = derive_cluster_conditions(ClusterPhase::WaitingForPods, 0, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_false(&c, "ClusterFormed");
        assert_false(&c, "SlotsAssigned");
    }

    #[test]
    fn test_initializing_cluster() {
        let c = derive_cluster_conditions(ClusterPhase::InitializingCluster, 6, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_false(&c, "ClusterFormed");
        assert_false(&c, "SlotsAssigned");
    }

    #[test]
    fn test_assigning_slots() {
        let c = derive_cluster_conditions(ClusterPhase::AssigningSlots, 6, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_true(&c, "ClusterFormed");
        assert_false(&c, "SlotsAssigned");
    }

    #[test]
    fn test_configuring_replicas() {
        let c = derive_cluster_conditions(ClusterPhase::ConfiguringReplicas, 6, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_true(&c, "ClusterFormed");
        assert_true(&c, "SlotsAssigned");
    }

    #[test]
    fn test_scaling_up_statefulset() {
        let c = derive_cluster_conditions(ClusterPhase::ScalingUpStatefulSet, 6, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_true(&c, "ClusterFormed");
        assert_true(&c, "SlotsAssigned");
    }

    #[test]
    fn test_waiting_for_new_pods() {
        let c = derive_cluster_conditions(ClusterPhase::WaitingForNewPods, 6, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_adding_nodes_to_cluster() {
        let c = derive_cluster_conditions(ClusterPhase::AddingNodesToCluster, 12, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_rebalancing_slots() {
        let c = derive_cluster_conditions(ClusterPhase::RebalancingSlots, 12, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_configuring_new_replicas() {
        let c = derive_cluster_conditions(ClusterPhase::ConfiguringNewReplicas, 12, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_evacuating_slots() {
        let c = derive_cluster_conditions(ClusterPhase::EvacuatingSlots, 12, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_true(&c, "ClusterFormed");
        assert_false(&c, "SlotsAssigned");
    }

    #[test]
    fn test_removing_nodes_from_cluster() {
        let c = derive_cluster_conditions(ClusterPhase::RemovingNodesFromCluster, 12, 12, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_true(&c, "ClusterFormed");
        assert_true(&c, "SlotsAssigned");
    }

    #[test]
    fn test_scaling_down_statefulset() {
        let c = derive_cluster_conditions(ClusterPhase::ScalingDownStatefulSet, 6, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
    }

    #[test]
    fn test_verifying_cluster_health() {
        let c = derive_cluster_conditions(ClusterPhase::VerifyingClusterHealth, 6, 6, None);
        assert_false(&c, "Ready");
        assert_true(&c, "Progressing");
        assert_true(&c, "ClusterFormed");
        assert_true(&c, "SlotsAssigned");
    }

    #[test]
    fn test_degraded() {
        let c = derive_cluster_conditions(ClusterPhase::Degraded, 4, 6, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_true(&c, "Degraded");
    }

    #[test]
    fn test_failed() {
        let c = derive_cluster_conditions(ClusterPhase::Failed, 0, 6, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
        assert_true(&c, "Degraded");
    }

    #[test]
    fn test_deleting() {
        let c = derive_cluster_conditions(ClusterPhase::Deleting, 0, 0, None);
        assert_false(&c, "Ready");
        assert_false(&c, "Progressing");
    }

    #[test]
    fn test_running_desired_zero_not_ready() {
        let c = derive_cluster_conditions(ClusterPhase::Running, 0, 0, None);
        assert_false(&c, "Ready");
    }

    #[test]
    fn test_merge_conditions_preserves_timestamp_when_status_unchanged() {
        let old = vec![Condition::new(
            "Ready",
            true,
            "ClusterHealthy",
            "All good",
            Some(1),
        )];
        let old_ts = old.first().unwrap().last_transition_time.clone();

        std::thread::sleep(std::time::Duration::from_millis(5));

        let new = vec![Condition::new(
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
        let old = vec![Condition::new(
            "Ready",
            false,
            "Creating",
            "Not ready yet",
            Some(1),
        )];
        let old_ts = old.first().unwrap().last_transition_time.clone();

        std::thread::sleep(std::time::Duration::from_millis(5));

        let new = vec![Condition::new(
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
        let old = vec![Condition::new(
            "Ready",
            true,
            "ClusterHealthy",
            "OK",
            Some(1),
        )];
        let new = vec![
            Condition::new("Ready", true, "ClusterHealthy", "OK", Some(1)),
            Condition::new("Progressing", false, "ReconcileComplete", "Done", Some(1)),
        ];
        let merged = merge_conditions(&old, new);
        assert_eq!(merged.len(), 2);
    }
}
