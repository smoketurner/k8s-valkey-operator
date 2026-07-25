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

/// Node and slot counts used to render the `Running` summary line.
pub(crate) struct RunningCounts {
    pub(crate) masters: i32,
    pub(crate) replicas: i32,
    pub(crate) slots: i32,
}

/// Derive `status.lastError` and `status.summary` for one reconcile.
///
/// Both values come from the same source of truth. In `Failed`, `previous_error`
/// is the fallback because the reconcile tail always passes `error_message: None` —
/// deriving the summary from `error_message` directly would replace a real message
/// with "unknown error" on the first reconcile after the cluster entered `Failed`.
pub(crate) fn derive_error_and_summary(
    phase: ClusterPhase,
    error_message: Option<&str>,
    previous_error: Option<&str>,
    counts: RunningCounts,
) -> (Option<String>, Option<String>) {
    if phase == ClusterPhase::Running {
        let summary = format!(
            "Healthy: {} masters, {} replicas, {}/{TOTAL_SLOTS} slots",
            counts.masters, counts.replicas, counts.slots
        );
        return (None, Some(summary));
    }

    if phase == ClusterPhase::Failed {
        let err = error_message.or(previous_error).map(str::to_owned);
        let summary = format!("Failed: {}", err.as_deref().unwrap_or("unknown error"));
        return (err, Some(summary));
    }

    // Any other phase keeps whatever error is already recorded; a transient
    // reconcile error is not promoted to lastError outside of Failed.
    (
        previous_error.map(str::to_owned),
        Some(format!("Phase: {phase}")),
    )
}

/// Build the JSON merge-patch body for a status update.
///
/// `ValkeyClusterStatus` marks its optional fields `skip_serializing_if =
/// "Option::is_none"`, so a `None` drops the key entirely — and under RFC 7386 an
/// absent key means "leave unchanged". Fields the operator deliberately clears
/// must therefore be written as explicit nulls, or the server keeps the stale
/// value forever (a recovered cluster would keep reporting the error that failed
/// it, long after returning to Running).
///
/// `topology` is intentionally not cleared this way: a `None` there means the
/// operator could not read the cluster this reconcile, so preserving the
/// last-known topology is more useful than blanking it on every transient blip.
pub(crate) fn build_status_patch(status: &ValkeyClusterStatus) -> serde_json::Value {
    let mut patch = serde_json::json!({ "status": status });

    if let Some(status_obj) = patch
        .get_mut("status")
        .and_then(serde_json::Value::as_object_mut)
    {
        if status.last_error.is_none() {
            status_obj.insert("lastError".to_owned(), serde_json::Value::Null);
        }
        if status.last_recovery_attempt.is_none() {
            status_obj.insert("lastRecoveryAttempt".to_owned(), serde_json::Value::Null);
        }
    }

    patch
}

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
    // Pod counts alone are not enough: a cluster can have every pod Running and
    // Ready while holding fewer masters than spec.masters (a multi-step spec edit
    // that only partly completed). Reporting Ready there hides real drift.
    //
    // `healthy_masters == 0` means check_cluster_health could not read the
    // cluster at all, not that every master vanished — don't flap Ready on that.
    // Use the explicit count rather than `is_healthy`, which also folds in
    // slots_pfail/slots_fail and would flap on transient gossip state.
    //
    // `>=` rather than `==`: healthy_masters_count() counts every non-failed node
    // carrying the master flag, including slot-free ones — a replica that lost its
    // PVC rejoins as an empty master until CLUSTER REPLICATE runs. A surplus there
    // is not an outage, and flagging it would make Ready flap on a serving cluster.
    let masters_match = health_status
        .is_none_or(|h| h.healthy_masters == 0 || h.healthy_masters >= h.expected_masters);
    let ready = phase == ClusterPhase::Running
        && ready_pods == desired_pods
        && desired_pods > 0
        && masters_match;
    let progressing = is_progressing(phase);
    let degraded = is_degraded(phase, ready_pods, desired_pods)
        || (phase == ClusterPhase::Running && !masters_match);
    let slots_stable = slots_are_stable(phase);
    let replicas_in_sync = health_status
        .and_then(|h| h.max_replica_lag)
        .map(|lag| lag < REPLICATION_LAG_TOLERANCE_BYTES);
    let (rr, rm) = if ready {
        (
            "ClusterHealthy",
            "Cluster is running and all pods are ready.",
        )
    } else if phase == ClusterPhase::Running && !masters_match {
        (
            "MasterCountMismatch",
            "Cluster master count does not match spec.masters.",
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
    /// Number of masters the spec asks for, so conditions can spot drift between
    /// the live cluster and `spec.masters` without re-reading the resource.
    pub expected_masters: i32,
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

        let (last_error, summary) = derive_error_and_summary(
            self.phase,
            self.error_message,
            existing_status.and_then(|s| s.last_error.as_deref()),
            RunningCounts {
                masters: ready_masters,
                replicas: self.health_status.map(|h| h.healthy_replicas).unwrap_or(0),
                slots: self
                    .health_status
                    .map(|h| h.slots_assigned)
                    .unwrap_or_else(|| i32::from(TOTAL_SLOTS)),
            },
        );

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
            message,
            reconcile_count,
            last_phase_transition,
            recovery_attempts,
            last_recovery_attempt,
            last_error,
            summary,
        };

        let patch = build_status_patch(&status);

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

    fn counts() -> RunningCounts {
        RunningCounts {
            masters: 3,
            replicas: 3,
            slots: 16384,
        }
    }

    #[test]
    fn test_failed_summary_falls_back_to_preserved_error() {
        // The reconcile tail always passes error_message: None, so a cluster that
        // stays Failed must keep reporting the error that put it there.
        let (last_error, summary) = derive_error_and_summary(
            ClusterPhase::Failed,
            None,
            Some("quorum lost: 1 of 3 masters reachable"),
            counts(),
        );
        assert_eq!(
            last_error.as_deref(),
            Some("quorum lost: 1 of 3 masters reachable")
        );
        assert_eq!(
            summary.as_deref(),
            Some("Failed: quorum lost: 1 of 3 masters reachable")
        );
    }

    #[test]
    fn test_failed_summary_prefers_current_error() {
        let (last_error, summary) = derive_error_and_summary(
            ClusterPhase::Failed,
            Some("new failure"),
            Some("old"),
            counts(),
        );
        assert_eq!(last_error.as_deref(), Some("new failure"));
        assert_eq!(summary.as_deref(), Some("Failed: new failure"));
    }

    #[test]
    fn test_failed_summary_without_any_error() {
        let (last_error, summary) =
            derive_error_and_summary(ClusterPhase::Failed, None, None, counts());
        assert_eq!(last_error, None);
        assert_eq!(summary.as_deref(), Some("Failed: unknown error"));
    }

    #[test]
    fn test_running_clears_last_error() {
        let (last_error, summary) =
            derive_error_and_summary(ClusterPhase::Running, None, Some("stale"), counts());
        assert_eq!(last_error, None);
        assert_eq!(
            summary.as_deref(),
            Some("Healthy: 3 masters, 3 replicas, 16384/16384 slots")
        );
    }

    #[test]
    fn test_other_phase_preserves_last_error() {
        let (last_error, summary) =
            derive_error_and_summary(ClusterPhase::Degraded, None, Some("stale"), counts());
        assert_eq!(last_error.as_deref(), Some("stale"));
        assert_eq!(summary.as_deref(), Some("Phase: Degraded"));
    }

    /// Look up a `status.<field>` entry in a merge-patch body.
    ///
    /// Returns `None` when the key is absent, which is the distinction that
    /// matters here — indexing would report an absent key as `Null`.
    fn patch_field<'a>(patch: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
        patch.get("status").and_then(|s| s.get(field))
    }

    #[test]
    fn test_patch_clears_last_error_with_explicit_null() {
        // A bare `None` is omitted by skip_serializing_if, and RFC 7386 preserves
        // omitted keys — so the key must be present and null to actually clear.
        let status = ValkeyClusterStatus {
            last_error: None,
            last_recovery_attempt: None,
            ..Default::default()
        };
        let patch = build_status_patch(&status);
        assert_eq!(
            patch_field(&patch, "lastError"),
            Some(&serde_json::Value::Null),
            "lastError must be present and null to clear it server-side"
        );
        assert_eq!(
            patch_field(&patch, "lastRecoveryAttempt"),
            Some(&serde_json::Value::Null),
            "lastRecoveryAttempt must be present and null to clear it server-side"
        );
    }

    #[test]
    fn test_patch_preserves_a_real_last_error() {
        let status = ValkeyClusterStatus {
            last_error: Some("boom".to_string()),
            last_recovery_attempt: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        };
        let patch = build_status_patch(&status);
        assert_eq!(
            patch_field(&patch, "lastError").and_then(serde_json::Value::as_str),
            Some("boom")
        );
        assert_eq!(
            patch_field(&patch, "lastRecoveryAttempt").and_then(serde_json::Value::as_str),
            Some("2026-01-01T00:00:00Z")
        );
    }

    #[test]
    fn test_patch_omits_topology_rather_than_nulling_it() {
        // None means "not observed this reconcile", so the last-known topology
        // must survive rather than being blanked on a transient read failure.
        let status = ValkeyClusterStatus::default();
        let patch = build_status_patch(&status);
        assert_eq!(patch_field(&patch, "topology"), None);
    }

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
            expected_masters: 3,
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
            expected_masters: 1,
            healthy_replicas: 0,
            slots_assigned: 16384,
            topology: None,
            max_replica_lag: Some(lag),
        }
    }

    fn short_by_one_master() -> ClusterHealthStatus {
        ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 3,
            expected_masters: 4,
            healthy_replicas: 4,
            slots_assigned: 16384,
            topology: None,
            max_replica_lag: Some(0),
        }
    }

    /// Issue #172: a cluster can have every pod Running and Ready while holding
    /// fewer masters than spec.masters. Pod counts alone reported that as Ready.
    #[test]
    fn test_ready_false_when_master_count_below_spec() {
        let health = short_by_one_master();
        let c =
            derive_cluster_conditions(ClusterPhase::Running, 8, 8, Some(2), Some(&health), None);
        assert_false(&c, "Ready");
        assert_true(&c, "Degraded");
        assert_eq!(find_condition(&c, "Ready").reason, "MasterCountMismatch");
    }

    #[test]
    fn test_ready_true_when_master_count_matches_spec() {
        let health = healthy();
        let c =
            derive_cluster_conditions(ClusterPhase::Running, 6, 6, Some(2), Some(&health), None);
        assert_true(&c, "Ready");
        assert_false(&c, "Degraded");
    }

    /// check_cluster_health returns healthy_masters: 0 when it cannot reach the
    /// cluster at all. That is "unreadable", not "every master vanished", so it
    /// must not flap Ready on a transient connection blip.
    #[test]
    fn test_ready_not_flapped_when_health_is_unreadable() {
        let health = ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 0,
            expected_masters: 3,
            healthy_replicas: 0,
            slots_assigned: 0,
            topology: None,
            max_replica_lag: None,
        };
        let c =
            derive_cluster_conditions(ClusterPhase::Running, 6, 6, Some(1), Some(&health), None);
        assert_true(&c, "Ready");
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
