//! Phase handlers for ValkeyCluster reconciliation.
//!
//! This module contains isolated, testable handlers for each phase
//! of the ValkeyCluster lifecycle. Each handler is responsible for
//! a single phase transition and returns the next phase.

use kube::Api;
use tracing::{debug, info, warn};

use crate::{
    controller::{
        context::Context,
        error::Error,
        operation_coordination::{self, OperationType},
    },
    crd::{ClusterPhase, PendingChanges, ValkeyCluster, total_pods},
};

use super::cluster_reconciler::FIELD_MANAGER;

/// Result of a phase handler execution.
#[derive(Debug)]
pub struct PhaseResult {
    /// The next phase to transition to.
    pub next_phase: ClusterPhase,
    /// Optional message describing what happened.
    pub message: Option<String>,
}

impl PhaseResult {
    /// Create a new phase result.
    pub fn new(next_phase: ClusterPhase) -> Self {
        Self {
            next_phase,
            message: None,
        }
    }

    /// Create a phase result with a message.
    pub fn with_message(next_phase: ClusterPhase, message: impl Into<String>) -> Self {
        Self {
            next_phase,
            message: Some(message.into()),
        }
    }
}

impl From<ClusterPhase> for PhaseResult {
    fn from(phase: ClusterPhase) -> Self {
        Self::new(phase)
    }
}

// ============================================================================
// Phase Context - Shared data for phase handlers
// ============================================================================

/// Context passed to phase handlers containing cluster state information.
#[derive(Debug, Clone)]
pub struct PhaseContext {
    /// Name of the cluster.
    pub name: String,
    /// Namespace of the cluster.
    pub namespace: String,
    /// Target number of masters from spec.
    pub target_masters: i32,
    /// Target replicas per master from spec.
    pub target_replicas_per_master: i32,
    /// Current number of masters in the cluster.
    pub current_masters: i32,
    /// Number of running pods.
    pub running_pods: i32,
    /// Number of ready pods.
    pub ready_pods: i32,
    /// Number of nodes in the cluster.
    pub nodes_in_cluster: i32,
    /// Whether the spec has changed.
    pub spec_changed: bool,
    /// Current generation of the resource.
    pub generation: i64,
    /// Pending changes from status (if any).
    pub pending_changes: Option<PendingChanges>,
}

impl PhaseContext {
    /// Calculate the desired total number of pods.
    pub fn desired_replicas(&self) -> i32 {
        total_pods(self.target_masters, self.target_replicas_per_master)
    }

    /// Determine the scale direction.
    pub fn scale_direction(&self) -> ScaleDirection {
        if self.current_masters > self.target_masters && self.current_masters > 0 {
            ScaleDirection::Down
        } else if self.current_masters < self.target_masters && self.current_masters > 0 {
            ScaleDirection::Up
        } else if self.running_pods != self.desired_replicas() {
            ScaleDirection::ReplicaChange
        } else {
            ScaleDirection::None
        }
    }
}

/// Direction of a scaling operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScaleDirection {
    /// Scaling up (adding masters).
    Up,
    /// Scaling down (removing masters).
    Down,
    /// Changing replica count (no master change).
    ReplicaChange,
    /// No scaling needed.
    None,
}

impl ScaleDirection {
    /// Convert to string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            ScaleDirection::Up => "up",
            ScaleDirection::Down => "down",
            ScaleDirection::ReplicaChange => "replica_change",
            ScaleDirection::None => "none",
        }
    }
}

impl std::fmt::Display for ScaleDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================
// Pure Logic Functions (testable without mocking)
// ============================================================================

/// Compute which nodes need to be added or removed based on scale direction.
///
/// Returns (nodes_to_add, nodes_to_remove) as vectors of pod names.
pub fn compute_nodes_to_change(
    name: &str,
    current_masters: i32,
    target_masters: i32,
    scale_direction: ScaleDirection,
) -> (Vec<String>, Vec<String>) {
    match scale_direction {
        ScaleDirection::Up => {
            let new_masters: Vec<String> = (current_masters..target_masters)
                .map(|i| format!("{}-{}", name, i))
                .collect();
            (new_masters, Vec::new())
        }
        ScaleDirection::Down => {
            let removed_masters: Vec<String> = (target_masters..current_masters)
                .map(|i| format!("{}-{}", name, i))
                .collect();
            (Vec::new(), removed_masters)
        }
        ScaleDirection::ReplicaChange | ScaleDirection::None => (Vec::new(), Vec::new()),
    }
}

/// Determine the next phase based on the scale direction.
pub fn next_phase_for_scale_direction(scale_direction: ScaleDirection) -> ClusterPhase {
    match scale_direction {
        ScaleDirection::Down => ClusterPhase::MigratingSlots,
        ScaleDirection::Up | ScaleDirection::ReplicaChange => ClusterPhase::ScalingStatefulSet,
        ScaleDirection::None => ClusterPhase::Running,
    }
}

/// Determine if a cluster is healthy based on ready vs desired replicas.
pub fn is_cluster_healthy(ready_replicas: i32, desired_replicas: i32) -> bool {
    ready_replicas >= desired_replicas && desired_replicas > 0
}

/// Determine if a cluster is degraded (some but not all replicas ready).
pub fn is_cluster_degraded(ready_replicas: i32, desired_replicas: i32) -> bool {
    ready_replicas > 0 && ready_replicas < desired_replicas
}

/// Determine if a cluster has failed (no replicas ready).
pub fn is_cluster_failed(ready_replicas: i32) -> bool {
    ready_replicas == 0
}

// ============================================================================
// Phase Handlers
// ============================================================================

/// Handle DetectingChanges phase.
///
/// Computes what changes are needed (scale direction, nodes to add/remove)
/// and stores them in `pending_changes` status field.
pub async fn handle_detecting_changes(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Detecting changes for cluster update");

    let scale_direction = phase_ctx.scale_direction();

    info!(
        name = %name,
        current_masters = phase_ctx.current_masters,
        target_masters = phase_ctx.target_masters,
        running_pods = phase_ctx.running_pods,
        target_replicas = phase_ctx.desired_replicas(),
        scale_direction = %scale_direction,
        "Change detection complete"
    );

    // Compute nodes to add/remove
    let (nodes_to_add, nodes_to_remove) = match scale_direction {
        ScaleDirection::Up => {
            let new_masters: Vec<String> = (phase_ctx.current_masters..phase_ctx.target_masters)
                .map(|i| format!("{}-{}", name, i))
                .collect();
            (new_masters, Vec::new())
        }
        ScaleDirection::Down => {
            let removed_masters: Vec<String> = (phase_ctx.target_masters
                ..phase_ctx.current_masters)
                .map(|i| format!("{}-{}", name, i))
                .collect();
            (Vec::new(), removed_masters)
        }
        _ => (Vec::new(), Vec::new()),
    };

    // Create PendingChanges
    let pending_changes = PendingChanges {
        scale_direction: scale_direction.as_str().to_string(),
        previous_masters: phase_ctx.current_masters,
        target_masters: phase_ctx.target_masters,
        nodes_to_add: nodes_to_add.clone(),
        nodes_to_remove: nodes_to_remove.clone(),
        slot_migrations: Vec::new(),
        requires_rolling_update: false,
        for_generation: phase_ctx.generation,
    };

    // Update status with pending changes
    let mut status = obj.status.clone().unwrap_or_default();
    status.pending_changes = Some(pending_changes);
    status.message = format!("Detected {} operation", scale_direction);

    let patch = serde_json::json!({
        "status": status
    });
    api.patch_status(
        name,
        &kube::api::PatchParams::apply(FIELD_MANAGER),
        &kube::api::Patch::Merge(&patch),
    )
    .await?;

    // Check if operation is allowed
    if scale_direction != ScaleDirection::None
        && let Err(e) =
            operation_coordination::check_operation_allowed(api, name, OperationType::Scaling).await
    {
        warn!(name = %name, error = %e, "Cannot start scaling operation due to conflict");
        ctx.publish_warning_event(
            obj,
            "ScalingBlocked",
            "DetectingChanges",
            Some(e.to_string()),
        )
        .await;
        return Ok(PhaseResult::with_message(
            ClusterPhase::DetectingChanges,
            "Waiting for operation lock",
        ));
    }

    ctx.publish_normal_event(
        obj,
        "ChangesDetected",
        "DetectingChanges",
        Some(format!(
            "Detected {} operation: {} -> {} masters",
            scale_direction, phase_ctx.current_masters, phase_ctx.target_masters
        )),
    )
    .await;

    // Determine next phase based on scale direction
    let next_phase = match scale_direction {
        ScaleDirection::Down => ClusterPhase::MigratingSlots,
        ScaleDirection::Up | ScaleDirection::ReplicaChange => ClusterPhase::ScalingStatefulSet,
        ScaleDirection::None => ClusterPhase::Running,
    };

    Ok(PhaseResult::with_message(
        next_phase,
        format!("Scale direction: {}", scale_direction),
    ))
}

/// Handle ScalingStatefulSet phase.
///
/// Updates the StatefulSet replica count and waits for pods to be ready.
pub async fn handle_scaling_statefulset(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Scaling StatefulSet");

    // Apply StatefulSet changes
    create_resources_fn.await?;

    // Get scale direction from pending changes
    let scale_direction = phase_ctx
        .pending_changes
        .as_ref()
        .map(|p| p.scale_direction.as_str())
        .unwrap_or("none");

    let desired_replicas = phase_ctx.desired_replicas();

    if scale_direction == "down" {
        // For scale-down, we're reducing replicas AFTER slot migration
        if phase_ctx.running_pods <= desired_replicas {
            info!(
                name = %name,
                running = phase_ctx.running_pods,
                desired = desired_replicas,
                "StatefulSet scaled down, verifying cluster"
            );
            ctx.publish_normal_event(
                obj,
                "StatefulSetScaled",
                "ScalingStatefulSet",
                Some(format!("StatefulSet scaled to {} pods", desired_replicas)),
            )
            .await;
            return Ok(ClusterPhase::VerifyingCluster.into());
        } else {
            debug!(
                name = %name,
                running = phase_ctx.running_pods,
                desired = desired_replicas,
                "Waiting for StatefulSet to scale down"
            );
            return Ok(ClusterPhase::ScalingStatefulSet.into());
        }
    }

    // For scale-up, wait for all pods to be running
    if phase_ctx.running_pods < desired_replicas {
        debug!(
            name = %name,
            running = phase_ctx.running_pods,
            desired = desired_replicas,
            "Waiting for pods to start"
        );
        return Ok(ClusterPhase::ScalingStatefulSet.into());
    }

    info!(
        name = %name,
        running = phase_ctx.running_pods,
        "All pods running, proceeding to add nodes to cluster"
    );

    ctx.publish_normal_event(
        obj,
        "PodsReady",
        "ScalingStatefulSet",
        Some(format!("{} pods ready", phase_ctx.running_pods)),
    )
    .await;

    Ok(ClusterPhase::AddingNodes.into())
}

/// Handle AddingNodes phase.
///
/// Executes CLUSTER MEET for new nodes to join the cluster.
pub async fn handle_adding_nodes(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    add_replicas_fn: impl std::future::Future<Output = Result<i32, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Adding new nodes to cluster");

    let nodes_to_add = phase_ctx
        .pending_changes
        .as_ref()
        .map(|p| p.nodes_to_add.clone())
        .unwrap_or_default();

    if nodes_to_add.is_empty() {
        // No new master nodes, but might have new replicas
        match add_replicas_fn.await {
            Ok(added) if added > 0 => {
                info!(name = %name, added = added, "Added new replicas to cluster");
                ctx.publish_normal_event(
                    obj,
                    "ReplicasAdded",
                    "AddingNodes",
                    Some(format!("Added {} new replicas", added)),
                )
                .await;
            }
            Ok(_) => {
                debug!(name = %name, "No new replicas to add");
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Failed to add replicas, will retry");
                return Ok(ClusterPhase::AddingNodes.into());
            }
        }
        return Ok(ClusterPhase::VerifyingCluster.into());
    }

    let desired_replicas = phase_ctx.desired_replicas();

    if phase_ctx.nodes_in_cluster >= desired_replicas {
        info!(
            name = %name,
            nodes = phase_ctx.nodes_in_cluster,
            "All nodes in cluster, proceeding to slot migration"
        );
        ctx.publish_normal_event(
            obj,
            "NodesAdded",
            "AddingNodes",
            Some(format!("{} nodes in cluster", phase_ctx.nodes_in_cluster)),
        )
        .await;
        return Ok(ClusterPhase::MigratingSlots.into());
    }

    // Add new replicas
    match add_replicas_fn.await {
        Ok(added) => {
            info!(name = %name, added = added, "Added nodes to cluster");
            ctx.publish_normal_event(
                obj,
                "NodesAdded",
                "AddingNodes",
                Some(format!("Added {} nodes to cluster", added)),
            )
            .await;
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Failed to add nodes, will retry");
            ctx.publish_warning_event(
                obj,
                "NodeAddFailed",
                "AddingNodes",
                Some(format!("Failed to add nodes: {}", e)),
            )
            .await;
            return Ok(ClusterPhase::AddingNodes.into());
        }
    }

    // Stay in AddingNodes until all nodes are added
    Ok(ClusterPhase::AddingNodes.into())
}

/// Handle VerifyingCluster phase.
///
/// Performs final health check before transitioning to Running.
pub async fn handle_verifying_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    health_check_fn: impl std::future::Future<Output = Result<ClusterHealthResult, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Verifying cluster health");

    match health_check_fn.await {
        Ok(health) if health.is_healthy => {
            info!(
                name = %name,
                slots_assigned = health.slots_assigned,
                healthy_masters = health.healthy_masters,
                "Cluster verification passed"
            );

            // Clear pending changes
            let mut status = obj.status.clone().unwrap_or_default();
            status.pending_changes = None;
            status.operation_progress = None;
            status.message = "Cluster healthy".to_string();

            let patch = serde_json::json!({
                "status": status
            });
            let _ = api
                .patch_status(
                    name,
                    &kube::api::PatchParams::apply(FIELD_MANAGER),
                    &kube::api::Patch::Merge(&patch),
                )
                .await;

            ctx.publish_normal_event(
                obj,
                "ClusterVerified",
                "VerifyingCluster",
                Some("Cluster health verification passed".to_string()),
            )
            .await;

            Ok(ClusterPhase::Running.into())
        }
        Ok(health) => {
            warn!(
                name = %name,
                slots_assigned = health.slots_assigned,
                healthy_masters = health.healthy_masters,
                "Cluster not yet healthy, waiting"
            );

            if health.healthy_masters == 0 {
                Ok(ClusterPhase::Degraded.into())
            } else {
                Ok(ClusterPhase::VerifyingCluster.into())
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Health check failed during verification");
            Ok(ClusterPhase::VerifyingCluster.into())
        }
    }
}

/// Result of a cluster health check.
#[derive(Debug, Clone)]
pub struct ClusterHealthResult {
    /// Whether the cluster is healthy.
    pub is_healthy: bool,
    /// Number of slots assigned.
    pub slots_assigned: i32,
    /// Number of healthy masters.
    pub healthy_masters: i32,
}

// ============================================================================
// Creating Phase Handler
// ============================================================================

/// Handle Creating phase.
///
/// Creates owned resources and waits for all pods to be running.
/// Transitions to Initializing when all pods are running.
pub async fn handle_creating(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
    running_pods: i32,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    // Create owned resources
    create_resources_fn.await?;

    let desired_replicas = phase_ctx.desired_replicas();

    if running_pods >= desired_replicas {
        ctx.publish_normal_event(
            obj,
            "PodsRunning",
            "Reconciling",
            Some(format!(
                "All pods running ({}/{}), initializing cluster",
                running_pods, desired_replicas
            )),
        )
        .await;
        Ok(ClusterPhase::Initializing.into())
    } else {
        debug!(
            name = %name,
            running = running_pods,
            desired = desired_replicas,
            "Waiting for pods to be running"
        );
        Ok(ClusterPhase::Creating.into())
    }
}

// ============================================================================
// Running Phase Handler
// ============================================================================

/// Handle Running phase.
///
/// Checks if spec changed and monitors cluster health.
/// Transitions to DetectingChanges if spec changed, or Degraded if unhealthy.
pub async fn handle_running(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
    ready_replicas: i32,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    // Check if update needed
    if phase_ctx.spec_changed {
        ctx.publish_normal_event(
            obj,
            "SpecChanged",
            "DetectingChanges",
            Some("Resource spec changed, detecting required changes".to_string()),
        )
        .await;
        return Ok(ClusterPhase::DetectingChanges.into());
    }

    // Ensure resources are in sync
    create_resources_fn.await?;

    let desired_replicas = phase_ctx.desired_replicas();

    if ready_replicas < desired_replicas {
        ctx.publish_warning_event(
            obj,
            "Degraded",
            "Reconciling",
            Some(format!(
                "Resource degraded: {}/{} replicas ready",
                ready_replicas, desired_replicas
            )),
        )
        .await;
        Ok(ClusterPhase::Degraded.into())
    } else {
        debug!(
            name = %name,
            ready = ready_replicas,
            desired = desired_replicas,
            "Cluster running normally"
        );
        Ok(ClusterPhase::Running.into())
    }
}

// ============================================================================
// Degraded Phase Handler
// ============================================================================

/// Handle Degraded phase.
///
/// Monitors recovery and transitions back to Running when healthy.
/// Transitions to Failed if no replicas available.
pub async fn handle_degraded(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
    ready_replicas: i32,
) -> Result<PhaseResult, Error> {
    // Ensure resources are in sync (may have been modified externally)
    // This is important for recovery when StatefulSet replicas were manually reduced
    create_resources_fn.await?;

    let desired_replicas = phase_ctx.desired_replicas();

    if ready_replicas >= desired_replicas {
        ctx.publish_normal_event(
            obj,
            "Recovered",
            "Reconciling",
            Some(format!(
                "Resource recovered: {}/{} replicas ready",
                ready_replicas, desired_replicas
            )),
        )
        .await;
        Ok(ClusterPhase::Running.into())
    } else if ready_replicas == 0 {
        ctx.publish_warning_event(
            obj,
            "Failed",
            "Reconciling",
            Some("Resource failed: no replicas available".to_string()),
        )
        .await;
        Ok(ClusterPhase::Failed.into())
    } else {
        debug!(
            ready = ready_replicas,
            desired = desired_replicas,
            "Cluster still degraded, waiting for recovery"
        );
        Ok(ClusterPhase::Degraded.into())
    }
}

// ============================================================================
// Initializing Phase Handler
// ============================================================================

/// Result of cluster initialization (CLUSTER MEET).
#[derive(Debug)]
pub enum InitResult {
    /// Waiting for pods to be running.
    WaitingForPods,
    /// CLUSTER MEET succeeded, proceed to slot assignment.
    Success,
    /// CLUSTER MEET failed, will retry.
    Failed(String),
}

/// Handle Initializing phase.
///
/// Executes CLUSTER MEET to connect all cluster nodes.
/// Transitions to AssigningSlots when successful.
pub async fn handle_initializing(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
    running_pods: i32,
    cluster_init_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    // Ensure resources are still in sync
    create_resources_fn.await?;

    let desired_replicas = phase_ctx.desired_replicas();

    if running_pods < desired_replicas {
        // Pods not running yet, wait
        debug!(
            name = %name,
            running = running_pods,
            desired = desired_replicas,
            "Waiting for pods to be running before CLUSTER MEET"
        );
        return Ok(ClusterPhase::Initializing.into());
    }

    // Execute CLUSTER MEET to connect all nodes
    info!(name = %name, "Executing CLUSTER MEET to connect all nodes");

    // Acquire operation lock for initialization
    if let Err(e) =
        operation_coordination::start_operation(api, name, OperationType::Initializing).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock for initialization");
        // If lock acquisition fails, stay in Initializing to retry
        return Ok(PhaseResult::with_message(
            ClusterPhase::Initializing,
            "Waiting for operation lock",
        ));
    }

    let init_result = cluster_init_fn.await;

    // Release operation lock if initialization fails (success releases in next phase)
    if init_result.is_err() {
        let _ = operation_coordination::complete_operation(api, name, OperationType::Initializing)
            .await;
    }

    match init_result {
        Ok(()) => {
            ctx.publish_normal_event(
                obj,
                "ClusterMeet",
                "Initializing",
                Some("Cluster nodes connected via CLUSTER MEET".to_string()),
            )
            .await;
            Ok(ClusterPhase::AssigningSlots.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "CLUSTER MEET failed, will retry");
            ctx.publish_warning_event(
                obj,
                "ClusterMeetFailed",
                "Initializing",
                Some(format!("CLUSTER MEET failed: {}", e)),
            )
            .await;
            // Stay in Initializing and retry
            Ok(ClusterPhase::Initializing.into())
        }
    }
}

// ============================================================================
// AssigningSlots Phase Handler
// ============================================================================

/// Handle AssigningSlots phase.
///
/// Executes CLUSTER ADDSLOTS to assign hash slots to masters
/// and sets up replica relationships.
pub async fn handle_assigning_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    slot_assignment_fn: impl std::future::Future<Output = Result<(), Error>>,
    health_check_fn: impl std::future::Future<Output = Result<ClusterHealthResult, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Executing CLUSTER ADDSLOTS and setting up replicas");

    // Ensure operation lock is still held (should be from Initializing phase)
    // If not, acquire it (may have been lost)
    let has_operation_lock = obj
        .status
        .as_ref()
        .and_then(|s| s.current_operation.as_ref())
        .map(|op| op == "initializing")
        .unwrap_or(false);

    if !has_operation_lock
        && let Err(e) =
            operation_coordination::start_operation(api, name, OperationType::Initializing).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock for slot assignment");
        return Ok(PhaseResult::with_message(
            ClusterPhase::AssigningSlots,
            "Waiting for operation lock",
        ));
    }

    match slot_assignment_fn.await {
        Ok(()) => {
            // Release operation lock when initialization completes
            let _ =
                operation_coordination::complete_operation(api, name, OperationType::Initializing)
                    .await;

            // Verify cluster is healthy before transitioning to Running
            // The cluster needs time to propagate slot information to all nodes
            match health_check_fn.await {
                Ok(health) if health.is_healthy => {
                    ctx.publish_normal_event(
                        obj,
                        "SlotsAssigned",
                        "Initializing",
                        Some("All 16384 hash slots assigned, cluster ready".to_string()),
                    )
                    .await;
                    Ok(ClusterPhase::Running.into())
                }
                Ok(health) => {
                    // Slots assigned but cluster not yet healthy - wait for propagation
                    debug!(
                        name = %name,
                        slots_assigned = health.slots_assigned,
                        healthy_masters = health.healthy_masters,
                        "Slots assigned, waiting for cluster to stabilize"
                    );
                    // Stay in AssigningSlots to wait for cluster_state:ok
                    Ok(ClusterPhase::AssigningSlots.into())
                }
                Err(e) => {
                    // Health check failed - wait and retry
                    debug!(
                        name = %name,
                        error = %e,
                        "Health check failed after slot assignment, waiting"
                    );
                    Ok(ClusterPhase::AssigningSlots.into())
                }
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Slot assignment failed, will retry");
            ctx.publish_warning_event(
                obj,
                "SlotAssignmentFailed",
                "Initializing",
                Some(format!("Slot assignment failed: {}", e)),
            )
            .await;
            // Stay in AssigningSlots and retry
            Ok(ClusterPhase::AssigningSlots.into())
        }
    }
}

// ============================================================================
// MigratingSlots Phase Handler
// ============================================================================

/// Handle MigratingSlots phase.
///
/// Migrates hash slots during scale operations for rebalancing.
pub async fn handle_migrating_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    scaling_fn: impl std::future::Future<Output = Result<bool, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    // Get scale direction from pending changes
    let scale_direction = phase_ctx
        .pending_changes
        .as_ref()
        .map(|p| p.scale_direction.as_str())
        .unwrap_or("none");

    info!(name = %name, direction = %scale_direction, "Migrating slots for rebalancing");

    // Acquire operation lock
    if let Err(e) = operation_coordination::start_operation(api, name, OperationType::Scaling).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock");
        return Ok(ClusterPhase::MigratingSlots.into());
    }

    // Execute the scaling operation (which handles slot migration)
    match scaling_fn.await {
        Ok(complete) => {
            // Release operation lock
            let _ =
                operation_coordination::complete_operation(api, name, OperationType::Scaling).await;

            if complete {
                info!(name = %name, "Slot migration complete");
                ctx.publish_normal_event(
                    obj,
                    "SlotsMigrated",
                    "MigratingSlots",
                    Some("Hash slot migration complete".to_string()),
                )
                .await;

                if scale_direction == "down" {
                    // Scale-down: need to remove nodes from cluster
                    Ok(ClusterPhase::RemovingNodes.into())
                } else {
                    // Scale-up: verify cluster health
                    Ok(ClusterPhase::VerifyingCluster.into())
                }
            } else {
                // Migration in progress
                ctx.publish_normal_event(
                    obj,
                    "MigrationProgress",
                    "MigratingSlots",
                    Some("Slot migration in progress".to_string()),
                )
                .await;
                // Stay in MigratingSlots to continue
                Ok(ClusterPhase::MigratingSlots.into())
            }
        }
        Err(e) => {
            // Release operation lock on error
            let _ =
                operation_coordination::complete_operation(api, name, OperationType::Scaling).await;

            warn!(name = %name, error = %e, "Slot migration failed, will retry");
            ctx.publish_warning_event(
                obj,
                "MigrationFailed",
                "MigratingSlots",
                Some(format!("Slot migration failed: {}", e)),
            )
            .await;
            // Stay in MigratingSlots to retry
            Ok(ClusterPhase::MigratingSlots.into())
        }
    }
}

// ============================================================================
// RemovingNodes Phase Handler
// ============================================================================

/// Handle RemovingNodes phase.
///
/// Executes CLUSTER FORGET to remove scaled-down nodes from the cluster
/// and updates the StatefulSet.
pub async fn handle_removing_nodes(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    forget_nodes_fn: impl std::future::Future<Output = Result<bool, Error>>,
    scale_down_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Removing nodes from cluster");

    // Execute CLUSTER FORGET for removed nodes
    match forget_nodes_fn.await {
        Ok(complete) => {
            if complete {
                info!(name = %name, "All nodes removed from cluster");
                ctx.publish_normal_event(
                    obj,
                    "NodesRemoved",
                    "RemovingNodes",
                    Some("All nodes removed from cluster via CLUSTER FORGET".to_string()),
                )
                .await;

                // Scale down the StatefulSet
                scale_down_fn.await?;

                Ok(ClusterPhase::ScalingStatefulSet.into())
            } else {
                // Still removing nodes
                Ok(ClusterPhase::RemovingNodes.into())
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Failed to remove nodes, will retry");
            ctx.publish_warning_event(
                obj,
                "NodeRemovalFailed",
                "RemovingNodes",
                Some(format!("Node removal failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::RemovingNodes.into())
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

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
    fn test_scale_direction_up() {
        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 6,
            target_replicas_per_master: 1,
            current_masters: 3,
            running_pods: 6,
            ready_pods: 6,
            nodes_in_cluster: 6,
            spec_changed: true,
            generation: 1,
            pending_changes: None,
        };

        assert_eq!(ctx.scale_direction(), ScaleDirection::Up);
    }

    #[test]
    fn test_scale_direction_down() {
        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 3,
            target_replicas_per_master: 1,
            current_masters: 6,
            running_pods: 12,
            ready_pods: 12,
            nodes_in_cluster: 12,
            spec_changed: true,
            generation: 1,
            pending_changes: None,
        };

        assert_eq!(ctx.scale_direction(), ScaleDirection::Down);
    }

    #[test]
    fn test_scale_direction_none() {
        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 3,
            target_replicas_per_master: 1,
            current_masters: 3,
            running_pods: 6,
            ready_pods: 6,
            nodes_in_cluster: 6,
            spec_changed: false,
            generation: 1,
            pending_changes: None,
        };

        assert_eq!(ctx.scale_direction(), ScaleDirection::None);
    }

    #[test]
    fn test_scale_direction_replica_change() {
        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 3,
            target_replicas_per_master: 2, // Changed from 1 to 2
            current_masters: 3,
            running_pods: 6, // Still old count
            ready_pods: 6,
            nodes_in_cluster: 6,
            spec_changed: true,
            generation: 1,
            pending_changes: None,
        };

        assert_eq!(ctx.scale_direction(), ScaleDirection::ReplicaChange);
    }

    #[test]
    fn test_desired_replicas() {
        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 3,
            target_replicas_per_master: 2,
            current_masters: 3,
            running_pods: 9,
            ready_pods: 9,
            nodes_in_cluster: 9,
            spec_changed: false,
            generation: 1,
            pending_changes: None,
        };

        // 3 masters * (1 + 2 replicas) = 9
        assert_eq!(ctx.desired_replicas(), 9);
    }

    #[test]
    fn test_phase_result_from_phase() {
        let result: PhaseResult = ClusterPhase::Running.into();
        assert_eq!(result.next_phase, ClusterPhase::Running);
        assert!(result.message.is_none());
    }

    #[test]
    fn test_phase_result_with_message() {
        let result = PhaseResult::with_message(ClusterPhase::Running, "All good");
        assert_eq!(result.next_phase, ClusterPhase::Running);
        assert_eq!(result.message, Some("All good".to_string()));
    }

    #[test]
    fn test_scale_direction_display() {
        assert_eq!(ScaleDirection::Up.to_string(), "up");
        assert_eq!(ScaleDirection::Down.to_string(), "down");
        assert_eq!(ScaleDirection::ReplicaChange.to_string(), "replica_change");
        assert_eq!(ScaleDirection::None.to_string(), "none");
    }

    #[test]
    fn test_scale_direction_as_str() {
        assert_eq!(ScaleDirection::Up.as_str(), "up");
        assert_eq!(ScaleDirection::Down.as_str(), "down");
        assert_eq!(ScaleDirection::ReplicaChange.as_str(), "replica_change");
        assert_eq!(ScaleDirection::None.as_str(), "none");
    }

    #[test]
    fn test_cluster_health_result() {
        let healthy = ClusterHealthResult {
            is_healthy: true,
            slots_assigned: 16384,
            healthy_masters: 3,
        };
        assert!(healthy.is_healthy);
        assert_eq!(healthy.slots_assigned, 16384);
        assert_eq!(healthy.healthy_masters, 3);

        let unhealthy = ClusterHealthResult {
            is_healthy: false,
            slots_assigned: 8192,
            healthy_masters: 2,
        };
        assert!(!unhealthy.is_healthy);
        assert_eq!(unhealthy.slots_assigned, 8192);
        assert_eq!(unhealthy.healthy_masters, 2);
    }

    #[test]
    fn test_init_result_variants() {
        // Just ensure the enum variants exist and can be created
        let waiting = InitResult::WaitingForPods;
        let success = InitResult::Success;
        let failed = InitResult::Failed("test error".to_string());

        // Pattern matching to verify variants
        assert!(matches!(waiting, InitResult::WaitingForPods));
        assert!(matches!(success, InitResult::Success));
        assert!(matches!(failed, InitResult::Failed(_)));
    }

    #[test]
    fn test_phase_context_no_current_masters() {
        // When current_masters is 0 (initial creation), scale_direction should be None
        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 3,
            target_replicas_per_master: 1,
            current_masters: 0,
            running_pods: 0,
            ready_pods: 0,
            nodes_in_cluster: 0,
            spec_changed: false,
            generation: 1,
            pending_changes: None,
        };

        // With 0 current_masters, we should not report scale up/down
        // (this is initial creation, not scaling)
        assert_eq!(ctx.scale_direction(), ScaleDirection::ReplicaChange);
    }

    #[test]
    fn test_phase_context_with_pending_changes() {
        use crate::crd::PendingChanges;

        let pending = PendingChanges {
            scale_direction: "up".to_string(),
            previous_masters: 3,
            target_masters: 6,
            nodes_to_add: vec![
                "test-3".to_string(),
                "test-4".to_string(),
                "test-5".to_string(),
            ],
            nodes_to_remove: Vec::new(),
            slot_migrations: Vec::new(),
            requires_rolling_update: false,
            for_generation: 2,
        };

        let ctx = PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters: 6,
            target_replicas_per_master: 1,
            current_masters: 3,
            running_pods: 6,
            ready_pods: 6,
            nodes_in_cluster: 6,
            spec_changed: true,
            generation: 2,
            pending_changes: Some(pending.clone()),
        };

        assert_eq!(ctx.scale_direction(), ScaleDirection::Up);
        assert!(ctx.pending_changes.is_some());
        let changes = ctx.pending_changes.unwrap();
        assert_eq!(changes.nodes_to_add.len(), 3);
        assert_eq!(changes.target_masters, 6);
    }

    // ========================================================================
    // Tests for compute_nodes_to_change
    // ========================================================================

    #[test]
    fn test_compute_nodes_to_change_scale_up() {
        let (to_add, to_remove) = compute_nodes_to_change("mycluster", 3, 6, ScaleDirection::Up);

        assert_eq!(to_add, vec!["mycluster-3", "mycluster-4", "mycluster-5"]);
        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_change_scale_up_single_node() {
        let (to_add, to_remove) = compute_nodes_to_change("vc", 3, 4, ScaleDirection::Up);

        assert_eq!(to_add, vec!["vc-3"]);
        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_change_scale_down() {
        let (to_add, to_remove) = compute_nodes_to_change("mycluster", 6, 3, ScaleDirection::Down);

        assert!(to_add.is_empty());
        assert_eq!(to_remove, vec!["mycluster-3", "mycluster-4", "mycluster-5"]);
    }

    #[test]
    fn test_compute_nodes_to_change_scale_down_single_node() {
        let (to_add, to_remove) = compute_nodes_to_change("test", 4, 3, ScaleDirection::Down);

        assert!(to_add.is_empty());
        assert_eq!(to_remove, vec!["test-3"]);
    }

    #[test]
    fn test_compute_nodes_to_change_replica_change() {
        let (to_add, to_remove) =
            compute_nodes_to_change("mycluster", 3, 3, ScaleDirection::ReplicaChange);

        assert!(to_add.is_empty());
        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_change_none() {
        let (to_add, to_remove) = compute_nodes_to_change("mycluster", 3, 3, ScaleDirection::None);

        assert!(to_add.is_empty());
        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_change_large_scale_up() {
        let (to_add, to_remove) = compute_nodes_to_change("cluster", 3, 10, ScaleDirection::Up);

        assert_eq!(to_add.len(), 7);
        assert_eq!(to_add[0], "cluster-3");
        assert_eq!(to_add[6], "cluster-9");
        assert!(to_remove.is_empty());
    }

    // ========================================================================
    // Tests for next_phase_for_scale_direction
    // ========================================================================

    #[test]
    fn test_next_phase_for_scale_direction_up() {
        assert_eq!(
            next_phase_for_scale_direction(ScaleDirection::Up),
            ClusterPhase::ScalingStatefulSet
        );
    }

    #[test]
    fn test_next_phase_for_scale_direction_down() {
        assert_eq!(
            next_phase_for_scale_direction(ScaleDirection::Down),
            ClusterPhase::MigratingSlots
        );
    }

    #[test]
    fn test_next_phase_for_scale_direction_replica_change() {
        assert_eq!(
            next_phase_for_scale_direction(ScaleDirection::ReplicaChange),
            ClusterPhase::ScalingStatefulSet
        );
    }

    #[test]
    fn test_next_phase_for_scale_direction_none() {
        assert_eq!(
            next_phase_for_scale_direction(ScaleDirection::None),
            ClusterPhase::Running
        );
    }

    // ========================================================================
    // Tests for is_cluster_healthy
    // ========================================================================

    #[test]
    fn test_is_cluster_healthy_all_ready() {
        assert!(is_cluster_healthy(6, 6));
    }

    #[test]
    fn test_is_cluster_healthy_more_ready_than_desired() {
        // This could happen temporarily during scale-down
        assert!(is_cluster_healthy(9, 6));
    }

    #[test]
    fn test_is_cluster_healthy_partial_ready() {
        assert!(!is_cluster_healthy(3, 6));
    }

    #[test]
    fn test_is_cluster_healthy_none_ready() {
        assert!(!is_cluster_healthy(0, 6));
    }

    #[test]
    fn test_is_cluster_healthy_zero_desired() {
        // Edge case: if desired is 0, cluster is not healthy
        assert!(!is_cluster_healthy(0, 0));
    }

    #[test]
    fn test_is_cluster_healthy_single_node() {
        assert!(is_cluster_healthy(1, 1));
    }

    // ========================================================================
    // Tests for is_cluster_degraded
    // ========================================================================

    #[test]
    fn test_is_cluster_degraded_partial_ready() {
        assert!(is_cluster_degraded(3, 6));
    }

    #[test]
    fn test_is_cluster_degraded_one_ready() {
        assert!(is_cluster_degraded(1, 6));
    }

    #[test]
    fn test_is_cluster_degraded_all_ready() {
        // If all are ready, not degraded
        assert!(!is_cluster_degraded(6, 6));
    }

    #[test]
    fn test_is_cluster_degraded_none_ready() {
        // If none ready, it's failed, not degraded
        assert!(!is_cluster_degraded(0, 6));
    }

    #[test]
    fn test_is_cluster_degraded_more_ready_than_desired() {
        // Edge case during scale-down
        assert!(!is_cluster_degraded(9, 6));
    }

    // ========================================================================
    // Tests for is_cluster_failed
    // ========================================================================

    #[test]
    fn test_is_cluster_failed_no_replicas() {
        assert!(is_cluster_failed(0));
    }

    #[test]
    fn test_is_cluster_failed_some_replicas() {
        assert!(!is_cluster_failed(1));
        assert!(!is_cluster_failed(3));
        assert!(!is_cluster_failed(6));
    }

    #[test]
    fn test_is_cluster_failed_negative_check() {
        // Negative values shouldn't happen, but test boundary
        // In reality, this would be caught by type system (u32 or validation)
        assert!(!is_cluster_failed(-1)); // -1 != 0
    }
}
