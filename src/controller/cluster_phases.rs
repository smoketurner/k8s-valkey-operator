//! Phase handlers for ValkeyCluster reconciliation.
//!
//! This module contains isolated, testable handlers for each phase
//! of the ValkeyCluster lifecycle. Each handler is responsible for
//! a single phase transition and returns the next phase.
//!
//! ## Phase Categories
//!
//! ### Initial Creation
//! Pending → Creating → WaitingForPods → InitializingCluster →
//! AssigningSlots → ConfiguringReplicas → VerifyingClusterHealth → Running
//!
//! ### Scale-Up (e.g., 3→4 masters)
//! Running → ScalingUpStatefulSet → WaitingForNewPods → AddingNodesToCluster →
//! RebalancingSlots → ConfiguringNewReplicas → VerifyingClusterHealth → Running
//!
//! ### Scale-Down (e.g., 4→3 masters)
//! Running → EvacuatingSlots → RemovingNodesFromCluster →
//! ScalingDownStatefulSet → VerifyingClusterHealth → Running
//!
//! ## Key Principle: No pending_changes
//!
//! Each phase handler queries cluster state directly to determine what work
//! needs to be done. The phase name itself encodes the direction (scale-up vs
//! scale-down), eliminating the need for context-passing via pending_changes.

use kube::Api;
use std::cmp::Ordering;
use tracing::{debug, info, warn};

use crate::{
    controller::{
        context::Context,
        error::Error,
        operation_coordination::{self, OperationType},
    },
    crd::{ClusterPhase, ValkeyCluster, total_pods},
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
///
/// Note: No pending_changes field - each handler queries cluster state directly.
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
    /// Current number of masters in the cluster (from CLUSTER INFO).
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
}

impl PhaseContext {
    /// Calculate the desired total number of pods.
    pub fn desired_replicas(&self) -> i32 {
        total_pods(self.target_masters, self.target_replicas_per_master)
    }

    /// Determine the scale direction based on current vs target masters.
    ///
    /// The phase name encodes direction, but this is useful for Running phase
    /// to determine which phase to transition to.
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

impl std::fmt::Display for ScaleDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScaleDirection::Up => write!(f, "up"),
            ScaleDirection::Down => write!(f, "down"),
            ScaleDirection::ReplicaChange => write!(f, "replica_change"),
            ScaleDirection::None => write!(f, "none"),
        }
    }
}

// ============================================================================
// Pure Logic Functions (testable without mocking)
// ============================================================================

/// Compute which nodes need to be added based on current and target master counts.
pub fn compute_nodes_to_add(name: &str, current_masters: i32, target_masters: i32) -> Vec<String> {
    if target_masters <= current_masters {
        return Vec::new();
    }
    (current_masters..target_masters)
        .map(|i| format!("{}-{}", name, i))
        .collect()
}

/// Compute which nodes need to be removed based on current and target master counts.
pub fn compute_nodes_to_remove(
    name: &str,
    current_masters: i32,
    target_masters: i32,
) -> Vec<String> {
    if target_masters >= current_masters {
        return Vec::new();
    }
    (target_masters..current_masters)
        .map(|i| format!("{}-{}", name, i))
        .collect()
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

// ============================================================================
// Initial Creation Phase Handlers
// ============================================================================

/// Handle Creating phase.
///
/// Creates owned Kubernetes resources (StatefulSet, Services, PDB, Certificate).
/// Transitions to WaitingForPods after resources are applied.
pub async fn handle_creating(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Creating Kubernetes resources");

    // Create owned resources
    create_resources_fn.await?;

    ctx.publish_normal_event(
        obj,
        "ResourcesCreated",
        "Creating",
        Some("StatefulSet, Services, and other resources created".to_string()),
    )
    .await;

    Ok(ClusterPhase::WaitingForPods.into())
}

/// Handle WaitingForPods phase.
///
/// Waits for all desired pods to be in Running state.
/// Transitions to InitializingCluster when all pods are running.
pub async fn handle_waiting_for_pods(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    // Ensure resources are still in sync
    create_resources_fn.await?;

    let desired_replicas = phase_ctx.desired_replicas();

    if phase_ctx.running_pods >= desired_replicas {
        ctx.publish_normal_event(
            obj,
            "PodsRunning",
            "WaitingForPods",
            Some(format!(
                "All pods running ({}/{}), proceeding to cluster initialization",
                phase_ctx.running_pods, desired_replicas
            )),
        )
        .await;
        Ok(ClusterPhase::InitializingCluster.into())
    } else {
        debug!(
            name = %name,
            running = phase_ctx.running_pods,
            desired = desired_replicas,
            "Waiting for pods to be running"
        );
        Ok(ClusterPhase::WaitingForPods.into())
    }
}

/// Handle InitializingCluster phase.
///
/// Executes CLUSTER MEET to connect all cluster nodes.
/// Transitions to AssigningSlots when successful.
pub async fn handle_initializing_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    cluster_meet_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Executing CLUSTER MEET to connect all nodes");

    // Acquire operation lock for initialization
    if let Err(e) =
        operation_coordination::start_operation(api, name, OperationType::Initializing).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock for initialization");
        return Ok(PhaseResult::with_message(
            ClusterPhase::InitializingCluster,
            "Waiting for operation lock",
        ));
    }

    let meet_result = cluster_meet_fn.await;

    // Release operation lock if initialization fails
    if meet_result.is_err() {
        let _ = operation_coordination::complete_operation(api, name, OperationType::Initializing)
            .await;
    }

    match meet_result {
        Ok(()) => {
            ctx.publish_normal_event(
                obj,
                "ClusterMeet",
                "InitializingCluster",
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
                "InitializingCluster",
                Some(format!("CLUSTER MEET failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::InitializingCluster.into())
        }
    }
}

/// Handle AssigningSlots phase.
///
/// Executes CLUSTER ADDSLOTS to assign hash slots to master nodes.
/// Transitions to ConfiguringReplicas or VerifyingClusterHealth.
pub async fn handle_assigning_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    slot_assignment_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Executing CLUSTER ADDSLOTS for initial slot assignment");

    // Ensure operation lock is held
    let has_lock = obj
        .status
        .as_ref()
        .and_then(|s| s.current_operation.as_ref())
        .map(|op| op == "initializing")
        .unwrap_or(false);

    if !has_lock
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
            ctx.publish_normal_event(
                obj,
                "SlotsAssigned",
                "AssigningSlots",
                Some("All 16384 hash slots assigned to masters".to_string()),
            )
            .await;

            // Transition based on whether we have replicas to configure
            if phase_ctx.target_replicas_per_master > 0 {
                Ok(ClusterPhase::ConfiguringReplicas.into())
            } else {
                // No replicas, skip to verification
                let _ = operation_coordination::complete_operation(
                    api,
                    name,
                    OperationType::Initializing,
                )
                .await;
                Ok(ClusterPhase::VerifyingClusterHealth.into())
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Slot assignment failed, will retry");
            ctx.publish_warning_event(
                obj,
                "SlotAssignmentFailed",
                "AssigningSlots",
                Some(format!("Slot assignment failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::AssigningSlots.into())
        }
    }
}

/// Handle ConfiguringReplicas phase.
///
/// Executes CLUSTER REPLICATE to set up replica relationships.
/// Transitions to VerifyingClusterHealth when successful.
pub async fn handle_configuring_replicas(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    setup_replicas_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    info!(name = %name, "Executing CLUSTER REPLICATE to set up replica relationships");

    match setup_replicas_fn.await {
        Ok(()) => {
            // Release operation lock when initialization completes
            let _ =
                operation_coordination::complete_operation(api, name, OperationType::Initializing)
                    .await;

            ctx.publish_normal_event(
                obj,
                "ReplicasConfigured",
                "ConfiguringReplicas",
                Some("All replica relationships established".to_string()),
            )
            .await;

            Ok(ClusterPhase::VerifyingClusterHealth.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Replica configuration failed, will retry");
            ctx.publish_warning_event(
                obj,
                "ReplicaConfigFailed",
                "ConfiguringReplicas",
                Some(format!("Replica configuration failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::ConfiguringReplicas.into())
        }
    }
}

// ============================================================================
// Steady State Phase Handler
// ============================================================================

/// Handle Running phase.
///
/// Monitors cluster health and detects spec changes.
/// Transitions to scale-up or scale-down phases when masters count changes.
pub async fn handle_running(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    // Ensure resources are in sync
    create_resources_fn.await?;

    // Detect scale operations by comparing spec masters to cluster masters
    match phase_ctx.target_masters.cmp(&phase_ctx.current_masters) {
        Ordering::Greater => {
            info!(
                name = %name,
                current = phase_ctx.current_masters,
                target = phase_ctx.target_masters,
                "Scale-up detected"
            );
            ctx.publish_normal_event(
                obj,
                "ScaleUpDetected",
                "Running",
                Some(format!(
                    "Scaling up from {} to {} masters",
                    phase_ctx.current_masters, phase_ctx.target_masters
                )),
            )
            .await;
            return Ok(ClusterPhase::ScalingUpStatefulSet.into());
        }
        Ordering::Less if phase_ctx.current_masters > 0 => {
            info!(
                name = %name,
                current = phase_ctx.current_masters,
                target = phase_ctx.target_masters,
                "Scale-down detected"
            );
            ctx.publish_normal_event(
                obj,
                "ScaleDownDetected",
                "Running",
                Some(format!(
                    "Scaling down from {} to {} masters",
                    phase_ctx.current_masters, phase_ctx.target_masters
                )),
            )
            .await;
            return Ok(ClusterPhase::EvacuatingSlots.into());
        }
        _ => {}
    }

    // Check for replica count changes (masters unchanged)
    let desired_replicas = phase_ctx.desired_replicas();
    if phase_ctx.running_pods != desired_replicas && phase_ctx.current_masters > 0 {
        if phase_ctx.running_pods > desired_replicas {
            // Replica scale-DOWN: go directly to node removal (skip slot evacuation)
            // Replicas don't hold slots, so EvacuatingSlots is unnecessary
            debug!(
                name = %name,
                running = phase_ctx.running_pods,
                desired = desired_replicas,
                "Replica scale-down detected"
            );
            ctx.publish_normal_event(
                obj,
                "ReplicaScaleDownDetected",
                "Running",
                Some(format!(
                    "Scaling down replicas from {} to {} pods",
                    phase_ctx.running_pods, desired_replicas
                )),
            )
            .await;
            return Ok(ClusterPhase::RemovingNodesFromCluster.into());
        }
        // Replica scale-UP: existing path
        debug!(
            name = %name,
            running = phase_ctx.running_pods,
            desired = desired_replicas,
            "Replica scale-up detected"
        );
        return Ok(ClusterPhase::ScalingUpStatefulSet.into());
    }

    // Check for degraded state
    if is_cluster_degraded(phase_ctx.ready_pods, desired_replicas) {
        ctx.publish_warning_event(
            obj,
            "Degraded",
            "Running",
            Some(format!(
                "Cluster degraded: {}/{} replicas ready",
                phase_ctx.ready_pods, desired_replicas
            )),
        )
        .await;
        return Ok(ClusterPhase::Degraded.into());
    }

    // Cluster is healthy, stay in Running
    debug!(
        name = %name,
        ready = phase_ctx.ready_pods,
        desired = desired_replicas,
        "Cluster running normally"
    );
    Ok(ClusterPhase::Running.into())
}

// ============================================================================
// Scale-Up Phase Handlers
// ============================================================================

/// Handle ScalingUpStatefulSet phase.
///
/// Updates the StatefulSet to increase replica count.
/// Transitions to WaitingForNewPods after StatefulSet is updated.
pub async fn handle_scaling_up_statefulset(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    let desired_replicas = phase_ctx.desired_replicas();

    info!(name = %name, target = desired_replicas, "Scaling up StatefulSet");

    // Acquire operation lock
    if let Err(e) = operation_coordination::start_operation(api, name, OperationType::Scaling).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock for scaling");
        return Ok(ClusterPhase::ScalingUpStatefulSet.into());
    }

    // Apply StatefulSet with new replica count
    create_resources_fn.await?;

    ctx.publish_normal_event(
        obj,
        "StatefulSetScaled",
        "ScalingUpStatefulSet",
        Some(format!(
            "StatefulSet scaled to {} replicas",
            desired_replicas
        )),
    )
    .await;

    Ok(ClusterPhase::WaitingForNewPods.into())
}

/// Handle WaitingForNewPods phase.
///
/// Waits for newly created pods to be in Running state.
/// Transitions to AddingNodesToCluster when all pods are running.
pub async fn handle_waiting_for_new_pods(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    let desired_replicas = phase_ctx.desired_replicas();

    if phase_ctx.running_pods >= desired_replicas {
        info!(
            name = %name,
            running = phase_ctx.running_pods,
            desired = desired_replicas,
            "All new pods running"
        );
        ctx.publish_normal_event(
            obj,
            "NewPodsRunning",
            "WaitingForNewPods",
            Some(format!("{} pods running", phase_ctx.running_pods)),
        )
        .await;
        Ok(ClusterPhase::AddingNodesToCluster.into())
    } else {
        debug!(
            name = %name,
            running = phase_ctx.running_pods,
            desired = desired_replicas,
            "Waiting for new pods to be running"
        );
        Ok(ClusterPhase::WaitingForNewPods.into())
    }
}

/// Handle AddingNodesToCluster phase.
///
/// Executes CLUSTER MEET for new nodes to join the cluster.
/// Transitions to RebalancingSlots when all nodes are in the cluster.
pub async fn handle_adding_nodes_to_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    cluster_meet_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    let desired_replicas = phase_ctx.desired_replicas();

    info!(name = %name, "Adding new nodes to cluster via CLUSTER MEET");

    // Check if all nodes are already in the cluster
    if phase_ctx.nodes_in_cluster >= desired_replicas {
        ctx.publish_normal_event(
            obj,
            "NodesInCluster",
            "AddingNodesToCluster",
            Some(format!(
                "All {} nodes are in the cluster",
                phase_ctx.nodes_in_cluster
            )),
        )
        .await;
        return Ok(ClusterPhase::RebalancingSlots.into());
    }

    // Execute CLUSTER MEET for new nodes
    match cluster_meet_fn.await {
        Ok(()) => {
            ctx.publish_normal_event(
                obj,
                "NodesAdded",
                "AddingNodesToCluster",
                Some("New nodes added to cluster".to_string()),
            )
            .await;
            // Stay in this phase to verify all nodes joined
            Ok(ClusterPhase::AddingNodesToCluster.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Failed to add nodes, will retry");
            ctx.publish_warning_event(
                obj,
                "NodeAddFailed",
                "AddingNodesToCluster",
                Some(format!("Failed to add nodes: {}", e)),
            )
            .await;
            Ok(ClusterPhase::AddingNodesToCluster.into())
        }
    }
}

/// Handle RebalancingSlots phase.
///
/// Migrates hash slots to new masters using CLUSTER MIGRATESLOTS.
/// Transitions to ConfiguringNewReplicas or VerifyingClusterHealth.
pub async fn handle_rebalancing_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    rebalance_fn: impl std::future::Future<Output = Result<bool, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    info!(name = %name, "Rebalancing slots to new masters");

    match rebalance_fn.await {
        Ok(true) => {
            // Rebalancing complete
            ctx.publish_normal_event(
                obj,
                "SlotsRebalanced",
                "RebalancingSlots",
                Some("Slots rebalanced across all masters".to_string()),
            )
            .await;

            // Transition based on whether we have new replicas to configure
            if phase_ctx.target_replicas_per_master > 0 {
                Ok(ClusterPhase::ConfiguringNewReplicas.into())
            } else {
                Ok(ClusterPhase::VerifyingClusterHealth.into())
            }
        }
        Ok(false) => {
            // Rebalancing still in progress
            debug!(name = %name, "Slot rebalancing in progress");
            Ok(ClusterPhase::RebalancingSlots.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Slot rebalancing failed, will retry");
            ctx.publish_warning_event(
                obj,
                "RebalanceFailed",
                "RebalancingSlots",
                Some(format!("Slot rebalancing failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::RebalancingSlots.into())
        }
    }
}

/// Handle ConfiguringNewReplicas phase.
///
/// Executes CLUSTER REPLICATE for replicas of new masters.
/// Transitions to VerifyingClusterHealth when complete.
pub async fn handle_configuring_new_replicas(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    setup_replicas_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    info!(name = %name, "Configuring replicas for new masters");

    match setup_replicas_fn.await {
        Ok(()) => {
            // Release operation lock
            let _ =
                operation_coordination::complete_operation(api, name, OperationType::Scaling).await;

            ctx.publish_normal_event(
                obj,
                "NewReplicasConfigured",
                "ConfiguringNewReplicas",
                Some("Replicas configured for new masters".to_string()),
            )
            .await;

            Ok(ClusterPhase::VerifyingClusterHealth.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Replica configuration failed, will retry");
            ctx.publish_warning_event(
                obj,
                "ReplicaConfigFailed",
                "ConfiguringNewReplicas",
                Some(format!("Replica configuration failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::ConfiguringNewReplicas.into())
        }
    }
}

// ============================================================================
// Scale-Down Phase Handlers
// ============================================================================

/// Handle EvacuatingSlots phase.
///
/// Migrates slots off of nodes being removed using CLUSTER MIGRATESLOTS.
/// Transitions to RemovingNodesFromCluster when evacuation is complete.
pub async fn handle_evacuating_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    evacuate_fn: impl std::future::Future<Output = Result<bool, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    info!(name = %name, "Evacuating slots from nodes being removed");

    // Acquire operation lock
    if let Err(e) = operation_coordination::start_operation(api, name, OperationType::Scaling).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock for evacuation");
        return Ok(ClusterPhase::EvacuatingSlots.into());
    }

    match evacuate_fn.await {
        Ok(true) => {
            // Evacuation complete
            ctx.publish_normal_event(
                obj,
                "SlotsEvacuated",
                "EvacuatingSlots",
                Some("All slots evacuated from nodes being removed".to_string()),
            )
            .await;
            Ok(ClusterPhase::RemovingNodesFromCluster.into())
        }
        Ok(false) => {
            // Evacuation still in progress
            debug!(name = %name, "Slot evacuation in progress");
            Ok(ClusterPhase::EvacuatingSlots.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Slot evacuation failed, will retry");
            ctx.publish_warning_event(
                obj,
                "EvacuationFailed",
                "EvacuatingSlots",
                Some(format!("Slot evacuation failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::EvacuatingSlots.into())
        }
    }
}

/// Handle RemovingNodesFromCluster phase.
///
/// Executes CLUSTER FORGET for nodes being removed.
/// Transitions to ScalingDownStatefulSet when all nodes are forgotten.
pub async fn handle_removing_nodes_from_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    forget_nodes_fn: impl std::future::Future<Output = Result<bool, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;

    info!(name = %name, "Removing nodes from cluster via CLUSTER FORGET");

    match forget_nodes_fn.await {
        Ok(true) => {
            // All nodes forgotten
            ctx.publish_normal_event(
                obj,
                "NodesRemoved",
                "RemovingNodesFromCluster",
                Some("All nodes removed from cluster".to_string()),
            )
            .await;
            Ok(ClusterPhase::ScalingDownStatefulSet.into())
        }
        Ok(false) => {
            // Still removing nodes
            debug!(name = %name, "Node removal in progress");
            Ok(ClusterPhase::RemovingNodesFromCluster.into())
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Node removal failed, will retry");
            ctx.publish_warning_event(
                obj,
                "NodeRemovalFailed",
                "RemovingNodesFromCluster",
                Some(format!("Node removal failed: {}", e)),
            )
            .await;
            Ok(ClusterPhase::RemovingNodesFromCluster.into())
        }
    }
}

/// Handle ScalingDownStatefulSet phase.
///
/// Decreases the StatefulSet replica count.
/// Transitions to VerifyingClusterHealth when StatefulSet is updated.
pub async fn handle_scaling_down_statefulset(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    let desired_replicas = phase_ctx.desired_replicas();

    info!(name = %name, target = desired_replicas, "Scaling down StatefulSet");

    // Apply StatefulSet with reduced replica count
    create_resources_fn.await?;

    // Check if StatefulSet has scaled down
    if phase_ctx.running_pods <= desired_replicas {
        // Release operation lock
        let _ = operation_coordination::complete_operation(api, name, OperationType::Scaling).await;

        ctx.publish_normal_event(
            obj,
            "StatefulSetScaledDown",
            "ScalingDownStatefulSet",
            Some(format!(
                "StatefulSet scaled down to {} replicas",
                desired_replicas
            )),
        )
        .await;
        Ok(ClusterPhase::VerifyingClusterHealth.into())
    } else {
        debug!(
            name = %name,
            running = phase_ctx.running_pods,
            desired = desired_replicas,
            "Waiting for StatefulSet to scale down"
        );
        Ok(ClusterPhase::ScalingDownStatefulSet.into())
    }
}

// ============================================================================
// Completion Phase Handler
// ============================================================================

/// Handle VerifyingClusterHealth phase.
///
/// Performs final health check before transitioning to Running.
pub async fn handle_verifying_cluster_health(
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

            // Clear operation progress
            let mut status = obj.status.clone().unwrap_or_default();
            status.operation_progress = None;
            status.message = "Cluster healthy".to_string();

            let patch = serde_json::json!({ "status": status });
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
                "VerifyingClusterHealth",
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
                Ok(ClusterPhase::VerifyingClusterHealth.into())
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Health check failed during verification");
            Ok(ClusterPhase::VerifyingClusterHealth.into())
        }
    }
}

// ============================================================================
// Problem State Handlers
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
) -> Result<PhaseResult, Error> {
    // Ensure resources are in sync
    create_resources_fn.await?;

    let desired_replicas = phase_ctx.desired_replicas();

    // Check for scale operations even in degraded state
    if phase_ctx.current_masters > 0 {
        match phase_ctx.target_masters.cmp(&phase_ctx.current_masters) {
            Ordering::Greater => {
                return Ok(ClusterPhase::ScalingUpStatefulSet.into());
            }
            Ordering::Less => {
                return Ok(ClusterPhase::EvacuatingSlots.into());
            }
            Ordering::Equal => {
                // Check for replica-only scale-down (masters unchanged)
                if phase_ctx.running_pods > desired_replicas {
                    return Ok(ClusterPhase::RemovingNodesFromCluster.into());
                }
            }
        }
    }

    if phase_ctx.ready_pods >= desired_replicas {
        ctx.publish_normal_event(
            obj,
            "Recovered",
            "Degraded",
            Some(format!(
                "Cluster recovered: {}/{} replicas ready",
                phase_ctx.ready_pods, desired_replicas
            )),
        )
        .await;
        Ok(ClusterPhase::Running.into())
    } else if phase_ctx.ready_pods == 0 {
        ctx.publish_warning_event(
            obj,
            "Failed",
            "Degraded",
            Some("Cluster failed: no replicas available".to_string()),
        )
        .await;
        Ok(ClusterPhase::Failed.into())
    } else {
        debug!(
            ready = phase_ctx.ready_pods,
            desired = desired_replicas,
            "Cluster still degraded, waiting for recovery"
        );
        Ok(ClusterPhase::Degraded.into())
    }
}

/// Handle Failed phase.
///
/// Attempts recovery via stale IP detection and cleanup.
/// Transitions to Degraded or Running if recovery succeeds.
pub async fn handle_failed(
    obj: &ValkeyCluster,
    ctx: &Context,
    phase_ctx: &PhaseContext,
    create_resources_fn: impl std::future::Future<Output = Result<(), Error>>,
    recovery_fn: impl std::future::Future<Output = Result<bool, Error>>,
) -> Result<PhaseResult, Error> {
    let name = &phase_ctx.name;
    let desired_replicas = phase_ctx.desired_replicas();

    // Ensure resources are in sync
    create_resources_fn.await?;

    // Check if cluster has recovered
    if phase_ctx.ready_pods >= desired_replicas {
        ctx.publish_normal_event(
            obj,
            "Recovered",
            "Failed",
            Some(format!(
                "Cluster recovered: {}/{} replicas ready",
                phase_ctx.ready_pods, desired_replicas
            )),
        )
        .await;
        return Ok(ClusterPhase::Running.into());
    }

    // Attempt recovery (stale IP detection, etc.)
    if phase_ctx.ready_pods > 0 {
        match recovery_fn.await {
            Ok(true) => {
                ctx.publish_normal_event(
                    obj,
                    "RecoveryInitiated",
                    "Failed",
                    Some("Recovery process initiated".to_string()),
                )
                .await;
                return Ok(ClusterPhase::Degraded.into());
            }
            Ok(false) => {
                debug!(name = %name, "No recovery actions taken");
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Recovery attempt failed");
            }
        }
    }

    // Stay in Failed
    debug!(
        name = %name,
        ready = phase_ctx.ready_pods,
        desired = desired_replicas,
        "Cluster still failed, waiting for intervention"
    );
    Ok(ClusterPhase::Failed.into())
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
        let waiting = InitResult::WaitingForPods;
        let success = InitResult::Success;
        let failed = InitResult::Failed("test error".to_string());

        assert!(matches!(waiting, InitResult::WaitingForPods));
        assert!(matches!(success, InitResult::Success));
        assert!(matches!(failed, InitResult::Failed(_)));
    }

    #[test]
    fn test_phase_context_no_current_masters() {
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
        };

        // With 0 current_masters, we should not report scale up/down
        assert_eq!(ctx.scale_direction(), ScaleDirection::ReplicaChange);
    }

    // ========================================================================
    // Tests for compute_nodes_to_add
    // ========================================================================

    #[test]
    fn test_compute_nodes_to_add_scale_up() {
        let to_add = compute_nodes_to_add("mycluster", 3, 6);

        assert_eq!(to_add, vec!["mycluster-3", "mycluster-4", "mycluster-5"]);
    }

    #[test]
    fn test_compute_nodes_to_add_single_node() {
        let to_add = compute_nodes_to_add("vc", 3, 4);

        assert_eq!(to_add, vec!["vc-3"]);
    }

    #[test]
    fn test_compute_nodes_to_add_no_change() {
        let to_add = compute_nodes_to_add("mycluster", 3, 3);

        assert!(to_add.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_add_scale_down() {
        let to_add = compute_nodes_to_add("mycluster", 6, 3);

        assert!(to_add.is_empty());
    }

    // ========================================================================
    // Tests for compute_nodes_to_remove
    // ========================================================================

    #[test]
    fn test_compute_nodes_to_remove_scale_down() {
        let to_remove = compute_nodes_to_remove("mycluster", 6, 3);

        assert_eq!(to_remove, vec!["mycluster-3", "mycluster-4", "mycluster-5"]);
    }

    #[test]
    fn test_compute_nodes_to_remove_single_node() {
        let to_remove = compute_nodes_to_remove("test", 4, 3);

        assert_eq!(to_remove, vec!["test-3"]);
    }

    #[test]
    fn test_compute_nodes_to_remove_no_change() {
        let to_remove = compute_nodes_to_remove("mycluster", 3, 3);

        assert!(to_remove.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_remove_scale_up() {
        let to_remove = compute_nodes_to_remove("mycluster", 3, 6);

        assert!(to_remove.is_empty());
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
        assert!(!is_cluster_degraded(6, 6));
    }

    #[test]
    fn test_is_cluster_degraded_none_ready() {
        assert!(!is_cluster_degraded(0, 6));
    }

    #[test]
    fn test_is_cluster_degraded_more_ready_than_desired() {
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
}
