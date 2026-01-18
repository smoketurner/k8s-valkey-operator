//! Formal finite state machine for ValkeyCluster lifecycle management.
//!
//! This module implements a proper FSM pattern with explicit state transitions,
//! guards, and actions. It ensures that only valid state transitions occur and
//! provides a clear audit trail of resource lifecycle events.
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

use std::fmt;

use crate::crd::ClusterPhase;

/// Events that trigger state transitions in the resource lifecycle.
///
/// Note: The new granular phases are self-describing, so fewer events are needed.
/// The phase name itself encodes the direction (e.g., ScalingUpStatefulSet vs ScalingDownStatefulSet).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClusterEvent {
    /// Kubernetes resources have been applied to the cluster
    ResourcesApplied,
    /// All desired pods are running (may not be ready yet)
    PodsRunning,
    /// Some replicas are ready but not all (resource is degraded)
    ReplicasDegraded,
    /// An error occurred during reconciliation
    ReconcileError,
    /// Deletion timestamp has been set on the resource
    DeletionRequested,
    /// Recovery from failed state has been initiated
    RecoveryInitiated,
    /// Resource has fully recovered from degraded state
    FullyRecovered,
    /// Phase-specific work completed successfully
    PhaseComplete,
    /// Scale-up operation detected (masters increased)
    ScaleUpDetected,
    /// Scale-down operation detected (masters decreased)
    ScaleDownDetected,
    /// Cluster health verification passed
    ClusterHealthy,
}

impl fmt::Display for ClusterEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterEvent::ResourcesApplied => write!(f, "ResourcesApplied"),
            ClusterEvent::PodsRunning => write!(f, "PodsRunning"),
            ClusterEvent::ReplicasDegraded => write!(f, "ReplicasDegraded"),
            ClusterEvent::ReconcileError => write!(f, "ReconcileError"),
            ClusterEvent::DeletionRequested => write!(f, "DeletionRequested"),
            ClusterEvent::RecoveryInitiated => write!(f, "RecoveryInitiated"),
            ClusterEvent::FullyRecovered => write!(f, "FullyRecovered"),
            ClusterEvent::PhaseComplete => write!(f, "PhaseComplete"),
            ClusterEvent::ScaleUpDetected => write!(f, "ScaleUpDetected"),
            ClusterEvent::ScaleDownDetected => write!(f, "ScaleDownDetected"),
            ClusterEvent::ClusterHealthy => write!(f, "ClusterHealthy"),
        }
    }
}

/// Context information available during state transitions
#[derive(Debug, Clone)]
pub struct TransitionContext {
    /// Current number of ready replicas
    pub ready_replicas: i32,
    /// Desired number of replicas
    pub desired_replicas: i32,
    /// Whether the spec has changed (generation mismatch)
    pub spec_changed: bool,
    /// Error message if in failed state
    pub error_message: Option<String>,
    /// Current retry count for backoff
    pub retry_count: i32,
}

impl TransitionContext {
    /// Create a new transition context
    pub fn new(ready_replicas: i32, desired_replicas: i32) -> Self {
        Self {
            ready_replicas,
            desired_replicas,
            spec_changed: false,
            error_message: None,
            retry_count: 0,
        }
    }

    /// Check if all replicas are ready
    pub fn all_replicas_ready(&self) -> bool {
        self.ready_replicas >= self.desired_replicas && self.desired_replicas > 0
    }

    /// Check if resource is degraded (some but not all replicas ready)
    pub fn is_degraded(&self) -> bool {
        self.ready_replicas > 0 && self.ready_replicas < self.desired_replicas
    }

    /// Check if resource has no ready replicas
    pub fn no_replicas_ready(&self) -> bool {
        self.ready_replicas == 0
    }

    /// Set spec_changed flag
    pub fn with_spec_changed(mut self, changed: bool) -> Self {
        self.spec_changed = changed;
        self
    }

    /// Set error message
    pub fn with_error(mut self, message: String) -> Self {
        self.error_message = Some(message);
        self
    }
}

/// A state transition definition with optional guard
#[derive(Debug)]
pub struct Transition {
    /// Source state
    pub from: ClusterPhase,
    /// Target state
    pub to: ClusterPhase,
    /// Event that triggers this transition
    pub event: ClusterEvent,
    /// Human-readable description of this transition
    pub description: &'static str,
}

impl Transition {
    /// Create a new transition
    const fn new(
        from: ClusterPhase,
        to: ClusterPhase,
        event: ClusterEvent,
        description: &'static str,
    ) -> Self {
        Self {
            from,
            to,
            event,
            description,
        }
    }
}

/// Result of attempting a state transition
#[derive(Debug)]
pub enum TransitionResult {
    /// Transition was successful
    Success {
        from: ClusterPhase,
        to: ClusterPhase,
        event: ClusterEvent,
        description: &'static str,
    },
    /// Transition was not valid for current state
    InvalidTransition {
        current: ClusterPhase,
        event: ClusterEvent,
    },
    /// Guard condition prevented the transition
    GuardFailed {
        from: ClusterPhase,
        to: ClusterPhase,
        event: ClusterEvent,
        reason: String,
    },
}

/// Formal state machine for ValkeyCluster lifecycle
pub struct ClusterStateMachine {
    transitions: Vec<Transition>,
}

impl Default for ClusterStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterStateMachine {
    /// Create a new state machine with the defined transition table
    pub fn new() -> Self {
        Self {
            transitions: vec![
                // ========================================
                // Pending state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::Pending,
                    ClusterPhase::Creating,
                    ClusterEvent::ResourcesApplied,
                    "Starting resource creation",
                ),
                Transition::new(
                    ClusterPhase::Pending,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Validation failed during pending",
                ),
                Transition::new(
                    ClusterPhase::Pending,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested before creation",
                ),
                // ========================================
                // Creating state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::Creating,
                    ClusterPhase::WaitingForPods,
                    ClusterEvent::ResourcesApplied,
                    "Resources created, waiting for pods",
                ),
                Transition::new(
                    ClusterPhase::Creating,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during resource creation",
                ),
                Transition::new(
                    ClusterPhase::Creating,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during creation",
                ),
                // ========================================
                // WaitingForPods state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::WaitingForPods,
                    ClusterPhase::InitializingCluster,
                    ClusterEvent::PodsRunning,
                    "All pods running, initializing cluster",
                ),
                Transition::new(
                    ClusterPhase::WaitingForPods,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error waiting for pods",
                ),
                Transition::new(
                    ClusterPhase::WaitingForPods,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while waiting",
                ),
                // ========================================
                // InitializingCluster state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::InitializingCluster,
                    ClusterPhase::AssigningSlots,
                    ClusterEvent::PhaseComplete,
                    "Cluster nodes connected via CLUSTER MEET",
                ),
                Transition::new(
                    ClusterPhase::InitializingCluster,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during cluster initialization",
                ),
                Transition::new(
                    ClusterPhase::InitializingCluster,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during initialization",
                ),
                // ========================================
                // AssigningSlots state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::AssigningSlots,
                    ClusterPhase::ConfiguringReplicas,
                    ClusterEvent::PhaseComplete,
                    "Slots assigned, configuring replicas",
                ),
                Transition::new(
                    ClusterPhase::AssigningSlots,
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterEvent::ClusterHealthy,
                    "Slots assigned, no replicas to configure",
                ),
                Transition::new(
                    ClusterPhase::AssigningSlots,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during slot assignment",
                ),
                Transition::new(
                    ClusterPhase::AssigningSlots,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during slot assignment",
                ),
                // ========================================
                // ConfiguringReplicas state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::ConfiguringReplicas,
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterEvent::PhaseComplete,
                    "Replicas configured, verifying cluster",
                ),
                Transition::new(
                    ClusterPhase::ConfiguringReplicas,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error configuring replicas",
                ),
                Transition::new(
                    ClusterPhase::ConfiguringReplicas,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while configuring replicas",
                ),
                // ========================================
                // Running state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::Running,
                    ClusterPhase::ScalingUpStatefulSet,
                    ClusterEvent::ScaleUpDetected,
                    "Scale-up detected, increasing StatefulSet replicas",
                ),
                Transition::new(
                    ClusterPhase::Running,
                    ClusterPhase::EvacuatingSlots,
                    ClusterEvent::ScaleDownDetected,
                    "Scale-down detected, evacuating slots first",
                ),
                Transition::new(
                    ClusterPhase::Running,
                    ClusterPhase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Resource health degraded",
                ),
                Transition::new(
                    ClusterPhase::Running,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error while running",
                ),
                Transition::new(
                    ClusterPhase::Running,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // ========================================
                // Scale-Up Path: ScalingUpStatefulSet
                // ========================================
                Transition::new(
                    ClusterPhase::ScalingUpStatefulSet,
                    ClusterPhase::WaitingForNewPods,
                    ClusterEvent::PhaseComplete,
                    "StatefulSet scaled up, waiting for new pods",
                ),
                Transition::new(
                    ClusterPhase::ScalingUpStatefulSet,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error scaling up StatefulSet",
                ),
                Transition::new(
                    ClusterPhase::ScalingUpStatefulSet,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during scale-up",
                ),
                // ========================================
                // Scale-Up Path: WaitingForNewPods
                // ========================================
                Transition::new(
                    ClusterPhase::WaitingForNewPods,
                    ClusterPhase::AddingNodesToCluster,
                    ClusterEvent::PodsRunning,
                    "New pods running, adding to cluster",
                ),
                Transition::new(
                    ClusterPhase::WaitingForNewPods,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error waiting for new pods",
                ),
                Transition::new(
                    ClusterPhase::WaitingForNewPods,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while waiting for pods",
                ),
                // ========================================
                // Scale-Up Path: AddingNodesToCluster
                // ========================================
                Transition::new(
                    ClusterPhase::AddingNodesToCluster,
                    ClusterPhase::RebalancingSlots,
                    ClusterEvent::PhaseComplete,
                    "Nodes added, rebalancing slots",
                ),
                Transition::new(
                    ClusterPhase::AddingNodesToCluster,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error adding nodes to cluster",
                ),
                Transition::new(
                    ClusterPhase::AddingNodesToCluster,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while adding nodes",
                ),
                // ========================================
                // Scale-Up Path: RebalancingSlots
                // ========================================
                Transition::new(
                    ClusterPhase::RebalancingSlots,
                    ClusterPhase::ConfiguringNewReplicas,
                    ClusterEvent::PhaseComplete,
                    "Slots rebalanced, configuring new replicas",
                ),
                Transition::new(
                    ClusterPhase::RebalancingSlots,
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterEvent::ClusterHealthy,
                    "Slots rebalanced, no replicas to configure",
                ),
                Transition::new(
                    ClusterPhase::RebalancingSlots,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error rebalancing slots",
                ),
                Transition::new(
                    ClusterPhase::RebalancingSlots,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during rebalancing",
                ),
                // ========================================
                // Scale-Up Path: ConfiguringNewReplicas
                // ========================================
                Transition::new(
                    ClusterPhase::ConfiguringNewReplicas,
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterEvent::PhaseComplete,
                    "New replicas configured, verifying cluster",
                ),
                Transition::new(
                    ClusterPhase::ConfiguringNewReplicas,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error configuring new replicas",
                ),
                Transition::new(
                    ClusterPhase::ConfiguringNewReplicas,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while configuring replicas",
                ),
                // ========================================
                // Scale-Down Path: EvacuatingSlots
                // ========================================
                Transition::new(
                    ClusterPhase::EvacuatingSlots,
                    ClusterPhase::RemovingNodesFromCluster,
                    ClusterEvent::PhaseComplete,
                    "Slots evacuated, removing nodes from cluster",
                ),
                Transition::new(
                    ClusterPhase::EvacuatingSlots,
                    ClusterPhase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Cluster degraded during evacuation",
                ),
                Transition::new(
                    ClusterPhase::EvacuatingSlots,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error evacuating slots",
                ),
                Transition::new(
                    ClusterPhase::EvacuatingSlots,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during evacuation",
                ),
                // ========================================
                // Scale-Down Path: RemovingNodesFromCluster
                // ========================================
                Transition::new(
                    ClusterPhase::RemovingNodesFromCluster,
                    ClusterPhase::ScalingDownStatefulSet,
                    ClusterEvent::PhaseComplete,
                    "Nodes removed, scaling down StatefulSet",
                ),
                Transition::new(
                    ClusterPhase::RemovingNodesFromCluster,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error removing nodes from cluster",
                ),
                Transition::new(
                    ClusterPhase::RemovingNodesFromCluster,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while removing nodes",
                ),
                // ========================================
                // Scale-Down Path: ScalingDownStatefulSet
                // ========================================
                Transition::new(
                    ClusterPhase::ScalingDownStatefulSet,
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterEvent::PhaseComplete,
                    "StatefulSet scaled down, verifying cluster",
                ),
                Transition::new(
                    ClusterPhase::ScalingDownStatefulSet,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error scaling down StatefulSet",
                ),
                Transition::new(
                    ClusterPhase::ScalingDownStatefulSet,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during scale-down",
                ),
                // ========================================
                // VerifyingClusterHealth state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterPhase::Running,
                    ClusterEvent::ClusterHealthy,
                    "Cluster verified healthy",
                ),
                Transition::new(
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterPhase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Cluster verification found degraded state",
                ),
                Transition::new(
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during cluster verification",
                ),
                Transition::new(
                    ClusterPhase::VerifyingClusterHealth,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during verification",
                ),
                // ========================================
                // Degraded state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::Running,
                    ClusterEvent::FullyRecovered,
                    "Resource fully recovered from degraded state",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::Running,
                    ClusterEvent::ClusterHealthy,
                    "Cluster now healthy",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::ScalingUpStatefulSet,
                    ClusterEvent::ScaleUpDetected,
                    "Scale-up detected while degraded",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::EvacuatingSlots,
                    ClusterEvent::ScaleDownDetected,
                    "Scale-down detected while degraded",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Degraded resource encountered error",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while degraded",
                ),
                // ========================================
                // Failed state transitions
                // ========================================
                Transition::new(
                    ClusterPhase::Failed,
                    ClusterPhase::Degraded,
                    ClusterEvent::RecoveryInitiated,
                    "Recovery initiated from failed state",
                ),
                Transition::new(
                    ClusterPhase::Failed,
                    ClusterPhase::Running,
                    ClusterEvent::ClusterHealthy,
                    "Cluster recovered to healthy state",
                ),
                Transition::new(
                    ClusterPhase::Failed,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while failed",
                ),
                // ========================================
                // Deleting state transitions (terminal)
                // ========================================
                // Deleting is a terminal state - no transitions out except completion
            ],
        }
    }

    /// Attempt to transition to a new state based on an event
    pub fn transition(
        &self,
        current: &ClusterPhase,
        event: ClusterEvent,
        ctx: &TransitionContext,
    ) -> TransitionResult {
        // Find a matching transition
        let transition = self
            .transitions
            .iter()
            .find(|t| t.from == *current && t.event == event);

        match transition {
            Some(t) => {
                // Apply guards based on the transition
                if let Some(reason) = self.check_guard(t, ctx) {
                    TransitionResult::GuardFailed {
                        from: t.from,
                        to: t.to,
                        event,
                        reason,
                    }
                } else {
                    TransitionResult::Success {
                        from: t.from,
                        to: t.to,
                        event,
                        description: t.description,
                    }
                }
            }
            None => TransitionResult::InvalidTransition {
                current: *current,
                event,
            },
        }
    }

    /// Check if a transition is valid (ignoring guards)
    pub fn can_transition(&self, from: &ClusterPhase, event: &ClusterEvent) -> bool {
        self.transitions
            .iter()
            .any(|t| t.from == *from && t.event == *event)
    }

    /// Get all valid events for a given state
    pub fn valid_events(&self, state: &ClusterPhase) -> Vec<&ClusterEvent> {
        self.transitions
            .iter()
            .filter(|t| t.from == *state)
            .map(|t| &t.event)
            .collect()
    }

    /// Check guard conditions for a transition
    fn check_guard(&self, transition: &Transition, ctx: &TransitionContext) -> Option<String> {
        match (&transition.from, &transition.to, &transition.event) {
            // Guard: ReplicasDegraded requires partial readiness
            (_, ClusterPhase::Degraded, ClusterEvent::ReplicasDegraded) => {
                if !ctx.is_degraded() {
                    Some(format!(
                        "Resource is not degraded: {}/{} replicas ready",
                        ctx.ready_replicas, ctx.desired_replicas
                    ))
                } else {
                    None
                }
            }
            // Guard: FullyRecovered requires all replicas ready
            (ClusterPhase::Degraded, ClusterPhase::Running, ClusterEvent::FullyRecovered) => {
                if !ctx.all_replicas_ready() {
                    Some(format!(
                        "Cannot mark as recovered, not all replicas ready: {}/{}",
                        ctx.ready_replicas, ctx.desired_replicas
                    ))
                } else {
                    None
                }
            }
            // No guard for other transitions
            _ => None,
        }
    }
}

/// Determine the appropriate event based on context
pub fn determine_event(
    current_phase: &ClusterPhase,
    ctx: &TransitionContext,
    has_deletion_timestamp: bool,
) -> ClusterEvent {
    // Deletion always takes priority
    if has_deletion_timestamp {
        return ClusterEvent::DeletionRequested;
    }

    // Determine event based on replica status and current phase
    match current_phase {
        ClusterPhase::Pending => ClusterEvent::ResourcesApplied,
        ClusterPhase::Creating => ClusterEvent::ResourcesApplied,
        ClusterPhase::WaitingForPods | ClusterPhase::WaitingForNewPods => {
            if ctx.all_replicas_ready() {
                ClusterEvent::PodsRunning
            } else {
                // Stay in waiting
                ClusterEvent::ResourcesApplied
            }
        }
        ClusterPhase::Failed => {
            if ctx.all_replicas_ready() {
                ClusterEvent::RecoveryInitiated
            } else {
                ClusterEvent::ReconcileError
            }
        }
        ClusterPhase::Degraded => {
            if ctx.all_replicas_ready() {
                ClusterEvent::FullyRecovered
            } else if ctx.is_degraded() {
                ClusterEvent::ReplicasDegraded
            } else {
                ClusterEvent::ReconcileError
            }
        }
        ClusterPhase::Running => {
            if ctx.is_degraded() {
                ClusterEvent::ReplicasDegraded
            } else {
                // Scale operations are detected by the reconciler
                ClusterEvent::ClusterHealthy
            }
        }
        // Phase-driven transitions: these phases complete via PhaseComplete
        ClusterPhase::InitializingCluster
        | ClusterPhase::AssigningSlots
        | ClusterPhase::ConfiguringReplicas
        | ClusterPhase::ScalingUpStatefulSet
        | ClusterPhase::AddingNodesToCluster
        | ClusterPhase::RebalancingSlots
        | ClusterPhase::ConfiguringNewReplicas
        | ClusterPhase::EvacuatingSlots
        | ClusterPhase::RemovingNodesFromCluster
        | ClusterPhase::ScalingDownStatefulSet => ClusterEvent::PhaseComplete,
        ClusterPhase::VerifyingClusterHealth => {
            if ctx.is_degraded() {
                ClusterEvent::ReplicasDegraded
            } else {
                ClusterEvent::ClusterHealthy
            }
        }
        ClusterPhase::Deleting => ClusterEvent::DeletionRequested,
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap,
    clippy::panic
)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_to_creating() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(0, 3);

        let result = sm.transition(&ClusterPhase::Pending, ClusterEvent::ResourcesApplied, &ctx);

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Pending);
                assert_eq!(to, ClusterPhase::Creating);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_creating_to_waiting_for_pods() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(0, 3);

        let result = sm.transition(
            &ClusterPhase::Creating,
            ClusterEvent::ResourcesApplied,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Creating);
                assert_eq!(to, ClusterPhase::WaitingForPods);
            }
            _ => panic!("Expected successful transition to WaitingForPods"),
        }
    }

    #[test]
    fn test_waiting_for_pods_to_initializing() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(
            &ClusterPhase::WaitingForPods,
            ClusterEvent::PodsRunning,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::WaitingForPods);
                assert_eq!(to, ClusterPhase::InitializingCluster);
            }
            _ => panic!("Expected successful transition to InitializingCluster"),
        }
    }

    #[test]
    fn test_initializing_to_assigning_slots() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(
            &ClusterPhase::InitializingCluster,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::InitializingCluster);
                assert_eq!(to, ClusterPhase::AssigningSlots);
            }
            _ => panic!("Expected successful transition to AssigningSlots"),
        }
    }

    #[test]
    fn test_assigning_slots_to_configuring_replicas() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(
            &ClusterPhase::AssigningSlots,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::AssigningSlots);
                assert_eq!(to, ClusterPhase::ConfiguringReplicas);
            }
            _ => panic!("Expected successful transition to ConfiguringReplicas"),
        }
    }

    #[test]
    fn test_configuring_replicas_to_verifying() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(
            &ClusterPhase::ConfiguringReplicas,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::ConfiguringReplicas);
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth);
            }
            _ => panic!("Expected successful transition to VerifyingClusterHealth"),
        }
    }

    #[test]
    fn test_verifying_to_running() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(
            &ClusterPhase::VerifyingClusterHealth,
            ClusterEvent::ClusterHealthy,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::VerifyingClusterHealth);
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition to Running"),
        }
    }

    #[test]
    fn test_running_to_degraded_guard() {
        let sm = ClusterStateMachine::new();

        // Should succeed with 2/3 replicas (degraded)
        let ctx = TransitionContext::new(2, 3);
        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ReplicasDegraded, &ctx);
        assert!(matches!(result, TransitionResult::Success { .. }));

        // Should fail with 3/3 replicas (not degraded)
        let ctx = TransitionContext::new(3, 3);
        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ReplicasDegraded, &ctx);
        assert!(matches!(result, TransitionResult::GuardFailed { .. }));
    }

    #[test]
    fn test_running_to_scaling_up() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ScaleUpDetected, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Running);
                assert_eq!(to, ClusterPhase::ScalingUpStatefulSet);
            }
            _ => panic!("Expected successful transition to ScalingUpStatefulSet"),
        }
    }

    #[test]
    fn test_running_to_evacuating_slots() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        let result = sm.transition(
            &ClusterPhase::Running,
            ClusterEvent::ScaleDownDetected,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Running);
                assert_eq!(to, ClusterPhase::EvacuatingSlots);
            }
            _ => panic!("Expected successful transition to EvacuatingSlots"),
        }
    }

    #[test]
    fn test_scale_up_path() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(8, 8);

        // ScalingUpStatefulSet -> WaitingForNewPods
        let result = sm.transition(
            &ClusterPhase::ScalingUpStatefulSet,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::WaitingForNewPods)
            }
            _ => panic!("Expected transition to WaitingForNewPods"),
        }

        // WaitingForNewPods -> AddingNodesToCluster
        let result = sm.transition(
            &ClusterPhase::WaitingForNewPods,
            ClusterEvent::PodsRunning,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::AddingNodesToCluster)
            }
            _ => panic!("Expected transition to AddingNodesToCluster"),
        }

        // AddingNodesToCluster -> RebalancingSlots
        let result = sm.transition(
            &ClusterPhase::AddingNodesToCluster,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => assert_eq!(to, ClusterPhase::RebalancingSlots),
            _ => panic!("Expected transition to RebalancingSlots"),
        }

        // RebalancingSlots -> ConfiguringNewReplicas
        let result = sm.transition(
            &ClusterPhase::RebalancingSlots,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::ConfiguringNewReplicas)
            }
            _ => panic!("Expected transition to ConfiguringNewReplicas"),
        }

        // ConfiguringNewReplicas -> VerifyingClusterHealth
        let result = sm.transition(
            &ClusterPhase::ConfiguringNewReplicas,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth)
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    #[test]
    fn test_scale_down_path() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        // EvacuatingSlots -> RemovingNodesFromCluster
        let result = sm.transition(
            &ClusterPhase::EvacuatingSlots,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::RemovingNodesFromCluster)
            }
            _ => panic!("Expected transition to RemovingNodesFromCluster"),
        }

        // RemovingNodesFromCluster -> ScalingDownStatefulSet
        let result = sm.transition(
            &ClusterPhase::RemovingNodesFromCluster,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::ScalingDownStatefulSet)
            }
            _ => panic!("Expected transition to ScalingDownStatefulSet"),
        }

        // ScalingDownStatefulSet -> VerifyingClusterHealth
        let result = sm.transition(
            &ClusterPhase::ScalingDownStatefulSet,
            ClusterEvent::PhaseComplete,
            &ctx,
        );
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth)
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    #[test]
    fn test_invalid_transition() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        // Running -> Creating is not a valid transition
        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ResourcesApplied, &ctx);
        assert!(matches!(result, TransitionResult::InvalidTransition { .. }));
    }

    #[test]
    fn test_deleting_is_terminal() {
        let sm = ClusterStateMachine::new();

        // No valid events should transition out of Deleting
        let valid_events = sm.valid_events(&ClusterPhase::Deleting);
        assert!(valid_events.is_empty());
    }

    #[test]
    fn test_deletion_from_any_state() {
        let sm = ClusterStateMachine::new();

        let states = vec![
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::WaitingForPods,
            ClusterPhase::InitializingCluster,
            ClusterPhase::AssigningSlots,
            ClusterPhase::ConfiguringReplicas,
            ClusterPhase::Running,
            ClusterPhase::ScalingUpStatefulSet,
            ClusterPhase::WaitingForNewPods,
            ClusterPhase::AddingNodesToCluster,
            ClusterPhase::RebalancingSlots,
            ClusterPhase::ConfiguringNewReplicas,
            ClusterPhase::EvacuatingSlots,
            ClusterPhase::RemovingNodesFromCluster,
            ClusterPhase::ScalingDownStatefulSet,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Degraded,
            ClusterPhase::Failed,
        ];

        for state in states {
            assert!(
                sm.can_transition(&state, &ClusterEvent::DeletionRequested),
                "Should be able to transition from {:?} to Deleting",
                state
            );
        }
    }

    #[test]
    fn test_determine_event_deletion_priority() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&ClusterPhase::Running, &ctx, true);
        assert_eq!(event, ClusterEvent::DeletionRequested);
    }

    #[test]
    fn test_determine_event_pods_running() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::WaitingForPods, &ctx, false);
        assert_eq!(event, ClusterEvent::PodsRunning);
    }

    #[test]
    fn test_determine_event_degraded() {
        let ctx = TransitionContext::new(2, 3);
        let event = determine_event(&ClusterPhase::Running, &ctx, false);
        assert_eq!(event, ClusterEvent::ReplicasDegraded);
    }

    #[test]
    fn test_event_display() {
        assert_eq!(format!("{}", ClusterEvent::PodsRunning), "PodsRunning");
        assert_eq!(format!("{}", ClusterEvent::PhaseComplete), "PhaseComplete");
        assert_eq!(
            format!("{}", ClusterEvent::ScaleUpDetected),
            "ScaleUpDetected"
        );
        assert_eq!(
            format!("{}", ClusterEvent::ScaleDownDetected),
            "ScaleDownDetected"
        );
        assert_eq!(
            format!("{}", ClusterEvent::ClusterHealthy),
            "ClusterHealthy"
        );
    }
}
