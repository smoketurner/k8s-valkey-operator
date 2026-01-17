//! Formal finite state machine for ValkeyCluster lifecycle management.
//!
//! This module implements a proper FSM pattern with explicit state transitions,
//! guards, and actions. It ensures that only valid state transitions occur and
//! provides a clear audit trail of resource lifecycle events.

use std::fmt;

use crate::crd::ClusterPhase;

/// Events that trigger state transitions in the resource lifecycle
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClusterEvent {
    /// Kubernetes resources have been applied to the cluster
    ResourcesApplied,
    /// All desired replicas are ready
    AllReplicasReady,
    /// All pods are running (but may not be ready yet)
    PodsRunning,
    /// Some replicas are ready but not all (resource is degraded)
    ReplicasDegraded,
    /// Resource spec has changed and requires reconciliation
    SpecChanged,
    /// An error occurred during reconciliation
    ReconcileError,
    /// Deletion timestamp has been set on the resource
    DeletionRequested,
    /// Recovery from failed state has been initiated
    RecoveryInitiated,
    /// Resource has fully recovered from degraded state
    FullyRecovered,
    // === New events for granular scaling phases ===
    /// Changes have been detected and recorded in pending_changes
    ChangesDetected,
    /// No changes needed, cluster is already at desired state
    NoChangesNeeded,
    /// StatefulSet has been updated with new replica count
    StatefulSetScaled,
    /// New nodes have been added to cluster via CLUSTER MEET
    NodesAdded,
    /// Hash slots have been migrated for rebalancing
    SlotsMigrated,
    /// Nodes have been removed from cluster via CLUSTER FORGET
    NodesRemoved,
    /// Cluster health verification passed
    ClusterHealthy,
}

impl fmt::Display for ClusterEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterEvent::ResourcesApplied => write!(f, "ResourcesApplied"),
            ClusterEvent::AllReplicasReady => write!(f, "AllReplicasReady"),
            ClusterEvent::PodsRunning => write!(f, "PodsRunning"),
            ClusterEvent::ReplicasDegraded => write!(f, "ReplicasDegraded"),
            ClusterEvent::SpecChanged => write!(f, "SpecChanged"),
            ClusterEvent::ReconcileError => write!(f, "ReconcileError"),
            ClusterEvent::DeletionRequested => write!(f, "DeletionRequested"),
            ClusterEvent::RecoveryInitiated => write!(f, "RecoveryInitiated"),
            ClusterEvent::FullyRecovered => write!(f, "FullyRecovered"),
            ClusterEvent::ChangesDetected => write!(f, "ChangesDetected"),
            ClusterEvent::NoChangesNeeded => write!(f, "NoChangesNeeded"),
            ClusterEvent::StatefulSetScaled => write!(f, "StatefulSetScaled"),
            ClusterEvent::NodesAdded => write!(f, "NodesAdded"),
            ClusterEvent::SlotsMigrated => write!(f, "SlotsMigrated"),
            ClusterEvent::NodesRemoved => write!(f, "NodesRemoved"),
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
                // === Pending state transitions ===
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
                // === Creating state transitions ===
                // Creating -> Initializing when all pods are running
                Transition::new(
                    ClusterPhase::Creating,
                    ClusterPhase::Initializing,
                    ClusterEvent::PodsRunning,
                    "All pods running, starting cluster initialization",
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
                // === Initializing state transitions ===
                Transition::new(
                    ClusterPhase::Initializing,
                    ClusterPhase::AssigningSlots,
                    ClusterEvent::ResourcesApplied,
                    "Cluster nodes connected via CLUSTER MEET",
                ),
                Transition::new(
                    ClusterPhase::Initializing,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during cluster initialization",
                ),
                Transition::new(
                    ClusterPhase::Initializing,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during initialization",
                ),
                // === AssigningSlots state transitions ===
                Transition::new(
                    ClusterPhase::AssigningSlots,
                    ClusterPhase::Running,
                    ClusterEvent::ClusterHealthy,
                    "Slots assigned and cluster healthy",
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
                // === Running state transitions ===
                Transition::new(
                    ClusterPhase::Running,
                    ClusterPhase::DetectingChanges,
                    ClusterEvent::SpecChanged,
                    "Spec changed, detecting required changes",
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
                // === DetectingChanges state transitions ===
                Transition::new(
                    ClusterPhase::DetectingChanges,
                    ClusterPhase::ScalingStatefulSet,
                    ClusterEvent::ChangesDetected,
                    "Scale-up detected, updating StatefulSet",
                ),
                Transition::new(
                    ClusterPhase::DetectingChanges,
                    ClusterPhase::MigratingSlots,
                    ClusterEvent::SlotsMigrated,
                    "Scale-down detected, migrating slots first",
                ),
                Transition::new(
                    ClusterPhase::DetectingChanges,
                    ClusterPhase::Running,
                    ClusterEvent::NoChangesNeeded,
                    "No changes needed",
                ),
                Transition::new(
                    ClusterPhase::DetectingChanges,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error detecting changes",
                ),
                Transition::new(
                    ClusterPhase::DetectingChanges,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === ScalingStatefulSet state transitions ===
                Transition::new(
                    ClusterPhase::ScalingStatefulSet,
                    ClusterPhase::AddingNodes,
                    ClusterEvent::PodsRunning,
                    "New pods running, adding to cluster",
                ),
                Transition::new(
                    ClusterPhase::ScalingStatefulSet,
                    ClusterPhase::VerifyingCluster,
                    ClusterEvent::StatefulSetScaled,
                    "StatefulSet scaled down, verifying cluster",
                ),
                Transition::new(
                    ClusterPhase::ScalingStatefulSet,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error scaling StatefulSet",
                ),
                Transition::new(
                    ClusterPhase::ScalingStatefulSet,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === AddingNodes state transitions ===
                Transition::new(
                    ClusterPhase::AddingNodes,
                    ClusterPhase::MigratingSlots,
                    ClusterEvent::NodesAdded,
                    "Nodes added, migrating slots for rebalancing",
                ),
                Transition::new(
                    ClusterPhase::AddingNodes,
                    ClusterPhase::VerifyingCluster,
                    ClusterEvent::ClusterHealthy,
                    "No slot migration needed, verifying cluster",
                ),
                Transition::new(
                    ClusterPhase::AddingNodes,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error adding nodes to cluster",
                ),
                Transition::new(
                    ClusterPhase::AddingNodes,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === MigratingSlots state transitions ===
                Transition::new(
                    ClusterPhase::MigratingSlots,
                    ClusterPhase::RemovingNodes,
                    ClusterEvent::SlotsMigrated,
                    "Slots migrated, removing old nodes",
                ),
                Transition::new(
                    ClusterPhase::MigratingSlots,
                    ClusterPhase::VerifyingCluster,
                    ClusterEvent::ClusterHealthy,
                    "Slots migrated for scale-up, verifying cluster",
                ),
                Transition::new(
                    ClusterPhase::MigratingSlots,
                    ClusterPhase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Cluster degraded during migration",
                ),
                Transition::new(
                    ClusterPhase::MigratingSlots,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during slot migration",
                ),
                Transition::new(
                    ClusterPhase::MigratingSlots,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === RemovingNodes state transitions ===
                Transition::new(
                    ClusterPhase::RemovingNodes,
                    ClusterPhase::ScalingStatefulSet,
                    ClusterEvent::NodesRemoved,
                    "Nodes removed from cluster, scaling down StatefulSet",
                ),
                Transition::new(
                    ClusterPhase::RemovingNodes,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error removing nodes from cluster",
                ),
                Transition::new(
                    ClusterPhase::RemovingNodes,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === VerifyingCluster state transitions ===
                Transition::new(
                    ClusterPhase::VerifyingCluster,
                    ClusterPhase::Running,
                    ClusterEvent::ClusterHealthy,
                    "Cluster verified healthy",
                ),
                Transition::new(
                    ClusterPhase::VerifyingCluster,
                    ClusterPhase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Cluster verification found degraded state",
                ),
                Transition::new(
                    ClusterPhase::VerifyingCluster,
                    ClusterPhase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during cluster verification",
                ),
                Transition::new(
                    ClusterPhase::VerifyingCluster,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === Degraded state transitions ===
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::Running,
                    ClusterEvent::FullyRecovered,
                    "Resource fully recovered from degraded state",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::Running,
                    ClusterEvent::AllReplicasReady,
                    "All replicas recovered",
                ),
                Transition::new(
                    ClusterPhase::Degraded,
                    ClusterPhase::DetectingChanges,
                    ClusterEvent::SpecChanged,
                    "Spec changed while degraded",
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
                // === Failed state transitions ===
                Transition::new(
                    ClusterPhase::Failed,
                    ClusterPhase::Pending,
                    ClusterEvent::RecoveryInitiated,
                    "Recovery initiated from failed state",
                ),
                Transition::new(
                    ClusterPhase::Failed,
                    ClusterPhase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while failed",
                ),
                // === Deleting state transitions (terminal) ===
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
            // Guard: AllReplicasReady requires all replicas to be ready
            (_, ClusterPhase::Running, ClusterEvent::AllReplicasReady)
            | (
                ClusterPhase::Creating,
                ClusterPhase::Initializing,
                ClusterEvent::AllReplicasReady,
            ) => {
                if !ctx.all_replicas_ready() {
                    Some(format!(
                        "Not all replicas ready: {}/{}",
                        ctx.ready_replicas, ctx.desired_replicas
                    ))
                } else {
                    None
                }
            }
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

    // Check for spec change
    if ctx.spec_changed
        && matches!(
            current_phase,
            ClusterPhase::Running | ClusterPhase::Degraded
        )
    {
        return ClusterEvent::SpecChanged;
    }

    // Determine event based on replica status and current phase
    match current_phase {
        ClusterPhase::Pending => ClusterEvent::ResourcesApplied,
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
        ClusterPhase::Creating => {
            // During initial creation, wait for pods to be running
            if ctx.all_replicas_ready() {
                ClusterEvent::PodsRunning
            } else {
                // Stay in Creating while waiting for pods
                ClusterEvent::ResourcesApplied
            }
        }
        ClusterPhase::Initializing | ClusterPhase::AssigningSlots => {
            // CLUSTER MEET and slot assignment phases
            ClusterEvent::ResourcesApplied
        }
        // Granular scaling phases - these are driven by phase handlers, not this function
        ClusterPhase::DetectingChanges
        | ClusterPhase::ScalingStatefulSet
        | ClusterPhase::AddingNodes
        | ClusterPhase::MigratingSlots
        | ClusterPhase::RemovingNodes
        | ClusterPhase::VerifyingCluster => {
            // These phases determine their own transitions based on handler results
            ClusterEvent::ResourcesApplied
        }
        ClusterPhase::Running => {
            if ctx.is_degraded() {
                ClusterEvent::ReplicasDegraded
            } else {
                ClusterEvent::AllReplicasReady
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
    fn test_creating_to_initializing() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        // Should succeed with PodsRunning event
        let result = sm.transition(&ClusterPhase::Creating, ClusterEvent::PodsRunning, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Creating);
                assert_eq!(to, ClusterPhase::Initializing);
            }
            _ => panic!("Expected successful transition to Initializing"),
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
    fn test_running_to_detecting_changes() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::SpecChanged, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Running);
                assert_eq!(to, ClusterPhase::DetectingChanges);
            }
            _ => panic!("Expected successful transition to DetectingChanges"),
        }
    }

    #[test]
    fn test_detecting_changes_to_scaling() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        // Scale-up path
        let result =
            sm.transition(&ClusterPhase::DetectingChanges, ClusterEvent::ChangesDetected, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::DetectingChanges);
                assert_eq!(to, ClusterPhase::ScalingStatefulSet);
            }
            _ => panic!("Expected successful transition to ScalingStatefulSet"),
        }

        // No changes needed
        let result =
            sm.transition(&ClusterPhase::DetectingChanges, ClusterEvent::NoChangesNeeded, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::DetectingChanges);
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition back to Running"),
        }
    }

    #[test]
    fn test_scaling_statefulset_transitions() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        // Scale-up: pods running -> AddingNodes
        let result =
            sm.transition(&ClusterPhase::ScalingStatefulSet, ClusterEvent::PodsRunning, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::ScalingStatefulSet);
                assert_eq!(to, ClusterPhase::AddingNodes);
            }
            _ => panic!("Expected successful transition to AddingNodes"),
        }

        // Scale-down: StatefulSet scaled -> VerifyingCluster
        let result = sm.transition(
            &ClusterPhase::ScalingStatefulSet,
            ClusterEvent::StatefulSetScaled,
            &ctx,
        );
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::ScalingStatefulSet);
                assert_eq!(to, ClusterPhase::VerifyingCluster);
            }
            _ => panic!("Expected successful transition to VerifyingCluster"),
        }
    }

    #[test]
    fn test_adding_nodes_transitions() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        // Nodes added -> MigratingSlots
        let result = sm.transition(&ClusterPhase::AddingNodes, ClusterEvent::NodesAdded, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::AddingNodes);
                assert_eq!(to, ClusterPhase::MigratingSlots);
            }
            _ => panic!("Expected successful transition to MigratingSlots"),
        }
    }

    #[test]
    fn test_migrating_slots_transitions() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        // Scale-down: slots migrated -> RemovingNodes
        let result =
            sm.transition(&ClusterPhase::MigratingSlots, ClusterEvent::SlotsMigrated, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::MigratingSlots);
                assert_eq!(to, ClusterPhase::RemovingNodes);
            }
            _ => panic!("Expected successful transition to RemovingNodes"),
        }

        // Scale-up: cluster healthy -> VerifyingCluster
        let result =
            sm.transition(&ClusterPhase::MigratingSlots, ClusterEvent::ClusterHealthy, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::MigratingSlots);
                assert_eq!(to, ClusterPhase::VerifyingCluster);
            }
            _ => panic!("Expected successful transition to VerifyingCluster"),
        }
    }

    #[test]
    fn test_removing_nodes_transitions() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        // Nodes removed -> ScalingStatefulSet
        let result =
            sm.transition(&ClusterPhase::RemovingNodes, ClusterEvent::NodesRemoved, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::RemovingNodes);
                assert_eq!(to, ClusterPhase::ScalingStatefulSet);
            }
            _ => panic!("Expected successful transition to ScalingStatefulSet"),
        }
    }

    #[test]
    fn test_verifying_cluster_transitions() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(6, 6);

        // Cluster healthy -> Running
        let result =
            sm.transition(&ClusterPhase::VerifyingCluster, ClusterEvent::ClusterHealthy, &ctx);
        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::VerifyingCluster);
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected successful transition to Running"),
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
            ClusterPhase::Initializing,
            ClusterPhase::AssigningSlots,
            ClusterPhase::Running,
            ClusterPhase::DetectingChanges,
            ClusterPhase::ScalingStatefulSet,
            ClusterPhase::AddingNodes,
            ClusterPhase::MigratingSlots,
            ClusterPhase::RemovingNodes,
            ClusterPhase::VerifyingCluster,
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
    fn test_determine_event_spec_changed() {
        let ctx = TransitionContext::new(3, 3).with_spec_changed(true);
        let event = determine_event(&ClusterPhase::Running, &ctx, false);
        assert_eq!(event, ClusterEvent::SpecChanged);
    }

    #[test]
    fn test_determine_event_pods_running() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&ClusterPhase::Creating, &ctx, false);
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
        assert_eq!(format!("{}", ClusterEvent::ChangesDetected), "ChangesDetected");
        assert_eq!(format!("{}", ClusterEvent::NodesAdded), "NodesAdded");
        assert_eq!(format!("{}", ClusterEvent::SlotsMigrated), "SlotsMigrated");
        assert_eq!(format!("{}", ClusterEvent::NodesRemoved), "NodesRemoved");
        assert_eq!(format!("{}", ClusterEvent::ClusterHealthy), "ClusterHealthy");
    }
}
