//! Formal finite state machine for ValkeyCluster lifecycle management.
//!
//! This module implements a proper FSM pattern with explicit state transitions,
//! guards, and actions. It ensures that only valid state transitions occur and
//! provides a clear audit trail of resource lifecycle events.

use std::fmt;

use crate::crd::Phase;

/// Events that trigger state transitions in the resource lifecycle
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ClusterEvent {
    /// Kubernetes resources have been applied to the cluster
    ResourcesApplied,
    /// All desired replicas are ready
    AllReplicasReady,
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
}

impl fmt::Display for ClusterEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterEvent::ResourcesApplied => write!(f, "ResourcesApplied"),
            ClusterEvent::AllReplicasReady => write!(f, "AllReplicasReady"),
            ClusterEvent::ReplicasDegraded => write!(f, "ReplicasDegraded"),
            ClusterEvent::SpecChanged => write!(f, "SpecChanged"),
            ClusterEvent::ReconcileError => write!(f, "ReconcileError"),
            ClusterEvent::DeletionRequested => write!(f, "DeletionRequested"),
            ClusterEvent::RecoveryInitiated => write!(f, "RecoveryInitiated"),
            ClusterEvent::FullyRecovered => write!(f, "FullyRecovered"),
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
    pub from: Phase,
    /// Target state
    pub to: Phase,
    /// Event that triggers this transition
    pub event: ClusterEvent,
    /// Human-readable description of this transition
    pub description: &'static str,
}

impl Transition {
    /// Create a new transition
    const fn new(from: Phase, to: Phase, event: ClusterEvent, description: &'static str) -> Self {
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
        from: Phase,
        to: Phase,
        event: ClusterEvent,
        description: &'static str,
    },
    /// Transition was not valid for current state
    InvalidTransition {
        current: Phase,
        event: ClusterEvent,
    },
    /// Guard condition prevented the transition
    GuardFailed {
        from: Phase,
        to: Phase,
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
                    Phase::Pending,
                    Phase::Creating,
                    ClusterEvent::ResourcesApplied,
                    "Starting resource creation",
                ),
                Transition::new(
                    Phase::Pending,
                    Phase::Failed,
                    ClusterEvent::ReconcileError,
                    "Validation failed during pending",
                ),
                Transition::new(
                    Phase::Pending,
                    Phase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested before creation",
                ),
                // === Creating state transitions ===
                Transition::new(
                    Phase::Creating,
                    Phase::Running,
                    ClusterEvent::AllReplicasReady,
                    "All replicas are ready, resource is now running",
                ),
                Transition::new(
                    Phase::Creating,
                    Phase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Some replicas ready but resource is degraded",
                ),
                Transition::new(
                    Phase::Creating,
                    Phase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during resource creation",
                ),
                Transition::new(
                    Phase::Creating,
                    Phase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during creation",
                ),
                // === Running state transitions ===
                Transition::new(
                    Phase::Running,
                    Phase::Updating,
                    ClusterEvent::SpecChanged,
                    "Resource spec changed, starting update",
                ),
                Transition::new(
                    Phase::Running,
                    Phase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Resource health degraded",
                ),
                Transition::new(
                    Phase::Running,
                    Phase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error while running",
                ),
                Transition::new(
                    Phase::Running,
                    Phase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested",
                ),
                // === Updating state transitions ===
                Transition::new(
                    Phase::Updating,
                    Phase::Running,
                    ClusterEvent::AllReplicasReady,
                    "Update completed, resource is running",
                ),
                Transition::new(
                    Phase::Updating,
                    Phase::Degraded,
                    ClusterEvent::ReplicasDegraded,
                    "Update in progress but resource degraded",
                ),
                Transition::new(
                    Phase::Updating,
                    Phase::Failed,
                    ClusterEvent::ReconcileError,
                    "Error during update",
                ),
                Transition::new(
                    Phase::Updating,
                    Phase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested during update",
                ),
                // === Degraded state transitions ===
                Transition::new(
                    Phase::Degraded,
                    Phase::Running,
                    ClusterEvent::FullyRecovered,
                    "Resource fully recovered from degraded state",
                ),
                Transition::new(
                    Phase::Degraded,
                    Phase::Running,
                    ClusterEvent::AllReplicasReady,
                    "All replicas recovered",
                ),
                Transition::new(
                    Phase::Degraded,
                    Phase::Updating,
                    ClusterEvent::SpecChanged,
                    "Spec changed while degraded",
                ),
                Transition::new(
                    Phase::Degraded,
                    Phase::Failed,
                    ClusterEvent::ReconcileError,
                    "Degraded resource encountered error",
                ),
                Transition::new(
                    Phase::Degraded,
                    Phase::Deleting,
                    ClusterEvent::DeletionRequested,
                    "Resource deletion requested while degraded",
                ),
                // === Failed state transitions ===
                Transition::new(
                    Phase::Failed,
                    Phase::Pending,
                    ClusterEvent::RecoveryInitiated,
                    "Recovery initiated from failed state",
                ),
                Transition::new(
                    Phase::Failed,
                    Phase::Deleting,
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
        current: &Phase,
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
    pub fn can_transition(&self, from: &Phase, event: &ClusterEvent) -> bool {
        self.transitions
            .iter()
            .any(|t| t.from == *from && t.event == *event)
    }

    /// Get all valid events for a given state
    pub fn valid_events(&self, state: &Phase) -> Vec<&ClusterEvent> {
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
            (_, Phase::Running, ClusterEvent::AllReplicasReady) => {
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
            (_, Phase::Degraded, ClusterEvent::ReplicasDegraded) => {
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
            (Phase::Degraded, Phase::Running, ClusterEvent::FullyRecovered) => {
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
    current_phase: &Phase,
    ctx: &TransitionContext,
    has_deletion_timestamp: bool,
) -> ClusterEvent {
    // Deletion always takes priority
    if has_deletion_timestamp {
        return ClusterEvent::DeletionRequested;
    }

    // Check for spec change
    if ctx.spec_changed && matches!(current_phase, Phase::Running | Phase::Degraded) {
        return ClusterEvent::SpecChanged;
    }

    // Determine event based on replica status
    if ctx.all_replicas_ready() {
        match current_phase {
            Phase::Failed => ClusterEvent::RecoveryInitiated,
            Phase::Degraded => ClusterEvent::FullyRecovered,
            _ => ClusterEvent::AllReplicasReady,
        }
    } else if ctx.is_degraded() {
        ClusterEvent::ReplicasDegraded
    } else if *current_phase == Phase::Pending {
        ClusterEvent::ResourcesApplied
    } else if ctx.no_replicas_ready() && matches!(current_phase, Phase::Creating | Phase::Updating)
    {
        // During initial bootstrap or updates, having 0 ready replicas
        // is a normal transitional state, not an error. Return ResourcesApplied
        // which has no valid transition from these states (InvalidTransition),
        // keeping us in the current state while waiting for pods to become ready.
        ClusterEvent::ResourcesApplied
    } else {
        // Default: error occurred
        ClusterEvent::ReconcileError
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic
)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_to_creating() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(0, 3);

        let result = sm.transition(&Phase::Pending, ClusterEvent::ResourcesApplied, &ctx);

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, Phase::Pending);
                assert_eq!(to, Phase::Creating);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_creating_to_running_guard() {
        let sm = ClusterStateMachine::new();

        // Should fail with 0/3 replicas ready
        let ctx = TransitionContext::new(0, 3);
        let result = sm.transition(&Phase::Creating, ClusterEvent::AllReplicasReady, &ctx);
        assert!(matches!(result, TransitionResult::GuardFailed { .. }));

        // Should succeed with 3/3 replicas ready
        let ctx = TransitionContext::new(3, 3);
        let result = sm.transition(&Phase::Creating, ClusterEvent::AllReplicasReady, &ctx);
        assert!(matches!(result, TransitionResult::Success { .. }));
    }

    #[test]
    fn test_running_to_degraded_guard() {
        let sm = ClusterStateMachine::new();

        // Should succeed with 2/3 replicas (degraded)
        let ctx = TransitionContext::new(2, 3);
        let result = sm.transition(&Phase::Running, ClusterEvent::ReplicasDegraded, &ctx);
        assert!(matches!(result, TransitionResult::Success { .. }));

        // Should fail with 3/3 replicas (not degraded)
        let ctx = TransitionContext::new(3, 3);
        let result = sm.transition(&Phase::Running, ClusterEvent::ReplicasDegraded, &ctx);
        assert!(matches!(result, TransitionResult::GuardFailed { .. }));
    }

    #[test]
    fn test_invalid_transition() {
        let sm = ClusterStateMachine::new();
        let ctx = TransitionContext::new(3, 3);

        // Running -> Creating is not a valid transition
        let result = sm.transition(&Phase::Running, ClusterEvent::ResourcesApplied, &ctx);
        assert!(matches!(result, TransitionResult::InvalidTransition { .. }));
    }

    #[test]
    fn test_deleting_is_terminal() {
        let sm = ClusterStateMachine::new();

        // No valid events should transition out of Deleting
        let valid_events = sm.valid_events(&Phase::Deleting);
        assert!(valid_events.is_empty());
    }

    #[test]
    fn test_deletion_from_any_state() {
        let sm = ClusterStateMachine::new();

        let states = vec![
            Phase::Pending,
            Phase::Creating,
            Phase::Running,
            Phase::Updating,
            Phase::Degraded,
            Phase::Failed,
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
        let event = determine_event(&Phase::Running, &ctx, true);
        assert_eq!(event, ClusterEvent::DeletionRequested);
    }

    #[test]
    fn test_determine_event_spec_changed() {
        let ctx = TransitionContext::new(3, 3).with_spec_changed(true);
        let event = determine_event(&Phase::Running, &ctx, false);
        assert_eq!(event, ClusterEvent::SpecChanged);
    }

    #[test]
    fn test_determine_event_all_replicas_ready() {
        let ctx = TransitionContext::new(3, 3);
        let event = determine_event(&Phase::Creating, &ctx, false);
        assert_eq!(event, ClusterEvent::AllReplicasReady);
    }

    #[test]
    fn test_determine_event_degraded() {
        let ctx = TransitionContext::new(2, 3);
        let event = determine_event(&Phase::Running, &ctx, false);
        assert_eq!(event, ClusterEvent::ReplicasDegraded);
    }
}
