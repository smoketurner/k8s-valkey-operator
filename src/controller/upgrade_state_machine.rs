//! State machine for ValkeyUpgrade phase transitions.
//!
//! This module implements a formal finite state machine for managing Valkey
//! cluster rolling upgrades with explicit state transitions, guards, and
//! event-driven logic.

use crate::crd::{ShardUpgradeState, UpgradePhase};

// ============================================================================
// Upgrade Events
// ============================================================================

/// Events that can trigger upgrade phase transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeEvent {
    /// Upgrade resource created, ready to start.
    Created,

    /// Pre-upgrade health checks passed.
    PreChecksPassed,

    /// Pre-upgrade health checks failed.
    PreChecksFailed,

    /// Shard upgrade completed successfully.
    ShardCompleted,

    /// All shards upgraded successfully.
    AllShardsCompleted,

    /// Shard upgrade failed.
    ShardFailed,

    /// Rollback requested (automatic on failure).
    RollbackRequested,

    /// Rollback completed successfully.
    RollbackCompleted,

    /// Rollback failed.
    RollbackFailed,

    /// Timeout waiting for operation.
    Timeout,

    /// Cluster health check failed.
    ClusterUnhealthy,
}

impl std::fmt::Display for UpgradeEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpgradeEvent::Created => write!(f, "Created"),
            UpgradeEvent::PreChecksPassed => write!(f, "PreChecksPassed"),
            UpgradeEvent::PreChecksFailed => write!(f, "PreChecksFailed"),
            UpgradeEvent::ShardCompleted => write!(f, "ShardCompleted"),
            UpgradeEvent::AllShardsCompleted => write!(f, "AllShardsCompleted"),
            UpgradeEvent::ShardFailed => write!(f, "ShardFailed"),
            UpgradeEvent::RollbackRequested => write!(f, "RollbackRequested"),
            UpgradeEvent::RollbackCompleted => write!(f, "RollbackCompleted"),
            UpgradeEvent::RollbackFailed => write!(f, "RollbackFailed"),
            UpgradeEvent::Timeout => write!(f, "Timeout"),
            UpgradeEvent::ClusterUnhealthy => write!(f, "ClusterUnhealthy"),
        }
    }
}

// ============================================================================
// Transition Context
// ============================================================================

/// Context data used to evaluate transition guards.
#[derive(Debug, Clone, Default)]
pub struct UpgradeTransitionContext {
    /// Whether the cluster is healthy.
    pub cluster_healthy: bool,

    /// Whether all slots are assigned and stable.
    pub slots_stable: bool,

    /// Total number of shards.
    pub total_shards: i32,

    /// Number of shards upgraded successfully.
    pub upgraded_shards: i32,

    /// Number of shards that failed.
    pub failed_shards: i32,

    /// Current shard being upgraded (-1 if none).
    pub current_shard: i32,

    /// Whether a timeout has occurred.
    pub timed_out: bool,

    /// Whether rollback is in progress.
    pub rollback_in_progress: bool,

    /// Original version (for rollback).
    pub original_version: Option<String>,
}

impl UpgradeTransitionContext {
    /// Check if all shards have been processed.
    pub fn all_shards_processed(&self) -> bool {
        self.upgraded_shards + self.failed_shards >= self.total_shards
    }

    /// Check if upgrade can proceed (no failures).
    pub fn can_proceed(&self) -> bool {
        self.failed_shards == 0 && self.cluster_healthy
    }

    /// Check if rollback is possible.
    pub fn can_rollback(&self) -> bool {
        self.original_version.is_some()
    }
}

// ============================================================================
// Upgrade Transition
// ============================================================================

/// Represents a single state transition.
#[derive(Debug, Clone)]
pub struct UpgradeTransition {
    /// Source phase.
    pub from: UpgradePhase,

    /// Target phase.
    pub to: UpgradePhase,

    /// Event that triggers this transition.
    pub event: UpgradeEvent,

    /// Human-readable description.
    pub description: &'static str,
}

// ============================================================================
// Upgrade State Machine
// ============================================================================

/// State machine for validating ValkeyUpgrade phase transitions.
#[derive(Debug, Clone, Default)]
pub struct UpgradeStateMachine {
    /// All valid transitions.
    transitions: Vec<UpgradeTransition>,
}

impl UpgradeStateMachine {
    /// Create a new state machine with all valid transitions.
    pub fn new() -> Self {
        let transitions = vec![
            // From Pending
            UpgradeTransition {
                from: UpgradePhase::Pending,
                to: UpgradePhase::PreChecks,
                event: UpgradeEvent::Created,
                description: "Start pre-upgrade health checks",
            },
            UpgradeTransition {
                from: UpgradePhase::Pending,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::ClusterUnhealthy,
                description: "Cluster unhealthy, cannot start upgrade",
            },
            // From PreChecks
            UpgradeTransition {
                from: UpgradePhase::PreChecks,
                to: UpgradePhase::InProgress,
                event: UpgradeEvent::PreChecksPassed,
                description: "Pre-checks passed, begin shard upgrades",
            },
            UpgradeTransition {
                from: UpgradePhase::PreChecks,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::PreChecksFailed,
                description: "Pre-checks failed, upgrade aborted",
            },
            UpgradeTransition {
                from: UpgradePhase::PreChecks,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::Timeout,
                description: "Pre-checks timed out",
            },
            // From InProgress
            UpgradeTransition {
                from: UpgradePhase::InProgress,
                to: UpgradePhase::Completed,
                event: UpgradeEvent::AllShardsCompleted,
                description: "All shards upgraded successfully",
            },
            UpgradeTransition {
                from: UpgradePhase::InProgress,
                to: UpgradePhase::RollingBack,
                event: UpgradeEvent::ShardFailed,
                description: "Shard failed, initiating rollback",
            },
            UpgradeTransition {
                from: UpgradePhase::InProgress,
                to: UpgradePhase::RollingBack,
                event: UpgradeEvent::RollbackRequested,
                description: "Rollback requested",
            },
            UpgradeTransition {
                from: UpgradePhase::InProgress,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::Timeout,
                description: "Upgrade timed out",
            },
            UpgradeTransition {
                from: UpgradePhase::InProgress,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::ClusterUnhealthy,
                description: "Cluster became unhealthy during upgrade",
            },
            // From RollingBack
            UpgradeTransition {
                from: UpgradePhase::RollingBack,
                to: UpgradePhase::RolledBack,
                event: UpgradeEvent::RollbackCompleted,
                description: "Rollback completed successfully",
            },
            UpgradeTransition {
                from: UpgradePhase::RollingBack,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::RollbackFailed,
                description: "Rollback failed",
            },
            UpgradeTransition {
                from: UpgradePhase::RollingBack,
                to: UpgradePhase::Failed,
                event: UpgradeEvent::Timeout,
                description: "Rollback timed out",
            },
        ];

        Self { transitions }
    }

    /// Attempt to transition from one phase to another given an event.
    ///
    /// Returns the transition if valid, None otherwise.
    pub fn transition(
        &self,
        from: UpgradePhase,
        event: UpgradeEvent,
        ctx: &UpgradeTransitionContext,
    ) -> Option<UpgradeTransition> {
        // Find matching transition
        let transition = self
            .transitions
            .iter()
            .find(|t| t.from == from && t.event == event)?;

        // Evaluate guards
        if !self.evaluate_guards(transition, ctx) {
            return None;
        }

        Some(transition.clone())
    }

    /// Evaluate guards for a transition.
    fn evaluate_guards(
        &self,
        transition: &UpgradeTransition,
        ctx: &UpgradeTransitionContext,
    ) -> bool {
        match (transition.from, transition.to) {
            // Cannot start upgrade if cluster unhealthy
            (UpgradePhase::Pending, UpgradePhase::PreChecks) => ctx.cluster_healthy,

            // Cannot proceed to InProgress without healthy cluster and stable slots
            (UpgradePhase::PreChecks, UpgradePhase::InProgress) => {
                ctx.cluster_healthy && ctx.slots_stable
            }

            // Can only complete if all shards upgraded
            (UpgradePhase::InProgress, UpgradePhase::Completed) => {
                ctx.upgraded_shards >= ctx.total_shards && ctx.failed_shards == 0
            }

            // Can only rollback if we have original version
            (UpgradePhase::InProgress, UpgradePhase::RollingBack) => ctx.can_rollback(),

            // Rollback completion requires all shards processed
            (UpgradePhase::RollingBack, UpgradePhase::RolledBack) => ctx.all_shards_processed(),

            // All other transitions pass guards
            _ => true,
        }
    }

    /// Check if a phase transition is valid (without guards).
    pub fn can_transition(&self, from: UpgradePhase, to: UpgradePhase) -> bool {
        // Same phase is always valid (no-op)
        if from == to {
            return true;
        }

        self.transitions
            .iter()
            .any(|t| t.from == from && t.to == to)
    }

    /// Get the list of valid next phases from a given phase.
    pub fn valid_transitions(&self, from: UpgradePhase) -> Vec<UpgradePhase> {
        self.transitions
            .iter()
            .filter(|t| t.from == from)
            .map(|t| t.to)
            .collect()
    }

    /// Check if a phase is terminal (no further transitions possible).
    pub fn is_terminal(&self, phase: UpgradePhase) -> bool {
        phase.is_terminal()
    }

    /// Get all transitions from a phase.
    pub fn transitions_from(&self, phase: UpgradePhase) -> Vec<&UpgradeTransition> {
        self.transitions
            .iter()
            .filter(|t| t.from == phase)
            .collect()
    }
}

/// Determine the appropriate upgrade event based on context.
///
/// Event priority:
/// 1. Timeout (highest priority)
/// 2. Cluster unhealthy
/// 3. Shard failures (trigger rollback)
/// 4. Phase-specific events
pub fn determine_upgrade_event(
    phase: UpgradePhase,
    ctx: &UpgradeTransitionContext,
) -> Option<UpgradeEvent> {
    // Priority 1: Timeout
    if ctx.timed_out {
        return Some(UpgradeEvent::Timeout);
    }

    // Priority 2: Cluster unhealthy (except during rollback)
    if !ctx.cluster_healthy && phase != UpgradePhase::RollingBack {
        return Some(UpgradeEvent::ClusterUnhealthy);
    }

    // Priority 3: Shard failures trigger rollback
    if ctx.failed_shards > 0 && phase == UpgradePhase::InProgress {
        return Some(UpgradeEvent::ShardFailed);
    }

    // Phase-specific events
    match phase {
        UpgradePhase::Pending => {
            if ctx.cluster_healthy {
                Some(UpgradeEvent::Created)
            } else {
                Some(UpgradeEvent::ClusterUnhealthy)
            }
        }

        UpgradePhase::PreChecks => {
            if ctx.cluster_healthy && ctx.slots_stable {
                Some(UpgradeEvent::PreChecksPassed)
            } else {
                Some(UpgradeEvent::PreChecksFailed)
            }
        }

        UpgradePhase::InProgress => {
            if ctx.upgraded_shards >= ctx.total_shards {
                Some(UpgradeEvent::AllShardsCompleted)
            } else {
                // Continue upgrading shards
                None
            }
        }

        UpgradePhase::RollingBack => {
            if ctx.all_shards_processed() {
                Some(UpgradeEvent::RollbackCompleted)
            } else {
                // Continue rollback
                None
            }
        }

        // Terminal phases have no events
        UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack => None,
    }
}

// ============================================================================
// Shard State Machine
// ============================================================================

/// Events for shard-level state transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardEvent {
    /// Start upgrading replicas.
    StartReplicaUpgrade,

    /// All replicas upgraded.
    ReplicasUpgraded,

    /// Replication is in sync.
    ReplicationSynced,

    /// Failover initiated.
    FailoverStarted,

    /// Failover completed.
    FailoverCompleted,

    /// Old master upgraded.
    OldMasterUpgraded,

    /// Operation failed.
    Failed,

    /// Shard skipped (e.g., already at target version).
    Skipped,

    /// Timeout waiting for operation.
    Timeout,
}

impl std::fmt::Display for ShardEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardEvent::StartReplicaUpgrade => write!(f, "StartReplicaUpgrade"),
            ShardEvent::ReplicasUpgraded => write!(f, "ReplicasUpgraded"),
            ShardEvent::ReplicationSynced => write!(f, "ReplicationSynced"),
            ShardEvent::FailoverStarted => write!(f, "FailoverStarted"),
            ShardEvent::FailoverCompleted => write!(f, "FailoverCompleted"),
            ShardEvent::OldMasterUpgraded => write!(f, "OldMasterUpgraded"),
            ShardEvent::Failed => write!(f, "Failed"),
            ShardEvent::Skipped => write!(f, "Skipped"),
            ShardEvent::Timeout => write!(f, "Timeout"),
        }
    }
}

/// Context for shard state transitions.
#[derive(Debug, Clone, Default)]
pub struct ShardTransitionContext {
    /// Number of replicas for this shard.
    pub replica_count: i32,

    /// Number of replicas upgraded.
    pub replicas_upgraded: i32,

    /// Whether replication is in sync.
    pub replication_synced: bool,

    /// Whether failover completed.
    pub failover_completed: bool,

    /// Whether the shard is already at target version.
    pub already_upgraded: bool,

    /// Whether a timeout occurred.
    pub timed_out: bool,
}

/// State machine for validating shard upgrade state transitions.
#[derive(Debug, Clone, Default)]
pub struct ShardStateMachine;

impl ShardStateMachine {
    /// Create a new shard state machine.
    pub fn new() -> Self {
        Self
    }

    /// Attempt state transition based on event and context.
    pub fn transition(
        &self,
        from: ShardUpgradeState,
        event: ShardEvent,
        ctx: &ShardTransitionContext,
    ) -> Option<ShardUpgradeState> {
        // Check for timeout first
        if ctx.timed_out {
            return Some(ShardUpgradeState::Failed);
        }

        let to = match (from, event) {
            // From Pending
            (ShardUpgradeState::Pending, ShardEvent::Skipped) => ShardUpgradeState::Skipped,
            (ShardUpgradeState::Pending, ShardEvent::StartReplicaUpgrade) => {
                ShardUpgradeState::UpgradingReplicas
            }
            (ShardUpgradeState::Pending, ShardEvent::ReplicationSynced)
                if ctx.replica_count == 0 =>
            {
                // No replicas, skip to waiting for sync
                ShardUpgradeState::WaitingForSync
            }
            (ShardUpgradeState::Pending, ShardEvent::Failed) => ShardUpgradeState::Failed,

            // From UpgradingReplicas
            (ShardUpgradeState::UpgradingReplicas, ShardEvent::ReplicasUpgraded) => {
                ShardUpgradeState::WaitingForSync
            }
            (ShardUpgradeState::UpgradingReplicas, ShardEvent::Failed) => ShardUpgradeState::Failed,

            // From WaitingForSync
            (ShardUpgradeState::WaitingForSync, ShardEvent::ReplicationSynced)
                if ctx.replica_count > 0 =>
            {
                ShardUpgradeState::FailingOver
            }
            (ShardUpgradeState::WaitingForSync, ShardEvent::ReplicationSynced) => {
                // No replicas, skip failover
                ShardUpgradeState::UpgradingOldMaster
            }
            (ShardUpgradeState::WaitingForSync, ShardEvent::Failed) => ShardUpgradeState::Failed,

            // From FailingOver
            (ShardUpgradeState::FailingOver, ShardEvent::FailoverCompleted) => {
                ShardUpgradeState::UpgradingOldMaster
            }
            (ShardUpgradeState::FailingOver, ShardEvent::Failed) => ShardUpgradeState::Failed,

            // From UpgradingOldMaster
            (ShardUpgradeState::UpgradingOldMaster, ShardEvent::OldMasterUpgraded) => {
                ShardUpgradeState::Completed
            }
            (ShardUpgradeState::UpgradingOldMaster, ShardEvent::Failed) => {
                ShardUpgradeState::Failed
            }

            // Invalid transitions
            _ => return None,
        };

        Some(to)
    }

    /// Check if a shard state transition is valid.
    pub fn can_transition(&self, from: ShardUpgradeState, to: ShardUpgradeState) -> bool {
        // Same state is always valid
        if from == to {
            return true;
        }

        match (from, to) {
            // From Pending
            (ShardUpgradeState::Pending, ShardUpgradeState::UpgradingReplicas) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::WaitingForSync) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::UpgradingOldMaster) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::Failed) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::Skipped) => true,

            // From UpgradingReplicas
            (ShardUpgradeState::UpgradingReplicas, ShardUpgradeState::WaitingForSync) => true,
            (ShardUpgradeState::UpgradingReplicas, ShardUpgradeState::Failed) => true,

            // From WaitingForSync
            (ShardUpgradeState::WaitingForSync, ShardUpgradeState::FailingOver) => true,
            (ShardUpgradeState::WaitingForSync, ShardUpgradeState::UpgradingOldMaster) => true,
            (ShardUpgradeState::WaitingForSync, ShardUpgradeState::Failed) => true,

            // From FailingOver
            (ShardUpgradeState::FailingOver, ShardUpgradeState::UpgradingOldMaster) => true,
            (ShardUpgradeState::FailingOver, ShardUpgradeState::Failed) => true,

            // From UpgradingOldMaster
            (ShardUpgradeState::UpgradingOldMaster, ShardUpgradeState::Completed) => true,
            (ShardUpgradeState::UpgradingOldMaster, ShardUpgradeState::Failed) => true,

            // Terminal states
            (ShardUpgradeState::Completed, _) => false,
            (ShardUpgradeState::Failed, _) => false,
            (ShardUpgradeState::Skipped, _) => false,

            // All other transitions are invalid
            _ => false,
        }
    }

    /// Check if a shard state is terminal.
    pub fn is_terminal(&self, state: ShardUpgradeState) -> bool {
        matches!(
            state,
            ShardUpgradeState::Completed | ShardUpgradeState::Failed | ShardUpgradeState::Skipped
        )
    }

    /// Get the expected next states from a given state.
    pub fn next_states(&self, from: ShardUpgradeState) -> Vec<ShardUpgradeState> {
        match from {
            ShardUpgradeState::Pending => vec![
                ShardUpgradeState::UpgradingReplicas,
                ShardUpgradeState::WaitingForSync,
                ShardUpgradeState::Skipped,
            ],
            ShardUpgradeState::UpgradingReplicas => {
                vec![ShardUpgradeState::WaitingForSync, ShardUpgradeState::Failed]
            }
            ShardUpgradeState::WaitingForSync => vec![
                ShardUpgradeState::FailingOver,
                ShardUpgradeState::UpgradingOldMaster,
                ShardUpgradeState::Failed,
            ],
            ShardUpgradeState::FailingOver => vec![
                ShardUpgradeState::UpgradingOldMaster,
                ShardUpgradeState::Failed,
            ],
            ShardUpgradeState::UpgradingOldMaster => {
                vec![ShardUpgradeState::Completed, ShardUpgradeState::Failed]
            }
            // Terminal states
            ShardUpgradeState::Completed
            | ShardUpgradeState::Failed
            | ShardUpgradeState::Skipped => {
                vec![]
            }
        }
    }
}

/// Determine the appropriate shard event based on context.
pub fn determine_shard_event(
    state: ShardUpgradeState,
    ctx: &ShardTransitionContext,
) -> Option<ShardEvent> {
    // Priority 1: Timeout
    if ctx.timed_out {
        return Some(ShardEvent::Timeout);
    }

    // Priority 2: Already upgraded
    if ctx.already_upgraded && state == ShardUpgradeState::Pending {
        return Some(ShardEvent::Skipped);
    }

    // State-specific events
    match state {
        ShardUpgradeState::Pending => {
            if ctx.replica_count > 0 {
                Some(ShardEvent::StartReplicaUpgrade)
            } else {
                Some(ShardEvent::ReplicationSynced)
            }
        }

        ShardUpgradeState::UpgradingReplicas => {
            if ctx.replicas_upgraded >= ctx.replica_count {
                Some(ShardEvent::ReplicasUpgraded)
            } else {
                None // Continue upgrading
            }
        }

        ShardUpgradeState::WaitingForSync => {
            if ctx.replication_synced {
                Some(ShardEvent::ReplicationSynced)
            } else {
                None // Continue waiting
            }
        }

        ShardUpgradeState::FailingOver => {
            if ctx.failover_completed {
                Some(ShardEvent::FailoverCompleted)
            } else {
                None // Continue failover
            }
        }

        ShardUpgradeState::UpgradingOldMaster => {
            // This is handled externally when pod is upgraded
            None
        }

        // Terminal states
        ShardUpgradeState::Completed | ShardUpgradeState::Failed | ShardUpgradeState::Skipped => {
            None
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;

    #[test]
    fn test_upgrade_phase_transitions() {
        let sm = UpgradeStateMachine::new();

        // Valid transitions from Pending
        assert!(sm.can_transition(UpgradePhase::Pending, UpgradePhase::PreChecks));
        assert!(sm.can_transition(UpgradePhase::Pending, UpgradePhase::Failed));

        // Valid transitions from PreChecks
        assert!(sm.can_transition(UpgradePhase::PreChecks, UpgradePhase::InProgress));
        assert!(sm.can_transition(UpgradePhase::PreChecks, UpgradePhase::Failed));

        // Valid transitions from InProgress
        assert!(sm.can_transition(UpgradePhase::InProgress, UpgradePhase::Completed));
        assert!(sm.can_transition(UpgradePhase::InProgress, UpgradePhase::RollingBack));
        assert!(sm.can_transition(UpgradePhase::InProgress, UpgradePhase::Failed));

        // Invalid transition
        assert!(!sm.can_transition(UpgradePhase::Pending, UpgradePhase::Completed));
        assert!(!sm.can_transition(UpgradePhase::PreChecks, UpgradePhase::Completed));

        // Terminal states cannot transition
        assert!(!sm.can_transition(UpgradePhase::Completed, UpgradePhase::InProgress));
        assert!(!sm.can_transition(UpgradePhase::Failed, UpgradePhase::Pending));
    }

    #[test]
    fn test_same_phase_transition() {
        let sm = UpgradeStateMachine::new();

        // Same phase should always be valid
        assert!(sm.can_transition(UpgradePhase::Pending, UpgradePhase::Pending));
        assert!(sm.can_transition(UpgradePhase::InProgress, UpgradePhase::InProgress));
        assert!(sm.can_transition(UpgradePhase::Completed, UpgradePhase::Completed));
    }

    #[test]
    fn test_valid_transitions_list() {
        let sm = UpgradeStateMachine::new();

        let from_pending = sm.valid_transitions(UpgradePhase::Pending);
        assert!(from_pending.contains(&UpgradePhase::PreChecks));
        assert!(from_pending.contains(&UpgradePhase::Failed));
        assert!(!from_pending.contains(&UpgradePhase::Completed));

        let from_completed = sm.valid_transitions(UpgradePhase::Completed);
        assert!(from_completed.is_empty());
    }

    #[test]
    fn test_transition_with_guards() {
        let sm = UpgradeStateMachine::new();

        // Healthy cluster can start
        let ctx = UpgradeTransitionContext {
            cluster_healthy: true,
            slots_stable: true,
            ..Default::default()
        };
        let transition = sm.transition(UpgradePhase::Pending, UpgradeEvent::Created, &ctx);
        assert!(transition.is_some());
        assert_eq!(transition.unwrap().to, UpgradePhase::PreChecks);

        // Unhealthy cluster cannot start
        let ctx = UpgradeTransitionContext {
            cluster_healthy: false,
            ..Default::default()
        };
        let transition = sm.transition(UpgradePhase::Pending, UpgradeEvent::Created, &ctx);
        assert!(transition.is_none());
    }

    #[test]
    fn test_completion_guard() {
        let sm = UpgradeStateMachine::new();

        // Cannot complete with shards remaining
        let ctx = UpgradeTransitionContext {
            cluster_healthy: true,
            total_shards: 3,
            upgraded_shards: 2,
            ..Default::default()
        };
        let transition = sm.transition(
            UpgradePhase::InProgress,
            UpgradeEvent::AllShardsCompleted,
            &ctx,
        );
        assert!(transition.is_none());

        // Can complete when all shards done
        let ctx = UpgradeTransitionContext {
            cluster_healthy: true,
            total_shards: 3,
            upgraded_shards: 3,
            ..Default::default()
        };
        let transition = sm.transition(
            UpgradePhase::InProgress,
            UpgradeEvent::AllShardsCompleted,
            &ctx,
        );
        assert!(transition.is_some());
    }

    #[test]
    fn test_determine_upgrade_event() {
        // Timeout takes priority
        let ctx = UpgradeTransitionContext {
            timed_out: true,
            cluster_healthy: true,
            ..Default::default()
        };
        assert_eq!(
            determine_upgrade_event(UpgradePhase::InProgress, &ctx),
            Some(UpgradeEvent::Timeout)
        );

        // Cluster unhealthy next priority
        let ctx = UpgradeTransitionContext {
            cluster_healthy: false,
            ..Default::default()
        };
        assert_eq!(
            determine_upgrade_event(UpgradePhase::InProgress, &ctx),
            Some(UpgradeEvent::ClusterUnhealthy)
        );

        // Shard failure triggers rollback
        let ctx = UpgradeTransitionContext {
            cluster_healthy: true,
            failed_shards: 1,
            ..Default::default()
        };
        assert_eq!(
            determine_upgrade_event(UpgradePhase::InProgress, &ctx),
            Some(UpgradeEvent::ShardFailed)
        );
    }

    #[test]
    fn test_shard_state_transitions() {
        let sm = ShardStateMachine::new();

        // Valid transitions
        assert!(sm.can_transition(
            ShardUpgradeState::Pending,
            ShardUpgradeState::UpgradingReplicas
        ));
        assert!(sm.can_transition(
            ShardUpgradeState::UpgradingReplicas,
            ShardUpgradeState::WaitingForSync
        ));
        assert!(sm.can_transition(
            ShardUpgradeState::WaitingForSync,
            ShardUpgradeState::FailingOver
        ));
        assert!(sm.can_transition(
            ShardUpgradeState::FailingOver,
            ShardUpgradeState::UpgradingOldMaster
        ));
        assert!(sm.can_transition(
            ShardUpgradeState::UpgradingOldMaster,
            ShardUpgradeState::Completed
        ));

        // Invalid transitions
        assert!(!sm.can_transition(ShardUpgradeState::Pending, ShardUpgradeState::Completed));
        assert!(!sm.can_transition(ShardUpgradeState::Completed, ShardUpgradeState::Pending));
    }

    #[test]
    fn test_shard_terminal_states() {
        let sm = ShardStateMachine::new();

        assert!(sm.is_terminal(ShardUpgradeState::Completed));
        assert!(sm.is_terminal(ShardUpgradeState::Failed));
        assert!(sm.is_terminal(ShardUpgradeState::Skipped));

        assert!(!sm.is_terminal(ShardUpgradeState::Pending));
        assert!(!sm.is_terminal(ShardUpgradeState::UpgradingReplicas));
    }

    #[test]
    fn test_rollback_transitions() {
        let sm = UpgradeStateMachine::new();

        // Can rollback from InProgress
        assert!(sm.can_transition(UpgradePhase::InProgress, UpgradePhase::RollingBack));

        // Rollback can complete or fail
        assert!(sm.can_transition(UpgradePhase::RollingBack, UpgradePhase::RolledBack));
        assert!(sm.can_transition(UpgradePhase::RollingBack, UpgradePhase::Failed));

        // Cannot rollback from terminal states
        assert!(!sm.can_transition(UpgradePhase::Completed, UpgradePhase::RollingBack));
    }

    #[test]
    fn test_shard_event_transition() {
        let sm = ShardStateMachine::new();

        // Normal flow with replicas
        let ctx = ShardTransitionContext {
            replica_count: 1,
            ..Default::default()
        };
        let next = sm.transition(
            ShardUpgradeState::Pending,
            ShardEvent::StartReplicaUpgrade,
            &ctx,
        );
        assert_eq!(next, Some(ShardUpgradeState::UpgradingReplicas));

        // Skip to WaitingForSync when no replicas
        let ctx = ShardTransitionContext {
            replica_count: 0,
            ..Default::default()
        };
        let next = sm.transition(
            ShardUpgradeState::Pending,
            ShardEvent::ReplicationSynced,
            &ctx,
        );
        assert_eq!(next, Some(ShardUpgradeState::WaitingForSync));
    }

    #[test]
    fn test_upgrade_event_display() {
        assert_eq!(format!("{}", UpgradeEvent::Created), "Created");
        assert_eq!(
            format!("{}", UpgradeEvent::PreChecksPassed),
            "PreChecksPassed"
        );
        assert_eq!(
            format!("{}", UpgradeEvent::RollbackRequested),
            "RollbackRequested"
        );
    }

    #[test]
    fn test_shard_event_display() {
        assert_eq!(
            format!("{}", ShardEvent::StartReplicaUpgrade),
            "StartReplicaUpgrade"
        );
        assert_eq!(
            format!("{}", ShardEvent::FailoverCompleted),
            "FailoverCompleted"
        );
    }
}
