//! State machine for ValkeyUpgrade phase transitions.
//!
//! This module defines valid phase transitions and provides validation
//! for the upgrade lifecycle.

use crate::crd::{ShardUpgradeState, UpgradePhase};

/// State machine for validating ValkeyUpgrade phase transitions.
#[derive(Debug, Clone, Copy, Default)]
pub struct UpgradeStateMachine;

impl UpgradeStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self
    }

    /// Check if a phase transition is valid.
    pub fn can_transition(&self, from: UpgradePhase, to: UpgradePhase) -> bool {
        // Same phase is always valid (no-op)
        if from == to {
            return true;
        }

        // Define valid transitions
        matches!(
            (from, to),
            // From Pending: can go to PreChecks or Failed
            (UpgradePhase::Pending, UpgradePhase::PreChecks)
                | (UpgradePhase::Pending, UpgradePhase::Failed)
                // From PreChecks: can go to InProgress or Failed
                | (UpgradePhase::PreChecks, UpgradePhase::InProgress)
                | (UpgradePhase::PreChecks, UpgradePhase::Failed)
                // From InProgress: can go to Completed, RollingBack, or Failed
                | (UpgradePhase::InProgress, UpgradePhase::Completed)
                | (UpgradePhase::InProgress, UpgradePhase::RollingBack)
                | (UpgradePhase::InProgress, UpgradePhase::Failed)
                // From RollingBack: can complete rollback or fail
                | (UpgradePhase::RollingBack, UpgradePhase::RolledBack)
                | (UpgradePhase::RollingBack, UpgradePhase::Failed)
        )
    }

    /// Get the list of valid next phases from a given phase.
    pub fn valid_transitions(&self, from: UpgradePhase) -> Vec<UpgradePhase> {
        let all_phases = [
            UpgradePhase::Pending,
            UpgradePhase::PreChecks,
            UpgradePhase::InProgress,
            UpgradePhase::Completed,
            UpgradePhase::Failed,
            UpgradePhase::RollingBack,
            UpgradePhase::RolledBack,
        ];

        all_phases
            .into_iter()
            .filter(|&to| to != from && self.can_transition(from, to))
            .collect()
    }

    /// Check if a phase is terminal (no further transitions possible).
    pub fn is_terminal(&self, phase: UpgradePhase) -> bool {
        phase.is_terminal()
    }
}

/// State machine for validating shard upgrade state transitions.
#[derive(Debug, Clone, Default)]
pub struct ShardStateMachine;

impl ShardStateMachine {
    /// Create a new shard state machine.
    pub fn new() -> Self {
        Self
    }

    /// Check if a shard state transition is valid.
    pub fn can_transition(&self, from: ShardUpgradeState, to: ShardUpgradeState) -> bool {
        // Same state is always valid
        if from == to {
            return true;
        }

        match (from, to) {
            // From Pending: can start upgrading replicas
            (ShardUpgradeState::Pending, ShardUpgradeState::UpgradingReplicas) => true,
            // If no replicas, can skip to WaitingForSync or UpgradingOldMaster
            (ShardUpgradeState::Pending, ShardUpgradeState::WaitingForSync) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::UpgradingOldMaster) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::Failed) => true,
            (ShardUpgradeState::Pending, ShardUpgradeState::Skipped) => true,

            // From UpgradingReplicas: can move to WaitingForSync or fail
            (ShardUpgradeState::UpgradingReplicas, ShardUpgradeState::WaitingForSync) => true,
            (ShardUpgradeState::UpgradingReplicas, ShardUpgradeState::Failed) => true,

            // From WaitingForSync: can move to FailingOver or UpgradingOldMaster (no replicas)
            (ShardUpgradeState::WaitingForSync, ShardUpgradeState::FailingOver) => true,
            (ShardUpgradeState::WaitingForSync, ShardUpgradeState::UpgradingOldMaster) => true,
            (ShardUpgradeState::WaitingForSync, ShardUpgradeState::Failed) => true,

            // From FailingOver: can move to UpgradingOldMaster or fail
            (ShardUpgradeState::FailingOver, ShardUpgradeState::UpgradingOldMaster) => true,
            (ShardUpgradeState::FailingOver, ShardUpgradeState::Failed) => true,

            // From UpgradingOldMaster: can complete or fail
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
            ShardUpgradeState::UpgradingReplicas => vec![
                ShardUpgradeState::WaitingForSync,
                ShardUpgradeState::Failed,
            ],
            ShardUpgradeState::WaitingForSync => vec![
                ShardUpgradeState::FailingOver,
                ShardUpgradeState::UpgradingOldMaster,
                ShardUpgradeState::Failed,
            ],
            ShardUpgradeState::FailingOver => vec![
                ShardUpgradeState::UpgradingOldMaster,
                ShardUpgradeState::Failed,
            ],
            ShardUpgradeState::UpgradingOldMaster => vec![
                ShardUpgradeState::Completed,
                ShardUpgradeState::Failed,
            ],
            // Terminal states
            ShardUpgradeState::Completed | ShardUpgradeState::Failed | ShardUpgradeState::Skipped => {
                vec![]
            }
        }
    }
}

#[cfg(test)]
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
    fn test_shard_state_transitions() {
        let sm = ShardStateMachine::new();

        // Valid transitions
        assert!(sm.can_transition(ShardUpgradeState::Pending, ShardUpgradeState::UpgradingReplicas));
        assert!(sm.can_transition(ShardUpgradeState::UpgradingReplicas, ShardUpgradeState::WaitingForSync));
        assert!(sm.can_transition(ShardUpgradeState::WaitingForSync, ShardUpgradeState::FailingOver));
        assert!(sm.can_transition(ShardUpgradeState::FailingOver, ShardUpgradeState::UpgradingOldMaster));
        assert!(sm.can_transition(ShardUpgradeState::UpgradingOldMaster, ShardUpgradeState::Completed));

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
}
