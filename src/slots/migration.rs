//! Slot migration state machine.
//!
//! This module models the Valkey slot migration protocol:
//! 1. SETSLOT IMPORTING on target
//! 2. SETSLOT MIGRATING on source
//! 3. MIGRATE keys in batches
//! 4. SETSLOT NODE on all nodes
//!
//! The state machine helps track progress and handle errors during migration.

/// State of a single slot migration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum MigrationState {
    /// Migration not started.
    #[default]
    Pending,
    /// Target node is importing (SETSLOT IMPORTING sent).
    Importing,
    /// Source node is migrating (SETSLOT MIGRATING sent).
    Migrating,
    /// Keys are being transferred.
    TransferringKeys {
        /// Number of keys moved so far.
        moved: u64,
        /// Number of keys remaining.
        remaining: u64,
    },
    /// Migration complete, propagating to all nodes.
    Finalizing,
    /// Migration complete.
    Complete,
    /// Migration failed.
    Failed {
        /// Error description.
        error: String,
    },
}

impl MigrationState {
    /// Check if this state indicates the migration is done.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            MigrationState::Complete | MigrationState::Failed { .. }
        )
    }

    /// Check if the migration completed successfully.
    pub fn is_complete(&self) -> bool {
        matches!(self, MigrationState::Complete)
    }

    /// Check if the migration failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, MigrationState::Failed { .. })
    }

    /// Check if the migration is in progress.
    pub fn is_in_progress(&self) -> bool {
        !self.is_terminal() && !matches!(self, MigrationState::Pending)
    }
}

impl std::fmt::Display for MigrationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationState::Pending => write!(f, "pending"),
            MigrationState::Importing => write!(f, "importing"),
            MigrationState::Migrating => write!(f, "migrating"),
            MigrationState::TransferringKeys { moved, remaining } => {
                write!(f, "transferring ({}/{})", moved, moved + remaining)
            }
            MigrationState::Finalizing => write!(f, "finalizing"),
            MigrationState::Complete => write!(f, "complete"),
            MigrationState::Failed { error } => write!(f, "failed: {}", error),
        }
    }
}

/// Tracks the state of a slot migration.
#[derive(Debug, Clone)]
pub struct SlotMigrationTracker {
    /// The slot being migrated.
    pub slot: u16,
    /// Source node ID (None if this is an initial assignment).
    pub source_node: Option<String>,
    /// Target node ID.
    pub target_node: String,
    /// Current state of the migration.
    pub state: MigrationState,
    /// Total keys migrated so far.
    pub keys_migrated: u64,
}

impl SlotMigrationTracker {
    /// Create a new migration tracker.
    pub fn new(slot: u16, source: Option<String>, target: String) -> Self {
        Self {
            slot,
            source_node: source,
            target_node: target,
            state: MigrationState::Pending,
            keys_migrated: 0,
        }
    }

    /// Transition to the next state.
    pub fn advance(&mut self, next: MigrationState) {
        self.state = next;
    }

    /// Record keys as migrated.
    pub fn record_keys_migrated(&mut self, count: u64) {
        self.keys_migrated += count;
    }

    /// Mark as failed with an error message.
    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = MigrationState::Failed {
            error: error.into(),
        };
    }

    /// Mark as complete.
    pub fn complete(&mut self) {
        self.state = MigrationState::Complete;
    }

    /// Check if this is a simple assignment (no source, unassigned slot).
    pub fn is_assignment(&self) -> bool {
        self.source_node.is_none()
    }

    /// Check if the migration is done.
    pub fn is_done(&self) -> bool {
        self.state.is_terminal()
    }

    /// Check if the migration completed successfully.
    pub fn is_complete(&self) -> bool {
        self.state.is_complete()
    }

    /// Check if the migration failed.
    pub fn is_failed(&self) -> bool {
        self.state.is_failed()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_state_default() {
        let state = MigrationState::default();
        assert_eq!(state, MigrationState::Pending);
    }

    #[test]
    fn test_migration_state_is_terminal() {
        assert!(!MigrationState::Pending.is_terminal());
        assert!(!MigrationState::Importing.is_terminal());
        assert!(!MigrationState::Migrating.is_terminal());
        assert!(
            !MigrationState::TransferringKeys {
                moved: 0,
                remaining: 0
            }
            .is_terminal()
        );
        assert!(!MigrationState::Finalizing.is_terminal());
        assert!(MigrationState::Complete.is_terminal());
        assert!(
            MigrationState::Failed {
                error: "test".to_string()
            }
            .is_terminal()
        );
    }

    #[test]
    fn test_migration_state_is_in_progress() {
        assert!(!MigrationState::Pending.is_in_progress());
        assert!(MigrationState::Importing.is_in_progress());
        assert!(MigrationState::Migrating.is_in_progress());
        assert!(
            MigrationState::TransferringKeys {
                moved: 0,
                remaining: 100
            }
            .is_in_progress()
        );
        assert!(MigrationState::Finalizing.is_in_progress());
        assert!(!MigrationState::Complete.is_in_progress());
        assert!(
            !MigrationState::Failed {
                error: "test".to_string()
            }
            .is_in_progress()
        );
    }

    #[test]
    fn test_migration_state_display() {
        assert_eq!(format!("{}", MigrationState::Pending), "pending");
        assert_eq!(format!("{}", MigrationState::Importing), "importing");
        assert_eq!(
            format!(
                "{}",
                MigrationState::TransferringKeys {
                    moved: 50,
                    remaining: 50
                }
            ),
            "transferring (50/100)"
        );
        assert_eq!(format!("{}", MigrationState::Complete), "complete");
        assert_eq!(
            format!(
                "{}",
                MigrationState::Failed {
                    error: "connection lost".to_string()
                }
            ),
            "failed: connection lost"
        );
    }

    #[test]
    fn test_tracker_new() {
        let tracker =
            SlotMigrationTracker::new(100, Some("source-node".into()), "target-node".into());

        assert_eq!(tracker.slot, 100);
        assert_eq!(tracker.source_node, Some("source-node".to_string()));
        assert_eq!(tracker.target_node, "target-node");
        assert_eq!(tracker.state, MigrationState::Pending);
        assert_eq!(tracker.keys_migrated, 0);
    }

    #[test]
    fn test_tracker_is_assignment() {
        let assignment = SlotMigrationTracker::new(100, None, "target-node".into());
        assert!(assignment.is_assignment());

        let migration =
            SlotMigrationTracker::new(100, Some("source-node".into()), "target-node".into());
        assert!(!migration.is_assignment());
    }

    #[test]
    fn test_tracker_advance() {
        let mut tracker =
            SlotMigrationTracker::new(100, Some("source-node".into()), "target-node".into());

        assert_eq!(tracker.state, MigrationState::Pending);
        assert!(!tracker.is_done());

        tracker.advance(MigrationState::Importing);
        assert_eq!(tracker.state, MigrationState::Importing);

        tracker.advance(MigrationState::Migrating);
        assert_eq!(tracker.state, MigrationState::Migrating);

        tracker.complete();
        assert!(tracker.is_done());
        assert!(tracker.is_complete());
    }

    #[test]
    fn test_tracker_fail() {
        let mut tracker =
            SlotMigrationTracker::new(100, Some("source-node".into()), "target-node".into());

        tracker.fail("connection timeout");

        assert!(tracker.is_done());
        assert!(tracker.is_failed());
        assert!(!tracker.is_complete());
    }

    #[test]
    fn test_tracker_record_keys_migrated() {
        let mut tracker =
            SlotMigrationTracker::new(100, Some("source-node".into()), "target-node".into());

        tracker.record_keys_migrated(50);
        assert_eq!(tracker.keys_migrated, 50);

        tracker.record_keys_migrated(25);
        assert_eq!(tracker.keys_migrated, 75);
    }
}
