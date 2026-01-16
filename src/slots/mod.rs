//! Pure slot management logic for Valkey clusters.
//!
//! This module provides testable, pure functions for slot distribution and migration planning.
//! It separates the computation of "what slots to move" from "how to move them".
//!
//! ## Module Structure
//!
//! - [`distribution`]: Pure slot distribution calculations
//! - [`planner`]: Migration planning with `MigrationPlan` and `ClusterSlotState`
//! - [`migration`]: State machine for the migration protocol

pub mod distribution;
pub mod migration;
pub mod planner;

// Re-export commonly used types
pub use distribution::{SlotRange, TOTAL_SLOTS, calculate_distribution, slot_owner};
pub use migration::{MigrationState, SlotMigrationTracker};
pub use planner::{ClusterSlotState, MigrationPlan, SlotMigration};
