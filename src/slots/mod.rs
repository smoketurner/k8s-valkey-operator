//! Pure slot management logic for Valkey clusters.
//!
//! This module provides testable, pure functions for slot distribution.
//!
//! ## Module Structure
//!
//! - [`distribution`]: Pure slot distribution calculations

pub mod distribution;

// Re-export commonly used types
pub use distribution::{SlotRange, TOTAL_SLOTS, calculate_distribution};
