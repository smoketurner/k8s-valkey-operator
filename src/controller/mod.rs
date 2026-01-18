//! Controller module for valkey-operator.
//!
//! Contains the reconciliation loop, state machine, error handling, status management,
//! and validation logic.
//!
//! This module supports two controllers:
//! - ValkeyCluster controller (cluster_* modules)
//! - ValkeyUpgrade controller (upgrade_* modules)

// Shared modules
pub mod common;
pub mod context;
pub mod error;
pub mod status;

// ValkeyCluster controller
pub mod cluster_init;
pub mod cluster_phases;
pub mod cluster_reconciler;
pub mod cluster_state_machine;
pub mod cluster_topology;
pub mod cluster_validation;
pub mod operation_coordination;

// ValkeyUpgrade controller
pub mod upgrade_reconciler;
pub mod upgrade_state_machine;
