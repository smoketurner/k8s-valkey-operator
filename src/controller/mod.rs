//! Controller module for my-operator.
//!
//! Contains the reconciliation loop, state machine, error handling, status management,
//! and validation logic.

pub mod context;
pub mod error;
pub mod reconciler;
pub mod state_machine;
pub mod status;
pub mod validation;
