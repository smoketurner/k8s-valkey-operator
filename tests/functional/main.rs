// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

//! Functional tests for ValkeyCluster state machine topology verification.
//!
//! These tests verify state machine transitions across various cluster topologies
//! WITHOUT requiring a live Kubernetes cluster. They mock K8s and Valkey responses
//! to validate phase sequences.
//!
//! ```bash
//! # Run all functional tests
//! cargo test --test functional
//!
//! # Run specific test
//! cargo test --test functional test_standard_cluster_lifecycle
//!
//! # Run with verbose output
//! cargo test --test functional -- --nocapture
//! ```
//!
//! ## Test Categories
//!
//! - **Topology tests**: Full lifecycle tests for each cluster topology
//!   (minimal, standard, high-HA, large)
//! - **Phase handler tests**: Individual handler tests for pure logic functions
//! - **Scenario tests**: Complex multi-step scenarios (scale up/down, recovery, etc.)
//!
//! ## Design Principles
//!
//! - **No K8s Required**: Tests run without any cluster infrastructure
//! - **Fast Execution**: All tests complete in milliseconds
//! - **Comprehensive Coverage**: All 19 phases and 74 transitions covered
//! - **Executable Documentation**: Tests serve as documentation of expected behavior

mod mock_state;
mod phase_handler_tests;
mod scenario_tests;
mod topology_tests;

// Re-export for use in tests
pub use mock_state::*;
