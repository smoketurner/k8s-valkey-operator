//! Resource generation module.
//!
//! Contains utilities for generating Kubernetes resources owned by ValkeyCluster.
//!
//! ## Resources Generated
//!
//! | Resource | Purpose |
//! |----------|---------|
//! | StatefulSet | Stable pod identity for cluster nodes |
//! | Headless Service | Cluster discovery (publishNotReadyAddresses) |
//! | Client Service | Client access endpoint |
//! | PodDisruptionBudget | Maintain quorum during disruptions |

pub mod common;
pub mod pdb;
pub mod services;
pub mod statefulset;

// Re-export commonly used functions
pub use common::{owner_reference, standard_labels};
pub use pdb::generate_pod_disruption_budget;
pub use services::{generate_client_service, generate_headless_service};
pub use statefulset::generate_statefulset;
