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
//! | Certificate | TLS certs via cert-manager |

pub mod certificate;
pub mod common;
pub mod pdb;
pub mod port_forward;
pub mod services;
pub mod statefulset;

// Re-export commonly used items from common
pub use common::{owner_reference, standard_labels};
