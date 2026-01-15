//! Valkey client module for cluster management operations.
//!
//! This module provides a type-safe wrapper around the `fred` Redis client
//! for performing Valkey cluster operations. It handles connection management,
//! TLS configuration, and provides high-level APIs for cluster management.
//!
//! ## Architecture
//!
//! - `valkey_client`: Core client wrapper with connection pooling and TLS support
//! - `types`: Parsed types for cluster information (nodes, slots, topology)
//! - `cluster_ops`: High-level cluster operations (meet, slot assignment, failover)
//! - `scaling`: Scale up/down operations with slot rebalancing
//!
//! ## Example
//!
//! ```rust,ignore
//! use valkey_operator::client::{ValkeyClient, ClusterOps};
//!
//! let client = ValkeyClient::connect(&cluster_spec).await?;
//! let info = client.cluster_info().await?;
//! if info.state == ClusterState::Ok {
//!     println!("Cluster is healthy");
//! }
//! ```

pub mod cluster_ops;
pub mod scaling;
pub mod types;
pub mod valkey_client;

pub use cluster_ops::ClusterOps;
pub use scaling::{ScalingContext, ScalingOps, ScalingResult};
pub use types::{
    ClusterInfo, ClusterNode, ClusterState, NodeFlags, NodeRole, ParsedClusterNodes, SlotRange,
};
pub use valkey_client::{ValkeyClient, ValkeyClientConfig};
