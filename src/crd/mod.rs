//! Custom Resource Definitions (CRDs) for valkey-operator.
//!
//! - `ValkeyCluster`: Deploy and manage Valkey cluster topology
//! - `ValkeyUpgrade`: Handle rolling upgrades with proper failover orchestration

mod valkey_cluster;
mod valkey_upgrade;

pub use valkey_cluster::*;
pub use valkey_upgrade::*;
