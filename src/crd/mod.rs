//! Custom Resource Definitions (CRDs) for valkey-operator.
//!
//! - `ValkeyCluster`: Deploy and manage Valkey cluster topology
//! - `ValkeyUpgrade`: Handle rolling upgrades with proper failover orchestration (TODO)

mod valkey_cluster;

pub use valkey_cluster::*;
