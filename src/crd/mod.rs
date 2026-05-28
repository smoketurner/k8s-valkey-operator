//! Custom Resource Definitions (CRDs) for valkey-operator.
//!
//! - `ValkeyCluster`: Deploy and manage Valkey cluster topology
//! - `ValkeyUpgrade`: Handle rolling upgrades with proper failover orchestration

mod conditions;
mod newtypes;
mod valkey_cluster;
mod valkey_upgrade;

pub use conditions::*;
pub use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
pub use newtypes::*;
pub use valkey_cluster::*;
pub use valkey_upgrade::*;
