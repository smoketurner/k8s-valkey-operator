//! ValkeyUpgrade Custom Resource Definition.
//!
//! This CRD handles rolling upgrades of Valkey clusters with proper
//! failover orchestration to minimize data loss and downtime.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::valkey_cluster::Condition;

// ============================================================================
// ValkeyUpgrade CRD
// ============================================================================

/// ValkeyUpgrade manages rolling upgrades of a Valkey cluster.
///
/// The upgrade process follows this workflow per shard:
/// 1. Upgrade replicas first (simple restart)
/// 2. For primary upgrade:
///    - Select best replica (lowest replication lag)
///    - Wait for sync: `INFO REPLICATION` → `master_link_status:up`
///    - Verify cluster consistency: `CLUSTER NODES` on all nodes
///    - Execute: `CLUSTER FAILOVER` on replica
///    - Verify: `ROLE` command shows promotion
///    - Upgrade old primary (now replica)
/// 3. Move to next shard
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "valkey-operator.smoketurner.com",
    version = "v1alpha1",
    kind = "ValkeyUpgrade",
    plural = "valkeyupgrades",
    shortname = "vug",
    status = "ValkeyUpgradeStatus",
    namespaced,
    printcolumn = r#"{"name":"Cluster","type":"string","jsonPath":".spec.clusterRef.name"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Progress","type":"string","jsonPath":".status.progress"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ValkeyUpgradeSpec {
    /// Reference to the target ValkeyCluster to upgrade.
    pub cluster_ref: ClusterReference,

    /// Target Valkey version (e.g., "9.0.1").
    /// The operator appends "-alpine" and constructs the full image reference.
    pub target_version: String,

    /// Timeout in seconds for waiting for replication sync before failover.
    /// Defaults to 300 seconds (5 minutes) if not specified.
    /// This timeout applies per shard during the upgrade process.
    #[serde(default = "default_replication_sync_timeout")]
    pub replication_sync_timeout_seconds: u64,
}

fn default_replication_sync_timeout() -> u64 {
    300 // 5 minutes default
}

/// Reference to a ValkeyCluster resource.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterReference {
    /// Name of the ValkeyCluster to upgrade.
    pub name: String,

    /// Namespace of the ValkeyCluster (defaults to same namespace as upgrade).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

// ============================================================================
// ValkeyUpgrade Status
// ============================================================================

/// Status of a ValkeyUpgrade operation.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValkeyUpgradeStatus {
    /// Current phase of the upgrade.
    #[serde(default)]
    pub phase: UpgradePhase,

    /// Human-readable progress string (e.g., "2/6 shards upgraded").
    #[serde(default)]
    pub progress: String,

    /// Current version before upgrade started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_version: Option<String>,

    /// Target version being upgraded to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_version: Option<String>,

    /// Timestamp when the upgrade started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,

    /// Timestamp when the upgrade completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,

    /// Total number of shards (masters).
    #[serde(default)]
    pub total_shards: i32,

    /// Number of shards successfully upgraded.
    #[serde(default)]
    pub upgraded_shards: i32,

    /// Number of shards that failed upgrade.
    #[serde(default)]
    pub failed_shards: i32,

    /// Currently upgrading shard index (-1 if not upgrading).
    #[serde(default = "default_current_shard")]
    pub current_shard: i32,

    /// Status of individual shard upgrades.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shard_statuses: Vec<ShardUpgradeStatus>,

    /// Observed generation of the ValkeyUpgrade spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Conditions representing the current state.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// Error message if upgrade failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,

    /// Timestamp when replication sync check started for the current shard.
    /// Used to track timeout progress during WaitingForSync phase.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_started_at: Option<String>,

    /// Elapsed time in seconds since sync check started.
    /// Updated during WaitingForSync phase to show timeout progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_elapsed_seconds: Option<u64>,
}

fn default_current_shard() -> i32 {
    -1
}

/// Status of a single shard upgrade.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ShardUpgradeStatus {
    /// Shard index (0-based).
    pub shard_index: i32,

    /// Master node ID.
    pub master_node_id: String,

    /// Master pod name.
    pub master_pod: String,

    /// Status of this shard's upgrade.
    pub status: ShardUpgradeState,

    /// Replica that was promoted (if failover occurred).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promoted_replica: Option<String>,

    /// Error message if shard upgrade failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// State of a shard upgrade.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum ShardUpgradeState {
    /// Shard not yet processed.
    #[default]
    Pending,

    /// Upgrading replica nodes.
    UpgradingReplicas,

    /// Waiting for replication sync.
    WaitingForSync,

    /// Executing failover.
    FailingOver,

    /// Upgrading old master (now replica).
    UpgradingOldMaster,

    /// Shard upgrade completed successfully.
    Completed,

    /// Shard upgrade failed.
    Failed,

    /// Shard upgrade was skipped.
    Skipped,
}

impl std::fmt::Display for ShardUpgradeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardUpgradeState::Pending => write!(f, "Pending"),
            ShardUpgradeState::UpgradingReplicas => write!(f, "UpgradingReplicas"),
            ShardUpgradeState::WaitingForSync => write!(f, "WaitingForSync"),
            ShardUpgradeState::FailingOver => write!(f, "FailingOver"),
            ShardUpgradeState::UpgradingOldMaster => write!(f, "UpgradingOldMaster"),
            ShardUpgradeState::Completed => write!(f, "Completed"),
            ShardUpgradeState::Failed => write!(f, "Failed"),
            ShardUpgradeState::Skipped => write!(f, "Skipped"),
        }
    }
}

/// Phase of the upgrade operation.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum UpgradePhase {
    /// Upgrade is pending, not yet started.
    #[default]
    Pending,

    /// Running pre-upgrade health checks.
    PreChecks,

    /// Upgrade is in progress.
    InProgress,

    /// Upgrade completed successfully.
    Completed,

    /// Upgrade failed.
    Failed,

    /// Rolling back the upgrade.
    RollingBack,

    /// Rollback completed.
    RolledBack,
}

impl std::fmt::Display for UpgradePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpgradePhase::Pending => write!(f, "Pending"),
            UpgradePhase::PreChecks => write!(f, "PreChecks"),
            UpgradePhase::InProgress => write!(f, "InProgress"),
            UpgradePhase::Completed => write!(f, "Completed"),
            UpgradePhase::Failed => write!(f, "Failed"),
            UpgradePhase::RollingBack => write!(f, "RollingBack"),
            UpgradePhase::RolledBack => write!(f, "RolledBack"),
        }
    }
}

impl UpgradePhase {
    /// Check if this is a terminal phase.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            UpgradePhase::Completed | UpgradePhase::Failed | UpgradePhase::RolledBack
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
mod tests {
    use super::*;

    #[test]
    fn test_spec() {
        let spec = ValkeyUpgradeSpec {
            cluster_ref: ClusterReference {
                name: "my-cluster".to_string(),
                namespace: None,
            },
            target_version: "9.0.1".to_string(),
            replication_sync_timeout_seconds: 300,
        };

        assert_eq!(spec.cluster_ref.name, "my-cluster");
        assert_eq!(spec.target_version, "9.0.1");
    }

    #[test]
    fn test_upgrade_phase_terminal() {
        assert!(!UpgradePhase::Pending.is_terminal());
        assert!(!UpgradePhase::PreChecks.is_terminal());
        assert!(!UpgradePhase::InProgress.is_terminal());
        assert!(UpgradePhase::Completed.is_terminal());
        assert!(UpgradePhase::Failed.is_terminal());
        assert!(!UpgradePhase::RollingBack.is_terminal());
        assert!(UpgradePhase::RolledBack.is_terminal());
    }

    #[test]
    fn test_phase_display() {
        assert_eq!(format!("{}", UpgradePhase::Pending), "Pending");
        assert_eq!(format!("{}", UpgradePhase::InProgress), "InProgress");
        assert_eq!(format!("{}", UpgradePhase::Completed), "Completed");
    }

    #[test]
    fn test_shard_state_display() {
        assert_eq!(format!("{}", ShardUpgradeState::Pending), "Pending");
        assert_eq!(
            format!("{}", ShardUpgradeState::UpgradingReplicas),
            "UpgradingReplicas"
        );
        assert_eq!(format!("{}", ShardUpgradeState::Completed), "Completed");
    }
}
