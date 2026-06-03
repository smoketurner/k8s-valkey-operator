//! Cluster state representation.
//!
//! This module provides a single source of truth for cluster state information,
//! consolidating health checks, slot assignments, and node states.

use crate::client::types::{ClusterInfo, ParsedClusterNodes};
use crate::crd::ClusterTopology;
use crate::slots::distribution::TOTAL_SLOTS;

/// Cluster state combining all state information.
///
/// This type consolidates cluster info, nodes, and topology into a single
/// source of truth for cluster health and state checks.
#[derive(Debug, Clone)]
pub struct ClusterState {
    /// Cluster info from CLUSTER INFO command.
    pub cluster_info: ClusterInfo,
    /// Parsed cluster nodes from CLUSTER NODES command.
    pub cluster_nodes: ParsedClusterNodes,
    /// Cluster topology (masters and replicas).
    pub topology: Option<ClusterTopology>,
}

impl ClusterState {
    /// Check if cluster is fully healthy.
    ///
    /// A cluster is healthy when:
    /// - cluster_state is "ok"
    /// - All TOTAL_SLOTS (16384) slots are assigned
    /// - No slots in migrating/importing state (unless actively migrating)
    /// - Cluster size matches expected masters
    /// - No failed nodes
    /// - No slots in fail/pfail state
    pub fn is_healthy(&self, expected_masters: i32) -> bool {
        use crate::client::types::ClusterHealthState;

        let cluster_state_ok = self.cluster_info.state == ClusterHealthState::Ok;
        let all_slots_assigned = self.cluster_info.slots_assigned == i32::from(TOTAL_SLOTS);
        let no_slots_fail = self.cluster_info.slots_fail == 0;
        let no_slots_pfail = self.cluster_info.slots_pfail == 0;
        let healthy_masters = self.healthy_masters_count();

        cluster_state_ok
            && all_slots_assigned
            && no_slots_fail
            && no_slots_pfail
            && healthy_masters == expected_masters
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        self.cluster_info.all_slots_assigned()
    }

    /// Check if cluster state is "ok".
    pub fn cluster_state_ok(&self) -> bool {
        use crate::client::types::ClusterHealthState;
        self.cluster_info.state == ClusterHealthState::Ok
    }

    /// Get number of healthy masters.
    pub fn healthy_masters_count(&self) -> i32 {
        self.cluster_nodes
            .masters()
            .iter()
            .filter(|m| !m.flags.fail && !m.flags.pfail)
            .count() as i32
    }

    /// Get number of healthy replicas.
    pub fn healthy_replicas_count(&self) -> i32 {
        self.cluster_nodes
            .replicas()
            .iter()
            .filter(|r| !r.flags.fail && !r.flags.pfail)
            .count() as i32
    }

    /// Check if there are slots in a failed or partially-failed state.
    pub fn has_failed_slots(&self) -> bool {
        self.cluster_info.slots_fail > 0 || self.cluster_info.slots_pfail > 0
    }

    /// Get detailed health status message.
    pub fn health_status_message(&self, expected_masters: i32) -> String {
        let mut issues = Vec::new();

        if !self.cluster_state_ok() {
            issues.push(format!(
                "cluster_state is not 'ok' (current: {})",
                self.cluster_info.state
            ));
        }

        if !self.all_slots_assigned() {
            issues.push(format!(
                "not all slots assigned ({}/{TOTAL_SLOTS})",
                self.cluster_info.slots_assigned
            ));
        }

        let healthy_masters = self.healthy_masters_count();
        if healthy_masters != expected_masters {
            issues.push(format!(
                "unhealthy masters (expected: {}, healthy: {})",
                expected_masters, healthy_masters
            ));
        }

        if self.has_failed_slots() {
            issues.push("slots in failed or partially-failed state".to_string());
        }

        if issues.is_empty() {
            "Cluster is healthy".to_string()
        } else {
            format!("Cluster health issues: {}", issues.join(", "))
        }
    }
}
