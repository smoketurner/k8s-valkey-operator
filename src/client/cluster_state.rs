//! Cluster state representation.
//!
//! This module provides a single source of truth for cluster state information,
//! consolidating health checks, slot assignments, and node states.

use crate::client::types::{ClusterInfo, ParsedClusterNodes};
use crate::crd::ClusterTopology;

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
    /// - All 16384 slots are assigned
    /// - No slots in migrating/importing state (unless actively migrating)
    /// - Cluster size matches expected masters
    /// - No failed nodes
    /// - No slots in fail/pfail state
    pub fn is_healthy(&self, expected_masters: i32) -> bool {
        use crate::client::types::ClusterHealthState;

        let cluster_state_ok = self.cluster_info.state == ClusterHealthState::Ok;
        let all_slots_assigned = self.cluster_info.slots_assigned == 16384;
        let cluster_size_matches = self.cluster_nodes.masters().len() as i32 == expected_masters;
        let no_failed_nodes = self
            .cluster_nodes
            .nodes
            .iter()
            .filter(|n| n.flags.fail)
            .count()
            == 0;
        let no_slots_fail = self.cluster_info.slots_fail == 0;
        let no_slots_pfail = self.cluster_info.slots_pfail == 0;

        // Check for healthy masters (not in fail/pfail state)
        let healthy_masters = self
            .cluster_nodes
            .masters()
            .iter()
            .filter(|m| !m.flags.fail && !m.flags.pfail)
            .count() as i32;

        cluster_state_ok
            && all_slots_assigned
            && cluster_size_matches
            && no_failed_nodes
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

    /// Check if there are any slots in migration state.
    ///
    /// Returns true if any slots are in migrating or importing state.
    pub fn has_migrating_slots(&self) -> bool {
        // Check for nodes with migrating/importing slots
        // This is a simplified check - in practice, we'd parse slot ranges with [slot-<-node] or [slot->-node]
        // For now, we check for failed/pfail nodes which often indicate migration issues
        self.cluster_nodes
            .nodes
            .iter()
            .any(|n| n.flags.fail || n.flags.pfail)
            || self.cluster_info.slots_fail > 0
            || self.cluster_info.slots_pfail > 0
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
                "not all slots assigned ({}/16384)",
                self.cluster_info.slots_assigned
            ));
        }

        let actual_masters = self.cluster_nodes.masters().len() as i32;
        if actual_masters != expected_masters {
            issues.push(format!(
                "master count mismatch (expected: {}, actual: {})",
                expected_masters, actual_masters
            ));
        }

        let healthy_masters = self.healthy_masters_count();
        if healthy_masters != expected_masters {
            issues.push(format!(
                "unhealthy masters (expected: {}, healthy: {})",
                expected_masters, healthy_masters
            ));
        }

        if self.has_migrating_slots() {
            issues.push("slots in migration or failed state".to_string());
        }

        if issues.is_empty() {
            "Cluster is healthy".to_string()
        } else {
            format!("Cluster health issues: {}", issues.join(", "))
        }
    }
}
