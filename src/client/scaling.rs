//! Scaling operations for Valkey clusters.
//!
//! This module provides high-level operations for scaling Valkey clusters:
//! - Scale up: Add new nodes and redistribute slots
//! - Scale down: Migrate slots away and remove nodes

use std::collections::HashMap;
use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use super::cluster_ops::{calculate_slot_distribution, ClusterOps};
use super::types::ClusterNode;
use super::valkey_client::{ClusterSetSlotState, ValkeyClient, ValkeyError};

/// Result of a scaling operation.
#[derive(Debug, Clone)]
pub struct ScalingResult {
    /// Whether the operation completed successfully.
    pub success: bool,
    /// Number of slots moved.
    pub slots_moved: i32,
    /// Nodes added (for scale up).
    pub nodes_added: Vec<String>,
    /// Nodes removed (for scale down).
    pub nodes_removed: Vec<String>,
    /// Error message if operation failed.
    pub error: Option<String>,
}

impl Default for ScalingResult {
    fn default() -> Self {
        Self {
            success: true,
            slots_moved: 0,
            nodes_added: Vec::new(),
            nodes_removed: Vec::new(),
            error: None,
        }
    }
}

/// Context for scaling operations.
#[derive(Debug, Clone)]
pub struct ScalingContext {
    /// Current number of masters.
    pub current_masters: i32,
    /// Target number of masters.
    pub target_masters: i32,
    /// Namespace for DNS resolution.
    pub namespace: String,
    /// Headless service name for DNS.
    pub headless_service: String,
    /// Port for Valkey connections.
    pub port: u16,
}

impl ScalingContext {
    /// Check if this is a scale up operation.
    pub fn is_scale_up(&self) -> bool {
        self.target_masters > self.current_masters
    }

    /// Check if this is a scale down operation.
    pub fn is_scale_down(&self) -> bool {
        self.target_masters < self.current_masters
    }

    /// Get the number of masters being added or removed.
    pub fn delta(&self) -> i32 {
        self.target_masters - self.current_masters
    }

    /// Generate pod DNS names for new masters (scale up).
    pub fn new_master_dns_names(&self) -> Vec<String> {
        if !self.is_scale_up() {
            return Vec::new();
        }

        (self.current_masters..self.target_masters)
            .map(|i| {
                format!(
                    "{}-{}.{}.{}.svc.cluster.local",
                    self.headless_service.trim_end_matches("-headless"),
                    i,
                    self.headless_service,
                    self.namespace
                )
            })
            .collect()
    }

    /// Get ordinals of masters being removed (scale down).
    pub fn masters_to_remove(&self) -> Vec<i32> {
        if !self.is_scale_down() {
            return Vec::new();
        }

        (self.target_masters..self.current_masters).collect()
    }
}

/// Extension trait for scaling operations.
pub trait ScalingOps {
    /// Execute a scale up operation.
    ///
    /// Steps:
    /// 1. CLUSTER MEET new nodes to join the cluster
    /// 2. Wait for nodes to be recognized
    /// 3. Rebalance slots across all masters
    fn scale_up(
        &self,
        ctx: &ScalingContext,
    ) -> impl std::future::Future<Output = Result<ScalingResult, ValkeyError>> + Send;

    /// Execute a scale down operation.
    ///
    /// Steps:
    /// 1. Migrate slots from nodes being removed to remaining nodes
    /// 2. CLUSTER FORGET to remove nodes from cluster
    fn scale_down(
        &self,
        ctx: &ScalingContext,
    ) -> impl std::future::Future<Output = Result<ScalingResult, ValkeyError>> + Send;

    /// Rebalance slots across all master nodes.
    ///
    /// This redistributes slots evenly across all masters.
    fn rebalance_slots(
        &self,
        master_count: i32,
    ) -> impl std::future::Future<Output = Result<i32, ValkeyError>> + Send;

    /// Migrate a single slot from one node to another.
    fn migrate_slot(
        &self,
        slot: u16,
        source_node_id: &str,
        dest_node_id: &str,
    ) -> impl std::future::Future<Output = Result<(), ValkeyError>> + Send;
}

impl ScalingOps for ValkeyClient {
    #[instrument(skip(self, ctx))]
    async fn scale_up(&self, ctx: &ScalingContext) -> Result<ScalingResult, ValkeyError> {
        let mut result = ScalingResult::default();

        if !ctx.is_scale_up() {
            return Ok(result);
        }

        info!(
            current = ctx.current_masters,
            target = ctx.target_masters,
            delta = ctx.delta(),
            "Starting scale up operation"
        );

        // Get new master addresses
        let new_masters = ctx.new_master_dns_names();

        // CLUSTER MEET each new node
        for dns_name in &new_masters {
            info!(node = %dns_name, "Adding node to cluster via CLUSTER MEET");
            self.cluster_meet(dns_name, ctx.port).await?;
            result.nodes_added.push(dns_name.clone());
        }

        // Wait for nodes to be recognized
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify nodes joined
        let nodes = self.cluster_nodes().await?;
        let master_count = nodes.masters().len();

        if master_count < ctx.target_masters as usize {
            warn!(
                expected = ctx.target_masters,
                actual = master_count,
                "Not all new nodes joined cluster yet"
            );
        }

        // Rebalance slots across all masters
        let slots_moved = self.rebalance_slots(ctx.target_masters).await?;
        result.slots_moved = slots_moved;

        info!(
            nodes_added = result.nodes_added.len(),
            slots_moved = result.slots_moved,
            "Scale up complete"
        );

        Ok(result)
    }

    #[instrument(skip(self, ctx))]
    async fn scale_down(&self, ctx: &ScalingContext) -> Result<ScalingResult, ValkeyError> {
        let mut result = ScalingResult::default();

        if !ctx.is_scale_down() {
            return Ok(result);
        }

        info!(
            current = ctx.current_masters,
            target = ctx.target_masters,
            delta = ctx.delta(),
            "Starting scale down operation"
        );

        // Get current cluster state
        let nodes = self.cluster_nodes().await?;
        let masters: Vec<&ClusterNode> = nodes.masters().into_iter().collect();

        // Find masters to remove (highest ordinals)
        let ordinals_to_remove = ctx.masters_to_remove();

        // Find node IDs to remove by matching DNS names
        let nodes_to_remove: Vec<&ClusterNode> = masters
            .iter()
            .filter(|m| {
                // Extract ordinal from address and check if it should be removed
                if let Some(ordinal) = extract_ordinal_from_address(&m.address) {
                    ordinals_to_remove.contains(&ordinal)
                } else {
                    false
                }
            })
            .copied()
            .collect();

        if nodes_to_remove.is_empty() {
            warn!("No nodes found to remove, skipping scale down");
            return Ok(result);
        }

        // Calculate new slot distribution for remaining masters
        let _new_distribution = calculate_slot_distribution(ctx.target_masters);

        // Migrate slots from nodes being removed
        for node in &nodes_to_remove {
            info!(node_id = %node.node_id, "Migrating slots from node");

            // Get slots owned by this node
            let node_slots: Vec<u16> = node
                .slots
                .iter()
                .flat_map(|range| (range.start..=range.end).map(|s| s as u16))
                .collect();

            // Migrate each slot to a remaining master
            for (i, slot) in node_slots.iter().enumerate() {
                // Determine destination node (round-robin to remaining masters)
                let dest_idx = i % ctx.target_masters as usize;
                if let Some(dest_node) = masters.get(dest_idx) {
                    if !nodes_to_remove.iter().any(|n| n.node_id == dest_node.node_id) {
                        debug!(
                            slot = slot,
                            from = %node.node_id,
                            to = %dest_node.node_id,
                            "Migrating slot"
                        );
                        // Use CLUSTER SETSLOT NODE to reassign slot
                        self.cluster_setslot(
                            *slot,
                            ClusterSetSlotState::Node(dest_node.node_id.clone()),
                        )
                        .await?;
                        result.slots_moved += 1;
                    }
                }
            }
        }

        // CLUSTER FORGET removed nodes
        for node in &nodes_to_remove {
            info!(node_id = %node.node_id, "Removing node from cluster via CLUSTER FORGET");
            self.cluster_forget(&node.node_id).await?;
            result.nodes_removed.push(node.node_id.clone());
        }

        info!(
            nodes_removed = result.nodes_removed.len(),
            slots_moved = result.slots_moved,
            "Scale down complete"
        );

        Ok(result)
    }

    #[instrument(skip(self))]
    async fn rebalance_slots(&self, master_count: i32) -> Result<i32, ValkeyError> {
        let mut slots_moved = 0;

        // Get current cluster state
        let nodes = self.cluster_nodes().await?;
        let masters: Vec<&ClusterNode> = nodes.masters().into_iter().collect();

        if masters.len() != master_count as usize {
            return Err(ValkeyError::ClusterNotReady(format!(
                "Expected {} masters but found {}",
                master_count,
                masters.len()
            )));
        }

        // Calculate target slot distribution
        let target_distribution = calculate_slot_distribution(master_count);

        // Build current slot ownership map
        let mut current_ownership: HashMap<u16, String> = HashMap::new();
        for master in &masters {
            for range in &master.slots {
                for slot in range.start..=range.end {
                    current_ownership.insert(slot as u16, master.node_id.clone());
                }
            }
        }

        // For each master, determine what slots it should own
        for (idx, master) in masters.iter().enumerate() {
            let (start, end) = target_distribution[idx];

            for slot in start..=end {
                let slot_u16 = slot as u16;

                // Check if slot needs to be moved to this master
                if let Some(current_owner) = current_ownership.get(&slot_u16) {
                    if *current_owner != master.node_id {
                        debug!(
                            slot = slot,
                            from = %current_owner,
                            to = %master.node_id,
                            "Reassigning slot"
                        );

                        // Reassign slot to this master
                        self.cluster_setslot(
                            slot_u16,
                            ClusterSetSlotState::Node(master.node_id.clone()),
                        )
                        .await?;

                        slots_moved += 1;
                    }
                } else {
                    // Slot is unassigned, assign it
                    debug!(
                        slot = slot,
                        to = %master.node_id,
                        "Assigning unowned slot"
                    );
                    self.cluster_setslot(
                        slot_u16,
                        ClusterSetSlotState::Node(master.node_id.clone()),
                    )
                    .await?;
                    slots_moved += 1;
                }
            }
        }

        info!(slots_moved = slots_moved, "Rebalance complete");
        Ok(slots_moved)
    }

    #[instrument(skip(self))]
    async fn migrate_slot(
        &self,
        slot: u16,
        _source_node_id: &str,
        dest_node_id: &str,
    ) -> Result<(), ValkeyError> {
        // Simplified slot migration using CLUSTER SETSLOT NODE
        // Full migration with IMPORTING/MIGRATING/MIGRATE requires connecting
        // to individual nodes and fred doesn't fully support the node-id params
        self.cluster_setslot(slot, ClusterSetSlotState::Node(dest_node_id.to_string()))
            .await?;

        Ok(())
    }
}

/// Extract pod ordinal from a Valkey node address.
///
/// Addresses are in format: `hostname:port` or `ip:port`
/// For StatefulSet pods, hostname is like `cluster-name-0`
fn extract_ordinal_from_address(address: &str) -> Option<i32> {
    // Remove port if present
    let hostname = address.split(':').next()?;

    // Try to extract ordinal from end of hostname
    // Format: name-N where N is the ordinal
    let parts: Vec<&str> = hostname.rsplitn(2, '-').collect();
    if parts.len() == 2 {
        parts[0].parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scaling_context_scale_up() {
        let ctx = ScalingContext {
            current_masters: 3,
            target_masters: 6,
            namespace: "default".to_string(),
            headless_service: "my-cluster-headless".to_string(),
            port: 6379,
        };

        assert!(ctx.is_scale_up());
        assert!(!ctx.is_scale_down());
        assert_eq!(ctx.delta(), 3);
    }

    #[test]
    fn test_scaling_context_scale_down() {
        let ctx = ScalingContext {
            current_masters: 6,
            target_masters: 3,
            namespace: "default".to_string(),
            headless_service: "my-cluster-headless".to_string(),
            port: 6379,
        };

        assert!(!ctx.is_scale_up());
        assert!(ctx.is_scale_down());
        assert_eq!(ctx.delta(), -3);
        assert_eq!(ctx.masters_to_remove(), vec![3, 4, 5]);
    }

    #[test]
    fn test_new_master_dns_names() {
        let ctx = ScalingContext {
            current_masters: 3,
            target_masters: 5,
            namespace: "production".to_string(),
            headless_service: "valkey-headless".to_string(),
            port: 6379,
        };

        let names = ctx.new_master_dns_names();
        assert_eq!(names.len(), 2);
        assert!(names[0].contains("valkey-3"));
        assert!(names[1].contains("valkey-4"));
    }

    #[test]
    fn test_extract_ordinal_from_address() {
        assert_eq!(extract_ordinal_from_address("my-cluster-0:6379"), Some(0));
        assert_eq!(extract_ordinal_from_address("my-cluster-5:6379"), Some(5));
        assert_eq!(
            extract_ordinal_from_address("my-cluster-10.ns.svc:6379"),
            None
        );
        assert_eq!(extract_ordinal_from_address("192.168.1.1:6379"), Some(1));
    }

    #[test]
    fn test_scaling_result_default() {
        let result = ScalingResult::default();
        assert!(result.success);
        assert_eq!(result.slots_moved, 0);
        assert!(result.nodes_added.is_empty());
        assert!(result.nodes_removed.is_empty());
        assert!(result.error.is_none());
    }
}
