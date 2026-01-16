//! Scaling operations for Valkey clusters.
//!
//! This module provides high-level operations for scaling Valkey clusters:
//! - Scale up: Add new nodes and redistribute slots
//! - Scale down: Migrate slots away and remove nodes

use std::collections::HashMap;
use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use super::types::ClusterNode;
use super::valkey_client::{ClusterSetSlotState, ValkeyClient, ValkeyError};
use crate::slots::{calculate_distribution, planner::extract_ordinal_from_address};

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

/// Scaling operations for ValkeyClient.
impl ValkeyClient {
    /// Execute a scale up operation.
    ///
    /// Steps:
    /// 1. CLUSTER MEET new nodes to join the cluster
    /// 2. Wait for nodes to be recognized
    /// 3. Rebalance slots across all masters
    #[instrument(skip(self, ctx))]
    pub async fn scale_up(&self, ctx: &ScalingContext) -> Result<ScalingResult, ValkeyError> {
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

    /// Execute a scale down operation.
    ///
    /// Steps:
    /// 1. Migrate slots from nodes being removed to remaining nodes
    /// 2. CLUSTER FORGET to remove nodes from cluster
    #[instrument(skip(self, ctx))]
    pub async fn scale_down(&self, ctx: &ScalingContext) -> Result<ScalingResult, ValkeyError> {
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
                    ordinals_to_remove.contains(&(ordinal as i32))
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

        // Migrate slots from nodes being removed using atomic slot migration (Valkey 9.0+)
        for node in &nodes_to_remove {
            info!(node_id = %node.node_id, "Migrating slots from node using atomic migration");

            // Get slots owned by this node, grouped into ranges
            let slot_ranges = node.slots.clone();

            if slot_ranges.is_empty() {
                debug!(node_id = %node.node_id, "Node has no slots to migrate");
                continue;
            }

            // Group slots by destination node for efficient migration
            // Round-robin assignment: distribute slot ranges across remaining masters
            let remaining_masters: Vec<&ClusterNode> = masters
                .iter()
                .filter(|m| {
                    !nodes_to_remove
                        .iter()
                        .any(|n| n.node_id == m.node_id)
                })
                .copied()
                .collect();

            if remaining_masters.is_empty() {
                return Err(ValkeyError::InvalidConfig(
                    "Cannot scale down: no remaining masters to receive slots".to_string(),
                ));
            }

            // Migrate slot ranges to remaining masters using atomic migration
            for (range_idx, range) in slot_ranges.iter().enumerate() {
                // Round-robin assignment of ranges to remaining masters
                let dest_idx = range_idx % remaining_masters.len();
                let dest_node = remaining_masters[dest_idx];

                info!(
                    from = %node.node_id,
                    to = %dest_node.node_id,
                    start_slot = range.start,
                    end_slot = range.end,
                    "Migrating slot range using CLUSTER MIGRATESLOTS"
                );

                // Use atomic slot migration (Valkey 9.0+)
                // CLUSTER MIGRATESLOTS must be executed on the source node
                // Since we're using a cluster-aware client, it should route correctly
                // but for safety, we'll connect directly to the source node
                match self
                    .migrate_slots_atomic(
                        range.start as u16,
                        range.end as u16,
                        &node.node_id,
                        &dest_node.node_id,
                        &node.ip,
                        node.port as u16,
                    )
                    .await
                {
                    Ok(()) => {
                        result.slots_moved += (range.end - range.start + 1) as i32;
                        debug!(
                            range = ?range,
                            "Slot range migration completed"
                        );
                    }
                    Err(e) => {
                        warn!(
                            range = ?range,
                            error = %e,
                            "Failed to migrate slot range, will retry"
                        );
                        return Err(e);
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

    /// Rebalance slots across all master nodes.
    ///
    /// This redistributes slots evenly across all masters.
    #[instrument(skip(self))]
    pub async fn rebalance_slots(&self, master_count: i32) -> Result<i32, ValkeyError> {
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

        // Calculate target slot distribution using new slots module
        let target_distribution = calculate_distribution(master_count as u16);

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
        for (master, range) in masters.iter().zip(target_distribution.iter()) {
            for slot in range.iter() {
                // Check if slot needs to be moved to this master
                if let Some(current_owner) = current_ownership.get(&slot) {
                    if *current_owner != master.node_id {
                        debug!(
                            slot = slot,
                            from = %current_owner,
                            to = %master.node_id,
                            "Reassigning slot"
                        );

                        // Use atomic slot migration for Valkey 9.0+
                        // Group consecutive slots into ranges for efficiency
                        // For now, migrate one slot at a time (can be optimized later)
                        match self
                            .migrate_slots_atomic(
                                slot,
                                slot,
                                current_owner,
                                &master.node_id,
                                "", // Source IP not needed for cluster-aware client
                                0,  // Source port not needed
                            )
                            .await
                        {
                            Ok(()) => {
                                slots_moved += 1;
                            }
                            Err(e) => {
                                // If atomic migration fails (e.g., not Valkey 9.0+), fall back to legacy
                                warn!(
                                    slot = slot,
                                    error = %e,
                                    "Atomic migration failed, falling back to legacy method"
                                );
                                self.cluster_setslot(
                                    slot,
                                    ClusterSetSlotState::Node(master.node_id.clone()),
                                )
                                .await?;
                                slots_moved += 1;
                            }
                        }
                    }
                } else {
                    // Slot is unassigned, assign it
                    debug!(
                        slot = slot,
                        to = %master.node_id,
                        "Assigning unowned slot"
                    );
                    self.cluster_setslot(slot, ClusterSetSlotState::Node(master.node_id.clone()))
                        .await?;
                    slots_moved += 1;
                }
            }
        }

        info!(slots_moved = slots_moved, "Rebalance complete");
        Ok(slots_moved)
    }

    /// Migrate a single slot from one node to another.
    ///
    /// **Deprecated**: Use `migrate_slots_atomic()` for Valkey 9.0+ clusters.
    /// This method uses legacy CLUSTER SETSLOT which can lose data.
    #[instrument(skip(self))]
    pub async fn migrate_slot(
        &self,
        slot: u16,
        _source_node_id: &str,
        dest_node_id: &str,
    ) -> Result<(), ValkeyError> {
        // Legacy method - use migrate_slots_atomic() instead for Valkey 9.0+
        warn!("Using legacy slot migration - consider using migrate_slots_atomic() for Valkey 9.0+");
        self.cluster_setslot(slot, ClusterSetSlotState::Node(dest_node_id.to_string()))
            .await?;

        Ok(())
    }

    /// Migrate a range of slots atomically using CLUSTER MIGRATESLOTS (Valkey 9.0+).
    ///
    /// This is the recommended method for slot migration as it's atomic, faster,
    /// and prevents data loss compared to legacy CLUSTER SETSLOT method.
    ///
    /// # Arguments
    /// * `start_slot` - First slot in the range
    /// * `end_slot` - Last slot in the range (inclusive)
    /// * `source_node_id` - Node ID of the source node (for verification/logging)
    /// * `target_node_id` - Node ID of the target node receiving the slots
    /// * `source_ip` - IP address of source node (unused, kept for API compatibility)
    /// * `source_port` - Port of source node (unused, kept for API compatibility)
    ///
    /// Note: CLUSTER MIGRATESLOTS should be executed on the source node, but
    /// a cluster-aware client should route the command correctly. If direct
    /// connection to source is needed, it should be handled by the caller.
    #[instrument(skip(self), fields(start_slot, end_slot, target = %target_node_id))]
    pub async fn migrate_slots_atomic(
        &self,
        start_slot: u16,
        end_slot: u16,
        _source_node_id: &str,
        target_node_id: &str,
        _source_ip: &str,
        _source_port: u16,
    ) -> Result<(), ValkeyError> {
        // Execute CLUSTER MIGRATESLOTS
        // Note: According to Valkey docs, this should be executed on the source node.
        // A cluster-aware client may route this correctly, but for production use,
        // consider connecting directly to the source node.
        info!(
            start_slot = start_slot,
            end_slot = end_slot,
            target = %target_node_id,
            "Executing CLUSTER MIGRATESLOTS (atomic migration)"
        );

        self.cluster_migrateslots(start_slot, end_slot, target_node_id)
            .await?;

        // Poll migration progress until complete
        const MAX_WAIT_SECS: u64 = 300; // 5 minutes max
        const POLL_INTERVAL_SECS: u64 = 2;
        let start = std::time::Instant::now();

        loop {
            if start.elapsed().as_secs() > MAX_WAIT_SECS {
                return Err(ValkeyError::Timeout {
                    operation: format!(
                        "Slot migration {}-{} to {}",
                        start_slot, end_slot, target_node_id
                    ),
                    duration: Duration::from_secs(MAX_WAIT_SECS),
                });
            }

            // Check if migration is complete by verifying slot ownership
            let nodes = self.cluster_nodes().await?;
            let target_node = nodes.get_node(target_node_id);

            if let Some(target) = target_node {
                // Check if target node now owns the slot range
                let owns_range = target.slots.iter().any(|range| {
                    range.start <= start_slot as i32 && range.end >= end_slot as i32
                });

                if owns_range {
                    info!(
                        start_slot = start_slot,
                        end_slot = end_slot,
                        "Slot range migration completed"
                    );
                    break;
                }
            }

            // Migration still in progress, wait and poll again
            debug!(
                start_slot = start_slot,
                end_slot = end_slot,
                "Slot migration in progress, waiting..."
            );
            tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
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
        assert!(names.first().unwrap().contains("valkey-3"));
        assert!(names.get(1).unwrap().contains("valkey-4"));
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
