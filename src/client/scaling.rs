//! Scaling operations for Valkey clusters.
//!
//! This module provides high-level operations for scaling Valkey clusters:
//! - Scale up: Add new nodes and redistribute slots
//! - Slot rebalancing across masters
//!
//! Note: Scale-down is handled by `execute_scale_down` in `cluster_reconciler.rs`
//! which uses `ClusterTopology` for correct IP→ordinal mapping.

use std::collections::HashMap;
use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use super::types::ClusterNode;
use super::valkey_client::{ClusterSetSlotState, ValkeyClient, ValkeyError};
use crate::slots::calculate_distribution;

/// Group consecutive slots into ranges.
///
/// Takes a sorted slice of slot numbers and returns a list of (start, end) ranges.
/// For example: [1, 2, 3, 5, 6, 10] -> [(1, 3), (5, 6), (10, 10)]
fn group_consecutive_slots(slots: &[u16]) -> Vec<(u16, u16)> {
    let mut iter = slots.iter().copied();
    let Some(first) = iter.next() else {
        return Vec::new();
    };

    let mut ranges = Vec::new();
    let mut start = first;
    let mut end = first;

    for slot in iter {
        if slot == end + 1 {
            // Consecutive, extend the range
            end = slot;
        } else {
            // Gap found, save current range and start new one
            ranges.push((start, end));
            start = slot;
            end = slot;
        }
    }
    // Don't forget the last range
    ranges.push((start, end));

    ranges
}

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
    // NOTE: scale_down was removed as dead code. Scale-down is now handled by
    // execute_scale_down in cluster_reconciler.rs which uses ClusterTopology
    // for correct IP→ordinal mapping.

    /// Rebalance slots across all master nodes.
    ///
    /// This redistributes slots evenly across all masters.
    /// Note: After CLUSTER MEET, new nodes may not have the "master" flag set yet,
    /// so we detect masters as nodes that are NOT replicas (have no master_id).
    #[instrument(skip(self))]
    pub async fn rebalance_slots(&self, master_count: i32) -> Result<i32, ValkeyError> {
        let mut slots_moved = 0;

        // Get current cluster state
        let nodes = self.cluster_nodes().await?;

        // Get masters including newly joined nodes that don't have the master flag yet.
        // After CLUSTER MEET, nodes start as "myself,master,noaddr" but may briefly
        // appear without the master flag. We detect masters as nodes that are NOT replicas
        // (i.e., have no master_id set or master_id is "-").
        let masters: Vec<&ClusterNode> = nodes
            .nodes
            .iter()
            .filter(|n| {
                // A master is any node that is NOT a replica
                // Replicas have master_id set (and slave flag), masters don't
                !n.is_replica()
                    && n.master_id
                        .as_ref()
                        .map(|id| id == "-" || id.is_empty())
                        .unwrap_or(true)
            })
            .collect();

        if masters.len() != master_count as usize {
            return Err(ValkeyError::ClusterNotReady(format!(
                "Expected {} masters but found {} (detected by not being replicas)",
                master_count,
                masters.len()
            )));
        }

        // Calculate target slot distribution using new slots module
        let target_distribution = calculate_distribution(master_count as u16);

        // Build current slot ownership map and node info lookup
        let mut current_ownership: HashMap<u16, String> = HashMap::new();
        let mut node_info: HashMap<String, (String, u16)> = HashMap::new(); // node_id -> (ip, port)
        for master in &masters {
            node_info.insert(
                master.node_id.clone(),
                (master.ip.clone(), master.port as u16),
            );
            for range in &master.slots {
                for slot in range.start..=range.end {
                    current_ownership.insert(slot as u16, master.node_id.clone());
                }
            }
        }

        // Collect all slot migrations grouped by (source, destination)
        // Key: (source_node_id, dest_node_id), Value: list of slots to migrate
        let mut migrations: HashMap<(String, String), Vec<u16>> = HashMap::new();
        let mut unassigned_slots: HashMap<String, Vec<u16>> = HashMap::new();

        for (master, range) in masters.iter().zip(target_distribution.iter()) {
            for slot in range.iter() {
                if let Some(current_owner) = current_ownership.get(&slot) {
                    if *current_owner != master.node_id {
                        // Slot needs to move from current_owner to master
                        migrations
                            .entry((current_owner.clone(), master.node_id.clone()))
                            .or_default()
                            .push(slot);
                    }
                } else {
                    // Slot is unassigned
                    unassigned_slots
                        .entry(master.node_id.clone())
                        .or_default()
                        .push(slot);
                }
            }
        }

        // Migrate slots in batched ranges for each (source, dest) pair
        for ((source, dest), mut slots) in migrations {
            // Sort slots to find consecutive ranges
            slots.sort_unstable();

            // Group consecutive slots into ranges
            let ranges = group_consecutive_slots(&slots);

            // Look up source node's IP and port for direct connection
            let (source_ip, source_port) = node_info.get(&source).ok_or_else(|| {
                ValkeyError::InvalidConfig(format!("Source node {} not found in cluster", source))
            })?;

            info!(
                from = %source,
                to = %dest,
                source_ip = %source_ip,
                source_port = %source_port,
                total_slots = slots.len(),
                num_ranges = ranges.len(),
                "Migrating slot ranges"
            );

            for (start, end) in ranges {
                debug!(
                    start_slot = start,
                    end_slot = end,
                    from = %source,
                    to = %dest,
                    "Migrating slot range"
                );

                match self
                    .migrate_slots_atomic(start, end, &source, &dest, source_ip, *source_port)
                    .await
                {
                    Ok(()) => {
                        slots_moved += (end - start + 1) as i32;
                    }
                    Err(e) => {
                        if e.to_string().contains("unknown subcommand") {
                            return Err(ValkeyError::InvalidConfig(
                                "Slot rebalancing requires Valkey 9.0+ (CLUSTER MIGRATESLOTS). \
                                Current cluster does not support atomic slot migration. \
                                Either upgrade to Valkey 9.0+ or manually rebalance using: \
                                valkey-cli --cluster rebalance <host>:<port>"
                                    .to_string(),
                            ));
                        }
                        warn!(
                            start_slot = start,
                            end_slot = end,
                            error = %e,
                            "Slot range migration failed"
                        );
                        return Err(e);
                    }
                }
            }
        }

        // Handle unassigned slots (batch them into ranges too)
        for (dest, mut slots) in unassigned_slots {
            slots.sort_unstable();
            let ranges = group_consecutive_slots(&slots);

            for (start, end) in ranges {
                debug!(
                    start_slot = start,
                    end_slot = end,
                    to = %dest,
                    "Assigning unowned slot range"
                );
                // For unassigned slots, we need to assign each one
                // CLUSTER SETSLOT doesn't support ranges
                for slot in start..=end {
                    self.cluster_setslot(slot, ClusterSetSlotState::Node(dest.clone()))
                        .await?;
                    slots_moved += 1;
                }
            }
        }

        info!(slots_moved = slots_moved, "Rebalance complete");
        Ok(slots_moved)
    }

    /// Migrate a range of slots atomically using CLUSTER MIGRATESLOTS (Valkey 9.0+).
    ///
    /// This method uses atomic slot migration which is faster and prevents data loss.
    /// IMPORTANT: CLUSTER MIGRATESLOTS must be executed on the SOURCE node that owns the slots.
    ///
    /// # Arguments
    /// * `start_slot` - First slot in the range
    /// * `end_slot` - Last slot in the range (inclusive)
    /// * `source_node_id` - Node ID of the source node (for verification/logging)
    /// * `target_node_id` - Node ID of the target node receiving the slots
    /// * `source_ip` - IP address of source node to connect to
    /// * `source_port` - Port of source node to connect to
    #[instrument(skip(self), fields(start_slot, end_slot, target = %target_node_id, source_ip = %source_ip, source_port))]
    pub async fn migrate_slots_atomic(
        &self,
        start_slot: u16,
        end_slot: u16,
        _source_node_id: &str,
        target_node_id: &str,
        source_ip: &str,
        source_port: u16,
    ) -> Result<(), ValkeyError> {
        info!(
            start_slot = start_slot,
            end_slot = end_slot,
            target = %target_node_id,
            source_ip = %source_ip,
            source_port = source_port,
            "Executing CLUSTER MIGRATESLOTS (atomic migration)"
        );

        // Connect directly to the source node - CLUSTER MIGRATESLOTS must be executed
        // on the node that currently owns the slots
        let source_client = ValkeyClient::connect_single(
            source_ip,
            source_port,
            self.config().password.as_deref(),
            None,
        )
        .await
        .map_err(|e| {
            ValkeyError::Connection(format!(
                "Failed to connect to source node {}:{} for slot migration: {}",
                source_ip, source_port, e
            ))
        })?;

        // Execute CLUSTER MIGRATESLOTS on the source node
        source_client
            .cluster_migrateslots(start_slot, end_slot, target_node_id)
            .await?;

        // Close the connection to the source node
        let _ = source_client.close().await;

        // Poll migration progress until complete using the main cluster client
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
                // Check if target node now owns ALL slots in the range
                // Note: Valkey may fragment slots into multiple ranges after migration,
                // so we need to check each slot individually rather than looking for
                // a single contiguous range that contains the entire migrated range.
                let owns_all_slots = (start_slot..=end_slot).all(|slot| {
                    target
                        .slots
                        .iter()
                        .any(|range| range.start <= slot as i32 && range.end >= slot as i32)
                });

                if owns_all_slots {
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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
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

    #[test]
    fn test_group_consecutive_slots_empty() {
        let slots: Vec<u16> = vec![];
        let ranges = group_consecutive_slots(&slots);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_group_consecutive_slots_single() {
        let slots = vec![42];
        let ranges = group_consecutive_slots(&slots);
        assert_eq!(ranges, vec![(42, 42)]);
    }

    #[test]
    fn test_group_consecutive_slots_consecutive() {
        let slots = vec![1, 2, 3, 4, 5];
        let ranges = group_consecutive_slots(&slots);
        assert_eq!(ranges, vec![(1, 5)]);
    }

    #[test]
    fn test_group_consecutive_slots_with_gaps() {
        let slots = vec![1, 2, 3, 5, 6, 10];
        let ranges = group_consecutive_slots(&slots);
        assert_eq!(ranges, vec![(1, 3), (5, 6), (10, 10)]);
    }

    #[test]
    fn test_group_consecutive_slots_all_separate() {
        let slots = vec![1, 3, 5, 7, 9];
        let ranges = group_consecutive_slots(&slots);
        assert_eq!(ranges, vec![(1, 1), (3, 3), (5, 5), (7, 7), (9, 9)]);
    }

    #[test]
    fn test_group_consecutive_slots_large_range() {
        // Simulate slots 0-4095 (1/4 of cluster moving to new master)
        let slots: Vec<u16> = (0..4096).collect();
        let ranges = group_consecutive_slots(&slots);
        assert_eq!(ranges, vec![(0, 4095)]);
    }
}
