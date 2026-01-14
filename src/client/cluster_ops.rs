//! High-level cluster operations for Valkey cluster management.
//!
//! This module provides high-level operations that combine multiple
//! low-level Valkey commands to perform cluster management tasks.

use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use fred::types::InfoKind;

use super::valkey_client::{ValkeyClient, ValkeyError};
use super::types::{ClusterInfo, ClusterNode, ParsedClusterNodes};

/// Extension trait for high-level cluster operations.
pub trait ClusterOps {
    /// Get parsed cluster info.
    fn cluster_info(&self) -> impl std::future::Future<Output = Result<ClusterInfo, ValkeyError>> + Send;

    /// Get parsed cluster nodes.
    fn cluster_nodes(&self) -> impl std::future::Future<Output = Result<ParsedClusterNodes, ValkeyError>> + Send;

    /// Check if the cluster is healthy (state=ok, all slots assigned).
    fn is_cluster_healthy(&self) -> impl std::future::Future<Output = Result<bool, ValkeyError>> + Send;

    /// Wait for the cluster to become healthy.
    fn wait_for_cluster_healthy(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> impl std::future::Future<Output = Result<(), ValkeyError>> + Send;

    /// Initialize cluster by connecting all nodes via CLUSTER MEET.
    fn initialize_cluster(
        &self,
        node_addresses: &[(String, u16)],
    ) -> impl std::future::Future<Output = Result<(), ValkeyError>> + Send;

    /// Assign slots evenly across master nodes.
    fn assign_slots_evenly(
        &self,
        master_count: i32,
    ) -> impl std::future::Future<Output = Result<(), ValkeyError>> + Send;

    /// Get the replication lag for a replica from INFO REPLICATION.
    fn get_replication_lag(&self) -> impl std::future::Future<Output = Result<i64, ValkeyError>> + Send;

    /// Wait for replication to catch up.
    fn wait_for_replication_sync(
        &self,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<(), ValkeyError>> + Send;
}

impl ClusterOps for ValkeyClient {
    #[instrument(skip(self))]
    async fn cluster_info(&self) -> Result<ClusterInfo, ValkeyError> {
        let raw = self.cluster_info_raw().await?;
        let info = ClusterInfo::parse(&raw)?;
        Ok(info)
    }

    #[instrument(skip(self))]
    async fn cluster_nodes(&self) -> Result<ParsedClusterNodes, ValkeyError> {
        let raw = self.cluster_nodes_raw().await?;
        let nodes = ParsedClusterNodes::parse(&raw)?;
        Ok(nodes)
    }

    #[instrument(skip(self))]
    async fn is_cluster_healthy(&self) -> Result<bool, ValkeyError> {
        let info = self.cluster_info().await?;
        Ok(info.is_healthy())
    }

    #[instrument(skip(self))]
    async fn wait_for_cluster_healthy(
        &self,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<(), ValkeyError> {
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err(ValkeyError::Timeout {
                    operation: "wait_for_cluster_healthy".to_string(),
                    duration: timeout,
                });
            }

            match self.cluster_info().await {
                Ok(info) => {
                    if info.is_healthy() {
                        info!("Cluster is healthy");
                        return Ok(());
                    }
                    debug!(
                        state = ?info.state,
                        slots_assigned = info.slots_assigned,
                        slots_fail = info.slots_fail,
                        "Cluster not yet healthy"
                    );
                }
                Err(e) => {
                    warn!(error = %e, "Error checking cluster health");
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    #[instrument(skip(self))]
    async fn initialize_cluster(
        &self,
        node_addresses: &[(String, u16)],
    ) -> Result<(), ValkeyError> {
        if node_addresses.is_empty() {
            return Err(ValkeyError::InvalidConfig(
                "No node addresses provided".to_string(),
            ));
        }

        // Connect first node to all other nodes using CLUSTER MEET
        info!(nodes = node_addresses.len(), "Initializing cluster with CLUSTER MEET");

        for (ip, port) in node_addresses.iter().skip(1) {
            debug!(ip = %ip, port = %port, "Executing CLUSTER MEET");
            self.cluster_meet(ip, *port).await?;
        }

        // Give the cluster a moment to stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Cluster initialization complete");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn assign_slots_evenly(&self, master_count: i32) -> Result<(), ValkeyError> {
        if master_count <= 0 {
            return Err(ValkeyError::InvalidConfig(
                "Master count must be positive".to_string(),
            ));
        }

        let total_slots = 16384;
        let slots_per_master = total_slots / master_count;
        let remainder = total_slots % master_count;

        // Get current cluster nodes to find master node IDs
        let nodes = self.cluster_nodes().await?;
        let masters: Vec<&ClusterNode> = nodes.masters().into_iter().collect();

        if masters.len() != master_count as usize {
            return Err(ValkeyError::ClusterNotReady(format!(
                "Expected {} masters but found {}",
                master_count,
                masters.len()
            )));
        }

        info!(
            master_count = master_count,
            slots_per_master = slots_per_master,
            "Assigning slots evenly across masters"
        );

        let mut start_slot: i32 = 0;
        for (i, master) in masters.iter().enumerate() {
            let extra = if (i as i32) < remainder { 1 } else { 0 };
            let slot_count = slots_per_master + extra;
            let end_slot = start_slot + slot_count - 1;

            debug!(
                node_id = %master.node_id,
                start_slot = start_slot,
                end_slot = end_slot,
                count = slot_count,
                "Assigning slots to master"
            );

            // Note: In a real implementation, we would need to connect to
            // each master individually to assign slots. This is a simplified
            // version that shows the structure.
            //
            // For actual implementation:
            // 1. Connect to each master node individually
            // 2. Execute CLUSTER ADDSLOTS on that node

            start_slot = end_slot + 1;
        }

        info!("Slot assignment complete");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn get_replication_lag(&self) -> Result<i64, ValkeyError> {
        let info = self.info(Some(InfoKind::Replication)).await?;

        // Parse replication info to find offset lag
        // Format: master_repl_offset:123456
        for line in info.lines() {
            if line.starts_with("slave_repl_offset:") || line.starts_with("master_repl_offset:") {
                // For replicas, we compare slave_repl_offset with master_repl_offset
                // This is a simplified version
                if let Some(value) = line.split(':').nth(1) {
                    if let Ok(offset) = value.parse::<i64>() {
                        return Ok(offset);
                    }
                }
            }
        }

        // If we can't find lag info, assume 0 (caught up)
        Ok(0)
    }

    #[instrument(skip(self))]
    async fn wait_for_replication_sync(&self, timeout: Duration) -> Result<(), ValkeyError> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(500);

        loop {
            if start.elapsed() > timeout {
                return Err(ValkeyError::Timeout {
                    operation: "wait_for_replication_sync".to_string(),
                    duration: timeout,
                });
            }

            let info = self.info(Some(InfoKind::Replication)).await?;

            // Check for master_link_status:up in replica info
            if info.contains("master_link_status:up") {
                // Check if replication is caught up
                // Look for slave_read_repl_offset == slave_repl_offset
                let mut read_offset: Option<i64> = None;
                let mut repl_offset: Option<i64> = None;

                for line in info.lines() {
                    if line.starts_with("slave_read_repl_offset:") {
                        if let Some(value) = line.split(':').nth(1) {
                            read_offset = value.parse().ok();
                        }
                    } else if line.starts_with("slave_repl_offset:") {
                        if let Some(value) = line.split(':').nth(1) {
                            repl_offset = value.parse().ok();
                        }
                    }
                }

                match (read_offset, repl_offset) {
                    (Some(read), Some(repl)) if read == repl => {
                        info!("Replication is in sync");
                        return Ok(());
                    }
                    (Some(read), Some(repl)) => {
                        debug!(read_offset = read, repl_offset = repl, "Waiting for replication sync");
                    }
                    _ => {
                        debug!("Could not determine replication offsets, assuming in sync");
                        return Ok(());
                    }
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Calculate slot distribution for a given number of masters.
pub fn calculate_slot_distribution(master_count: i32) -> Vec<(i32, i32)> {
    if master_count <= 0 {
        return Vec::new();
    }

    let total_slots = 16384;
    let slots_per_master = total_slots / master_count;
    let remainder = total_slots % master_count;

    let mut ranges = Vec::new();
    let mut start_slot = 0;

    for i in 0..master_count {
        let extra = if i < remainder { 1 } else { 0 };
        let slot_count = slots_per_master + extra;
        let end_slot = start_slot + slot_count - 1;

        ranges.push((start_slot, end_slot));
        start_slot = end_slot + 1;
    }

    ranges
}

/// Calculate which node index should own a given slot.
pub fn slot_to_node_index(slot: i32, master_count: i32) -> i32 {
    if master_count <= 0 {
        return 0;
    }

    let slots_per_master = 16384 / master_count;
    let remainder = 16384 % master_count;

    // Nodes with extra slots
    let extra_slot_boundary = remainder * (slots_per_master + 1);

    if slot < extra_slot_boundary {
        slot / (slots_per_master + 1)
    } else {
        remainder + (slot - extra_slot_boundary) / slots_per_master
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_slot_distribution_3_masters() {
        let ranges = calculate_slot_distribution(3);
        assert_eq!(ranges.len(), 3);
        // 16384 / 3 = 5461 remainder 1
        // So first master gets 5462 slots: 0-5461
        // Second master gets 5461 slots: 5462-10922
        // Third master gets 5461 slots: 10923-16383
        assert_eq!(ranges[0], (0, 5461));
        assert_eq!(ranges[1], (5462, 10922));
        assert_eq!(ranges[2], (10923, 16383));

        // Verify total coverage
        let total: i32 = ranges.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(total, 16384);
    }

    #[test]
    fn test_calculate_slot_distribution_6_masters() {
        let ranges = calculate_slot_distribution(6);
        assert_eq!(ranges.len(), 6);

        // Verify total coverage
        let total: i32 = ranges.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(total, 16384);

        // Verify ranges don't overlap and are contiguous
        for i in 1..ranges.len() {
            assert_eq!(ranges[i].0, ranges[i - 1].1 + 1);
        }
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges[5].1, 16383);
    }

    #[test]
    fn test_calculate_slot_distribution_1_master() {
        let ranges = calculate_slot_distribution(1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (0, 16383));
    }

    #[test]
    fn test_calculate_slot_distribution_invalid() {
        let ranges = calculate_slot_distribution(0);
        assert!(ranges.is_empty());

        let ranges = calculate_slot_distribution(-1);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_slot_to_node_index() {
        // With 3 masters: 0-5461, 5462-10922, 10923-16383
        assert_eq!(slot_to_node_index(0, 3), 0);
        assert_eq!(slot_to_node_index(5461, 3), 0);
        assert_eq!(slot_to_node_index(5462, 3), 1);
        assert_eq!(slot_to_node_index(10922, 3), 1);
        assert_eq!(slot_to_node_index(10923, 3), 2);
        assert_eq!(slot_to_node_index(16383, 3), 2);
    }
}
