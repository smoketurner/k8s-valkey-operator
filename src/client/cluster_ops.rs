//! High-level cluster operations for Valkey cluster management.
//!
//! This module provides high-level operations that combine multiple
//! low-level Valkey commands to perform cluster management tasks.

use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use fred::types::InfoKind;

use super::types::{ClusterInfo, ParsedClusterNodes};
use super::valkey_client::{ValkeyClient, ValkeyError};

/// High-level cluster operations for ValkeyClient.
impl ValkeyClient {
    /// Get parsed cluster info.
    #[instrument(skip(self))]
    pub async fn cluster_info(&self) -> Result<ClusterInfo, ValkeyError> {
        let raw = self.cluster_info_raw().await?;
        let info = ClusterInfo::parse(&raw)?;
        Ok(info)
    }

    /// Get parsed cluster nodes.
    #[instrument(skip(self))]
    pub async fn cluster_nodes(&self) -> Result<ParsedClusterNodes, ValkeyError> {
        let raw = self.cluster_nodes_raw().await?;
        let nodes = ParsedClusterNodes::parse(&raw)?;
        Ok(nodes)
    }

    /// Check if the cluster is healthy (state=ok, all slots assigned).
    #[instrument(skip(self))]
    pub async fn is_cluster_healthy(&self) -> Result<bool, ValkeyError> {
        let info = self.cluster_info().await?;
        Ok(info.is_healthy())
    }

    /// Wait for the cluster to become healthy.
    #[instrument(skip(self))]
    pub async fn wait_for_cluster_healthy(
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

    /// Initialize cluster by connecting all nodes via CLUSTER MEET.
    #[instrument(skip(self))]
    pub async fn initialize_cluster(
        &self,
        node_addresses: &[(String, u16)],
    ) -> Result<(), ValkeyError> {
        if node_addresses.is_empty() {
            return Err(ValkeyError::InvalidConfig(
                "No node addresses provided".to_string(),
            ));
        }

        // Connect first node to all other nodes using CLUSTER MEET
        info!(
            nodes = node_addresses.len(),
            "Initializing cluster with CLUSTER MEET"
        );

        for (ip, port) in node_addresses.iter().skip(1) {
            debug!(ip = %ip, port = %port, "Executing CLUSTER MEET");
            self.cluster_meet(ip, *port).await?;
        }

        // Give the cluster a moment to stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;

        info!("Cluster initialization complete");
        Ok(())
    }

    /// Get the replication lag for a replica from INFO REPLICATION.
    ///
    /// TODO: This is a simplified implementation that returns offset values
    /// but doesn't compute actual byte lag between master and replica.
    /// A proper implementation would compare master_repl_offset from the master
    /// with slave_repl_offset from the replica.
    #[instrument(skip(self))]
    pub async fn get_replication_lag(&self) -> Result<i64, ValkeyError> {
        let info = self.info(Some(InfoKind::Replication)).await?;

        // Parse replication info to find offset lag
        // Format: master_repl_offset:123456
        for line in info.lines() {
            if line.starts_with("slave_repl_offset:") || line.starts_with("master_repl_offset:") {
                // For replicas, we compare slave_repl_offset with master_repl_offset
                // This is a simplified version
                if let Some(value) = line.split(':').nth(1)
                    && let Ok(offset) = value.parse::<i64>()
                {
                    return Ok(offset);
                }
            }
        }

        // If we can't find lag info, assume 0 (caught up)
        Ok(0)
    }

    /// Wait for replication to catch up.
    #[instrument(skip(self))]
    pub async fn wait_for_replication_sync(&self, timeout: Duration) -> Result<(), ValkeyError> {
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
                    if line.starts_with("slave_read_repl_offset:")
                        && let Some(value) = line.split(':').nth(1)
                    {
                        read_offset = value.parse().ok();
                    } else if line.starts_with("slave_repl_offset:")
                        && let Some(value) = line.split(':').nth(1)
                    {
                        repl_offset = value.parse().ok();
                    }
                }

                match (read_offset, repl_offset) {
                    (Some(read), Some(repl)) if read == repl => {
                        info!("Replication is in sync");
                        return Ok(());
                    }
                    (Some(read), Some(repl)) => {
                        debug!(
                            read_offset = read,
                            repl_offset = repl,
                            "Waiting for replication sync"
                        );
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
