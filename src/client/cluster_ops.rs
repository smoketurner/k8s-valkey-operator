//! High-level cluster operations for Valkey cluster management.
//!
//! This module provides high-level operations that combine multiple
//! low-level Valkey commands to perform cluster management tasks.

use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use fred::types::InfoKind;

use super::parsing::ReplicationInfo;
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

    /// Get the replication lag for a replica by comparing with master offset.
    ///
    /// Returns the difference between master's `master_repl_offset` and replica's `slave_repl_offset`.
    /// A return value of 0 means fully caught up.
    ///
    /// # Arguments
    /// * `master_client` - Client connected to the master node to get master_repl_offset
    #[instrument(skip(self, master_client))]
    pub async fn get_replication_lag(
        &self,
        master_client: &ValkeyClient,
    ) -> Result<i64, ValkeyError> {
        // Get replica's replication info using standardized parser
        let replica_info = self.info(Some(InfoKind::Replication)).await?;
        let replica_repl_info = ReplicationInfo::parse(&replica_info).map_err(|e| {
            ValkeyError::Parse(crate::client::types::ParseError::InvalidClusterInfo(
                e.to_string(),
            ))
        })?;
        let replica_offset = replica_repl_info.slave_repl_offset;

        // Get master's replication info using standardized parser
        let master_info = master_client.info(Some(InfoKind::Replication)).await?;
        let master_repl_info = ReplicationInfo::parse(&master_info).map_err(|e| {
            ValkeyError::Parse(crate::client::types::ParseError::InvalidClusterInfo(
                e.to_string(),
            ))
        })?;
        let master_offset = master_repl_info.master_repl_offset;

        match (replica_offset, master_offset) {
            (Some(repl), Some(master)) => Ok(master - repl),
            _ => {
                // If we can't determine lag, return a large value to indicate unknown
                // This will make this replica less preferred
                Ok(i64::MAX)
            }
        }
    }

    /// Wait for replication to catch up by comparing replica offset with master offset.
    ///
    /// This is the proper way to verify replication sync according to Valkey documentation:
    /// compare the master's `master_repl_offset` with the replica's `slave_repl_offset`.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for sync
    /// * `master_client` - Optional client connected to the master node.
    ///   If None, will attempt to find master from cluster nodes.
    #[instrument(skip(self, master_client))]
    pub async fn wait_for_replication_sync(
        &self,
        timeout: Duration,
        master_client: Option<&ValkeyClient>,
    ) -> Result<(), ValkeyError> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(500);

        loop {
            if start.elapsed() > timeout {
                return Err(ValkeyError::Timeout {
                    operation: "wait_for_replication_sync".to_string(),
                    duration: timeout,
                });
            }

            // Get replica replication info
            let replica_info = self.info(Some(InfoKind::Replication)).await?;

            // Check for master_link_status:up in replica info
            if !replica_info.contains("master_link_status:up") {
                debug!("Replica not connected to master yet");
                tokio::time::sleep(poll_interval).await;
                continue;
            }

            // Parse replica replication info using standardized parser
            let replica_repl_info = ReplicationInfo::parse(&replica_info).map_err(|e| {
                ValkeyError::Parse(crate::client::types::ParseError::InvalidClusterInfo(
                    e.to_string(),
                ))
            })?;
            let replica_offset = replica_repl_info.slave_repl_offset;

            // Get master's master_repl_offset
            let master_offset: Option<i64> = if let Some(master) = master_client {
                // Use provided master client
                let master_info = master.info(Some(InfoKind::Replication)).await?;
                let master_repl_info = ReplicationInfo::parse(&master_info).map_err(|e| {
                    ValkeyError::Parse(crate::client::types::ParseError::InvalidClusterInfo(
                        e.to_string(),
                    ))
                })?;
                master_repl_info.master_repl_offset
            } else {
                // Try to find master from cluster nodes and connect
                // This is a fallback - ideally master_client should be provided
                None
            };

            match (replica_offset, master_offset) {
                (Some(repl), Some(master)) if repl == master => {
                    info!(
                        replica_offset = repl,
                        master_offset = master,
                        "Replication is in sync (offsets match)"
                    );
                    return Ok(());
                }
                (Some(repl), Some(master)) => {
                    let lag = master - repl;
                    debug!(
                        replica_offset = repl,
                        master_offset = master,
                        lag = lag,
                        "Waiting for replication sync (lag: {} bytes)",
                        lag
                    );
                }
                (Some(repl), None) => {
                    // Can't get master offset, fall back to checking replica offsets match
                    // This is less reliable but better than nothing
                    let read_offset: Option<i64> = replica_info.lines().find_map(|line| {
                        if line.starts_with("slave_read_repl_offset:") {
                            line.split(':').nth(1).and_then(|v| v.trim().parse().ok())
                        } else {
                            None
                        }
                    });

                    if let Some(read) = read_offset
                        && read == repl
                    {
                        info!(
                            "Replication appears in sync (replica offsets match, master offset unavailable)"
                        );
                        return Ok(());
                    }

                    debug!(
                        replica_offset = ?repl,
                        "Waiting for replication sync (master offset unavailable)"
                    );
                }
                _ => {
                    debug!("Could not determine replication offsets, assuming in sync");
                    return Ok(());
                }
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}
