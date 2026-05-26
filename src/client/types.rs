//! Types for parsed Valkey cluster information.
//!
//! These types represent the parsed output of various Valkey cluster commands
//! like `CLUSTER NODES` and `CLUSTER INFO`.

use std::collections::HashMap;
use std::str::FromStr;

use thiserror::Error;

use crate::slots::distribution::TOTAL_SLOTS;

/// Errors that can occur when parsing cluster data.
#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Invalid cluster info format: {0}")]
    InvalidClusterInfo(String),
    #[error("Invalid cluster nodes format: {0}")]
    InvalidClusterNodes(String),
    #[error("Invalid slot range: {0}")]
    InvalidSlotRange(String),
    #[error("Invalid node flags: {0}")]
    InvalidNodeFlags(String),
    #[error("Missing required field: {0}")]
    MissingField(String),
}

/// State of the Valkey cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealthState {
    /// Cluster is healthy and serving requests.
    Ok,
    /// Cluster is in a failed state.
    Fail,
}

impl FromStr for ClusterHealthState {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "ok" => Ok(ClusterHealthState::Ok),
            "fail" => Ok(ClusterHealthState::Fail),
            _ => Err(ParseError::InvalidClusterInfo(format!(
                "Unknown cluster state: {}",
                s
            ))),
        }
    }
}

impl std::fmt::Display for ClusterHealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterHealthState::Ok => write!(f, "ok"),
            ClusterHealthState::Fail => write!(f, "fail"),
        }
    }
}

/// Parsed output of `CLUSTER INFO` command.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// Current state of the cluster.
    pub state: ClusterHealthState,
    /// Number of hash slots assigned to this node.
    pub slots_assigned: i32,
    /// Number of hash slots that are OK (served by nodes).
    pub slots_ok: i32,
    /// Number of hash slots in PFAIL state.
    pub slots_pfail: i32,
    /// Number of hash slots in FAIL state.
    pub slots_fail: i32,
    /// Total number of known nodes in the cluster.
    pub known_nodes: i32,
    /// Size of the cluster (number of master nodes serving at least one slot).
    pub cluster_size: i32,
    /// Current cluster epoch.
    pub current_epoch: i64,
    /// Epoch of this node.
    pub my_epoch: i64,
    /// Total number of pending messages in the cluster bus.
    pub messages_sent: i64,
    /// Total number of messages received via the cluster bus.
    pub messages_received: i64,
    /// All raw key-value pairs from the info output.
    pub raw: HashMap<String, String>,
}

impl ClusterInfo {
    /// Parse the output of `CLUSTER INFO` command.
    pub fn parse(info: &str) -> Result<Self, ParseError> {
        let mut raw = HashMap::new();

        for line in info.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once(':') {
                raw.insert(key.to_string(), value.to_string());
            }
        }

        let state = raw
            .get("cluster_state")
            .ok_or_else(|| ParseError::MissingField("cluster_state".to_string()))?
            .parse()?;

        let get_i32 = |key: &str| -> Result<i32, ParseError> {
            raw.get(key)
                .ok_or_else(|| ParseError::MissingField(key.to_string()))?
                .parse()
                .map_err(|_| ParseError::InvalidClusterInfo(format!("Invalid {} value", key)))
        };

        let get_i64 = |key: &str| -> Result<i64, ParseError> {
            raw.get(key)
                .ok_or_else(|| ParseError::MissingField(key.to_string()))?
                .parse()
                .map_err(|_| ParseError::InvalidClusterInfo(format!("Invalid {} value", key)))
        };

        Ok(ClusterInfo {
            state,
            slots_assigned: get_i32("cluster_slots_assigned")?,
            slots_ok: get_i32("cluster_slots_ok")?,
            slots_pfail: get_i32("cluster_slots_pfail")?,
            slots_fail: get_i32("cluster_slots_fail")?,
            known_nodes: get_i32("cluster_known_nodes")?,
            cluster_size: get_i32("cluster_size")?,
            current_epoch: get_i64("cluster_current_epoch")?,
            my_epoch: get_i64("cluster_my_epoch")?,
            messages_sent: get_i64("cluster_stats_messages_sent").unwrap_or(0),
            messages_received: get_i64("cluster_stats_messages_received").unwrap_or(0),
            raw,
        })
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        self.slots_assigned == i32::from(TOTAL_SLOTS)
    }

    /// Check if the cluster is healthy.
    ///
    /// Requires full slot coverage in addition to `cluster_state:ok`. Because
    /// the operator runs with `--cluster-require-full-coverage no`, Valkey
    /// will report `cluster_state:ok` even when slots are unassigned; without
    /// the explicit `all_slots_assigned()` check the operator would mark the
    /// cluster Running while clients receive CLUSTERDOWN errors on the
    /// missing slots.
    pub fn is_healthy(&self) -> bool {
        self.state == ClusterHealthState::Ok
            && self.all_slots_assigned()
            && self.slots_fail == 0
            && self.slots_pfail == 0
    }
}

/// Role of a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeRole {
    /// Node is a master serving hash slots.
    Master,
    /// Node is a replica of a master.
    Replica,
    /// Node state is unknown (pod exists but not yet in cluster).
    #[default]
    Unknown,
}

impl NodeRole {
    /// Returns the role string as used in the Valkey wire protocol.
    /// Masters are "master", replicas are "slave" (legacy Redis/Valkey protocol name).
    pub fn as_valkey_str(&self) -> &'static str {
        match self {
            NodeRole::Master => "master",
            NodeRole::Replica => "slave",
            NodeRole::Unknown => "?",
        }
    }

    /// Returns the human-readable display string.
    pub(crate) fn as_display_str(&self) -> &'static str {
        match self {
            NodeRole::Master => "master",
            NodeRole::Replica => "replica",
            NodeRole::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_display_str())
    }
}

/// Flags indicating the state of a cluster node.
#[derive(Debug, Clone, Default)]
pub struct NodeFlags {
    /// This is the local node (myself).
    pub myself: bool,
    /// Node is a master.
    pub master: bool,
    /// Node is a replica.
    pub slave: bool,
    /// Node is in PFAIL state (potentially failed).
    pub pfail: bool,
    /// Node is in FAIL state.
    pub fail: bool,
    /// Node is in handshake state.
    pub handshake: bool,
    /// Node has no address yet.
    pub noaddr: bool,
    /// Node has no flags set.
    pub noflags: bool,
}

impl NodeFlags {
    /// Parse flags from the CLUSTER NODES output.
    pub fn parse(flags_str: &str) -> Self {
        let mut flags = NodeFlags::default();
        for flag in flags_str.split(',') {
            match flag.trim() {
                "myself" => flags.myself = true,
                "master" => flags.master = true,
                "slave" => flags.slave = true,
                "pfail" => flags.pfail = true,
                "fail" => flags.fail = true,
                "handshake" => flags.handshake = true,
                "noaddr" => flags.noaddr = true,
                "noflags" => flags.noflags = true,
                _ => {}
            }
        }
        flags
    }

    /// Get the role based on flags.
    pub fn role(&self) -> NodeRole {
        if self.master {
            NodeRole::Master
        } else if self.slave {
            NodeRole::Replica
        } else {
            NodeRole::Unknown
        }
    }

    /// Check if the node is healthy (not in fail or pfail state).
    pub fn is_healthy(&self) -> bool {
        !self.fail && !self.pfail && !self.handshake && !self.noaddr
    }
}

/// A hash slot range owned by a master node.
///
/// Re-exported from [`crate::slots::distribution::SlotRange`]. All code should
/// use this canonical type; the fields are `u16` because slots are 0..=16383.
pub use crate::slots::distribution::SlotRange;

/// A cluster node as reported by `CLUSTER NODES`.
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique node ID (40 hex characters).
    pub node_id: String,
    /// IP address and client port.
    pub address: String,
    /// IP address only.
    pub ip: String,
    /// Client port.
    pub port: i32,
    /// Cluster bus port.
    pub cluster_bus_port: i32,
    /// Node flags.
    pub flags: NodeFlags,
    /// Master node ID if this is a replica, "-" otherwise.
    pub master_id: Option<String>,
    /// Ping sent timestamp.
    pub ping_sent: i64,
    /// Pong received timestamp.
    pub pong_recv: i64,
    /// Config epoch.
    pub config_epoch: i64,
    /// Link state ("connected" or "disconnected").
    pub link_state: String,
    /// Slot ranges owned by this node (masters only).
    pub slots: Vec<SlotRange>,
}

impl ClusterNode {
    /// Get the role of this node.
    pub fn role(&self) -> NodeRole {
        self.flags.role()
    }

    /// Check if this is a master node.
    pub fn is_master(&self) -> bool {
        self.flags.master
    }

    /// Check if this is a replica node.
    pub fn is_replica(&self) -> bool {
        self.flags.slave
    }

    /// Check if this is the local node.
    pub fn is_myself(&self) -> bool {
        self.flags.myself
    }

    /// Check if the node is connected.
    pub fn is_connected(&self) -> bool {
        self.link_state == "connected"
    }

    /// Check if the node is healthy.
    pub fn is_healthy(&self) -> bool {
        self.flags.is_healthy() && self.is_connected()
    }

    /// Get total number of slots owned by this node.
    pub fn slot_count(&self) -> i32 {
        self.slots.iter().map(|r| i32::from(r.count())).sum()
    }

    /// Parse a single line from `CLUSTER NODES` output.
    pub fn parse_line(line: &str) -> Result<Self, ParseError> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            return Err(ParseError::InvalidClusterNodes(format!(
                "Not enough fields in line: {}",
                line
            )));
        }

        let node_id = parts
            .first()
            .ok_or_else(|| ParseError::MissingField("node_id".to_string()))?
            .to_string();

        // Parse address: ip:port@cport or ip:port
        let addr_field = parts
            .get(1)
            .ok_or_else(|| ParseError::MissingField("address".to_string()))?;
        let addr_parts: Vec<&str> = addr_field.split('@').collect();
        let ip_port = addr_parts
            .first()
            .ok_or_else(|| ParseError::MissingField("ip:port".to_string()))?;
        let (ip, port) = if let Some((ip, port_str)) = ip_port.rsplit_once(':') {
            let port = port_str.parse().map_err(|_| {
                ParseError::InvalidClusterNodes(format!("Invalid port: {}", port_str))
            })?;
            (ip.to_string(), port)
        } else {
            return Err(ParseError::InvalidClusterNodes(format!(
                "Invalid address format: {}",
                ip_port
            )));
        };

        let cluster_bus_port = addr_parts
            .get(1)
            .map(|s| {
                s.split(',')
                    .next()
                    .unwrap_or("")
                    .parse()
                    .unwrap_or(port + 10000)
            })
            .unwrap_or(port + 10000);

        let flags_str = parts
            .get(2)
            .ok_or_else(|| ParseError::MissingField("flags".to_string()))?;
        let flags = NodeFlags::parse(flags_str);

        let master_field = parts
            .get(3)
            .ok_or_else(|| ParseError::MissingField("master_id".to_string()))?;
        let master_id = if *master_field == "-" {
            None
        } else {
            Some(master_field.to_string())
        };

        let ping_sent = parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
        let pong_recv = parts.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
        let config_epoch = parts.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
        let link_state = parts
            .get(7)
            .ok_or_else(|| ParseError::MissingField("link_state".to_string()))?
            .to_string();

        // Parse slot ranges (remaining parts for master nodes)
        let slots: Vec<SlotRange> = parts
            .get(8..)
            .unwrap_or_default()
            .iter()
            .filter_map(|s| SlotRange::parse_valkey(s))
            .collect();

        Ok(ClusterNode {
            node_id,
            address: ip_port.to_string(),
            ip,
            port,
            cluster_bus_port,
            flags,
            master_id,
            ping_sent,
            pong_recv,
            config_epoch,
            link_state,
            slots,
        })
    }
}

/// Parsed output of `CLUSTER NODES` command.
#[derive(Debug, Clone)]
pub struct ParsedClusterNodes {
    /// All nodes in the cluster.
    pub nodes: Vec<ClusterNode>,
}

impl ParsedClusterNodes {
    /// Parse the output of `CLUSTER NODES` command.
    pub fn parse(output: &str) -> Result<Self, ParseError> {
        let nodes: Vec<ClusterNode> = output
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(ClusterNode::parse_line)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(ParsedClusterNodes { nodes })
    }

    /// Get all master nodes.
    pub fn masters(&self) -> Vec<&ClusterNode> {
        self.nodes.iter().filter(|n| n.is_master()).collect()
    }

    /// Get all replica nodes.
    pub fn replicas(&self) -> Vec<&ClusterNode> {
        self.nodes.iter().filter(|n| n.is_replica()).collect()
    }

    /// Get the local node (marked with "myself" flag).
    pub fn myself(&self) -> Option<&ClusterNode> {
        self.nodes.iter().find(|n| n.is_myself())
    }

    /// Get replicas of a specific master.
    pub fn replicas_of(&self, master_id: &str) -> Vec<&ClusterNode> {
        self.nodes
            .iter()
            .filter(|n| n.master_id.as_deref() == Some(master_id))
            .collect()
    }

    /// Get a node by its ID.
    pub fn get_node(&self, node_id: &str) -> Option<&ClusterNode> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }

    /// Get total number of slots assigned.
    pub fn total_slots_assigned(&self) -> i32 {
        self.masters().iter().map(|m| m.slot_count()).sum()
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        self.total_slots_assigned() == i32::from(TOTAL_SLOTS)
    }

    /// Get all healthy nodes.
    pub fn healthy_nodes(&self) -> Vec<&ClusterNode> {
        self.nodes.iter().filter(|n| n.is_healthy()).collect()
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
    fn test_parse_cluster_info() {
        let info = r#"
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:6
cluster_size:3
cluster_current_epoch:6
cluster_my_epoch:2
"#;

        let parsed = ClusterInfo::parse(info).expect("should parse");
        assert_eq!(parsed.state, ClusterHealthState::Ok);
        assert_eq!(parsed.slots_assigned, 16384);
        assert_eq!(parsed.slots_ok, 16384);
        assert_eq!(parsed.slots_pfail, 0);
        assert_eq!(parsed.slots_fail, 0);
        assert_eq!(parsed.known_nodes, 6);
        assert_eq!(parsed.cluster_size, 3);
        assert!(parsed.all_slots_assigned());
        assert!(parsed.is_healthy());
    }

    #[test]
    fn test_parse_cluster_info_fail() {
        let info = r#"
cluster_state:fail
cluster_slots_assigned:10922
cluster_slots_ok:10922
cluster_slots_pfail:0
cluster_slots_fail:5462
cluster_known_nodes:6
cluster_size:2
cluster_current_epoch:6
cluster_my_epoch:2
"#;

        let parsed = ClusterInfo::parse(info).expect("should parse");
        assert_eq!(parsed.state, ClusterHealthState::Fail);
        assert!(!parsed.all_slots_assigned());
        assert!(!parsed.is_healthy());
    }

    #[test]
    fn test_is_healthy_requires_full_slot_coverage() {
        // cluster_state:ok with partial slot coverage occurs when running with
        // --cluster-require-full-coverage no after a failed scale or rebalance.
        // is_healthy() must return false so the operator does not mark the
        // cluster Running while clients see CLUSTERDOWN errors on missing slots.
        let info = r#"
cluster_state:ok
cluster_slots_assigned:16000
cluster_slots_ok:16000
cluster_slots_pfail:0
cluster_slots_fail:0
cluster_known_nodes:6
cluster_size:3
cluster_current_epoch:6
cluster_my_epoch:2
"#;

        let parsed = ClusterInfo::parse(info).expect("should parse");
        assert_eq!(parsed.state, ClusterHealthState::Ok);
        assert_eq!(parsed.slots_fail, 0);
        assert_eq!(parsed.slots_pfail, 0);
        assert!(!parsed.all_slots_assigned());
        assert!(
            !parsed.is_healthy(),
            "cluster with missing slots must not be reported healthy"
        );
    }

    #[test]
    fn test_parse_slot_range() {
        assert_eq!(
            SlotRange::parse_valkey("0-5460").unwrap(),
            SlotRange::new(0, 5460)
        );
        assert_eq!(
            SlotRange::parse_valkey("5461").unwrap(),
            SlotRange::single(5461)
        );
        assert_eq!(
            SlotRange::parse_valkey("10923-16383").unwrap(),
            SlotRange::new(10923, 16383)
        );
        assert!(SlotRange::parse_valkey("[100-<-node]").is_none());
    }

    #[test]
    fn test_slot_range_count() {
        assert_eq!(SlotRange::new(0, 5460).count(), 5461);
        assert_eq!(SlotRange::single(100).count(), 1);
    }

    #[test]
    fn test_parse_node_flags() {
        let flags = NodeFlags::parse("myself,master");
        assert!(flags.myself);
        assert!(flags.master);
        assert!(!flags.slave);
        assert!(flags.is_healthy());

        let flags = NodeFlags::parse("slave");
        assert!(!flags.myself);
        assert!(!flags.master);
        assert!(flags.slave);
        assert!(flags.is_healthy());

        let flags = NodeFlags::parse("master,fail");
        assert!(flags.master);
        assert!(flags.fail);
        assert!(!flags.is_healthy());
    }

    #[test]
    fn test_parse_cluster_node() {
        let line = "07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:6379@16379 myself,master - 0 1426238317239 2 connected 5461-10922";

        let node = ClusterNode::parse_line(line).expect("should parse");
        assert_eq!(node.node_id, "07c37dfeb235213a872192d90877d0cd55635b91");
        assert_eq!(node.ip, "127.0.0.1");
        assert_eq!(node.port, 6379);
        assert_eq!(node.cluster_bus_port, 16379);
        assert!(node.is_master());
        assert!(node.is_myself());
        assert!(node.is_connected());
        assert_eq!(node.slots.len(), 1);
        assert_eq!(node.slots.first().unwrap(), &SlotRange::new(5461, 10922));
        assert_eq!(node.slot_count(), 5462);
    }

    #[test]
    fn test_parse_cluster_node_replica() {
        let line = "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:6380@16380 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 3 connected";

        let node = ClusterNode::parse_line(line).expect("should parse");
        assert!(node.is_replica());
        assert!(!node.is_master());
        assert!(!node.is_myself());
        assert_eq!(
            node.master_id,
            Some("67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1".to_string())
        );
        assert!(node.slots.is_empty());
    }

    #[test]
    fn test_parse_cluster_nodes() {
        let output = r#"07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:6379@16379 myself,master - 0 1426238317239 2 connected 5461-10922
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:6381@16381 master - 0 1426238316232 1 connected 0-5460
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:6382@16382 master - 0 1426238316232 3 connected 10923-16383
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:6380@16380 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 1 connected"#;

        let parsed = ParsedClusterNodes::parse(output).expect("should parse");
        assert_eq!(parsed.nodes.len(), 4);
        assert_eq!(parsed.masters().len(), 3);
        assert_eq!(parsed.replicas().len(), 1);
        assert!(parsed.myself().is_some());
        assert!(parsed.all_slots_assigned());
        assert_eq!(parsed.total_slots_assigned(), 16384);

        let replicas = parsed.replicas_of("67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1");
        assert_eq!(replicas.len(), 1);
    }
}
