//! Types for parsed Valkey cluster information.
//!
//! These types represent the parsed output of various Valkey cluster commands
//! like `CLUSTER NODES` and `CLUSTER INFO`.

use std::collections::HashMap;
use std::str::FromStr;

use thiserror::Error;

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
pub enum ClusterState {
    /// Cluster is healthy and serving requests.
    Ok,
    /// Cluster is in a failed state.
    Fail,
}

impl FromStr for ClusterState {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "ok" => Ok(ClusterState::Ok),
            "fail" => Ok(ClusterState::Fail),
            _ => Err(ParseError::InvalidClusterInfo(format!(
                "Unknown cluster state: {}",
                s
            ))),
        }
    }
}

impl std::fmt::Display for ClusterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterState::Ok => write!(f, "ok"),
            ClusterState::Fail => write!(f, "fail"),
        }
    }
}

/// Parsed output of `CLUSTER INFO` command.
#[derive(Debug, Clone)]
pub struct ClusterInfo {
    /// Current state of the cluster.
    pub state: ClusterState,
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
        self.slots_assigned == 16384
    }

    /// Check if the cluster is healthy.
    pub fn is_healthy(&self) -> bool {
        self.state == ClusterState::Ok && self.slots_fail == 0 && self.slots_pfail == 0
    }
}

/// Role of a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Node is a master serving hash slots.
    Master,
    /// Node is a replica of a master.
    Replica,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Master => write!(f, "master"),
            NodeRole::Replica => write!(f, "slave"),
        }
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
        } else {
            NodeRole::Replica
        }
    }

    /// Check if the node is healthy (not in fail or pfail state).
    pub fn is_healthy(&self) -> bool {
        !self.fail && !self.pfail && !self.handshake && !self.noaddr
    }
}

/// A hash slot range owned by a master node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotRange {
    /// Start of the slot range (inclusive).
    pub start: i32,
    /// End of the slot range (inclusive).
    pub end: i32,
}

impl SlotRange {
    /// Create a new slot range.
    pub fn new(start: i32, end: i32) -> Self {
        Self { start, end }
    }

    /// Create a single-slot range.
    pub fn single(slot: i32) -> Self {
        Self {
            start: slot,
            end: slot,
        }
    }

    /// Get the number of slots in this range.
    pub fn count(&self) -> i32 {
        self.end - self.start + 1
    }

    /// Parse a slot range from string (e.g., "0-5460" or "5461").
    pub fn parse(s: &str) -> Result<Self, ParseError> {
        let s = s.trim();

        // Check for importing/migrating state markers
        if s.starts_with('[') {
            // [slot-<-importing-node-id] or [slot->-migrating-node-id]
            // Skip these for now
            return Err(ParseError::InvalidSlotRange(format!(
                "Slot in migration: {}",
                s
            )));
        }

        if let Some((start_str, end_str)) = s.split_once('-') {
            let start = start_str.parse().map_err(|_| {
                ParseError::InvalidSlotRange(format!("Invalid start slot: {}", start_str))
            })?;
            let end = end_str.parse().map_err(|_| {
                ParseError::InvalidSlotRange(format!("Invalid end slot: {}", end_str))
            })?;
            Ok(SlotRange::new(start, end))
        } else {
            let slot = s
                .parse()
                .map_err(|_| ParseError::InvalidSlotRange(format!("Invalid slot: {}", s)))?;
            Ok(SlotRange::single(slot))
        }
    }
}

impl std::fmt::Display for SlotRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

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
        self.slots.iter().map(|r| r.count()).sum()
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

        let node_id = parts[0].to_string();

        // Parse address: ip:port@cport or ip:port
        let addr_parts: Vec<&str> = parts[1].split('@').collect();
        let ip_port = addr_parts[0];
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

        let cluster_bus_port = if addr_parts.len() > 1 {
            addr_parts[1]
                .split(',')
                .next()
                .unwrap_or("")
                .parse()
                .unwrap_or(port + 10000)
        } else {
            port + 10000
        };

        let flags = NodeFlags::parse(parts[2]);

        let master_id = if parts[3] == "-" {
            None
        } else {
            Some(parts[3].to_string())
        };

        let ping_sent = parts[4].parse().unwrap_or(0);
        let pong_recv = parts[5].parse().unwrap_or(0);
        let config_epoch = parts[6].parse().unwrap_or(0);
        let link_state = parts[7].to_string();

        // Parse slot ranges (remaining parts for master nodes)
        let slots: Vec<SlotRange> = parts[8..]
            .iter()
            .filter_map(|s| SlotRange::parse(s).ok())
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
        self.total_slots_assigned() == 16384
    }

    /// Get all healthy nodes.
    pub fn healthy_nodes(&self) -> Vec<&ClusterNode> {
        self.nodes.iter().filter(|n| n.is_healthy()).collect()
    }
}

#[cfg(test)]
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
        assert_eq!(parsed.state, ClusterState::Ok);
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
        assert_eq!(parsed.state, ClusterState::Fail);
        assert!(!parsed.all_slots_assigned());
        assert!(!parsed.is_healthy());
    }

    #[test]
    fn test_parse_slot_range() {
        assert_eq!(SlotRange::parse("0-5460").unwrap(), SlotRange::new(0, 5460));
        assert_eq!(SlotRange::parse("5461").unwrap(), SlotRange::single(5461));
        assert_eq!(
            SlotRange::parse("10923-16383").unwrap(),
            SlotRange::new(10923, 16383)
        );
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
        assert_eq!(node.slots[0], SlotRange::new(5461, 10922));
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
