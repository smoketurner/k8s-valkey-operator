//! Centralized cluster topology management.
//!
//! Provides [`ClusterTopology`] and [`ClusterNodeInfo`] for unified access to
//! pod-to-node mappings, eliminating scattered mapping logic across the codebase.
//!
//! # Overview
//!
//! This module correlates Kubernetes pod information with Valkey cluster node state,
//! providing a single source of truth for:
//! - IP → ordinal mapping
//! - ordinal → node_id mapping
//! - Node role identification (master/replica)
//! - Orphan detection
//! - Scale-up/scale-down candidate identification
//!
//! # Example
//!
//! ```ignore
//! let topology = ClusterTopology::build(
//!     &client,
//!     "default",
//!     "my-cluster",
//!     Some(&cluster_nodes),
//! ).await?;
//!
//! // Lookup by various keys
//! let node = topology.by_ordinal(0)?;
//! let node = topology.by_ip("10.0.0.1")?;
//!
//! // Get filtered views
//! for master in topology.masters() {
//!     println!("Master: {}", master.ordinal);
//! }
//! ```

use std::collections::HashMap;

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};

use crate::client::ValkeyError;
use crate::client::types::{ParsedClusterNodes, SlotRange as ClusterSlotRange};
use crate::crd::ValkeyClusterSpec;

// ============================================================================
// Utility Functions
// ============================================================================

/// Extract pod ordinal from a StatefulSet pod address.
///
/// Address formats supported:
/// - `my-cluster-0.my-cluster-headless.ns.svc.cluster.local:6379@16379`
/// - `my-cluster-0:6379`
/// - `my-cluster-0`
///
/// Returns the ordinal (e.g., `0` from `my-cluster-0`).
///
/// # Warning: Does Not Work for IP Addresses!
///
/// This function **returns incorrect values for IP addresses**. For example,
/// `10.0.0.5:6379` returns `Some(10)` (the first octet), not the pod ordinal.
///
/// For IP-based lookups, use [`ClusterTopology::by_ip`] which correlates
/// cluster node IPs with Kubernetes pod IPs to get the correct ordinal.
///
/// # Examples
///
/// ```
/// use valkey_operator::controller::cluster_topology::extract_ordinal_from_address;
///
/// // DNS names work correctly
/// assert_eq!(extract_ordinal_from_address("my-cluster-0:6379"), Some(0));
/// assert_eq!(extract_ordinal_from_address("my-cluster-5.my-cluster-headless.ns.svc:6379"), Some(5));
///
/// // IP addresses return WRONG values - use ClusterTopology::by_ip() instead!
/// assert_eq!(extract_ordinal_from_address("10.0.0.5:6379"), Some(10)); // Wrong!
/// ```
pub fn extract_ordinal_from_address(address: &str) -> Option<u16> {
    // Remove port suffix if present (handles both :6379 and :6379@16379)
    let hostname = address.split(':').next()?;
    // Get the pod name part (before first dot)
    let pod_name = hostname.split('.').next()?;
    // Extract ordinal from pod name (last part after last hyphen)
    let ordinal_str = pod_name.rsplit('-').next()?;
    ordinal_str.parse().ok()
}

// ============================================================================
// Types
// ============================================================================

/// Default Valkey client port.
pub const DEFAULT_VALKEY_PORT: u16 = 6379;

/// Default Valkey cluster bus port offset.
pub const CLUSTER_BUS_PORT_OFFSET: u16 = 10000;

/// A network endpoint (IP address and port).
///
/// This struct provides type-safe handling of IP/port pairs, eliminating
/// string manipulation errors and making the code more explicit about
/// what data is being passed around.
///
/// # Examples
///
/// ```
/// use valkey_operator::controller::cluster_topology::Endpoint;
///
/// let endpoint = Endpoint::new("10.0.0.5", 6379);
/// assert_eq!(endpoint.ip(), "10.0.0.5");
/// assert_eq!(endpoint.port(), 6379);
/// assert_eq!(endpoint.cluster_bus_port(), 16379);
/// assert_eq!(endpoint.to_string(), "10.0.0.5:6379");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    ip: String,
    port: u16,
}

impl Endpoint {
    /// Create a new endpoint with the given IP and port.
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self {
            ip: ip.into(),
            port,
        }
    }

    /// Create a new endpoint with the default Valkey port (6379).
    pub fn with_default_port(ip: impl Into<String>) -> Self {
        Self::new(ip, DEFAULT_VALKEY_PORT)
    }

    /// Get the IP address.
    pub fn ip(&self) -> &str {
        &self.ip
    }

    /// Get the port.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the cluster bus port (client port + 10000).
    pub fn cluster_bus_port(&self) -> u16 {
        self.port + CLUSTER_BUS_PORT_OFFSET
    }

    /// Check if this endpoint matches the given IP (port-agnostic comparison).
    pub fn has_ip(&self, ip: &str) -> bool {
        self.ip == ip
    }

    /// Parse an endpoint from an address string like "10.0.0.5:6379" or "10.0.0.5:6379@16379".
    pub fn parse(address: &str) -> Option<Self> {
        // Remove cluster bus port suffix if present (e.g., ":6379@16379" -> ":6379")
        let address = address.split('@').next()?;

        // Split into IP and port
        let (ip, port_str) = address.rsplit_once(':')?;
        let port = port_str.parse().ok()?;

        Some(Self::new(ip, port))
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}

impl From<(&str, u16)> for Endpoint {
    fn from((ip, port): (&str, u16)) -> Self {
        Self::new(ip, port)
    }
}

impl From<(String, u16)> for Endpoint {
    fn from((ip, port): (String, u16)) -> Self {
        Self::new(ip, port)
    }
}

/// Role of a cluster node from the topology perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeRole {
    /// Pod exists but not yet in cluster (unknown state).
    #[default]
    Unknown,
    /// Node is a master serving hash slots.
    Master,
    /// Node is a replica of a master.
    Replica,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Unknown => write!(f, "unknown"),
            NodeRole::Master => write!(f, "master"),
            NodeRole::Replica => write!(f, "replica"),
        }
    }
}

/// Information about a single node in the cluster.
///
/// Correlates Kubernetes pod information with Valkey cluster node state.
#[derive(Debug, Clone)]
pub struct ClusterNodeInfo {
    // === Kubernetes pod info (always present) ===
    /// StatefulSet pod name (e.g., "my-cluster-0").
    pub pod_name: String,

    /// Pod ordinal in the StatefulSet (e.g., 0, 1, 2).
    pub ordinal: i32,

    /// Pod endpoint (IP and port). None if pod not yet running.
    pub endpoint: Option<Endpoint>,

    /// Full DNS name for the pod.
    pub dns_name: String,

    /// Container image (for ValkeyUpgrade tracking).
    pub current_image: Option<String>,

    // === Valkey cluster info (may be None if not yet in cluster) ===
    /// Unique node ID (40 hex characters). None if not yet in cluster.
    pub node_id: Option<String>,

    /// Node role in the cluster.
    pub role: NodeRole,

    /// If replica, the master's node_id.
    pub master_id: Option<String>,

    /// Slot ranges owned by this node (masters only).
    pub slots: Vec<SlotRange>,

    /// Whether the node is connected to the cluster.
    pub is_connected: bool,

    /// Whether the node is in a failed state.
    pub is_failed: bool,
}

/// A hash slot range.
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

    /// Get the number of slots in this range.
    pub fn count(&self) -> i32 {
        self.end - self.start + 1
    }
}

impl From<&ClusterSlotRange> for SlotRange {
    fn from(r: &ClusterSlotRange) -> Self {
        Self {
            start: r.start,
            end: r.end,
        }
    }
}

impl ClusterNodeInfo {
    /// Check if this node is in the Valkey cluster.
    pub fn is_in_cluster(&self) -> bool {
        self.node_id.is_some()
    }

    /// Check if this node is a master with slots.
    pub fn is_master_with_slots(&self) -> bool {
        self.role == NodeRole::Master && !self.slots.is_empty()
    }

    /// Get total number of slots owned by this node.
    pub fn slot_count(&self) -> i32 {
        self.slots.iter().map(|r| r.count()).sum()
    }

    /// Check if the node is healthy.
    pub fn is_healthy(&self) -> bool {
        self.is_connected && !self.is_failed
    }

    /// Get the IP address if available.
    pub fn ip(&self) -> Option<&str> {
        self.endpoint.as_ref().map(|e| e.ip())
    }

    /// Get the port if available.
    pub fn port(&self) -> Option<u16> {
        self.endpoint.as_ref().map(|e| e.port())
    }

    /// Check if this node has the given IP address.
    pub fn has_ip(&self, ip: &str) -> bool {
        self.endpoint.as_ref().is_some_and(|e| e.has_ip(ip))
    }
}

/// Complete view of cluster nodes correlating K8s pods with Valkey state.
///
/// This struct provides a unified interface for querying cluster topology,
/// replacing scattered mapping logic throughout the codebase.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// All nodes in the cluster.
    nodes: Vec<ClusterNodeInfo>,
    /// Cluster name.
    cluster_name: String,
    /// Kubernetes namespace.
    namespace: String,

    // Pre-built indexes for fast lookups
    ordinal_index: HashMap<i32, usize>,
    ip_index: HashMap<String, usize>,
    node_id_index: HashMap<String, usize>,
}

impl ClusterTopology {
    /// Build topology from K8s pods and optional CLUSTER NODES output.
    ///
    /// This queries Kubernetes for all pods matching the cluster's label selector
    /// and correlates them with Valkey cluster state if provided.
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `namespace` - Kubernetes namespace
    /// * `cluster_name` - Name of the ValkeyCluster
    /// * `cluster_nodes` - Optional parsed CLUSTER NODES output
    ///
    /// # Returns
    /// A new `ClusterTopology` with all node information for pods that exist.
    pub async fn build(
        client: &Client,
        namespace: &str,
        cluster_name: &str,
        cluster_nodes: Option<&ParsedClusterNodes>,
    ) -> Result<Self, ValkeyError> {
        // Get pod information from Kubernetes
        let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
        let label_selector = format!("app.kubernetes.io/name={}", cluster_name);

        let pods = pod_api
            .list(&ListParams::default().labels(&label_selector))
            .await
            .map_err(|e| ValkeyError::Connection(format!("Failed to list pods: {}", e)))?;

        // Build IP -> ClusterNode map from cluster_nodes if available
        let mut ip_to_cluster_node: HashMap<&str, &crate::client::types::ClusterNode> =
            HashMap::new();
        if let Some(cn) = cluster_nodes {
            for node in &cn.nodes {
                ip_to_cluster_node.insert(&node.ip, node);
            }
        }

        // Build nodes from pods that exist
        let mut nodes = Vec::with_capacity(pods.items.len());

        for pod in &pods.items {
            let Some(pod_name) = pod.metadata.name.as_ref() else {
                continue;
            };

            // Extract ordinal from pod name (e.g., "my-cluster-3" -> 3)
            let ordinal = pod_name
                .rsplit('-')
                .next()
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(-1);

            if ordinal < 0 {
                // Skip pods that don't match StatefulSet naming convention
                continue;
            }

            let dns_name = super::cluster_init::pod_dns_name(cluster_name, namespace, ordinal);

            let endpoint = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_ref())
                .map(|ip| Endpoint::with_default_port(ip.clone()));

            // Get container image
            let current_image = pod
                .spec
                .as_ref()
                .and_then(|s| s.containers.first().and_then(|c| c.image.clone()));

            // Try to match with cluster node info using IP
            let cluster_node = endpoint
                .as_ref()
                .and_then(|ep| ip_to_cluster_node.get(ep.ip()).copied());

            // Extract Valkey cluster info if available
            let (node_id, role, master_id, slots, is_connected, is_failed) =
                if let Some(cn) = cluster_node {
                    let role = if cn.is_master() {
                        NodeRole::Master
                    } else if cn.is_replica() {
                        NodeRole::Replica
                    } else {
                        NodeRole::Unknown
                    };
                    let slots: Vec<SlotRange> = cn.slots.iter().map(SlotRange::from).collect();
                    (
                        Some(cn.node_id.clone()),
                        role,
                        cn.master_id.clone(),
                        slots,
                        cn.is_connected(),
                        cn.flags.fail,
                    )
                } else {
                    (None, NodeRole::Unknown, None, Vec::new(), false, false)
                };

            nodes.push(ClusterNodeInfo {
                pod_name: pod_name.clone(),
                ordinal,
                endpoint,
                dns_name,
                current_image,
                node_id,
                role,
                master_id,
                slots,
                is_connected,
                is_failed,
            });
        }

        // Sort by ordinal for consistent ordering
        nodes.sort_by_key(|n| n.ordinal);

        // Build indexes
        let mut ordinal_index = HashMap::with_capacity(nodes.len());
        let mut ip_index = HashMap::with_capacity(nodes.len());
        let mut node_id_index = HashMap::with_capacity(nodes.len());

        for (idx, node) in nodes.iter().enumerate() {
            ordinal_index.insert(node.ordinal, idx);
            if let Some(ip) = node.ip() {
                ip_index.insert(ip.to_string(), idx);
            }
            if let Some(ref nid) = node.node_id {
                node_id_index.insert(nid.clone(), idx);
            }
        }

        Ok(Self {
            nodes,
            cluster_name: cluster_name.to_string(),
            namespace: namespace.to_string(),
            ordinal_index,
            ip_index,
            node_id_index,
        })
    }

    /// Build topology from existing pod endpoints (without K8s API call).
    ///
    /// This is useful when pod IPs are already available from a previous call.
    pub fn from_pod_ips(
        namespace: &str,
        cluster_name: &str,
        pod_ips: &[(String, String, u16)], // (pod_name, ip, port)
        cluster_nodes: Option<&ParsedClusterNodes>,
    ) -> Self {
        // Build IP -> ClusterNode map from cluster_nodes if available
        let mut ip_to_cluster_node: HashMap<&str, &crate::client::types::ClusterNode> =
            HashMap::new();
        if let Some(cn) = cluster_nodes {
            for node in &cn.nodes {
                ip_to_cluster_node.insert(&node.ip, node);
            }
        }

        // Build nodes from pod_ips
        let mut nodes = Vec::with_capacity(pod_ips.len());

        for (pod_name, ip, port) in pod_ips {
            // Extract ordinal from pod name
            let ordinal = pod_name
                .rsplit('-')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            let dns_name = super::cluster_init::pod_dns_name(cluster_name, namespace, ordinal);
            let endpoint = Endpoint::new(ip.clone(), *port);

            // Try to match with cluster node info using IP
            let cluster_node = ip_to_cluster_node.get(ip.as_str()).copied();

            // Extract Valkey cluster info if available
            let (node_id, role, master_id, slots, is_connected, is_failed) =
                if let Some(cn) = cluster_node {
                    let role = if cn.is_master() {
                        NodeRole::Master
                    } else if cn.is_replica() {
                        NodeRole::Replica
                    } else {
                        NodeRole::Unknown
                    };
                    let slots: Vec<SlotRange> = cn.slots.iter().map(SlotRange::from).collect();
                    (
                        Some(cn.node_id.clone()),
                        role,
                        cn.master_id.clone(),
                        slots,
                        cn.is_connected(),
                        cn.flags.fail,
                    )
                } else {
                    (None, NodeRole::Unknown, None, Vec::new(), false, false)
                };

            nodes.push(ClusterNodeInfo {
                pod_name: pod_name.clone(),
                ordinal,
                endpoint: Some(endpoint),
                dns_name,
                current_image: None,
                node_id,
                role,
                master_id,
                slots,
                is_connected,
                is_failed,
            });
        }

        // Build indexes
        let mut ordinal_index = HashMap::with_capacity(nodes.len());
        let mut ip_index = HashMap::with_capacity(nodes.len());
        let mut node_id_index = HashMap::with_capacity(nodes.len());

        for (idx, node) in nodes.iter().enumerate() {
            ordinal_index.insert(node.ordinal, idx);
            if let Some(ip) = node.ip() {
                ip_index.insert(ip.to_string(), idx);
            }
            if let Some(ref nid) = node.node_id {
                node_id_index.insert(nid.clone(), idx);
            }
        }

        Self {
            nodes,
            cluster_name: cluster_name.to_string(),
            namespace: namespace.to_string(),
            ordinal_index,
            ip_index,
            node_id_index,
        }
    }

    // =========================================================================
    // Lookups
    // =========================================================================

    /// Look up a node by its ordinal.
    pub fn by_ordinal(&self, ordinal: i32) -> Option<&ClusterNodeInfo> {
        self.ordinal_index
            .get(&ordinal)
            .and_then(|&idx| self.nodes.get(idx))
    }

    /// Look up a node by its pod IP.
    pub fn by_ip(&self, ip: &str) -> Option<&ClusterNodeInfo> {
        self.ip_index.get(ip).and_then(|&idx| self.nodes.get(idx))
    }

    /// Look up a node by its Valkey node ID.
    pub fn by_node_id(&self, node_id: &str) -> Option<&ClusterNodeInfo> {
        self.node_id_index
            .get(node_id)
            .and_then(|&idx| self.nodes.get(idx))
    }

    /// Look up a node by its pod name.
    pub fn by_pod_name(&self, name: &str) -> Option<&ClusterNodeInfo> {
        self.nodes.iter().find(|n| n.pod_name == name)
    }

    // =========================================================================
    // Filtered Views
    // =========================================================================

    /// Get all nodes.
    pub fn all_nodes(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes.iter()
    }

    /// Get all master nodes.
    pub fn masters(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes.iter().filter(|n| n.role == NodeRole::Master)
    }

    /// Get all replica nodes.
    pub fn replicas(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes.iter().filter(|n| n.role == NodeRole::Replica)
    }

    /// Get nodes that are in the Valkey cluster.
    pub fn nodes_in_cluster(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes.iter().filter(|n| n.is_in_cluster())
    }

    /// Get nodes that are NOT in the Valkey cluster (pods exist but not joined).
    pub fn nodes_not_in_cluster(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes.iter().filter(|n| !n.is_in_cluster())
    }

    /// Get healthy master nodes.
    pub fn healthy_masters(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Master && n.is_healthy())
    }

    // =========================================================================
    // Business Logic Methods
    // =========================================================================

    /// Find replicas that should be promoted to masters.
    ///
    /// This correctly identifies nodes with ordinals 0..target_masters that
    /// are currently replicas instead of masters.
    ///
    /// # Arguments
    /// * `target_masters` - The desired number of masters
    ///
    /// # Returns
    /// Nodes that should be masters but are currently replicas.
    pub fn replicas_that_should_be_masters(&self, target_masters: i32) -> Vec<&ClusterNodeInfo> {
        self.nodes
            .iter()
            .filter(|n| {
                // Should be a master if ordinal < target_masters
                n.ordinal < target_masters && n.role == NodeRole::Replica
            })
            .collect()
    }

    /// Find orphaned nodes - nodes in the cluster with no matching pod.
    ///
    /// This uses IP matching instead of ordinal extraction, fixing the bug
    /// where `extract_ordinal_from_address` fails on IP addresses.
    ///
    /// Orphaned nodes are cluster nodes whose IP doesn't match any current pod IP.
    pub fn orphaned_nodes<'a>(
        &self,
        cluster_nodes: &'a ParsedClusterNodes,
    ) -> Vec<&'a crate::client::types::ClusterNode> {
        let pod_ips: std::collections::HashSet<&str> =
            self.nodes.iter().filter_map(|n| n.ip()).collect();

        cluster_nodes
            .nodes
            .iter()
            .filter(|cn| !pod_ips.contains(cn.ip.as_str()))
            .collect()
    }

    /// Find nodes that should be removed during scale-down.
    ///
    /// Returns nodes with ordinals >= target_masters (for masters).
    ///
    /// # Arguments
    /// * `target_masters` - The desired number of masters after scale-down
    ///
    /// # Returns
    /// Master nodes that should be removed.
    pub fn masters_to_remove(&self, target_masters: i32) -> Vec<&ClusterNodeInfo> {
        self.nodes
            .iter()
            .filter(|n| n.role == NodeRole::Master && n.ordinal >= target_masters)
            .collect()
    }

    /// Find new master ordinals that need replicas configured.
    ///
    /// Returns ordinals of masters that don't have their expected replicas set up.
    pub fn masters_needing_replicas(
        &self,
        replicas_per_master: i32,
        total_masters: i32,
    ) -> Vec<i32> {
        let mut result = Vec::new();

        for master_ordinal in 0..total_masters {
            // Check if this master exists and has the expected replicas
            if self.by_ordinal(master_ordinal).is_none() {
                continue;
            }

            // Check expected replica ordinals for this master
            let mut has_all_replicas = true;
            for r in 0..replicas_per_master {
                let replica_ordinal = total_masters + (master_ordinal * replicas_per_master) + r;
                let replica = self.by_ordinal(replica_ordinal);

                // Check if replica exists and is replicating this master
                if let Some(node) = replica {
                    let master = self.by_ordinal(master_ordinal);
                    let master_node_id = master.and_then(|m| m.node_id.as_ref());

                    if node.role != NodeRole::Replica || node.master_id.as_ref() != master_node_id {
                        has_all_replicas = false;
                        break;
                    }
                } else {
                    has_all_replicas = false;
                    break;
                }
            }

            if !has_all_replicas {
                result.push(master_ordinal);
            }
        }

        result
    }

    // =========================================================================
    // Counts and Stats
    // =========================================================================

    /// Get the total number of nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Check if the topology is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Iterate over all nodes in the topology.
    pub fn iter(&self) -> impl Iterator<Item = &ClusterNodeInfo> {
        self.nodes.iter()
    }

    /// Get the number of master nodes.
    pub fn master_count(&self) -> usize {
        self.masters().count()
    }

    /// Get the number of healthy master nodes.
    pub fn healthy_master_count(&self) -> usize {
        self.healthy_masters().count()
    }

    /// Get total slots assigned across all masters.
    pub fn total_slots_assigned(&self) -> i32 {
        self.masters().map(|m| m.slot_count()).sum()
    }

    /// Get the cluster name.
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    // =========================================================================
    // Spec Comparison
    // =========================================================================

    /// Compare topology against spec to determine required scaling action.
    pub fn compare_to_spec(&self, spec: &ValkeyClusterSpec) -> ClusterDiff {
        let current_masters = self.master_count() as i32;
        let master_diff = spec.masters - current_masters;

        let nodes_needing_promotion: Vec<ClusterNodeInfo> = self
            .replicas_that_should_be_masters(spec.masters)
            .into_iter()
            .cloned()
            .collect();

        let nodes_to_add: Vec<ClusterNodeInfo> = self.nodes_not_in_cluster().cloned().collect();

        let nodes_to_remove: Vec<ClusterNodeInfo> = self
            .masters_to_remove(spec.masters)
            .into_iter()
            .cloned()
            .collect();

        // Check if slots need rebalancing
        let slots_need_rebalancing = self.needs_slot_rebalancing(spec.masters);

        ClusterDiff {
            master_diff,
            nodes_needing_promotion,
            nodes_to_add,
            nodes_to_remove,
            slots_need_rebalancing,
        }
    }

    /// Check if slots need rebalancing for the target number of masters.
    fn needs_slot_rebalancing(&self, target_masters: i32) -> bool {
        if target_masters <= 0 {
            return false;
        }

        let masters: Vec<_> = self.masters().collect();
        if masters.is_empty() {
            return false;
        }

        // Calculate expected slots per master
        let expected_per_master = 16384 / target_masters;
        let tolerance = expected_per_master / 10; // 10% tolerance

        // Check if any master has significantly different slot count
        for master in &masters {
            let slot_count = master.slot_count();
            if (slot_count - expected_per_master).abs() > tolerance {
                return true;
            }
        }

        false
    }

    /// Build an IP to ordinal mapping (for compatibility with existing code).
    pub fn ip_to_ordinal_map(&self) -> HashMap<String, i32> {
        self.nodes
            .iter()
            .filter_map(|n| n.ip().map(|ip| (ip.to_string(), n.ordinal)))
            .collect()
    }

    /// Build an ordinal to node_id mapping (for compatibility with existing code).
    pub fn ordinal_to_node_id_map(&self) -> HashMap<i32, String> {
        self.nodes
            .iter()
            .filter_map(|n| n.node_id.as_ref().map(|nid| (n.ordinal, nid.clone())))
            .collect()
    }

    /// Get all pod endpoints as (pod_name, endpoint) tuples.
    ///
    /// Only includes nodes that have an endpoint (running pods with IPs).
    pub fn pod_endpoints(&self) -> Vec<(String, Endpoint)> {
        self.nodes
            .iter()
            .filter_map(|n| {
                n.endpoint
                    .as_ref()
                    .map(|ep| (n.pod_name.clone(), ep.clone()))
            })
            .collect()
    }
}

/// Result of comparing topology to spec.
#[derive(Debug, Clone)]
pub struct ClusterDiff {
    /// Master difference: +N = scale up, -N = scale down.
    pub master_diff: i32,
    /// Nodes that need to be promoted from replica to master.
    pub nodes_needing_promotion: Vec<ClusterNodeInfo>,
    /// Nodes that need to be added to the cluster.
    pub nodes_to_add: Vec<ClusterNodeInfo>,
    /// Nodes that need to be removed from the cluster.
    pub nodes_to_remove: Vec<ClusterNodeInfo>,
    /// Whether slots need rebalancing.
    pub slots_need_rebalancing: bool,
}

impl ClusterDiff {
    /// Check if scaling up.
    pub fn is_scale_up(&self) -> bool {
        self.master_diff > 0
    }

    /// Check if scaling down.
    pub fn is_scale_down(&self) -> bool {
        self.master_diff < 0
    }

    /// Check if the cluster is stable (no changes needed).
    pub fn is_stable(&self) -> bool {
        self.master_diff == 0
            && self.nodes_needing_promotion.is_empty()
            && self.nodes_to_add.is_empty()
            && !self.slots_need_rebalancing
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

    fn make_node(ordinal: i32, role: NodeRole, node_id: Option<&str>) -> ClusterNodeInfo {
        ClusterNodeInfo {
            pod_name: format!("test-cluster-{}", ordinal),
            ordinal,
            endpoint: Some(Endpoint::new(
                format!("10.0.0.{}", ordinal + 1),
                DEFAULT_VALKEY_PORT,
            )),
            dns_name: format!(
                "test-cluster-{}.test-cluster-headless.default.svc.cluster.local",
                ordinal
            ),
            current_image: Some("valkey:7.2".to_string()),
            node_id: node_id.map(|s| s.to_string()),
            role,
            master_id: None,
            slots: if role == NodeRole::Master {
                vec![SlotRange::new(0, 5461)]
            } else {
                vec![]
            },
            is_connected: true,
            is_failed: false,
        }
    }

    fn make_topology(nodes: Vec<ClusterNodeInfo>) -> ClusterTopology {
        let mut ordinal_index = HashMap::new();
        let mut ip_index = HashMap::new();
        let mut node_id_index = HashMap::new();

        for (idx, node) in nodes.iter().enumerate() {
            ordinal_index.insert(node.ordinal, idx);
            if let Some(ip) = node.ip() {
                ip_index.insert(ip.to_string(), idx);
            }
            if let Some(ref nid) = node.node_id {
                node_id_index.insert(nid.clone(), idx);
            }
        }

        ClusterTopology {
            nodes,
            cluster_name: "test-cluster".to_string(),
            namespace: "default".to_string(),
            ordinal_index,
            ip_index,
            node_id_index,
        }
    }

    #[test]
    fn test_by_ordinal_lookup() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Master, Some("node-1")),
            make_node(2, NodeRole::Master, Some("node-2")),
        ];
        let topology = make_topology(nodes);

        assert!(topology.by_ordinal(0).is_some());
        assert_eq!(topology.by_ordinal(0).unwrap().ordinal, 0);
        assert!(topology.by_ordinal(3).is_none());
    }

    #[test]
    fn test_by_ip_lookup() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Master, Some("node-1")),
        ];
        let topology = make_topology(nodes);

        assert!(topology.by_ip("10.0.0.1").is_some());
        assert_eq!(topology.by_ip("10.0.0.1").unwrap().ordinal, 0);
        assert!(topology.by_ip("10.0.0.99").is_none());
    }

    #[test]
    fn test_by_node_id_lookup() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("abc123")),
            make_node(1, NodeRole::Master, Some("def456")),
        ];
        let topology = make_topology(nodes);

        assert!(topology.by_node_id("abc123").is_some());
        assert_eq!(topology.by_node_id("abc123").unwrap().ordinal, 0);
        assert!(topology.by_node_id("unknown").is_none());
    }

    #[test]
    fn test_masters_filter() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Master, Some("node-1")),
            make_node(2, NodeRole::Replica, Some("node-2")),
            make_node(3, NodeRole::Replica, Some("node-3")),
        ];
        let topology = make_topology(nodes);

        let masters: Vec<_> = topology.masters().collect();
        assert_eq!(masters.len(), 2);
        assert!(masters.iter().all(|m| m.role == NodeRole::Master));
    }

    #[test]
    fn test_replicas_filter() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Replica, Some("node-1")),
            make_node(2, NodeRole::Replica, Some("node-2")),
        ];
        let topology = make_topology(nodes);

        let replicas: Vec<_> = topology.replicas().collect();
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn test_replicas_that_should_be_masters() {
        // Scenario: ordinal 0 is incorrectly a replica when target is 4 masters
        let nodes = vec![
            make_node(0, NodeRole::Replica, Some("node-0")), // BUG: Should be master
            make_node(1, NodeRole::Master, Some("node-1")),
            make_node(2, NodeRole::Master, Some("node-2")),
            make_node(3, NodeRole::Master, Some("node-3")),
        ];
        let topology = make_topology(nodes);

        let to_promote = topology.replicas_that_should_be_masters(4);
        assert_eq!(to_promote.len(), 1);
        assert_eq!(to_promote[0].ordinal, 0);
    }

    #[test]
    fn test_masters_to_remove() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Master, Some("node-1")),
            make_node(2, NodeRole::Master, Some("node-2")),
            make_node(3, NodeRole::Master, Some("node-3")),
        ];
        let topology = make_topology(nodes);

        // Scale down from 4 to 3 masters
        let to_remove = topology.masters_to_remove(3);
        assert_eq!(to_remove.len(), 1);
        assert_eq!(to_remove[0].ordinal, 3);

        // Scale down from 4 to 2 masters
        let to_remove = topology.masters_to_remove(2);
        assert_eq!(to_remove.len(), 2);
        assert!(to_remove.iter().any(|n| n.ordinal == 2));
        assert!(to_remove.iter().any(|n| n.ordinal == 3));
    }

    #[test]
    fn test_nodes_in_cluster() {
        // node_in: ordinal 0, has node_id → is in cluster
        // node_out: ordinal 1, no node_id → not in cluster
        let node_in = make_node(0, NodeRole::Master, Some("node-0"));
        let node_out = make_node(1, NodeRole::Unknown, None);

        let topology = make_topology(vec![node_in, node_out]);

        let in_cluster: Vec<_> = topology.nodes_in_cluster().collect();
        assert_eq!(in_cluster.len(), 1);
        assert_eq!(in_cluster[0].ordinal, 0);

        let not_in_cluster: Vec<_> = topology.nodes_not_in_cluster().collect();
        assert_eq!(not_in_cluster.len(), 1);
        assert_eq!(not_in_cluster[0].ordinal, 1);
    }

    #[test]
    fn test_master_count() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Master, Some("node-1")),
            make_node(2, NodeRole::Replica, Some("node-2")),
        ];
        let topology = make_topology(nodes);

        assert_eq!(topology.master_count(), 2);
    }

    #[test]
    fn test_slot_count() {
        let mut node = make_node(0, NodeRole::Master, Some("node-0"));
        node.slots = vec![SlotRange::new(0, 5460), SlotRange::new(5461, 10922)];

        let topology = make_topology(vec![node]);

        assert_eq!(topology.total_slots_assigned(), 10923);
    }

    #[test]
    fn test_ip_to_ordinal_map() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("node-0")),
            make_node(1, NodeRole::Master, Some("node-1")),
        ];
        let topology = make_topology(nodes);

        let map = topology.ip_to_ordinal_map();
        assert_eq!(map.get("10.0.0.1"), Some(&0));
        assert_eq!(map.get("10.0.0.2"), Some(&1));
    }

    #[test]
    fn test_ordinal_to_node_id_map() {
        let nodes = vec![
            make_node(0, NodeRole::Master, Some("abc123")),
            make_node(1, NodeRole::Master, Some("def456")),
        ];
        let topology = make_topology(nodes);

        let map = topology.ordinal_to_node_id_map();
        assert_eq!(map.get(&0), Some(&"abc123".to_string()));
        assert_eq!(map.get(&1), Some(&"def456".to_string()));
    }

    #[test]
    fn test_cluster_diff_is_scale_up() {
        let diff = ClusterDiff {
            master_diff: 2,
            nodes_needing_promotion: vec![],
            nodes_to_add: vec![],
            nodes_to_remove: vec![],
            slots_need_rebalancing: false,
        };
        assert!(diff.is_scale_up());
        assert!(!diff.is_scale_down());
    }

    #[test]
    fn test_cluster_diff_is_scale_down() {
        let diff = ClusterDiff {
            master_diff: -1,
            nodes_needing_promotion: vec![],
            nodes_to_add: vec![],
            nodes_to_remove: vec![],
            slots_need_rebalancing: false,
        };
        assert!(diff.is_scale_down());
        assert!(!diff.is_scale_up());
    }

    #[test]
    fn test_cluster_diff_is_stable() {
        let diff = ClusterDiff {
            master_diff: 0,
            nodes_needing_promotion: vec![],
            nodes_to_add: vec![],
            nodes_to_remove: vec![],
            slots_need_rebalancing: false,
        };
        assert!(diff.is_stable());

        let diff_with_promotions = ClusterDiff {
            master_diff: 0,
            nodes_needing_promotion: vec![make_node(0, NodeRole::Replica, Some("node-0"))],
            nodes_to_add: vec![],
            nodes_to_remove: vec![],
            slots_need_rebalancing: false,
        };
        assert!(!diff_with_promotions.is_stable());
    }

    #[test]
    fn test_from_pod_ips() {
        let pod_ips = vec![
            ("test-cluster-0".to_string(), "10.0.0.1".to_string(), 6379),
            ("test-cluster-1".to_string(), "10.0.0.2".to_string(), 6379),
        ];

        let topology = ClusterTopology::from_pod_ips("default", "test-cluster", &pod_ips, None);

        assert_eq!(topology.len(), 2);
        assert!(topology.by_ordinal(0).is_some());
        assert!(topology.by_ip("10.0.0.1").is_some());
    }

    // =========================================================================
    // extract_ordinal_from_address tests
    // =========================================================================

    #[test]
    fn test_extract_ordinal_full_dns() {
        assert_eq!(
            extract_ordinal_from_address(
                "my-cluster-0.my-cluster-headless.default.svc.cluster.local:6379@16379"
            ),
            Some(0)
        );
        assert_eq!(
            extract_ordinal_from_address(
                "my-cluster-5.my-cluster-headless.ns.svc.cluster.local:6379@16379"
            ),
            Some(5)
        );
    }

    #[test]
    fn test_extract_ordinal_short_hostname() {
        assert_eq!(extract_ordinal_from_address("my-cluster-0:6379"), Some(0));
        assert_eq!(extract_ordinal_from_address("my-cluster-5:6379"), Some(5));
    }

    #[test]
    fn test_extract_ordinal_no_port() {
        assert_eq!(extract_ordinal_from_address("my-cluster-0"), Some(0));
        assert_eq!(
            extract_ordinal_from_address("my-cluster-0.my-cluster-headless"),
            Some(0)
        );
    }

    #[test]
    fn test_extract_ordinal_invalid() {
        assert_eq!(extract_ordinal_from_address(""), None);
        assert_eq!(extract_ordinal_from_address("invalid"), None);
        assert_eq!(extract_ordinal_from_address("no-number-here"), None);
    }

    #[test]
    fn test_extract_ordinal_ip_address_returns_wrong_value() {
        // IMPORTANT: IP addresses return INCORRECT ordinal values!
        // This is the bug that ClusterTopology.by_ip() fixes.
        //
        // For "10.0.0.5:6379":
        //   1. Split by ':' -> "10.0.0.5"
        //   2. Split by '.' and take first -> "10"
        //   3. Parse "10" -> Some(10)  // WRONG! This is not an ordinal
        //
        // Always use ClusterTopology.by_ip() for IP-based lookups.
        assert_eq!(extract_ordinal_from_address("10.0.0.5:6379"), Some(10)); // Wrong!
        assert_eq!(
            extract_ordinal_from_address("192.168.1.100:6379"),
            Some(192)
        ); // Wrong!
    }
}
