//! Cluster initialization operations for ValkeyCluster.
//!
//! This module handles the initial setup of a Valkey cluster including:
//! - CLUSTER MEET operations to connect all nodes
//! - Slot assignment via CLUSTER ADDSLOTS
//! - Replica setup via CLUSTER REPLICATE
//!
//! When the operator runs locally (outside the cluster), port forwarding
//! is used to connect to Valkey pods since K8s DNS is not available.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client, ResourceExt};
use tokio::sync::Mutex;
use tracing::{debug, info, instrument, warn};

use crate::client::valkey_client::{TlsCertData, ValkeyClient, ValkeyError};
use crate::crd::ValkeyCluster;
use crate::resources::port_forward::{PortForward, PortForwardError, PortForwardTarget};
use crate::slots::{calculate_distribution, planner::extract_ordinal_from_address};

/// Build the DNS name for a pod in the StatefulSet.
///
/// Format: `{cluster-name}-{ordinal}.{cluster-name}-headless.{namespace}.svc.cluster.local`
pub fn pod_dns_name(cluster_name: &str, namespace: &str, ordinal: i32) -> String {
    let headless = format!("{}-headless", cluster_name);
    format!(
        "{}-{}.{}.{}.svc.cluster.local",
        cluster_name, ordinal, headless, namespace
    )
}

/// Build a list of all pod DNS names for a cluster.
pub fn all_pod_dns_names(cluster: &ValkeyCluster) -> Vec<(String, u16)> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let total_pods = crate::crd::total_pods(cluster.spec.masters, cluster.spec.replicas_per_master);

    (0..total_pods)
        .map(|i| (pod_dns_name(&name, &namespace, i), 6379))
        .collect()
}

/// Get the DNS names for master nodes only.
///
/// Masters are pods 0 through (masters-1) in the StatefulSet.
pub fn master_pod_dns_names(cluster: &ValkeyCluster) -> Vec<(String, u16)> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());

    (0..cluster.spec.masters)
        .map(|i| (pod_dns_name(&name, &namespace, i), 6379))
        .collect()
}

/// Get the DNS names for replica nodes for a specific master.
///
/// For master index M with R replicas per master:
/// - Replica ordinals are: masters + (M * replicas_per_master) + 0..replicas_per_master
pub fn replica_pod_dns_names_for_master(
    cluster: &ValkeyCluster,
    master_index: i32,
) -> Vec<(String, u16)> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let replicas = cluster.spec.replicas_per_master;

    let base_ordinal = cluster.spec.masters + (master_index * replicas);
    (0..replicas)
        .map(|r| (pod_dns_name(&name, &namespace, base_ordinal + r), 6379))
        .collect()
}

/// Get pod name (StatefulSet pod name, not DNS)
pub fn pod_name(cluster_name: &str, ordinal: i32) -> String {
    format!("{}-{}", cluster_name, ordinal)
}

/// Get all pod names for a cluster
pub fn all_pod_names(cluster: &ValkeyCluster) -> Vec<String> {
    let name = cluster.name_any();
    let total_pods = crate::crd::total_pods(cluster.spec.masters, cluster.spec.replicas_per_master);

    (0..total_pods).map(|i| pod_name(&name, i)).collect()
}

/// Get pod IP addresses from the Kubernetes API.
///
/// CLUSTER MEET requires IP addresses, not DNS names.
/// Returns a vector of (pod_name, ip_address, port) tuples.
pub async fn get_pod_ips(
    client: &Client,
    namespace: &str,
    cluster_name: &str,
    total_pods: i32,
) -> Result<Vec<(String, String, u16)>, ValkeyError> {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    // Use app.kubernetes.io/name for consistency with check_running_pods and StatefulSet
    let label_selector = format!("app.kubernetes.io/name={}", cluster_name);

    let pods = pod_api
        .list(&ListParams::default().labels(&label_selector))
        .await
        .map_err(|e| ValkeyError::Connection(format!("Failed to list pods: {}", e)))?;

    let mut pod_ips: Vec<(String, String, u16)> = Vec::new();

    for ordinal in 0..total_pods {
        let pod_name = format!("{}-{}", cluster_name, ordinal);

        let pod = pods
            .items
            .iter()
            .find(|p| p.metadata.name.as_deref() == Some(&pod_name))
            .ok_or_else(|| ValkeyError::ClusterNotReady(format!("Pod {} not found", pod_name)))?;

        let ip = pod
            .status
            .as_ref()
            .and_then(|s| s.pod_ip.as_ref())
            .ok_or_else(|| ValkeyError::ClusterNotReady(format!("Pod {} has no IP", pod_name)))?;

        pod_ips.push((pod_name, ip.clone(), 6379));
    }

    Ok(pod_ips)
}

/// Connection strategy for connecting to Valkey pods
///
/// We always use port forwarding as it works both in-cluster and locally.
/// The overhead is minimal for an operator making occasional admin connections.
#[derive(Clone)]
pub struct ConnectionStrategy {
    port_forwards: Arc<Mutex<ClusterPortForwards>>,
}

impl ConnectionStrategy {
    /// Create a new connection strategy with port forwarding
    pub fn new(client: Client, namespace: &str, cluster_name: &str) -> Self {
        let forwards = ClusterPortForwards::new(client, namespace, cluster_name);
        Self {
            port_forwards: Arc::new(Mutex::new(forwards)),
        }
    }

    /// Get a connection address for a pod by ordinal
    pub async fn get_connection(&self, ordinal: i32) -> Result<(String, u16), ValkeyError> {
        let mut pf = self.port_forwards.lock().await;
        pf.ensure_forward(ordinal)
            .await
            .map_err(|e| ValkeyError::Connection(format!("Port forward error: {}", e)))
    }
}

/// Manages port forwards for all pods in a cluster
pub struct ClusterPortForwards {
    client: Client,
    namespace: String,
    cluster_name: String,
    /// Map from pod ordinal to port forward
    forwards: HashMap<i32, PortForward>,
}

impl ClusterPortForwards {
    /// Create a new port forward manager
    pub fn new(client: Client, namespace: &str, cluster_name: &str) -> Self {
        Self {
            client,
            namespace: namespace.to_string(),
            cluster_name: cluster_name.to_string(),
            forwards: HashMap::new(),
        }
    }

    /// Ensure a port forward exists for the given pod ordinal
    pub async fn ensure_forward(
        &mut self,
        ordinal: i32,
    ) -> Result<(String, u16), PortForwardError> {
        if let Some(pf) = self.forwards.get(&ordinal) {
            return Ok(("127.0.0.1".to_string(), pf.local_port()));
        }

        let pod_name = pod_name(&self.cluster_name, ordinal);

        info!(pod = %pod_name, ordinal = ordinal, "Creating port forward for pod");

        let pf = PortForward::start(
            self.client.clone(),
            &self.namespace,
            PortForwardTarget::pod(&pod_name, 6379),
            None,
        )
        .await?;

        let local_port = pf.local_port();
        self.forwards.insert(ordinal, pf);

        Ok(("127.0.0.1".to_string(), local_port))
    }

    /// Get the connection address for a pod ordinal
    pub fn get_address(&self, ordinal: i32) -> Option<(String, u16)> {
        self.forwards
            .get(&ordinal)
            .map(|pf| ("127.0.0.1".to_string(), pf.local_port()))
    }

    /// Create port forwards for all pods in the cluster
    pub async fn ensure_all_forwards(&mut self, total_pods: i32) -> Result<(), PortForwardError> {
        for i in 0..total_pods {
            self.ensure_forward(i).await?;
        }
        Ok(())
    }
}

/// Execute CLUSTER MEET from the first node to all other nodes.
///
/// This connects all nodes in the cluster so they can communicate via gossip.
///
/// Uses pod IP addresses for CLUSTER MEET since Valkey requires IP addresses
/// (DNS names are not accepted by the CLUSTER MEET command).
#[instrument(skip(k8s_client, cluster, password, tls_certs, strategy))]
pub async fn execute_cluster_meet(
    k8s_client: &Client,
    cluster: &ValkeyCluster,
    password: Option<&str>,
    tls_certs: Option<&TlsCertData>,
    strategy: &ConnectionStrategy,
) -> Result<(), ValkeyError> {
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let cluster_name = cluster.name_any();
    let total_pods = crate::crd::total_pods(cluster.spec.masters, cluster.spec.replicas_per_master);

    // Get pod IP addresses from Kubernetes API
    // CLUSTER MEET requires IP addresses, not DNS names
    let pod_ips = get_pod_ips(k8s_client, &namespace, &cluster_name, total_pods).await?;

    if pod_ips.is_empty() {
        return Err(ValkeyError::InvalidConfig("No pods in cluster".to_string()));
    }

    // Get connection address for first node via port forwarding
    let (connect_host, connect_port) = strategy.get_connection(0).await?;

    info!(
        host = %connect_host,
        port = %connect_port,
        total_nodes = pod_ips.len(),
        "Connecting to first node to execute CLUSTER MEET"
    );

    // Connect to the first node via port forward
    // TLS is still used - the port forward is just a transport layer tunnel
    let client =
        ValkeyClient::connect_single(&connect_host, connect_port, password, tls_certs).await?;

    // Execute CLUSTER MEET for all other nodes using their IP addresses
    for (_pod_name, ip, port) in pod_ips.iter().skip(1) {
        debug!(ip = %ip, port = %port, "Executing CLUSTER MEET");
        match client.cluster_meet(ip, *port).await {
            Ok(()) => {
                debug!(ip = %ip, port = %port, "CLUSTER MEET successful");
            }
            Err(e) => {
                warn!(ip = %ip, port = %port, error = %e, "CLUSTER MEET failed");
                return Err(e);
            }
        }
    }

    // Give the cluster time to propagate the MEET commands via gossip
    tokio::time::sleep(Duration::from_secs(2)).await;

    client.close().await?;
    info!("CLUSTER MEET complete, all nodes connected");
    Ok(())
}

/// Assign hash slots evenly across master nodes.
///
/// Connects to each master and assigns its portion of the 16384 hash slots.
/// This function is idempotent - it checks existing slot assignments and only
/// assigns slots that haven't been assigned yet.
#[instrument(skip(cluster, password, tls_certs, strategy))]
pub async fn assign_slots_to_masters(
    cluster: &ValkeyCluster,
    password: Option<&str>,
    tls_certs: Option<&TlsCertData>,
    strategy: &ConnectionStrategy,
) -> Result<(), ValkeyError> {
    let masters = cluster.spec.masters;
    let slot_ranges = calculate_distribution(masters as u16);
    let master_pods = master_pod_dns_names(cluster);

    if slot_ranges.len() != master_pods.len() {
        return Err(ValkeyError::InvalidConfig(format!(
            "Slot distribution mismatch: {} ranges for {} masters",
            slot_ranges.len(),
            master_pods.len()
        )));
    }

    // First, check if all slots are already assigned by querying the cluster
    let (connect_host, connect_port) = strategy.get_connection(0).await?;
    let check_client =
        ValkeyClient::connect_single(&connect_host, connect_port, password, tls_certs).await?;

    let cluster_info = check_client.cluster_info().await?;
    check_client.close().await?;

    if cluster_info.all_slots_assigned() {
        info!(
            slots_assigned = cluster_info.slots_assigned,
            "All slots already assigned, skipping slot assignment"
        );
        return Ok(());
    }

    info!(
        masters = masters,
        slots_assigned = cluster_info.slots_assigned,
        "Assigning slots to {} masters",
        masters
    );

    // Get current slot assignments from cluster nodes
    let (connect_host, connect_port) = strategy.get_connection(0).await?;
    let check_client =
        ValkeyClient::connect_single(&connect_host, connect_port, password, tls_certs).await?;
    let cluster_nodes = check_client.cluster_nodes().await?;
    check_client.close().await?;

    // Build a set of already-assigned slots
    let mut assigned_slots: std::collections::HashSet<u16> = std::collections::HashSet::new();
    for master in cluster_nodes.masters() {
        for slot_range in &master.slots {
            for slot in slot_range.start..=slot_range.end {
                assigned_slots.insert(slot as u16);
            }
        }
    }

    for (i, ((_dns_host, _dns_port), range)) in
        master_pods.iter().zip(slot_ranges.iter()).enumerate()
    {
        // Filter out already-assigned slots
        let slots_to_assign: Vec<u16> = range
            .iter()
            .filter(|slot| !assigned_slots.contains(slot))
            .collect();

        if slots_to_assign.is_empty() {
            debug!(
                master_index = i,
                "All slots for this master already assigned, skipping"
            );
            continue;
        }

        // Get connection address via port forwarding
        let (connect_host, connect_port) = strategy.get_connection(i as i32).await?;

        debug!(
            master_index = i,
            host = %connect_host,
            port = %connect_port,
            start_slot = range.start,
            end_slot = range.end,
            slots_to_assign = slots_to_assign.len(),
            "Connecting to master to assign slots"
        );

        // Connect to this specific master via port forward
        // TLS is still used - the port forward is just a transport layer tunnel
        let client =
            ValkeyClient::connect_single(&connect_host, connect_port, password, tls_certs).await?;

        debug!(
            master_index = i,
            slot_count = slots_to_assign.len(),
            "Executing CLUSTER ADDSLOTS"
        );

        client.cluster_add_slots(slots_to_assign).await?;
        client.close().await?;

        info!(
            master_index = i,
            slots = %range,
            "Slots assigned to master"
        );
    }

    info!("All 16384 slots assigned to masters");
    Ok(())
}

/// Set up replica nodes to replicate their assigned masters.
///
/// For each master, connects to its replicas and executes CLUSTER REPLICATE.
#[instrument(skip(cluster, password, tls_certs, strategy))]
pub async fn setup_replicas(
    cluster: &ValkeyCluster,
    password: Option<&str>,
    tls_certs: Option<&TlsCertData>,
    strategy: &ConnectionStrategy,
) -> Result<(), ValkeyError> {
    if cluster.spec.replicas_per_master == 0 {
        info!("No replicas configured, skipping replica setup");
        return Ok(());
    }

    let masters = cluster.spec.masters;
    let replicas_per_master = cluster.spec.replicas_per_master;

    info!(
        masters = masters,
        replicas_per_master = replicas_per_master,
        "Setting up replicas"
    );

    // First, get the node IDs for all masters
    // We need to connect to the cluster and get the topology
    let master_pods = master_pod_dns_names(cluster);

    if master_pods.is_empty() {
        return Err(ValkeyError::InvalidConfig(
            "No master pods found".to_string(),
        ));
    }

    // Get connection address for first master via port forwarding
    let (connect_host, connect_port) = strategy.get_connection(0).await?;

    // Connect to first master to get cluster topology
    // TLS is still used - the port forward is just a transport layer tunnel
    let client =
        ValkeyClient::connect_single(&connect_host, connect_port, password, tls_certs).await?;

    // Get cluster nodes to find master node IDs
    let nodes_raw = client.cluster_nodes_raw().await?;
    client.close().await?;

    // Parse the cluster nodes output to get master node IDs
    let master_node_ids: Vec<String> = parse_master_node_ids(&nodes_raw, masters as usize)?;

    if master_node_ids.len() != masters as usize {
        return Err(ValkeyError::ClusterNotReady(format!(
            "Expected {} masters but found {} in cluster topology",
            masters,
            master_node_ids.len()
        )));
    }

    // For each master, set up its replicas
    for (master_index, master_node_id) in master_node_ids.iter().enumerate() {
        let replica_pods = replica_pod_dns_names_for_master(cluster, master_index as i32);

        for (replica_index, (_replica_dns_host, _replica_dns_port)) in
            replica_pods.iter().enumerate()
        {
            // Calculate the ordinal for this replica pod
            let replica_ordinal =
                masters + (master_index as i32 * replicas_per_master) + replica_index as i32;

            // Get connection address for this replica via port forwarding
            let (connect_host, connect_port) = strategy.get_connection(replica_ordinal).await?;

            debug!(
                master_index = master_index,
                master_node_id = %master_node_id,
                replica_index = replica_index,
                replica_host = %connect_host,
                "Setting up replica"
            );

            // Connect to the replica via port forward
            // TLS is still used - the port forward is just a transport layer tunnel
            let replica_client =
                ValkeyClient::connect_single(&connect_host, connect_port, password, tls_certs)
                    .await?;

            // Execute CLUSTER REPLICATE to make this node replicate the master
            replica_client.cluster_replicate(master_node_id).await?;
            replica_client.close().await?;

            info!(
                master_index = master_index,
                replica_index = replica_index,
                "Replica configured to replicate master"
            );
        }
    }

    info!("All replicas configured");
    Ok(())
}

/// Parse master node IDs from CLUSTER NODES output.
///
/// Returns node IDs for nodes that have slots assigned (masters).
fn parse_master_node_ids(
    nodes_output: &str,
    expected_masters: usize,
) -> Result<Vec<String>, ValkeyError> {
    let mut master_ids: Vec<(String, i32)> = Vec::new();

    for line in nodes_output.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();

        // Extract required fields safely
        let Some(node_id) = parts.first() else {
            continue;
        };
        let Some(address) = parts.get(1) else {
            continue;
        };
        let Some(flags) = parts.get(2) else { continue };

        // A master has "master" in flags and has slot assignments
        // After CLUSTER ADDSLOTS, masters will have slots listed after the 8th field
        if flags.contains("master") && parts.len() > 8 {
            // Check if this node has slots assigned (indicated by slot ranges after field 8)
            let has_slots = parts
                .iter()
                .skip(8)
                .any(|p| p.contains('-') || p.parse::<i32>().is_ok());
            if has_slots {
                // Extract ordinal from address to maintain order
                // Address format: hostname:port@cluster-bus-port
                // For StatefulSet: my-cluster-0.my-cluster-headless.ns.svc.cluster.local:6379@16379
                let ordinal = extract_ordinal_from_address(address)
                    .map(|o| o as i32)
                    .unwrap_or(master_ids.len() as i32);
                master_ids.push(((*node_id).to_string(), ordinal));
            }
        }
    }

    // Sort by ordinal to ensure consistent ordering
    master_ids.sort_by_key(|(_, ordinal)| *ordinal);
    let ids: Vec<String> = master_ids.into_iter().map(|(id, _)| id).collect();

    if ids.len() < expected_masters {
        // If we don't have enough masters with slots, also include masters without slots
        // This handles the case where we're getting node IDs before slot assignment
        let mut all_masters: Vec<(String, i32)> = Vec::new();

        for line in nodes_output.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();

            // Extract required fields safely
            let Some(node_id) = parts.first() else {
                continue;
            };
            let Some(address) = parts.get(1) else {
                continue;
            };
            let Some(flags) = parts.get(2) else { continue };

            if flags.contains("master") && !flags.contains("fail") {
                let ordinal = extract_ordinal_from_address(address)
                    .map(|o| o as i32)
                    .unwrap_or(all_masters.len() as i32);
                all_masters.push(((*node_id).to_string(), ordinal));
            }
        }

        all_masters.sort_by_key(|(_, ordinal)| *ordinal);
        return Ok(all_masters
            .into_iter()
            .map(|(id, _)| id)
            .take(expected_masters)
            .collect());
    }

    Ok(ids)
}

/// Full cluster initialization workflow.
///
/// Executes all steps needed to initialize a new cluster:
/// 1. CLUSTER MEET all nodes
/// 2. Assign slots to masters
/// 3. Set up replicas
#[instrument(skip(k8s_client, cluster, password, tls_certs, strategy))]
pub async fn initialize_cluster(
    k8s_client: &Client,
    cluster: &ValkeyCluster,
    password: Option<&str>,
    tls_certs: Option<&TlsCertData>,
    strategy: &ConnectionStrategy,
) -> Result<(), ValkeyError> {
    let name = cluster.name_any();
    info!(cluster = %name, "Starting cluster initialization");

    // Step 1: Connect all nodes via CLUSTER MEET
    info!(cluster = %name, "Step 1: Executing CLUSTER MEET");
    execute_cluster_meet(k8s_client, cluster, password, tls_certs, strategy).await?;

    // Give nodes time to exchange topology information
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 2: Assign slots to masters
    info!(cluster = %name, "Step 2: Assigning slots to masters");
    assign_slots_to_masters(cluster, password, tls_certs, strategy).await?;

    // Give cluster time to propagate slot assignments
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 3: Set up replicas
    if cluster.spec.replicas_per_master > 0 {
        info!(cluster = %name, "Step 3: Setting up replicas");
        setup_replicas(cluster, password, tls_certs, strategy).await?;
    }

    info!(cluster = %name, "Cluster initialization complete");
    Ok(())
}

/// Create a connection strategy for connecting to Valkey pods.
///
/// Always uses port forwarding as it works both in-cluster and locally.
pub fn create_connection_strategy(
    client: Client,
    namespace: &str,
    cluster_name: &str,
) -> ConnectionStrategy {
    info!(namespace = %namespace, cluster = %cluster_name, "Creating connection strategy with port forwarding");
    ConnectionStrategy::new(client, namespace, cluster_name)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;
    use crate::crd::{AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyClusterSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    fn test_cluster(name: &str, masters: i32, replicas: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters,
                replicas_per_master: replicas,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "test-issuer".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-secret".to_string(),
                        ..Default::default()
                    },
                },
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_pod_dns_name() {
        assert_eq!(
            pod_dns_name("my-cluster", "default", 0),
            "my-cluster-0.my-cluster-headless.default.svc.cluster.local"
        );
        assert_eq!(
            pod_dns_name("my-cluster", "production", 5),
            "my-cluster-5.my-cluster-headless.production.svc.cluster.local"
        );
    }

    #[test]
    fn test_all_pod_dns_names() {
        let cluster = test_cluster("my-cluster", 3, 1);
        let pods = all_pod_dns_names(&cluster);

        // 3 masters + 3 replicas = 6 pods
        assert_eq!(pods.len(), 6);
        assert_eq!(
            pods[0].0,
            "my-cluster-0.my-cluster-headless.default.svc.cluster.local"
        );
        assert_eq!(
            pods[5].0,
            "my-cluster-5.my-cluster-headless.default.svc.cluster.local"
        );
    }

    #[test]
    fn test_master_pod_dns_names() {
        let cluster = test_cluster("my-cluster", 3, 1);
        let masters = master_pod_dns_names(&cluster);

        assert_eq!(masters.len(), 3);
        assert!(masters[0].0.contains("my-cluster-0"));
        assert!(masters[1].0.contains("my-cluster-1"));
        assert!(masters[2].0.contains("my-cluster-2"));
    }

    #[test]
    fn test_replica_pod_dns_names_for_master() {
        let cluster = test_cluster("my-cluster", 3, 2);

        // Master 0 replicas should be pods 3 and 4
        let replicas_0 = replica_pod_dns_names_for_master(&cluster, 0);
        assert_eq!(replicas_0.len(), 2);
        assert!(replicas_0[0].0.contains("my-cluster-3"));
        assert!(replicas_0[1].0.contains("my-cluster-4"));

        // Master 1 replicas should be pods 5 and 6
        let replicas_1 = replica_pod_dns_names_for_master(&cluster, 1);
        assert_eq!(replicas_1.len(), 2);
        assert!(replicas_1[0].0.contains("my-cluster-5"));
        assert!(replicas_1[1].0.contains("my-cluster-6"));

        // Master 2 replicas should be pods 7 and 8
        let replicas_2 = replica_pod_dns_names_for_master(&cluster, 2);
        assert_eq!(replicas_2.len(), 2);
        assert!(replicas_2[0].0.contains("my-cluster-7"));
        assert!(replicas_2[1].0.contains("my-cluster-8"));
    }

    #[test]
    fn test_parse_master_node_ids() {
        let nodes_output = r#"abc123 my-cluster-0.my-cluster-headless.default.svc.cluster.local:6379@16379 myself,master - 0 1234567890 1 connected 0-5461
def456 my-cluster-1.my-cluster-headless.default.svc.cluster.local:6379@16379 master - 0 1234567890 2 connected 5462-10922
ghi789 my-cluster-2.my-cluster-headless.default.svc.cluster.local:6379@16379 master - 0 1234567890 3 connected 10923-16383"#;

        let ids = parse_master_node_ids(nodes_output, 3).unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], "abc123");
        assert_eq!(ids[1], "def456");
        assert_eq!(ids[2], "ghi789");
    }

    // ==========================================================================
    // Additional pod_name and all_pod_names tests
    // ==========================================================================

    #[test]
    fn test_pod_name() {
        assert_eq!(pod_name("my-cluster", 0), "my-cluster-0");
        assert_eq!(pod_name("my-cluster", 5), "my-cluster-5");
        assert_eq!(pod_name("valkey-prod", 10), "valkey-prod-10");
    }

    #[test]
    fn test_all_pod_names() {
        let cluster = test_cluster("my-cluster", 3, 1);
        let names = all_pod_names(&cluster);

        // 3 masters + 3 replicas = 6 pods
        assert_eq!(names.len(), 6);
        assert_eq!(names[0], "my-cluster-0");
        assert_eq!(names[5], "my-cluster-5");
    }

    #[test]
    fn test_all_pod_names_no_replicas() {
        let cluster = test_cluster("my-cluster", 3, 0);
        let names = all_pod_names(&cluster);

        // 3 masters + 0 replicas = 3 pods
        assert_eq!(names.len(), 3);
        assert_eq!(names[0], "my-cluster-0");
        assert_eq!(names[2], "my-cluster-2");
    }

    #[test]
    fn test_master_pod_dns_names_only_masters() {
        // Verify masters are correctly identified regardless of replicas
        let cluster = test_cluster("my-cluster", 5, 2);
        let masters = master_pod_dns_names(&cluster);

        assert_eq!(masters.len(), 5);
        for (i, (dns, port)) in masters.iter().enumerate() {
            assert!(dns.contains(&format!("my-cluster-{}", i)));
            assert_eq!(*port, 6379);
        }
    }

    #[test]
    fn test_pod_dns_name_special_namespace() {
        assert_eq!(
            pod_dns_name("cluster", "kube-system", 0),
            "cluster-0.cluster-headless.kube-system.svc.cluster.local"
        );
        assert_eq!(
            pod_dns_name("my-valkey", "prod-us-east-1", 3),
            "my-valkey-3.my-valkey-headless.prod-us-east-1.svc.cluster.local"
        );
    }

    #[test]
    fn test_replica_pod_dns_names_no_replicas() {
        let cluster = test_cluster("my-cluster", 3, 0);
        let replicas = replica_pod_dns_names_for_master(&cluster, 0);
        assert!(replicas.is_empty());
    }
}
