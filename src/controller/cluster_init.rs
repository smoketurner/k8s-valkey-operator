//! Cluster initialization operations for ValkeyCluster.
//!
//! This module handles the initial setup of a Valkey cluster including:
//! - CLUSTER MEET operations to connect all nodes
//! - Slot assignment via CLUSTER ADDSLOTS
//! - Replica setup via CLUSTER REPLICATE

use std::time::Duration;

use kube::ResourceExt;
use tracing::{debug, info, instrument, warn};

use crate::client::cluster_ops::calculate_slot_distribution;
use crate::client::valkey_client::{ValkeyClient, ValkeyError};
use crate::crd::ValkeyCluster;

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
    let namespace = cluster
        .namespace()
        .unwrap_or_else(|| "default".to_string());
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
    let namespace = cluster
        .namespace()
        .unwrap_or_else(|| "default".to_string());

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
    let namespace = cluster
        .namespace()
        .unwrap_or_else(|| "default".to_string());
    let replicas = cluster.spec.replicas_per_master;

    let base_ordinal = cluster.spec.masters + (master_index * replicas);
    (0..replicas)
        .map(|r| (pod_dns_name(&name, &namespace, base_ordinal + r), 6379))
        .collect()
}

/// Execute CLUSTER MEET from the first node to all other nodes.
///
/// This connects all nodes in the cluster so they can communicate via gossip.
#[instrument(skip(cluster, password))]
pub async fn execute_cluster_meet(
    cluster: &ValkeyCluster,
    password: Option<&str>,
    use_tls: bool,
) -> Result<(), ValkeyError> {
    let all_pods = all_pod_dns_names(cluster);

    if all_pods.is_empty() {
        return Err(ValkeyError::InvalidConfig("No pods in cluster".to_string()));
    }

    let (first_host, first_port) = &all_pods[0];
    info!(
        host = %first_host,
        port = %first_port,
        total_nodes = all_pods.len(),
        "Connecting to first node to execute CLUSTER MEET"
    );

    // Connect to the first node
    let client = ValkeyClient::connect_single(first_host, *first_port, password, use_tls).await?;

    // Execute CLUSTER MEET for all other nodes
    for (host, port) in all_pods.iter().skip(1) {
        debug!(host = %host, port = %port, "Executing CLUSTER MEET");
        match client.cluster_meet(host, *port).await {
            Ok(()) => {
                debug!(host = %host, port = %port, "CLUSTER MEET successful");
            }
            Err(e) => {
                warn!(host = %host, port = %port, error = %e, "CLUSTER MEET failed");
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
#[instrument(skip(cluster, password))]
pub async fn assign_slots_to_masters(
    cluster: &ValkeyCluster,
    password: Option<&str>,
    use_tls: bool,
) -> Result<(), ValkeyError> {
    let masters = cluster.spec.masters;
    let slot_ranges = calculate_slot_distribution(masters);
    let master_pods = master_pod_dns_names(cluster);

    if slot_ranges.len() != master_pods.len() {
        return Err(ValkeyError::InvalidConfig(format!(
            "Slot distribution mismatch: {} ranges for {} masters",
            slot_ranges.len(),
            master_pods.len()
        )));
    }

    info!(
        masters = masters,
        "Assigning slots to {} masters",
        masters
    );

    for (i, ((host, port), (start_slot, end_slot))) in
        master_pods.iter().zip(slot_ranges.iter()).enumerate()
    {
        debug!(
            master_index = i,
            host = %host,
            port = %port,
            start_slot = start_slot,
            end_slot = end_slot,
            "Connecting to master to assign slots"
        );

        // Connect to this specific master
        let client = ValkeyClient::connect_single(host, *port, password, use_tls).await?;

        // Build slot range and assign
        let slots: Vec<u16> = (*start_slot as u16..=*end_slot as u16).collect();
        debug!(
            master_index = i,
            slot_count = slots.len(),
            "Executing CLUSTER ADDSLOTS"
        );

        client.cluster_add_slots(slots).await?;
        client.close().await?;

        info!(
            master_index = i,
            slots = format!("{}-{}", start_slot, end_slot),
            "Slots assigned to master"
        );
    }

    info!("All 16384 slots assigned to masters");
    Ok(())
}

/// Set up replica nodes to replicate their assigned masters.
///
/// For each master, connects to its replicas and executes CLUSTER REPLICATE.
#[instrument(skip(cluster, password))]
pub async fn setup_replicas(
    cluster: &ValkeyCluster,
    password: Option<&str>,
    use_tls: bool,
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

    // Connect to first master to get cluster topology
    let (first_host, first_port) = &master_pods[0];
    let client = ValkeyClient::connect_single(first_host, *first_port, password, use_tls).await?;

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

        for (replica_index, (replica_host, replica_port)) in replica_pods.iter().enumerate() {
            debug!(
                master_index = master_index,
                master_node_id = %master_node_id,
                replica_index = replica_index,
                replica_host = %replica_host,
                "Setting up replica"
            );

            // Connect to the replica
            let replica_client =
                ValkeyClient::connect_single(replica_host, *replica_port, password, use_tls)
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
fn parse_master_node_ids(nodes_output: &str, expected_masters: usize) -> Result<Vec<String>, ValkeyError> {
    let mut master_ids: Vec<(String, i32)> = Vec::new();

    for line in nodes_output.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 3 {
            continue;
        }

        let node_id = parts[0];
        let flags = parts[2];

        // A master has "master" in flags and has slot assignments
        // After CLUSTER ADDSLOTS, masters will have slots listed after the 8th field
        if flags.contains("master") && parts.len() > 8 {
            // Check if this node has slots assigned (indicated by slot ranges after field 8)
            let has_slots = parts.iter().skip(8).any(|p| p.contains('-') || p.parse::<i32>().is_ok());
            if has_slots {
                // Extract ordinal from address to maintain order
                // Address format: hostname:port@cluster-bus-port
                // For StatefulSet: my-cluster-0.my-cluster-headless.ns.svc.cluster.local:6379@16379
                let ordinal = extract_ordinal_from_address(parts[1]).unwrap_or(master_ids.len() as i32);
                master_ids.push((node_id.to_string(), ordinal));
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
            if parts.len() < 3 {
                continue;
            }

            let node_id = parts[0];
            let flags = parts[2];

            if flags.contains("master") && !flags.contains("fail") {
                let ordinal = extract_ordinal_from_address(parts.get(1).unwrap_or(&""))
                    .unwrap_or(all_masters.len() as i32);
                all_masters.push((node_id.to_string(), ordinal));
            }
        }

        all_masters.sort_by_key(|(_, ordinal)| *ordinal);
        return Ok(all_masters.into_iter().map(|(id, _)| id).take(expected_masters).collect());
    }

    Ok(ids)
}

/// Extract pod ordinal from a StatefulSet pod address.
///
/// Address format: `my-cluster-0.my-cluster-headless.ns.svc.cluster.local:6379@16379`
fn extract_ordinal_from_address(address: &str) -> Option<i32> {
    // Remove port suffix if present
    let hostname = address.split(':').next()?;
    // Get the pod name part (before first dot)
    let pod_name = hostname.split('.').next()?;
    // Extract ordinal from pod name (last part after last hyphen)
    let ordinal_str = pod_name.rsplit('-').next()?;
    ordinal_str.parse().ok()
}

/// Full cluster initialization workflow.
///
/// Executes all steps needed to initialize a new cluster:
/// 1. CLUSTER MEET all nodes
/// 2. Assign slots to masters
/// 3. Set up replicas
#[instrument(skip(cluster, password))]
pub async fn initialize_cluster(
    cluster: &ValkeyCluster,
    password: Option<&str>,
    use_tls: bool,
) -> Result<(), ValkeyError> {
    let name = cluster.name_any();
    info!(cluster = %name, "Starting cluster initialization");

    // Step 1: Connect all nodes via CLUSTER MEET
    info!(cluster = %name, "Step 1: Executing CLUSTER MEET");
    execute_cluster_meet(cluster, password, use_tls).await?;

    // Give nodes time to exchange topology information
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 2: Assign slots to masters
    info!(cluster = %name, "Step 2: Assigning slots to masters");
    assign_slots_to_masters(cluster, password, use_tls).await?;

    // Give cluster time to propagate slot assignments
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Step 3: Set up replicas
    if cluster.spec.replicas_per_master > 0 {
        info!(cluster = %name, "Step 3: Setting up replicas");
        setup_replicas(cluster, password, use_tls).await?;
    }

    info!(cluster = %name, "Cluster initialization complete");
    Ok(())
}

#[cfg(test)]
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
    fn test_extract_ordinal_from_address() {
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
        assert_eq!(extract_ordinal_from_address("invalid"), None);
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
}
