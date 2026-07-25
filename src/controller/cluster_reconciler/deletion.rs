//! Deletion and node-removal operations for ValkeyCluster.

use std::time::Duration;

use tracing::{debug, info, warn};

use crate::{
    client::ParsedClusterNodes,
    controller::{cluster_topology::ClusterTopology, context::Context, error::Error},
    crd::{NodeId, ValkeyCluster},
};

use kube::ResourceExt;

/// How many times `CLUSTER NODES` is read before concluding a node still owns slots.
const FORGET_SLOT_CHECK_ATTEMPTS: u32 = 3;

/// Delay between `CLUSTER NODES` re-reads, to let gossip catch up after a migration.
const FORGET_SLOT_CHECK_DELAY: Duration = Duration::from_secs(2);

/// Execute CLUSTER FORGET for nodes whose pods no longer exist.
///
/// Returns `true` when all orphaned nodes have been forgotten, `false` when more
/// iterations are needed.
#[tracing::instrument(skip(obj, ctx), fields(name = %obj.name_any(), namespace = %namespace))]
pub(crate) async fn execute_forget_removed_nodes(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    let name = obj.name_any();

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;
    let cluster_nodes = client.cluster_nodes().await?;
    let _ = client.close().await;

    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(&cluster_nodes)).await?;

    let report = topology.orphan_report(&cluster_nodes);

    for (node, reason) in &report.inconclusive {
        warn!(
            name = %name,
            node_id = %node.node_id,
            ip = %node.ip,
            hostname = ?node.hostname,
            reason = %reason,
            "Cluster node could not be matched to a pod; leaving it in the cluster"
        );
    }

    let mut orphan_node_ids: Vec<NodeId> = Vec::with_capacity(report.orphaned.len());
    for node in &report.orphaned {
        debug!(
            node_id = %node.node_id,
            ip = %node.ip,
            hostname = ?node.hostname,
            "Found orphaned node (no matching pod by hostname or IP)"
        );
        orphan_node_ids.push(node.node_id.clone());
    }

    if orphan_node_ids.is_empty() {
        return Ok(true);
    }

    info!(
        name = %name,
        orphan_count = orphan_node_ids.len(),
        "Removing orphaned nodes from cluster"
    );

    forget_nodes_with_quorum(obj, ctx, namespace, &orphan_node_ids).await?;

    Ok(false)
}

/// Split node IDs into those safe to `CLUSTER FORGET` and those that still own slots.
///
/// Forgetting a node removes it from gossip but does not reassign its slots, so
/// any slot it still owns becomes unowned and its keys return CLUSTERDOWN. IDs
/// the cluster no longer knows are safe: FORGET is a no-op for them.
///
/// Returns `(safe, blocked)` where each blocked entry carries the slot count.
pub(crate) fn partition_forgettable<'a>(
    cluster_nodes: &'a ParsedClusterNodes,
    node_ids: &'a [NodeId],
) -> (Vec<NodeId>, Vec<(&'a NodeId, usize)>) {
    let mut safe = Vec::with_capacity(node_ids.len());
    let mut blocked = Vec::new();

    for node_id in node_ids {
        let Some(node) = cluster_nodes.get_node(node_id) else {
            safe.push(node_id.clone());
            continue;
        };

        let mut slot_count = 0usize;
        for range in &node.slots {
            slot_count += usize::from(range.count());
        }

        if slot_count > 0 {
            blocked.push((node_id, slot_count));
        } else {
            safe.push(node_id.clone());
        }
    }

    (safe, blocked)
}

/// Fetch `CLUSTER NODES` from the first pod ordinal that answers.
///
/// Pod 0 may be down during a scale-down or recovery, so every surviving
/// ordinal is tried before giving up.
async fn cluster_nodes_from_any_pod(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    total_pods: i32,
) -> Result<ParsedClusterNodes, Error> {
    let mut last_error: Option<Error> = None;

    for ordinal in 0..total_pods {
        let client = match ctx.connect_to_cluster(obj, namespace, ordinal).await {
            Ok(client) => client,
            Err(e) => {
                debug!(ordinal, error = %e, "Failed to connect while reading CLUSTER NODES");
                last_error = Some(e);
                continue;
            }
        };

        let nodes = client.cluster_nodes().await;
        let _ = client.close().await;

        match nodes {
            Ok(nodes) => return Ok(nodes),
            Err(e) => {
                debug!(ordinal, error = %e, "CLUSTER NODES failed, trying next pod");
                last_error = Some(e.into());
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        Error::Transient(format!(
            "No pod out of {} responded to CLUSTER NODES; retry once pods are reachable",
            total_pods
        ))
    }))
}

/// Re-read `CLUSTER NODES` until no target owns slots, or the attempts run out.
///
/// A migration that just finished can take a gossip round to show up on the
/// node we happen to query, so a single stale read must not block the removal.
async fn partition_forgettable_with_retry(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    total_pods: i32,
    node_ids_to_forget: &[NodeId],
) -> Result<(Vec<NodeId>, Vec<String>), Error> {
    let mut safe = Vec::new();
    let mut blocked = Vec::new();

    for attempt in 0..FORGET_SLOT_CHECK_ATTEMPTS {
        let cluster_nodes = cluster_nodes_from_any_pod(obj, ctx, namespace, total_pods).await?;
        let (safe_ids, blocked_nodes) = partition_forgettable(&cluster_nodes, node_ids_to_forget);

        safe = safe_ids;
        blocked = blocked_nodes
            .iter()
            .map(|(node_id, slots)| format!("{} owns {} slot(s)", node_id, slots))
            .collect();

        if blocked.is_empty() {
            break;
        }

        if attempt + 1 < FORGET_SLOT_CHECK_ATTEMPTS {
            debug!(
                attempt,
                blocked = ?blocked,
                "Nodes still own slots; re-reading CLUSTER NODES to rule out gossip lag"
            );
            tokio::time::sleep(FORGET_SLOT_CHECK_DELAY).await;
        }
    }

    Ok((safe, blocked))
}

/// Execute CLUSTER FORGET for a set of node IDs, requiring quorum acknowledgement.
#[tracing::instrument(skip(obj, ctx, node_ids_to_forget), fields(name = %obj.name_any(), namespace = %namespace, node_count = node_ids_to_forget.len()))]
pub(crate) async fn forget_nodes_with_quorum(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    node_ids_to_forget: &[NodeId],
) -> Result<(), Error> {
    if node_ids_to_forget.is_empty() {
        return Ok(());
    }

    let name = obj.name_any();

    // FORGET must be sent from the surviving cluster members, which are the pods that
    // remain after any scale-down: ordinals 0..total_pods (the spec already reflects the
    // target size at this point). Iterate real StatefulSet pod ordinals — a node's position
    // within `CLUSTER NODES` is unrelated to its pod ordinal, so indexing that list would
    // connect to the wrong pods. The node(s) being removed sit at ordinals >= total_pods
    // and are therefore naturally excluded as FORGET sources.
    let total_pods = crate::crd::total_pods(obj.spec.masters, obj.spec.replicas_per_master);
    let required = (total_pods / 2) + 1;

    // This is the only route to CLUSTER FORGET, so the slot check lives here:
    // callers cannot bypass it. Forgetting a node that still owns slots strands
    // those slots with no owner and their keys start returning CLUSTERDOWN.
    let (safe_to_forget, blocked) =
        partition_forgettable_with_retry(obj, ctx, namespace, total_pods, node_ids_to_forget)
            .await?;

    for node_id in &safe_to_forget {
        let mut successes = 0i32;
        let mut failures = Vec::new();

        for ordinal in 0..total_pods {
            match ctx.connect_to_cluster(obj, namespace, ordinal).await {
                Ok(node_client) => {
                    match node_client.cluster_forget(node_id).await {
                        Ok(()) => {
                            successes += 1;
                            debug!(
                                name = %name,
                                node_id = %node_id,
                                source_ordinal = %ordinal,
                                "CLUSTER FORGET succeeded"
                            );
                        }
                        Err(e) => {
                            // CLUSTER FORGET is idempotent from our perspective: if this
                            // source already doesn't know the target (e.g. it was forgotten
                            // on a previous, partially-successful attempt), or the target is
                            // the source itself, the desired end state already holds. Count
                            // these as successes so retries don't stall on already-removed
                            // nodes. Mirrors the handling in cluster_init's forget loop.
                            let error_str = e.to_string().to_lowercase();
                            if error_str.contains("unknown node") || error_str.contains("myself") {
                                successes += 1;
                                debug!(
                                    name = %name,
                                    node_id = %node_id,
                                    source_ordinal = %ordinal,
                                    "Node already forgotten or is self, treating as success"
                                );
                            } else {
                                failures.push(format!("{}:{}", ordinal, e));
                            }
                        }
                    }
                    let _ = node_client.close().await;
                }
                Err(e) => {
                    failures.push(format!("{}:connect failed: {}", ordinal, e));
                }
            }
        }

        if successes < required {
            warn!(
                name = %name,
                node_id = %node_id,
                successes = successes,
                required = required,
                failures = ?failures,
                "CLUSTER FORGET did not achieve quorum"
            );
            return Err(Error::ForgetQuorumFailed {
                successes,
                required,
            });
        }

        info!(
            name = %name,
            node_id = %node_id,
            successes = successes,
            required = required,
            "CLUSTER FORGET achieved quorum"
        );
    }

    // Reported after the safe nodes are removed so a mixed batch still makes
    // progress and only the unsafe remainder needs operator attention.
    if !blocked.is_empty() {
        warn!(
            name = %name,
            blocked = ?blocked,
            forgotten = safe_to_forget.len(),
            "Refusing to forget nodes that still own hash slots"
        );
        return Err(Error::UnsafeForget {
            node_count: blocked.len(),
            detail: blocked.join("; "),
        });
    }

    Ok(())
}

/// Verify quorum before scale-down operations.
pub(crate) async fn verify_scale_down_quorum(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;
    let cluster_nodes = client.cluster_nodes().await?;
    let _ = client.close().await;

    let total_masters = cluster_nodes.masters().len() as i32;
    let required_quorum = (total_masters / 2) + 1;

    // Only count masters the cluster itself reports as healthy (no fail/pfail
    // flags and a connected link). Counting every known master made this check
    // a no-op: reachable == total can never be below (total/2)+1.
    let reachable_masters = cluster_nodes
        .masters()
        .iter()
        .filter(|m| m.is_healthy())
        .count() as i32;

    debug!(
        name = %name,
        total_masters = total_masters,
        required_quorum = required_quorum,
        reachable_masters = reachable_masters,
        "Verifying scale-down quorum"
    );

    if reachable_masters < required_quorum {
        return Err(Error::InsufficientQuorum {
            reachable: reachable_masters,
            required: required_quorum,
        });
    }

    Ok(())
}

/// Handle deletion of a ValkeyCluster.
#[tracing::instrument(skip(obj, ctx), fields(name = %obj.name_any(), namespace = %namespace))]
pub(crate) async fn handle_deletion(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<kube::runtime::controller::Action, Error> {
    use crate::controller::common::remove_finalizer;

    let name = obj.name_any();
    info!(name = %name, "Handling deletion");

    ctx.transport_pool().evict_cluster(namespace, &name).await;

    let api: kube::Api<ValkeyCluster> = kube::Api::namespaced(ctx.client.clone(), namespace);
    remove_finalizer(&api, &name, super::FINALIZER).await?;

    Ok(kube::runtime::controller::Action::await_change())
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

    const MASTER_WITH_SLOTS: &str = "1111111111111111111111111111111111111111";
    const MASTER_NO_SLOTS: &str = "2222222222222222222222222222222222222222";
    const REPLICA: &str = "3333333333333333333333333333333333333333";

    fn cluster_nodes() -> ParsedClusterNodes {
        let output = format!(
            "{MASTER_WITH_SLOTS} 10.0.0.1:6379@16379,c-0.c-headless.default.svc.cluster.local myself,master - 0 1 1 connected 0-5460 8000-8100\n\
             {MASTER_NO_SLOTS} 10.0.0.2:6379@16379,c-1.c-headless.default.svc.cluster.local master - 0 1 2 connected\n\
             {REPLICA} 10.0.0.3:6379@16379,c-2.c-headless.default.svc.cluster.local slave {MASTER_WITH_SLOTS} 0 1 1 connected"
        );
        ParsedClusterNodes::parse(&output).expect("fixture should parse")
    }

    #[test]
    fn test_partition_forgettable_unknown_node_is_safe() {
        // FORGET for a node the cluster no longer knows is a no-op, so it must
        // not block the rest of the batch.
        let nodes = cluster_nodes();
        let ids = vec![NodeId::from("4444444444444444444444444444444444444444")];

        let (safe, blocked) = partition_forgettable(&nodes, &ids);

        assert_eq!(safe, ids);
        assert!(blocked.is_empty());
    }

    #[test]
    fn test_partition_forgettable_master_without_slots_is_safe() {
        let nodes = cluster_nodes();
        let ids = vec![NodeId::from(MASTER_NO_SLOTS)];

        let (safe, blocked) = partition_forgettable(&nodes, &ids);

        assert_eq!(safe, ids);
        assert!(blocked.is_empty());
    }

    #[test]
    fn test_partition_forgettable_master_with_slots_is_blocked() {
        let nodes = cluster_nodes();
        let ids = vec![NodeId::from(MASTER_WITH_SLOTS)];

        let (safe, blocked) = partition_forgettable(&nodes, &ids);

        assert!(safe.is_empty());
        assert_eq!(blocked.len(), 1);
        // 0-5460 is 5461 slots, 8000-8100 is 101 slots.
        assert_eq!(blocked[0].1, 5562);
    }

    #[test]
    fn test_partition_forgettable_replica_is_safe() {
        // Replicas never own slots, so removing one cannot strand keys.
        let nodes = cluster_nodes();
        let ids = vec![NodeId::from(REPLICA)];

        let (safe, blocked) = partition_forgettable(&nodes, &ids);

        assert_eq!(safe, ids);
        assert!(blocked.is_empty());
    }

    #[test]
    fn test_partition_forgettable_mixed_batch_keeps_safe_nodes() {
        // A blocked node must not stop the safe ones from being forgotten.
        let nodes = cluster_nodes();
        let ids = vec![
            NodeId::from(MASTER_WITH_SLOTS),
            NodeId::from(MASTER_NO_SLOTS),
            NodeId::from(REPLICA),
        ];

        let (safe, blocked) = partition_forgettable(&nodes, &ids);

        assert_eq!(
            safe,
            vec![NodeId::from(MASTER_NO_SLOTS), NodeId::from(REPLICA)]
        );
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].0, &NodeId::from(MASTER_WITH_SLOTS));
    }

    #[test]
    fn test_partition_forgettable_empty_input() {
        let nodes = cluster_nodes();

        let (safe, blocked) = partition_forgettable(&nodes, &[]);

        assert!(safe.is_empty());
        assert!(blocked.is_empty());
    }
}
