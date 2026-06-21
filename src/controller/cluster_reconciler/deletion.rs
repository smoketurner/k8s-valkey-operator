//! Deletion and node-removal operations for ValkeyCluster.

use tracing::{debug, info, warn};

use crate::{
    controller::{cluster_topology::ClusterTopology, context::Context, error::Error},
    crd::ValkeyCluster,
};

use kube::ResourceExt;

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

    let orphaned = topology.orphaned_nodes(&cluster_nodes);
    let orphan_node_ids: Vec<crate::crd::NodeId> =
        orphaned.iter().map(|n| n.node_id.clone()).collect();

    if orphan_node_ids.is_empty() {
        return Ok(true);
    }

    for node in &orphaned {
        debug!(
            node_id = %node.node_id,
            ip = %node.ip,
            "Found orphaned node (IP not in current pods)"
        );
    }

    info!(
        name = %name,
        orphan_count = orphan_node_ids.len(),
        "Removing orphaned nodes from cluster"
    );

    forget_nodes_with_quorum(obj, ctx, namespace, &orphan_node_ids).await?;

    Ok(false)
}

/// Execute CLUSTER FORGET for a set of node IDs, requiring quorum acknowledgement.
#[tracing::instrument(skip(obj, ctx, node_ids_to_forget), fields(name = %obj.name_any(), namespace = %namespace, node_count = node_ids_to_forget.len()))]
pub(crate) async fn forget_nodes_with_quorum(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    node_ids_to_forget: &[crate::crd::NodeId],
) -> Result<(), Error> {
    let name = obj.name_any();

    // FORGET must be sent from the surviving cluster members, which are the pods that
    // remain after any scale-down: ordinals 0..total_pods (the spec already reflects the
    // target size at this point). Iterate real StatefulSet pod ordinals — a node's position
    // within `CLUSTER NODES` is unrelated to its pod ordinal, so indexing that list would
    // connect to the wrong pods. The node(s) being removed sit at ordinals >= total_pods
    // and are therefore naturally excluded as FORGET sources.
    let total_pods = crate::crd::total_pods(obj.spec.masters, obj.spec.replicas_per_master);
    let required = (total_pods / 2) + 1;

    for node_id in node_ids_to_forget {
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

    let reachable_masters = total_masters;

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
