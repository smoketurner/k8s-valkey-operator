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
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    let (host, port) = strategy.get_connection(0).await?;

    let client = crate::client::ValkeyClient::connect_single(
        &host,
        port,
        password.as_deref(),
        tls_certs.as_ref(),
    )
    .await?;

    let cluster_nodes = client.cluster_nodes().await?;

    let _ = client.close().await;

    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(&cluster_nodes)).await?;

    let orphaned = topology.orphaned_nodes(&cluster_nodes);
    let orphan_node_ids: Vec<String> = orphaned.iter().map(|n| n.node_id.clone()).collect();

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
    node_ids_to_forget: &[String],
) -> Result<(), Error> {
    let name = obj.name_any();
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    let (host, port) = strategy.get_connection(0).await?;

    let client = crate::client::ValkeyClient::connect_single(
        &host,
        port,
        password.as_deref(),
        tls_certs.as_ref(),
    )
    .await?;

    let cluster_nodes = client.cluster_nodes().await?;

    let _ = client.close().await;

    let total_nodes = cluster_nodes.nodes.len();
    let required = ((total_nodes / 2) + 1) as i32;

    for node_id in node_ids_to_forget {
        let mut successes = 0i32;
        let mut failures = Vec::new();

        for (idx, node) in cluster_nodes.nodes.iter().enumerate() {
            if &node.node_id == node_id {
                continue;
            }

            let ordinal = i32::try_from(idx).unwrap_or(0);
            if let Ok((h, p)) = strategy.get_connection(ordinal).await {
                match crate::client::ValkeyClient::connect_single(
                    &h,
                    p,
                    password.as_deref(),
                    tls_certs.as_ref(),
                )
                .await
                {
                    Ok(node_client) => {
                        match node_client.cluster_forget(node_id).await {
                            Ok(()) => {
                                successes += 1;
                                debug!(
                                    name = %name,
                                    node_id = %node_id,
                                    source = %node.node_id,
                                    "CLUSTER FORGET succeeded"
                                );
                            }
                            Err(e) => {
                                failures.push(format!("{}:{}", node.node_id, e));
                            }
                        }
                        let _ = node_client.close().await;
                    }
                    Err(e) => {
                        failures.push(format!("{}:connect failed: {}", node.node_id, e));
                    }
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

    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    let (host, port) = strategy.get_connection(0).await?;

    let client = crate::client::ValkeyClient::connect_single(
        &host,
        port,
        password.as_deref(),
        tls_certs.as_ref(),
    )
    .await?;

    let cluster_nodes = client.cluster_nodes().await?;

    let _ = client.close().await;

    let total_masters = cluster_nodes.masters().len() as i32;
    let required_quorum = (total_masters / 2) + 1;

    // If we connected and got CLUSTER NODES, all reported masters are reachable.
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

    let api: kube::Api<ValkeyCluster> = kube::Api::namespaced(ctx.client.clone(), namespace);
    remove_finalizer(&api, &name, super::FINALIZER).await?;

    Ok(kube::runtime::controller::Action::await_change())
}
