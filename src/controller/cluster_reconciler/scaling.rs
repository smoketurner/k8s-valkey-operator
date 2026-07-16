//! Scaling operations for ValkeyCluster (scale-up, scale-down, rebalance).

use std::collections::HashMap;

use tracing::{debug, info, warn};

use crate::{
    client::{ScalingContext, ValkeyError},
    controller::{cluster_init, cluster_topology::ClusterTopology, context::Context, error::Error},
    crd::ValkeyCluster,
    resources::common,
    slots::calculate_distribution,
};

use kube::ResourceExt;

/// Execute a scaling operation (scale-up, scale-down, or rebalance).
///
/// For scale-down, connects directly to each source node to execute
/// CLUSTER MIGRATESLOTS (must run on the slot-owning node).
#[tracing::instrument(skip(obj, ctx), fields(name = %obj.name_any(), namespace = %namespace))]
pub(crate) async fn execute_scaling_operation(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<crate::client::ScalingResult, Error> {
    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Err(ValkeyError::InvalidConfig("No pod addresses available".to_string()).into());
    }

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

    let cluster_nodes = client.cluster_nodes().await?;

    // Count only masters with assigned slots for the scaling decision.
    // Raw masters() count may include unconfigured pods at replica ordinals
    // that appear as masters (CLUSTER REPLICATE runs in a later phase).
    let masters_with_slots = cluster_nodes
        .masters()
        .iter()
        .filter(|m| !m.slots.is_empty())
        .count() as i32;
    let target_masters = obj.spec.masters;
    let _ = client.close().await;

    if target_masters > masters_with_slots {
        let promoted =
            promote_replicas_to_masters(obj, ctx, namespace, masters_with_slots, target_masters)
                .await?;
        if !promoted.is_empty() {
            info!(promoted = ?promoted, "Promoted replicas to masters before scaling");
        }
    }

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

    let cluster_nodes = client.cluster_nodes().await?;

    // Count masters that map to an existing pod, excluding stale gossip
    // entries for deleted nodes. We don't filter by ordinal range here
    // because during scale-down the current masters sit at ordinals >=
    // target_masters and must be counted.
    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(&cluster_nodes)).await?;
    let ip_to_ordinal = topology.ip_to_ordinal_map();

    let current_masters = cluster_nodes
        .masters()
        .iter()
        .filter(|m| ip_to_ordinal.contains_key(&m.ip))
        .count() as i32;

    let scaling_ctx = ScalingContext {
        current_masters,
        target_masters,
        namespace: namespace.to_string(),
        headless_service: common::headless_service_name(obj),
        port: 6379,
    };

    let result = if scaling_ctx.is_scale_up() {
        info!(
            current = current_masters,
            target = target_masters,
            "Executing scale up"
        );
        let result = client.scale_up(&scaling_ctx).await;
        let _ = client.close().await;
        result
    } else if scaling_ctx.is_scale_down() {
        info!(
            current = current_masters,
            target = target_masters,
            "Executing scale down"
        );
        let _ = client.close().await;
        execute_scale_down(obj, ctx, namespace, &cluster_nodes, &scaling_ctx).await
    } else {
        info!(
            current = current_masters,
            target = target_masters,
            "Rebalancing slots (masters already at target count)"
        );
        let _ = client.close().await;
        execute_rebalance_slots(obj, ctx, namespace, &cluster_nodes, target_masters).await
    };

    result.map_err(Error::from)
}

/// Execute a scale-down: migrate slots off nodes being removed, then CLUSTER FORGET them.
///
/// CLUSTER MIGRATESLOTS must be executed on the source node.
#[tracing::instrument(skip(obj, ctx, cluster_nodes, scaling_ctx), fields(name = %obj.name_any(), namespace = %namespace))]
pub(crate) async fn execute_scale_down(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    cluster_nodes: &crate::client::ParsedClusterNodes,
    scaling_ctx: &ScalingContext,
) -> Result<crate::client::ScalingResult, ValkeyError> {
    let mut result = crate::client::ScalingResult::default();
    let masters: Vec<_> = cluster_nodes.masters().into_iter().collect();

    let ordinals_to_remove = scaling_ctx.masters_to_remove();

    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(cluster_nodes)).await?;

    let ip_to_ordinal = topology.ip_to_ordinal_map();

    let nodes_to_remove: Vec<_> = masters
        .iter()
        .filter(|m| {
            if let Some(&ordinal) = ip_to_ordinal.get(&m.ip) {
                ordinals_to_remove.contains(&ordinal)
            } else {
                false
            }
        })
        .copied()
        .collect();

    let orphaned_nodes = topology.orphaned_nodes(cluster_nodes);
    let orphaned_masters: Vec<_> = orphaned_nodes
        .iter()
        .filter(|n| n.flags.master)
        .copied()
        .collect();

    if !orphaned_masters.is_empty() {
        info!(
            orphan_count = orphaned_masters.len(),
            orphan_ids = ?orphaned_masters.iter().map(|n| &n.node_id).collect::<Vec<_>>(),
            "Found orphaned master nodes (no matching pods)"
        );
    }

    let orphans_with_slots: Vec<_> = orphaned_masters
        .iter()
        .filter(|n| !n.slots.is_empty())
        .collect();

    if !orphans_with_slots.is_empty() {
        warn!(
            orphans_with_slots = orphans_with_slots.len(),
            orphan_ids = ?orphans_with_slots.iter().map(|n| &n.node_id).collect::<Vec<_>>(),
            "Orphaned nodes have slots - cannot migrate from deleted pods. \
             These nodes will be forgotten, which may cause data loss."
        );
    }

    if nodes_to_remove.is_empty() && orphaned_masters.is_empty() {
        return Err(ValkeyError::InvalidConfig(format!(
            "Scale-down failed: no nodes found to remove. Expected ordinals {:?}, but none matched IPs. \
             Master IPs: {:?}, IP→ordinal map: {:?}. No orphan nodes found either.",
            ordinals_to_remove,
            masters.iter().map(|m| &m.ip).collect::<Vec<_>>(),
            ip_to_ordinal
        )));
    }

    let remaining_masters: Vec<_> = masters
        .iter()
        .filter(|m| {
            !nodes_to_remove.iter().any(|n| n.node_id == m.node_id)
                && !orphaned_masters.iter().any(|n| n.node_id == m.node_id)
        })
        .copied()
        .collect();

    if remaining_masters.is_empty() && !nodes_to_remove.is_empty() {
        return Err(ValkeyError::InvalidConfig(
            "Cannot scale down: no remaining masters to receive slots".to_string(),
        ));
    }

    info!(
        nodes_to_remove = nodes_to_remove.len(),
        orphaned_masters = orphaned_masters.len(),
        remaining_masters = remaining_masters.len(),
        "Starting scale-down slot migration"
    );

    for node in &nodes_to_remove {
        let slot_ranges = node.slots.clone();

        if slot_ranges.is_empty() {
            debug!(node_id = %node.node_id, "Node has no slots to migrate");
            continue;
        }

        let Some(&ordinal) = ip_to_ordinal.get(&node.ip) else {
            return Err(ValkeyError::InvalidConfig(format!(
                "Cannot find ordinal for node IP {}: no matching pod",
                node.ip
            )));
        };

        info!(
            node_id = %node.node_id,
            ip = %node.ip,
            ordinal = %ordinal,
            slot_ranges = slot_ranges.len(),
            "Connecting to source node for slot migration"
        );

        let source_client = ctx
            .connect_to_cluster(obj, namespace, ordinal)
            .await
            .map_err(|e| {
                ValkeyError::Connection(format!(
                    "Failed to connect to source node {} (ordinal {}): {}",
                    node.node_id, ordinal, e
                ))
            })?;

        for (range_idx, range) in slot_ranges.iter().enumerate() {
            let dest_idx = range_idx % remaining_masters.len();
            let Some(dest_node) = remaining_masters.get(dest_idx) else {
                let _ = source_client.close().await;
                return Err(ValkeyError::InvalidConfig(
                    "Cannot determine destination master for slot migration".to_string(),
                ));
            };

            let target_ordinal = ip_to_ordinal
                .get(&dest_node.ip)
                .copied()
                .unwrap_or_default();

            info!(
                from_node = %node.node_id,
                to_node = %dest_node.node_id,
                target_ordinal = %target_ordinal,
                start_slot = range.start,
                end_slot = range.end,
                "Migrating slot range using CLUSTER MIGRATESLOTS"
            );

            match source_client
                .cluster_migrateslots(range.start, range.end, &dest_node.node_id)
                .await
            {
                Ok(()) => {
                    if let Err(e) = wait_for_slot_migration(
                        ctx,
                        obj,
                        namespace,
                        &source_client,
                        target_ordinal,
                        range.start,
                        range.end,
                        &dest_node.node_id,
                    )
                    .await
                    {
                        let _ = source_client.close().await;
                        return Err(e);
                    }
                    result.slots_moved += i32::from(range.end - range.start) + 1;
                    debug!(range = ?range, "Slot range migration completed");
                }
                Err(e) => {
                    warn!(range = ?range, error = %e, "Failed to migrate slot range");
                    let _ = source_client.close().await;
                    return Err(e);
                }
            }
        }

        let _ = source_client.close().await;
    }

    // CLUSTER FORGET the removed nodes after migrations.
    //
    // FORGET only removes a node from the *receiving* node's local table; the remaining
    // nodes will re-propagate it via gossip unless FORGET reaches all of them (Valkey's
    // documented ban-list behavior). Sending FORGET to a single node therefore leaves
    // orphaned entries that reappear in CLUSTER NODES. Use the all-nodes quorum helper
    // (also used by deletion) so the removal sticks, and only report nodes as removed
    // once FORGET has achieved quorum.
    let mut node_ids_to_forget: Vec<crate::crd::NodeId> = Vec::new();
    for node in nodes_to_remove.iter().chain(orphaned_masters.iter()) {
        if !node_ids_to_forget.contains(&node.node_id) {
            node_ids_to_forget.push(node.node_id.clone());
        }
    }

    if !node_ids_to_forget.is_empty() {
        super::deletion::forget_nodes_with_quorum(obj, ctx, namespace, &node_ids_to_forget)
            .await
            .map_err(|e| ValkeyError::Connection(format!("CLUSTER FORGET failed: {}", e)))?;
        result.nodes_removed = node_ids_to_forget;
    }

    info!(
        nodes_removed = result.nodes_removed.len(),
        slots_moved = result.slots_moved,
        "Scale down complete"
    );

    Ok(result)
}

/// Execute slot rebalancing using per-node port-forwarded connections.
///
/// CLUSTER MIGRATESLOTS must be executed on the source node.
#[tracing::instrument(skip(obj, ctx, cluster_nodes), fields(name = %obj.name_any(), namespace = %namespace, target_masters = %target_masters))]
pub(crate) async fn execute_rebalance_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    cluster_nodes: &crate::client::ParsedClusterNodes,
    target_masters: i32,
) -> Result<crate::client::ScalingResult, ValkeyError> {
    let mut result = crate::client::ScalingResult::default();

    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(cluster_nodes)).await?;
    let ip_to_ordinal = topology.ip_to_ordinal_map();

    // Filter masters to only include pods in the expected master ordinal range.
    // After CLUSTER MEET, pods at replica ordinals may appear as masters because
    // CLUSTER REPLICATE hasn't run yet (ConfiguringNewReplicas is a later phase).
    let masters: Vec<_> = cluster_nodes
        .nodes
        .iter()
        .filter(|n| {
            !n.is_replica()
                && n.master_id
                    .as_ref()
                    .is_none_or(|id| id == "-" || id.is_empty())
                && ip_to_ordinal
                    .get(&n.ip)
                    .is_some_and(|&ord| ord < target_masters)
        })
        .collect();

    if masters.len() != target_masters as usize {
        return Err(ValkeyError::ClusterNotReady(format!(
            "Expected {} masters but found {}",
            target_masters,
            masters.len()
        )));
    }

    let target_distribution = calculate_distribution(target_masters as u16);

    let mut current_ownership: HashMap<u16, crate::crd::NodeId> = HashMap::new();
    let mut node_info: HashMap<crate::crd::NodeId, (String, crate::crd::PodOrdinal)> =
        HashMap::new();

    for master in &masters {
        if let Some(&ordinal) = ip_to_ordinal.get(&master.ip) {
            node_info.insert(master.node_id.clone(), (master.ip.clone(), ordinal));
        }
        for range in &master.slots {
            for slot in range.start..=range.end {
                current_ownership.insert(slot, master.node_id.clone());
            }
        }
    }

    let mut masters_sorted: Vec<_> = masters.clone();
    masters_sorted.sort_by_key(|m| {
        ip_to_ordinal
            .get(&m.ip)
            .copied()
            .unwrap_or(crate::crd::PodOrdinal::new(i32::MAX))
    });

    let mut migrations_by_source: HashMap<crate::crd::NodeId, Vec<(u16, crate::crd::NodeId)>> =
        HashMap::new();
    let mut unassigned_slots: HashMap<crate::crd::NodeId, Vec<u16>> = HashMap::new();

    for (master, range) in masters_sorted.iter().zip(target_distribution.iter()) {
        for slot in range.iter() {
            if let Some(current_owner) = current_ownership.get(&slot) {
                if *current_owner != master.node_id {
                    migrations_by_source
                        .entry(current_owner.clone())
                        .or_default()
                        .push((slot, master.node_id.clone()));
                }
            } else {
                unassigned_slots
                    .entry(master.node_id.clone())
                    .or_default()
                    .push(slot);
            }
        }
    }

    for (source_node_id, slot_migrations) in migrations_by_source {
        let mut by_dest: HashMap<crate::crd::NodeId, Vec<u16>> = HashMap::new();
        for (slot, dest) in slot_migrations {
            by_dest.entry(dest).or_default().push(slot);
        }

        let Some((_, ordinal)) = node_info.get(&source_node_id) else {
            return Err(ValkeyError::InvalidConfig(format!(
                "Cannot find ordinal for source node {}",
                source_node_id
            )));
        };
        let ordinal = *ordinal;

        info!(
            source_node = %source_node_id,
            ordinal = %ordinal,
            destinations = by_dest.len(),
            "Connecting to source node for slot migration"
        );

        let source_client = ctx
            .connect_to_cluster(obj, namespace, ordinal)
            .await
            .map_err(|e| {
                ValkeyError::Connection(format!(
                    "Failed to connect to source node {} (ordinal {}): {}",
                    source_node_id, ordinal, e
                ))
            })?;

        for (dest_node_id, mut slots) in by_dest {
            slots.sort_unstable();
            let ranges = group_consecutive_slots(&slots);

            info!(
                from = %source_node_id,
                to = %dest_node_id,
                total_slots = slots.len(),
                num_ranges = ranges.len(),
                "Migrating slot ranges"
            );

            for (start, end) in ranges {
                debug!(
                    start_slot = start,
                    end_slot = end,
                    from = %source_node_id,
                    to = %dest_node_id,
                    "Migrating slot range"
                );

                let target_ordinal = node_info
                    .get(&dest_node_id)
                    .map(|(_, ord)| *ord)
                    .unwrap_or_default();

                match source_client
                    .cluster_migrateslots(start, end, &dest_node_id)
                    .await
                {
                    Ok(()) => {
                        if let Err(e) = wait_for_slot_migration(
                            ctx,
                            obj,
                            namespace,
                            &source_client,
                            target_ordinal,
                            start,
                            end,
                            &dest_node_id,
                        )
                        .await
                        {
                            let _ = source_client.close().await;
                            return Err(e);
                        }
                        result.slots_moved += i32::from(end - start) + 1;
                        debug!(start, end, "Slot range migration completed");
                    }
                    Err(e) => {
                        warn!(start, end, error = %e, "Failed to migrate slot range");
                        let _ = source_client.close().await;
                        return Err(e);
                    }
                }
            }
        }

        let _ = source_client.close().await;
    }

    if !unassigned_slots.is_empty() {
        // Slots missing from current_ownership may still be owned by a master
        // that was excluded from the target set (e.g. an orphaned node with no
        // matching pod or an ordinal outside the target range). CLUSTER SETSLOT
        // only rewrites ownership metadata; reassigning such a slot would strand
        // the keys on the excluded owner and silently lose data once that node
        // is forgotten. Refuse to proceed so the operator surfaces the problem.
        let target_master_ids: std::collections::HashSet<&crate::crd::NodeId> =
            masters.iter().map(|m| &m.node_id).collect();
        let mut orphan_owners: HashMap<u16, &crate::crd::NodeId> = HashMap::new();
        for node in &cluster_nodes.nodes {
            if target_master_ids.contains(&node.node_id) {
                continue;
            }
            for range in &node.slots {
                for slot in range.start..=range.end {
                    orphan_owners.insert(slot, &node.node_id);
                }
            }
        }

        let mut conflicting: Vec<(u16, &crate::crd::NodeId)> = unassigned_slots
            .values()
            .flatten()
            .filter_map(|slot| orphan_owners.get(slot).map(|owner| (*slot, *owner)))
            .collect();
        if !conflicting.is_empty() {
            conflicting.sort_unstable_by_key(|(slot, _)| *slot);
            let owners: std::collections::HashSet<_> = conflicting
                .iter()
                .map(|(_, owner)| owner.to_string())
                .collect();
            return Err(ValkeyError::SlotMigrationFailed(format!(
                "{} slot(s) (first: {}) are owned by node(s) [{}] excluded from the \
                 target master set; reassigning them via CLUSTER SETSLOT would lose \
                 data. Recover or manually migrate these nodes before rebalancing",
                conflicting.len(),
                conflicting
                    .first()
                    .map(|(slot, _)| *slot)
                    .unwrap_or_default(),
                owners.into_iter().collect::<Vec<_>>().join(", "),
            )));
        }

        let client = ctx
            .connect_to_cluster(obj, namespace, 0)
            .await
            .map_err(|e| {
                ValkeyError::Connection(format!(
                    "Failed to connect for unassigned slot assignment: {}",
                    e
                ))
            })?;

        for (dest, mut slots) in unassigned_slots {
            slots.sort_unstable();
            for slot in slots {
                client
                    .cluster_setslot(
                        slot,
                        crate::client::valkey_client::ClusterSetSlotState::Node(dest.clone()),
                    )
                    .await?;
                result.slots_moved += 1;
            }
        }

        let _ = client.close().await;
    }

    info!(slots_moved = result.slots_moved, "Rebalance complete");
    Ok(result)
}

/// Promote replicas that should be masters (for scale-up where new pods start as replicas).
#[tracing::instrument(skip(obj, ctx), fields(name = %obj.name_any(), namespace = %namespace))]
pub(crate) async fn promote_replicas_to_masters(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    current_masters: i32,
    target_masters: i32,
) -> Result<Vec<crate::crd::PodOrdinal>, Error> {
    if target_masters <= current_masters {
        return Ok(Vec::new());
    }

    let name = obj.name_any();
    let mut promoted: Vec<crate::crd::PodOrdinal> = Vec::new();

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

    let cluster_nodes = client.cluster_nodes().await?;

    let _ = client.close().await;

    let topology =
        ClusterTopology::build(&ctx.client, namespace, &name, Some(&cluster_nodes)).await?;

    let to_promote = topology.replicas_that_should_be_masters(target_masters);

    for node in to_promote {
        info!(
            cluster = %name,
            ordinal = %node.ordinal,
            node_id = %node.node_id.as_ref().map_or("unknown", |n| n.as_str()),
            "Promoting replica to master via CLUSTER RESET SOFT"
        );

        let replica_client = ctx.connect_to_cluster(obj, namespace, node.ordinal).await?;

        match replica_client.cluster_reset(false).await {
            Ok(()) => {
                info!(cluster = %name, ordinal = %node.ordinal, "Replica promoted to master");
                promoted.push(node.ordinal);
            }
            Err(e) => {
                warn!(cluster = %name, ordinal = %node.ordinal, error = %e, "Failed to promote replica");
            }
        }

        let _ = replica_client.close().await;
    }

    if !promoted.is_empty() {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

        for &ordinal in &promoted {
            if let Some(node) = topology.by_ordinal(ordinal)
                && let Some(endpoint) = &node.endpoint
            {
                info!(
                    cluster = %name,
                    ordinal = %ordinal,
                    ip = %endpoint.ip(),
                    "Re-adding promoted node via CLUSTER MEET"
                );
                if let Err(e) = client.cluster_meet(endpoint.ip(), endpoint.port()).await {
                    warn!(cluster = %name, ordinal = %ordinal, error = %e, "Failed to CLUSTER MEET promoted node");
                }
            }
        }

        let _ = client.close().await;

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    Ok(promoted)
}

// ── private helpers ────────────────────────────────────────────────────────

fn group_consecutive_slots(slots: &[u16]) -> Vec<(u16, u16)> {
    let mut iter = slots.iter().copied();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let mut start = first;
    let mut end = first;
    for slot in iter {
        if slot == end + 1 {
            end = slot;
        } else {
            ranges.push((start, end));
            start = slot;
            end = slot;
        }
    }
    ranges.push((start, end));
    ranges
}

fn owns_all_slots_in_range(slot_ranges: &[crate::client::SlotRange], start: u16, end: u16) -> bool {
    if slot_ranges.is_empty() {
        return false;
    }

    let mut sorted_ranges: Vec<_> = slot_ranges.iter().map(|r| (r.start, r.end)).collect();
    sorted_ranges.sort_by_key(|r| r.0);

    let mut current = start;
    for (range_start, range_end) in sorted_ranges {
        if range_start > current {
            return false;
        }
        if range_start <= current && range_end >= current {
            if range_end >= end {
                return true;
            }
            current = range_end + 1;
        }
    }
    false
}

fn migrations_in_progress_for_range(
    migrations: &[crate::client::SlotMigrationInfo],
    start_slot: u16,
    end_slot: u16,
) -> bool {
    migrations
        .iter()
        .any(|m| m.is_in_progress() && m.overlaps_range(start_slot, end_slot))
}

/// Wait for a slot migration to complete using dual-strategy verification.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn wait_for_slot_migration(
    ctx: &Context,
    obj: &ValkeyCluster,
    namespace: &str,
    source_client: &crate::client::ValkeyClient,
    target_ordinal: crate::crd::PodOrdinal,
    start_slot: u16,
    end_slot: u16,
    target_node_id: &crate::crd::NodeId,
) -> Result<(), ValkeyError> {
    use std::time::{Duration, Instant};

    const MAX_WAIT_SECS: u64 = 300;
    const POLL_INTERVAL_SECS: u64 = 2;
    let start = Instant::now();

    loop {
        if start.elapsed().as_secs() > MAX_WAIT_SECS {
            return Err(ValkeyError::Timeout {
                operation: format!(
                    "Slot migration {}-{} to {} (GETSLOTMIGRATIONS check)",
                    start_slot, end_slot, target_node_id
                ),
                duration: Duration::from_secs(MAX_WAIT_SECS),
            });
        }

        match source_client.cluster_getslotmigrations().await {
            Ok(migrations) => {
                if !migrations_in_progress_for_range(&migrations, start_slot, end_slot) {
                    debug!(
                        start_slot,
                        end_slot, "Source node reports no active migrations"
                    );
                    break;
                }
                debug!(
                    start_slot,
                    end_slot, "Source node still has active migration"
                );
            }
            Err(e) => {
                debug!(error = %e, "GETSLOTMIGRATIONS failed, will verify with target node");
                break;
            }
        }

        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }

    let target_client = ctx
        .connect_to_cluster(obj, namespace, target_ordinal)
        .await
        .map_err(|e| {
            ValkeyError::Connection(format!(
                "Failed to connect to target node (ordinal {}) for slot verification: {}",
                target_ordinal, e
            ))
        })?;

    const MAX_TARGET_RETRIES: u32 = 10;
    const TARGET_RETRY_INTERVAL_SECS: u64 = 1;

    for attempt in 0..MAX_TARGET_RETRIES {
        if start.elapsed().as_secs() > MAX_WAIT_SECS {
            let _ = target_client.close().await;
            return Err(ValkeyError::Timeout {
                operation: format!(
                    "Slot migration {}-{} to {} (target verification)",
                    start_slot, end_slot, target_node_id
                ),
                duration: Duration::from_secs(MAX_WAIT_SECS),
            });
        }

        match target_client.cluster_nodes().await {
            Ok(nodes) => {
                if let Some(target) = nodes.get_node(target_node_id) {
                    if owns_all_slots_in_range(&target.slots, start_slot, end_slot) {
                        debug!(
                            start_slot,
                            end_slot,
                            target_node = %target_node_id,
                            "Slot range migration verified on target node"
                        );
                        let _ = target_client.close().await;
                        return Ok(());
                    }
                    debug!(
                        attempt,
                        target_node = %target_node_id,
                        "Target node does not yet own expected slot range"
                    );
                } else {
                    debug!(
                        attempt,
                        target_node = %target_node_id,
                        "Target node not found in cluster nodes"
                    );
                }
            }
            Err(e) => {
                debug!(attempt, error = %e, "Failed to get cluster nodes from target, will retry");
            }
        }

        tokio::time::sleep(Duration::from_secs(TARGET_RETRY_INTERVAL_SECS)).await;
    }

    let _ = target_client.close().await;
    Err(ValkeyError::SlotMigrationFailed(format!(
        "Target node {} did not receive slots {}-{} after {} retries",
        target_node_id, start_slot, end_slot, MAX_TARGET_RETRIES
    )))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    fn make_slot_range(start: u16, end: u16) -> crate::client::SlotRange {
        crate::client::SlotRange::new(start, end)
    }

    #[test]
    fn test_owns_all_slots_single_range_exact_match() {
        let ranges = vec![make_slot_range(0, 5460)];
        assert!(owns_all_slots_in_range(&ranges, 0, 5460));
    }

    #[test]
    fn test_owns_all_slots_single_range_covers_subset() {
        let ranges = vec![make_slot_range(0, 5460)];
        assert!(owns_all_slots_in_range(&ranges, 100, 500));
    }

    #[test]
    fn test_owns_all_slots_contiguous_ranges() {
        let ranges = vec![
            make_slot_range(0, 1000),
            make_slot_range(1001, 2000),
            make_slot_range(2001, 3000),
        ];
        assert!(owns_all_slots_in_range(&ranges, 0, 3000));
    }

    #[test]
    fn test_owns_all_slots_gap_in_ranges() {
        let ranges = vec![make_slot_range(0, 1000), make_slot_range(2001, 3000)];
        assert!(!owns_all_slots_in_range(&ranges, 0, 3000));
    }

    #[test]
    fn test_owns_all_slots_empty_ranges() {
        let ranges: Vec<crate::client::SlotRange> = vec![];
        assert!(!owns_all_slots_in_range(&ranges, 0, 100));
    }

    #[test]
    fn test_owns_all_slots_unsorted_ranges() {
        let ranges = vec![
            make_slot_range(2001, 3000),
            make_slot_range(0, 1000),
            make_slot_range(1001, 2000),
        ];
        assert!(owns_all_slots_in_range(&ranges, 0, 3000));
    }

    #[test]
    fn test_owns_all_slots_range_starts_after_request() {
        let ranges = vec![make_slot_range(1000, 2000)];
        assert!(!owns_all_slots_in_range(&ranges, 0, 500));
    }

    #[test]
    fn test_owns_all_slots_range_ends_before_request() {
        let ranges = vec![make_slot_range(0, 500)];
        assert!(!owns_all_slots_in_range(&ranges, 0, 1000));
    }

    #[test]
    fn test_owns_all_slots_single_slot() {
        let ranges = vec![make_slot_range(100, 100)];
        assert!(owns_all_slots_in_range(&ranges, 100, 100));
    }

    #[test]
    fn test_owns_all_slots_overlapping_ranges() {
        let ranges = vec![make_slot_range(0, 1500), make_slot_range(1000, 2000)];
        assert!(owns_all_slots_in_range(&ranges, 0, 2000));
    }

    #[test]
    fn test_migrations_in_progress_empty() {
        let migrations: Vec<crate::client::SlotMigrationInfo> = vec![];
        assert!(!migrations_in_progress_for_range(&migrations, 0, 1000));
    }

    #[test]
    fn test_migrations_in_progress_active_overlapping() {
        let migrations = vec![crate::client::SlotMigrationInfo {
            slot_range: Some((0, 2730)),
            state: Some("migrating".to_string()),
            operation: Some("EXPORT".to_string()),
        }];
        assert!(migrations_in_progress_for_range(&migrations, 0, 1000));
        assert!(migrations_in_progress_for_range(&migrations, 1000, 2000));
        assert!(migrations_in_progress_for_range(&migrations, 2000, 3000));
        assert!(!migrations_in_progress_for_range(&migrations, 3000, 4000));
    }

    #[test]
    fn test_migrations_in_progress_completed_overlapping() {
        let migrations = vec![crate::client::SlotMigrationInfo {
            slot_range: Some((0, 2730)),
            state: Some("success".to_string()),
            operation: Some("EXPORT".to_string()),
        }];
        assert!(!migrations_in_progress_for_range(&migrations, 0, 1000));
    }

    #[test]
    fn test_migrations_in_progress_multiple_migrations() {
        let migrations = vec![
            crate::client::SlotMigrationInfo {
                slot_range: Some((0, 1000)),
                state: Some("success".to_string()),
                operation: None,
            },
            crate::client::SlotMigrationInfo {
                slot_range: Some((1001, 2000)),
                state: Some("migrating".to_string()),
                operation: None,
            },
            crate::client::SlotMigrationInfo {
                slot_range: Some((2001, 3000)),
                state: Some("failed".to_string()),
                operation: None,
            },
        ];
        assert!(!migrations_in_progress_for_range(&migrations, 0, 500));
        assert!(migrations_in_progress_for_range(&migrations, 1500, 1600));
        assert!(!migrations_in_progress_for_range(&migrations, 2500, 2600));
    }

    #[test]
    fn test_migrations_in_progress_no_slot_range() {
        let migrations = vec![crate::client::SlotMigrationInfo {
            slot_range: None,
            state: Some("migrating".to_string()),
            operation: None,
        }];
        assert!(!migrations_in_progress_for_range(&migrations, 0, 1000));
    }

    #[test]
    fn test_migrations_in_progress_cancelled_state() {
        let migrations = vec![crate::client::SlotMigrationInfo {
            slot_range: Some((0, 2730)),
            state: Some("cancelled".to_string()),
            operation: Some("EXPORT".to_string()),
        }];
        assert!(!migrations_in_progress_for_range(&migrations, 0, 1000));
    }
}
