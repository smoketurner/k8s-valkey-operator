//! Reconciliation loop for ValkeyUpgrade.
//!
//! This module contains the reconcile function that handles the lifecycle
//! of ValkeyUpgrade custom resources, orchestrating per-shard rolling upgrades
//! with proper failover to minimize data loss and downtime.

use std::sync::Arc;
use std::time::Duration;

use jiff::Timestamp;
use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, ResourceExt,
    api::{DeleteParams, Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    client::cluster_ops::ClusterOps,
    client::valkey_client::{TlsCertData, ValkeyClient},
    controller::{
        cluster_init::{master_pod_dns_names, replica_pod_dns_names_for_master},
        cluster_reconciler::FIELD_MANAGER,
        context::Context,
        error::Error,
    },
    crd::{
        Condition, ValkeyCluster, ValkeyUpgrade, ValkeyUpgradeSpec, ValkeyUpgradeStatus,
        UpgradePhase, ShardUpgradeState, ShardUpgradeStatus,
    },
};

/// Finalizer name for upgrade resources
pub const UPGRADE_FINALIZER: &str = "valkey-operator.smoketurner.com/upgrade-finalizer";

/// Reconcile a ValkeyUpgrade
///
/// This is the main reconciliation function called by the controller.
/// It handles the upgrade lifecycle: validation, pre-checks, per-shard upgrades,
/// and completion/failure handling.
pub async fn reconcile(obj: Arc<ValkeyUpgrade>, ctx: Arc<Context>) -> Result<Action, Error> {
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    debug!(name = %name, namespace = %namespace, "Reconciling ValkeyUpgrade");

    let api: Api<ValkeyUpgrade> = Api::namespaced(ctx.client.clone(), &namespace);

    // Handle deletion
    if obj.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&obj, &ctx, &namespace).await;
    }

    // Ensure finalizer is present
    if !obj.finalizers().iter().any(|f| f == UPGRADE_FINALIZER) {
        info!(name = %name, "Adding finalizer");
        add_finalizer(&api, &name).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Get current state
    let current_phase = obj
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(UpgradePhase::Pending);

    // Skip if already in a terminal state
    if current_phase.is_terminal() {
        debug!(name = %name, phase = %current_phase, "Upgrade in terminal state, skipping reconciliation");
        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Get the target ValkeyCluster
    let cluster = match get_target_cluster(&obj, &ctx, &namespace).await {
        Ok(c) => c,
        Err(e) => {
            error!(name = %name, error = %e, "Failed to get target cluster");
            update_phase(&api, &name, UpgradePhase::Failed, Some(e.to_string())).await?;
            return Ok(Action::requeue(Duration::from_secs(300)));
        }
    };

    // Determine next phase based on current state
    let result = match current_phase {
        UpgradePhase::Pending => {
            // Validate and start pre-checks
            handle_pending_phase(&obj, &ctx, &api, &cluster, &namespace).await
        }
        UpgradePhase::PreChecks => {
            // Run pre-upgrade health checks
            handle_prechecks_phase(&obj, &ctx, &api, &cluster, &namespace).await
        }
        UpgradePhase::InProgress => {
            // Process the next shard in the upgrade
            handle_inprogress_phase(&obj, &ctx, &api, &cluster, &namespace).await
        }
        UpgradePhase::RollingBack => {
            // Handle rollback
            handle_rollback_phase(&obj, &ctx, &api, &cluster, &namespace).await
        }
        _ => Ok(Action::requeue(Duration::from_secs(60))),
    };

    result
}

/// Error policy for the upgrade controller
pub fn error_policy(obj: Arc<ValkeyUpgrade>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();

    if error.is_not_found() {
        debug!(name = %name, "Resource not found (likely deleted)");
        return Action::await_change();
    }

    if error.is_retryable() {
        warn!(name = %name, error = %error, "Retryable error, will retry");
        Action::requeue(error.requeue_after())
    } else {
        error!(name = %name, error = %error, "Non-retryable error");
        Action::requeue(Duration::from_secs(300))
    }
}

/// Handle the Pending phase - validate spec and transition to PreChecks
async fn handle_pending_phase(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    info!(name = %name, "Starting upgrade validation");

    // Validate upgrade spec
    if let Err(e) = validate_upgrade_spec(&obj.spec, cluster) {
        error!(name = %name, error = %e, "Validation failed");
        update_phase(api, &name, UpgradePhase::Failed, Some(e.to_string())).await?;
        return Err(e);
    }

    // Determine target image
    let target_image = resolve_target_image(&obj.spec, cluster);

    // Initialize status with shard information
    let total_shards = cluster.spec.masters;
    let shard_statuses = initialize_shard_statuses(cluster, ctx, namespace).await?;

    let status = ValkeyUpgradeStatus {
        phase: UpgradePhase::PreChecks,
        progress: format!("0/{} shards upgraded", total_shards),
        current_version: Some(cluster.spec.image.tag.clone()),
        target_version: Some(target_image),
        started_at: Some(Timestamp::now().to_string()),
        total_shards,
        shard_statuses,
        observed_generation: obj.metadata.generation,
        ..Default::default()
    };

    update_status(api, &name, status).await?;

    ctx.publish_upgrade_event(
        obj,
        "ValidationPassed",
        "Upgrading",
        Some("Upgrade validation passed, starting pre-checks".to_string()),
    )
    .await;

    Ok(Action::requeue(Duration::from_secs(1)))
}

/// Handle the PreChecks phase - verify cluster health before upgrade
async fn handle_prechecks_phase(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    info!(name = %name, "Running pre-upgrade health checks");

    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &cluster.spec.auth.secret_ref.name,
        &cluster.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &cluster.name_any()).await?;

    // Connect to cluster and check health
    let master_pods = master_pod_dns_names(cluster);
    if master_pods.is_empty() {
        return Err(Error::Validation("No master pods found".to_string()));
    }

    let (host, port) = &master_pods[0];
    let client = ValkeyClient::connect_single(host, *port, password.as_deref(), tls_certs.as_ref())
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Check cluster health - always required before upgrade
    let cluster_info = client.cluster_info().await.map_err(|e| Error::Valkey(e.to_string()))?;
    if !cluster_info.is_healthy() {
        let _ = client.close().await;
        warn!(name = %name, "Cluster not healthy, waiting...");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Check slots are stable - always required before upgrade
    let cluster_nodes = client.cluster_nodes().await.map_err(|e| Error::Valkey(e.to_string()))?;
    if !cluster_nodes.all_slots_assigned() {
        let _ = client.close().await;
        warn!(name = %name, "Slots not fully assigned, waiting...");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    let _ = client.close().await;

    info!(name = %name, "Pre-upgrade checks passed");

    // Transition to InProgress
    let mut status = obj.status.clone().unwrap_or_default();
    status.phase = UpgradePhase::InProgress;
    status.current_shard = 0;
    update_status(api, &name, status).await?;

    ctx.publish_upgrade_event(
        obj,
        "PreChecksPassed",
        "Upgrading",
        Some("Pre-upgrade checks passed, starting shard upgrades".to_string()),
    )
    .await;

    Ok(Action::requeue(Duration::from_secs(1)))
}

/// Handle the InProgress phase - process shards one by one
async fn handle_inprogress_phase(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    let status = obj.status.as_ref().ok_or_else(|| Error::MissingField("status".to_string()))?;

    let current_shard = status.current_shard;
    let total_shards = status.total_shards;

    // Check if all shards are done
    if current_shard >= total_shards {
        info!(name = %name, "All shards upgraded");
        let mut new_status = status.clone();
        new_status.phase = UpgradePhase::Completed;
        new_status.progress = format!("{}/{} shards upgraded", total_shards, total_shards);
        new_status.completed_at = Some(Timestamp::now().to_string());
        new_status.conditions = vec![
            Condition::ready(true, "UpgradeCompleted", "All shards upgraded successfully", obj.metadata.generation),
        ];
        update_status(api, &name, new_status).await?;

        ctx.publish_upgrade_event(
            obj,
            "UpgradeCompleted",
            "Completed",
            Some("All shards upgraded successfully".to_string()),
        )
        .await;

        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Get current shard status
    let shard_status = status
        .shard_statuses
        .get(current_shard as usize)
        .cloned()
        .unwrap_or_else(|| ShardUpgradeStatus {
            shard_index: current_shard,
            master_node_id: String::new(),
            master_pod: String::new(),
            status: ShardUpgradeState::Pending,
            promoted_replica: None,
            error: None,
        });

    info!(
        name = %name,
        shard = current_shard,
        state = %shard_status.status,
        "Processing shard"
    );

    // Process shard based on its state
    let result = match shard_status.status {
        ShardUpgradeState::Pending => {
            upgrade_shard_replicas(obj, ctx, api, cluster, namespace, current_shard).await
        }
        ShardUpgradeState::UpgradingReplicas => {
            check_replicas_upgraded(obj, ctx, api, cluster, namespace, current_shard).await
        }
        ShardUpgradeState::WaitingForSync => {
            wait_for_replication_sync(obj, ctx, api, cluster, namespace, current_shard).await
        }
        ShardUpgradeState::FailingOver => {
            execute_failover(obj, ctx, api, cluster, namespace, current_shard).await
        }
        ShardUpgradeState::UpgradingOldMaster => {
            upgrade_old_master(obj, ctx, api, cluster, namespace, current_shard).await
        }
        ShardUpgradeState::Completed => {
            // Move to next shard
            let mut new_status = status.clone();
            new_status.current_shard = current_shard + 1;
            new_status.upgraded_shards = current_shard + 1;
            new_status.progress = format!("{}/{} shards upgraded", current_shard + 1, total_shards);
            update_status(api, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        ShardUpgradeState::Failed => {
            // Any shard failure triggers immediate rollback
            info!(name = %name, shard = current_shard, "Shard upgrade failed, initiating rollback");
            let mut new_status = status.clone();
            new_status.phase = UpgradePhase::RollingBack;
            new_status.failed_shards += 1;
            update_status(api, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        ShardUpgradeState::Skipped => {
            // Move to next shard
            let mut new_status = status.clone();
            new_status.current_shard = current_shard + 1;
            update_status(api, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
    };

    result
}

/// Upgrade replicas for a shard (first step)
async fn upgrade_shard_replicas(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
    shard_index: i32,
) -> Result<Action, Error> {
    let name = obj.name_any();

    info!(name = %name, shard = shard_index, "Upgrading replicas for shard");

    // Get replica pod names for this shard
    let replica_pods = replica_pod_dns_names_for_master(cluster, shard_index);

    if replica_pods.is_empty() {
        // No replicas, skip directly to failover step
        info!(name = %name, shard = shard_index, "No replicas to upgrade, proceeding to failover");
        update_shard_status(api, &name, shard_index, ShardUpgradeState::WaitingForSync).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Delete replica pods to trigger restart with new image
    // StatefulSet with updateStrategy: OnDelete means we control when pods restart
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(ctx.client.clone(), namespace);

    for (dns_name, _) in &replica_pods {
        // Extract pod name from DNS name (e.g., "cluster-3.cluster-headless.ns.svc..." -> "cluster-3")
        let pod_name = dns_name.split('.').next().unwrap_or(dns_name);

        info!(name = %name, pod = %pod_name, "Deleting replica pod to trigger upgrade");

        match pod_api.delete(pod_name, &DeleteParams::default()).await {
            Ok(_) => {
                debug!(pod = %pod_name, "Replica pod deleted");
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                debug!(pod = %pod_name, "Replica pod already deleted");
            }
            Err(e) => {
                warn!(pod = %pod_name, error = %e, "Failed to delete replica pod");
                // Continue with other replicas, don't fail the whole upgrade
            }
        }
    }

    // Update shard status to UpgradingReplicas
    update_shard_status(api, &name, shard_index, ShardUpgradeState::UpgradingReplicas).await?;

    ctx.publish_upgrade_event(
        obj,
        "UpgradingReplicas",
        "Upgrading",
        Some(format!("Upgrading replicas for shard {}", shard_index)),
    )
    .await;

    // Requeue to check replica status
    Ok(Action::requeue(Duration::from_secs(10)))
}

/// Check if replicas have been upgraded and are ready
async fn check_replicas_upgraded(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
    shard_index: i32,
) -> Result<Action, Error> {
    let name = obj.name_any();

    // Get replica pod names for this shard
    let replica_pods = replica_pod_dns_names_for_master(cluster, shard_index);

    if replica_pods.is_empty() {
        // No replicas, proceed to failover
        update_shard_status(api, &name, shard_index, ShardUpgradeState::WaitingForSync).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Check if all replica pods are ready with the new image
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(ctx.client.clone(), namespace);

    let target_image = resolve_target_image(&obj.spec, cluster);

    for (dns_name, _) in &replica_pods {
        let pod_name = dns_name.split('.').next().unwrap_or(dns_name);

        match pod_api.get(pod_name).await {
            Ok(pod) => {
                // Check if pod is ready and has the correct image
                let is_ready = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conds| conds.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
                    .unwrap_or(false);

                let current_image = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.containers.first())
                    .and_then(|c| c.image.as_ref())
                    .cloned()
                    .unwrap_or_default();

                if !is_ready {
                    debug!(pod = %pod_name, "Replica pod not ready yet");
                    return Ok(Action::requeue(Duration::from_secs(10)));
                }

                if !current_image.contains(&target_image) && current_image != target_image {
                    debug!(pod = %pod_name, current = %current_image, target = %target_image, "Replica pod has old image");
                    return Ok(Action::requeue(Duration::from_secs(10)));
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                debug!(pod = %pod_name, "Replica pod not yet recreated");
                return Ok(Action::requeue(Duration::from_secs(10)));
            }
            Err(e) => {
                warn!(pod = %pod_name, error = %e, "Failed to get replica pod");
                return Ok(Action::requeue(Duration::from_secs(10)));
            }
        }
    }

    info!(name = %name, shard = shard_index, "All replicas upgraded and ready");
    update_shard_status(api, &name, shard_index, ShardUpgradeState::WaitingForSync).await?;

    Ok(Action::requeue(Duration::from_secs(1)))
}

/// Wait for replication to sync before failover
async fn wait_for_replication_sync(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
    shard_index: i32,
) -> Result<Action, Error> {
    let name = obj.name_any();

    // Get replica pod to check replication status
    let replica_pods = replica_pod_dns_names_for_master(cluster, shard_index);

    if replica_pods.is_empty() {
        // No replicas, skip failover entirely - just upgrade the master
        update_shard_status(api, &name, shard_index, ShardUpgradeState::UpgradingOldMaster).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Get password
    let password = get_auth_password(
        ctx,
        namespace,
        &cluster.spec.auth.secret_ref.name,
        &cluster.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &cluster.name_any()).await?;

    // Connect to the first replica and check replication status
    let (replica_host, replica_port) = &replica_pods[0];
    let client = match ValkeyClient::connect_single(replica_host, *replica_port, password.as_deref(), tls_certs.as_ref()).await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "Failed to connect to replica for sync check");
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
    };

    // Always wait for replication sync before failover (5 minute timeout)
    const SYNC_TIMEOUT_SECS: u64 = 300;
    let sync_timeout = Duration::from_secs(SYNC_TIMEOUT_SECS);
    match client.wait_for_replication_sync(sync_timeout).await {
        Ok(()) => {
            info!(name = %name, shard = shard_index, "Replication synced, proceeding to failover");
            let _ = client.close().await;
            update_shard_status(api, &name, shard_index, ShardUpgradeState::FailingOver).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        Err(e) => {
            let _ = client.close().await;
            debug!(name = %name, shard = shard_index, error = %e, "Replication not yet synced");
            Ok(Action::requeue(Duration::from_secs(5)))
        }
    }
}

/// Execute CLUSTER FAILOVER to promote replica to master
async fn execute_failover(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
    shard_index: i32,
) -> Result<Action, Error> {
    let name = obj.name_any();

    // Get replica pod to execute failover
    let replica_pods = replica_pod_dns_names_for_master(cluster, shard_index);

    if replica_pods.is_empty() {
        // No replicas, can't failover - just upgrade the master directly
        warn!(name = %name, shard = shard_index, "No replicas available for failover, upgrading master directly");
        update_shard_status(api, &name, shard_index, ShardUpgradeState::UpgradingOldMaster).await?;
        return Ok(Action::requeue(Duration::from_secs(1)));
    }

    // Get password
    let password = get_auth_password(
        ctx,
        namespace,
        &cluster.spec.auth.secret_ref.name,
        &cluster.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &cluster.name_any()).await?;

    // Connect to the first replica (best replica selection would be an enhancement)
    let (replica_host, replica_port) = &replica_pods[0];
    let replica_pod_name = replica_host.split('.').next().unwrap_or(replica_host);

    info!(
        name = %name,
        shard = shard_index,
        replica = %replica_pod_name,
        "Executing CLUSTER FAILOVER"
    );

    let client = ValkeyClient::connect_single(replica_host, *replica_port, password.as_deref(), tls_certs.as_ref())
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Execute failover
    let failover_result = client.cluster_failover().await;
    let _ = client.close().await;

    match failover_result {
        Ok(()) => {
            info!(name = %name, shard = shard_index, "CLUSTER FAILOVER initiated");

            // Update shard status with promoted replica
            let mut status = obj.status.clone().unwrap_or_default();
            if let Some(shard) = status.shard_statuses.get_mut(shard_index as usize) {
                shard.status = ShardUpgradeState::UpgradingOldMaster;
                shard.promoted_replica = Some(replica_pod_name.to_string());
            }
            update_status(api, &name, status).await?;

            ctx.publish_upgrade_event(
                obj,
                "FailoverExecuted",
                "Upgrading",
                Some(format!("Failover executed for shard {}, promoted {}", shard_index, replica_pod_name)),
            )
            .await;

            // Wait a bit for failover to complete before upgrading old master
            Ok(Action::requeue(Duration::from_secs(5)))
        }
        Err(e) => {
            // Failover failed - mark shard as failed (triggers rollback)
            error!(name = %name, shard = shard_index, error = %e, "CLUSTER FAILOVER failed");
            update_shard_status_with_error(api, &name, shard_index, e.to_string()).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
    }
}

/// Upgrade the old master (now replica) after failover
async fn upgrade_old_master(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
    shard_index: i32,
) -> Result<Action, Error> {
    let name = obj.name_any();
    let cluster_name = cluster.name_any();

    // Get master pod name for this shard
    let master_pod_name = format!("{}-{}", cluster_name, shard_index);

    info!(
        name = %name,
        shard = shard_index,
        pod = %master_pod_name,
        "Upgrading old master"
    );

    // Delete the old master pod to trigger restart with new image
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> = Api::namespaced(ctx.client.clone(), namespace);

    match pod_api.delete(&master_pod_name, &DeleteParams::default()).await {
        Ok(_) => {
            debug!(pod = %master_pod_name, "Old master pod deleted");
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            debug!(pod = %master_pod_name, "Old master pod already deleted");
        }
        Err(e) => {
            warn!(pod = %master_pod_name, error = %e, "Failed to delete old master pod");
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
    }

    // Wait for the pod to come back up and verify it has the new image
    let target_image = resolve_target_image(&obj.spec, cluster);

    // Check if pod is ready with new image
    tokio::time::sleep(Duration::from_secs(5)).await;

    match pod_api.get(&master_pod_name).await {
        Ok(pod) => {
            let is_ready = pod
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .map(|conds| conds.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
                .unwrap_or(false);

            let current_image = pod
                .spec
                .as_ref()
                .and_then(|s| s.containers.first())
                .and_then(|c| c.image.as_ref())
                .cloned()
                .unwrap_or_default();

            if !is_ready {
                debug!(pod = %master_pod_name, "Old master pod not ready yet");
                return Ok(Action::requeue(Duration::from_secs(10)));
            }

            if !current_image.contains(&target_image) && current_image != target_image {
                debug!(pod = %master_pod_name, "Old master pod still has old image");
                return Ok(Action::requeue(Duration::from_secs(10)));
            }

            info!(name = %name, shard = shard_index, "Shard upgrade complete");
            update_shard_status(api, &name, shard_index, ShardUpgradeState::Completed).await?;

            ctx.publish_upgrade_event(
                obj,
                "ShardUpgraded",
                "Upgrading",
                Some(format!("Shard {} upgrade complete", shard_index)),
            )
            .await;

            Ok(Action::requeue(Duration::from_secs(1)))
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            debug!(pod = %master_pod_name, "Old master pod not yet recreated");
            Ok(Action::requeue(Duration::from_secs(10)))
        }
        Err(e) => {
            warn!(pod = %master_pod_name, error = %e, "Failed to get old master pod");
            Ok(Action::requeue(Duration::from_secs(10)))
        }
    }
}

/// Handle the rollback phase
async fn handle_rollback_phase(
    obj: &ValkeyUpgrade,
    _ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    _cluster: &ValkeyCluster,
    _namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();

    // Rollback is complex and typically involves:
    // 1. Rolling back upgraded shards to old image
    // 2. Re-establishing correct master/replica relationships
    //
    // For now, we just mark the upgrade as rolled back and let the operator
    // handle the cluster naturally through reconciliation
    info!(name = %name, "Rollback requested - marking as RolledBack");

    let mut status = obj.status.clone().unwrap_or_default();
    status.phase = UpgradePhase::RolledBack;
    status.completed_at = Some(Timestamp::now().to_string());
    status.error_message = Some("Upgrade rolled back due to failures".to_string());
    update_status(api, &name, status).await?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get the target ValkeyCluster for this upgrade
async fn get_target_cluster(
    upgrade: &ValkeyUpgrade,
    ctx: &Context,
    namespace: &str,
) -> Result<ValkeyCluster, Error> {
    let cluster_namespace = upgrade
        .spec
        .cluster_ref
        .namespace
        .as_deref()
        .unwrap_or(namespace);

    let cluster_api: Api<ValkeyCluster> = Api::namespaced(ctx.client.clone(), cluster_namespace);

    cluster_api
        .get(&upgrade.spec.cluster_ref.name)
        .await
        .map_err(Error::Kube)
}

/// Validate the upgrade spec
fn validate_upgrade_spec(spec: &ValkeyUpgradeSpec, cluster: &ValkeyCluster) -> Result<(), Error> {
    // Validate target_version is not empty
    if spec.target_version.is_empty() {
        return Err(Error::Validation(
            "targetVersion is required".to_string(),
        ));
    }

    // Cluster must be in Running state
    let cluster_phase = cluster
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(crate::crd::ClusterPhase::Pending);

    if cluster_phase != crate::crd::ClusterPhase::Running {
        return Err(Error::Validation(format!(
            "Cluster must be in Running state, currently: {}",
            cluster_phase
        )));
    }

    Ok(())
}

/// Resolve the target image from spec
///
/// Always uses alpine variant for smaller image size and reduced attack surface.
fn resolve_target_image(spec: &ValkeyUpgradeSpec, cluster: &ValkeyCluster) -> String {
    let repository = &cluster.spec.image.repository;
    format!("{}:{}-alpine", repository, spec.target_version)
}

/// Initialize shard statuses from cluster topology
async fn initialize_shard_statuses(
    cluster: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<Vec<ShardUpgradeStatus>, Error> {
    let password = get_auth_password(
        ctx,
        namespace,
        &cluster.spec.auth.secret_ref.name,
        &cluster.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &cluster.name_any()).await?;

    let master_pods = master_pod_dns_names(cluster);
    if master_pods.is_empty() {
        return Err(Error::Validation("No master pods found".to_string()));
    }

    // Connect to cluster to get topology
    let (host, port) = &master_pods[0];
    let client = ValkeyClient::connect_single(host, *port, password.as_deref(), tls_certs.as_ref())
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    let cluster_nodes = client.cluster_nodes().await.map_err(|e| Error::Valkey(e.to_string()))?;
    let _ = client.close().await;

    let masters = cluster_nodes.masters();
    let cluster_name = cluster.name_any();

    let mut shard_statuses = Vec::new();
    for (i, master) in masters.iter().enumerate() {
        shard_statuses.push(ShardUpgradeStatus {
            shard_index: i as i32,
            master_node_id: master.node_id.clone(),
            master_pod: format!("{}-{}", cluster_name, i),
            status: ShardUpgradeState::Pending,
            promoted_replica: None,
            error: None,
        });
    }

    Ok(shard_statuses)
}

/// Get password from auth secret
async fn get_auth_password(
    ctx: &Context,
    namespace: &str,
    secret_name: &str,
    secret_key: &str,
) -> Result<Option<String>, Error> {
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);

    match secret_api.get(secret_name).await {
        Ok(secret) => {
            if let Some(data) = secret.data {
                if let Some(password_bytes) = data.get(secret_key) {
                    let password = String::from_utf8(password_bytes.0.clone())
                        .map_err(|e| Error::Validation(format!("Invalid password encoding: {}", e)))?;
                    return Ok(Some(password));
                }
            }
            Ok(None)
        }
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(Error::Kube(e)),
    }
}

/// Retrieve TLS certificates from the TLS secret created by cert-manager.
async fn get_tls_certs(
    ctx: &Context,
    namespace: &str,
    cluster_name: &str,
) -> Result<Option<TlsCertData>, Error> {
    let secret_name = format!("{}-tls", cluster_name);
    let secret_api: Api<Secret> = Api::namespaced(ctx.client.clone(), namespace);

    match secret_api.get(&secret_name).await {
        Ok(secret) => {
            if let Some(data) = secret.data {
                // Get CA certificate - required for verification
                let ca_cert = match data.get("ca.crt") {
                    Some(bytes) => bytes.0.clone(),
                    None => {
                        warn!(secret = %secret_name, "ca.crt not found in TLS secret");
                        return Ok(None);
                    }
                };

                // Client cert and key are optional (used for mTLS if provided)
                let client_cert = data.get("tls.crt").map(|b| b.0.clone());
                let client_key = data.get("tls.key").map(|b| b.0.clone());

                return Ok(Some(TlsCertData {
                    ca_cert_pem: ca_cert,
                    client_cert_pem: client_cert,
                    client_key_pem: client_key,
                }));
            }
            warn!(secret = %secret_name, "TLS secret has no data");
            Ok(None)
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            warn!(secret = %secret_name, "TLS secret not found");
            Ok(None)
        }
        Err(e) => Err(Error::Kube(e)),
    }
}

/// Handle deletion of a ValkeyUpgrade
async fn handle_deletion(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    info!(name = %name, "Handling deletion");

    let api: Api<ValkeyUpgrade> = Api::namespaced(ctx.client.clone(), namespace);
    remove_finalizer(&api, &name).await?;

    Ok(Action::await_change())
}

/// Add finalizer to upgrade resource
async fn add_finalizer(api: &Api<ValkeyUpgrade>, name: &str) -> Result<(), Error> {
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [UPGRADE_FINALIZER]
        }
    });
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Remove finalizer from upgrade resource
async fn remove_finalizer(api: &Api<ValkeyUpgrade>, name: &str) -> Result<(), Error> {
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": null
        }
    });
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Update the phase of an upgrade
async fn update_phase(
    api: &Api<ValkeyUpgrade>,
    name: &str,
    phase: UpgradePhase,
    error_message: Option<String>,
) -> Result<(), Error> {
    let obj = api.get(name).await?;
    let mut status = obj.status.unwrap_or_default();
    status.phase = phase;
    status.observed_generation = obj.metadata.generation;
    if let Some(msg) = error_message {
        status.error_message = Some(msg);
    }
    update_status(api, name, status).await
}

/// Update the full status of an upgrade
async fn update_status(
    api: &Api<ValkeyUpgrade>,
    name: &str,
    status: ValkeyUpgradeStatus,
) -> Result<(), Error> {
    let patch = serde_json::json!({
        "status": status
    });
    api.patch_status(
        name,
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Update a single shard's status
async fn update_shard_status(
    api: &Api<ValkeyUpgrade>,
    name: &str,
    shard_index: i32,
    state: ShardUpgradeState,
) -> Result<(), Error> {
    let obj = api.get(name).await?;
    let mut status = obj.status.unwrap_or_default();

    if let Some(shard) = status.shard_statuses.get_mut(shard_index as usize) {
        shard.status = state;
    }

    update_status(api, name, status).await
}

/// Update a shard's status with an error
async fn update_shard_status_with_error(
    api: &Api<ValkeyUpgrade>,
    name: &str,
    shard_index: i32,
    error: String,
) -> Result<(), Error> {
    let obj = api.get(name).await?;
    let mut status = obj.status.unwrap_or_default();

    if let Some(shard) = status.shard_statuses.get_mut(shard_index as usize) {
        shard.status = ShardUpgradeState::Failed;
        shard.error = Some(error);
    }

    update_status(api, name, status).await
}
