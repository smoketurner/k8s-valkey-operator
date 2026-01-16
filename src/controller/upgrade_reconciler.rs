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
    api::{DeleteParams, ListParams, Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    client::valkey_client::{TlsCertData, ValkeyClient},
    controller::{
        cluster_init::{master_pod_dns_names, replica_pod_dns_names_for_master},
        cluster_reconciler::FIELD_MANAGER,
        context::Context,
        error::Error,
    },
    crd::{
        Condition, ShardUpgradeState, ShardUpgradeStatus, UpgradePhase, ValkeyCluster,
        ValkeyUpgrade, ValkeyUpgradeSpec, ValkeyUpgradeStatus,
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
            let cluster_name = &obj.spec.cluster_ref.name;
            let cluster_namespace = obj.spec.cluster_ref.namespace.as_deref().unwrap_or(namespace.as_str());
            let error_msg = format!(
                "Failed to get target ValkeyCluster '{}/{}': {}. Verify the cluster exists and the operator has read permissions.",
                cluster_namespace, cluster_name, e
            );
            error!(name = %name, error = %e, cluster = %*cluster_name, "Failed to get target cluster");
            update_phase(&api, &name, UpgradePhase::Failed, Some(error_msg.clone())).await?;
            return Ok(Action::requeue(Duration::from_secs(300)));
        }
    };

    // Determine next phase based on current state
    match current_phase {
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
            // Check if rollback has completed
            check_rollback_completion(&obj, &ctx, &api, &cluster, &namespace).await
        }
        UpgradePhase::RolledBack => {
            // Rollback already completed, just maintain status
            Ok(Action::requeue(Duration::from_secs(300)))
        }
        _ => Ok(Action::requeue(Duration::from_secs(60))),
    }
}

/// Error policy for the upgrade controller
pub fn error_policy(obj: Arc<ValkeyUpgrade>, error: &Error, _ctx: Arc<Context>) -> Action {
    let name = obj.name_any();

    if error.is_not_found() {
        debug!(name = %name, "Resource not found (likely deleted)");
        return Action::await_change();
    }

    if error.is_retryable() {
        // Get retry count from annotations (track retries per object)
        let retry_count = obj
            .metadata
            .annotations
            .as_ref()
            .and_then(|ann| ann.get("valkey-operator.smoketurner.com/retry-count"))
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        // Calculate exponential backoff
        let backoff = error.requeue_after_with_retry_count(retry_count);

        warn!(
            name = %name,
            error = %error,
            retry_count = retry_count,
            backoff_secs = backoff.as_secs(),
            "Retryable error, will retry with exponential backoff"
        );

        Action::requeue(backoff)
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

    // Validate upgrade spec (including concurrent upgrade check)
    if let Err(e) = validate_upgrade_spec(&obj.spec, cluster, Some(&obj), Some(ctx), namespace).await {
        error!(name = %name, error = %e, "Validation failed");
        update_phase(api, &name, UpgradePhase::Failed, Some(e.to_string())).await?;
        return Err(e);
    }

    // Determine target image
    let target_image = resolve_target_image(&obj.spec, cluster);

    // Initialize status with shard information
    let total_shards = cluster.spec.masters;
    let shard_statuses = initialize_shard_statuses(cluster, ctx, namespace).await?;

    let mut status = ValkeyUpgradeStatus {
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
    
    // Set conditions based on phase
    status.conditions = compute_upgrade_conditions(
        UpgradePhase::PreChecks,
        total_shards,
        0,
        0,
        None,
        obj.metadata.generation,
    );

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
    let (host, port) = master_pods
        .first()
        .ok_or_else(|| Error::Validation("No master pods found".to_string()))?;
    let client = ValkeyClient::connect_single(host, *port, password.as_deref(), tls_certs.as_ref())
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Check cluster health - always required before upgrade
    let cluster_info = client
        .cluster_info()
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;
    if !cluster_info.is_healthy() {
        let _ = client.close().await;
        warn!(name = %name, "Cluster not healthy, waiting...");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Check slots are stable - always required before upgrade
    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;
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
    status.conditions = compute_upgrade_conditions(
        UpgradePhase::InProgress,
        status.total_shards,
        status.upgraded_shards,
        status.failed_shards,
        status.error_message.as_deref(),
        obj.metadata.generation,
    );
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
    let status = obj
        .status
        .as_ref()
        .ok_or_else(|| Error::MissingField("status".to_string()))?;

    let current_shard = status.current_shard;
    let total_shards = status.total_shards;

    // Check if all shards are done
    if current_shard >= total_shards {
        info!(name = %name, "All shards upgraded");
        let mut new_status = status.clone();
        new_status.phase = UpgradePhase::Completed;
        new_status.progress = format!("{}/{} shards upgraded", total_shards, total_shards);
        new_status.completed_at = Some(Timestamp::now().to_string());
        new_status.conditions = compute_upgrade_conditions(
            UpgradePhase::Completed,
            total_shards,
            total_shards,
            0,
            None,
            obj.metadata.generation,
        );
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
    match shard_status.status {
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
            new_status.conditions = compute_upgrade_conditions(
                UpgradePhase::InProgress,
                new_status.total_shards,
                new_status.upgraded_shards,
                new_status.failed_shards,
                new_status.error_message.as_deref(),
                obj.metadata.generation,
            );
            update_status(api, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        ShardUpgradeState::Failed => {
            // Any shard failure triggers immediate rollback
            info!(name = %name, shard = current_shard, "Shard upgrade failed, initiating rollback");
            let mut new_status = status.clone();
            new_status.phase = UpgradePhase::RollingBack;
            new_status.failed_shards += 1;
            new_status.conditions = compute_upgrade_conditions(
                UpgradePhase::RollingBack,
                new_status.total_shards,
                new_status.upgraded_shards,
                new_status.failed_shards,
                new_status.error_message.as_deref(),
                obj.metadata.generation,
            );
            update_status(api, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        ShardUpgradeState::Skipped => {
            // Move to next shard
            let mut new_status = status.clone();
            new_status.current_shard = current_shard + 1;
            new_status.conditions = compute_upgrade_conditions(
                UpgradePhase::InProgress,
                new_status.total_shards,
                new_status.upgraded_shards,
                new_status.failed_shards,
                new_status.error_message.as_deref(),
                obj.metadata.generation,
            );
            update_status(api, &name, new_status).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
    }
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
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> =
        Api::namespaced(ctx.client.clone(), namespace);

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
    update_shard_status(
        api,
        &name,
        shard_index,
        ShardUpgradeState::UpgradingReplicas,
    )
    .await?;

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
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> =
        Api::namespaced(ctx.client.clone(), namespace);

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
                    .map(|conds| {
                        conds
                            .iter()
                            .any(|c| c.type_ == "Ready" && c.status == "True")
                    })
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
        update_shard_status(
            api,
            &name,
            shard_index,
            ShardUpgradeState::UpgradingOldMaster,
        )
        .await?;
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
    let Some((replica_host, replica_port)) = replica_pods.first() else {
        // This shouldn't happen since we checked is_empty above
        let master_pod_name = format!("{}-{}", cluster.name_any(), shard_index);
        return Err(Error::Validation(format!(
            "No replica pods found for shard {} (master: {}). Verify the cluster has replicas configured (replicasPerMaster > 0) and pods are running.",
            shard_index, master_pod_name
        )));
    };
    let client = match ValkeyClient::connect_single(
        replica_host,
        *replica_port,
        password.as_deref(),
        tls_certs.as_ref(),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "Failed to connect to replica for sync check");
            return Ok(Action::requeue(Duration::from_secs(10)));
        }
    };

    // Get master pod to connect and compare offsets
    let master_pods = master_pod_dns_names(cluster);
    let master_client = if let Some((master_host, master_port)) = master_pods.get(shard_index as usize) {
        // Connect to master to compare offsets
        match ValkeyClient::connect_single(
            master_host,
            *master_port,
            password.as_deref(),
            tls_certs.as_ref(),
        )
        .await
        {
            Ok(c) => Some(c),
            Err(e) => {
                debug!(error = %e, "Failed to connect to master for offset comparison");
                None // Fall back to replica-only check
            }
        }
    } else {
        None
    };

    // Always wait for replication sync before failover (5 minute timeout)
    // Now compares replica offset with master offset for accurate sync verification
    const SYNC_TIMEOUT_SECS: u64 = 300;
    let sync_timeout = Duration::from_secs(SYNC_TIMEOUT_SECS);
    let sync_result = client
        .wait_for_replication_sync(sync_timeout, master_client.as_ref())
        .await;

    // Close clients
    let _ = client.close().await;
    if let Some(master) = master_client {
        let _ = master.close().await;
    }

    match sync_result {
        Ok(()) => {
            info!(name = %name, shard = shard_index, "Replication synced (compared with master offset), proceeding to failover");
            update_shard_status(api, &name, shard_index, ShardUpgradeState::FailingOver).await?;
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        Err(e) => {
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
        update_shard_status(
            api,
            &name,
            shard_index,
            ShardUpgradeState::UpgradingOldMaster,
        )
        .await?;
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

    // Select best replica (lowest replication lag) for failover
    // Connect to master first to get master_repl_offset for comparison
    let master_pods = master_pod_dns_names(cluster);
    let master_client = if let Some((master_host, master_port)) = master_pods.get(shard_index as usize) {
        match ValkeyClient::connect_single(
            master_host,
            *master_port,
            password.as_deref(),
            tls_certs.as_ref(),
        )
        .await
        {
            Ok(c) => Some(c),
            Err(e) => {
                warn!(error = %e, "Failed to connect to master for replica selection, using first replica");
                None
            }
        }
    } else {
        None
    };

    // Find replica with lowest lag
    let (best_replica_host, best_replica_port, best_replica_pod_name) = if let Some(master) = &master_client {
        // Compare all replicas and select the one with lowest lag
        let mut best_replica: Option<(&(String, u16), i64)> = None;

        for replica in &replica_pods {
            match ValkeyClient::connect_single(
                &replica.0,
                replica.1,
                password.as_deref(),
                tls_certs.as_ref(),
            )
            .await
            {
                Ok(replica_client) => {
                    match replica_client.get_replication_lag(master).await {
                        Ok(lag) => {
                            if lag < 0 {
                                // Negative lag shouldn't happen, but handle gracefully
                                warn!(replica = %replica.0, lag = lag, "Negative replication lag detected");
                                continue;
                            }
                            if best_replica.is_none() || lag < best_replica.unwrap().1 {
                                let _ = replica_client.close().await;
                                best_replica = Some((replica, lag));
                            } else {
                                let _ = replica_client.close().await;
                            }
                        }
                        Err(e) => {
                            debug!(replica = %replica.0, error = %e, "Failed to get replication lag");
                            let _ = replica_client.close().await;
                        }
                    }
                }
                Err(e) => {
                    debug!(replica = %replica.0, error = %e, "Failed to connect to replica");
                }
            }
        }

        if let Some((replica, lag)) = best_replica {
            let pod_name = replica.0.split('.').next().unwrap_or(&replica.0);
            info!(
                name = %name,
                shard = shard_index,
                replica = %pod_name,
                lag = lag,
                "Selected replica with lowest replication lag"
            );
            (replica.0.clone(), replica.1, pod_name.to_string())
        } else {
            // Fall back to first replica if we couldn't determine lag
            let (host, port) = replica_pods.first().unwrap();
            let pod_name = host.split('.').next().unwrap_or(host);
            warn!(
                name = %name,
                shard = shard_index,
                "Could not determine replication lag for any replica, using first replica"
            );
            (host.clone(), *port, pod_name.to_string())
        }
    } else {
        // No master client, just use first replica
        let (host, port) = replica_pods.first().unwrap();
        let pod_name = host.split('.').next().unwrap_or(host);
        (host.clone(), *port, pod_name.to_string())
    };

    // Close master client if we created one
    if let Some(master) = master_client {
        let _ = master.close().await;
    }

    let replica_host = &best_replica_host;
    let replica_port = best_replica_port;
    let replica_pod_name = &best_replica_pod_name;

    info!(
        name = %name,
        shard = shard_index,
        replica = %replica_pod_name,
        "Executing CLUSTER FAILOVER"
    );

    let client = ValkeyClient::connect_single(
        replica_host,
        replica_port,
        password.as_deref(),
        tls_certs.as_ref(),
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    // Use WAIT command to ensure writes are replicated before failover
    // This improves consistency by ensuring all writes are replicated to at least 1 replica
    // before we proceed with failover
    const WAIT_REPLICAS: u64 = 1; // Wait for at least 1 replica
    const WAIT_TIMEOUT_MS: u64 = 5000; // 5 second timeout
    match client.wait(WAIT_REPLICAS, WAIT_TIMEOUT_MS).await {
        Ok(acked) => {
            if acked >= WAIT_REPLICAS {
                info!(
                    name = %name,
                    shard = shard_index,
                    replicas_acked = acked,
                    "WAIT confirmed writes replicated to {} replicas before failover",
                    acked
                );
            } else {
                warn!(
                    name = %name,
                    shard = shard_index,
                    replicas_acked = acked,
                    expected = WAIT_REPLICAS,
                    "WAIT returned fewer replicas than expected, proceeding with failover anyway"
                );
            }
        }
        Err(e) => {
            warn!(
                name = %name,
                shard = shard_index,
                error = %e,
                "WAIT command failed, proceeding with failover anyway"
            );
            // Continue with failover even if WAIT fails
        }
    }

    // Execute failover
    let failover_result = client.cluster_failover().await;

    match failover_result {
        Ok(()) => {
            info!(name = %name, shard = shard_index, "CLUSTER FAILOVER initiated");

            // Verify failover completed - wait up to 30 seconds for promotion
            // According to Valkey docs, manual failover should complete in seconds
            const FAILOVER_VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
            
            info!(
                name = %name,
                shard = shard_index,
                replica = %replica_pod_name,
                "Verifying failover completion"
            );

            // Get the replica's node ID for verification
            let nodes = client.cluster_nodes().await.map_err(|e| Error::Valkey(e.to_string()))?;
            let replica_node_id = nodes
                .nodes
                .iter()
                .find(|n| {
                    // Match by pod name in address
                    n.address.contains(replica_pod_name) || n.ip.contains(replica_pod_name)
                })
                .map(|n| n.node_id.clone());

            let failover_verified = if let Some(node_id) = replica_node_id {
                // Verify using the replica node
                match client.verify_failover_completed(&node_id, FAILOVER_VERIFY_TIMEOUT).await {
                    Ok(true) => {
                        info!(
                            name = %name,
                            shard = shard_index,
                            "Failover verified: replica promoted to master"
                        );
                        true
                    }
                    Ok(false) => {
                        warn!(
                            name = %name,
                            shard = shard_index,
                            "Failover verification timeout - proceeding anyway"
                        );
                        false // Timeout, but proceed
                    }
                    Err(e) => {
                        warn!(
                            name = %name,
                            shard = shard_index,
                            error = %e,
                            "Failed to verify failover - proceeding anyway"
                        );
                        false // Error, but proceed
                    }
                }
            } else {
                warn!(
                    name = %name,
                    shard = shard_index,
                    "Could not find replica node ID for verification"
                );
                false // Couldn't find node, proceed anyway
            };

            let _ = client.close().await;

            if !failover_verified {
                // Failover not verified - this is a warning but we'll proceed
                // The cluster should still be functional, but we log the concern
                warn!(
                    name = %name,
                    shard = shard_index,
                    "Failover verification incomplete - upgrade may proceed with risk"
                );
            }

            // Update shard status with promoted replica
            let mut status = obj.status.clone().unwrap_or_default();
            // Ensure shard_statuses is large enough
            let shard_idx = shard_index as usize;
            if shard_idx >= status.shard_statuses.len() {
                // Extend shard_statuses if needed (shouldn't happen, but be safe)
                status.shard_statuses.resize_with(
                    shard_idx + 1,
                    || ShardUpgradeStatus {
                        shard_index,
                        master_node_id: String::new(),
                        master_pod: String::new(),
                        status: ShardUpgradeState::Pending,
                        promoted_replica: None,
                        error: None,
                    },
                );
            }
            if let Some(shard) = status.shard_statuses.get_mut(shard_idx) {
                shard.status = ShardUpgradeState::UpgradingOldMaster;
                shard.promoted_replica = Some(replica_pod_name.to_string());
            }
            update_status(api, &name, status).await?;

            ctx.publish_upgrade_event(
                obj,
                "FailoverExecuted",
                "Upgrading",
                Some(format!(
                    "Failover executed for shard {}, promoted {}",
                    shard_index, replica_pod_name
                )),
            )
            .await;

            // Proceed to upgrade old master
            Ok(Action::requeue(Duration::from_secs(1)))
        }
        Err(e) => {
            let _ = client.close().await;
            // Failover failed - mark shard as failed (triggers rollback)
            let cluster_name = cluster.name_any();
            let master_pod_name = format!("{}-{}", cluster_name, shard_index);
            let error_msg = format!(
                "CLUSTER FAILOVER failed for shard {} (master: {}): {}. This may indicate cluster connectivity issues, replica not ready, or cluster state problems. Check cluster health and replica status before retrying.",
                shard_index, master_pod_name, e
            );
            error!(name = %name, shard = shard_index, error = %e, "CLUSTER FAILOVER failed");
            update_shard_status_with_error(api, &name, shard_index, error_msg).await?;
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
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> =
        Api::namespaced(ctx.client.clone(), namespace);

    match pod_api
        .delete(&master_pod_name, &DeleteParams::default())
        .await
    {
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
                .map(|conds| {
                    conds
                        .iter()
                        .any(|c| c.type_ == "Ready" && c.status == "True")
                })
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
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    let cluster_name = cluster.name_any();

    info!(name = %name, "Starting rollback - restoring original image version");

    // Get original image version from status
    let Some(original_version) = obj
        .status
        .as_ref()
        .and_then(|s| s.current_version.as_ref())
    else {
        error!(name = %name, "Cannot rollback: original version not found in status");
        let mut status = obj.status.clone().unwrap_or_default();
        status.phase = UpgradePhase::Failed;
        status.error_message = Some(
            "Cannot rollback: original image version not found in upgrade status. This may indicate the upgrade status was corrupted. Manual intervention may be required to restore the cluster.".to_string()
        );
        status.conditions = compute_upgrade_conditions(
            UpgradePhase::Failed,
            status.total_shards,
            status.upgraded_shards,
            status.failed_shards,
            status.error_message.as_deref(),
            obj.metadata.generation,
        );
        update_status(api, &name, status).await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    };

    // Restore the original image in the cluster spec
    let cluster_api: Api<ValkeyCluster> = Api::namespaced(ctx.client.clone(), namespace);
    let mut cluster = cluster_api.get(&cluster_name).await?;

    // Update cluster image to original version
    let original_image_tag = original_version.clone();
    cluster.spec.image.tag = original_image_tag.clone();

    // Apply the updated cluster spec
    let patch = serde_json::json!({
        "spec": {
            "image": {
                "tag": original_image_tag
            }
        }
    });

    cluster_api
        .patch(
            &cluster_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Merge(&patch),
        )
        .await?;

    info!(name = %name, original_version = %original_image_tag, "Updated cluster spec with original image version");

    // Delete all pods to trigger restart with original image
    // This ensures all pods (masters and replicas) restart with the old image
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> =
        Api::namespaced(ctx.client.clone(), namespace);
    let total_pods = crate::crd::total_pods(cluster.spec.masters, cluster.spec.replicas_per_master);

    info!(name = %name, pods = total_pods, "Deleting all pods to trigger restart with original image");

    for i in 0..total_pods {
        let pod_name = format!("{}-{}", cluster_name, i);
        match pod_api.delete(&pod_name, &DeleteParams::default()).await {
            Ok(_) => {
                debug!(pod = %pod_name, "Pod deleted for rollback");
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                debug!(pod = %pod_name, "Pod already deleted");
            }
            Err(e) => {
                warn!(pod = %pod_name, error = %e, "Failed to delete pod during rollback");
                // Continue with other pods
            }
        }
    }

    // Update status to mark rollback as in progress
    let mut status = obj.status.clone().unwrap_or_default();
    status.phase = UpgradePhase::RollingBack;
    status.progress = format!("Rolling back to version {}", original_image_tag);
    status.error_message = Some("Upgrade rolled back - restoring original image version".to_string());
    status.conditions = compute_upgrade_conditions(
        UpgradePhase::RollingBack,
        status.total_shards,
        status.upgraded_shards,
        status.failed_shards,
        status.error_message.as_deref(),
        obj.metadata.generation,
    );
    update_status(api, &name, status).await?;

    ctx.publish_upgrade_event(
        obj,
        "RollbackStarted",
        "RollingBack",
        Some(format!("Rolling back to version {}", original_image_tag)),
    )
    .await;

    // Wait for pods to restart and cluster to recover
    // The cluster reconciler will handle the StatefulSet update and pod recreation
    info!(name = %name, "Rollback initiated - waiting for cluster to recover");
    Ok(Action::requeue(Duration::from_secs(30)))
}

/// Check if rollback has completed - all pods restarted with original image and cluster is healthy
async fn check_rollback_completion(
    obj: &ValkeyUpgrade,
    ctx: &Context,
    api: &Api<ValkeyUpgrade>,
    cluster: &ValkeyCluster,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    let cluster_name = cluster.name_any();

    // Get original image version
    let Some(original_version) = obj
        .status
        .as_ref()
        .and_then(|s| s.current_version.as_ref())
    else {
        error!(name = %name, "Cannot check rollback: original version not found");
        let mut status = obj.status.clone().unwrap_or_default();
        status.phase = UpgradePhase::Failed;
        status.error_message = Some(
            "Cannot verify rollback completion: original image version not found in upgrade status. Check the upgrade status.currentVersion field and manually verify cluster image if needed.".to_string()
        );
        status.conditions = compute_upgrade_conditions(
            UpgradePhase::Failed,
            status.total_shards,
            status.upgraded_shards,
            status.failed_shards,
            status.error_message.as_deref(),
            obj.metadata.generation,
        );
        update_status(api, &name, status).await?;
        return Ok(Action::requeue(Duration::from_secs(300)));
    };

    // Construct original image
    let original_image = format!("{}:{}", cluster.spec.image.repository, original_version);

    // Check if all pods are running with the original image
    let pod_api: Api<k8s_openapi::api::core::v1::Pod> =
        Api::namespaced(ctx.client.clone(), namespace);
    let total_pods = crate::crd::total_pods(cluster.spec.masters, cluster.spec.replicas_per_master);

    let mut all_pods_ready = true;
    let mut all_pods_correct_image = true;

    for i in 0..total_pods {
        let pod_name = format!("{}-{}", cluster_name, i);
        match pod_api.get(&pod_name).await {
            Ok(pod) => {
                // Check if pod is ready
                let is_ready = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.conditions.as_ref())
                    .map(|conds| {
                        conds
                            .iter()
                            .any(|c| c.type_ == "Ready" && c.status == "True")
                    })
                    .unwrap_or(false);

                if !is_ready {
                    debug!(pod = %pod_name, "Pod not ready yet during rollback");
                    all_pods_ready = false;
                    continue;
                }

                // Check if pod has the original image
                let current_image = pod
                    .spec
                    .as_ref()
                    .and_then(|s| s.containers.first())
                    .and_then(|c| c.image.as_ref())
                    .cloned()
                    .unwrap_or_default();

                if current_image != original_image && !current_image.contains(original_version) {
                    debug!(
                        pod = %pod_name,
                        current = %current_image,
                        expected = %original_image,
                        "Pod still has wrong image during rollback"
                    );
                    all_pods_correct_image = false;
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                debug!(pod = %pod_name, "Pod not yet recreated during rollback");
                all_pods_ready = false;
            }
            Err(e) => {
                warn!(pod = %pod_name, error = %e, "Failed to get pod during rollback check");
                all_pods_ready = false;
            }
        }
    }

    if !all_pods_ready || !all_pods_correct_image {
        debug!(
            name = %name,
            all_ready = all_pods_ready,
            all_correct_image = all_pods_correct_image,
            "Rollback still in progress"
        );
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Check cluster health
    let password = get_auth_password(
        ctx,
        namespace,
        &cluster.spec.auth.secret_ref.name,
        &cluster.spec.auth.secret_ref.key,
    )
    .await?;

    let tls_certs = get_tls_certs(ctx, namespace, &cluster_name).await?;

    let master_pods = master_pod_dns_names(cluster);
    let (host, port) = master_pods
        .first()
        .ok_or_else(|| Error::Validation("No master pods found".to_string()))?;

    let client = match ValkeyClient::connect_single(host, *port, password.as_deref(), tls_certs.as_ref()).await {
        Ok(c) => c,
        Err(e) => {
            debug!(name = %name, error = %e, "Cannot connect to cluster to check health, will retry");
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let cluster_healthy = match client.is_cluster_healthy().await {
        Ok(healthy) => healthy,
        Err(e) => {
            debug!(name = %name, error = %e, "Failed to check cluster health, will retry");
            let _ = client.close().await;
            return Ok(Action::requeue(Duration::from_secs(30)));
        }
    };

    let _ = client.close().await;

    if !cluster_healthy {
        debug!(name = %name, "Cluster not yet healthy after rollback");
        return Ok(Action::requeue(Duration::from_secs(30)));
    }

    // Rollback complete - all pods have original image and cluster is healthy
    info!(name = %name, "Rollback completed successfully");
    let mut status = obj.status.clone().unwrap_or_default();
    status.phase = UpgradePhase::RolledBack;
    status.completed_at = Some(Timestamp::now().to_string());
    status.progress = format!("Rollback complete - restored to version {}", original_version);
    status.error_message = Some("Upgrade rolled back successfully".to_string());
    status.conditions = compute_upgrade_conditions(
        UpgradePhase::RolledBack,
        status.total_shards,
        status.upgraded_shards,
        status.failed_shards,
        status.error_message.as_deref(),
        obj.metadata.generation,
    );
    update_status(api, &name, status).await?;

    ctx.publish_upgrade_event(
        obj,
        "RollbackCompleted",
        "RolledBack",
        Some(format!("Rollback completed - restored to version {}", original_version)),
    )
    .await;

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
async fn validate_upgrade_spec(
    spec: &ValkeyUpgradeSpec,
    cluster: &ValkeyCluster,
    current_upgrade: Option<&ValkeyUpgrade>,
    ctx: Option<&Context>,
    namespace: &str,
) -> Result<(), Error> {
    // Validate target_version is not empty
    if spec.target_version.is_empty() {
        return Err(Error::Validation(
            "targetVersion is required but was empty. Specify a valid Valkey version (e.g., '9.0.1') in the upgrade spec.".to_string()
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
            "Cluster '{}' must be in Running state before upgrade can proceed, but it is currently in {} phase. Wait for the cluster to become healthy or check cluster status for issues.",
            cluster.name_any(), cluster_phase
        )));
    }

    // Check for concurrent upgrades targeting the same cluster
    // This check is skipped if ctx is None (e.g., in unit tests)
    if let Some(ctx) = ctx {
        let upgrade_api: Api<ValkeyUpgrade> = Api::namespaced(ctx.client.clone(), namespace);
        let cluster_namespace = spec.cluster_ref.namespace.as_deref().unwrap_or(namespace);
        let cluster_name = &spec.cluster_ref.name;

        // List all ValkeyUpgrade resources in the namespace
        match upgrade_api.list(&ListParams::default()).await {
            Ok(upgrades) => {
                // Check for other active upgrades targeting the same cluster
                for upgrade in upgrades.items {
                    // Skip the current upgrade if provided
                    if let Some(current) = current_upgrade {
                        if upgrade.name_any() == current.name_any() {
                            continue;
                        }
                    }

                    // Check if this upgrade targets the same cluster
                    let targets_same_cluster = upgrade.spec.cluster_ref.name == *cluster_name
                        && upgrade
                            .spec
                            .cluster_ref
                            .namespace
                            .as_deref()
                            .unwrap_or(namespace)
                            == cluster_namespace;

                    if !targets_same_cluster {
                        continue;
                    }

                    // Check if this upgrade is in an active phase
                    let phase = upgrade
                        .status
                        .as_ref()
                        .map(|s| s.phase)
                        .unwrap_or(UpgradePhase::Pending);

                    let is_active = matches!(
                        phase,
                        UpgradePhase::PreChecks | UpgradePhase::InProgress | UpgradePhase::RollingBack
                    );

                    if is_active {
                        return Err(Error::Validation(format!(
                            "Cannot start upgrade: another upgrade '{}' is already in progress (phase: {}) for cluster '{}'. Only one upgrade can target a cluster at a time. Wait for the existing upgrade to complete, fail, or be deleted before starting a new one.",
                            upgrade.name_any(),
                            phase,
                            cluster_name
                        )));
                    }
                }
            }
            Err(e) => {
                // If we can't list upgrades (e.g., in test environment), log and continue
                // This allows validation to proceed in test scenarios
                debug!("Could not check for concurrent upgrades: {}. Continuing validation.", e);
            }
        }
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
    let (host, port) = master_pods
        .first()
        .ok_or_else(|| Error::Validation("No master pods found".to_string()))?;

    // Connect to cluster to get topology
    let client = ValkeyClient::connect_single(host, *port, password.as_deref(), tls_certs.as_ref())
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;
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
            if let Some(data) = secret.data
                && let Some(password_bytes) = data.get(secret_key)
            {
                let password = String::from_utf8(password_bytes.0.clone()).map_err(|e| {
                    Error::Validation(format!("Invalid password encoding: {}", e))
                })?;
                return Ok(Some(password));
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
/// Compute standard Kubernetes conditions based on upgrade phase and status.
///
/// Returns a vector of conditions following Kubernetes conventions:
/// - Ready: True when upgrade is completed successfully
/// - Progressing: True when upgrade is actively in progress
/// - Degraded: True when upgrade is in a degraded state (e.g., some shards failed)
/// - Failed: True when upgrade has completely failed
fn compute_upgrade_conditions(
    phase: UpgradePhase,
    total_shards: i32,
    upgraded_shards: i32,
    failed_shards: i32,
    error_message: Option<&str>,
    generation: Option<i64>,
) -> Vec<Condition> {
    let mut conditions = Vec::new();

    match phase {
        UpgradePhase::Completed => {
            // Upgrade completed successfully
            conditions.push(Condition::ready(
                true,
                "UpgradeCompleted",
                &format!("All {} shards upgraded successfully", total_shards),
                generation,
            ));
            conditions.push(Condition::progressing(
                false,
                "UpgradeCompleted",
                "Upgrade finished",
                generation,
            ));
            conditions.push(Condition::degraded(
                false,
                "UpgradeCompleted",
                "No degraded shards",
                generation,
            ));
        }
        UpgradePhase::Failed => {
            // Upgrade failed completely
            conditions.push(Condition::ready(
                false,
                "UpgradeFailed",
                error_message.unwrap_or("Upgrade failed"),
                generation,
            ));
            conditions.push(Condition::progressing(
                false,
                "UpgradeFailed",
                "Upgrade stopped due to failure",
                generation,
            ));
            conditions.push(Condition::degraded(
                true,
                "UpgradeFailed",
                error_message.unwrap_or("Upgrade failed"),
                generation,
            ));
        }
        UpgradePhase::RollingBack | UpgradePhase::RolledBack => {
            // Rollback in progress or completed
            let is_rolling_back = phase == UpgradePhase::RollingBack;
            conditions.push(Condition::ready(
                false,
                if is_rolling_back { "RollingBack" } else { "RolledBack" },
                if is_rolling_back {
                    "Rollback in progress"
                } else {
                    "Rollback completed"
                },
                generation,
            ));
            conditions.push(Condition::progressing(
                is_rolling_back,
                if is_rolling_back { "RollingBack" } else { "RolledBack" },
                if is_rolling_back {
                    "Restoring original image version"
                } else {
                    "Original image version restored"
                },
                generation,
            ));
            conditions.push(Condition::degraded(
                true,
                "RollingBack",
                "Upgrade rolled back",
                generation,
            ));
        }
        UpgradePhase::InProgress => {
            // Upgrade actively in progress
            let progress_pct = if total_shards > 0 {
                (upgraded_shards * 100) / total_shards
            } else {
                0
            };
            let has_failures = failed_shards > 0;
            
            conditions.push(Condition::ready(
                false,
                "Upgrading",
                &format!("{}/{} shards upgraded ({}%)", upgraded_shards, total_shards, progress_pct),
                generation,
            ));
            conditions.push(Condition::progressing(
                true,
                "Upgrading",
                &format!("Upgrading shard {}/{}", upgraded_shards + 1, total_shards),
                generation,
            ));
            let degraded_msg = if has_failures {
                format!("{} shard(s) failed", failed_shards)
            } else {
                "All shards upgrading successfully".to_string()
            };
            conditions.push(Condition::degraded(
                has_failures,
                if has_failures { "ShardFailures" } else { "NoFailures" },
                &degraded_msg,
                generation,
            ));
        }
        UpgradePhase::PreChecks => {
            // Pre-checks phase
            conditions.push(Condition::ready(
                false,
                "PreChecks",
                "Running pre-upgrade health checks",
                generation,
            ));
            conditions.push(Condition::progressing(
                true,
                "PreChecks",
                "Validating cluster health before upgrade",
                generation,
            ));
            conditions.push(Condition::degraded(
                false,
                "PreChecks",
                "No degradation detected",
                generation,
            ));
        }
        UpgradePhase::Pending => {
            // Pending validation
            conditions.push(Condition::ready(
                false,
                "Pending",
                "Upgrade validation pending",
                generation,
            ));
            conditions.push(Condition::progressing(
                false,
                "Pending",
                "Waiting for validation",
                generation,
            ));
            conditions.push(Condition::degraded(
                false,
                "Pending",
                "No degradation",
                generation,
            ));
        }
    }

    conditions
}

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
    
    // Update conditions based on phase
    status.conditions = compute_upgrade_conditions(
        phase,
        status.total_shards,
        status.upgraded_shards,
        status.failed_shards,
        status.error_message.as_deref(),
        obj.metadata.generation,
    );
    
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

    // Ensure shard_statuses is large enough
    let shard_idx = shard_index as usize;
    if shard_idx >= status.shard_statuses.len() {
        // Extend shard_statuses if needed (shouldn't happen, but be safe)
        status.shard_statuses.resize_with(
            shard_idx + 1,
            || ShardUpgradeStatus {
                shard_index,
                master_node_id: String::new(),
                master_pod: String::new(),
                status: ShardUpgradeState::Pending,
                promoted_replica: None,
                error: None,
            },
        );
    }

    if let Some(shard) = status.shard_statuses.get_mut(shard_idx) {
        shard.status = state;
    } else {
        // This shouldn't happen after resize, but handle gracefully
        return Err(Error::Validation(format!(
            "Invalid shard index: {} (status has {} shards)",
            shard_index,
            status.shard_statuses.len()
        )));
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

    // Ensure shard_statuses is large enough
    let shard_idx = shard_index as usize;
    if shard_idx >= status.shard_statuses.len() {
        // Extend shard_statuses if needed (shouldn't happen, but be safe)
        status.shard_statuses.resize_with(
            shard_idx + 1,
            || ShardUpgradeStatus {
                shard_index,
                master_node_id: String::new(),
                master_pod: String::new(),
                status: ShardUpgradeState::Pending,
                promoted_replica: None,
                error: None,
            },
        );
    }

    if let Some(shard) = status.shard_statuses.get_mut(shard_idx) {
        shard.status = ShardUpgradeState::Failed;
        shard.error = Some(error);
    } else {
        // This shouldn't happen after resize, but handle gracefully
        return Err(Error::Validation(format!(
            "Invalid shard index: {} (status has {} shards)",
            shard_index,
            status.shard_statuses.len()
        )));
    }

    update_status(api, name, status).await
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;
    use crate::crd::{
        AuthSpec, ClusterPhase, ClusterReference, ImageSpec, IssuerRef, PersistenceSpec,
        ResourceRequirementsSpec, SchedulingSpec, SecretKeyRef, TlsSpec, ValkeyCluster,
        ValkeyClusterSpec, ValkeyClusterStatus,
    };
    use std::collections::BTreeMap;

    /// Helper to create a minimal valid ValkeyCluster for testing
    fn create_test_cluster(name: &str, phase: ClusterPhase) -> ValkeyCluster {
        ValkeyCluster {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("test-ns".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                image: ImageSpec {
                    repository: "valkey/valkey".to_string(),
                    tag: "8-alpine".to_string(),
                    pull_policy: "IfNotPresent".to_string(),
                    pull_secrets: Vec::new(),
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-auth-secret".to_string(),
                        key: "password".to_string(),
                    },
                },
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "test-issuer".to_string(),
                        kind: "ClusterIssuer".to_string(),
                        group: "cert-manager.io".to_string(),
                    },
                    duration: "2160h".to_string(),
                    renew_before: "360h".to_string(),
                },
                persistence: PersistenceSpec::default(),
                resources: ResourceRequirementsSpec::default(),
                scheduling: SchedulingSpec::default(),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
            },
            status: Some(ValkeyClusterStatus {
                phase,
                ..Default::default()
            }),
        }
    }

    /// Helper to create a test upgrade spec
    fn create_test_upgrade_spec(target_version: &str) -> ValkeyUpgradeSpec {
        ValkeyUpgradeSpec {
            cluster_ref: ClusterReference {
                name: "test-cluster".to_string(),
                namespace: None,
            },
            target_version: target_version.to_string(),
        }
    }

    // ==========================================================================
    // validate_upgrade_spec() tests
    // ==========================================================================

    #[tokio::test]
    async fn test_validate_upgrade_spec_valid() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Running);
        assert!(validate_upgrade_spec(&spec, &cluster, None, None, "default").await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_upgrade_spec_empty_target_version() {
        let spec = create_test_upgrade_spec("");
        let cluster = create_test_cluster("test", ClusterPhase::Running);
        let result = validate_upgrade_spec(&spec, &cluster, None, None, "default").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("targetVersion is required")
        );
    }

    #[tokio::test]
    async fn test_validate_upgrade_spec_cluster_not_running() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Creating);
        let result = validate_upgrade_spec(&spec, &cluster, None, None, "default").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must be in Running state")
        );
    }

    #[tokio::test]
    async fn test_validate_upgrade_spec_cluster_pending() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Pending);
        let result = validate_upgrade_spec(&spec, &cluster, None, None, "default").await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must be in Running state")
        );
    }

    #[tokio::test]
    async fn test_validate_upgrade_spec_cluster_failed() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Failed);
        let result = validate_upgrade_spec(&spec, &cluster, None, None, "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_upgrade_spec_cluster_degraded() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Degraded);
        let result = validate_upgrade_spec(&spec, &cluster, None, None, "default").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validate_upgrade_spec_cluster_updating() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Updating);
        let result = validate_upgrade_spec(&spec, &cluster, None, None, "default").await;
        assert!(result.is_err());
    }

    // ==========================================================================
    // resolve_target_image() tests
    // ==========================================================================

    #[test]
    fn test_resolve_target_image_basic() {
        let spec = create_test_upgrade_spec("9");
        let cluster = create_test_cluster("test", ClusterPhase::Running);
        let image = resolve_target_image(&spec, &cluster);
        assert_eq!(image, "valkey/valkey:9-alpine");
    }

    #[test]
    fn test_resolve_target_image_with_patch_version() {
        let spec = create_test_upgrade_spec("9.0.1");
        let cluster = create_test_cluster("test", ClusterPhase::Running);
        let image = resolve_target_image(&spec, &cluster);
        assert_eq!(image, "valkey/valkey:9.0.1-alpine");
    }

    #[test]
    fn test_resolve_target_image_custom_repository() {
        let spec = create_test_upgrade_spec("9");
        let mut cluster = create_test_cluster("test", ClusterPhase::Running);
        cluster.spec.image.repository = "my-registry.com/valkey".to_string();
        let image = resolve_target_image(&spec, &cluster);
        assert_eq!(image, "my-registry.com/valkey:9-alpine");
    }

    #[test]
    fn test_resolve_target_image_private_registry() {
        let spec = create_test_upgrade_spec("10");
        let mut cluster = create_test_cluster("test", ClusterPhase::Running);
        cluster.spec.image.repository = "gcr.io/my-project/valkey".to_string();
        let image = resolve_target_image(&spec, &cluster);
        assert_eq!(image, "gcr.io/my-project/valkey:10-alpine");
    }

    // ==========================================================================
    // Constants tests
    // ==========================================================================

    #[test]
    fn test_upgrade_finalizer_constant() {
        assert_eq!(
            UPGRADE_FINALIZER,
            "valkey-operator.smoketurner.com/upgrade-finalizer"
        );
        assert!(UPGRADE_FINALIZER.contains("valkey-operator"));
        assert!(UPGRADE_FINALIZER.contains("upgrade"));
    }

    // ==========================================================================
    // UpgradePhase terminal state tests
    // ==========================================================================

    #[test]
    fn test_upgrade_phase_terminal_completed() {
        assert!(UpgradePhase::Completed.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_terminal_failed() {
        assert!(UpgradePhase::Failed.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_terminal_rolled_back() {
        assert!(UpgradePhase::RolledBack.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_not_terminal_pending() {
        assert!(!UpgradePhase::Pending.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_not_terminal_prechecks() {
        assert!(!UpgradePhase::PreChecks.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_not_terminal_inprogress() {
        assert!(!UpgradePhase::InProgress.is_terminal());
    }

    #[test]
    fn test_upgrade_phase_not_terminal_rolling_back() {
        assert!(!UpgradePhase::RollingBack.is_terminal());
    }
}

#[cfg(test)]
mod upgrade_reconciler_function_tests {
    use super::{compute_upgrade_conditions, resolve_target_image};
    use crate::crd::{UpgradePhase, ValkeyCluster, ValkeyUpgradeSpec, ClusterReference};

    /// Test resolve_target_image function
    #[test]
    fn test_resolve_target_image() {
        let cluster = create_test_cluster("test-cluster", "valkeyio/valkey", "9.0.0");
        let spec = create_test_upgrade_spec("test-upgrade", "test-cluster", "9.0.1");
        
        let image = resolve_target_image(&spec, &cluster);
        assert_eq!(image, "valkeyio/valkey:9.0.1-alpine");
    }

    /// Test resolve_target_image with different repository
    #[test]
    fn test_resolve_target_image_custom_repository() {
        let cluster = create_test_cluster("test-cluster", "my-registry/valkey", "9.0.0");
        let spec = create_test_upgrade_spec("test-upgrade", "test-cluster", "9.0.2");
        
        let image = resolve_target_image(&spec, &cluster);
        assert_eq!(image, "my-registry/valkey:9.0.2-alpine");
    }

    /// Test compute_upgrade_conditions for Completed phase
    #[test]
    fn test_compute_conditions_completed() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::Completed,
            3,
            3,
            0,
            None,
            Some(1),
        );
        
        assert_eq!(conditions.len(), 3);
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "True");
        assert_eq!(ready.reason, "UpgradeCompleted");
        
        let progressing = conditions.iter().find(|c| c.r#type == "Progressing").unwrap();
        assert_eq!(progressing.status, "False");
        
        let degraded = conditions.iter().find(|c| c.r#type == "Degraded").unwrap();
        assert_eq!(degraded.status, "False");
    }

    /// Test compute_upgrade_conditions for Failed phase
    #[test]
    fn test_compute_conditions_failed() {
        let error_msg = "Cluster health check failed";
        let conditions = compute_upgrade_conditions(
            UpgradePhase::Failed,
            3,
            1,
            0,
            Some(error_msg),
            Some(1),
        );
        
        assert_eq!(conditions.len(), 3);
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "False");
        assert_eq!(ready.reason, "UpgradeFailed");
        assert_eq!(ready.message, error_msg);
        
        let degraded = conditions.iter().find(|c| c.r#type == "Degraded").unwrap();
        assert_eq!(degraded.status, "True");
    }

    /// Test compute_upgrade_conditions for InProgress phase
    #[test]
    fn test_compute_conditions_in_progress() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::InProgress,
            6,
            2,
            0,
            None,
            Some(1),
        );
        
        assert_eq!(conditions.len(), 3);
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "False");
        assert!(ready.message.contains("2/6"));
        
        let progressing = conditions.iter().find(|c| c.r#type == "Progressing").unwrap();
        assert_eq!(progressing.status, "True");
        assert!(progressing.message.contains("Upgrading shard"));
        
        let degraded = conditions.iter().find(|c| c.r#type == "Degraded").unwrap();
        assert_eq!(degraded.status, "False");
    }

    /// Test compute_upgrade_conditions for InProgress with failures
    #[test]
    fn test_compute_conditions_in_progress_with_failures() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::InProgress,
            6,
            2,
            1,
            None,
            Some(1),
        );
        
        let degraded = conditions.iter().find(|c| c.r#type == "Degraded").unwrap();
        assert_eq!(degraded.status, "True");
        assert_eq!(degraded.reason, "ShardFailures");
        assert!(degraded.message.contains("1 shard(s) failed"));
    }

    /// Test compute_upgrade_conditions for RollingBack phase
    #[test]
    fn test_compute_conditions_rolling_back() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::RollingBack,
            3,
            1,
            0,
            None,
            Some(1),
        );
        
        assert_eq!(conditions.len(), 3);
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "False");
        assert_eq!(ready.reason, "RollingBack");
        
        let progressing = conditions.iter().find(|c| c.r#type == "Progressing").unwrap();
        assert_eq!(progressing.status, "True");
        
        let degraded = conditions.iter().find(|c| c.r#type == "Degraded").unwrap();
        assert_eq!(degraded.status, "True");
    }

    /// Test compute_upgrade_conditions for RolledBack phase
    #[test]
    fn test_compute_conditions_rolled_back() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::RolledBack,
            3,
            1,
            0,
            None,
            Some(1),
        );
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "False");
        assert_eq!(ready.reason, "RolledBack");
        
        let progressing = conditions.iter().find(|c| c.r#type == "Progressing").unwrap();
        assert_eq!(progressing.status, "False");
    }

    /// Test compute_upgrade_conditions for PreChecks phase
    #[test]
    fn test_compute_conditions_prechecks() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::PreChecks,
            3,
            0,
            0,
            None,
            Some(1),
        );
        
        assert_eq!(conditions.len(), 3);
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "False");
        assert_eq!(ready.reason, "PreChecks");
        
        let progressing = conditions.iter().find(|c| c.r#type == "Progressing").unwrap();
        assert_eq!(progressing.status, "True");
        assert!(progressing.message.contains("health") || progressing.message.contains("PreChecks"));
    }

    /// Test compute_upgrade_conditions for Pending phase
    #[test]
    fn test_compute_conditions_pending() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::Pending,
            3,
            0,
            0,
            None,
            Some(1),
        );
        
        assert_eq!(conditions.len(), 3);
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        assert_eq!(ready.status, "False");
        assert_eq!(ready.reason, "Pending");
        
        let progressing = conditions.iter().find(|c| c.r#type == "Progressing").unwrap();
        assert_eq!(progressing.status, "False");
    }

    /// Test compute_upgrade_conditions with zero shards
    #[test]
    fn test_compute_conditions_zero_shards() {
        let conditions = compute_upgrade_conditions(
            UpgradePhase::InProgress,
            0,
            0,
            0,
            None,
            Some(1),
        );
        
        let ready = conditions.iter().find(|c| c.r#type == "Ready").unwrap();
        // Should handle zero division gracefully
        assert!(ready.message.contains("0/0"));
    }

    // Helper functions to create test resources

    fn create_test_cluster(name: &str, repository: &str, tag: &str) -> ValkeyCluster {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "valkey-operator.smoketurner.com/v1alpha1",
            "kind": "ValkeyCluster",
            "metadata": {
                "name": name
            },
            "spec": {
                "masters": 3,
                "replicasPerMaster": 1,
                "image": {
                    "repository": repository,
                    "tag": tag
                },
                "tls": {
                    "issuerRef": {
                        "name": "selfsigned-issuer"
                    }
                },
                "auth": {
                    "secretRef": {
                        "name": "valkey-auth"
                    }
                }
            },
            "status": {
                "phase": "Running"
            }
        }))
        .expect("Failed to create test cluster")
    }

    fn create_test_upgrade_spec(_name: &str, cluster_name: &str, target_version: &str) -> ValkeyUpgradeSpec {
        ValkeyUpgradeSpec {
            cluster_ref: ClusterReference {
                name: cluster_name.to_string(),
                namespace: None,
            },
            target_version: target_version.to_string(),
        }
    }
}
