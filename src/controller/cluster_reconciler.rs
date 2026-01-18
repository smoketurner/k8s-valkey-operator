//! Reconciliation loop for ValkeyCluster.
//!
//! This module contains the main reconcile function that handles the lifecycle
//! of ValkeyCluster custom resources.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use kube::{
    Api, ResourceExt,
    api::{ApiResource, DynamicObject, Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    client::ScalingContext,
    controller::{
        cluster_init,
        cluster_phases::{self, ClusterHealthResult, PhaseContext},
        cluster_state_machine::ClusterStateMachine,
        cluster_topology::{ClusterTopology, NodeRole},
        common::{add_finalizer, extract_pod_name, remove_finalizer},
        context::Context,
        error::Error,
    },
    crd::{ClusterPhase, Condition, ValkeyCluster, ValkeyClusterStatus, total_pods},
    resources::{certificate, common, pdb, services, statefulset},
};

/// Field manager name for server-side apply
pub const FIELD_MANAGER: &str = "valkey-operator";

/// Finalizer name for graceful deletion
pub const FINALIZER: &str = "valkey-operator.smoketurner.com/finalizer";

/// Annotation key indicating an upgrade is in progress.
#[allow(dead_code)] // Used in tests
const UPGRADE_IN_PROGRESS_ANNOTATION: &str = "valkey-operator.smoketurner.com/upgrade-in-progress";

/// Annotation key storing the name of the active upgrade.
#[allow(dead_code)] // Used in tests
const UPGRADE_NAME_ANNOTATION: &str = "valkey-operator.smoketurner.com/upgrade-name";

/// Check if an upgrade is in progress for this cluster.
#[allow(dead_code)] // Used in tests
fn is_upgrade_in_progress(cluster: &ValkeyCluster) -> bool {
    cluster
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(UPGRADE_IN_PROGRESS_ANNOTATION))
        .is_some_and(|v| v == "true")
}

/// Get the requeue duration for a given phase.
///
/// These are fallback intervals - the controller primarily reacts to watch events
/// for immediate reconciliation. Short intervals ensure quick recovery from any
/// missed events without unnecessary delay.
fn requeue_duration(phase: ClusterPhase) -> std::time::Duration {
    match phase {
        // Running: periodic health check, can be longer
        ClusterPhase::Running => std::time::Duration::from_secs(30),

        // Initial creation phases: quick requeue
        ClusterPhase::Creating
        | ClusterPhase::WaitingForPods
        | ClusterPhase::InitializingCluster
        | ClusterPhase::AssigningSlots
        | ClusterPhase::ConfiguringReplicas => std::time::Duration::from_secs(2),

        // Scale-up phases: quick requeue
        ClusterPhase::ScalingUpStatefulSet
        | ClusterPhase::WaitingForNewPods
        | ClusterPhase::AddingNodesToCluster
        | ClusterPhase::ConfiguringNewReplicas => std::time::Duration::from_secs(2),

        // Scale-down phases: quick requeue
        ClusterPhase::RemovingNodesFromCluster | ClusterPhase::ScalingDownStatefulSet => {
            std::time::Duration::from_secs(2)
        }

        // Slot operations: moderate interval (migrations take time)
        ClusterPhase::RebalancingSlots | ClusterPhase::EvacuatingSlots => {
            std::time::Duration::from_secs(5)
        }

        // Verification: quick requeue
        ClusterPhase::VerifyingClusterHealth => std::time::Duration::from_secs(2),

        // Recovery phases: moderate interval
        ClusterPhase::Degraded => std::time::Duration::from_secs(10),

        // Failed: longer interval, needs manual intervention
        ClusterPhase::Failed => std::time::Duration::from_secs(60),

        // Pending/Deleting: moderate interval
        ClusterPhase::Pending | ClusterPhase::Deleting => std::time::Duration::from_secs(5),
    }
}

/// Reconcile a ValkeyCluster
///
/// This is the main reconciliation function called by the controller.
/// It handles the full lifecycle: creation, updates, and deletion.
pub async fn reconcile(obj: Arc<ValkeyCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let start_time = Instant::now();
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    debug!(name = %name, namespace = %namespace, "Reconciling ValkeyCluster");

    let api: Api<ValkeyCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    // Handle deletion
    if obj.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&obj, &ctx, &namespace).await;
    }

    // Ensure finalizer is present
    if !obj.finalizers().iter().any(|f| f == FINALIZER) {
        info!(name = %name, "Adding finalizer");
        add_finalizer(&api, &name, FINALIZER).await?;
        return Ok(Action::requeue(std::time::Duration::from_secs(1)));
    }

    // Get current state
    let current_phase = obj
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(ClusterPhase::Pending);

    // Check if spec changed
    let observed_gen = obj.status.as_ref().and_then(|s| s.observed_generation);
    let current_gen = obj.metadata.generation;
    let spec_changed = observed_gen != current_gen;

    if spec_changed {
        info!(
            name = %name,
            current_gen = ?current_gen,
            observed_gen = ?observed_gen,
            "Spec changed, reconciling"
        );
    }

    // Initialize state machine for transition validation
    // TODO: Use state_machine for validating transitions and preventing invalid state changes
    let _state_machine = ClusterStateMachine::new();

    // Stuck detection: check if we've been in the same non-terminal phase too long
    // This prevents infinite loops and helps diagnose stuck operations
    const MAX_RECONCILE_COUNT: i32 = 50;
    const MAX_STUCK_DURATION_SECS: i64 = 300; // 5 minutes

    let status = obj.status.as_ref();
    let reconcile_count = status.map(|s| s.reconcile_count).unwrap_or(0);
    let last_transition = status.and_then(|s| s.last_phase_transition.as_ref());

    // Check for stuck condition (only in non-terminal, non-running phases)
    let is_operational_phase = matches!(
        current_phase,
        // Initial creation phases
        ClusterPhase::Creating
            | ClusterPhase::WaitingForPods
            | ClusterPhase::InitializingCluster
            | ClusterPhase::AssigningSlots
            | ClusterPhase::ConfiguringReplicas
            // Scale-up phases
            | ClusterPhase::ScalingUpStatefulSet
            | ClusterPhase::WaitingForNewPods
            | ClusterPhase::AddingNodesToCluster
            | ClusterPhase::RebalancingSlots
            | ClusterPhase::ConfiguringNewReplicas
            // Scale-down phases
            | ClusterPhase::EvacuatingSlots
            | ClusterPhase::RemovingNodesFromCluster
            | ClusterPhase::ScalingDownStatefulSet
            // Verification
            | ClusterPhase::VerifyingClusterHealth
    );

    if is_operational_phase {
        // Check iteration count
        if reconcile_count > MAX_RECONCILE_COUNT {
            let error_msg = format!(
                "Phase {} stuck: {} reconcile iterations exceeded threshold of {}",
                current_phase, reconcile_count, MAX_RECONCILE_COUNT
            );
            error!(name = %name, phase = %current_phase, reconcile_count = reconcile_count, "Stuck detection triggered");
            ctx.publish_warning_event(
                &obj,
                "StuckDetected",
                "Reconciling",
                Some(error_msg.clone()),
            )
            .await;
            update_status(
                &api,
                &name,
                ClusterPhase::Failed,
                0,
                Some(&error_msg),
                None,
                current_gen,
            )
            .await?;
            return Ok(Action::requeue(requeue_duration(ClusterPhase::Failed)));
        }

        // Check time-based stuck detection
        if let Some(transition_time) = last_transition
            && let Ok(ts) = jiff::Timestamp::from_str(transition_time)
        {
            let elapsed = jiff::Timestamp::now().duration_since(ts).as_secs();
            if elapsed > MAX_STUCK_DURATION_SECS {
                let error_msg = format!(
                    "Phase {} stuck: {} seconds since last transition exceeds threshold of {}",
                    current_phase, elapsed, MAX_STUCK_DURATION_SECS
                );
                error!(name = %name, phase = %current_phase, elapsed_secs = elapsed, "Stuck detection triggered");
                ctx.publish_warning_event(
                    &obj,
                    "StuckDetected",
                    "Reconciling",
                    Some(error_msg.clone()),
                )
                .await;
                update_status(
                    &api,
                    &name,
                    ClusterPhase::Failed,
                    0,
                    Some(&error_msg),
                    None,
                    current_gen,
                )
                .await?;
                return Ok(Action::requeue(requeue_duration(ClusterPhase::Failed)));
            }
        }
    }

    // Build phase context for handlers
    let running_pods = check_running_pods(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);
    let ready_pods = check_ready_replicas(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);
    let current_masters = get_current_master_count(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);
    let nodes_in_cluster = get_nodes_in_cluster_count(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);

    let phase_ctx = PhaseContext {
        name: name.clone(),
        namespace: namespace.clone(),
        target_masters: obj.spec.masters,
        target_replicas_per_master: obj.spec.replicas_per_master,
        current_masters,
        running_pods,
        ready_pods,
        nodes_in_cluster,
        spec_changed,
        generation: current_gen.unwrap_or(0),
    };

    // Determine next phase based on current state
    let next_phase = match current_phase {
        ClusterPhase::Pending => {
            // Validate and start creating
            if let Err(e) = validate_spec(&obj) {
                error!(name = %name, error = %e, "Validation failed");
                ctx.publish_warning_event(
                    &obj,
                    "ValidationFailed",
                    "Validating",
                    Some(e.to_string()),
                )
                .await;
                update_status(
                    &api,
                    &name,
                    ClusterPhase::Failed,
                    0,
                    Some(&e.to_string()),
                    None,
                    current_gen,
                )
                .await?;
                return Err(e);
            }
            ctx.publish_normal_event(
                &obj,
                "Creating",
                "CreateResources",
                Some("Starting resource creation".to_string()),
            )
            .await;
            ClusterPhase::Creating
        }
        ClusterPhase::Creating => {
            // Create owned resources and transition to WaitingForPods
            let result = cluster_phases::handle_creating(
                &obj,
                &ctx,
                &phase_ctx,
                create_owned_resources(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::WaitingForPods => {
            let result = cluster_phases::handle_waiting_for_pods(
                &obj,
                &ctx,
                &phase_ctx,
                create_owned_resources(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::InitializingCluster => {
            let result = cluster_phases::handle_initializing_cluster(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                execute_cluster_init(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::AssigningSlots => {
            let result = cluster_phases::handle_assigning_slots(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                execute_slot_assignment_no_replicas(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::ConfiguringReplicas => {
            let result = cluster_phases::handle_configuring_replicas(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                execute_replica_setup(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::Running => {
            // Use handler from cluster_phases
            let result = cluster_phases::handle_running(
                &obj,
                &ctx,
                &phase_ctx,
                create_owned_resources(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }

        // === Scale-Up Phases ===
        ClusterPhase::ScalingUpStatefulSet => {
            let result = cluster_phases::handle_scaling_up_statefulset(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                create_owned_resources(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::WaitingForNewPods => {
            let result =
                cluster_phases::handle_waiting_for_new_pods(&obj, &ctx, &phase_ctx).await?;
            result.next_phase
        }
        ClusterPhase::AddingNodesToCluster => {
            let result =
                cluster_phases::handle_adding_nodes_to_cluster(&obj, &ctx, &phase_ctx, async {
                    add_new_replicas_to_cluster(&obj, &ctx, &namespace)
                        .await
                        .map(|_| ())
                })
                .await?;
            result.next_phase
        }
        ClusterPhase::RebalancingSlots => {
            let result = cluster_phases::handle_rebalancing_slots(
                &obj,
                &ctx,
                &phase_ctx,
                execute_scale_up_rebalance(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::ConfiguringNewReplicas => {
            let result = cluster_phases::handle_configuring_new_replicas(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                execute_replica_setup(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }

        // === Scale-Down Phases ===
        ClusterPhase::EvacuatingSlots => {
            let result = cluster_phases::handle_evacuating_slots(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                execute_scale_down_evacuation(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::RemovingNodesFromCluster => {
            let result = cluster_phases::handle_removing_nodes_from_cluster(
                &obj,
                &ctx,
                &phase_ctx,
                execute_forget_removed_nodes(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::ScalingDownStatefulSet => {
            let result = cluster_phases::handle_scaling_down_statefulset(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                create_owned_resources(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }

        // === Verification Phase ===
        ClusterPhase::VerifyingClusterHealth => {
            let result = cluster_phases::handle_verifying_cluster_health(
                &obj,
                &ctx,
                &api,
                &phase_ctx,
                async {
                    check_cluster_health(&obj, &ctx, &namespace).await.map(|h| {
                        ClusterHealthResult {
                            is_healthy: h.is_healthy,
                            slots_assigned: h.slots_assigned,
                            healthy_masters: h.healthy_masters,
                        }
                    })
                },
            )
            .await?;
            result.next_phase
        }

        // === Problem States ===
        ClusterPhase::Degraded => {
            let result = cluster_phases::handle_degraded(
                &obj,
                &ctx,
                &phase_ctx,
                create_owned_resources(&obj, &ctx, &namespace),
            )
            .await?;
            result.next_phase
        }
        ClusterPhase::Failed => {
            // Attempt automatic recovery for stale IP conditions
            // Recovery conditions:
            // 1. All pods Running but not Ready (indicates cluster health issue, not pod issue)
            // 2. Under max attempts (5)
            // 3. Backoff elapsed (30s * 2^attempts)
            let running_pods = check_running_pods(&obj, &ctx, &namespace)
                .await
                .unwrap_or(0);
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace)
                .await
                .unwrap_or(0);
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

            // Get recovery state from status
            let recovery_attempts = obj
                .status
                .as_ref()
                .map(|s| s.recovery_attempts)
                .unwrap_or(0);
            let last_recovery = obj
                .status
                .as_ref()
                .and_then(|s| s.last_recovery_attempt.as_ref());

            // Constants for recovery behavior
            const MAX_RECOVERY_ATTEMPTS: i32 = 5;
            const BASE_BACKOFF_SECS: i64 = 30;

            // Check if we can attempt recovery
            let can_attempt = running_pods >= desired_replicas
                && ready_replicas < desired_replicas
                && recovery_attempts < MAX_RECOVERY_ATTEMPTS;

            // Check backoff elapsed
            let backoff_elapsed = if let Some(last_attempt) = last_recovery {
                if let Ok(ts) =
                    std::str::FromStr::from_str(last_attempt).map(|t: jiff::Timestamp| t)
                {
                    let backoff_secs = BASE_BACKOFF_SECS * (1i64 << recovery_attempts.min(10));
                    let elapsed = jiff::Timestamp::now().duration_since(ts).as_secs();
                    elapsed >= backoff_secs
                } else {
                    true // Invalid timestamp, allow attempt
                }
            } else {
                true // No previous attempt, allow
            };

            if can_attempt && backoff_elapsed {
                info!(
                    name = %name,
                    running_pods = running_pods,
                    ready_replicas = ready_replicas,
                    recovery_attempts = recovery_attempts,
                    "Attempting automatic stale IP recovery"
                );

                ctx.publish_normal_event(
                    &obj,
                    "RecoveryAttempt",
                    "Recovering",
                    Some(format!(
                        "Attempting automatic recovery (attempt {}/{})",
                        recovery_attempts + 1,
                        MAX_RECOVERY_ATTEMPTS
                    )),
                )
                .await;

                // Execute recovery via CLUSTER MEET
                let recovery_result = execute_stale_ip_recovery(&obj, &ctx, &namespace).await;

                // Update recovery tracking in status
                let mut status = obj.status.clone().unwrap_or_default();
                status.recovery_attempts = recovery_attempts + 1;
                status.last_recovery_attempt = Some(jiff::Timestamp::now().to_string());

                let patch = serde_json::json!({
                    "status": {
                        "recovery_attempts": status.recovery_attempts,
                        "last_recovery_attempt": status.last_recovery_attempt,
                    }
                });
                let _ = api
                    .patch_status(
                        &name,
                        &PatchParams::apply(FIELD_MANAGER),
                        &Patch::Merge(&patch),
                    )
                    .await;

                match recovery_result {
                    Ok(()) => {
                        info!(name = %name, "Stale IP recovery successful, transitioning to Degraded");
                        ctx.publish_normal_event(
                            &obj,
                            "RecoverySucceeded",
                            "Recovered",
                            Some("Automatic stale IP recovery successful".to_string()),
                        )
                        .await;
                        ClusterPhase::Degraded
                    }
                    Err(e) => {
                        warn!(name = %name, error = %e, "Stale IP recovery failed, will retry");
                        ctx.publish_warning_event(
                            &obj,
                            "RecoveryFailed",
                            "Recovering",
                            Some(format!("Recovery attempt failed: {}", e)),
                        )
                        .await;
                        ClusterPhase::Failed
                    }
                }
            } else if recovery_attempts >= MAX_RECOVERY_ATTEMPTS {
                warn!(
                    name = %name,
                    recovery_attempts = recovery_attempts,
                    "Max recovery attempts reached, manual intervention required"
                );
                ClusterPhase::Failed
            } else if ready_replicas >= desired_replicas {
                // All replicas are Ready but we're still in Failed state
                // This can happen after successful recovery - check actual cluster health
                info!(
                    name = %name,
                    ready_replicas = ready_replicas,
                    "All replicas ready in Failed state, checking cluster health"
                );

                match check_cluster_health(&obj, &ctx, &namespace).await {
                    Ok(health) if health.is_healthy => {
                        info!(name = %name, "Cluster is healthy, transitioning to Running");
                        ctx.publish_normal_event(
                            &obj,
                            "Recovered",
                            "Running",
                            Some("Cluster recovered and healthy".to_string()),
                        )
                        .await;
                        ClusterPhase::Running
                    }
                    Ok(health) => {
                        info!(
                            name = %name,
                            healthy_masters = health.healthy_masters,
                            slots_assigned = health.slots_assigned,
                            "Cluster not fully healthy yet, transitioning to Degraded"
                        );
                        ClusterPhase::Degraded
                    }
                    Err(e) => {
                        debug!(name = %name, error = %e, "Failed to check cluster health");
                        ClusterPhase::Failed
                    }
                }
            } else {
                // Not ready for recovery attempt yet
                debug!(
                    name = %name,
                    running_pods = running_pods,
                    ready_replicas = ready_replicas,
                    desired = desired_replicas,
                    can_attempt = can_attempt,
                    backoff_elapsed = backoff_elapsed,
                    "Waiting for recovery conditions"
                );
                ClusterPhase::Failed
            }
        }
        ClusterPhase::Deleting => {
            // Should be handled by deletion branch above
            ClusterPhase::Deleting
        }
    };

    // Update status
    let ready_replicas_final = check_ready_replicas(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);

    // Check cluster health when running
    let health_status = if next_phase == ClusterPhase::Running {
        match check_cluster_health(&obj, &ctx, &namespace).await {
            Ok(health) => {
                if !health.is_healthy {
                    debug!(name = %name, "Cluster health check indicates unhealthy state");
                }
                Some(health)
            }
            Err(e) => {
                debug!(name = %name, error = %e, "Failed to check cluster health");
                None
            }
        }
    } else {
        None
    };

    update_status(
        &api,
        &name,
        next_phase,
        ready_replicas_final,
        None,
        health_status.as_ref(),
        current_gen,
    )
    .await?;

    // Record metrics
    if let Some(ref health_state) = ctx.health_state {
        let duration = start_time.elapsed().as_secs_f64();
        let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
        health_state
            .metrics
            .record_reconcile(&namespace, &name, duration);
        health_state.metrics.set_resource_replicas(
            &namespace,
            &name,
            i64::from(desired_replicas),
            i64::from(ready_replicas_final),
        );
    }

    // Determine requeue interval based on state (uses fast mode if VALKEY_OPERATOR_FAST_REQUEUE=1)
    Ok(Action::requeue(requeue_duration(next_phase)))
}

/// Error policy for the controller
pub fn error_policy(obj: Arc<ValkeyCluster>, error: &Error, ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    // Record error metric
    if let Some(ref health_state) = ctx.health_state {
        health_state.metrics.record_error(&namespace, &name);
    }

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

        // Note: We can't update annotations here (error policy is read-only)
        // The retry count will be tracked on the next reconciliation attempt
        // For now, we use exponential backoff but don't persist retry count
        // A future enhancement could update annotations in the reconciler on error

        Action::requeue(backoff)
    } else {
        error!(name = %name, error = %error, "Non-retryable error");
        Action::requeue(std::time::Duration::from_secs(300))
    }
}

/// Validate the resource spec
fn validate_spec(obj: &ValkeyCluster) -> Result<(), Error> {
    // Valkey cluster requires minimum 3 masters for proper quorum
    if obj.spec.masters < 3 {
        return Err(Error::Validation(
            "masters must be at least 3 for cluster quorum".to_string(),
        ));
    }
    if obj.spec.masters > 100 {
        return Err(Error::Validation("masters cannot exceed 100".to_string()));
    }
    if obj.spec.replicas_per_master < 0 {
        return Err(Error::Validation(
            "replicasPerMaster cannot be negative".to_string(),
        ));
    }
    if obj.spec.replicas_per_master > 5 {
        return Err(Error::Validation(
            "replicasPerMaster cannot exceed 5".to_string(),
        ));
    }
    // TLS is required - validate issuer ref
    if obj.spec.tls.issuer_ref.name.is_empty() {
        return Err(Error::Validation(
            "tls.issuerRef.name is required".to_string(),
        ));
    }
    // Auth is required - validate secret ref
    if obj.spec.auth.secret_ref.name.is_empty() {
        return Err(Error::Validation(
            "auth.secretRef.name is required".to_string(),
        ));
    }
    Ok(())
}

/// Handle deletion of a ValkeyCluster
async fn handle_deletion(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    info!(name = %name, "Handling deletion");

    // Clean up owned resources (they should be garbage collected via owner references,
    // but we can do explicit cleanup here if needed)

    // Remove finalizer
    let api: Api<ValkeyCluster> = Api::namespaced(ctx.client.clone(), namespace);
    remove_finalizer(&api, &name).await?;

    Ok(Action::await_change())
}

/// Create owned resources (StatefulSet, Services, PDB)
async fn create_owned_resources(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();

    // Apply StatefulSet
    let statefulset = statefulset::generate_statefulset(obj);
    let sts_api: Api<k8s_openapi::api::apps::v1::StatefulSet> =
        Api::namespaced(ctx.client.clone(), namespace);
    sts_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&statefulset),
        )
        .await?;

    // Apply Headless Service (for cluster discovery)
    let headless_svc = services::generate_headless_service(obj);
    let headless_name = common::headless_service_name(obj);
    let svc_api: Api<k8s_openapi::api::core::v1::Service> =
        Api::namespaced(ctx.client.clone(), namespace);
    svc_api
        .patch(
            &headless_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&headless_svc),
        )
        .await?;

    // Apply Client Service (for client connections)
    let client_svc = services::generate_client_service(obj);
    svc_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&client_svc),
        )
        .await?;

    // Apply Read Service (for read-only traffic distribution) if enabled
    if let Some(read_svc) = services::generate_read_service(obj) {
        let read_svc_name = common::read_service_name(obj);
        svc_api
            .patch(
                &read_svc_name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(&read_svc),
            )
            .await?;
    }

    // Apply PodDisruptionBudget
    let pdb = pdb::generate_pod_disruption_budget(obj);
    let pdb_api: Api<k8s_openapi::api::policy::v1::PodDisruptionBudget> =
        Api::namespaced(ctx.client.clone(), namespace);
    pdb_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&pdb),
        )
        .await?;

    // Apply cert-manager Certificate for TLS
    let certificate = certificate::generate_certificate(obj);
    let cert_name = certificate::certificate_secret_name(obj);
    let cert_ar = ApiResource::from_gvk(&kube::api::GroupVersionKind {
        group: "cert-manager.io".to_string(),
        version: "v1".to_string(),
        kind: "Certificate".to_string(),
    });
    let cert_api: Api<DynamicObject> =
        Api::namespaced_with(ctx.client.clone(), namespace, &cert_ar);
    let cert_value: serde_json::Value = serde_json::to_value(&certificate)
        .map_err(|e| Error::Validation(format!("Failed to serialize certificate: {}", e)))?;
    cert_api
        .patch(
            &cert_name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&cert_value),
        )
        .await?;

    debug!(name = %name, "Applied owned resources");
    Ok(())
}

/// Check number of ready replicas from StatefulSet
async fn check_ready_replicas(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    let name = obj.name_any();
    let sts_api: Api<k8s_openapi::api::apps::v1::StatefulSet> =
        Api::namespaced(ctx.client.clone(), namespace);

    match sts_api.get(&name).await {
        Ok(statefulset) => {
            let ready = statefulset
                .status
                .as_ref()
                .and_then(|s| s.ready_replicas)
                .unwrap_or(0);
            Ok(ready)
        }
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(0),
        Err(e) => Err(Error::Kube(e)),
    }
}

/// Check number of running pods (containers started, but not necessarily Ready).
/// Used during cluster initialization when readiness probe requires cluster to be OK.
async fn check_running_pods(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    use k8s_openapi::api::core::v1::Pod;

    let name = obj.name_any();
    let pod_api: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);

    let label_selector = format!("app.kubernetes.io/name={}", name);
    let pods = pod_api
        .list(&kube::api::ListParams::default().labels(&label_selector))
        .await?;

    let running_count = pods
        .items
        .iter()
        .filter(|pod| {
            pod.status
                .as_ref()
                .and_then(|s| s.phase.as_ref())
                .map(|phase| phase == "Running")
                .unwrap_or(false)
        })
        .count();

    Ok(running_count as i32)
}

/// Execute cluster initialization (CLUSTER MEET)
async fn execute_cluster_init(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Build topology to get pod IPs for CLUSTER MEET
    let topology = ClusterTopology::build(&ctx.client, namespace, &name, None)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Execute cluster meet
    cluster_init::execute_cluster_meet(
        obj,
        &topology,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(())
}

/// Execute stale IP recovery via CLUSTER MEET.
///
/// This is called when the cluster is in Failed state with all pods Running
/// but not Ready, indicating a potential stale IP condition where nodes.conf
/// has old IP addresses after pod restarts.
async fn execute_stale_ip_recovery(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Build topology to get current pod IPs
    let topology = ClusterTopology::build(&ctx.client, namespace, &name, None)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Execute recovery via CLUSTER MEET
    cluster_init::recover_stale_ips(
        obj,
        &topology,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(())
}

/// Execute slot assignment without setting up replicas.
/// Used during initial creation when replicas are configured in a separate phase.
async fn execute_slot_assignment_no_replicas(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Assign slots to masters only
    cluster_init::assign_slots_to_masters(obj, password.as_deref(), tls_certs.as_ref(), &strategy)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(())
}

/// Execute replica setup (CLUSTER REPLICATE).
/// Used during initial creation after slots are assigned.
async fn execute_replica_setup(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    if obj.spec.replicas_per_master == 0 {
        return Ok(());
    }

    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    cluster_init::setup_replicas(obj, password.as_deref(), tls_certs.as_ref(), &strategy)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(())
}

/// Execute slot rebalancing during scale-up.
/// Returns true when rebalancing is complete, false if still in progress.
async fn execute_scale_up_rebalance(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    // Execute the scaling operation to rebalance slots
    // This internally handles promotion and slot migration
    let result = execute_scaling_operation(obj, ctx, namespace).await?;

    // Check if scaling is complete
    if let Some(ref e) = result.error {
        return Err(Error::Valkey(e.clone()));
    }
    // Operation is complete when it succeeds and all slots have been moved
    Ok(result.success)
}

/// Execute slot evacuation during scale-down.
/// Returns true when evacuation is complete, false if still in progress.
async fn execute_scale_down_evacuation(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    // Use the scaling operation which handles both scale-up and scale-down
    let result = execute_scaling_operation(obj, ctx, namespace).await?;

    // Check if scaling is complete
    if let Some(ref e) = result.error {
        return Err(Error::Valkey(e.clone()));
    }
    // Operation is complete when it succeeds
    Ok(result.success)
}

/// Execute CLUSTER FORGET for nodes being removed during scale-down.
/// Returns true when all nodes are forgotten, false if still in progress.
async fn execute_forget_removed_nodes(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    let name = obj.name_any();
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Connect to cluster
    let (host, port) = strategy
        .get_connection(0)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    let client = crate::client::ValkeyClient::connect_single(
        &host,
        port,
        password.as_deref(),
        tls_certs.as_ref(),
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    let _ = client.close().await;

    // Build ClusterTopology to find orphaned nodes
    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(&cluster_nodes))
            .await
            .map_err(|e| Error::Valkey(e.to_string()))?;

    // Find orphaned nodes: cluster nodes whose IP is NOT in our current pod list
    let orphaned = topology.orphaned_nodes(&cluster_nodes);
    let orphan_node_ids: Vec<String> = orphaned.iter().map(|n| n.node_id.clone()).collect();

    if orphan_node_ids.is_empty() {
        // All orphaned nodes have been forgotten
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

    // Execute CLUSTER FORGET for orphaned nodes
    cluster_init::forget_nodes_with_retry(
        obj,
        &orphan_node_ids,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    // Still need to verify all nodes are gone
    Ok(false)
}

/// Health check result containing cluster state information.
pub struct ClusterHealthStatus {
    /// Whether the cluster is healthy (state=ok, all slots assigned).
    pub is_healthy: bool,
    /// Number of healthy master nodes.
    pub healthy_masters: i32,
    /// Number of healthy replica nodes.
    pub healthy_replicas: i32,
    /// Total slots assigned.
    pub slots_assigned: i32,
    /// Cluster topology if available.
    pub topology: Option<crate::crd::ClusterTopology>,
}

impl From<crate::client::ClusterState> for ClusterHealthStatus {
    fn from(state: crate::client::ClusterState) -> Self {
        ClusterHealthStatus {
            is_healthy: state.is_healthy(0), // Will be set correctly by caller
            healthy_masters: state.healthy_masters_count(),
            healthy_replicas: state.healthy_replicas_count(),
            slots_assigned: state.cluster_info.slots_assigned,
            topology: state.topology,
        }
    }
}

/// Check the health of the Valkey cluster.
async fn check_cluster_health(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<ClusterHealthStatus, Error> {
    use crate::crd::{ClusterTopology, MasterNode, ReplicaNode};

    // Build host list for connecting to the cluster
    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Ok(ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 0,
            healthy_replicas: 0,
            slots_assigned: 0,
            topology: None,
        });
    }

    let client = match ctx.connect_to_cluster(obj, namespace, 0).await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "Failed to connect to cluster for health check");
            return Ok(ClusterHealthStatus {
                is_healthy: false,
                healthy_masters: 0,
                healthy_replicas: 0,
                slots_assigned: 0,
                topology: None,
            });
        }
    };

    // Get cluster info
    let cluster_info = match client.cluster_info().await {
        Ok(info) => info,
        Err(e) => {
            debug!(error = %e, "Failed to get cluster info");
            let _ = client.close().await;
            return Ok(ClusterHealthStatus {
                is_healthy: false,
                healthy_masters: 0,
                healthy_replicas: 0,
                slots_assigned: 0,
                topology: None,
            });
        }
    };

    // Get cluster nodes for topology
    let cluster_nodes = match client.cluster_nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            debug!(error = %e, "Failed to get cluster nodes");
            let _ = client.close().await;
            return Ok(ClusterHealthStatus {
                is_healthy: cluster_info.is_healthy(),
                healthy_masters: cluster_info.cluster_size,
                healthy_replicas: 0,
                slots_assigned: cluster_info.slots_assigned,
                topology: None,
            });
        }
    };

    let _ = client.close().await;

    // Create cluster state (single source of truth)
    let cluster_state = crate::client::ClusterState {
        cluster_info: cluster_info.clone(),
        cluster_nodes: cluster_nodes.clone(),
        topology: None, // Will be set below
    };

    // Use cluster state for health check (single source of truth)
    let expected_masters = obj.spec.masters;
    let is_healthy = cluster_state.is_healthy(expected_masters);

    if !is_healthy {
        let health_msg = cluster_state.health_status_message(expected_masters);
        warn!(
            cluster = %obj.name_any(),
            message = %health_msg,
            cluster_state = %cluster_state.cluster_info.state,
            slots_assigned = cluster_state.cluster_info.slots_assigned,
            slots_fail = cluster_state.cluster_info.slots_fail,
            slots_pfail = cluster_state.cluster_info.slots_pfail,
            masters_count = cluster_state.cluster_nodes.masters().len(),
            healthy_masters = cluster_state.healthy_masters_count(),
            "Cluster health check failed"
        );
    }

    // Build topology from cluster nodes
    let masters = cluster_nodes.masters();

    // Build topology structure
    let topology = ClusterTopology {
        masters: masters
            .iter()
            .map(|m| {
                let node_replicas = cluster_nodes.replicas_of(&m.node_id);
                MasterNode {
                    node_id: m.node_id.clone(),
                    pod_name: extract_pod_name(&m.address),
                    slot_ranges: m.slots.iter().map(|s| s.to_string()).collect(),
                    replicas: node_replicas
                        .iter()
                        .map(|r| ReplicaNode {
                            node_id: r.node_id.clone(),
                            pod_name: extract_pod_name(&r.address),
                            replication_lag: 0, // TODO: Get actual lag from INFO REPLICATION
                        })
                        .collect(),
                }
            })
            .collect(),
    };

    Ok(ClusterHealthStatus {
        is_healthy: cluster_state.is_healthy(expected_masters),
        healthy_masters: cluster_state.healthy_masters_count(),
        healthy_replicas: cluster_state.healthy_replicas_count(),
        slots_assigned: cluster_state.cluster_info.slots_assigned,
        topology: Some(topology),
    })
}

/// Update the status of a ValkeyCluster
///
/// IMPORTANT: The `reconcile_generation` parameter should be the generation
/// from the object at the START of reconciliation, not re-fetched. This prevents
/// race conditions where a spec change during reconciliation could cause the
/// observed_generation to be set to the new value before we've processed the change.
async fn update_status(
    api: &Api<ValkeyCluster>,
    name: &str,
    phase: ClusterPhase,
    ready_replicas: i32,
    error_message: Option<&str>,
    health_status: Option<&ClusterHealthStatus>,
    reconcile_generation: Option<i64>,
) -> Result<(), Error> {
    let obj = api.get(name).await?;

    // Use the generation from reconcile start, not the current generation
    // This prevents race conditions when spec changes during reconciliation
    let generation = reconcile_generation.or(obj.metadata.generation);
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    // Calculate expected totals
    let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

    let conditions = if phase == ClusterPhase::Running {
        vec![Condition::ready(
            true,
            "AllReplicasReady",
            "All replicas are ready",
            generation,
        )]
    } else if phase == ClusterPhase::Failed {
        vec![Condition::ready(
            false,
            "ReconciliationFailed",
            error_message.unwrap_or("Resource failed"),
            generation,
        )]
    } else {
        vec![Condition::progressing(
            true,
            "Reconciling",
            &format!("Phase: {}", phase),
            generation,
        )]
    };

    // Calculate connection endpoint (client service)
    let connection_endpoint = if phase == ClusterPhase::Running {
        Some(format!("valkey://{}.{}.svc:6379", name, namespace))
    } else {
        None
    };

    // Calculate assigned slots string from health status or defaults
    let (assigned_slots, topology, ready_masters) = if let Some(health) = health_status {
        (
            format!("{}/16384", health.slots_assigned),
            health.topology.clone(),
            health.healthy_masters,
        )
    } else if phase == ClusterPhase::Running {
        ("16384/16384".to_string(), None, obj.spec.masters)
    } else {
        ("0/16384".to_string(), None, 0)
    };

    // Get TLS secret name from certificate
    let tls_secret_name = certificate::certificate_secret_name(&obj);

    // Preserve existing operation state if present
    let existing_status = obj.status.as_ref();
    let current_operation = existing_status.and_then(|s| s.current_operation.clone());
    let operation_started_at = existing_status.and_then(|s| s.operation_started_at.clone());

    // Set connection secret to auth secret name when running
    let connection_secret = if phase == ClusterPhase::Running {
        Some(obj.spec.auth.secret_ref.name.clone())
    } else {
        None
    };

    let operation_progress = existing_status.and_then(|s| s.operation_progress.clone());
    let message = existing_status
        .map(|s| s.message.clone())
        .unwrap_or_default();
    let last_phase_transition = existing_status.and_then(|s| s.last_phase_transition.clone());

    // Track reconcile count for stuck detection
    let prev_phase = existing_status
        .map(|s| s.phase)
        .unwrap_or(ClusterPhase::Pending);
    let reconcile_count = if phase == prev_phase {
        existing_status.map(|s| s.reconcile_count).unwrap_or(0) + 1
    } else {
        0
    };

    // Update last_phase_transition if phase changed
    let last_phase_transition = if phase != prev_phase {
        Some(jiff::Timestamp::now().to_string())
    } else {
        last_phase_transition
    };

    // Handle recovery tracking fields
    // Reset recovery_attempts when transitioning away from Failed state
    // Preserve them when staying in Failed state
    let (recovery_attempts, last_recovery_attempt) = if phase != ClusterPhase::Failed {
        // Transitioning away from Failed (or never in Failed) - reset recovery state
        (0, None)
    } else {
        // Staying in Failed state - preserve existing recovery state
        (
            existing_status.map(|s| s.recovery_attempts).unwrap_or(0),
            existing_status.and_then(|s| s.last_recovery_attempt.clone()),
        )
    };

    // Set last_error and summary based on phase and health
    let (last_error, summary) = if phase == ClusterPhase::Running {
        // Healthy - clear error and set healthy summary
        let slots = health_status.map(|h| h.slots_assigned).unwrap_or(16384);
        let healthy_replicas = health_status.map(|h| h.healthy_replicas).unwrap_or(0);
        (
            None,
            Some(format!(
                "Healthy: {} masters, {} replicas, {}/16384 slots",
                ready_masters, healthy_replicas, slots
            )),
        )
    } else if phase == ClusterPhase::Failed {
        // Failed - preserve or set error message
        let err = error_message
            .map(|e| e.to_string())
            .or_else(|| existing_status.and_then(|s| s.last_error.clone()));
        (
            err,
            Some(format!(
                "Failed: {}",
                error_message.unwrap_or("unknown error")
            )),
        )
    } else {
        // In progress - preserve existing error if any
        let err = existing_status.and_then(|s| s.last_error.clone());
        (err, Some(format!("Phase: {}", phase)))
    };

    let status = ValkeyClusterStatus {
        phase,
        ready_nodes: format!("{}/{}", ready_replicas, desired_replicas),
        ready_masters,
        ready_replicas,
        assigned_slots,
        observed_generation: generation,
        conditions,
        topology,
        valkey_version: Some("9".to_string()),
        connection_endpoint,
        connection_secret,
        tls_secret: Some(tls_secret_name),
        current_operation,
        operation_started_at,
        operation_progress,
        message,
        reconcile_count,
        last_phase_transition,
        recovery_attempts,
        last_recovery_attempt,
        last_error,
        summary,
    };

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

/// Get the current number of master nodes from the Valkey cluster.
async fn get_current_master_count(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    // Build host list for connecting to the cluster
    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Ok(0);
    }

    let client = match ctx.connect_to_cluster(obj, namespace, 0).await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "Failed to connect to cluster for master count");
            return Ok(0);
        }
    };

    // Get cluster nodes
    let cluster_nodes = match client.cluster_nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            debug!(error = %e, "Failed to get cluster nodes");
            let _ = client.close().await;
            return Ok(0);
        }
    };

    let _ = client.close().await;

    let masters = cluster_nodes.masters();
    Ok(masters.len() as i32)
}

/// Promote replicas to masters during scale-up.
///
/// When scaling from N to M masters (where M > N), pods at ordinals N..M may
/// currently be replicas in the cluster. These need to be promoted to masters
/// before rebalancing can assign them slots.
///
/// This function uses ClusterTopology to correctly identify replicas that should
/// be masters by correlating pod IPs with cluster node state. This fixes the bug
/// where `extract_ordinal_from_address` failed on IP addresses.
///
/// This function:
/// 1. Identifies ordinals that should be masters but are replicas
/// 2. Executes CLUSTER RESET SOFT on them to clear replication
/// 3. Waits for the cluster to stabilize
///
/// Returns the list of ordinals that were promoted.
async fn promote_replicas_to_masters(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    current_masters: i32,
    target_masters: i32,
) -> Result<Vec<i32>, Error> {
    if target_masters <= current_masters {
        return Ok(Vec::new());
    }

    let name = obj.name_any();
    let mut promoted = Vec::new();

    // Get current cluster topology
    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to connect for promotion check: {}", e)))?;

    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(format!("Failed to get cluster nodes: {}", e)))?;

    let _ = client.close().await;

    // Build ClusterTopology to correlate pods with cluster nodes via IP matching
    // This fixes the bug where extract_ordinal_from_address fails on IP addresses
    let topology = ClusterTopology::build(&ctx.client, namespace, &name, Some(&cluster_nodes))
        .await
        .map_err(|e| Error::Valkey(format!("Failed to build topology: {}", e)))?;

    // Use topology to find replicas that should be masters
    // This correctly handles all ordinals 0..target_masters, not just new ones
    let to_promote = topology.replicas_that_should_be_masters(target_masters);

    for node in to_promote {
        info!(
            cluster = %name,
            ordinal = node.ordinal,
            node_id = %node.node_id.as_deref().unwrap_or("unknown"),
            "Promoting replica to master via CLUSTER RESET SOFT"
        );

        // Connect to this specific pod and reset it
        let replica_client = ctx
            .connect_to_cluster(obj, namespace, node.ordinal)
            .await
            .map_err(|e| {
                Error::Valkey(format!("Failed to connect to pod {}: {}", node.ordinal, e))
            })?;

        match replica_client.cluster_reset(false).await {
            Ok(()) => {
                info!(
                    cluster = %name,
                    ordinal = node.ordinal,
                    "Replica promoted to master (CLUSTER RESET SOFT succeeded)"
                );
                promoted.push(node.ordinal);
            }
            Err(e) => {
                warn!(
                    cluster = %name,
                    ordinal = node.ordinal,
                    error = %e,
                    "Failed to promote replica to master"
                );
            }
        }

        let _ = replica_client.close().await;
    }

    if !promoted.is_empty() {
        // Wait for the cluster to stabilize after resets
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Re-meet the promoted nodes back into the cluster
        // (CLUSTER RESET removes them from the cluster configuration)
        // Connect to master 0 to issue CLUSTER MEET
        let client = ctx
            .connect_to_cluster(obj, namespace, 0)
            .await
            .map_err(|e| Error::Valkey(format!("Failed to connect for CLUSTER MEET: {}", e)))?;

        for &ordinal in &promoted {
            // Use topology to get the IP for this ordinal
            if let Some(node) = topology.by_ordinal(ordinal)
                && let Some(endpoint) = &node.endpoint
            {
                info!(
                    cluster = %name,
                    ordinal = ordinal,
                    ip = %endpoint.ip(),
                    port = endpoint.port(),
                    "Re-adding promoted node via CLUSTER MEET"
                );
                if let Err(e) = client.cluster_meet(endpoint.ip(), endpoint.port()).await {
                    warn!(
                        cluster = %name,
                        ordinal = ordinal,
                        error = %e,
                        "Failed to CLUSTER MEET promoted node"
                    );
                }
            }
        }

        let _ = client.close().await;

        // Wait for gossip propagation
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    Ok(promoted)
}

/// Execute a scaling operation (scale up or scale down).
///
/// For scale-down operations, this function connects directly to each source node
/// to execute CLUSTER MIGRATESLOTS, since that command must run on the node that
/// owns the slots being migrated.
async fn execute_scaling_operation(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<crate::client::ScalingResult, Error> {
    // Build host list for connecting to the cluster
    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Err(Error::Valkey("No pod addresses available".to_string()));
    }

    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to connect for scaling: {}", e)))?;

    // Get current master count
    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(format!("Failed to get cluster nodes: {}", e)))?;

    let current_masters = cluster_nodes.masters().len() as i32;
    let target_masters = obj.spec.masters;
    let _ = client.close().await;

    // For scale-up: promote any replicas that should become masters
    if target_masters > current_masters {
        let promoted =
            promote_replicas_to_masters(obj, ctx, namespace, current_masters, target_masters)
                .await?;
        if !promoted.is_empty() {
            info!(
                promoted = ?promoted,
                "Promoted replicas to masters before scaling"
            );
        }
    }

    // Re-connect and get updated cluster state after promotions
    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to reconnect for scaling: {}", e)))?;

    let cluster_nodes = client.cluster_nodes().await.map_err(|e| {
        Error::Valkey(format!(
            "Failed to get cluster nodes after promotion: {}",
            e
        ))
    })?;

    let current_masters = cluster_nodes.masters().len() as i32;

    // Build scaling context
    let scaling_ctx = ScalingContext {
        current_masters,
        target_masters,
        namespace: namespace.to_string(),
        headless_service: common::headless_service_name(obj),
        port: 6379,
    };

    // Execute the appropriate scaling operation
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
        // For scale-down, we need to connect to each source node directly
        // because CLUSTER MIGRATESLOTS must be executed on the source node
        let _ = client.close().await;
        execute_scale_down(obj, ctx, namespace, &cluster_nodes, &scaling_ctx).await
    } else {
        // current_masters == target_masters: nodes are already in cluster, just need to rebalance.
        // This happens when CLUSTER MEET was done in AddingNodes phase and we're in MigratingSlots
        // to redistribute slots to the newly joined nodes.
        info!(
            current = current_masters,
            target = target_masters,
            "Rebalancing slots (masters already at target count)"
        );
        let _ = client.close().await;
        // Use execute_rebalance_slots which uses port-forwarding for each source node
        execute_rebalance_slots(obj, ctx, namespace, &cluster_nodes, target_masters).await
    };

    result.map_err(|e| Error::Valkey(format!("Scaling operation failed: {}", e)))
}

/// Execute a scale-down operation with proper per-node connections.
///
/// CLUSTER MIGRATESLOTS must be executed on the source node (the node that currently
/// owns the slots). This function connects to each node being removed and executes
/// the migration commands from there.
///
/// Uses ClusterTopology for correct IP→ordinal mapping, fixing issues where
/// extract_ordinal_from_address fails on IP addresses.
async fn execute_scale_down(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    cluster_nodes: &crate::client::ParsedClusterNodes,
    scaling_ctx: &ScalingContext,
) -> Result<crate::client::ScalingResult, crate::client::ValkeyError> {
    use crate::client::ValkeyError;

    let mut result = crate::client::ScalingResult::default();
    let masters: Vec<_> = cluster_nodes.masters().into_iter().collect();

    // Find masters to remove (highest ordinals)
    let ordinals_to_remove = scaling_ctx.masters_to_remove();

    // Build ClusterTopology for IP→ordinal mapping
    // This replaces the ad-hoc HashMap building that was prone to bugs
    let cluster_name = obj.name_any();

    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(cluster_nodes)).await?;

    // Use topology's IP→ordinal map for lookups
    let ip_to_ordinal = topology.ip_to_ordinal_map();

    // Find nodes to remove by matching their IPs to ordinals
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

    if nodes_to_remove.is_empty() {
        warn!(
            ordinals_to_remove = ?ordinals_to_remove,
            master_ips = ?masters.iter().map(|m| &m.ip).collect::<Vec<_>>(),
            ip_to_ordinal = ?ip_to_ordinal,
            "No nodes found to remove, skipping scale down"
        );
        return Ok(result);
    }

    // Find remaining masters (those not being removed)
    let remaining_masters: Vec<_> = masters
        .iter()
        .filter(|m| !nodes_to_remove.iter().any(|n| n.node_id == m.node_id))
        .copied()
        .collect();

    if remaining_masters.is_empty() {
        return Err(ValkeyError::InvalidConfig(
            "Cannot scale down: no remaining masters to receive slots".to_string(),
        ));
    }

    info!(
        nodes_to_remove = nodes_to_remove.len(),
        remaining_masters = remaining_masters.len(),
        "Starting scale-down slot migration"
    );

    // Migrate slots from each node being removed
    for node in &nodes_to_remove {
        let slot_ranges = node.slots.clone();

        if slot_ranges.is_empty() {
            debug!(node_id = %node.node_id, "Node has no slots to migrate");
            continue;
        }

        // Look up the ordinal for this node's IP using topology
        let Some(&ordinal) = ip_to_ordinal.get(&node.ip) else {
            return Err(ValkeyError::InvalidConfig(format!(
                "Cannot find ordinal for node IP {}: no matching pod",
                node.ip
            )));
        };

        info!(
            node_id = %node.node_id,
            ip = %node.ip,
            ordinal = ordinal,
            slot_ranges = slot_ranges.len(),
            "Connecting to source node for slot migration"
        );

        // Connect to the source node via port forwarding using its ordinal
        // CLUSTER MIGRATESLOTS must be executed on the node that owns the slots
        let source_client = ctx
            .connect_to_cluster(obj, namespace, ordinal)
            .await
            .map_err(|e| {
                ValkeyError::Connection(format!(
                    "Failed to connect to source node {} (ordinal {}): {}",
                    node.node_id, ordinal, e
                ))
            })?;

        // Migrate each slot range to a remaining master (round-robin)
        for (range_idx, range) in slot_ranges.iter().enumerate() {
            let dest_idx = range_idx % remaining_masters.len();
            let Some(dest_node) = remaining_masters.get(dest_idx) else {
                let _ = source_client.close().await;
                return Err(ValkeyError::InvalidConfig(
                    "Cannot determine destination master for slot migration".to_string(),
                ));
            };

            info!(
                from_node = %node.node_id,
                to_node = %dest_node.node_id,
                start_slot = range.start,
                end_slot = range.end,
                "Migrating slot range using CLUSTER MIGRATESLOTS"
            );

            // Execute CLUSTER MIGRATESLOTS from the source node
            match source_client
                .cluster_migrateslots(range.start as u16, range.end as u16, &dest_node.node_id)
                .await
            {
                Ok(()) => {
                    // Wait for migration to complete
                    if let Err(e) = wait_for_slot_migration(
                        ctx,
                        obj,
                        namespace,
                        range.start as u16,
                        range.end as u16,
                        &dest_node.node_id,
                    )
                    .await
                    {
                        let _ = source_client.close().await;
                        return Err(e);
                    }
                    result.slots_moved += range.end - range.start + 1;
                    debug!(range = ?range, "Slot range migration completed");
                }
                Err(e) => {
                    warn!(
                        range = ?range,
                        error = %e,
                        "Failed to migrate slot range"
                    );
                    let _ = source_client.close().await;
                    return Err(e);
                }
            }
        }

        let _ = source_client.close().await;
    }

    // After all migrations complete, CLUSTER FORGET the removed nodes
    // This can be done from any node in the cluster
    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| ValkeyError::Connection(format!("Failed to connect for FORGET: {}", e)))?;

    for node in &nodes_to_remove {
        info!(node_id = %node.node_id, "Removing node from cluster via CLUSTER FORGET");
        if let Err(e) = client.cluster_forget(&node.node_id).await {
            warn!(node_id = %node.node_id, error = %e, "Failed to forget node, continuing");
            // Don't fail the whole operation if FORGET fails for one node
        }
        result.nodes_removed.push(node.node_id.clone());
    }

    let _ = client.close().await;

    info!(
        nodes_removed = result.nodes_removed.len(),
        slots_moved = result.slots_moved,
        "Scale down complete"
    );

    Ok(result)
}

/// Execute slot rebalancing with port-forwarding for each source node.
///
/// CLUSTER MIGRATESLOTS must be executed on the source node (the node that currently
/// owns the slots). This function connects to each source node via port-forwarding
/// and executes the migration commands from there.
///
/// Uses ClusterTopology for correct IP→ordinal mapping.
async fn execute_rebalance_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    cluster_nodes: &crate::client::ParsedClusterNodes,
    target_masters: i32,
) -> Result<crate::client::ScalingResult, crate::client::ValkeyError> {
    use crate::client::ValkeyError;
    use crate::slots::calculate_distribution;
    use std::collections::HashMap;

    let mut result = crate::client::ScalingResult::default();
    let masters: Vec<_> = cluster_nodes
        .nodes
        .iter()
        .filter(|n| {
            !n.is_replica()
                && n.master_id
                    .as_ref()
                    .is_none_or(|id| id == "-" || id.is_empty())
        })
        .collect();

    if masters.len() != target_masters as usize {
        return Err(ValkeyError::ClusterNotReady(format!(
            "Expected {} masters but found {}",
            target_masters,
            masters.len()
        )));
    }

    // Build ClusterTopology for IP→ordinal mapping
    let cluster_name = obj.name_any();
    let topology =
        ClusterTopology::build(&ctx.client, namespace, &cluster_name, Some(cluster_nodes)).await?;

    // Use topology's IP→ordinal map
    let ip_to_ordinal = topology.ip_to_ordinal_map();

    // Calculate target slot distribution
    let target_distribution = calculate_distribution(target_masters as u16);

    // Build current slot ownership map and node info lookup
    let mut current_ownership: HashMap<u16, String> = HashMap::new();
    let mut node_info: HashMap<String, (String, i32)> = HashMap::new(); // node_id -> (ip, ordinal)

    for master in &masters {
        if let Some(&ordinal) = ip_to_ordinal.get(&master.ip) {
            node_info.insert(master.node_id.clone(), (master.ip.clone(), ordinal));
        }
        for range in &master.slots {
            for slot in range.start..=range.end {
                current_ownership.insert(slot as u16, master.node_id.clone());
            }
        }
    }

    // Collect all slot migrations grouped by source node
    // Key: source_node_id, Value: list of (slot, dest_node_id)
    let mut migrations_by_source: HashMap<String, Vec<(u16, String)>> = HashMap::new();
    let mut unassigned_slots: HashMap<String, Vec<u16>> = HashMap::new();

    for (master, range) in masters.iter().zip(target_distribution.iter()) {
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

    // Group consecutive slots into ranges helper
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

    // Migrate slots from each source node
    for (source_node_id, slot_migrations) in migrations_by_source {
        // Group by destination
        let mut by_dest: HashMap<String, Vec<u16>> = HashMap::new();
        for (slot, dest) in slot_migrations {
            by_dest.entry(dest).or_default().push(slot);
        }

        // Get source node's ordinal for port-forwarding (using topology)
        let Some((_, ordinal)) = node_info.get(&source_node_id) else {
            return Err(ValkeyError::InvalidConfig(format!(
                "Cannot find ordinal for source node {}",
                source_node_id
            )));
        };
        let ordinal = *ordinal;

        info!(
            source_node = %source_node_id,
            ordinal = ordinal,
            destinations = by_dest.len(),
            "Connecting to source node for slot migration"
        );

        // Connect to the source node via port forwarding
        let source_client = ctx
            .connect_to_cluster(obj, namespace, ordinal)
            .await
            .map_err(|e| {
                ValkeyError::Connection(format!(
                    "Failed to connect to source node {} (ordinal {}): {}",
                    source_node_id, ordinal, e
                ))
            })?;

        // Migrate slots to each destination in batched ranges
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

                // Execute CLUSTER MIGRATESLOTS from the source node
                match source_client
                    .cluster_migrateslots(start, end, &dest_node_id)
                    .await
                {
                    Ok(()) => {
                        // Wait for migration to complete
                        if let Err(e) =
                            wait_for_slot_migration(ctx, obj, namespace, start, end, &dest_node_id)
                                .await
                        {
                            let _ = source_client.close().await;
                            return Err(e);
                        }
                        result.slots_moved += (end - start + 1) as i32;
                    }
                    Err(e) => {
                        if e.to_string().contains("unknown subcommand") {
                            let _ = source_client.close().await;
                            return Err(ValkeyError::InvalidConfig(
                                "Slot rebalancing requires Valkey 9.0+ (CLUSTER MIGRATESLOTS)."
                                    .to_string(),
                            ));
                        }
                        warn!(
                            start_slot = start,
                            end_slot = end,
                            error = %e,
                            "Slot range migration failed"
                        );
                        let _ = source_client.close().await;
                        return Err(e);
                    }
                }
            }
        }

        let _ = source_client.close().await;
    }

    // Handle unassigned slots (should be rare, but handle for completeness)
    if !unassigned_slots.is_empty() {
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

/// Wait for a slot migration to complete by polling cluster state.
async fn wait_for_slot_migration(
    ctx: &Context,
    obj: &ValkeyCluster,
    namespace: &str,
    start_slot: u16,
    end_slot: u16,
    target_node_id: &str,
) -> Result<(), crate::client::ValkeyError> {
    use crate::client::ValkeyError;
    use std::time::{Duration, Instant};

    const MAX_WAIT_SECS: u64 = 300; // 5 minutes max
    const POLL_INTERVAL_SECS: u64 = 2;
    let start = Instant::now();

    loop {
        if start.elapsed().as_secs() > MAX_WAIT_SECS {
            return Err(ValkeyError::Timeout {
                operation: format!(
                    "Slot migration {}-{} to {}",
                    start_slot, end_slot, target_node_id
                ),
                duration: Duration::from_secs(MAX_WAIT_SECS),
            });
        }

        // Connect to check cluster state
        let client = ctx
            .connect_to_cluster(obj, namespace, 0)
            .await
            .map_err(|e| {
                ValkeyError::Connection(format!("Failed to connect for migration check: {}", e))
            })?;

        let nodes_result = client.cluster_nodes().await;
        let _ = client.close().await;

        match nodes_result {
            Ok(nodes) => {
                if let Some(target) = nodes.get_node(target_node_id) {
                    // Check if target node now owns ALL slots in the range
                    // Note: Valkey may fragment slots into multiple ranges after migration,
                    // so we need to check each slot individually rather than looking for
                    // a single contiguous range that contains the entire migrated range.
                    let owns_all_slots = (start_slot..=end_slot).all(|slot| {
                        target
                            .slots
                            .iter()
                            .any(|range| range.start <= slot as i32 && range.end >= slot as i32)
                    });

                    if owns_all_slots {
                        debug!(
                            start_slot = start_slot,
                            end_slot = end_slot,
                            "Slot range migration verified"
                        );
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                debug!(error = %e, "Failed to get cluster nodes, will retry");
            }
        }

        // Migration still in progress, wait and poll again
        debug!(
            start_slot = start_slot,
            end_slot = end_slot,
            "Slot migration in progress, waiting..."
        );
        tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECS)).await;
    }
}

/// Get the total number of nodes in the Valkey cluster.
async fn get_nodes_in_cluster_count(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    let client = match ctx.connect_to_cluster(obj, namespace, 0).await {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "Failed to connect to cluster for node count");
            return Ok(0);
        }
    };

    let cluster_nodes = match client.cluster_nodes().await {
        Ok(nodes) => nodes,
        Err(e) => {
            let _ = client.close().await;
            debug!(error = %e, "Failed to get cluster nodes");
            return Ok(0);
        }
    };

    let _ = client.close().await;
    Ok(cluster_nodes.nodes.len() as i32)
}

/// Add new replica pods to an existing cluster.
/// Returns the number of replicas added.
#[allow(clippy::too_many_lines)]
async fn add_new_replicas_to_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    let name = obj.name_any();
    let masters = obj.spec.masters;
    let replicas_per_master = obj.spec.replicas_per_master;

    // First, get the current cluster state
    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to connect: {}", e)))?;

    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(format!("Failed to get cluster nodes: {}", e)))?;

    let current_node_count = cluster_nodes.nodes.len() as i32;
    let desired_total = total_pods(masters, replicas_per_master);
    let _ = client.close().await;

    // If we already have all nodes, nothing to do
    if current_node_count >= desired_total {
        return Ok(0);
    }

    // Build ClusterTopology to find pods not yet in cluster
    let topology = ClusterTopology::build(&ctx.client, namespace, &name, Some(&cluster_nodes))
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Get pods not yet in cluster
    let new_pods: Vec<_> = topology.nodes_not_in_cluster().collect();

    if new_pods.is_empty() {
        return Ok(0);
    }

    info!(
        name = %name,
        new_count = new_pods.len(),
        "Adding new nodes to cluster via CLUSTER MEET"
    );

    // Connect to first master and run CLUSTER MEET for each new node
    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to connect: {}", e)))?;

    // Execute CLUSTER MEET for each new pod
    for node in &new_pods {
        let Some(endpoint) = &node.endpoint else {
            warn!(pod = %node.pod_name, "Pod has no IP, skipping CLUSTER MEET");
            continue;
        };
        info!(pod = %node.pod_name, ip = %endpoint.ip(), "Executing CLUSTER MEET");
        client
            .cluster_meet(endpoint.ip(), endpoint.port())
            .await
            .map_err(|e| {
                Error::Valkey(format!("CLUSTER MEET failed for {}: {}", node.pod_name, e))
            })?;
    }

    let _ = client.close().await;

    // Wait a moment for gossip to propagate
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Now configure new nodes as replicas of their corresponding masters
    // Replica pods are numbered after masters: if masters=3, then:
    //   - pod-0, pod-1, pod-2 are masters
    //   - pod-3 is replica of pod-0, pod-4 is replica of pod-1, pod-5 is replica of pod-2
    //   - pod-6 is replica of pod-0, pod-7 is replica of pod-1, pod-8 is replica of pod-2, etc.

    // Get updated cluster nodes to get node IDs
    let client = ctx
        .connect_to_cluster(obj, namespace, 0)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to connect: {}", e)))?;

    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(format!("Failed to get cluster nodes: {}", e)))?;

    let _ = client.close().await;

    // Rebuild topology with updated cluster nodes after CLUSTER MEET
    let topology = ClusterTopology::build(&ctx.client, namespace, &name, Some(&cluster_nodes))
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Get master node IDs (from ordinal 0 to masters-1)
    // IMPORTANT: Only include nodes that are ACTUALLY masters in the cluster topology.
    let mut master_node_ids: Vec<Option<String>> = Vec::with_capacity(masters as usize);
    for ordinal in 0..masters {
        if let Some(node) = topology.by_ordinal(ordinal) {
            // Only include if it's actually a master (has master flag or has slots)
            if node.is_master_with_slots() || node.role == NodeRole::Master {
                master_node_ids.push(node.node_id.clone());
            } else {
                // This pod ordinal should be a master but is still a replica in cluster
                debug!(
                    ordinal = ordinal,
                    node_id = ?node.node_id,
                    "Pod expected to be master is still a replica in cluster topology"
                );
                master_node_ids.push(None);
            }
        } else {
            // Node not found in cluster yet
            master_node_ids.push(None);
        }
    }

    // For each new replica, configure it
    let mut added = 0;
    for node in &new_pods {
        // Skip if this is a master pod
        if node.ordinal < masters {
            continue;
        }

        // Determine which master this replica belongs to
        let master_index = ((node.ordinal - masters) % masters) as usize;
        let master_node_id = match master_node_ids.get(master_index) {
            Some(Some(id)) => id,
            Some(None) => {
                // Master at this index is not yet a master in cluster topology
                debug!(
                    pod = %node.pod_name,
                    master_index = master_index,
                    "Skipping replica config: master not yet established in cluster"
                );
                continue;
            }
            None => {
                warn!(pod = %node.pod_name, "Cannot find master for replica");
                continue;
            }
        };

        // Get the node ID for this replica (should be set after CLUSTER MEET)
        let replica_node_id = match &node.node_id {
            Some(id) => id,
            None => {
                warn!(pod = %node.pod_name, "Replica has no node ID yet");
                continue;
            }
        };

        info!(
            pod = %node.pod_name,
            replica_node = %replica_node_id,
            master_node = %master_node_id,
            master_index = master_index,
            "Configuring replica"
        );

        let replica_client = ctx
            .connect_to_cluster(obj, namespace, node.ordinal)
            .await
            .map_err(|e| Error::Valkey(format!("Failed to connect to replica: {}", e)))?;

        match replica_client.cluster_replicate(master_node_id).await {
            Ok(()) => {
                info!(pod = %node.pod_name, "Successfully configured as replica");
                added += 1;
            }
            Err(e) => {
                warn!(pod = %node.pod_name, error = %e, "Failed to configure replica");
            }
        }

        let _ = replica_client.close().await;
    }

    Ok(added)
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
    use crate::crd::{
        AuthSpec, ImageSpec, IssuerRef, PersistenceSpec, ResourceRequirementsSpec, SchedulingSpec,
        SecretKeyRef, TlsSpec, ValkeyClusterSpec,
    };
    use std::collections::BTreeMap;

    /// Helper to create a minimal valid ValkeyCluster for testing
    fn create_test_cluster(name: &str, masters: i32, replicas_per_master: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: kube::api::ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("test-ns".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters,
                replicas_per_master,
                image: ImageSpec::default(),
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-auth-secret".to_string(),
                        key: "password".to_string(),
                    },
                    ..Default::default()
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
                ..Default::default()
            },
            status: None,
        }
    }

    // ==========================================================================
    // validate_spec() tests
    // ==========================================================================

    #[test]
    fn test_validate_spec_valid_minimum_cluster() {
        let cluster = create_test_cluster("test", 3, 0);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_validate_spec_valid_with_replicas() {
        let cluster = create_test_cluster("test", 3, 1);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_validate_spec_valid_max_replicas() {
        let cluster = create_test_cluster("test", 3, 5);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_validate_spec_valid_large_cluster() {
        let cluster = create_test_cluster("test", 100, 2);
        assert!(validate_spec(&cluster).is_ok());
    }

    #[test]
    fn test_validate_spec_invalid_zero_masters() {
        let cluster = create_test_cluster("test", 0, 1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Validation(_)));
        assert!(err.to_string().contains("masters must be at least 3"));
    }

    #[test]
    fn test_validate_spec_invalid_one_master() {
        let cluster = create_test_cluster("test", 1, 1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("masters must be at least 3")
        );
    }

    #[test]
    fn test_validate_spec_invalid_two_masters() {
        let cluster = create_test_cluster("test", 2, 1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("masters must be at least 3")
        );
    }

    #[test]
    fn test_validate_spec_invalid_too_many_masters() {
        let cluster = create_test_cluster("test", 101, 1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("masters cannot exceed 100")
        );
    }

    #[test]
    fn test_validate_spec_invalid_negative_replicas() {
        let cluster = create_test_cluster("test", 3, -1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("replicasPerMaster cannot be negative")
        );
    }

    #[test]
    fn test_validate_spec_invalid_too_many_replicas() {
        let cluster = create_test_cluster("test", 3, 6);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("replicasPerMaster cannot exceed 5")
        );
    }

    #[test]
    fn test_validate_spec_invalid_empty_tls_issuer() {
        let mut cluster = create_test_cluster("test", 3, 1);
        cluster.spec.tls.issuer_ref.name = String::new();
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("tls.issuerRef.name is required")
        );
    }

    #[test]
    fn test_validate_spec_invalid_empty_auth_secret() {
        let mut cluster = create_test_cluster("test", 3, 1);
        cluster.spec.auth.secret_ref.name = String::new();
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("auth.secretRef.name is required")
        );
    }

    // ==========================================================================
    // extract_pod_name() tests
    // ==========================================================================

    #[test]
    fn test_extract_pod_name_standard_format() {
        let address = "my-cluster-0.my-cluster-headless.default.svc.cluster.local:6379";
        assert_eq!(extract_pod_name(address), "my-cluster-0");
    }

    #[test]
    fn test_extract_pod_name_short_format() {
        let address = "my-cluster-0.my-cluster-headless:6379";
        assert_eq!(extract_pod_name(address), "my-cluster-0");
    }

    #[test]
    fn test_extract_pod_name_ip_address() {
        let address = "10.0.0.1:6379";
        assert_eq!(extract_pod_name(address), "10");
    }

    #[test]
    fn test_extract_pod_name_no_port() {
        let address = "my-cluster-0.my-cluster-headless";
        assert_eq!(extract_pod_name(address), "my-cluster-0");
    }

    #[test]
    fn test_extract_pod_name_empty_string() {
        let address = "";
        assert_eq!(extract_pod_name(address), "unknown");
    }

    #[test]
    fn test_extract_pod_name_just_port() {
        let address = ":6379";
        assert_eq!(extract_pod_name(address), "unknown");
    }

    #[test]
    fn test_extract_pod_name_complex_name() {
        let address =
            "my-valkey-cluster-prod-42.my-valkey-cluster-prod-headless.production.svc:6379";
        assert_eq!(extract_pod_name(address), "my-valkey-cluster-prod-42");
    }

    #[test]
    fn test_extract_pod_name_with_numbers() {
        let address = "cluster123-5.cluster123-headless.ns:6379";
        assert_eq!(extract_pod_name(address), "cluster123-5");
    }

    // ==========================================================================
    // Requeue duration tests
    // ==========================================================================

    #[test]
    fn test_requeue_duration_running_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Running).as_secs(), 30);
    }

    #[test]
    fn test_requeue_duration_creating_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Creating).as_secs(), 2);
    }

    #[test]
    fn test_requeue_duration_initial_creation_phases() {
        assert_eq!(requeue_duration(ClusterPhase::WaitingForPods).as_secs(), 2);
        assert_eq!(
            requeue_duration(ClusterPhase::InitializingCluster).as_secs(),
            2
        );
        assert_eq!(requeue_duration(ClusterPhase::AssigningSlots).as_secs(), 2);
        assert_eq!(
            requeue_duration(ClusterPhase::ConfiguringReplicas).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_failed_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Failed).as_secs(), 60);
    }

    #[test]
    fn test_requeue_duration_scale_up_phases() {
        assert_eq!(
            requeue_duration(ClusterPhase::ScalingUpStatefulSet).as_secs(),
            2
        );
        assert_eq!(
            requeue_duration(ClusterPhase::WaitingForNewPods).as_secs(),
            2
        );
        assert_eq!(
            requeue_duration(ClusterPhase::AddingNodesToCluster).as_secs(),
            2
        );
        assert_eq!(
            requeue_duration(ClusterPhase::ConfiguringNewReplicas).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_scale_down_phases() {
        assert_eq!(
            requeue_duration(ClusterPhase::RemovingNodesFromCluster).as_secs(),
            2
        );
        assert_eq!(
            requeue_duration(ClusterPhase::ScalingDownStatefulSet).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_verifying_cluster_health_phase() {
        assert_eq!(
            requeue_duration(ClusterPhase::VerifyingClusterHealth).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_slot_phases() {
        assert_eq!(
            requeue_duration(ClusterPhase::RebalancingSlots).as_secs(),
            5
        );
        assert_eq!(requeue_duration(ClusterPhase::EvacuatingSlots).as_secs(), 5);
    }

    #[test]
    fn test_requeue_duration_degraded_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Degraded).as_secs(), 10);
    }

    #[test]
    fn test_requeue_duration_pending_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Pending).as_secs(), 5);
    }

    #[test]
    fn test_requeue_duration_deleting_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Deleting).as_secs(), 5);
    }

    // ==========================================================================
    // ClusterHealthStatus tests
    // ==========================================================================

    #[test]
    fn test_cluster_health_status_default_values() {
        let status = ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 0,
            healthy_replicas: 0,
            slots_assigned: 0,
            topology: None,
        };
        assert!(!status.is_healthy);
        assert_eq!(status.healthy_masters, 0);
        assert_eq!(status.slots_assigned, 0);
    }

    #[test]
    fn test_cluster_health_status_healthy_cluster() {
        let status = ClusterHealthStatus {
            is_healthy: true,
            healthy_masters: 3,
            healthy_replicas: 3,
            slots_assigned: 16384,
            topology: None,
        };
        assert!(status.is_healthy);
        assert_eq!(status.healthy_masters, 3);
        assert_eq!(status.slots_assigned, 16384);
    }

    // ==========================================================================
    // Constants tests
    // ==========================================================================

    #[test]
    fn test_field_manager_constant() {
        assert_eq!(FIELD_MANAGER, "valkey-operator");
    }

    #[test]
    fn test_finalizer_constant() {
        assert_eq!(FINALIZER, "valkey-operator.smoketurner.com/finalizer");
        assert!(FINALIZER.contains("valkey-operator"));
    }

    // ==========================================================================
    // Upgrade protection tests
    // ==========================================================================

    /// Helper to create a cluster with upgrade annotation
    fn create_cluster_with_upgrade(name: &str, upgrade_name: &str) -> ValkeyCluster {
        let mut cluster = create_test_cluster(name, 3, 1);
        let mut annotations = BTreeMap::new();
        annotations.insert(
            UPGRADE_IN_PROGRESS_ANNOTATION.to_string(),
            "true".to_string(),
        );
        annotations.insert(
            UPGRADE_NAME_ANNOTATION.to_string(),
            upgrade_name.to_string(),
        );
        cluster.metadata.annotations = Some(annotations);
        cluster
    }

    #[test]
    fn test_is_upgrade_in_progress_no_annotations() {
        let cluster = create_test_cluster("test", 3, 1);
        assert!(!is_upgrade_in_progress(&cluster));
    }

    #[test]
    fn test_is_upgrade_in_progress_empty_annotations() {
        let mut cluster = create_test_cluster("test", 3, 1);
        cluster.metadata.annotations = Some(BTreeMap::new());
        assert!(!is_upgrade_in_progress(&cluster));
    }

    #[test]
    fn test_is_upgrade_in_progress_annotation_false() {
        let mut cluster = create_test_cluster("test", 3, 1);
        let mut annotations = BTreeMap::new();
        annotations.insert(
            UPGRADE_IN_PROGRESS_ANNOTATION.to_string(),
            "false".to_string(),
        );
        cluster.metadata.annotations = Some(annotations);
        assert!(!is_upgrade_in_progress(&cluster));
    }

    #[test]
    fn test_is_upgrade_in_progress_annotation_true() {
        let cluster = create_cluster_with_upgrade("test", "my-upgrade");
        assert!(is_upgrade_in_progress(&cluster));
    }

    #[test]
    fn test_upgrade_annotation_constants() {
        assert_eq!(
            UPGRADE_IN_PROGRESS_ANNOTATION,
            "valkey-operator.smoketurner.com/upgrade-in-progress"
        );
        assert_eq!(
            UPGRADE_NAME_ANNOTATION,
            "valkey-operator.smoketurner.com/upgrade-name"
        );
    }

    #[test]
    fn test_is_upgrade_in_progress_preserves_upgrade_name() {
        let cluster = create_cluster_with_upgrade("test", "production-upgrade-v2");
        assert!(is_upgrade_in_progress(&cluster));

        // Verify the upgrade name is accessible
        let upgrade_name = cluster
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(UPGRADE_NAME_ANNOTATION))
            .cloned();
        assert_eq!(upgrade_name, Some("production-upgrade-v2".to_string()));
    }
}
