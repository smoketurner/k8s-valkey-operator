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
        cluster_state_machine::ClusterStateMachine,
        common::{add_finalizer, extract_pod_name, remove_finalizer},
        context::Context,
        error::Error,
        operation_coordination::{self, OperationType},
    },
    crd::{ClusterPhase, Condition, ValkeyCluster, ValkeyClusterStatus, total_pods},
    resources::{certificate, common, pdb, services, statefulset},
};

/// Field manager name for server-side apply
pub const FIELD_MANAGER: &str = "valkey-operator";

/// Finalizer name for graceful deletion
pub const FINALIZER: &str = "valkey-operator.smoketurner.com/finalizer";

/// Annotation key indicating an upgrade is in progress.
const UPGRADE_IN_PROGRESS_ANNOTATION: &str = "valkey-operator.smoketurner.com/upgrade-in-progress";

/// Check if an upgrade is in progress for this cluster.
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
        // Active phases: quick requeue to catch state changes
        ClusterPhase::Creating
        | ClusterPhase::Initializing
        | ClusterPhase::AssigningSlots
        | ClusterPhase::DetectingChanges
        | ClusterPhase::ScalingStatefulSet
        | ClusterPhase::AddingNodes
        | ClusterPhase::RemovingNodes
        | ClusterPhase::VerifyingCluster => std::time::Duration::from_secs(2),
        // Slot migration: moderate interval (migrations take time)
        ClusterPhase::MigratingSlots => std::time::Duration::from_secs(5),
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
        ClusterPhase::Creating
            | ClusterPhase::Initializing
            | ClusterPhase::AssigningSlots
            | ClusterPhase::DetectingChanges
            | ClusterPhase::ScalingStatefulSet
            | ClusterPhase::AddingNodes
            | ClusterPhase::MigratingSlots
            | ClusterPhase::RemovingNodes
            | ClusterPhase::VerifyingCluster
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
            // Create owned resources
            create_owned_resources(&obj, &ctx, &namespace).await?;

            // Check if pods are running (not Ready - readiness requires cluster to be OK)
            // We need running pods to start cluster initialization
            let running_pods = check_running_pods(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
            if running_pods >= desired_replicas {
                ctx.publish_normal_event(
                    &obj,
                    "PodsRunning",
                    "Reconciling",
                    Some(format!(
                        "All pods running ({}/{}), initializing cluster",
                        running_pods, desired_replicas
                    )),
                )
                .await;
                // Transition to Initializing for CLUSTER MEET operations
                ClusterPhase::Initializing
            } else {
                debug!(
                    name = %name,
                    running = running_pods,
                    desired = desired_replicas,
                    "Waiting for pods to be running"
                );
                ClusterPhase::Creating
            }
        }
        ClusterPhase::Running => {
            // Check if update needed
            if spec_changed {
                // Log if spec changed while upgrade is in progress
                // Note: Spec changes during upgrade are expected when upgrade reconciler
                // updates the image tag. This is informational logging only.
                if is_upgrade_in_progress(&obj) {
                    info!(
                        name = %name,
                        "Spec changed while upgrade is in progress - processing changes initiated by upgrade reconciler"
                    );
                }

                ctx.publish_normal_event(
                    &obj,
                    "SpecChanged",
                    "DetectingChanges",
                    Some("Resource spec changed, detecting required changes".to_string()),
                )
                .await;
                ClusterPhase::DetectingChanges
            } else {
                // Ensure resources are in sync
                create_owned_resources(&obj, &ctx, &namespace).await?;

                let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
                let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
                if ready_replicas < desired_replicas {
                    ctx.publish_warning_event(
                        &obj,
                        "Degraded",
                        "Reconciling",
                        Some(format!(
                            "Resource degraded: {}/{} replicas ready",
                            ready_replicas, desired_replicas
                        )),
                    )
                    .await;
                    ClusterPhase::Degraded
                } else {
                    ClusterPhase::Running
                }
            }
        }
        ClusterPhase::Degraded => {
            // Ensure resources are in sync (may have been modified externally)
            // This is important for recovery when StatefulSet replicas were manually reduced
            create_owned_resources(&obj, &ctx, &namespace).await?;

            // Check if recovered
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            let running_pods = check_running_pods(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

            if ready_replicas >= desired_replicas {
                ctx.publish_normal_event(
                    &obj,
                    "Recovered",
                    "Reconciling",
                    Some(format!(
                        "Resource recovered: {}/{} replicas ready",
                        ready_replicas, desired_replicas
                    )),
                )
                .await;
                ClusterPhase::Running
            } else if ready_replicas == 0 && running_pods == 0 {
                ctx.publish_warning_event(
                    &obj,
                    "Failed",
                    "Reconciling",
                    Some("Resource failed: no replicas available".to_string()),
                )
                .await;
                ClusterPhase::Failed
            } else if running_pods >= desired_replicas && ready_replicas < desired_replicas {
                // Pods are running but not ready - likely cluster health issue
                // Try to heal the cluster by running CLUSTER MEET to re-integrate nodes
                debug!(
                    name = %name,
                    running_pods = running_pods,
                    ready_replicas = ready_replicas,
                    "Pods running but not ready, attempting cluster healing"
                );

                // Execute CLUSTER MEET to re-integrate any orphan nodes
                if let Err(e) = execute_cluster_init(&obj, &ctx, &namespace).await {
                    debug!(
                        name = %name,
                        error = %e,
                        "Cluster healing failed, will retry"
                    );
                }

                ClusterPhase::Degraded
            } else {
                ClusterPhase::Degraded
            }
        }
        ClusterPhase::Initializing => {
            // Cluster nodes are up, performing CLUSTER MEET
            // Ensure resources are still in sync
            create_owned_resources(&obj, &ctx, &namespace).await?;

            // Check running pods (not Ready - readiness requires cluster to be OK)
            let running_pods = check_running_pods(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

            if running_pods < desired_replicas {
                // Pods not running yet, wait
                debug!(
                    name = %name,
                    running = running_pods,
                    desired = desired_replicas,
                    "Waiting for pods to be running before CLUSTER MEET"
                );
                ClusterPhase::Initializing
            } else {
                // Execute CLUSTER MEET to connect all nodes
                info!(name = %name, "Executing CLUSTER MEET to connect all nodes");

                // Acquire operation lock for initialization
                if let Err(e) = operation_coordination::start_operation(
                    &api,
                    &name,
                    OperationType::Initializing,
                )
                .await
                {
                    warn!(name = %name, error = %e, "Failed to acquire operation lock for initialization");
                    // If lock acquisition fails, stay in Initializing to retry
                    return Ok(Action::requeue(std::time::Duration::from_secs(10)));
                }

                let init_result = execute_cluster_init(&obj, &ctx, &namespace).await;

                // Release operation lock if initialization fails (success releases in next phase)
                if init_result.is_err() {
                    let _ = operation_coordination::complete_operation(
                        &api,
                        &name,
                        OperationType::Initializing,
                    )
                    .await;
                }

                match init_result {
                    Ok(()) => {
                        ctx.publish_normal_event(
                            &obj,
                            "ClusterMeet",
                            "Initializing",
                            Some("Cluster nodes connected via CLUSTER MEET".to_string()),
                        )
                        .await;
                        ClusterPhase::AssigningSlots
                    }
                    Err(e) => {
                        warn!(name = %name, error = %e, "CLUSTER MEET failed, will retry");
                        ctx.publish_warning_event(
                            &obj,
                            "ClusterMeetFailed",
                            "Initializing",
                            Some(format!("CLUSTER MEET failed: {}", e)),
                        )
                        .await;
                        // Stay in Initializing and retry
                        ClusterPhase::Initializing
                    }
                }
            }
        }
        ClusterPhase::AssigningSlots => {
            // Hash slots are being assigned to masters
            info!(name = %name, "Executing CLUSTER ADDSLOTS and setting up replicas");

            // Ensure operation lock is still held (should be from Initializing phase)
            // If not, acquire it (may have been lost)
            if obj
                .status
                .as_ref()
                .and_then(|s| s.current_operation.as_ref())
                .map(|op| op != "initializing")
                .unwrap_or(true)
                && let Err(e) = operation_coordination::start_operation(
                    &api,
                    &name,
                    OperationType::Initializing,
                )
                .await
            {
                warn!(name = %name, error = %e, "Failed to acquire operation lock for slot assignment");
                return Ok(Action::requeue(std::time::Duration::from_secs(10)));
            }

            match execute_slot_assignment(&obj, &ctx, &namespace).await {
                Ok(()) => {
                    // Release operation lock when initialization completes
                    let _ = operation_coordination::complete_operation(
                        &api,
                        &name,
                        OperationType::Initializing,
                    )
                    .await;

                    // Verify cluster is healthy before transitioning to Running
                    // The cluster needs time to propagate slot information to all nodes
                    match check_cluster_health(&obj, &ctx, &namespace).await {
                        Ok(health) if health.is_healthy => {
                            ctx.publish_normal_event(
                                &obj,
                                "SlotsAssigned",
                                "Initializing",
                                Some("All 16384 hash slots assigned, cluster ready".to_string()),
                            )
                            .await;
                            ClusterPhase::Running
                        }
                        Ok(health) => {
                            // Slots assigned but cluster not yet healthy - wait for propagation
                            debug!(
                                name = %name,
                                slots_assigned = health.slots_assigned,
                                healthy_masters = health.healthy_masters,
                                "Slots assigned, waiting for cluster to stabilize"
                            );
                            // Stay in AssigningSlots to wait for cluster_state:ok
                            ClusterPhase::AssigningSlots
                        }
                        Err(e) => {
                            // Health check failed - wait and retry
                            debug!(
                                name = %name,
                                error = %e,
                                "Health check failed after slot assignment, waiting"
                            );
                            ClusterPhase::AssigningSlots
                        }
                    }
                }
                Err(e) => {
                    warn!(name = %name, error = %e, "Slot assignment failed, will retry");
                    ctx.publish_warning_event(
                        &obj,
                        "SlotAssignmentFailed",
                        "Initializing",
                        Some(format!("Slot assignment failed: {}", e)),
                    )
                    .await;
                    // Stay in AssigningSlots and retry
                    ClusterPhase::AssigningSlots
                }
            }
        }
        // Granular phases for scaling operations
        // These provide better observability and resumability
        ClusterPhase::DetectingChanges => {
            // Compute what changes are needed and store in pending_changes
            handle_detecting_changes(&obj, &ctx, &api, &namespace).await?
        }
        ClusterPhase::ScalingStatefulSet => {
            // Update StatefulSet replica count
            handle_scaling_statefulset(&obj, &ctx, &api, &namespace).await?
        }
        ClusterPhase::AddingNodes => {
            // Execute CLUSTER MEET for new nodes
            handle_adding_nodes(&obj, &ctx, &api, &namespace).await?
        }
        ClusterPhase::MigratingSlots => {
            // Migrate slots for rebalancing
            handle_migrating_slots(&obj, &ctx, &api, &namespace).await?
        }
        ClusterPhase::RemovingNodes => {
            // Execute CLUSTER FORGET for removed nodes
            handle_removing_nodes(&obj, &ctx, &api, &namespace).await?
        }
        ClusterPhase::VerifyingCluster => {
            // Final health check before transitioning to Running
            handle_verifying_cluster(&obj, &ctx, &api, &namespace).await?
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
    let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace)
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
        ready_replicas,
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
            i64::from(ready_replicas),
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

/// Check if the cluster image differs from the current StatefulSet image.
///
/// Returns `true` if the desired image differs from what's currently in the StatefulSet,
/// indicating that a rolling update is needed.
async fn check_image_changed(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    let name = obj.name_any();
    let sts_api: Api<k8s_openapi::api::apps::v1::StatefulSet> =
        Api::namespaced(ctx.client.clone(), namespace);

    // Get current StatefulSet
    let sts = match sts_api.get(&name).await {
        Ok(sts) => sts,
        Err(kube::Error::Api(e)) if e.code == 404 => {
            // StatefulSet doesn't exist yet, not an image change scenario
            return Ok(false);
        }
        Err(e) => return Err(Error::Kube(e)),
    };

    // Extract current image from StatefulSet (first container is valkey)
    let current_image = sts
        .spec
        .as_ref()
        .and_then(|s| s.template.spec.as_ref())
        .and_then(|s| s.containers.first())
        .and_then(|c| c.image.as_ref())
        .cloned()
        .unwrap_or_default();

    // Build desired image from cluster spec
    let desired_image = format!("{}:{}", obj.spec.image.repository, obj.spec.image.tag);

    let changed = current_image != desired_image;

    if changed {
        debug!(
            name = %name,
            current_image = %current_image,
            desired_image = %desired_image,
            "Image change detected"
        );
    }

    Ok(changed)
}

/// Execute cluster initialization (CLUSTER MEET)
async fn execute_cluster_init(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Execute cluster meet
    cluster_init::execute_cluster_meet(
        &ctx.client,
        obj,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(())
}

/// Execute slot assignment (CLUSTER ADDSLOTS)
async fn execute_slot_assignment(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Assign slots to masters
    cluster_init::assign_slots_to_masters(obj, password.as_deref(), tls_certs.as_ref(), &strategy)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Set up replicas
    if obj.spec.replicas_per_master > 0 {
        cluster_init::setup_replicas(obj, password.as_deref(), tls_certs.as_ref(), &strategy)
            .await
            .map_err(|e| Error::Valkey(e.to_string()))?;
    }

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
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    // Execute recovery via CLUSTER MEET
    cluster_init::recover_stale_ips(
        &ctx.client,
        obj,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(())
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
        debug!(
            cluster = %obj.name_any(),
            message = %health_msg,
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

    // Preserve new status fields if present
    let pending_changes = existing_status.and_then(|s| s.pending_changes.clone());
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
        pending_changes,
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
        // No scaling needed
        let _ = client.close().await;
        return Ok(crate::client::ScalingResult::default());
    };

    result.map_err(|e| Error::Valkey(format!("Scaling operation failed: {}", e)))
}

/// Execute a scale-down operation with proper per-node connections.
///
/// CLUSTER MIGRATESLOTS must be executed on the source node (the node that currently
/// owns the slots). This function connects to each node being removed and executes
/// the migration commands from there.
async fn execute_scale_down(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    cluster_nodes: &crate::client::ParsedClusterNodes,
    scaling_ctx: &ScalingContext,
) -> Result<crate::client::ScalingResult, crate::client::ValkeyError> {
    use crate::client::ValkeyError;
    use std::collections::HashMap;

    let mut result = crate::client::ScalingResult::default();
    let masters: Vec<_> = cluster_nodes.masters().into_iter().collect();

    // Find masters to remove (highest ordinals)
    let ordinals_to_remove = scaling_ctx.masters_to_remove();

    // Get pod IPs to build a mapping from IP -> ordinal
    // CLUSTER NODES returns IP addresses, not DNS names, so we need this mapping
    let cluster_name = obj.name_any();
    let current_total =
        crate::crd::total_pods(scaling_ctx.current_masters, obj.spec.replicas_per_master);

    let pod_ips =
        cluster_init::get_pod_ips(&ctx.client, namespace, &cluster_name, current_total).await?;

    // Build IP -> ordinal mapping
    let ip_to_ordinal: HashMap<&str, i32> = pod_ips
        .iter()
        .filter_map(|(pod_name, ip, _)| {
            // Extract ordinal from pod name (e.g., "scale-down-test-3" -> 3)
            let ordinal = pod_name.rsplit('-').next()?.parse().ok()?;
            Some((ip.as_str(), ordinal))
        })
        .collect();

    // Find nodes to remove by matching their IPs to ordinals
    let nodes_to_remove: Vec<_> = masters
        .iter()
        .filter(|m| {
            if let Some(&ordinal) = ip_to_ordinal.get(m.ip.as_str()) {
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

        // Look up the ordinal for this node's IP
        let Some(&ordinal) = ip_to_ordinal.get(node.ip.as_str()) else {
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
                    // Check if target node now owns the slot range
                    let owns_range = target.slots.iter().any(|range| {
                        range.start <= start_slot as i32 && range.end >= end_slot as i32
                    });

                    if owns_range {
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

    // Get pod IPs for new nodes
    let pod_ips = cluster_init::get_pod_ips(&ctx.client, namespace, &name, desired_total)
        .await
        .map_err(|e| Error::Valkey(format!("Failed to get pod IPs: {}", e)))?;

    // Filter to only pods not yet in cluster
    let existing_ips: std::collections::HashSet<_> = cluster_nodes
        .nodes
        .iter()
        .map(|n| n.address.split(':').next().unwrap_or(""))
        .collect();

    let new_pods: Vec<_> = pod_ips
        .iter()
        .filter(|(_, ip, _)| !existing_ips.contains(ip.as_str()))
        .collect();

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
    for (pod_name, ip, port) in &new_pods {
        info!(pod = %pod_name, ip = %ip, "Executing CLUSTER MEET");
        client
            .cluster_meet(ip, *port)
            .await
            .map_err(|e| Error::Valkey(format!("CLUSTER MEET failed for {}: {}", pod_name, e)))?;
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

    // Build a mapping of IP to node ID
    let ip_to_node_id: std::collections::HashMap<String, String> = cluster_nodes
        .nodes
        .iter()
        .map(|n| (n.ip.clone(), n.node_id.clone()))
        .collect();

    // Get master node IDs (from pod-0 to pod-(masters-1))
    let master_ips: Vec<String> = pod_ips
        .iter()
        .take(masters as usize)
        .map(|(_, ip, _)| ip.clone())
        .collect();

    let master_node_ids: Vec<String> = master_ips
        .iter()
        .filter_map(|ip| ip_to_node_id.get(ip).cloned())
        .collect();

    // For each new replica, configure it
    let mut added = 0;
    for (pod_name, ip, _) in &new_pods {
        // Extract pod ordinal from name (e.g., "test-cluster-3" -> 3)
        let ordinal = pod_name
            .rsplit('-')
            .next()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        // Skip if this is a master pod
        if ordinal < masters {
            continue;
        }

        // Determine which master this replica belongs to
        let master_index = ((ordinal - masters) % masters) as usize;
        let Some(master_node_id) = master_node_ids.get(master_index) else {
            warn!(pod = %pod_name, "Cannot find master for replica");
            continue;
        };

        // Get the node ID for this replica
        let replica_node_id = match ip_to_node_id.get(ip.as_str()) {
            Some(id) => id,
            None => {
                warn!(pod = %pod_name, ip = %ip, "Cannot find node ID for replica");
                continue;
            }
        };

        info!(
            pod = %pod_name,
            replica_node = %replica_node_id,
            master_node = %master_node_id,
            master_index = master_index,
            "Configuring replica"
        );

        let replica_client = ctx
            .connect_to_cluster(obj, namespace, ordinal)
            .await
            .map_err(|e| Error::Valkey(format!("Failed to connect to replica: {}", e)))?;

        match replica_client.cluster_replicate(master_node_id).await {
            Ok(()) => {
                info!(pod = %pod_name, "Successfully configured as replica");
                added += 1;
            }
            Err(e) => {
                warn!(pod = %pod_name, error = %e, "Failed to configure replica");
            }
        }

        let _ = replica_client.close().await;
    }

    Ok(added)
}

// ============================================================================
// Phase Handlers - Granular scaling operation phases
// ============================================================================

/// Handle DetectingChanges phase.
///
/// Computes what changes are needed (scale direction, nodes to add/remove)
/// and stores them in `pending_changes` status field.
///
/// Annotation key storing the name of the active upgrade.
const UPGRADE_NAME_ANNOTATION: &str = "valkey-operator.smoketurner.com/upgrade-name";

async fn handle_detecting_changes(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    namespace: &str,
) -> Result<ClusterPhase, Error> {
    let name = obj.name_any();
    info!(name = %name, "Detecting changes for cluster update");

    // Get current cluster state
    let current_masters = get_current_master_count(obj, ctx, namespace).await?;
    let target_masters = obj.spec.masters;
    let target_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

    // Get running pods to compare
    let running_pods = check_running_pods(obj, ctx, namespace).await?;

    // Determine scale direction
    let scale_direction = if current_masters > target_masters && current_masters > 0 {
        "down"
    } else if current_masters < target_masters && current_masters > 0 {
        "up"
    } else if running_pods != target_replicas {
        "replica_change"
    } else {
        "none"
    };

    // Check if image changed by comparing with current StatefulSet
    let image_changed = check_image_changed(obj, ctx, namespace).await?;

    info!(
        name = %name,
        current_masters = current_masters,
        target_masters = target_masters,
        running_pods = running_pods,
        target_replicas = target_replicas,
        scale_direction = scale_direction,
        image_changed = image_changed,
        "Change detection complete"
    );

    // Defense in depth: Block non-image changes during an active upgrade
    // The webhook should have blocked this, but we check here as well
    if is_upgrade_in_progress(obj) && scale_direction != "none" {
        let upgrade_name = obj
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(UPGRADE_NAME_ANNOTATION))
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let error_msg = format!(
            "Cannot {} cluster while upgrade '{}' is in progress. \
             Wait for the upgrade to complete or roll it back first. \
             Check status: kubectl get valkeyupgrade {} -n {}",
            scale_direction, upgrade_name, upgrade_name, namespace
        );

        warn!(name = %name, upgrade = %upgrade_name, "Blocking scale operation during upgrade");

        // Update status with error
        let mut status = obj.status.clone().unwrap_or_default();
        status.last_error = Some(error_msg.clone());
        status.message = format!("Blocked: {}", error_msg);

        let patch = serde_json::json!({ "status": status });
        let _ = api
            .patch_status(
                &name,
                &kube::api::PatchParams::apply(FIELD_MANAGER),
                &kube::api::Patch::Merge(&patch),
            )
            .await;

        ctx.publish_warning_event(
            obj,
            "UpgradeInProgress",
            "DetectingChanges",
            Some(error_msg),
        )
        .await;

        // Return to Running phase - the spec change is effectively ignored
        return Ok(ClusterPhase::Running);
    }

    // Compute nodes to add/remove
    let (nodes_to_add, nodes_to_remove) = if scale_direction == "up" {
        let new_masters: Vec<String> = (current_masters..target_masters)
            .map(|i| format!("{}-{}", name, i))
            .collect();
        (new_masters, Vec::new())
    } else if scale_direction == "down" {
        let removed_masters: Vec<String> = (target_masters..current_masters)
            .map(|i| format!("{}-{}", name, i))
            .collect();
        (Vec::new(), removed_masters)
    } else {
        (Vec::new(), Vec::new())
    };

    // Create PendingChanges
    let pending_changes = crate::crd::PendingChanges {
        scale_direction: scale_direction.to_string(),
        previous_masters: current_masters,
        target_masters,
        nodes_to_add: nodes_to_add.clone(),
        nodes_to_remove: nodes_to_remove.clone(),
        slot_migrations: Vec::new(), // Will be populated during MigratingSlots
        requires_rolling_update: image_changed,
        for_generation: obj.metadata.generation.unwrap_or(0),
    };

    // Update status with pending changes
    let mut status = obj.status.clone().unwrap_or_default();
    status.pending_changes = Some(pending_changes);
    status.message = if image_changed && scale_direction == "none" {
        "Detected image change".to_string()
    } else if image_changed {
        format!("Detected {} operation with image change", scale_direction)
    } else {
        format!("Detected {} operation", scale_direction)
    };

    // Update status via patch
    let patch = serde_json::json!({
        "status": status
    });
    api.patch_status(
        &name,
        &kube::api::PatchParams::apply(FIELD_MANAGER),
        &kube::api::Patch::Merge(&patch),
    )
    .await?;

    // Check if operation is allowed
    if scale_direction != "none"
        && let Err(e) =
            operation_coordination::check_operation_allowed(api, &name, OperationType::Scaling)
                .await
    {
        warn!(name = %name, error = %e, "Cannot start scaling operation due to conflict");
        ctx.publish_warning_event(
            obj,
            "ScalingBlocked",
            "DetectingChanges",
            Some(e.to_string()),
        )
        .await;
        // Stay in DetectingChanges to retry
        return Ok(ClusterPhase::DetectingChanges);
    }

    ctx.publish_normal_event(
        obj,
        "ChangesDetected",
        "DetectingChanges",
        Some(format!(
            "Detected {} operation: {} -> {} masters",
            scale_direction, current_masters, target_masters
        )),
    )
    .await;

    // Determine next phase based on scale direction and image changes
    match scale_direction {
        "down" => {
            // Scale-down: must migrate slots BEFORE removing pods
            Ok(ClusterPhase::MigratingSlots)
        }
        "up" | "replica_change" => {
            // Scale-up or replica change: update StatefulSet first
            Ok(ClusterPhase::ScalingStatefulSet)
        }
        _ if image_changed => {
            // Image changed but no scaling needed - still need to update StatefulSet
            info!(
                name = %name,
                "Image change detected, transitioning to ScalingStatefulSet to apply update"
            );
            ctx.publish_normal_event(
                obj,
                "ImageChangeDetected",
                "DetectingChanges",
                Some("Image change detected, updating StatefulSet".to_string()),
            )
            .await;
            Ok(ClusterPhase::ScalingStatefulSet)
        }
        _ => {
            // No changes needed
            Ok(ClusterPhase::Running)
        }
    }
}

/// Handle ScalingStatefulSet phase.
///
/// Updates the StatefulSet replica count and waits for pods to be ready.
async fn handle_scaling_statefulset(
    obj: &ValkeyCluster,
    ctx: &Context,
    _api: &Api<ValkeyCluster>,
    namespace: &str,
) -> Result<ClusterPhase, Error> {
    let name = obj.name_any();
    info!(name = %name, "Scaling StatefulSet");

    // Apply StatefulSet changes
    create_owned_resources(obj, ctx, namespace).await?;

    // Get pending changes to determine what we're doing
    let pending_changes = obj.status.as_ref().and_then(|s| s.pending_changes.as_ref());

    let scale_direction = pending_changes
        .map(|p| p.scale_direction.as_str())
        .unwrap_or("none");
    let requires_rolling_update = pending_changes
        .map(|p| p.requires_rolling_update)
        .unwrap_or(false);

    // Check running pods
    let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
    let running_pods = check_running_pods(obj, ctx, namespace).await?;

    // Handle image-only change (no scaling, just rolling update)
    if scale_direction == "none" && requires_rolling_update {
        info!(
            name = %name,
            "Image change applied to StatefulSet, transitioning to VerifyingCluster"
        );
        ctx.publish_normal_event(
            obj,
            "StatefulSetUpdated",
            "ScalingStatefulSet",
            Some("StatefulSet template updated with new image".to_string()),
        )
        .await;
        // Go to VerifyingCluster to check cluster health
        // Kubernetes will handle the rolling update automatically
        return Ok(ClusterPhase::VerifyingCluster);
    }

    if scale_direction == "down" {
        // For scale-down, we're reducing replicas AFTER slot migration
        // StatefulSet should already be updated, just verify
        if running_pods <= desired_replicas {
            info!(
                name = %name,
                running = running_pods,
                desired = desired_replicas,
                "StatefulSet scaled down, verifying cluster"
            );
            ctx.publish_normal_event(
                obj,
                "StatefulSetScaled",
                "ScalingStatefulSet",
                Some(format!("StatefulSet scaled to {} pods", desired_replicas)),
            )
            .await;
            return Ok(ClusterPhase::VerifyingCluster);
        } else {
            // Still waiting for pods to terminate
            debug!(
                name = %name,
                running = running_pods,
                desired = desired_replicas,
                "Waiting for StatefulSet to scale down"
            );
            return Ok(ClusterPhase::ScalingStatefulSet);
        }
    }

    // For scale-up, wait for all pods to be running
    if running_pods < desired_replicas {
        debug!(
            name = %name,
            running = running_pods,
            desired = desired_replicas,
            "Waiting for pods to start"
        );
        return Ok(ClusterPhase::ScalingStatefulSet);
    }

    // All pods running, move to adding nodes
    info!(
        name = %name,
        running = running_pods,
        "All pods running, proceeding to add nodes to cluster"
    );

    ctx.publish_normal_event(
        obj,
        "PodsReady",
        "ScalingStatefulSet",
        Some(format!("{} pods ready", running_pods)),
    )
    .await;

    Ok(ClusterPhase::AddingNodes)
}

/// Handle AddingNodes phase.
///
/// Executes CLUSTER MEET for new nodes to join the cluster.
async fn handle_adding_nodes(
    obj: &ValkeyCluster,
    ctx: &Context,
    _api: &Api<ValkeyCluster>,
    namespace: &str,
) -> Result<ClusterPhase, Error> {
    let name = obj.name_any();
    info!(name = %name, "Adding new nodes to cluster");

    // Get pending changes to see what nodes to add
    let pending_changes = obj.status.as_ref().and_then(|s| s.pending_changes.as_ref());

    let nodes_to_add = pending_changes
        .map(|p| p.nodes_to_add.clone())
        .unwrap_or_default();

    if nodes_to_add.is_empty() {
        // No new master nodes, but might have new replicas
        match add_new_replicas_to_cluster(obj, ctx, namespace).await {
            Ok(added) if added > 0 => {
                info!(name = %name, added = added, "Added new replicas to cluster");
                ctx.publish_normal_event(
                    obj,
                    "ReplicasAdded",
                    "AddingNodes",
                    Some(format!("Added {} new replicas", added)),
                )
                .await;
            }
            Ok(_) => {
                debug!(name = %name, "No new replicas to add");
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Failed to add replicas, will retry");
                return Ok(ClusterPhase::AddingNodes);
            }
        }
        // No scaling needed, verify cluster
        return Ok(ClusterPhase::VerifyingCluster);
    }

    // Check how many nodes are in the cluster
    let nodes_in_cluster = get_nodes_in_cluster_count(obj, ctx, namespace).await?;
    let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

    if nodes_in_cluster >= desired_replicas {
        // All nodes already in cluster
        info!(
            name = %name,
            nodes = nodes_in_cluster,
            "All nodes in cluster, proceeding to slot migration"
        );
        ctx.publish_normal_event(
            obj,
            "NodesAdded",
            "AddingNodes",
            Some(format!("{} nodes in cluster", nodes_in_cluster)),
        )
        .await;
        return Ok(ClusterPhase::MigratingSlots);
    }

    // Add new replicas (which includes CLUSTER MEET for new masters)
    match add_new_replicas_to_cluster(obj, ctx, namespace).await {
        Ok(added) => {
            info!(name = %name, added = added, "Added nodes to cluster");
            ctx.publish_normal_event(
                obj,
                "NodesAdded",
                "AddingNodes",
                Some(format!("Added {} nodes to cluster", added)),
            )
            .await;
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Failed to add nodes, will retry");
            ctx.publish_warning_event(
                obj,
                "NodeAddFailed",
                "AddingNodes",
                Some(format!("Failed to add nodes: {}", e)),
            )
            .await;
            return Ok(ClusterPhase::AddingNodes);
        }
    }

    // Check again if all nodes are now in cluster
    let nodes_in_cluster = get_nodes_in_cluster_count(obj, ctx, namespace).await?;
    if nodes_in_cluster >= desired_replicas {
        Ok(ClusterPhase::MigratingSlots)
    } else {
        // Still more nodes to add
        Ok(ClusterPhase::AddingNodes)
    }
}

/// Handle MigratingSlots phase.
///
/// Migrates slots for rebalancing during scale operations.
async fn handle_migrating_slots(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    namespace: &str,
) -> Result<ClusterPhase, Error> {
    let name = obj.name_any();
    info!(name = %name, "Migrating slots");

    // Acquire operation lock for scaling
    if let Err(e) =
        operation_coordination::start_operation(api, &name, OperationType::Scaling).await
    {
        warn!(name = %name, error = %e, "Failed to acquire operation lock");
        return Ok(ClusterPhase::MigratingSlots);
    }

    // Execute the scaling operation (which handles slot migration)
    let scaling_result = execute_scaling_operation(obj, ctx, namespace).await;

    // Release operation lock
    let _ = operation_coordination::complete_operation(api, &name, OperationType::Scaling).await;

    match scaling_result {
        Ok(result) => {
            if result.success {
                info!(
                    name = %name,
                    slots_moved = result.slots_moved,
                    nodes_added = result.nodes_added.len(),
                    nodes_removed = result.nodes_removed.len(),
                    "Slot migration completed"
                );

                ctx.publish_normal_event(
                    obj,
                    "SlotsMigrated",
                    "MigratingSlots",
                    Some(format!("{} slots migrated", result.slots_moved)),
                )
                .await;

                // Determine next phase based on operation type
                let pending_changes = obj.status.as_ref().and_then(|s| s.pending_changes.as_ref());

                let scale_direction = pending_changes
                    .map(|p| p.scale_direction.as_str())
                    .unwrap_or("none");

                if scale_direction == "down" {
                    // Scale-down: need to remove nodes from cluster
                    Ok(ClusterPhase::RemovingNodes)
                } else {
                    // Scale-up: verify cluster health
                    Ok(ClusterPhase::VerifyingCluster)
                }
            } else {
                warn!(name = %name, error = ?result.error, "Slot migration failed");
                ctx.publish_warning_event(
                    obj,
                    "SlotMigrationFailed",
                    "MigratingSlots",
                    result.error.clone(),
                )
                .await;
                // Stay in MigratingSlots to retry
                Ok(ClusterPhase::MigratingSlots)
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Slot migration error");
            ctx.publish_warning_event(
                obj,
                "SlotMigrationFailed",
                "MigratingSlots",
                Some(format!("Migration failed: {}", e)),
            )
            .await;
            // Stay in MigratingSlots to retry
            Ok(ClusterPhase::MigratingSlots)
        }
    }
}

/// Handle RemovingNodes phase.
///
/// Executes CLUSTER FORGET for nodes being removed during scale-down.
async fn handle_removing_nodes(
    obj: &ValkeyCluster,
    ctx: &Context,
    _api: &Api<ValkeyCluster>,
    _namespace: &str,
) -> Result<ClusterPhase, Error> {
    let name = obj.name_any();
    info!(name = %name, "Removing nodes from cluster");

    // Get pending changes to see what nodes to remove
    let pending_changes = obj.status.as_ref().and_then(|s| s.pending_changes.as_ref());

    let nodes_to_remove = pending_changes
        .map(|p| p.nodes_to_remove.clone())
        .unwrap_or_default();

    if nodes_to_remove.is_empty() {
        info!(name = %name, "No nodes to remove");
        return Ok(ClusterPhase::ScalingStatefulSet);
    }

    // The execute_scaling_operation already handles CLUSTER FORGET
    // So at this point, the nodes should already be forgotten
    // We just need to verify and update the StatefulSet

    info!(
        name = %name,
        nodes = ?nodes_to_remove,
        "Nodes removed from cluster, updating StatefulSet"
    );

    ctx.publish_normal_event(
        obj,
        "NodesRemoved",
        "RemovingNodes",
        Some(format!(
            "Removed {} nodes from cluster",
            nodes_to_remove.len()
        )),
    )
    .await;

    // Proceed to scale down the StatefulSet
    Ok(ClusterPhase::ScalingStatefulSet)
}

/// Detect and clean up orphaned nodes (masters with no slots beyond expected count).
///
/// This handles the case where a node dies during scale-down before CLUSTER FORGET
/// can be executed. The orphaned node will still appear in CLUSTER NODES with fail/pfail
/// flags, causing the health check to fail.
///
/// Returns the list of node IDs that were successfully cleaned up.
async fn detect_and_cleanup_orphans(
    cluster: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<Vec<String>, Error> {
    let expected_masters = cluster.spec.masters;
    let name = cluster.name_any();

    // Get connection context
    let (password, tls_certs, strategy) = ctx.connection_context(cluster, namespace).await?;

    // Connect to get cluster state
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

    let orphans = cluster_init::detect_orphaned_nodes(&client, expected_masters)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    let _ = client.close().await;

    if orphans.is_empty() {
        return Ok(vec![]);
    }

    let orphan_ids: Vec<String> = orphans.iter().map(|n| n.node_id.clone()).collect();

    info!(
        cluster = %name,
        orphan_count = orphans.len(),
        orphan_ids = ?orphan_ids,
        "Detected orphaned nodes, cleaning up via CLUSTER FORGET"
    );

    // Execute CLUSTER FORGET with retry across pods
    let forgotten = cluster_init::forget_nodes_with_retry(
        cluster,
        &orphan_ids,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(forgotten)
}

/// Rebalance replicas across masters to ensure even distribution.
///
/// After scaling operations, replicas may become unevenly distributed. This function
/// detects masters with too many replicas and moves excess replicas to masters with
/// too few replicas.
///
/// Returns the number of replicas moved.
async fn rebalance_replicas(
    cluster: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    let expected_masters = cluster.spec.masters;
    let expected_replicas_per_master = cluster.spec.replicas_per_master;

    if expected_replicas_per_master == 0 {
        return Ok(0);
    }

    // Get connection context
    let (password, tls_certs, strategy) = ctx.connection_context(cluster, namespace).await?;

    // Connect to get cluster state
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

    // Build map of master node_id -> (ordinal, replica_count, replica_node_ids)
    let masters = cluster_nodes.masters();
    let mut master_info: Vec<(String, i32, i32, Vec<String>)> = Vec::new();

    for master in &masters {
        let ordinal = crate::slots::planner::extract_ordinal_from_address(&master.address)
            .map(|o| o as i32)
            .unwrap_or(-1);

        // Only consider masters within expected range
        if ordinal < 0 || ordinal >= expected_masters {
            continue;
        }

        let replicas = cluster_nodes.replicas_of(&master.node_id);
        let replica_ids: Vec<String> = replicas.iter().map(|r| r.node_id.clone()).collect();

        master_info.push((
            master.node_id.clone(),
            ordinal,
            replicas.len() as i32,
            replica_ids,
        ));
    }

    // Sort by ordinal to ensure consistent ordering
    master_info.sort_by_key(|(_, ordinal, _, _)| *ordinal);

    // Find masters with too many and too few replicas
    let mut masters_with_excess: Vec<(String, i32, Vec<String>)> = Vec::new();
    let mut masters_needing_replicas: Vec<(String, i32)> = Vec::new();

    for (node_id, ordinal, replica_count, replica_ids) in master_info {
        if replica_count > expected_replicas_per_master {
            // Calculate excess replicas
            let excess = replica_count - expected_replicas_per_master;
            // Take the last `excess` replicas to move
            let excess_replica_ids: Vec<String> = replica_ids
                .into_iter()
                .rev()
                .take(excess as usize)
                .collect();
            masters_with_excess.push((node_id, ordinal, excess_replica_ids));
        } else if replica_count < expected_replicas_per_master {
            for _ in 0..(expected_replicas_per_master - replica_count) {
                masters_needing_replicas.push((node_id.clone(), ordinal));
            }
        }
    }

    if masters_with_excess.is_empty() || masters_needing_replicas.is_empty() {
        // No rebalancing needed
        return Ok(0);
    }

    info!(
        masters_with_excess = masters_with_excess.len(),
        masters_needing_replicas = masters_needing_replicas.len(),
        "Rebalancing replicas across masters"
    );

    let mut replicas_moved = 0;

    // For each master needing replicas, take one from a master with excess
    for (target_master_id, target_ordinal) in masters_needing_replicas {
        // Find a master with excess replicas
        let Some((source_idx, (_, source_ordinal, excess_replicas))) = masters_with_excess
            .iter_mut()
            .enumerate()
            .find(|(_, (_, _, replicas))| !replicas.is_empty())
        else {
            // No more excess replicas available
            break;
        };

        let Some(replica_to_move) = excess_replicas.pop() else {
            continue;
        };

        info!(
            replica_id = %replica_to_move,
            from_master_ordinal = source_ordinal,
            to_master_ordinal = target_ordinal,
            "Moving replica to rebalance cluster"
        );

        // Find the replica's ordinal to connect to it
        let replica_ordinal = cluster_nodes
            .nodes
            .iter()
            .find(|n| n.node_id == replica_to_move)
            .and_then(|n| crate::slots::planner::extract_ordinal_from_address(&n.address))
            .map(|o| o as i32);

        let Some(replica_ordinal) = replica_ordinal else {
            warn!(
                replica_id = %replica_to_move,
                "Could not find ordinal for replica, skipping"
            );
            continue;
        };

        // Connect to the replica and execute CLUSTER REPLICATE
        let (replica_host, replica_port) = strategy
            .get_connection(replica_ordinal)
            .await
            .map_err(|e| Error::Valkey(e.to_string()))?;

        let replica_client = crate::client::ValkeyClient::connect_single(
            &replica_host,
            replica_port,
            password.as_deref(),
            tls_certs.as_ref(),
        )
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

        match replica_client.cluster_replicate(&target_master_id).await {
            Ok(()) => {
                info!(
                    replica_ordinal = replica_ordinal,
                    target_master_ordinal = target_ordinal,
                    "Replica reassigned to new master"
                );
                replicas_moved += 1;
            }
            Err(e) => {
                warn!(
                    replica_ordinal = replica_ordinal,
                    target_master_ordinal = target_ordinal,
                    error = %e,
                    "Failed to reassign replica"
                );
            }
        }

        let _ = replica_client.close().await;

        // Clean up empty entries
        if excess_replicas.is_empty() {
            masters_with_excess.remove(source_idx);
        }
    }

    if replicas_moved > 0 {
        info!(
            replicas_moved = replicas_moved,
            "Replica rebalancing complete"
        );
    }

    Ok(replicas_moved)
}

/// Set up replicas for new masters if they don't have the expected number of replicas.
///
/// This function detects masters that have fewer replicas than expected and configures
/// the missing replicas. This handles the scale-up case where CLUSTER MEET succeeded
/// for new masters but their replicas weren't configured.
async fn setup_replicas_for_new_masters_if_needed(
    cluster: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    let expected_masters = cluster.spec.masters;
    let expected_replicas_per_master = cluster.spec.replicas_per_master;

    if expected_replicas_per_master == 0 {
        return Ok(0);
    }

    // Get connection context
    let (password, tls_certs, strategy) = ctx.connection_context(cluster, namespace).await?;

    // Connect to get cluster state
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

    // Find masters and count their replicas
    let masters = cluster_nodes.masters();
    let mut new_master_ordinals: Vec<i32> = Vec::new();

    for master in &masters {
        // Extract ordinal from master address
        let ordinal = match crate::slots::planner::extract_ordinal_from_address(&master.address) {
            Some(o) => o as i32,
            None => continue,
        };

        // Only check masters within the expected range
        if ordinal >= expected_masters {
            continue;
        }

        // Count replicas for this master
        let replica_count = cluster_nodes.replicas_of(&master.node_id).len() as i32;

        if replica_count < expected_replicas_per_master {
            debug!(
                master_ordinal = ordinal,
                current_replicas = replica_count,
                expected_replicas = expected_replicas_per_master,
                "Master has fewer replicas than expected"
            );
            new_master_ordinals.push(ordinal);
        }
    }

    if new_master_ordinals.is_empty() {
        return Ok(0);
    }

    info!(
        masters_needing_replicas = ?new_master_ordinals,
        "Setting up replicas for masters with missing replicas"
    );

    // Set up replicas for the masters that need them
    let configured = cluster_init::setup_replicas_for_new_masters(
        cluster,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
        &new_master_ordinals,
    )
    .await
    .map_err(|e| Error::Valkey(e.to_string()))?;

    Ok(configured)
}

/// Handle VerifyingCluster phase.
///
/// Performs final health check before transitioning to Running.
/// Also detects and cleans up orphaned nodes that may remain from failed scale-down operations.
async fn handle_verifying_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    namespace: &str,
) -> Result<ClusterPhase, Error> {
    let name = obj.name_any();
    info!(name = %name, "Verifying cluster health");

    // Step 1: Check for orphaned nodes (masters with no slots beyond expected count)
    // This handles the case where CLUSTER FORGET failed during scale-down
    match detect_and_cleanup_orphans(obj, ctx, namespace).await {
        Ok(forgotten) if !forgotten.is_empty() => {
            info!(
                name = %name,
                orphan_count = forgotten.len(),
                "Cleaned up orphaned nodes, will re-verify"
            );
            ctx.publish_normal_event(
                obj,
                "OrphanedNodesRemoved",
                "Cleanup",
                Some(format!(
                    "Removed {} orphaned nodes from cluster",
                    forgotten.len()
                )),
            )
            .await;

            // Give gossip time to propagate, then re-verify
            return Ok(ClusterPhase::VerifyingCluster);
        }
        Ok(_) => {
            // No orphaned nodes found
        }
        Err(e) => {
            // Log but continue with health check - orphan cleanup is best-effort
            warn!(name = %name, error = %e, "Failed to check for orphaned nodes, continuing");
        }
    }

    // Step 2: Ensure replicas are configured for any new masters (scale-up scenario)
    // This handles the case where CLUSTER MEET succeeded but replicas weren't set up
    if obj.spec.replicas_per_master > 0 {
        match setup_replicas_for_new_masters_if_needed(obj, ctx, namespace).await {
            Ok(configured) if configured > 0 => {
                info!(
                    name = %name,
                    replicas_configured = configured,
                    "Configured replicas for new masters"
                );
                ctx.publish_normal_event(
                    obj,
                    "ReplicasConfigured",
                    "VerifyingCluster",
                    Some(format!(
                        "Configured {} replicas for new masters",
                        configured
                    )),
                )
                .await;
            }
            Ok(_) => {
                // No new replicas needed
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Failed to configure replicas for new masters, continuing");
            }
        }
    }

    // Step 3: Rebalance replicas if needed (after scale cycles)
    // This handles the case where replicas are unevenly distributed
    if obj.spec.replicas_per_master > 0 {
        match rebalance_replicas(obj, ctx, namespace).await {
            Ok(moved) if moved > 0 => {
                info!(
                    name = %name,
                    replicas_moved = moved,
                    "Rebalanced replicas across masters"
                );
                ctx.publish_normal_event(
                    obj,
                    "ReplicasRebalanced",
                    "VerifyingCluster",
                    Some(format!("Rebalanced {} replicas across masters", moved)),
                )
                .await;
                // Give cluster time to stabilize before health check
                return Ok(ClusterPhase::VerifyingCluster);
            }
            Ok(_) => {
                // No rebalancing needed
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Failed to rebalance replicas, continuing");
            }
        }
    }

    // Step 4: Check cluster health
    match check_cluster_health(obj, ctx, namespace).await {
        Ok(health) if health.is_healthy => {
            info!(
                name = %name,
                slots_assigned = health.slots_assigned,
                healthy_masters = health.healthy_masters,
                "Cluster verification passed"
            );

            // Clear pending changes
            let mut status = obj.status.clone().unwrap_or_default();
            status.pending_changes = None;
            status.operation_progress = None;
            status.message = "Cluster healthy".to_string();

            let patch = serde_json::json!({
                "status": status
            });
            let _ = api
                .patch_status(
                    &name,
                    &kube::api::PatchParams::apply(FIELD_MANAGER),
                    &kube::api::Patch::Merge(&patch),
                )
                .await;

            ctx.publish_normal_event(
                obj,
                "ClusterVerified",
                "VerifyingCluster",
                Some("Cluster health verification passed".to_string()),
            )
            .await;

            Ok(ClusterPhase::Running)
        }
        Ok(health) => {
            // Cluster not healthy yet, check if degraded
            warn!(
                name = %name,
                slots_assigned = health.slots_assigned,
                healthy_masters = health.healthy_masters,
                "Cluster not yet healthy, waiting"
            );

            if health.healthy_masters == 0 {
                // No healthy masters - this is a problem
                Ok(ClusterPhase::Degraded)
            } else {
                // Some masters healthy, might still be stabilizing
                Ok(ClusterPhase::VerifyingCluster)
            }
        }
        Err(e) => {
            warn!(name = %name, error = %e, "Health check failed during verification");
            // Might be transient, stay in VerifyingCluster to retry
            Ok(ClusterPhase::VerifyingCluster)
        }
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
    fn test_requeue_duration_initializing_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Initializing).as_secs(), 2);
        assert_eq!(requeue_duration(ClusterPhase::AssigningSlots).as_secs(), 2);
    }

    #[test]
    fn test_requeue_duration_failed_phase() {
        assert_eq!(requeue_duration(ClusterPhase::Failed).as_secs(), 60);
    }

    #[test]
    fn test_requeue_duration_detecting_changes_phase() {
        assert_eq!(
            requeue_duration(ClusterPhase::DetectingChanges).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_scaling_statefulset_phase() {
        assert_eq!(
            requeue_duration(ClusterPhase::ScalingStatefulSet).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_adding_nodes_phase() {
        assert_eq!(requeue_duration(ClusterPhase::AddingNodes).as_secs(), 2);
    }

    #[test]
    fn test_requeue_duration_removing_nodes_phase() {
        assert_eq!(requeue_duration(ClusterPhase::RemovingNodes).as_secs(), 2);
    }

    #[test]
    fn test_requeue_duration_verifying_cluster_phase() {
        assert_eq!(
            requeue_duration(ClusterPhase::VerifyingCluster).as_secs(),
            2
        );
    }

    #[test]
    fn test_requeue_duration_migrating_slots_phase() {
        assert_eq!(requeue_duration(ClusterPhase::MigratingSlots).as_secs(), 5);
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
