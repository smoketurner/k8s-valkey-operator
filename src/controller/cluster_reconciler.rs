//! Reconciliation loop for ValkeyCluster.
//!
//! This module contains the main reconcile function that handles the lifecycle
//! of ValkeyCluster custom resources.

mod deletion;
mod scaling;
mod status;

pub(crate) use deletion::{execute_forget_removed_nodes, handle_deletion};
pub(crate) use status::StatusUpdate;
pub use status::{ClusterHealthStatus, derive_cluster_conditions, merge_conditions};

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
    controller::{
        cluster_init,
        cluster_phases::{self, ClusterHealthResult, PhaseContext},
        cluster_state_machine::ClusterStateMachine,
        cluster_topology::ClusterTopology,
        cluster_validation::validate_spec,
        common::{add_finalizer, extract_pod_name},
        context::Context,
        diagnostic_hints::DiagnosticHint,
        error::Error,
    },
    crd::{ClusterPhase, ValkeyCluster, total_pods},
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
fn requeue_duration(phase: ClusterPhase) -> std::time::Duration {
    match phase {
        ClusterPhase::Running => std::time::Duration::from_secs(30),
        ClusterPhase::Creating
        | ClusterPhase::WaitingForPods
        | ClusterPhase::InitializingCluster
        | ClusterPhase::AssigningSlots
        | ClusterPhase::ConfiguringReplicas => std::time::Duration::from_secs(2),
        ClusterPhase::ScalingUpStatefulSet
        | ClusterPhase::WaitingForNewPods
        | ClusterPhase::AddingNodesToCluster
        | ClusterPhase::ConfiguringNewReplicas => std::time::Duration::from_secs(2),
        ClusterPhase::RemovingNodesFromCluster | ClusterPhase::ScalingDownStatefulSet => {
            std::time::Duration::from_secs(2)
        }
        ClusterPhase::RebalancingSlots | ClusterPhase::EvacuatingSlots => {
            std::time::Duration::from_secs(5)
        }
        ClusterPhase::VerifyingClusterHealth => std::time::Duration::from_secs(2),
        ClusterPhase::Degraded => std::time::Duration::from_secs(10),
        ClusterPhase::Failed => std::time::Duration::from_secs(60),
        ClusterPhase::Pending | ClusterPhase::Deleting => std::time::Duration::from_secs(5),
    }
}

/// Reconcile a ValkeyCluster.
#[tracing::instrument(
    skip(obj, ctx),
    fields(
        name = %obj.name_any(),
        namespace = obj.namespace().as_deref().unwrap_or("default"),
        phase = ?obj.status.as_ref().map(|s| s.phase)
    )
)]
pub async fn reconcile(obj: Arc<ValkeyCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let start_time = Instant::now();
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    debug!(name = %name, namespace = %namespace, "Reconciling ValkeyCluster");

    let api: Api<ValkeyCluster> = Api::namespaced(ctx.client.clone(), &namespace);

    if obj.metadata.deletion_timestamp.is_some() {
        return handle_deletion(&obj, &ctx, &namespace).await;
    }

    if !obj.finalizers().iter().any(|f| f == FINALIZER) {
        info!(name = %name, "Adding finalizer");
        add_finalizer(&api, &name, FINALIZER).await?;
        return Ok(Action::requeue(std::time::Duration::from_secs(1)));
    }

    let current_phase = obj
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(ClusterPhase::Pending);

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

    // TODO: Use state_machine for validating transitions and preventing invalid state changes
    let _state_machine = ClusterStateMachine::new();

    const MAX_RECONCILE_COUNT: i32 = 50;
    const MAX_STUCK_DURATION_SECS: i64 = 300;

    let status = obj.status.as_ref();
    let reconcile_count = status.map(|s| s.reconcile_count).unwrap_or(0);
    let last_transition = status.and_then(|s| s.last_phase_transition.as_ref());

    let is_operational_phase = matches!(
        current_phase,
        ClusterPhase::Creating
            | ClusterPhase::WaitingForPods
            | ClusterPhase::InitializingCluster
            | ClusterPhase::AssigningSlots
            | ClusterPhase::ConfiguringReplicas
            | ClusterPhase::ScalingUpStatefulSet
            | ClusterPhase::WaitingForNewPods
            | ClusterPhase::AddingNodesToCluster
            | ClusterPhase::RebalancingSlots
            | ClusterPhase::ConfiguringNewReplicas
            | ClusterPhase::EvacuatingSlots
            | ClusterPhase::RemovingNodesFromCluster
            | ClusterPhase::ScalingDownStatefulSet
            | ClusterPhase::VerifyingClusterHealth
    );

    if is_operational_phase {
        let diagnostic_hint = DiagnosticHint::for_phase(current_phase, &namespace, &name);

        if reconcile_count > MAX_RECONCILE_COUNT {
            let event_msg = format!(
                "Phase {} stuck: {} reconcile iterations exceeded threshold of {}. See status.message for diagnostics.",
                current_phase, reconcile_count, MAX_RECONCILE_COUNT
            );
            let error_msg = format!(
                "Phase {} stuck: {} reconcile iterations exceeded threshold of {}.\n\n{}",
                current_phase, reconcile_count, MAX_RECONCILE_COUNT, diagnostic_hint
            );
            error!(name = %name, phase = %current_phase, reconcile_count, "Stuck detection triggered");
            ctx.publish_warning_event(&obj, "StuckDetected", "Reconciling", Some(event_msg))
                .await;
            StatusUpdate {
                api: &api,
                name: &name,
                phase: ClusterPhase::Failed,
                ready_replicas: 0,
                error_message: Some(&error_msg),
                health_status: None,
                generation: current_gen,
            }
            .apply()
            .await?;
            return Ok(Action::requeue(requeue_duration(ClusterPhase::Failed)));
        }

        if let Some(transition_time) = last_transition
            && let Ok(ts) = jiff::Timestamp::from_str(transition_time)
        {
            let elapsed = jiff::Timestamp::now().duration_since(ts).as_secs();
            if elapsed > MAX_STUCK_DURATION_SECS {
                let event_msg = format!(
                    "Phase {} stuck: {} seconds since last transition exceeds threshold of {}. See status.message for diagnostics.",
                    current_phase, elapsed, MAX_STUCK_DURATION_SECS
                );
                let error_msg = format!(
                    "Phase {} stuck: {} seconds since last transition exceeds threshold of {}.\n\n{}",
                    current_phase, elapsed, MAX_STUCK_DURATION_SECS, diagnostic_hint
                );
                error!(name = %name, phase = %current_phase, elapsed_secs = elapsed, "Stuck detection triggered");
                ctx.publish_warning_event(&obj, "StuckDetected", "Reconciling", Some(event_msg))
                    .await;
                StatusUpdate {
                    api: &api,
                    name: &name,
                    phase: ClusterPhase::Failed,
                    ready_replicas: 0,
                    error_message: Some(&error_msg),
                    health_status: None,
                    generation: current_gen,
                }
                .apply()
                .await?;
                return Ok(Action::requeue(requeue_duration(ClusterPhase::Failed)));
            }
        }
    }

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

    let next_phase =
        dispatch_phase(&obj, &ctx, &api, &phase_ctx, &namespace, &name, current_gen).await?;

    let ready_replicas_final = check_ready_replicas(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);

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

    StatusUpdate {
        api: &api,
        name: &name,
        phase: next_phase,
        ready_replicas: ready_replicas_final,
        error_message: None,
        health_status: health_status.as_ref(),
        generation: current_gen,
    }
    .apply()
    .await?;

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

    Ok(Action::requeue(requeue_duration(next_phase)))
}

/// Dispatch to the appropriate phase handler and return the next phase.
///
/// Extracted from `reconcile()` to keep that function under 200 lines.
#[allow(clippy::too_many_lines)]
async fn dispatch_phase(
    obj: &ValkeyCluster,
    ctx: &Context,
    api: &Api<ValkeyCluster>,
    phase_ctx: &PhaseContext,
    namespace: &str,
    name: &str,
    current_gen: Option<i64>,
) -> Result<ClusterPhase, Error> {
    let current_phase = obj
        .status
        .as_ref()
        .map(|s| s.phase)
        .unwrap_or(ClusterPhase::Pending);

    match current_phase {
        ClusterPhase::Pending => {
            if let Err(e) = validate_spec(obj) {
                error!(name = %name, error = %e, "Validation failed");
                ctx.publish_warning_event(
                    obj,
                    "ValidationFailed",
                    "Validating",
                    Some(e.to_string()),
                )
                .await;
                StatusUpdate {
                    api,
                    name,
                    phase: ClusterPhase::Failed,
                    ready_replicas: 0,
                    error_message: Some(&e.to_string()),
                    health_status: None,
                    generation: current_gen,
                }
                .apply()
                .await?;
                return Err(e);
            }
            ctx.publish_normal_event(
                obj,
                "Creating",
                "CreateResources",
                Some("Starting resource creation".to_string()),
            )
            .await;
            Ok(ClusterPhase::Creating)
        }
        ClusterPhase::Creating => {
            let result = cluster_phases::handle_creating(
                obj,
                ctx,
                phase_ctx,
                create_owned_resources(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::WaitingForPods => {
            let result = cluster_phases::handle_waiting_for_pods(
                obj,
                ctx,
                phase_ctx,
                create_owned_resources(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::InitializingCluster => {
            let result = cluster_phases::handle_initializing_cluster(
                obj,
                ctx,
                api,
                phase_ctx,
                execute_cluster_init(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::AssigningSlots => {
            let result = cluster_phases::handle_assigning_slots(
                obj,
                ctx,
                api,
                phase_ctx,
                execute_slot_assignment_no_replicas(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::ConfiguringReplicas => {
            let result = cluster_phases::handle_configuring_replicas(
                obj,
                ctx,
                api,
                phase_ctx,
                execute_replica_setup(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::Running => {
            let result = cluster_phases::handle_running(
                obj,
                ctx,
                phase_ctx,
                create_owned_resources(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::ScalingUpStatefulSet => {
            let result = cluster_phases::handle_scaling_up_statefulset(
                obj,
                ctx,
                api,
                phase_ctx,
                create_owned_resources(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::WaitingForNewPods => {
            let result = cluster_phases::handle_waiting_for_new_pods(obj, ctx, phase_ctx).await?;
            Ok(result.next_phase)
        }
        ClusterPhase::AddingNodesToCluster => {
            let result =
                cluster_phases::handle_adding_nodes_to_cluster(obj, ctx, phase_ctx, async {
                    add_new_replicas_to_cluster(obj, ctx, namespace)
                        .await
                        .map(|_| ())
                })
                .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::RebalancingSlots => {
            let result = cluster_phases::handle_rebalancing_slots(
                obj,
                ctx,
                phase_ctx,
                execute_scale_up_rebalance(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::ConfiguringNewReplicas => {
            let result = cluster_phases::handle_configuring_new_replicas(
                obj,
                ctx,
                api,
                phase_ctx,
                execute_replica_setup(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::EvacuatingSlots => {
            let result = cluster_phases::handle_evacuating_slots(
                obj,
                ctx,
                api,
                phase_ctx,
                execute_scale_down_evacuation(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::RemovingNodesFromCluster => {
            let result = cluster_phases::handle_removing_nodes_from_cluster(
                obj,
                ctx,
                phase_ctx,
                execute_forget_removed_nodes(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::ScalingDownStatefulSet => {
            let result = cluster_phases::handle_scaling_down_statefulset(
                obj,
                ctx,
                api,
                phase_ctx,
                create_owned_resources(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::VerifyingClusterHealth => {
            let result =
                cluster_phases::handle_verifying_cluster_health(obj, ctx, api, phase_ctx, async {
                    check_cluster_health(obj, ctx, namespace)
                        .await
                        .map(|h| ClusterHealthResult {
                            is_healthy: h.is_healthy,
                            slots_assigned: h.slots_assigned,
                            healthy_masters: h.healthy_masters,
                        })
                })
                .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::Degraded => {
            let result = cluster_phases::handle_degraded(
                obj,
                ctx,
                phase_ctx,
                create_owned_resources(obj, ctx, namespace),
            )
            .await?;
            Ok(result.next_phase)
        }
        ClusterPhase::Failed => handle_failed_phase(obj, ctx, namespace, name).await,
        ClusterPhase::Deleting => Ok(ClusterPhase::Deleting),
    }
}

/// Handle the Failed phase: attempt automatic stale-IP recovery or stay failed.
async fn handle_failed_phase(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    name: &str,
) -> Result<ClusterPhase, Error> {
    let running_pods = check_running_pods(obj, ctx, namespace).await.unwrap_or(0);
    let ready_replicas = check_ready_replicas(obj, ctx, namespace).await.unwrap_or(0);
    let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

    let recovery_attempts = obj
        .status
        .as_ref()
        .map(|s| s.recovery_attempts)
        .unwrap_or(0);
    let last_recovery = obj
        .status
        .as_ref()
        .and_then(|s| s.last_recovery_attempt.as_ref());

    const MAX_RECOVERY_ATTEMPTS: i32 = 5;
    const BASE_BACKOFF_SECS: i64 = 30;

    let can_attempt = running_pods >= desired_replicas
        && ready_replicas < desired_replicas
        && recovery_attempts < MAX_RECOVERY_ATTEMPTS;

    let backoff_elapsed = if let Some(last_attempt) = last_recovery {
        if let Ok(ts) = std::str::FromStr::from_str(last_attempt).map(|t: jiff::Timestamp| t) {
            let backoff_secs = BASE_BACKOFF_SECS * (1i64 << recovery_attempts.min(10));
            let elapsed = jiff::Timestamp::now().duration_since(ts).as_secs();
            elapsed >= backoff_secs
        } else {
            true
        }
    } else {
        true
    };

    if can_attempt && backoff_elapsed {
        info!(
            name = %name,
            running_pods,
            ready_replicas,
            recovery_attempts,
            "Attempting automatic stale IP recovery"
        );

        ctx.publish_normal_event(
            obj,
            "RecoveryAttempt",
            "Recovering",
            Some(format!(
                "Attempting automatic recovery (attempt {}/{})",
                recovery_attempts + 1,
                MAX_RECOVERY_ATTEMPTS
            )),
        )
        .await;

        let recovery_result = execute_stale_ip_recovery(obj, ctx, namespace).await;

        let mut status = obj.status.clone().unwrap_or_default();
        status.recovery_attempts = recovery_attempts + 1;
        status.last_recovery_attempt = Some(jiff::Timestamp::now().to_string());

        let api: Api<ValkeyCluster> = Api::namespaced(ctx.client.clone(), namespace);
        let patch = serde_json::json!({
            "status": {
                "recovery_attempts": status.recovery_attempts,
                "last_recovery_attempt": status.last_recovery_attempt,
            }
        });
        let _ = api
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await;

        match recovery_result {
            Ok(()) => {
                info!(name = %name, "Stale IP recovery successful, transitioning to Degraded");
                ctx.publish_normal_event(
                    obj,
                    "RecoverySucceeded",
                    "Recovered",
                    Some("Automatic stale IP recovery successful".to_string()),
                )
                .await;
                Ok(ClusterPhase::Degraded)
            }
            Err(e) => {
                warn!(name = %name, error = %e, "Stale IP recovery failed, will retry");
                ctx.publish_warning_event(
                    obj,
                    "RecoveryFailed",
                    "Recovering",
                    Some(format!("Recovery attempt failed: {}", e)),
                )
                .await;
                Ok(ClusterPhase::Failed)
            }
        }
    } else if recovery_attempts >= MAX_RECOVERY_ATTEMPTS {
        warn!(
            name = %name,
            recovery_attempts,
            "Max recovery attempts reached, manual intervention required"
        );
        Ok(ClusterPhase::Failed)
    } else if ready_replicas >= desired_replicas {
        info!(
            name = %name,
            ready_replicas,
            "All replicas ready in Failed state, checking cluster health"
        );

        match check_cluster_health(obj, ctx, namespace).await {
            Ok(health) if health.is_healthy => {
                info!(name = %name, "Cluster is healthy, transitioning to Running");
                ctx.publish_normal_event(
                    obj,
                    "Recovered",
                    "Running",
                    Some("Cluster recovered and healthy".to_string()),
                )
                .await;
                Ok(ClusterPhase::Running)
            }
            Ok(health) => {
                info!(
                    name = %name,
                    healthy_masters = health.healthy_masters,
                    slots_assigned = health.slots_assigned,
                    "Cluster not fully healthy yet, transitioning to Degraded"
                );
                Ok(ClusterPhase::Degraded)
            }
            Err(e) => {
                debug!(name = %name, error = %e, "Failed to check cluster health");
                Ok(ClusterPhase::Failed)
            }
        }
    } else {
        debug!(
            name = %name,
            running_pods,
            ready_replicas,
            desired = desired_replicas,
            can_attempt,
            backoff_elapsed,
            "Waiting for recovery conditions"
        );
        Ok(ClusterPhase::Failed)
    }
}

/// Error policy for the controller.
pub fn error_policy(obj: Arc<ValkeyCluster>, error: &Error, ctx: Arc<Context>) -> Action {
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    if let Some(ref health_state) = ctx.health_state {
        health_state.metrics.record_error(&namespace, &name);
    }

    if error.is_not_found() {
        debug!(name = %name, "Resource not found (likely deleted)");
        return Action::await_change();
    }

    if error.is_retryable() {
        let retry_count = obj
            .metadata
            .annotations
            .as_ref()
            .and_then(|ann| ann.get("valkey-operator.smoketurner.com/retry-count"))
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0);

        let backoff = error.requeue_after_with_retry_count(retry_count);

        warn!(
            name = %name,
            error = %error,
            retry_count,
            backoff_secs = backoff.as_secs(),
            "Retryable error, will retry with exponential backoff"
        );

        Action::requeue(backoff)
    } else {
        error!(name = %name, error = %error, "Non-retryable error");
        Action::requeue(std::time::Duration::from_secs(300))
    }
}

// ── private helpers ────────────────────────────────────────────────────────

/// Create owned resources (StatefulSet, Services, PDB, Certificate).
async fn create_owned_resources(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();

    if obj.uid().is_none() {
        return Err(Error::Validation(format!(
            "ValkeyCluster {} has no UID - cannot create child resources without valid owner reference",
            name
        )));
    }

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

    let client_svc = services::generate_client_service(obj);
    svc_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&client_svc),
        )
        .await?;

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

    let cert_resource = certificate::generate_certificate(obj);
    let cert_name = certificate::certificate_secret_name(obj);
    let cert_ar = ApiResource::from_gvk(&kube::api::GroupVersionKind {
        group: "cert-manager.io".to_string(),
        version: "v1".to_string(),
        kind: "Certificate".to_string(),
    });
    let cert_api: Api<DynamicObject> =
        Api::namespaced_with(ctx.client.clone(), namespace, &cert_ar);
    let cert_value: serde_json::Value = serde_json::to_value(&cert_resource)
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
                .is_some_and(|phase| phase == "Running")
        })
        .count();

    Ok(running_count as i32)
}

async fn execute_cluster_init(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    let topology = ClusterTopology::build(&ctx.client, namespace, &name, None).await?;

    cluster_init::execute_cluster_meet(
        obj,
        &topology,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await?;

    Ok(())
}

async fn execute_stale_ip_recovery(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    let topology = ClusterTopology::build(&ctx.client, namespace, &name, None).await?;

    cluster_init::recover_stale_ips(
        obj,
        &topology,
        password.as_deref(),
        tls_certs.as_ref(),
        &strategy,
    )
    .await?;

    Ok(())
}

async fn execute_slot_assignment_no_replicas(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    cluster_init::assign_slots_to_masters(obj, password.as_deref(), tls_certs.as_ref(), &strategy)
        .await?;

    Ok(())
}

async fn execute_replica_setup(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    if obj.spec.replicas_per_master == 0 {
        return Ok(());
    }

    let (password, tls_certs, strategy) = ctx.connection_context(obj, namespace).await?;

    cluster_init::setup_replicas(obj, password.as_deref(), tls_certs.as_ref(), &strategy).await?;

    Ok(())
}

async fn execute_scale_up_rebalance(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    let result = scaling::execute_scaling_operation(obj, ctx, namespace).await?;

    if let Some(ref e) = result.error {
        return Err(crate::client::ValkeyError::SlotMigrationFailed(e.clone()).into());
    }
    Ok(result.success)
}

async fn execute_scale_down_evacuation(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<bool, Error> {
    deletion::verify_scale_down_quorum(obj, ctx, namespace).await?;

    let result = scaling::execute_scaling_operation(obj, ctx, namespace).await?;

    if let Some(ref e) = result.error {
        return Err(crate::client::ValkeyError::SlotMigrationFailed(e.clone()).into());
    }
    Ok(result.success)
}

pub(crate) async fn check_cluster_health(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<ClusterHealthStatus, Error> {
    use crate::crd::{ClusterTopology, MasterNode, NodeId, ReplicaNode};

    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Ok(ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 0,
            healthy_replicas: 0,
            slots_assigned: 0,
            topology: None,
            max_replica_lag: None,
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
                max_replica_lag: None,
            });
        }
    };

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
                max_replica_lag: None,
            });
        }
    };

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
                max_replica_lag: None,
            });
        }
    };

    let _ = client.close().await;

    let cluster_state = crate::client::ClusterState {
        cluster_info: cluster_info.clone(),
        cluster_nodes: cluster_nodes.clone(),
        topology: None,
    };

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

    let masters = cluster_nodes.masters();

    let replica_lags = observe_replica_lags(obj, ctx, namespace, &cluster_nodes).await;
    let max_replica_lag = replica_lags.values().copied().max();

    let topology = ClusterTopology {
        masters: masters
            .iter()
            .map(|m| {
                let node_replicas = cluster_nodes.replicas_of(&m.node_id);
                MasterNode {
                    node_id: NodeId::from(m.node_id.clone()),
                    pod_name: extract_pod_name(&m.address),
                    slot_ranges: m.slots.iter().map(|s| s.to_string()).collect(),
                    replicas: node_replicas
                        .iter()
                        .map(|r| ReplicaNode {
                            node_id: NodeId::from(r.node_id.clone()),
                            pod_name: extract_pod_name(&r.address),
                            replication_lag: replica_lags.get(&r.node_id).copied().unwrap_or(0),
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
        max_replica_lag,
    })
}

/// Observe replication lag per replica.
///
/// Returns a map of replica `node_id` → observed lag in bytes. Per-replica
/// connection or query failures are logged at debug level and the replica is
/// omitted from the map — callers treat absence as "not observed" rather than
/// "infinite lag" so a transient connection blip cannot flip
/// `ReplicasInSync` to False.
async fn observe_replica_lags(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
    cluster_nodes: &crate::client::types::ParsedClusterNodes,
) -> std::collections::HashMap<String, i64> {
    let mut lags = std::collections::HashMap::new();
    for master in cluster_nodes.masters() {
        let master_port: u16 = master.port.try_into().unwrap_or(6379);
        let master_client = match ctx
            .connect_to_host(obj, namespace, &master.ip, master_port)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                debug!(
                    node_id = %master.node_id,
                    error = %e,
                    "Skipping replica-lag observation: master connection failed"
                );
                continue;
            }
        };

        for replica in cluster_nodes.replicas_of(&master.node_id) {
            let replica_port: u16 = replica.port.try_into().unwrap_or(6379);
            let replica_client = match ctx
                .connect_to_host(obj, namespace, &replica.ip, replica_port)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    debug!(
                        node_id = %replica.node_id,
                        error = %e,
                        "Skipping replica-lag observation: replica connection failed"
                    );
                    continue;
                }
            };

            match replica_client.get_replication_lag(&master_client).await {
                Ok(lag) => {
                    lags.insert(replica.node_id.clone(), lag);
                }
                Err(e) => {
                    debug!(
                        node_id = %replica.node_id,
                        error = %e,
                        "Skipping replica-lag observation: INFO REPLICATION failed"
                    );
                }
            }
            let _ = replica_client.close().await;
        }

        let _ = master_client.close().await;
    }
    lags
}

async fn get_current_master_count(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
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

#[allow(clippy::too_many_lines)]
async fn add_new_replicas_to_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    use crate::controller::cluster_topology::NodeRole;

    let name = obj.name_any();
    let masters = obj.spec.masters;
    let replicas_per_master = obj.spec.replicas_per_master;

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

    let cluster_nodes = client.cluster_nodes().await?;

    let current_node_count = cluster_nodes.nodes.len() as i32;
    let desired_total = total_pods(masters, replicas_per_master);
    let _ = client.close().await;

    if current_node_count >= desired_total {
        return Ok(0);
    }

    let topology =
        ClusterTopology::build(&ctx.client, namespace, &name, Some(&cluster_nodes)).await?;

    let new_pods: Vec<_> = topology.nodes_not_in_cluster().collect();

    if new_pods.is_empty() {
        return Ok(0);
    }

    info!(name = %name, new_count = new_pods.len(), "Adding new nodes to cluster via CLUSTER MEET");

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

    for node in &new_pods {
        let Some(endpoint) = &node.endpoint else {
            warn!(pod = %node.pod_name, "Pod has no IP, skipping CLUSTER MEET");
            continue;
        };
        info!(pod = %node.pod_name, ip = %endpoint.ip(), "Executing CLUSTER MEET");
        client.cluster_meet(endpoint.ip(), endpoint.port()).await?;
    }

    let _ = client.close().await;

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let client = ctx.connect_to_cluster(obj, namespace, 0).await?;

    let cluster_nodes = client.cluster_nodes().await?;

    let _ = client.close().await;

    let topology =
        ClusterTopology::build(&ctx.client, namespace, &name, Some(&cluster_nodes)).await?;

    let mut master_node_ids: Vec<Option<String>> = Vec::with_capacity(masters as usize);
    for ordinal in 0..masters {
        if let Some(node) = topology.by_ordinal(ordinal) {
            if node.is_master_with_slots() || node.role == NodeRole::Master {
                master_node_ids.push(node.node_id.clone());
            } else {
                debug!(
                    ordinal,
                    node_id = ?node.node_id,
                    "Pod expected to be master is still a replica in cluster topology"
                );
                master_node_ids.push(None);
            }
        } else {
            master_node_ids.push(None);
        }
    }

    let mut added = 0;
    for node in &new_pods {
        if node.ordinal < masters {
            continue;
        }

        let master_index = ((node.ordinal - masters) % masters) as usize;
        let master_node_id = match master_node_ids.get(master_index) {
            Some(Some(id)) => id,
            Some(None) => {
                debug!(
                    pod = %node.pod_name,
                    master_index,
                    "Skipping replica config: master not yet established in cluster"
                );
                continue;
            }
            None => {
                warn!(pod = %node.pod_name, "Cannot find master for replica");
                continue;
            }
        };

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
            master_index,
            "Configuring replica"
        );

        let replica_client = ctx.connect_to_cluster(obj, namespace, node.ordinal).await?;

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
        assert!(err.to_string().contains("below minimum 3"));
    }

    #[test]
    fn test_validate_spec_invalid_one_master() {
        let cluster = create_test_cluster("test", 1, 1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("below minimum 3"));
    }

    #[test]
    fn test_validate_spec_invalid_two_masters() {
        let cluster = create_test_cluster("test", 2, 1);
        let result = validate_spec(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("below minimum 3"));
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
                .contains("exceeds maximum 100")
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
                .contains("replicas per master cannot be negative")
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
                .contains("exceeds maximum 5")
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

    #[test]
    fn test_cluster_health_status_default_values() {
        let status = ClusterHealthStatus {
            is_healthy: false,
            healthy_masters: 0,
            healthy_replicas: 0,
            slots_assigned: 0,
            topology: None,
            max_replica_lag: None,
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
            max_replica_lag: None,
        };
        assert!(status.is_healthy);
        assert_eq!(status.healthy_masters, 3);
        assert_eq!(status.slots_assigned, 16384);
    }

    #[test]
    fn test_field_manager_constant() {
        assert_eq!(FIELD_MANAGER, "valkey-operator");
    }

    #[test]
    fn test_finalizer_constant() {
        assert_eq!(FINALIZER, "valkey-operator.smoketurner.com/finalizer");
        assert!(FINALIZER.contains("valkey-operator"));
    }

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

        let upgrade_name = cluster
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(UPGRADE_NAME_ANNOTATION))
            .cloned();
        assert_eq!(upgrade_name, Some("production-upgrade-v2".to_string()));
    }
}
