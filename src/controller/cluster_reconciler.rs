//! Reconciliation loop for ValkeyCluster.
//!
//! This module contains the main reconcile function that handles the lifecycle
//! of ValkeyCluster custom resources.

use std::sync::Arc;
use std::time::Instant;

use kube::{
    Api, ResourceExt,
    api::{Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    controller::{context::Context, error::Error, cluster_state_machine::ClusterStateMachine},
    crd::{Condition, ValkeyCluster, ValkeyClusterStatus, ClusterPhase, total_pods},
    resources,
};

/// Field manager name for server-side apply
pub const FIELD_MANAGER: &str = "valkey-operator";

/// Finalizer name for graceful deletion
pub const FINALIZER: &str = "valkeyoperator.smoketurner.com/finalizer";

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
        add_finalizer(&api, &name).await?;
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
    let state_machine = ClusterStateMachine::new();

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
                update_status(&api, &name, ClusterPhase::Failed, 0, Some(&e.to_string())).await?;
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

            // Check if resources are ready
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
            if ready_replicas >= desired_replicas {
                ctx.publish_normal_event(
                    &obj,
                    "Ready",
                    "Reconciling",
                    Some(format!(
                        "Resource is ready with {}/{} replicas",
                        ready_replicas, desired_replicas
                    )),
                )
                .await;
                ClusterPhase::Running
            } else {
                ClusterPhase::Creating
            }
        }
        ClusterPhase::Running => {
            // Check if update needed
            if spec_changed {
                ctx.publish_normal_event(
                    &obj,
                    "SpecChanged",
                    "Updating",
                    Some("Resource spec changed, applying updates".to_string()),
                )
                .await;
                ClusterPhase::Updating
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
        ClusterPhase::Updating => {
            // Apply updates
            create_owned_resources(&obj, &ctx, &namespace).await?;

            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
            if ready_replicas >= desired_replicas {
                ctx.publish_normal_event(
                    &obj,
                    "Updated",
                    "Updating",
                    Some("Resource update completed successfully".to_string()),
                )
                .await;
                ClusterPhase::Running
            } else {
                ClusterPhase::Updating
            }
        }
        ClusterPhase::Degraded => {
            // Check if recovered
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
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
            } else if ready_replicas == 0 {
                ctx.publish_warning_event(
                    &obj,
                    "Failed",
                    "Reconciling",
                    Some("Resource failed: no replicas available".to_string()),
                )
                .await;
                ClusterPhase::Failed
            } else {
                ClusterPhase::Degraded
            }
        }
        ClusterPhase::Initializing => {
            // Cluster nodes are up, performing CLUSTER MEET
            // TODO: Implement cluster initialization logic
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
            if ready_replicas >= desired_replicas {
                ClusterPhase::AssigningSlots
            } else {
                ClusterPhase::Initializing
            }
        }
        ClusterPhase::AssigningSlots => {
            // Hash slots are being assigned to masters
            // TODO: Implement slot assignment logic
            ClusterPhase::Running
        }
        ClusterPhase::Resharding => {
            // Hash slots are being migrated (scaling operation)
            // TODO: Implement resharding logic
            ClusterPhase::Running
        }
        ClusterPhase::Failed => {
            // Manual intervention required
            warn!(name = %name, "Resource in Failed state, waiting for intervention");
            ClusterPhase::Failed
        }
        ClusterPhase::Deleting => {
            // Should be handled by deletion branch above
            ClusterPhase::Deleting
        }
    };

    // Log state transition (state machine available for advanced validation)
    let _state_machine = state_machine; // Suppress unused warning, available for enhancement

    // Update status
    let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace)
        .await
        .unwrap_or(0);
    update_status(&api, &name, next_phase, ready_replicas, None).await?;

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

    // Determine requeue interval based on state
    let requeue_duration = match next_phase {
        ClusterPhase::Running => std::time::Duration::from_secs(60),
        ClusterPhase::Creating | ClusterPhase::Updating => std::time::Duration::from_secs(10),
        ClusterPhase::Initializing | ClusterPhase::AssigningSlots => std::time::Duration::from_secs(5),
        ClusterPhase::Resharding => std::time::Duration::from_secs(15),
        ClusterPhase::Degraded => std::time::Duration::from_secs(30),
        ClusterPhase::Failed => std::time::Duration::from_secs(300),
        ClusterPhase::Pending | ClusterPhase::Deleting => std::time::Duration::from_secs(30),
    };

    Ok(Action::requeue(requeue_duration))
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
        warn!(name = %name, error = %error, "Retryable error, will retry");
        Action::requeue(error.requeue_after())
    } else {
        error!(name = %name, error = %error, "Non-retryable error");
        Action::requeue(std::time::Duration::from_secs(300))
    }
}

/// Validate the resource spec
fn validate_spec(obj: &ValkeyCluster) -> Result<(), Error> {
    // Valkey cluster requires minimum 3 masters for proper quorum
    if obj.spec.masters < 3 {
        return Err(Error::Validation("masters must be at least 3 for cluster quorum".to_string()));
    }
    if obj.spec.masters > 100 {
        return Err(Error::Validation("masters cannot exceed 100".to_string()));
    }
    if obj.spec.replicas_per_master < 0 {
        return Err(Error::Validation("replicasPerMaster cannot be negative".to_string()));
    }
    if obj.spec.replicas_per_master > 5 {
        return Err(Error::Validation("replicasPerMaster cannot exceed 5".to_string()));
    }
    // TLS is required - validate issuer ref
    if obj.spec.tls.issuer_ref.name.is_empty() {
        return Err(Error::Validation("tls.issuerRef.name is required".to_string()));
    }
    // Auth is required - validate secret ref
    if obj.spec.auth.secret_ref.name.is_empty() {
        return Err(Error::Validation("auth.secretRef.name is required".to_string()));
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

/// Add finalizer to resource
async fn add_finalizer(api: &Api<ValkeyCluster>, name: &str) -> Result<(), Error> {
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": [FINALIZER]
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

/// Remove finalizer from resource
async fn remove_finalizer(api: &Api<ValkeyCluster>, name: &str) -> Result<(), Error> {
    let patch = serde_json::json!({
        "metadata": {
            "finalizers": null
        }
    });
    api.patch(
        name,
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
}

/// Create owned resources (Deployment, ConfigMap, Service)
async fn create_owned_resources(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    let name = obj.name_any();

    // Apply ConfigMap
    let configmap = resources::common::generate_configmap(obj);
    let cm_api: Api<k8s_openapi::api::core::v1::ConfigMap> =
        Api::namespaced(ctx.client.clone(), namespace);
    cm_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&configmap),
        )
        .await?;

    // Apply Deployment
    let deployment = resources::common::generate_deployment(obj);
    let deploy_api: Api<k8s_openapi::api::apps::v1::Deployment> =
        Api::namespaced(ctx.client.clone(), namespace);
    deploy_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&deployment),
        )
        .await?;

    // Apply Service
    let service = resources::common::generate_service(obj);
    let svc_api: Api<k8s_openapi::api::core::v1::Service> =
        Api::namespaced(ctx.client.clone(), namespace);
    svc_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&service),
        )
        .await?;

    debug!(name = %name, "Applied owned resources");
    Ok(())
}

/// Check number of ready replicas
async fn check_ready_replicas(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    let name = obj.name_any();
    let deploy_api: Api<k8s_openapi::api::apps::v1::Deployment> =
        Api::namespaced(ctx.client.clone(), namespace);

    match deploy_api.get(&name).await {
        Ok(deployment) => {
            let ready = deployment
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

/// Update the status of a ValkeyCluster
async fn update_status(
    api: &Api<ValkeyCluster>,
    name: &str,
    phase: ClusterPhase,
    ready_replicas: i32,
    error_message: Option<&str>,
) -> Result<(), Error> {
    let generation = api.get(name).await?.metadata.generation;

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

    let status = ValkeyClusterStatus {
        phase,
        ready_nodes: String::new(), // TODO: Calculate properly
        ready_masters: 0,           // TODO: Calculate properly
        ready_replicas,
        assigned_slots: String::new(), // TODO: Calculate properly
        observed_generation: generation,
        conditions,
        topology: None,
        valkey_version: None,
        connection_endpoint: None,
        connection_secret: None,
        tls_secret: None,
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
