//! Reconciliation loop for MyResource.
//!
//! This module contains the main reconcile function that handles the lifecycle
//! of MyResource custom resources.

use std::sync::Arc;
use std::time::Instant;

use kube::{
    Api, ResourceExt,
    api::{Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    controller::{context::Context, error::Error, state_machine::ResourceStateMachine},
    crd::{Condition, MyResource, MyResourceStatus, Phase},
    resources,
};

/// Field manager name for server-side apply
pub const FIELD_MANAGER: &str = "my-operator";

/// Finalizer name for graceful deletion
pub const FINALIZER: &str = "myoperator.example.com/finalizer";

/// Reconcile a MyResource
///
/// This is the main reconciliation function called by the controller.
/// It handles the full lifecycle: creation, updates, and deletion.
pub async fn reconcile(obj: Arc<MyResource>, ctx: Arc<Context>) -> Result<Action, Error> {
    let start_time = Instant::now();
    let name = obj.name_any();
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());

    debug!(name = %name, namespace = %namespace, "Reconciling MyResource");

    let api: Api<MyResource> = Api::namespaced(ctx.client.clone(), &namespace);

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
        .unwrap_or(Phase::Pending);

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
    let state_machine = ResourceStateMachine::new();

    // Determine next phase based on current state
    let next_phase = match current_phase {
        Phase::Pending => {
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
                update_status(&api, &name, Phase::Failed, 0, Some(&e.to_string())).await?;
                return Err(e);
            }
            ctx.publish_normal_event(
                &obj,
                "Creating",
                "CreateResources",
                Some("Starting resource creation".to_string()),
            )
            .await;
            Phase::Creating
        }
        Phase::Creating => {
            // Create owned resources
            create_owned_resources(&obj, &ctx, &namespace).await?;

            // Check if resources are ready
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            if ready_replicas >= obj.spec.replicas {
                ctx.publish_normal_event(
                    &obj,
                    "Ready",
                    "Reconciling",
                    Some(format!(
                        "Resource is ready with {}/{} replicas",
                        ready_replicas, obj.spec.replicas
                    )),
                )
                .await;
                Phase::Running
            } else {
                Phase::Creating
            }
        }
        Phase::Running => {
            // Check if update needed
            if spec_changed {
                ctx.publish_normal_event(
                    &obj,
                    "SpecChanged",
                    "Updating",
                    Some("Resource spec changed, applying updates".to_string()),
                )
                .await;
                Phase::Updating
            } else {
                // Ensure resources are in sync
                create_owned_resources(&obj, &ctx, &namespace).await?;

                let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
                if ready_replicas < obj.spec.replicas {
                    ctx.publish_warning_event(
                        &obj,
                        "Degraded",
                        "Reconciling",
                        Some(format!(
                            "Resource degraded: {}/{} replicas ready",
                            ready_replicas, obj.spec.replicas
                        )),
                    )
                    .await;
                    Phase::Degraded
                } else {
                    Phase::Running
                }
            }
        }
        Phase::Updating => {
            // Apply updates
            create_owned_resources(&obj, &ctx, &namespace).await?;

            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            if ready_replicas >= obj.spec.replicas {
                ctx.publish_normal_event(
                    &obj,
                    "Updated",
                    "Updating",
                    Some("Resource update completed successfully".to_string()),
                )
                .await;
                Phase::Running
            } else {
                Phase::Updating
            }
        }
        Phase::Degraded => {
            // Check if recovered
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            if ready_replicas >= obj.spec.replicas {
                ctx.publish_normal_event(
                    &obj,
                    "Recovered",
                    "Reconciling",
                    Some(format!(
                        "Resource recovered: {}/{} replicas ready",
                        ready_replicas, obj.spec.replicas
                    )),
                )
                .await;
                Phase::Running
            } else if ready_replicas == 0 {
                ctx.publish_warning_event(
                    &obj,
                    "Failed",
                    "Reconciling",
                    Some("Resource failed: no replicas available".to_string()),
                )
                .await;
                Phase::Failed
            } else {
                Phase::Degraded
            }
        }
        Phase::Failed => {
            // Manual intervention required
            warn!(name = %name, "Resource in Failed state, waiting for intervention");
            Phase::Failed
        }
        Phase::Deleting => {
            // Should be handled by deletion branch above
            Phase::Deleting
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
        health_state
            .metrics
            .record_reconcile(&namespace, &name, duration);
        health_state.metrics.set_resource_replicas(
            &namespace,
            &name,
            i64::from(obj.spec.replicas),
            i64::from(ready_replicas),
        );
    }

    // Determine requeue interval based on state
    let requeue_duration = match next_phase {
        Phase::Running => std::time::Duration::from_secs(60),
        Phase::Creating | Phase::Updating => std::time::Duration::from_secs(10),
        Phase::Degraded => std::time::Duration::from_secs(30),
        Phase::Failed => std::time::Duration::from_secs(300),
        _ => std::time::Duration::from_secs(30),
    };

    Ok(Action::requeue(requeue_duration))
}

/// Error policy for the controller
pub fn error_policy(obj: Arc<MyResource>, error: &Error, ctx: Arc<Context>) -> Action {
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
fn validate_spec(obj: &MyResource) -> Result<(), Error> {
    if obj.spec.replicas < 1 {
        return Err(Error::Validation("replicas must be at least 1".to_string()));
    }
    if obj.spec.replicas > 10 {
        return Err(Error::Validation("replicas cannot exceed 10".to_string()));
    }
    Ok(())
}

/// Handle deletion of a MyResource
async fn handle_deletion(
    obj: &MyResource,
    ctx: &Context,
    namespace: &str,
) -> Result<Action, Error> {
    let name = obj.name_any();
    info!(name = %name, "Handling deletion");

    // Clean up owned resources (they should be garbage collected via owner references,
    // but we can do explicit cleanup here if needed)

    // Remove finalizer
    let api: Api<MyResource> = Api::namespaced(ctx.client.clone(), namespace);
    remove_finalizer(&api, &name).await?;

    Ok(Action::await_change())
}

/// Add finalizer to resource
async fn add_finalizer(api: &Api<MyResource>, name: &str) -> Result<(), Error> {
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
async fn remove_finalizer(api: &Api<MyResource>, name: &str) -> Result<(), Error> {
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
    obj: &MyResource,
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
    obj: &MyResource,
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

/// Update the status of a MyResource
async fn update_status(
    api: &Api<MyResource>,
    name: &str,
    phase: Phase,
    ready_replicas: i32,
    error_message: Option<&str>,
) -> Result<(), Error> {
    let generation = api.get(name).await?.metadata.generation;

    let conditions = if phase == Phase::Running {
        vec![Condition::ready(
            true,
            "AllReplicasReady",
            "All replicas are ready",
            generation,
        )]
    } else if phase == Phase::Failed {
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

    let status = MyResourceStatus {
        phase,
        ready_replicas,
        observed_generation: generation,
        conditions,
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
