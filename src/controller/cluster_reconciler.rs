//! Reconciliation loop for ValkeyCluster.
//!
//! This module contains the main reconcile function that handles the lifecycle
//! of ValkeyCluster custom resources.

use std::sync::Arc;
use std::time::Instant;

use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, ResourceExt,
    api::{ApiResource, DynamicObject, Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    client::{ScalingContext, ScalingOps, TlsCertData},
    controller::{
        context::Context,
        error::Error,
        cluster_state_machine::ClusterStateMachine,
        cluster_init,
    },
    crd::{Condition, ValkeyCluster, ValkeyClusterStatus, ClusterPhase, total_pods},
    resources::{certificate, common, pdb, services, statefulset},
};

/// Field manager name for server-side apply
pub const FIELD_MANAGER: &str = "valkey-operator";

/// Finalizer name for graceful deletion
pub const FINALIZER: &str = "valkey-operator.smoketurner.com/finalizer";

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
                update_status(&api, &name, ClusterPhase::Failed, 0, Some(&e.to_string()), None).await?;
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

            // Check running pods - new pods might need to be added to cluster
            let running_pods = check_running_pods(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

            // Check if all desired pods are running
            if running_pods < desired_replicas {
                debug!(
                    name = %name,
                    running = running_pods,
                    desired = desired_replicas,
                    "Waiting for pods to be running"
                );
                ClusterPhase::Updating
            } else {
                // All pods are running, check if new nodes need to be added to cluster
                let nodes_in_cluster = get_nodes_in_cluster_count(&obj, &ctx, &namespace).await?;

                if nodes_in_cluster < desired_replicas {
                    // New pods need to be added to cluster via CLUSTER MEET + REPLICATE
                    info!(
                        name = %name,
                        nodes_in_cluster = nodes_in_cluster,
                        desired = desired_replicas,
                        "New pods detected, adding to cluster"
                    );

                    match add_new_replicas_to_cluster(&obj, &ctx, &namespace).await {
                        Ok(added) => {
                            info!(name = %name, added = added, "Added new replicas to cluster");
                            ctx.publish_normal_event(
                                &obj,
                                "ReplicasAdded",
                                "Updating",
                                Some(format!("Added {} new replicas to cluster", added)),
                            )
                            .await;
                        }
                        Err(e) => {
                            warn!(name = %name, error = %e, "Failed to add replicas, will retry");
                            ctx.publish_warning_event(
                                &obj,
                                "ReplicaAddFailed",
                                "Updating",
                                Some(format!("Failed to add replicas: {}", e)),
                            )
                            .await;
                        }
                    }
                    // Stay in Updating to verify all replicas are ready
                    ClusterPhase::Updating
                } else {
                    // All pods are in cluster, check ready status
                    let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;

                    if ready_replicas >= desired_replicas {
                        // Check if this update involves scaling (masters count change)
                        let current_masters = get_current_master_count(&obj, &ctx, &namespace).await?;
                        let target_masters = obj.spec.masters;

                        if current_masters != target_masters && current_masters > 0 {
                            // Scaling operation needed - transition to Resharding
                            let direction = if target_masters > current_masters {
                                "up"
                            } else {
                                "down"
                            };
                            info!(
                                name = %name,
                                current_masters = current_masters,
                                target_masters = target_masters,
                                direction = direction,
                                "Scaling detected, transitioning to Resharding"
                            );
                            ctx.publish_normal_event(
                                &obj,
                                "ScalingDetected",
                                "Updating",
                                Some(format!(
                                    "Scaling {} from {} to {} masters",
                                    direction, current_masters, target_masters
                                )),
                            )
                            .await;
                            ClusterPhase::Resharding
                        } else {
                            ctx.publish_normal_event(
                                &obj,
                                "Updated",
                                "Updating",
                                Some("Resource update completed successfully".to_string()),
                            )
                            .await;
                            ClusterPhase::Running
                        }
                    } else {
                        // Wait for pods to become ready
                        debug!(
                            name = %name,
                            ready = ready_replicas,
                            desired = desired_replicas,
                            "Waiting for pods to become ready"
                        );
                        ClusterPhase::Updating
                    }
                }
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
                match execute_cluster_init(&obj, &ctx, &namespace).await {
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
            match execute_slot_assignment(&obj, &ctx, &namespace).await {
                Ok(()) => {
                    ctx.publish_normal_event(
                        &obj,
                        "SlotsAssigned",
                        "Initializing",
                        Some("All 16384 hash slots assigned, cluster ready".to_string()),
                    )
                    .await;
                    ClusterPhase::Running
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
        ClusterPhase::Resharding => {
            // Hash slots are being migrated (scaling operation)
            info!(name = %name, "Executing scaling operation");

            match execute_scaling_operation(&obj, &ctx, &namespace).await {
                Ok(result) => {
                    if result.success {
                        let direction = if !result.nodes_added.is_empty() {
                            "up"
                        } else {
                            "down"
                        };
                        info!(
                            name = %name,
                            direction = direction,
                            nodes_added = result.nodes_added.len(),
                            nodes_removed = result.nodes_removed.len(),
                            slots_moved = result.slots_moved,
                            "Scaling operation completed"
                        );
                        ctx.publish_normal_event(
                            &obj,
                            "ReshardingComplete",
                            "Scaling",
                            Some(format!(
                                "Scaling {} complete: {} slots moved",
                                direction, result.slots_moved
                            )),
                        )
                        .await;
                        ClusterPhase::Running
                    } else {
                        warn!(
                            name = %name,
                            error = ?result.error,
                            "Scaling operation failed"
                        );
                        ctx.publish_warning_event(
                            &obj,
                            "ReshardingFailed",
                            "Scaling",
                            result.error.clone(),
                        )
                        .await;
                        // Stay in Resharding to retry
                        ClusterPhase::Resharding
                    }
                }
                Err(e) => {
                    warn!(name = %name, error = %e, "Scaling operation error, will retry");
                    ctx.publish_warning_event(
                        &obj,
                        "ReshardingFailed",
                        "Scaling",
                        Some(format!("Scaling failed: {}", e)),
                    )
                    .await;
                    // Stay in Resharding to retry
                    ClusterPhase::Resharding
                }
            }
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

    update_status(&api, &name, next_phase, ready_replicas, None, health_status.as_ref()).await?;

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
        &PatchParams::apply(FIELD_MANAGER),
        &Patch::Merge(&patch),
    )
    .await?;
    Ok(())
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

/// Retrieve password from the auth secret
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
            // Secret exists but key not found
            warn!(
                secret = %secret_name,
                key = %secret_key,
                "Password key not found in secret"
            );
            Ok(None)
        }
        Err(kube::Error::Api(e)) if e.code == 404 => {
            warn!(secret = %secret_name, "Auth secret not found");
            Ok(None)
        }
        Err(e) => Err(Error::Kube(e)),
    }
}

/// Retrieve TLS certificates from the TLS secret created by cert-manager.
///
/// The secret contains:
/// - `ca.crt`: CA certificate for verification
/// - `tls.crt`: Server/client certificate
/// - `tls.key`: Server/client private key
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

/// Execute cluster initialization (CLUSTER MEET)
async fn execute_cluster_init(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<(), Error> {
    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy based on environment
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &obj.name_any(),
    );

    // Execute cluster meet
    cluster_init::execute_cluster_meet(&ctx.client, obj, password.as_deref(), tls_certs.as_ref(), &strategy)
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
    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy based on environment
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &obj.name_any(),
    );

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

/// Check the health of the Valkey cluster.
async fn check_cluster_health(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<ClusterHealthStatus, Error> {
    use crate::client::cluster_ops::ClusterOps;
    use crate::client::valkey_client::ValkeyClient;
    use crate::crd::{ClusterTopology, MasterNode, ReplicaNode};

    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

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

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy for port forwarding
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &obj.name_any(),
    );

    // Get connection address for first master via port forwarding
    let (connect_host, connect_port) = match strategy.get_connection(0).await {
        Ok(addr) => addr,
        Err(e) => {
            debug!(error = %e, "Failed to create port forward for health check");
            return Ok(ClusterHealthStatus {
                is_healthy: false,
                healthy_masters: 0,
                healthy_replicas: 0,
                slots_assigned: 0,
                topology: None,
            });
        }
    };

    let client = match ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref()).await
    {
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

    // Build topology from cluster nodes
    let masters = cluster_nodes.masters();
    let healthy_masters = masters.iter().filter(|m| m.is_healthy()).count() as i32;
    let replicas = cluster_nodes.replicas();
    let healthy_replicas = replicas.iter().filter(|r| r.is_healthy()).count() as i32;

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
        is_healthy: cluster_info.is_healthy(),
        healthy_masters,
        healthy_replicas,
        slots_assigned: cluster_info.slots_assigned,
        topology: Some(topology),
    })
}

/// Extract pod name from address (e.g., "my-cluster-0.my-cluster-headless.ns:6379" -> "my-cluster-0").
fn extract_pod_name(address: &str) -> String {
    address
        .split(':')
        .next()
        .and_then(|h| h.split('.').next())
        .unwrap_or("unknown")
        .to_string()
}

/// Update the status of a ValkeyCluster
async fn update_status(
    api: &Api<ValkeyCluster>,
    name: &str,
    phase: ClusterPhase,
    ready_replicas: i32,
    error_message: Option<&str>,
    health_status: Option<&ClusterHealthStatus>,
) -> Result<(), Error> {
    let obj = api.get(name).await?;
    let generation = obj.metadata.generation;
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
        connection_secret: None,
        tls_secret: Some(tls_secret_name),
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
    use crate::client::cluster_ops::ClusterOps;
    use crate::client::valkey_client::ValkeyClient;

    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

    // Build host list for connecting to the cluster
    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Ok(0);
    }

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy for port forwarding
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &obj.name_any(),
    );

    // Get connection address for first master via port forwarding
    let (connect_host, connect_port) = match strategy.get_connection(0).await {
        Ok(addr) => addr,
        Err(e) => {
            debug!(error = %e, "Failed to create port forward for master count");
            return Ok(0);
        }
    };

    let client = match ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref()).await
    {
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
async fn execute_scaling_operation(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<crate::client::ScalingResult, Error> {
    use crate::client::cluster_ops::ClusterOps;
    use crate::client::valkey_client::ValkeyClient;

    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

    // Build host list for connecting to the cluster
    let pod_addresses = cluster_init::master_pod_dns_names(obj);
    if pod_addresses.is_empty() {
        return Err(Error::Valkey("No pod addresses available".to_string()));
    }

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy for port forwarding
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &obj.name_any(),
    );

    // Get connection address for first master via port forwarding
    let (connect_host, connect_port) = strategy.get_connection(0).await
        .map_err(|e| Error::Valkey(format!("Failed to create port forward for scaling: {}", e)))?;

    let client = ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref())
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
        client.scale_up(&scaling_ctx).await
    } else if scaling_ctx.is_scale_down() {
        info!(
            current = current_masters,
            target = target_masters,
            "Executing scale down"
        );
        client.scale_down(&scaling_ctx).await
    } else {
        // No scaling needed
        let _ = client.close().await;
        return Ok(crate::client::ScalingResult::default());
    };

    let _ = client.close().await;

    result.map_err(|e| Error::Valkey(format!("Scaling operation failed: {}", e)))
}

/// Get the total number of nodes in the Valkey cluster.
async fn get_nodes_in_cluster_count(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    use crate::client::cluster_ops::ClusterOps;
    use crate::client::valkey_client::ValkeyClient;

    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy for port forwarding
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &obj.name_any(),
    );

    // Get connection address for first master via port forwarding
    let (connect_host, connect_port) = match strategy.get_connection(0).await {
        Ok(addr) => addr,
        Err(e) => {
            debug!(error = %e, "Failed to create port forward for node count");
            return Ok(0);
        }
    };

    let client = match ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref()).await {
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
async fn add_new_replicas_to_cluster(
    obj: &ValkeyCluster,
    ctx: &Context,
    namespace: &str,
) -> Result<i32, Error> {
    use crate::client::cluster_ops::ClusterOps;
    use crate::client::valkey_client::ValkeyClient;

    let name = obj.name_any();
    let masters = obj.spec.masters;
    let replicas_per_master = obj.spec.replicas_per_master;

    // Get password from secret
    let password = get_auth_password(
        ctx,
        namespace,
        &obj.spec.auth.secret_ref.name,
        &obj.spec.auth.secret_ref.key,
    )
    .await?;

    // Get TLS certificates from secret
    let tls_certs = get_tls_certs(ctx, namespace, &obj.name_any()).await?;

    // Create connection strategy for port forwarding
    let strategy = cluster_init::create_connection_strategy(
        ctx.client.clone(),
        namespace,
        &name,
    );

    // First, get the current cluster state
    let (connect_host, connect_port) = strategy.get_connection(0).await
        .map_err(|e| Error::Valkey(format!("Failed to create port forward: {}", e)))?;

    let client = ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref())
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
    let existing_ips: std::collections::HashSet<_> = cluster_nodes.nodes.iter()
        .map(|n| n.address.split(':').next().unwrap_or(""))
        .collect();

    let new_pods: Vec<_> = pod_ips.iter()
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
    let (connect_host, connect_port) = strategy.get_connection(0).await
        .map_err(|e| Error::Valkey(format!("Failed to create port forward: {}", e)))?;

    let client = ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref())
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
    let (connect_host, connect_port) = strategy.get_connection(0).await
        .map_err(|e| Error::Valkey(format!("Failed to create port forward: {}", e)))?;

    let client = ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref())
        .await
        .map_err(|e| Error::Valkey(format!("Failed to connect: {}", e)))?;

    let cluster_nodes = client
        .cluster_nodes()
        .await
        .map_err(|e| Error::Valkey(format!("Failed to get cluster nodes: {}", e)))?;

    let _ = client.close().await;

    // Build a mapping of IP to node ID
    let ip_to_node_id: std::collections::HashMap<String, String> = cluster_nodes.nodes.iter()
        .map(|n| (n.ip.clone(), n.node_id.clone()))
        .collect();

    // Get master node IDs (from pod-0 to pod-(masters-1))
    let master_ips: Vec<String> = pod_ips.iter()
        .take(masters as usize)
        .map(|(_, ip, _)| ip.clone())
        .collect();

    let master_node_ids: Vec<String> = master_ips.iter()
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
        if master_index >= master_node_ids.len() {
            warn!(pod = %pod_name, "Cannot find master for replica");
            continue;
        }

        let master_node_id = &master_node_ids[master_index];

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

        // Connect to the replica and configure it
        let (connect_host, connect_port) = strategy.get_connection(ordinal).await
            .map_err(|e| Error::Valkey(format!("Failed to create port forward for replica: {}", e)))?;

        let replica_client = ValkeyClient::connect_single(&connect_host, connect_port, password.as_deref(), tls_certs.as_ref())
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
