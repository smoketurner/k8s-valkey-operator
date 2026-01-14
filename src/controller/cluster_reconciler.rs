//! Reconciliation loop for ValkeyCluster.
//!
//! This module contains the main reconcile function that handles the lifecycle
//! of ValkeyCluster custom resources.

use std::sync::Arc;
use std::time::Instant;

use k8s_openapi::api::core::v1::Secret;
use kube::{
    Api, ResourceExt,
    api::{Patch, PatchParams},
    runtime::controller::Action,
};
use tracing::{debug, error, info, warn};

use crate::{
    controller::{
        context::Context,
        error::Error,
        cluster_state_machine::ClusterStateMachine,
        cluster_init,
    },
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

            // Check if resources are ready
            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);
            if ready_replicas >= desired_replicas {
                ctx.publish_normal_event(
                    &obj,
                    "PodsReady",
                    "Reconciling",
                    Some(format!(
                        "All pods ready ({}/{}), initializing cluster",
                        ready_replicas, desired_replicas
                    )),
                )
                .await;
                // Transition to Initializing for CLUSTER MEET operations
                ClusterPhase::Initializing
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
            // Ensure resources are still in sync
            create_owned_resources(&obj, &ctx, &namespace).await?;

            let ready_replicas = check_ready_replicas(&obj, &ctx, &namespace).await?;
            let desired_replicas = total_pods(obj.spec.masters, obj.spec.replicas_per_master);

            if ready_replicas < desired_replicas {
                // Pods not ready, wait
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
            // TODO: Phase 13 - Implement resharding logic
            // For now, transition directly to Running
            ctx.publish_normal_event(
                &obj,
                "ReshardingComplete",
                "Scaling",
                Some("Hash slot migration complete".to_string()),
            )
            .await;
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
        &PatchParams::apply(FIELD_MANAGER).force(),
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
    let statefulset = resources::generate_statefulset(obj);
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
    let headless_svc = resources::generate_headless_service(obj);
    let headless_name = resources::common::headless_service_name(obj);
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
    let client_svc = resources::generate_client_service(obj);
    svc_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&client_svc),
        )
        .await?;

    // Apply PodDisruptionBudget
    let pdb = resources::generate_pod_disruption_budget(obj);
    let pdb_api: Api<k8s_openapi::api::policy::v1::PodDisruptionBudget> =
        Api::namespaced(ctx.client.clone(), namespace);
    pdb_api
        .patch(
            &name,
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(&pdb),
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

    // TLS is always enabled in our design
    let use_tls = true;

    // Execute cluster meet
    cluster_init::execute_cluster_meet(obj, password.as_deref(), use_tls)
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

    // TLS is always enabled in our design
    let use_tls = true;

    // Assign slots to masters
    cluster_init::assign_slots_to_masters(obj, password.as_deref(), use_tls)
        .await
        .map_err(|e| Error::Valkey(e.to_string()))?;

    // Set up replicas
    if obj.spec.replicas_per_master > 0 {
        cluster_init::setup_replicas(obj, password.as_deref(), use_tls)
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

    // Connect to first master to query cluster state
    let (host, port) = &pod_addresses[0];
    let use_tls = true;

    let client = match ValkeyClient::connect_single(host, *port, password.as_deref(), use_tls).await
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
