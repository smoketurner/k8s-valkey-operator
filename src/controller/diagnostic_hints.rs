//! Diagnostic hints for stuck phase detection.
//!
//! Provides actionable kubectl commands that operators can run to diagnose
//! issues when a ValkeyCluster gets stuck in a particular phase.

use crate::crd::ClusterPhase;
use std::fmt;

/// A diagnostic command that can be run to investigate an issue.
#[derive(Debug, Clone)]
pub struct DiagnosticCommand {
    /// Description of what this command checks.
    pub description: &'static str,
    /// The actual command to run.
    pub command: String,
}

impl DiagnosticCommand {
    fn new(description: &'static str, command: impl Into<String>) -> Self {
        Self {
            description,
            command: command.into(),
        }
    }
}

/// Diagnostic hint for a stuck phase.
///
/// Contains a description of the likely issue and a list of commands
/// that operators can run to diagnose the problem.
#[derive(Debug, Clone)]
pub struct DiagnosticHint {
    /// Summary of the likely issue.
    pub summary: &'static str,
    /// Common causes of this issue.
    pub common_causes: Vec<&'static str>,
    /// Commands to run to diagnose the issue.
    pub commands: Vec<DiagnosticCommand>,
}

impl DiagnosticHint {
    /// Generate a diagnostic hint for a phase stuck condition.
    ///
    /// # Arguments
    /// * `phase` - The cluster phase that is stuck
    /// * `namespace` - The Kubernetes namespace
    /// * `cluster_name` - The ValkeyCluster name
    pub fn for_phase(phase: ClusterPhase, namespace: &str, cluster_name: &str) -> Self {
        match phase {
            // Initial creation phases
            ClusterPhase::Creating => Self::creating_phase(namespace, cluster_name),
            ClusterPhase::WaitingForPods => Self::waiting_for_pods_phase(namespace, cluster_name),
            ClusterPhase::InitializingCluster => {
                Self::initializing_cluster_phase(namespace, cluster_name)
            }
            ClusterPhase::AssigningSlots => Self::assigning_slots_phase(namespace, cluster_name),
            ClusterPhase::ConfiguringReplicas => {
                Self::configuring_replicas_phase(namespace, cluster_name)
            }

            // Scale-up phases
            ClusterPhase::ScalingUpStatefulSet => {
                Self::scaling_up_statefulset_phase(namespace, cluster_name)
            }
            ClusterPhase::WaitingForNewPods => {
                Self::waiting_for_new_pods_phase(namespace, cluster_name)
            }
            ClusterPhase::AddingNodesToCluster => {
                Self::adding_nodes_to_cluster_phase(namespace, cluster_name)
            }
            ClusterPhase::RebalancingSlots => {
                Self::rebalancing_slots_phase(namespace, cluster_name)
            }
            ClusterPhase::ConfiguringNewReplicas => {
                Self::configuring_new_replicas_phase(namespace, cluster_name)
            }

            // Scale-down phases
            ClusterPhase::EvacuatingSlots => Self::evacuating_slots_phase(namespace, cluster_name),
            ClusterPhase::RemovingNodesFromCluster => {
                Self::removing_nodes_from_cluster_phase(namespace, cluster_name)
            }
            ClusterPhase::ScalingDownStatefulSet => {
                Self::scaling_down_statefulset_phase(namespace, cluster_name)
            }

            // Verification
            ClusterPhase::VerifyingClusterHealth => {
                Self::verifying_cluster_health_phase(namespace, cluster_name)
            }

            // Recovery phases
            ClusterPhase::Degraded => Self::degraded_phase(namespace, cluster_name),

            // Terminal/other phases
            ClusterPhase::Running
            | ClusterPhase::Failed
            | ClusterPhase::Pending
            | ClusterPhase::Deleting => Self::unexpected_phase(namespace, cluster_name),
        }
    }

    fn creating_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "StatefulSet creation or update failed",
            common_causes: vec![
                "PVC storage class does not exist or has no available capacity",
                "Resource quota exceeded in namespace",
                "Admission webhook rejected the StatefulSet",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check StatefulSet status",
                    format!("kubectl get statefulset {cluster_name} -n {namespace} -o wide"),
                ),
                DiagnosticCommand::new(
                    "Check StatefulSet events",
                    format!("kubectl describe statefulset {cluster_name} -n {namespace}"),
                ),
                DiagnosticCommand::new(
                    "Check PVCs",
                    format!(
                        "kubectl get pvc -n {namespace} -l app.kubernetes.io/instance={cluster_name}"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check storage classes",
                    "kubectl get storageclass".to_string(),
                ),
                DiagnosticCommand::new(
                    "Check resource quotas",
                    format!("kubectl get resourcequota -n {namespace}"),
                ),
            ],
        }
    }

    fn waiting_for_pods_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Pods not becoming ready",
            common_causes: vec![
                "Image pull failed (ImagePullBackOff)",
                "Container crashing on startup (CrashLoopBackOff)",
                "Pod stuck pending due to resource constraints or PVC issues",
                "Readiness probe failing",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check pod status",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} -o wide"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check pod events and conditions",
                    format!(
                        "kubectl describe pods -n {namespace} -l app.kubernetes.io/instance={cluster_name}"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check container logs (first pod)",
                    format!("kubectl logs -n {namespace} {cluster_name}-0 --all-containers"),
                ),
                DiagnosticCommand::new(
                    "Check PVC binding status",
                    format!(
                        "kubectl get pvc -n {namespace} -l app.kubernetes.io/instance={cluster_name}"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check events in namespace",
                    format!(
                        "kubectl get events -n {namespace} --sort-by='.lastTimestamp' | tail -20"
                    ),
                ),
            ],
        }
    }

    fn initializing_cluster_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "CLUSTER MEET failing - nodes cannot join cluster",
            common_causes: vec![
                "Network connectivity issues between pods",
                "TLS certificate problems (expired, wrong CA)",
                "Auth password mismatch",
                "DNS resolution failure",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check Valkey logs for CLUSTER MEET errors",
                    format!(
                        "kubectl logs -n {namespace} {cluster_name}-0 | grep -i 'cluster\\|meet\\|error'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check TLS certificate",
                    format!("kubectl get certificate -n {namespace} {cluster_name}-tls -o wide"),
                ),
                DiagnosticCommand::new(
                    "Check auth secret exists",
                    format!("kubectl get secret -n {namespace} | grep -i valkey"),
                ),
                DiagnosticCommand::new(
                    "Test connectivity between pods",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- sh -c 'for i in 0 1 2; do echo \"Testing {cluster_name}-$i:\"; timeout 2 sh -c \"</dev/tcp/{cluster_name}-$i.{cluster_name}-headless.{namespace}.svc.cluster.local/6379\" && echo OK || echo FAIL; done'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check DNS resolution",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- nslookup {cluster_name}-headless.{namespace}.svc.cluster.local"
                    ),
                ),
            ],
        }
    }

    fn assigning_slots_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Slot assignment failing",
            common_causes: vec![
                "Cluster nodes not recognizing each other",
                "Some nodes unreachable",
                "Cluster in inconsistent state",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check cluster state",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER INFO"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check cluster nodes",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check Valkey logs",
                    format!(
                        "kubectl logs -n {namespace} {cluster_name}-0 | grep -i 'slot\\|cluster\\|error' | tail -30"
                    ),
                ),
            ],
        }
    }

    fn configuring_replicas_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Replica configuration failing",
            common_causes: vec![
                "Replicas cannot connect to masters",
                "TLS/auth configuration mismatch between masters and replicas",
                "Masters not yet fully initialized",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check cluster nodes and roles",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check replica logs for errors",
                    format!(
                        "kubectl logs -n {namespace} {cluster_name}-3 | grep -i 'replica\\|master\\|sync\\|error' | tail -30"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check replication status on replica",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-3 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" INFO REPLICATION"
                    ),
                ),
            ],
        }
    }

    fn scaling_up_statefulset_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "StatefulSet scale-up failed",
            common_causes: vec![
                "Resource quota limit reached",
                "Admission webhook blocking the change",
                "Persistent volume provisioning failed",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check StatefulSet status",
                    format!("kubectl get statefulset {cluster_name} -n {namespace} -o wide"),
                ),
                DiagnosticCommand::new(
                    "Check StatefulSet events",
                    format!("kubectl describe statefulset {cluster_name} -n {namespace}"),
                ),
                DiagnosticCommand::new(
                    "Check resource quotas",
                    format!("kubectl get resourcequota -n {namespace} -o yaml"),
                ),
                DiagnosticCommand::new(
                    "Check PVC status",
                    format!(
                        "kubectl get pvc -n {namespace} -l app.kubernetes.io/instance={cluster_name}"
                    ),
                ),
            ],
        }
    }

    fn waiting_for_new_pods_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "New pods not becoming ready during scale-up",
            common_causes: vec![
                "Image pull failed for new pods",
                "New pods crashing on startup",
                "PVC binding issues for new pods",
                "Resource constraints preventing scheduling",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check all pod status",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} -o wide"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check pending/not-ready pods",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} --field-selector=status.phase!=Running"
                    ),
                ),
                DiagnosticCommand::new(
                    "Describe non-ready pods",
                    format!(
                        "kubectl describe pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} --field-selector=status.phase!=Running"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check recent events",
                    format!(
                        "kubectl get events -n {namespace} --sort-by='.lastTimestamp' | grep -i {cluster_name} | tail -15"
                    ),
                ),
            ],
        }
    }

    fn adding_nodes_to_cluster_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "CLUSTER MEET failing for new nodes",
            common_causes: vec![
                "Network connectivity from existing nodes to new nodes",
                "TLS certificate not yet ready for new pods",
                "DNS not resolving new pod names",
                "Firewall or NetworkPolicy blocking traffic",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check cluster nodes (existing view)",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check new pod TLS certificate",
                    format!("kubectl exec -n {namespace} {cluster_name}-3 -- ls -la /tls/"),
                ),
                DiagnosticCommand::new(
                    "Test connectivity from existing to new pod",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- timeout 5 sh -c '</dev/tcp/{cluster_name}-3.{cluster_name}-headless.{namespace}.svc.cluster.local/6379' && echo 'Connection OK' || echo 'Connection FAILED'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check new pod logs",
                    format!("kubectl logs -n {namespace} {cluster_name}-3 | tail -50"),
                ),
                DiagnosticCommand::new(
                    "Check NetworkPolicies",
                    format!("kubectl get networkpolicy -n {namespace}"),
                ),
            ],
        }
    }

    fn rebalancing_slots_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Slot migration stalled",
            common_causes: vec![
                "Very large keys blocking migration",
                "Network issues between source and target nodes",
                "Target node became unreachable",
                "High memory pressure on nodes",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check migrating/importing slots",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES | grep -E 'migrating|importing'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check cluster state",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER INFO"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check memory usage",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" INFO MEMORY | grep -E 'used_memory|maxmemory'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Find large keys (may take time)",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" --bigkeys --sample 1000"
                    ),
                ),
            ],
        }
    }

    fn configuring_new_replicas_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "New replica configuration failing",
            common_causes: vec![
                "New replicas cannot connect to assigned masters",
                "Replication sync not completing",
                "TLS/auth issues between new replicas and masters",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check cluster nodes and roles",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check replication status on new nodes",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- sh -c 'for pod in $(kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} -o name); do echo \"=== $pod ===\"; kubectl exec -n {namespace} $pod -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" INFO REPLICATION | head -10; done'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check new replica logs",
                    format!(
                        "kubectl logs -n {namespace} -l app.kubernetes.io/instance={cluster_name} --tail=30 | grep -i 'replica\\|sync\\|error'"
                    ),
                ),
            ],
        }
    }

    fn evacuating_slots_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Slot evacuation stalled during scale-down",
            common_causes: vec![
                "Very large keys blocking migration",
                "Target nodes unreachable or full",
                "Network issues during slot transfer",
                "Slots stuck in migrating/importing state",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check migrating/importing slots",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES | grep -E 'migrating|importing'"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check slot distribution",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER SLOTS"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check cluster state",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER INFO"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check nodes being evacuated (high ordinals)",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} -o wide"
                    ),
                ),
                DiagnosticCommand::new(
                    "Clear stuck slots (CAUTION: manual recovery)",
                    format!(
                        "# To clear a stuck slot, run on BOTH source and target nodes:\n# kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER SETSLOT <slot_number> STABLE"
                    ),
                ),
            ],
        }
    }

    fn removing_nodes_from_cluster_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "CLUSTER FORGET failing",
            common_causes: vec![
                "Nodes being removed are unreachable",
                "Not all nodes received the FORGET command",
                "Cluster consensus issues",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check cluster nodes",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check for orphan nodes (nodes without matching pods)",
                    format!(
                        "# Compare pod count to cluster node count:\nkubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} --no-headers | wc -l"
                    ),
                ),
                DiagnosticCommand::new(
                    "Manually forget a node (CAUTION)",
                    format!(
                        "# Get node ID from CLUSTER NODES, then run on ALL other nodes:\n# kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER FORGET <node-id>"
                    ),
                ),
            ],
        }
    }

    fn scaling_down_statefulset_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "StatefulSet scale-down stalled",
            common_causes: vec![
                "PodDisruptionBudget blocking pod deletion",
                "Finalizers preventing pod termination",
                "Pod stuck in Terminating state",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check StatefulSet status",
                    format!("kubectl get statefulset {cluster_name} -n {namespace} -o wide"),
                ),
                DiagnosticCommand::new(
                    "Check PodDisruptionBudget",
                    format!("kubectl get pdb -n {namespace} {cluster_name}-pdb -o yaml"),
                ),
                DiagnosticCommand::new(
                    "Check terminating pods",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} --field-selector=status.phase=Running -o wide"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check pod finalizers",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} -o jsonpath='{{range .items[*]}}{{.metadata.name}}: {{.metadata.finalizers}}{{\"\\n\"}}{{end}}'"
                    ),
                ),
            ],
        }
    }

    fn verifying_cluster_health_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Cluster health verification stuck",
            common_causes: vec![
                "Cluster state not 'ok'",
                "Slot coverage incomplete",
                "Epoch mismatch between nodes",
                "Some nodes not recognizing cluster topology",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check cluster info",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER INFO"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check cluster nodes on each pod",
                    format!(
                        "for i in 0 1 2; do echo \"=== {cluster_name}-$i ===\"; kubectl exec -n {namespace} {cluster_name}-$i -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES | head -10; done"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check slot coverage",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER SLOTS"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check for failed nodes",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES | grep -E 'fail|handshake'"
                    ),
                ),
            ],
        }
    }

    fn degraded_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Cluster in degraded state - cannot recover automatically",
            common_causes: vec![
                "One or more pods not running",
                "Network partition between nodes",
                "Data inconsistency between nodes",
                "Persistent storage issues",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check pod status",
                    format!(
                        "kubectl get pods -n {namespace} -l app.kubernetes.io/instance={cluster_name} -o wide"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check cluster state",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER INFO"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check cluster nodes for failures",
                    format!(
                        "kubectl exec -n {namespace} {cluster_name}-0 -- valkey-cli --tls --cert /tls/tls.crt --key /tls/tls.key --cacert /tls/ca.crt -a \"$VALKEY_PASSWORD\" CLUSTER NODES"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check operator logs",
                    format!(
                        "kubectl logs -n valkey-operator-system -l app.kubernetes.io/name=valkey-operator --tail=100 | grep -i {cluster_name}"
                    ),
                ),
                DiagnosticCommand::new(
                    "Check ValkeyCluster status",
                    format!("kubectl get valkeycluster {cluster_name} -n {namespace} -o yaml"),
                ),
            ],
        }
    }

    fn unexpected_phase(namespace: &str, cluster_name: &str) -> Self {
        Self {
            summary: "Unexpected stuck detection in this phase",
            common_causes: vec![
                "Operator bug - stuck detection should not trigger in this phase",
                "Status corruption",
            ],
            commands: vec![
                DiagnosticCommand::new(
                    "Check ValkeyCluster status",
                    format!("kubectl get valkeycluster {cluster_name} -n {namespace} -o yaml"),
                ),
                DiagnosticCommand::new(
                    "Check operator logs",
                    format!(
                        "kubectl logs -n valkey-operator-system -l app.kubernetes.io/name=valkey-operator --tail=100 | grep -i {cluster_name}"
                    ),
                ),
            ],
        }
    }
}

impl fmt::Display for DiagnosticHint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Issue: {}", self.summary)?;
        writeln!(f)?;
        writeln!(f, "Common causes:")?;
        for cause in &self.common_causes {
            writeln!(f, "  • {cause}")?;
        }
        writeln!(f)?;
        writeln!(f, "Diagnostic commands:")?;
        for (i, cmd) in self.commands.iter().enumerate() {
            writeln!(f, "  {}. {}", i + 1, cmd.description)?;
            writeln!(f, "     $ {}", cmd.command)?;
        }
        Ok(())
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

    #[test]
    fn test_hint_contains_namespace_and_cluster() {
        let hint =
            DiagnosticHint::for_phase(ClusterPhase::WaitingForPods, "my-namespace", "my-cluster");

        // Check that commands contain the actual namespace and cluster name
        for cmd in &hint.commands {
            let has_namespace = cmd.command.contains("my-namespace");
            let has_cluster = cmd.command.contains("my-cluster");
            // At least one of them should be present (some commands like storage class check don't need them)
            assert!(
                has_namespace || has_cluster || !cmd.command.contains("-n"),
                "Command should contain namespace or cluster name: {}",
                cmd.command
            );
        }
    }

    #[test]
    fn test_all_phases_have_hints() {
        let phases = vec![
            ClusterPhase::Creating,
            ClusterPhase::WaitingForPods,
            ClusterPhase::InitializingCluster,
            ClusterPhase::AssigningSlots,
            ClusterPhase::ConfiguringReplicas,
            ClusterPhase::ScalingUpStatefulSet,
            ClusterPhase::WaitingForNewPods,
            ClusterPhase::AddingNodesToCluster,
            ClusterPhase::RebalancingSlots,
            ClusterPhase::ConfiguringNewReplicas,
            ClusterPhase::EvacuatingSlots,
            ClusterPhase::RemovingNodesFromCluster,
            ClusterPhase::ScalingDownStatefulSet,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Degraded,
            ClusterPhase::Running,
            ClusterPhase::Failed,
            ClusterPhase::Pending,
            ClusterPhase::Deleting,
        ];

        for phase in phases {
            let hint = DiagnosticHint::for_phase(phase, "test-ns", "test-cluster");
            assert!(
                !hint.summary.is_empty(),
                "Phase {:?} has empty summary",
                phase
            );
            assert!(
                !hint.commands.is_empty(),
                "Phase {:?} has no commands",
                phase
            );
        }
    }

    #[test]
    fn test_display_format() {
        let hint = DiagnosticHint::for_phase(ClusterPhase::WaitingForPods, "default", "my-valkey");

        let output = hint.to_string();
        assert!(output.contains("Issue:"));
        assert!(output.contains("Common causes:"));
        assert!(output.contains("Diagnostic commands:"));
        assert!(output.contains("kubectl"));
    }
}
