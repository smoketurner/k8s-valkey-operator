//! ValkeyCluster Custom Resource Definition.
//!
//! Defines the ValkeyCluster CRD for deploying and managing Valkey clusters
//! on Kubernetes. Designed with secure-by-default principles: TLS and auth
//! are required, not optional.

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// ValkeyCluster is a custom resource for deploying Valkey clusters.
///
/// Example:
/// ```yaml
/// apiVersion: valkeyoperator.smoketurner.com/v1alpha1
/// kind: ValkeyCluster
/// metadata:
///   name: my-cluster
/// spec:
///   masters: 3
///   replicasPerMaster: 1
///   tls:
///     issuerRef:
///       name: ca-issuer
///       kind: ClusterIssuer
///   auth:
///     secretRef:
///       name: valkey-auth
///       key: password
/// ```
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "valkeyoperator.smoketurner.com",
    version = "v1alpha1",
    kind = "ValkeyCluster",
    plural = "valkeyclusters",
    shortname = "vc",
    status = "ValkeyClusterStatus",
    namespaced,
    // Print columns for kubectl get
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Masters", "type":"integer", "jsonPath":".spec.masters"}"#,
    printcolumn = r#"{"name":"Replicas", "type":"integer", "jsonPath":".spec.replicasPerMaster"}"#,
    printcolumn = r#"{"name":"Ready", "type":"string", "jsonPath":".status.readyNodes"}"#,
    printcolumn = r#"{"name":"Slots", "type":"string", "jsonPath":".status.assignedSlots"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct ValkeyClusterSpec {
    // === Topology (minimal config) ===
    /// Number of master nodes (minimum 3, default 3).
    /// Each master is responsible for a portion of the 16384 hash slots.
    #[serde(default = "default_masters")]
    pub masters: i32,

    /// Number of replicas per master (default 1).
    /// Replicas provide high availability via automatic failover.
    #[serde(default = "default_replicas_per_master")]
    pub replicas_per_master: i32,

    // === Image ===
    /// Valkey container image configuration.
    #[serde(default)]
    pub image: ImageSpec,

    // === TLS - REQUIRED (secure-by-default) ===
    /// TLS configuration using cert-manager.
    /// REQUIRED: Valkey cluster communication must be encrypted.
    pub tls: TlsSpec,

    // === Authentication - REQUIRED (secure-by-default) ===
    /// Authentication configuration.
    /// REQUIRED: Cluster must have authentication enabled.
    pub auth: AuthSpec,

    // === Storage ===
    /// Persistence configuration for data durability.
    #[serde(default)]
    pub persistence: PersistenceSpec,

    // === Resources ===
    /// Resource requests and limits for Valkey pods.
    #[serde(default)]
    pub resources: ResourceRequirementsSpec,

    // === Affinity & Scheduling ===
    /// Pod scheduling constraints.
    #[serde(default)]
    pub scheduling: SchedulingSpec,

    // === Custom Labels ===
    /// Additional labels to apply to all managed resources.
    /// Useful for cost allocation and organization.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,

    // === Custom Annotations ===
    /// Additional annotations to apply to all managed resources.
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

impl Default for ValkeyClusterSpec {
    fn default() -> Self {
        Self {
            masters: default_masters(),
            replicas_per_master: default_replicas_per_master(),
            image: ImageSpec::default(),
            tls: TlsSpec::default(),
            auth: AuthSpec::default(),
            persistence: PersistenceSpec::default(),
            resources: ResourceRequirementsSpec::default(),
            scheduling: SchedulingSpec::default(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        }
    }
}

fn default_masters() -> i32 {
    3
}

fn default_replicas_per_master() -> i32 {
    1
}

/// Container image specification.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImageSpec {
    /// Container image repository (default: valkey/valkey).
    #[serde(default = "default_image_repository")]
    pub repository: String,

    /// Image tag (default: 9).
    #[serde(default = "default_image_tag")]
    pub tag: String,

    /// Image pull policy (default: IfNotPresent).
    #[serde(default = "default_image_pull_policy")]
    pub pull_policy: String,

    /// Image pull secrets.
    #[serde(default)]
    pub pull_secrets: Vec<String>,
}

impl Default for ImageSpec {
    fn default() -> Self {
        Self {
            repository: default_image_repository(),
            tag: default_image_tag(),
            pull_policy: default_image_pull_policy(),
            pull_secrets: Vec::new(),
        }
    }
}

fn default_image_repository() -> String {
    "valkey/valkey".to_string()
}

fn default_image_tag() -> String {
    "9-alpine".to_string()
}

fn default_image_pull_policy() -> String {
    "IfNotPresent".to_string()
}

/// TLS configuration for secure cluster communication.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TlsSpec {
    /// cert-manager Issuer or ClusterIssuer reference.
    pub issuer_ref: IssuerRef,

    /// Duration of the certificate (default: 2160h = 90 days).
    #[serde(default = "default_cert_duration")]
    pub duration: String,

    /// Time before expiry to renew the certificate (default: 360h = 15 days).
    #[serde(default = "default_renew_before")]
    pub renew_before: String,
}

fn default_cert_duration() -> String {
    "2160h".to_string()
}

fn default_renew_before() -> String {
    "360h".to_string()
}

/// Reference to a cert-manager Issuer or ClusterIssuer.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct IssuerRef {
    /// Name of the Issuer or ClusterIssuer.
    pub name: String,

    /// Kind of the issuer (Issuer or ClusterIssuer, default: ClusterIssuer).
    #[serde(default = "default_issuer_kind")]
    pub kind: String,

    /// Group of the issuer (default: cert-manager.io).
    #[serde(default = "default_issuer_group")]
    pub group: String,
}

fn default_issuer_kind() -> String {
    "ClusterIssuer".to_string()
}

fn default_issuer_group() -> String {
    "cert-manager.io".to_string()
}

/// Authentication configuration.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AuthSpec {
    /// Reference to a Secret containing the password.
    pub secret_ref: SecretKeyRef,
}

/// Reference to a key within a Secret.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Name of the Secret.
    pub name: String,

    /// Key within the Secret containing the password (default: password).
    #[serde(default = "default_password_key")]
    pub key: String,
}

fn default_password_key() -> String {
    "password".to_string()
}

/// Persistence configuration for data durability.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PersistenceSpec {
    /// Enable persistence (default: true).
    #[serde(default = "default_persistence_enabled")]
    pub enabled: bool,

    /// Storage class name for PersistentVolumeClaims.
    /// If not set, uses cluster default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,

    /// Size of the PersistentVolumeClaim (default: 10Gi).
    #[serde(default = "default_storage_size")]
    pub size: String,

    /// AOF (Append Only File) persistence settings.
    #[serde(default)]
    pub aof: AofSpec,

    /// RDB (Redis Database) snapshot settings.
    #[serde(default)]
    pub rdb: RdbSpec,
}

impl Default for PersistenceSpec {
    fn default() -> Self {
        Self {
            enabled: default_persistence_enabled(),
            storage_class_name: None,
            size: default_storage_size(),
            aof: AofSpec::default(),
            rdb: RdbSpec::default(),
        }
    }
}

fn default_persistence_enabled() -> bool {
    true
}

fn default_storage_size() -> String {
    "10Gi".to_string()
}

/// AOF (Append Only File) persistence settings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AofSpec {
    /// Enable AOF persistence (default: true).
    #[serde(default = "default_aof_enabled")]
    pub enabled: bool,

    /// AOF fsync policy (default: everysec).
    /// Options: always, everysec, no
    #[serde(default = "default_aof_fsync")]
    pub fsync: String,
}

impl Default for AofSpec {
    fn default() -> Self {
        Self {
            enabled: default_aof_enabled(),
            fsync: default_aof_fsync(),
        }
    }
}

fn default_aof_enabled() -> bool {
    true
}

fn default_aof_fsync() -> String {
    "everysec".to_string()
}

/// RDB (Redis Database) snapshot settings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RdbSpec {
    /// Enable RDB snapshots (default: true).
    #[serde(default = "default_rdb_enabled")]
    pub enabled: bool,

    /// RDB save rules in "seconds changes" format.
    /// Default: ["900 1", "300 10", "60 10000"]
    #[serde(default = "default_rdb_save_rules")]
    pub save_rules: Vec<String>,
}

impl Default for RdbSpec {
    fn default() -> Self {
        Self {
            enabled: default_rdb_enabled(),
            save_rules: default_rdb_save_rules(),
        }
    }
}

fn default_rdb_enabled() -> bool {
    true
}

fn default_rdb_save_rules() -> Vec<String> {
    vec![
        "900 1".to_string(),    // Save after 900s if 1 key changed
        "300 10".to_string(),   // Save after 300s if 10 keys changed
        "60 10000".to_string(), // Save after 60s if 10000 keys changed
    ]
}

/// Resource requests and limits for Valkey pods.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirementsSpec {
    /// CPU and memory requests.
    #[serde(default)]
    pub requests: ResourceSpec,

    /// CPU and memory limits.
    #[serde(default)]
    pub limits: ResourceLimitsSpec,
}

impl Default for ResourceRequirementsSpec {
    fn default() -> Self {
        Self {
            requests: ResourceSpec::default(),
            limits: ResourceLimitsSpec::default(),
        }
    }
}

/// Resource requests specification.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpec {
    /// CPU request (default: 100m).
    #[serde(default = "default_cpu_request")]
    pub cpu: String,

    /// Memory request (default: 256Mi).
    #[serde(default = "default_memory_request")]
    pub memory: String,
}

impl Default for ResourceSpec {
    fn default() -> Self {
        Self {
            cpu: default_cpu_request(),
            memory: default_memory_request(),
        }
    }
}

fn default_cpu_request() -> String {
    "100m".to_string()
}

fn default_memory_request() -> String {
    "256Mi".to_string()
}

/// Resource limits specification.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimitsSpec {
    /// CPU limit (default: 1).
    #[serde(default = "default_cpu_limit")]
    pub cpu: String,

    /// Memory limit (default: 1Gi).
    #[serde(default = "default_memory_limit")]
    pub memory: String,
}

impl Default for ResourceLimitsSpec {
    fn default() -> Self {
        Self {
            cpu: default_cpu_limit(),
            memory: default_memory_limit(),
        }
    }
}

fn default_cpu_limit() -> String {
    "1".to_string()
}

fn default_memory_limit() -> String {
    "1Gi".to_string()
}

/// Pod scheduling configuration.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SchedulingSpec {
    /// Spread masters across availability zones (default: true).
    /// Enables pod anti-affinity to prevent multiple masters on same node.
    #[serde(default = "default_spread_masters")]
    pub spread_masters: bool,

    /// Topology key for master spreading (default: kubernetes.io/hostname).
    #[serde(default = "default_topology_key")]
    pub topology_key: String,

    /// Node selector for pod placement.
    #[serde(default)]
    pub node_selector: BTreeMap<String, String>,

    /// Tolerations for pod scheduling.
    #[serde(default)]
    pub tolerations: Vec<Toleration>,
}

impl Default for SchedulingSpec {
    fn default() -> Self {
        Self {
            spread_masters: default_spread_masters(),
            topology_key: default_topology_key(),
            node_selector: BTreeMap::new(),
            tolerations: Vec::new(),
        }
    }
}

fn default_spread_masters() -> bool {
    true
}

fn default_topology_key() -> String {
    "kubernetes.io/hostname".to_string()
}

/// Toleration for pod scheduling.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    /// Toleration key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Toleration operator (Equal or Exists).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,

    /// Toleration value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,

    /// Toleration effect (NoSchedule, PreferNoSchedule, or NoExecute).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,

    /// Toleration seconds (for NoExecute effect).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

/// Status of a ValkeyCluster.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ValkeyClusterStatus {
    /// Current phase of the cluster lifecycle.
    #[serde(default)]
    pub phase: ClusterPhase,

    /// Ready nodes in "ready/total" format (e.g., "6/6").
    #[serde(default)]
    pub ready_nodes: String,

    /// Number of ready master nodes.
    #[serde(default)]
    pub ready_masters: i32,

    /// Number of ready replica nodes.
    #[serde(default)]
    pub ready_replicas: i32,

    /// Assigned hash slots in "assigned/total" format (e.g., "16384/16384").
    #[serde(default)]
    pub assigned_slots: String,

    /// The generation most recently observed by the controller.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,

    /// Conditions describing the current state.
    #[serde(default)]
    pub conditions: Vec<Condition>,

    /// Current cluster topology.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topology: Option<ClusterTopology>,

    /// Detected Valkey server version.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valkey_version: Option<String>,

    /// Connection endpoint for clients (e.g., "valkey://my-cluster.ns.svc:6379").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_endpoint: Option<String>,

    /// Name of the Secret containing connection info and credentials.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_secret: Option<String>,

    /// Name of the Secret containing TLS CA certificate for clients.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_secret: Option<String>,
}

/// ClusterPhase represents the current lifecycle phase of a ValkeyCluster.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
pub enum ClusterPhase {
    /// Initial state, waiting for reconciliation.
    #[default]
    Pending,
    /// Kubernetes resources (StatefulSet, Services) are being created.
    Creating,
    /// Cluster nodes are running, performing CLUSTER MEET and slot assignment.
    Initializing,
    /// Hash slots are being assigned to master nodes.
    AssigningSlots,
    /// Cluster is fully operational.
    Running,
    /// Cluster configuration is being updated.
    Updating,
    /// Hash slots are being migrated (scaling operation).
    Resharding,
    /// Cluster is operational but degraded (some nodes unavailable).
    Degraded,
    /// Cluster has failed and requires intervention.
    Failed,
    /// Cluster is being deleted.
    Deleting,
}

impl std::fmt::Display for ClusterPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterPhase::Pending => write!(f, "Pending"),
            ClusterPhase::Creating => write!(f, "Creating"),
            ClusterPhase::Initializing => write!(f, "Initializing"),
            ClusterPhase::AssigningSlots => write!(f, "AssigningSlots"),
            ClusterPhase::Running => write!(f, "Running"),
            ClusterPhase::Updating => write!(f, "Updating"),
            ClusterPhase::Resharding => write!(f, "Resharding"),
            ClusterPhase::Degraded => write!(f, "Degraded"),
            ClusterPhase::Failed => write!(f, "Failed"),
            ClusterPhase::Deleting => write!(f, "Deleting"),
        }
    }
}

/// Condition describes the state of a cluster at a certain point.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition.
    pub r#type: String,
    /// Status of the condition ("True", "False", "Unknown").
    pub status: String,
    /// Machine-readable reason for the condition's last transition.
    pub reason: String,
    /// Human-readable message indicating details about last transition.
    pub message: String,
    /// Last time the condition transitioned from one status to another.
    pub last_transition_time: String,
    /// The generation of the resource this condition was observed for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

impl Condition {
    /// Create a new condition.
    pub fn new(
        condition_type: &str,
        status: bool,
        reason: &str,
        message: &str,
        generation: Option<i64>,
    ) -> Self {
        Self {
            r#type: condition_type.to_string(),
            status: if status {
                "True".to_string()
            } else {
                "False".to_string()
            },
            reason: reason.to_string(),
            message: message.to_string(),
            last_transition_time: jiff::Timestamp::now().to_string(),
            observed_generation: generation,
        }
    }

    /// Create a "Ready" condition.
    pub fn ready(ready: bool, reason: &str, message: &str, generation: Option<i64>) -> Self {
        Self::new("Ready", ready, reason, message, generation)
    }

    /// Create a "Progressing" condition.
    pub fn progressing(
        progressing: bool,
        reason: &str,
        message: &str,
        generation: Option<i64>,
    ) -> Self {
        Self::new("Progressing", progressing, reason, message, generation)
    }

    /// Create a "Degraded" condition.
    pub fn degraded(degraded: bool, reason: &str, message: &str, generation: Option<i64>) -> Self {
        Self::new("Degraded", degraded, reason, message, generation)
    }
}

/// Types of conditions for ValkeyCluster.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, JsonSchema)]
pub enum ConditionType {
    /// Cluster is fully operational and healthy.
    Ready,
    /// All 16384 hash slots have been assigned to nodes.
    SlotsAssigned,
    /// No slot migration is in progress.
    SlotsStable,
    /// TLS certificates are valid and not expiring soon.
    TLSReady,
    /// All nodes are reachable and cluster is consistent.
    ClusterHealthy,
    /// RDB/AOF persistence is functioning correctly.
    PersistenceHealthy,
    /// All replicas are synchronized with their masters.
    ReplicasInSync,
    /// Reconciliation is in progress.
    Progressing,
    /// Cluster is in a degraded state.
    Degraded,
}

impl std::fmt::Display for ConditionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConditionType::Ready => write!(f, "Ready"),
            ConditionType::SlotsAssigned => write!(f, "SlotsAssigned"),
            ConditionType::SlotsStable => write!(f, "SlotsStable"),
            ConditionType::TLSReady => write!(f, "TLSReady"),
            ConditionType::ClusterHealthy => write!(f, "ClusterHealthy"),
            ConditionType::PersistenceHealthy => write!(f, "PersistenceHealthy"),
            ConditionType::ReplicasInSync => write!(f, "ReplicasInSync"),
            ConditionType::Progressing => write!(f, "Progressing"),
            ConditionType::Degraded => write!(f, "Degraded"),
        }
    }
}

/// Cluster topology information.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterTopology {
    /// List of master nodes with their slot ranges.
    pub masters: Vec<MasterNode>,
}

/// Information about a master node and its replicas.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MasterNode {
    /// Node ID (Valkey cluster node identifier).
    pub node_id: String,
    /// Pod name.
    pub pod_name: String,
    /// Hash slot ranges owned by this master (e.g., ["0-5460"]).
    pub slot_ranges: Vec<String>,
    /// Replica nodes for this master.
    pub replicas: Vec<ReplicaNode>,
}

/// Information about a replica node.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaNode {
    /// Node ID (Valkey cluster node identifier).
    pub node_id: String,
    /// Pod name.
    pub pod_name: String,
    /// Replication lag in bytes (0 means fully synchronized).
    pub replication_lag: i64,
}

/// Total number of hash slots in a Valkey cluster.
pub const TOTAL_HASH_SLOTS: i32 = 16384;

/// Default client port for Valkey.
pub const DEFAULT_CLIENT_PORT: i32 = 6379;

/// Cluster bus port offset from client port (client port + 10000).
pub const CLUSTER_BUS_PORT_OFFSET: i32 = 10000;

/// Calculate the total number of pods for a cluster.
pub fn total_pods(masters: i32, replicas_per_master: i32) -> i32 {
    masters + (masters * replicas_per_master)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phase_display() {
        assert_eq!(ClusterPhase::Pending.to_string(), "Pending");
        assert_eq!(ClusterPhase::Creating.to_string(), "Creating");
        assert_eq!(ClusterPhase::Initializing.to_string(), "Initializing");
        assert_eq!(ClusterPhase::AssigningSlots.to_string(), "AssigningSlots");
        assert_eq!(ClusterPhase::Running.to_string(), "Running");
        assert_eq!(ClusterPhase::Updating.to_string(), "Updating");
        assert_eq!(ClusterPhase::Resharding.to_string(), "Resharding");
        assert_eq!(ClusterPhase::Degraded.to_string(), "Degraded");
        assert_eq!(ClusterPhase::Failed.to_string(), "Failed");
        assert_eq!(ClusterPhase::Deleting.to_string(), "Deleting");
    }

    #[test]
    fn test_phase_default() {
        assert_eq!(ClusterPhase::default(), ClusterPhase::Pending);
    }

    #[test]
    fn test_total_pods() {
        // 3 masters + 3 replicas = 6 total
        assert_eq!(total_pods(3, 1), 6);
        // 3 masters + 6 replicas = 9 total
        assert_eq!(total_pods(3, 2), 9);
        // 6 masters + 6 replicas = 12 total
        assert_eq!(total_pods(6, 1), 12);
    }

    #[test]
    fn test_default_spec() {
        let spec = ValkeyClusterSpec::default();
        assert_eq!(spec.masters, 3);
        assert_eq!(spec.replicas_per_master, 1);
        assert_eq!(spec.image.repository, "valkey/valkey");
        assert_eq!(spec.image.tag, "9-alpine");
        assert!(spec.persistence.enabled);
        assert_eq!(spec.persistence.size, "10Gi");
        assert!(spec.persistence.aof.enabled);
        assert!(spec.persistence.rdb.enabled);
        assert!(spec.scheduling.spread_masters);
    }

    #[test]
    fn test_spec_serialization() {
        let spec = ValkeyClusterSpec {
            masters: 3,
            replicas_per_master: 1,
            tls: TlsSpec {
                issuer_ref: IssuerRef {
                    name: "ca-issuer".to_string(),
                    kind: "ClusterIssuer".to_string(),
                    group: "cert-manager.io".to_string(),
                },
                ..Default::default()
            },
            auth: AuthSpec {
                secret_ref: SecretKeyRef {
                    name: "valkey-auth".to_string(),
                    key: "password".to_string(),
                },
            },
            ..Default::default()
        };

        let json = serde_json::to_string(&spec).expect("serialization should succeed");
        let parsed: ValkeyClusterSpec =
            serde_json::from_str(&json).expect("deserialization should succeed");

        assert_eq!(parsed.masters, 3);
        assert_eq!(parsed.replicas_per_master, 1);
        assert_eq!(parsed.tls.issuer_ref.name, "ca-issuer");
        assert_eq!(parsed.auth.secret_ref.name, "valkey-auth");
    }

    #[test]
    fn test_condition_type_display() {
        assert_eq!(ConditionType::Ready.to_string(), "Ready");
        assert_eq!(ConditionType::SlotsAssigned.to_string(), "SlotsAssigned");
        assert_eq!(ConditionType::ClusterHealthy.to_string(), "ClusterHealthy");
    }

    #[test]
    fn test_condition_ready() {
        let condition = Condition::ready(true, "AllReady", "All components ready", Some(1));
        assert_eq!(condition.r#type, "Ready");
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "AllReady");
        assert_eq!(condition.message, "All components ready");
        assert_eq!(condition.observed_generation, Some(1));
    }

    #[test]
    fn test_condition_not_ready() {
        let condition = Condition::ready(false, "NotReady", "Components starting", None);
        assert_eq!(condition.status, "False");
    }

    #[test]
    fn test_condition_progressing() {
        let condition = Condition::progressing(true, "Reconciling", "Updating resources", Some(2));
        assert_eq!(condition.r#type, "Progressing");
        assert_eq!(condition.status, "True");
    }

    #[test]
    fn test_condition_degraded() {
        let condition = Condition::degraded(true, "PodFailed", "Pod in crash loop", Some(3));
        assert_eq!(condition.r#type, "Degraded");
        assert_eq!(condition.status, "True");
    }
}
