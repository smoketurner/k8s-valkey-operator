//! Custom Resource Definitions (CRDs) for my-operator.
//!
//! This module defines the `MyResource` CRD using kube-rs derive macros.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// MyResource is a custom resource managed by this operator.
///
/// Example:
/// ```yaml
/// apiVersion: myoperator.example.com/v1alpha1
/// kind: MyResource
/// metadata:
///   name: example
/// spec:
///   replicas: 3
///   message: "Hello, World!"
/// ```
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "myoperator.example.com",
    version = "v1alpha1",
    kind = "MyResource",
    plural = "myresources",
    shortname = "mr",
    status = "MyResourceStatus",
    namespaced,
    // Print columns for kubectl get
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Replicas", "type":"integer", "jsonPath":".spec.replicas"}"#,
    printcolumn = r#"{"name":"Ready", "type":"integer", "jsonPath":".status.readyReplicas"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MyResourceSpec {
    /// Number of replicas for the managed deployment
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// A message to be stored in the managed ConfigMap
    #[serde(default)]
    pub message: String,

    /// Custom labels to apply to managed resources
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
}

fn default_replicas() -> i32 {
    1
}

/// Status of a MyResource
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MyResourceStatus {
    /// Current phase of the resource
    #[serde(default)]
    pub phase: Phase,

    /// Number of ready replicas
    #[serde(default)]
    pub ready_replicas: i32,

    /// The generation most recently observed by the controller
    #[serde(default)]
    pub observed_generation: Option<i64>,

    /// Conditions describing the current state
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Phase represents the current lifecycle phase of a MyResource
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Deserialize, Serialize, JsonSchema)]
pub enum Phase {
    /// Resource is being created
    #[default]
    Pending,
    /// Resource is being created, waiting for dependencies
    Creating,
    /// Resource is ready and operational
    Running,
    /// Resource is being updated
    Updating,
    /// Resource is in a degraded state but operational
    Degraded,
    /// Resource has failed and requires intervention
    Failed,
    /// Resource is being deleted
    Deleting,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Phase::Pending => write!(f, "Pending"),
            Phase::Creating => write!(f, "Creating"),
            Phase::Running => write!(f, "Running"),
            Phase::Updating => write!(f, "Updating"),
            Phase::Degraded => write!(f, "Degraded"),
            Phase::Failed => write!(f, "Failed"),
            Phase::Deleting => write!(f, "Deleting"),
        }
    }
}

/// Condition describes the state of a resource at a certain point
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type of condition (e.g., "Ready", "Progressing", "Degraded")
    pub r#type: String,
    /// Status of the condition ("True", "False", "Unknown")
    pub status: String,
    /// Machine-readable reason for the condition's last transition
    pub reason: String,
    /// Human-readable message indicating details about last transition
    pub message: String,
    /// Last time the condition transitioned from one status to another
    pub last_transition_time: String,
    /// The generation of the resource this condition was observed for
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

impl Condition {
    /// Create a new condition
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

    /// Create a "Ready" condition
    pub fn ready(ready: bool, reason: &str, message: &str, generation: Option<i64>) -> Self {
        Self::new("Ready", ready, reason, message, generation)
    }

    /// Create a "Progressing" condition
    pub fn progressing(
        progressing: bool,
        reason: &str,
        message: &str,
        generation: Option<i64>,
    ) -> Self {
        Self::new("Progressing", progressing, reason, message, generation)
    }

    /// Create a "Degraded" condition
    pub fn degraded(degraded: bool, reason: &str, message: &str, generation: Option<i64>) -> Self {
        Self::new("Degraded", degraded, reason, message, generation)
    }
}

/// Standard condition types
pub mod condition_types {
    /// Ready indicates the resource is fully operational
    pub const READY: &str = "Ready";
    /// Progressing indicates the resource is being reconciled
    pub const PROGRESSING: &str = "Progressing";
    /// Degraded indicates the resource is operational but not at full capacity
    pub const DEGRADED: &str = "Degraded";
    /// ConfigurationValid indicates the spec passed validation
    pub const CONFIGURATION_VALID: &str = "ConfigurationValid";
}
