//! Helpers for constructing Kubernetes-standard `metav1.Condition` values.
//!
//! The condition type itself comes from `k8s_openapi`. These constructors set
//! the `type_` and a True/False/Unknown `status`, and stamp
//! `last_transition_time` from `jiff::Timestamp::now()`. Callers that want
//! Kubernetes-conformant timestamp preservation (only update on actual
//! transitions) should run the result through `merge_conditions`.
//!
//! The free-function form is used (rather than inherent methods on
//! `k8s_openapi::Condition`) because the type lives in another crate and Rust
//! disallows inherent impls on foreign types.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};

/// Convert a `bool` to the Kubernetes condition status string.
pub(crate) fn bool_status(b: bool) -> String {
    if b {
        "True".to_string()
    } else {
        "False".to_string()
    }
}

/// Construct a condition of the given type with True/False status.
pub fn new_condition(
    condition_type: &str,
    status: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Condition {
    Condition {
        type_: condition_type.to_string(),
        status: bool_status(status),
        reason: reason.to_string(),
        message: message.to_string(),
        last_transition_time: Time(jiff::Timestamp::now()),
        observed_generation: generation,
    }
}

/// Construct a `Ready` condition.
pub fn ready_condition(
    ready: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Condition {
    new_condition("Ready", ready, reason, message, generation)
}

/// Construct a `Progressing` condition.
pub fn progressing_condition(
    progressing: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Condition {
    new_condition("Progressing", progressing, reason, message, generation)
}

/// Construct a `Degraded` condition.
pub fn degraded_condition(
    degraded: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Condition {
    new_condition("Degraded", degraded, reason, message, generation)
}

/// Construct a tri-state condition where the status may be unknown.
///
/// `None` produces a `status: "Unknown"` condition — the Kubernetes convention
/// when the operator has no observation for this signal yet.
pub fn tri_state_condition(
    condition_type: &str,
    status: Option<bool>,
    reason: &str,
    message: &str,
    generation: Option<i64>,
) -> Condition {
    let status_str = match status {
        Some(true) => "True".to_string(),
        Some(false) => "False".to_string(),
        None => "Unknown".to_string(),
    };
    Condition {
        type_: condition_type.to_string(),
        status: status_str,
        reason: reason.to_string(),
        message: message.to_string(),
        last_transition_time: Time(jiff::Timestamp::now()),
        observed_generation: generation,
    }
}
