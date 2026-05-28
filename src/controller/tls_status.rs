//! Observed TLS readiness derived from the cert-manager `Certificate` resource.
//!
//! The operator generates a `Certificate` per cluster and reads it back during
//! reconciliation to populate the `TLSReady` status condition. The condition is
//! a tri-state observation:
//!
//! - `True`: cert-manager reports `Ready=True` and `notAfter` is far enough in
//!   the future that the certificate is not yet inside its renewal window.
//! - `False`: cert-manager reports `Ready=False`, or `notAfter` is at/below the
//!   cluster's configured `renewBefore` lead time (a leading indicator of
//!   stalled or failing renewal).
//! - `Unknown` (`None`): the `Certificate` object could not be observed, or its
//!   status has not been populated yet, or the timestamps could not be parsed.

use std::str::FromStr;

use jiff::{SignedDuration, Timestamp};
use kube::{
    Api, ResourceExt,
    api::{ApiResource, DynamicObject},
};
use tracing::debug;

use crate::{controller::context::Context, crd::ValkeyCluster, resources::certificate};

/// Outcome of the TLS readiness check, decoupled from the condition encoding.
#[derive(Clone, Debug)]
pub struct TlsObservation {
    /// Tri-state ready value: `Some(true)` healthy, `Some(false)` unhealthy,
    /// `None` unknown.
    pub ready: Option<bool>,
    /// Short, machine-readable reason for the condition.
    pub reason: &'static str,
    /// Human-readable message describing the observation.
    pub message: String,
}

impl TlsObservation {
    fn unknown(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            ready: None,
            reason,
            message: message.into(),
        }
    }

    fn ok(message: impl Into<String>) -> Self {
        Self {
            ready: Some(true),
            reason: "CertificateReady",
            message: message.into(),
        }
    }

    fn not_ready(reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            ready: Some(false),
            reason,
            message: message.into(),
        }
    }
}

/// Observe the cert-manager `Certificate` for this cluster and produce a
/// `TLSReady` observation.
///
/// Returns `Unknown` for any condition the operator cannot positively verify:
/// missing Certificate, missing status, unparseable timestamps. This matches
/// the Kubernetes convention that absent observations should be reported as
/// `Unknown` rather than guessed.
pub(crate) async fn observe(obj: &ValkeyCluster, ctx: &Context) -> TlsObservation {
    let namespace = obj.namespace().unwrap_or_else(|| "default".to_string());
    let cert_name = certificate::certificate_secret_name(obj);
    let cert_ar = ApiResource::from_gvk(&kube::api::GroupVersionKind {
        group: "cert-manager.io".to_string(),
        version: "v1".to_string(),
        kind: "Certificate".to_string(),
    });
    let cert_api: Api<DynamicObject> =
        Api::namespaced_with(ctx.client.clone(), &namespace, &cert_ar);

    let cert = match cert_api.get(&cert_name).await {
        Ok(c) => c,
        Err(e) => {
            debug!(name = %cert_name, error = %e, "TLSReady: failed to fetch Certificate");
            return TlsObservation::unknown(
                "CertificateNotObserved",
                "cert-manager Certificate could not be read.",
            );
        }
    };

    let Some(status) = cert.data.get("status") else {
        return TlsObservation::unknown(
            "CertificateStatusMissing",
            "cert-manager Certificate has no status populated yet.",
        );
    };

    let ready_status = cert_manager_ready_status(status);
    if matches!(ready_status, Some(false)) {
        let message = ready_message(status)
            .unwrap_or_else(|| "cert-manager reports Certificate Ready=False.".to_string());
        return TlsObservation::not_ready("CertificateNotReady", message);
    }

    let Some(not_after_str) = status.get("notAfter").and_then(|v| v.as_str()) else {
        return TlsObservation::unknown(
            "NotAfterMissing",
            "cert-manager Certificate has no notAfter timestamp.",
        );
    };

    let Ok(not_after) = Timestamp::from_str(not_after_str) else {
        return TlsObservation::unknown(
            "NotAfterUnparseable",
            format!("cert-manager notAfter timestamp '{not_after_str}' could not be parsed."),
        );
    };

    let Some(renew_before) = parse_duration(&obj.spec.tls.renew_before) else {
        return TlsObservation::unknown(
            "RenewBeforeUnparseable",
            format!(
                "spec.tls.renewBefore '{}' could not be parsed.",
                obj.spec.tls.renew_before
            ),
        );
    };

    let now = Timestamp::now();
    let time_to_expiry = not_after.duration_since(now);

    if time_to_expiry <= SignedDuration::ZERO {
        return TlsObservation::not_ready(
            "CertificateExpired",
            format!("Certificate expired at {not_after_str}."),
        );
    }

    if time_to_expiry <= renew_before {
        return TlsObservation::not_ready(
            "RenewalWindow",
            format!(
                "Certificate expires at {not_after_str}; within the {} renewBefore window without successful renewal.",
                obj.spec.tls.renew_before
            ),
        );
    }

    if ready_status != Some(true) {
        return TlsObservation::unknown(
            "ReadyConditionMissing",
            "cert-manager Certificate has no Ready condition yet.",
        );
    }

    TlsObservation::ok(format!("Certificate is Ready; expires at {not_after_str}."))
}

fn cert_manager_ready_status(status: &serde_json::Value) -> Option<bool> {
    let conditions = status.get("conditions")?.as_array()?;
    let ready = conditions
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"))?;
    match ready.get("status").and_then(|s| s.as_str())? {
        "True" => Some(true),
        "False" => Some(false),
        _ => None,
    }
}

fn ready_message(status: &serde_json::Value) -> Option<String> {
    let conditions = status.get("conditions")?.as_array()?;
    let ready = conditions
        .iter()
        .find(|c| c.get("type").and_then(|t| t.as_str()) == Some("Ready"))?;
    ready
        .get("message")
        .and_then(|m| m.as_str())
        .map(str::to_string)
}

/// Parse a Go-style duration string ("360h", "1h30m", "45s") into a
/// `SignedDuration`. Accepts unit suffixes `h`, `m`, `s`. Returns `None` for
/// any input that is not a sequence of `<integer><unit>` pairs.
///
/// We control the format used when generating the cert-manager Certificate
/// (it comes from `ValkeyClusterSpec::tls::renew_before`, default `360h`), so
/// the parser intentionally rejects unknown units instead of silently
/// coercing.
fn parse_duration(s: &str) -> Option<SignedDuration> {
    if s.is_empty() {
        return None;
    }
    let mut total_secs: i64 = 0;
    let mut digits = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            continue;
        }
        if digits.is_empty() {
            return None;
        }
        let value: i64 = digits.parse().ok()?;
        digits.clear();
        let unit_secs: i64 = match ch {
            'h' => 3600,
            'm' => 60,
            's' => 1,
            _ => return None,
        };
        total_secs = total_secs.checked_add(value.checked_mul(unit_secs)?)?;
    }
    if !digits.is_empty() {
        return None;
    }
    Some(SignedDuration::from_secs(total_secs))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_hours() {
        assert_eq!(
            parse_duration("360h"),
            Some(SignedDuration::from_secs(360 * 3600))
        );
    }

    #[test]
    fn parse_duration_compound() {
        assert_eq!(
            parse_duration("1h30m"),
            Some(SignedDuration::from_secs(3600 + 30 * 60))
        );
    }

    #[test]
    fn parse_duration_seconds() {
        assert_eq!(parse_duration("45s"), Some(SignedDuration::from_secs(45)));
    }

    #[test]
    fn parse_duration_rejects_empty() {
        assert_eq!(parse_duration(""), None);
    }

    #[test]
    fn parse_duration_rejects_unknown_unit() {
        assert_eq!(parse_duration("360d"), None);
    }

    #[test]
    fn parse_duration_rejects_trailing_digits() {
        assert_eq!(parse_duration("360"), None);
    }

    #[test]
    fn parse_duration_rejects_leading_unit() {
        assert_eq!(parse_duration("h360"), None);
    }

    #[test]
    fn cert_manager_ready_status_true() {
        let status = serde_json::json!({
            "conditions": [
                {"type": "Ready", "status": "True"}
            ]
        });
        assert_eq!(cert_manager_ready_status(&status), Some(true));
    }

    #[test]
    fn cert_manager_ready_status_false() {
        let status = serde_json::json!({
            "conditions": [
                {"type": "Ready", "status": "False"}
            ]
        });
        assert_eq!(cert_manager_ready_status(&status), Some(false));
    }

    #[test]
    fn cert_manager_ready_status_missing() {
        let status = serde_json::json!({
            "conditions": [
                {"type": "Issuing", "status": "True"}
            ]
        });
        assert_eq!(cert_manager_ready_status(&status), None);
    }
}
