//! Admission webhook server.
//!
//! Provides HTTP endpoints for Kubernetes admission webhooks.
//!
//! To enable webhooks:
//! 1. Deploy cert-manager for TLS certificates
//! 2. Create a ValidatingWebhookConfiguration
//! 3. Mount the TLS certificate secret to the operator pod at /etc/webhook/certs/
//!
//! The webhook server starts automatically when certificates are present.

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::post};
use kube::Client;
use kube::Resource;
use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview, Operation};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::crd::ValkeyCluster;
use crate::webhooks::policies::{ValidationContext, validate_all};

/// Default path to webhook TLS certificate
pub const WEBHOOK_CERT_PATH: &str = "/etc/webhook/certs/tls.crt";
/// Default path to webhook TLS private key
pub const WEBHOOK_KEY_PATH: &str = "/etc/webhook/certs/tls.key";
/// Default webhook server port
pub const WEBHOOK_PORT: u16 = 9443;

/// Shared state for webhook handlers
pub struct WebhookState;

impl WebhookState {
    pub fn new(_client: Client) -> Self {
        // Client intentionally not stored - available for future use if needed
        Self
    }
}

/// Create a denial response with reason embedded in message.
/// kube-rs deny() only sets status.message, so we format as "[reason] message"
fn deny_with_reason<T: Resource<DynamicType = ()>>(
    request: &AdmissionRequest<T>,
    message: &str,
    reason: &str,
) -> AdmissionReview<kube::core::DynamicObject> {
    let full_message = format!("[{}] {}", reason, message);
    AdmissionResponse::from(request)
        .deny(full_message)
        .into_review()
}

/// Create the webhook router
pub fn create_webhook_router(state: Arc<WebhookState>) -> Router {
    Router::new()
        .route("/validate-valkeycluster", post(validate_valkeycluster))
        .with_state(state)
}

/// Validate a ValkeyCluster admission webhook handler
async fn validate_valkeycluster(
    State(_state): State<Arc<WebhookState>>,
    Json(review): Json<AdmissionReview<ValkeyCluster>>,
) -> impl IntoResponse {
    let request: AdmissionRequest<ValkeyCluster> = match review.try_into() {
        Ok(req) => req,
        Err(e) => {
            error!(error = %e, "Failed to extract admission request");
            return (
                StatusCode::BAD_REQUEST,
                Json(
                    AdmissionResponse::invalid(format!("Invalid AdmissionReview: {}", e))
                        .into_review(),
                ),
            );
        }
    };

    let uid = &request.uid;
    debug!(
        uid = %uid,
        operation = ?request.operation,
        namespace = ?request.namespace,
        name = ?request.name,
        "Processing admission request"
    );

    // DELETE operations are always allowed
    if request.operation == Operation::Delete {
        info!(uid = %uid, "Admission request allowed (DELETE)");
        return (
            StatusCode::OK,
            Json(AdmissionResponse::from(&request).into_review()),
        );
    }

    // Get the new object (already typed as ValkeyCluster)
    let resource: ValkeyCluster = match &request.object {
        Some(obj) => obj.clone(),
        None => {
            error!(uid = %uid, "Missing object in request");
            return (
                StatusCode::OK,
                Json(deny_with_reason(
                    &request,
                    "Missing object in request",
                    "InvalidRequest",
                )),
            );
        }
    };

    // Get the old object for UPDATE operations (already typed)
    let old_resource: Option<ValkeyCluster> = request.old_object.clone();

    // Create validation context
    let ctx = ValidationContext {
        resource: &resource,
        old_resource: old_resource.as_ref(),
        dry_run: request.dry_run,
        namespace: request.namespace.as_deref(),
    };

    // Run tiered validation policies
    let result = validate_all(&ctx);

    if !result.allowed {
        let reason = result
            .reason
            .unwrap_or_else(|| "ValidationFailed".to_string());
        let message = result
            .message
            .unwrap_or_else(|| "Validation failed".to_string());
        warn!(uid = %uid, reason = %reason, message = %message, "Admission request denied");
        return (
            StatusCode::OK,
            Json(deny_with_reason(&request, &message, &reason)),
        );
    }

    info!(uid = %uid, "Admission request allowed");
    (
        StatusCode::OK,
        Json(AdmissionResponse::from(&request).into_review()),
    )
}

/// Errors that can occur when running the webhook server
#[derive(Debug)]
pub enum WebhookError {
    /// TLS configuration error
    TlsConfig(String),
    /// Server error
    Server(String),
}

impl std::fmt::Display for WebhookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WebhookError::TlsConfig(msg) => write!(f, "TLS configuration error: {}", msg),
            WebhookError::Server(msg) => write!(f, "Webhook server error: {}", msg),
        }
    }
}

impl std::error::Error for WebhookError {}

/// Run the webhook server with TLS
///
/// Binds to 0.0.0.0:9443 and serves the /validate-valkeycluster endpoint.
/// TLS certificates are loaded from the paths specified.
///
/// # Arguments
/// * `client` - Kubernetes client
/// * `cert_path` - Path to TLS certificate file (PEM format)
/// * `key_path` - Path to TLS private key file (PEM format)
pub async fn run_webhook_server(
    client: Client,
    cert_path: &str,
    key_path: &str,
) -> Result<(), WebhookError> {
    use axum_server::tls_rustls::RustlsConfig;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    let state = Arc::new(WebhookState::new(client));
    let app = create_webhook_router(state);

    let config = RustlsConfig::from_pem_file(PathBuf::from(cert_path), PathBuf::from(key_path))
        .await
        .map_err(|e| WebhookError::TlsConfig(e.to_string()))?;

    let addr = SocketAddr::from(([0, 0, 0, 0], WEBHOOK_PORT));
    info!(port = WEBHOOK_PORT, "Webhook server listening with TLS");

    axum_server::bind_rustls(addr, config)
        .serve(app.into_make_service())
        .await
        .map_err(|e| WebhookError::Server(e.to_string()))?;

    Ok(())
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
    use crate::crd::{AuthSpec, IssuerRef, SecretKeyRef, TlsSpec};
    use crate::crd::{ValkeyCluster, ValkeyClusterSpec};
    use crate::webhooks::policies::ValidationContext;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    fn create_resource(masters: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters,
                replicas_per_master: 1,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "test-issuer".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-secret".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                labels: BTreeMap::new(),
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_valid_create_request() {
        let resource = create_resource(3);
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate_all(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_invalid_masters_on_create() {
        let resource = create_resource(2); // Below minimum of 3
        let ctx = ValidationContext {
            resource: &resource,
            old_resource: None,
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate_all(&ctx);
        assert!(!result.allowed);
    }

    #[test]
    fn test_valid_update_request() {
        let old = create_resource(3);
        let new = create_resource(6);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate_all(&ctx);
        assert!(result.allowed);
    }

    #[test]
    fn test_scale_below_minimum_on_update() {
        let old = create_resource(3);
        let new = create_resource(2);
        let ctx = ValidationContext {
            resource: &new,
            old_resource: Some(&old),
            dry_run: false,
            namespace: Some("default"),
        };

        let result = validate_all(&ctx);
        assert!(!result.allowed);
        // Tier 1 (masters) policy rejects this before Tier 2 (immutability) runs
        assert!(result.message.unwrap().contains("at least"));
    }
}
