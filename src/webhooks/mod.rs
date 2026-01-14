//! Webhook module for validating admission requests.
//!
//! This module provides ValidatingAdmissionWebhooks with tiered validation policies:
//! - Tier 1 (Critical): Always enforced (e.g., replica validation)
//! - Tier 2 (Update): Only on UPDATE operations (e.g., immutability)

pub mod policies;
mod server;

pub use policies::{ValidationContext, ValidationResult};
pub use server::{
    WEBHOOK_CERT_PATH, WEBHOOK_KEY_PATH, WEBHOOK_PORT, WebhookError, run_webhook_server,
};

// Re-export kube-rs admission types for contract testing
pub use kube::core::admission::{AdmissionRequest, AdmissionResponse, AdmissionReview, Operation};
