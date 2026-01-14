//! Error types for the controller.
//!
//! Defines custom error types with classification for retry behavior.

use std::time::Duration;
use thiserror::Error;

/// Error type for controller operations
#[derive(Error, Debug)]
pub enum Error {
    /// Kubernetes API error
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    /// Missing required field in resource
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// Validation error in resource spec
    #[error("Validation error: {0}")]
    Validation(String),

    /// Transient error that should be retried
    #[error("Transient error: {0}")]
    Transient(String),

    /// Permanent error that should not be retried
    #[error("Permanent error: {0}")]
    Permanent(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

impl Error {
    /// Check if this error indicates a not-found condition
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::Kube(kube::Error::Api(e)) if e.code == 404)
    }

    /// Check if this error should be retried
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Kube(e) => {
                // Retry on network errors, rate limiting, and server errors
                matches!(
                    e,
                    kube::Error::Api(api_err) if api_err.code >= 500 || api_err.code == 429
                ) || matches!(e, kube::Error::Service(_))
            }
            Error::Transient(_) => true,
            Error::Validation(_) | Error::Permanent(_) | Error::MissingField(_) => false,
            Error::Serialization(_) => false,
        }
    }

    /// Get the recommended requeue duration for this error
    pub fn requeue_after(&self) -> Duration {
        if self.is_retryable() {
            // Exponential backoff would be better, but use a simple default for now
            Duration::from_secs(30)
        } else {
            // Don't requeue for non-retryable errors
            Duration::from_secs(3600)
        }
    }
}

/// Result type alias for controller operations
pub type Result<T> = std::result::Result<T, Error>;
