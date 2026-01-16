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

    /// Valkey client error
    #[error("Valkey error: {0}")]
    Valkey(String),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),
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
            Error::Valkey(_) | Error::Timeout(_) => true, // Valkey errors are typically transient
            Error::Validation(_) | Error::Permanent(_) | Error::MissingField(_) => false,
            Error::Serialization(_) => false,
        }
    }

    /// Get the recommended requeue duration for this error
    ///
    /// Uses simple fixed backoff. For exponential backoff, use `requeue_after_with_retry_count()`.
    pub fn requeue_after(&self) -> Duration {
        if self.is_retryable() {
            Duration::from_secs(30)
        } else {
            // Don't requeue for non-retryable errors
            Duration::from_secs(3600)
        }
    }

    /// Get the recommended requeue duration with exponential backoff based on retry count.
    ///
    /// # Arguments
    /// * `retry_count` - Number of times this error has been retried (0 = first retry)
    ///
    /// # Returns
    /// Duration with exponential backoff: base * 2^retry_count, capped at max_backoff
    pub fn requeue_after_with_retry_count(&self, retry_count: u32) -> Duration {
        if !self.is_retryable() {
            // Non-retryable errors: long delay (1 hour)
            return Duration::from_secs(3600);
        }

        // Exponential backoff: base * 2^retry_count
        const BASE_BACKOFF_SECS: u64 = 5; // Start with 5 seconds
        const MAX_BACKOFF_SECS: u64 = 300; // Cap at 5 minutes

        let backoff_secs = BASE_BACKOFF_SECS.saturating_mul(1 << retry_count.min(6)); // Limit exponent to prevent overflow
        let backoff_secs = backoff_secs.min(MAX_BACKOFF_SECS);

        Duration::from_secs(backoff_secs)
    }
}

/// Result type alias for controller operations
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
mod tests {
    use super::*;

    // ==========================================================================
    // Error::is_retryable() tests
    // ==========================================================================

    #[test]
    fn test_is_retryable_transient_error() {
        let error = Error::Transient("connection reset".to_string());
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_error() {
        let error = Error::Valkey("CLUSTERDOWN".to_string());
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_timeout_error() {
        let error = Error::Timeout("operation timed out".to_string());
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_validation_error_not_retryable() {
        let error = Error::Validation("invalid spec".to_string());
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_permanent_error_not_retryable() {
        let error = Error::Permanent("unrecoverable".to_string());
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_missing_field_not_retryable() {
        let error = Error::MissingField("metadata.name".to_string());
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_serialization_not_retryable() {
        let json_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let error = Error::Serialization(json_err);
        assert!(!error.is_retryable());
    }

    // ==========================================================================
    // Error::requeue_after() tests
    // ==========================================================================

    #[test]
    fn test_requeue_after_retryable_error() {
        let error = Error::Transient("test".to_string());
        let duration = error.requeue_after();
        assert_eq!(duration.as_secs(), 30);
    }

    #[test]
    fn test_requeue_after_non_retryable_error() {
        let error = Error::Validation("test".to_string());
        let duration = error.requeue_after();
        assert_eq!(duration.as_secs(), 3600);
    }

    #[test]
    fn test_requeue_after_valkey_error() {
        let error = Error::Valkey("CLUSTERDOWN".to_string());
        let duration = error.requeue_after();
        assert_eq!(duration.as_secs(), 30); // Retryable
    }

    #[test]
    fn test_requeue_after_permanent_error() {
        let error = Error::Permanent("fatal".to_string());
        let duration = error.requeue_after();
        assert_eq!(duration.as_secs(), 3600); // Non-retryable
    }

    // ==========================================================================
    // Error::is_not_found() tests
    // ==========================================================================

    #[test]
    fn test_is_not_found_non_kube_error() {
        let error = Error::Validation("test".to_string());
        assert!(!error.is_not_found());
    }

    #[test]
    fn test_is_not_found_transient() {
        let error = Error::Transient("test".to_string());
        assert!(!error.is_not_found());
    }

    // ==========================================================================
    // Error Display tests
    // ==========================================================================

    #[test]
    fn test_error_display_validation() {
        let error = Error::Validation("masters must be at least 3".to_string());
        assert_eq!(
            error.to_string(),
            "Validation error: masters must be at least 3"
        );
    }

    #[test]
    fn test_error_display_transient() {
        let error = Error::Transient("connection reset".to_string());
        assert_eq!(error.to_string(), "Transient error: connection reset");
    }

    #[test]
    fn test_error_display_permanent() {
        let error = Error::Permanent("unrecoverable state".to_string());
        assert_eq!(error.to_string(), "Permanent error: unrecoverable state");
    }

    #[test]
    fn test_error_display_missing_field() {
        let error = Error::MissingField("metadata.name".to_string());
        assert_eq!(error.to_string(), "Missing required field: metadata.name");
    }

    #[test]
    fn test_error_display_valkey() {
        let error = Error::Valkey("ERR CLUSTERDOWN".to_string());
        assert_eq!(error.to_string(), "Valkey error: ERR CLUSTERDOWN");
    }

    #[test]
    fn test_error_display_timeout() {
        let error = Error::Timeout("operation exceeded 30s".to_string());
        assert_eq!(error.to_string(), "Timeout: operation exceeded 30s");
    }

    // ==========================================================================
    // Error classification comprehensive tests
    // ==========================================================================

    #[test]
    fn test_error_classification_matrix() {
        // Test all error types and their classification
        let test_cases = vec![
            (Error::Transient("test".to_string()), true, false),
            (Error::Valkey("test".to_string()), true, false),
            (Error::Timeout("test".to_string()), true, false),
            (Error::Validation("test".to_string()), false, false),
            (Error::Permanent("test".to_string()), false, false),
            (Error::MissingField("test".to_string()), false, false),
        ];

        for (error, expected_retryable, expected_not_found) in test_cases {
            assert_eq!(
                error.is_retryable(),
                expected_retryable,
                "is_retryable failed for {}",
                error
            );
            assert_eq!(
                error.is_not_found(),
                expected_not_found,
                "is_not_found failed for {}",
                error
            );
        }
    }
}
