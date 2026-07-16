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
    Valkey(#[from] crate::client::ValkeyError),

    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),

    /// Quorum check failed - not enough nodes reachable
    #[error("Insufficient quorum: {reachable} nodes reachable, need {required}")]
    InsufficientQuorum { reachable: i32, required: i32 },

    /// CLUSTER FORGET quorum failed
    #[error("CLUSTER FORGET failed to reach quorum: {successes}/{required} nodes acknowledged")]
    ForgetQuorumFailed { successes: i32, required: i32 },

    /// Failover verification failed
    #[error("Failover verification timed out after {elapsed_secs}s")]
    FailoverVerificationTimeout { elapsed_secs: u64 },

    /// Transport layer error
    #[error("Transport error: {0}")]
    Transport(#[from] crate::controller::transport::TransportError),
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
                // Retry on network errors, rate limiting, server errors, and optimistic concurrency
                // conflicts (409 = resourceVersion mismatch from concurrent reconciliations).
                // HyperError and HttpError are transient HTTP transport failures
                // (connection reset, DNS, TLS handshake) — kube-client maps raw
                // hyper errors to HyperError, not Service, and its retry layer
                // only retries on status codes, so these reach us directly.
                matches!(
                    e,
                    kube::Error::Api(api_err)
                        if api_err.code >= 500 || api_err.code == 429 || api_err.code == 409
                ) || matches!(
                    e,
                    kube::Error::Service(_)
                        | kube::Error::HyperError(_)
                        | kube::Error::HttpError(_)
                )
            }
            Error::Transient(_) => true,
            Error::Timeout(_) => true,
            Error::Valkey(ve) => ve.is_retryable(),
            Error::Validation(_) | Error::Permanent(_) | Error::MissingField(_) => false,
            Error::Serialization(_) => false,
            // Quorum errors are permanent - require manual intervention
            Error::InsufficientQuorum { .. } | Error::ForgetQuorumFailed { .. } => false,
            // Failover verification timeout requires investigation
            Error::FailoverVerificationTimeout { .. } => false,
            // Transport errors are transient (pod not ready, port-forward failure)
            Error::Transport(_) => true,
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
    use crate::client::ValkeyError;

    // ==========================================================================
    // Error::is_retryable() tests
    // ==========================================================================

    #[test]
    fn test_is_retryable_transient_error() {
        let error = Error::Transient("connection reset".to_string());
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_cluster_not_ready() {
        let error = Error::Valkey(ValkeyError::ClusterNotReady("CLUSTERDOWN".to_string()));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_connection() {
        let error = Error::Valkey(ValkeyError::Connection("refused".to_string()));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_timeout() {
        let error = Error::Valkey(ValkeyError::Timeout {
            operation: "CLUSTER INFO".to_string(),
            duration: std::time::Duration::from_secs(5),
        });
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_parse_not_retryable() {
        use crate::client::types::ParseError;
        let error = Error::Valkey(ValkeyError::Parse(ParseError::MissingField(
            "node_id".to_string(),
        )));
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_invalid_config_not_retryable() {
        let error = Error::Valkey(ValkeyError::InvalidConfig("No hosts".to_string()));
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_valkey_slot_migration_failed_not_retryable() {
        let error = Error::Valkey(ValkeyError::SlotMigrationFailed("shard 0".to_string()));
        assert!(!error.is_retryable());
    }

    #[test]
    fn test_is_retryable_kube_409_conflict() {
        let status = kube::core::Status {
            code: 409,
            message: "the object has been modified".to_string(),
            reason: "Conflict".to_string(),
            ..Default::default()
        };
        let error = Error::Kube(kube::Error::Api(Box::new(status)));
        assert!(error.is_retryable());
    }

    #[test]
    fn test_is_retryable_kube_transport_error() {
        // Transient HTTP transport failures must get the short retry backoff,
        // not the 1-hour non-retryable delay. HyperError shares the same match
        // arm as HttpError but hyper::Error cannot be constructed in tests.
        let http_err = http::Request::builder()
            .uri("http://[invalid-uri")
            .body(())
            .unwrap_err();
        let error = Error::Kube(kube::Error::HttpError(http_err));
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
        let error = Error::Valkey(ValkeyError::ClusterNotReady("ERR CLUSTERDOWN".to_string()));
        assert_eq!(
            error.to_string(),
            "Valkey error: Cluster not ready: ERR CLUSTERDOWN"
        );
    }

    #[test]
    fn test_error_display_timeout() {
        let error = Error::Timeout("operation exceeded 30s".to_string());
        assert_eq!(error.to_string(), "Timeout: operation exceeded 30s");
    }

    #[test]
    fn test_error_display_insufficient_quorum() {
        let error = Error::InsufficientQuorum {
            reachable: 1,
            required: 2,
        };
        assert_eq!(
            error.to_string(),
            "Insufficient quorum: 1 nodes reachable, need 2"
        );
    }

    #[test]
    fn test_error_display_forget_quorum_failed() {
        let error = Error::ForgetQuorumFailed {
            successes: 1,
            required: 2,
        };
        assert_eq!(
            error.to_string(),
            "CLUSTER FORGET failed to reach quorum: 1/2 nodes acknowledged"
        );
    }

    #[test]
    fn test_error_display_failover_verification_timeout() {
        let error = Error::FailoverVerificationTimeout { elapsed_secs: 60 };
        assert_eq!(
            error.to_string(),
            "Failover verification timed out after 60s"
        );
    }

    // ==========================================================================
    // Error classification comprehensive tests
    // ==========================================================================

    #[test]
    fn test_is_retryable_transport_error() {
        use crate::controller::transport::TransportError;
        use crate::crd::PodOrdinal;

        let error = Error::Transport(TransportError::PortForwardFailed {
            ordinal: PodOrdinal::new(0),
            reason: "timeout".to_string(),
        });
        assert!(error.is_retryable());
    }

    #[test]
    fn test_error_classification_matrix() {
        // Test all error types and their classification
        let test_cases = vec![
            (Error::Transient("test".to_string()), true, false),
            (
                Error::Valkey(ValkeyError::ClusterNotReady("test".to_string())),
                true,
                false,
            ),
            (Error::Timeout("test".to_string()), true, false),
            (Error::Validation("test".to_string()), false, false),
            (Error::Permanent("test".to_string()), false, false),
            (Error::MissingField("test".to_string()), false, false),
            // Quorum errors are non-retryable (require intervention)
            (
                Error::InsufficientQuorum {
                    reachable: 1,
                    required: 2,
                },
                false,
                false,
            ),
            (
                Error::ForgetQuorumFailed {
                    successes: 1,
                    required: 2,
                },
                false,
                false,
            ),
            (
                Error::FailoverVerificationTimeout { elapsed_secs: 60 },
                false,
                false,
            ),
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
