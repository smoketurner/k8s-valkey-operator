//! Scoped operator spawning for integration tests.
//!
//! Allows running the operator in a test-specific scope, either
//! watching a single namespace or cluster-wide.
//!
//! # Usage
//!
//! ```rust,ignore
//! let operator = ScopedOperator::start(client.clone(), "my-namespace").await;
//! // ... run tests ...
//! // operator is automatically stopped when dropped
//! ```
//!
//! The operator runs both ValkeyCluster and ValkeyUpgrade controllers concurrently,
//! matching production behavior. Controllers panic if they exit unexpectedly,
//! causing tests to fail immediately rather than timing out.

use std::sync::Arc;

use kube::Client;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// A scoped operator instance for integration testing.
///
/// Runs both ValkeyCluster and ValkeyUpgrade controllers in a background task.
/// The controllers are automatically stopped when this struct is dropped (RAII pattern).
pub struct ScopedOperator {
    /// Handle for the combined controller task.
    handle: JoinHandle<()>,
    /// Shutdown signal sender (oneshot for clean shutdown).
    shutdown_tx: Option<oneshot::Sender<()>>,
    /// Namespace being watched.
    namespace: Arc<str>,
}

impl ScopedOperator {
    /// Start a new operator instance watching a specific namespace.
    ///
    /// This spawns both the ValkeyCluster and ValkeyUpgrade controllers
    /// concurrently in a single background task. The operator is automatically
    /// stopped when the ScopedOperator instance is dropped.
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `namespace` - Namespace to watch
    ///
    /// # Returns
    /// A ScopedOperator instance that will run until dropped.
    pub async fn start(client: Client, namespace: &str) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let ns: Arc<str> = namespace.into();

        info!("Starting scoped operator in namespace: {}", ns);

        let handle = Self::spawn_controllers(client, Arc::clone(&ns), shutdown_rx);

        // Give the controllers a moment to start watching
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        Self {
            handle,
            shutdown_tx: Some(shutdown_tx),
            namespace: ns,
        }
    }

    /// Spawn the controller task.
    fn spawn_controllers(
        client: Client,
        namespace: Arc<str>,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            tokio::select! {
                _ = Self::run_controllers(client, &namespace) => {
                    debug!("Controllers exited normally");
                }
                _ = shutdown_rx => {
                    debug!("Controllers received shutdown signal");
                }
            }
        })
    }

    /// Run all controllers concurrently.
    ///
    /// Controllers panic if they exit unexpectedly, causing tests to fail
    /// immediately rather than timing out.
    async fn run_controllers(client: Client, namespace: &str) {
        tokio::select! {
            _ = valkey_operator::run_controller_scoped(client.clone(), None, Some(namespace)) => {
                panic!("ValkeyCluster controller exited unexpectedly");
            }
            _ = valkey_operator::run_upgrade_controller_scoped(client, Some(namespace)) => {
                panic!("ValkeyUpgrade controller exited unexpectedly");
            }
        }
    }

    /// Check if the operator is still running.
    pub fn is_running(&self) -> bool {
        !self.handle.is_finished()
    }

    /// Get the namespace being watched.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}

impl Drop for ScopedOperator {
    fn drop(&mut self) {
        // Send shutdown signal if not already sent
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Abort the task
        self.handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires Kubernetes cluster with CRD installed"]
    async fn test_scoped_operator_lifecycle() {
        // Initialize tracing for test output
        let _ = tracing_subscriber::fmt::try_init();

        let client = Client::try_default().await.unwrap();
        let operator = ScopedOperator::start(client, "test-ns").await;

        // Operator should be running
        assert!(operator.is_running(), "Operator should be running");
        assert_eq!(operator.namespace(), "test-ns");

        // Give it a moment to start watching
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Operator is stopped automatically when dropped
    }

    #[tokio::test]
    #[ignore = "requires Kubernetes cluster with CRD installed"]
    async fn test_scoped_operator_drop() {
        let _ = tracing_subscriber::fmt::try_init();

        let client = Client::try_default().await.unwrap();
        let operator = ScopedOperator::start(client, "drop-test").await;

        // Verify it's running
        assert!(operator.is_running());

        // Drop should trigger shutdown
        drop(operator);

        // Give it time to clean up
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
