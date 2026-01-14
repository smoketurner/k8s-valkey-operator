//! Scoped operator spawning for integration tests.
//!
//! Allows running the operator in a test-specific scope, either
//! watching a single namespace or cluster-wide.

use kube::Client;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use my_operator::controller::context::Context;

/// A scoped operator instance for testing.
///
/// The operator runs in a background task and is automatically
/// stopped when this struct is dropped.
pub struct ScopedOperator {
    handle: JoinHandle<()>,
    shutdown_tx: watch::Sender<bool>,
}

impl ScopedOperator {
    /// Start a new operator instance watching a specific namespace.
    ///
    /// # Arguments
    /// * `client` - Kubernetes client
    /// * `namespace` - Namespace to watch (None for cluster-wide)
    pub async fn start(client: Client, namespace: Option<String>) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let ctx = Arc::new(Context::new(client.clone(), None));

        let handle = tokio::spawn(async move {
            // Run the controller with shutdown signal
            let controller_future = run_controller(ctx, namespace);

            tokio::select! {
                _ = controller_future => {
                    tracing::info!("Controller completed");
                }
                _ = wait_for_shutdown(shutdown_rx) => {
                    tracing::info!("Controller shutdown requested");
                }
            }
        });

        Self {
            handle,
            shutdown_tx,
        }
    }

    /// Stop the operator gracefully.
    pub async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        // Give the controller time to shut down
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        self.handle.abort();
    }
}

impl Drop for ScopedOperator {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self.handle.abort();
    }
}

/// Run the controller (placeholder - implement based on your controller setup).
async fn run_controller(_ctx: Arc<Context>, _namespace: Option<String>) {
    // In a real implementation, this would:
    // 1. Create the controller with appropriate namespace filter
    // 2. Run the reconciliation loop
    //
    // Example:
    // ```
    // use kube::runtime::Controller;
    // use my_operator::crd::MyResource;
    //
    // let api: Api<MyResource> = match namespace {
    //     Some(ns) => Api::namespaced(ctx.client.clone(), &ns),
    //     None => Api::all(ctx.client.clone()),
    // };
    //
    // Controller::new(api, Default::default())
    //     .run(reconcile, error_policy, ctx)
    //     .for_each(|_| async {})
    //     .await;
    // ```

    // For now, just wait indefinitely
    std::future::pending::<()>().await;
}

/// Wait for shutdown signal.
async fn wait_for_shutdown(mut rx: watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires Kubernetes cluster"]
    async fn test_scoped_operator_lifecycle() {
        let client = Client::try_default().await.unwrap();
        let operator = ScopedOperator::start(client, Some("test-ns".to_string())).await;

        // Operator is running in background
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Stop it
        operator.stop().await;
    }
}
