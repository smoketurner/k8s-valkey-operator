//! Shared test cluster singleton.
//!
//! Provides a Kubernetes cluster connection for all integration tests.
//! Each test creates its own Client from the shared cluster for isolation.

use kube::{Client, Config};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::OnceCell;

/// Global shared test cluster instance.
static SHARED_CLUSTER: OnceCell<Arc<SharedTestCluster>> = OnceCell::const_new();

/// Shared test cluster providing Kubernetes connectivity.
///
/// Uses a singleton pattern to ensure connectivity is validated once.
/// Each test creates its own Client via `new_client()` for isolation.
pub struct SharedTestCluster {
    _marker: (),
}

impl SharedTestCluster {
    /// Get or create the shared test cluster.
    ///
    /// This function is safe to call from multiple tests concurrently.
    /// The first call validates connectivity, subsequent calls return
    /// the cached instance.
    pub async fn get() -> Arc<SharedTestCluster> {
        SHARED_CLUSTER
            .get_or_init(|| async {
                let cluster = Self::connect().await.expect(
                    "Failed to connect to Kubernetes cluster. Is your kubeconfig configured?",
                );
                Arc::new(cluster)
            })
            .await
            .clone()
    }

    /// Create a new Kubernetes client.
    ///
    /// Each test should call this to get its own isolated client.
    pub async fn new_client(&self) -> Client {
        let config = Config::infer().await.expect("Failed to infer kube config");
        Client::try_from(config).expect("Failed to create kube client")
    }

    /// For backwards compatibility - alias to new_client
    pub async fn client(&self) -> Client {
        self.new_client().await
    }

    /// Connect to the cluster using kubeconfig
    async fn connect() -> Result<Self, kube::Error> {
        // Verify we can connect
        let config = Config::infer()
            .await
            .map_err(|e| kube::Error::Service(std::io::Error::other(e.to_string()).into()))?;
        let client = Client::try_from(config)?;

        // Quick health check
        let version = client.apiserver_version().await?;
        tracing::info!(
            "Connected to Kubernetes cluster: {} {}",
            version.platform,
            version.git_version
        );

        Ok(Self { _marker: () })
    }
}

/// Initialize tracing for tests (optional, call once).
static TRACING_INIT: OnceLock<()> = OnceLock::new();

#[allow(dead_code)]
pub fn init_tracing() {
    TRACING_INIT.get_or_init(|| {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    });
}
