//! Kubernetes cluster connection and CRD management for integration tests.
//!
//! Provides [`SharedTestCluster`], a singleton that manages the connection to the
//! Kubernetes cluster using the existing kubeconfig (~/.kube/config or KUBECONFIG
//! environment variable), and CRD installation helpers.
//!
//! # Usage
//!
//! ```ignore
//! // Get or initialize the shared cluster connection
//! let cluster = SharedTestCluster::get().await?;
//!
//! // Create a new client for API calls
//! let client = cluster.new_client().await?;
//!
//! // Ensure CRDs are installed (idempotent - only installs once per test run)
//! ensure_cluster_crd_installed(&cluster).await?;
//! ensure_upgrade_crd_installed(&cluster).await?;
//! ```
//!
//! # Design
//!
//! - Uses a static `OnceCell` to ensure only one cluster connection is created
//! - CRD installation is tracked to avoid redundant installs across tests
//! - Each test creates its own `Client` from the shared cluster for isolation
//! - Uses server-side apply for idempotent CRD installation
//! - Waits up to 30 seconds for each CRD to become established

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{Patch, PatchParams};
use kube::runtime::wait::{await_condition, conditions};
use kube::{Api, Client, Config};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::OnceCell;

#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("Failed to create kube client: {0}")]
    ClientCreation(#[from] kube::Error),

    #[error("Failed to infer config: {0}")]
    InferConfig(#[from] kube::config::InferConfigError),
}

#[derive(Error, Debug)]
pub enum CrdError {
    #[error("Failed to parse CRD YAML: {0}")]
    ParseError(#[from] serde_saphyr::Error),

    #[error("Kubernetes API error: {0}")]
    KubeError(#[from] kube::Error),

    #[error("CRD establishment timeout")]
    EstablishmentTimeout,

    #[error("Wait error: {0}")]
    WaitError(#[from] kube::runtime::wait::Error),
}

/// Global shared cluster instance
static SHARED_CLUSTER: OnceCell<Arc<SharedTestCluster>> = OnceCell::const_new();

/// ValkeyCluster CRD installation tracking (should only be done once)
static CLUSTER_CRD_INSTALLED: OnceCell<()> = OnceCell::const_new();

/// ValkeyUpgrade CRD installation tracking (should only be done once)
static UPGRADE_CRD_INSTALLED: OnceCell<()> = OnceCell::const_new();

/// A shared Kubernetes cluster connection for all integration tests.
///
/// This is a singleton that connects to an existing Kubernetes cluster
/// via kubeconfig. Each test creates its own `Client` from this shared
/// connection using [`new_client()`](Self::new_client).
pub struct SharedTestCluster {
    _marker: (),
}

impl SharedTestCluster {
    /// Get or initialize the shared cluster
    ///
    /// Uses the existing kubeconfig from ~/.kube/config or KUBECONFIG env var.
    pub async fn get() -> Result<Arc<SharedTestCluster>, ClusterError> {
        SHARED_CLUSTER
            .get_or_try_init(|| async {
                let cluster = Self::connect().await?;
                Ok(Arc::new(cluster))
            })
            .await
            .map(Arc::clone)
    }

    /// Create a new kube Client
    pub async fn new_client(&self) -> Result<Client, ClusterError> {
        let config = Config::infer().await?;
        Ok(Client::try_from(config)?)
    }

    /// Connect to the cluster using kubeconfig
    async fn connect() -> Result<Self, ClusterError> {
        // Verify we can connect
        let config = Config::infer().await?;
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

/// Generic helper to install a CRD from YAML content.
///
/// This function handles parsing, applying, and waiting for establishment.
async fn install_crd_from_yaml(
    client: Client,
    crd_yaml: &str,
    crd_name: &str,
    display_name: &str,
) -> Result<(), CrdError> {
    let crd: CustomResourceDefinition = serde_saphyr::from_str(crd_yaml)?;
    let crds: Api<CustomResourceDefinition> = Api::all(client);
    let params = PatchParams::apply("integration-test").force();

    tracing::info!("Installing {} CRD...", display_name);

    crds.patch(crd_name, &params, &Patch::Apply(&crd)).await?;

    // Wait for CRD to be established (up to 30 seconds)
    tracing::info!("Waiting for {} CRD to be established...", display_name);

    let establish = await_condition(crds, crd_name, conditions::is_crd_established());

    tokio::time::timeout(Duration::from_secs(30), establish)
        .await
        .map_err(|_| CrdError::EstablishmentTimeout)??;

    tracing::info!("{} CRD installed and established", display_name);

    Ok(())
}

/// Ensure ValkeyCluster CRD is installed (idempotent - only runs once per test run)
pub async fn ensure_cluster_crd_installed(cluster: &SharedTestCluster) -> Result<(), CrdError> {
    CLUSTER_CRD_INSTALLED
        .get_or_try_init(|| async {
            let client = cluster.new_client().await.map_err(|e| {
                CrdError::KubeError(kube::Error::Service(
                    std::io::Error::other(e.to_string()).into(),
                ))
            })?;
            install_crd_from_yaml(
                client,
                include_str!("../../config/crd/valkeycluster.yaml"),
                "valkeyclusters.valkey-operator.smoketurner.com",
                "ValkeyCluster",
            )
            .await
        })
        .await
        .map(|_| ())
}

/// Ensure ValkeyUpgrade CRD is installed (idempotent - only runs once per test run)
pub async fn ensure_upgrade_crd_installed(cluster: &SharedTestCluster) -> Result<(), CrdError> {
    UPGRADE_CRD_INSTALLED
        .get_or_try_init(|| async {
            let client = cluster.new_client().await.map_err(|e| {
                CrdError::KubeError(kube::Error::Service(
                    std::io::Error::other(e.to_string()).into(),
                ))
            })?;
            install_crd_from_yaml(
                client,
                include_str!("../../config/crd/valkeyupgrade.yaml"),
                "valkeyupgrades.valkey-operator.smoketurner.com",
                "ValkeyUpgrade",
            )
            .await
        })
        .await
        .map(|_| ())
}
