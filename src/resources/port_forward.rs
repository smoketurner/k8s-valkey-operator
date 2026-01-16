//! Port forwarding utilities for Valkey connections
//!
//! Provides `PortForward` which uses kube-rs native port-forwarding to establish
//! connections to Valkey pods. This enables direct connections without
//! requiring pod exec, useful when running the operator locally.
//!
//! When a `PortForward` goes out of scope, it will automatically stop
//! the forwarding (RAII pattern).

use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use kube::{Api, Client};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Errors that can occur during port forwarding
#[derive(Error, Debug)]
pub enum PortForwardError {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Port forward failed to become ready within timeout")]
    Timeout,

    #[error("No pods found for selector: {0}")]
    NoPodsFound(String),

    #[error("Pod has no IP address")]
    NoPodIp,

    #[error("Port forward task failed")]
    TaskFailed,

    #[error("Port forward join error: {0}")]
    JoinError(String),
}

/// Target for port forwarding
pub enum PortForwardTarget {
    /// Forward to a specific Pod by name
    Pod { name: String, port: u16 },
    /// Forward to a pod matching a label selector
    LabelSelector { selector: String, port: u16 },
}

impl PortForwardTarget {
    /// Create a target for a pod by name
    pub fn pod(name: impl Into<String>, port: u16) -> Self {
        Self::Pod {
            name: name.into(),
            port,
        }
    }

    /// Create a target for pods matching a label selector
    pub fn label_selector(selector: impl Into<String>, port: u16) -> Self {
        Self::LabelSelector {
            selector: selector.into(),
            port,
        }
    }

    fn remote_port(&self) -> u16 {
        match self {
            Self::Pod { port, .. } | Self::LabelSelector { port, .. } => *port,
        }
    }
}

/// RAII wrapper for port forwarding using kube-rs
///
/// When this struct is dropped, it will stop the port-forward.
pub struct PortForward {
    local_port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    _handle: JoinHandle<()>,
    cleanup_initiated: AtomicBool,
}

impl PortForward {
    /// Start a port-forward to the specified target
    ///
    /// For LabelSelector targets, this resolves to a running pod matching the selector.
    /// If `local_port` is None, an available port will be automatically selected.
    pub async fn start(
        client: Client,
        namespace: &str,
        target: PortForwardTarget,
        local_port: Option<u16>,
    ) -> Result<Self, PortForwardError> {
        let local_port = match local_port {
            Some(p) => p,
            None => get_available_port()?,
        };

        let remote_port = target.remote_port();

        // Resolve target to a pod name
        let pod_name = match &target {
            PortForwardTarget::Pod { name, .. } => name.clone(),
            PortForwardTarget::LabelSelector { selector, .. } => {
                resolve_selector_to_pod(&client, namespace, selector).await?
            }
        };

        tracing::debug!(
            namespace = namespace,
            pod = %pod_name,
            local_port = local_port,
            remote_port = remote_port,
            "Starting port-forward"
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        // Clone values for the spawned task
        let ns = namespace.to_string();
        let pod = pod_name.clone();

        // Start the port forwarding task
        let handle = tokio::spawn(async move {
            if let Err(e) =
                run_port_forward(client, &ns, &pod, local_port, remote_port, shutdown_rx).await
            {
                tracing::warn!(error = %e, "Port forward error");
            }
        });

        // Wait a moment for the listener to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        tracing::info!(
            local_port = local_port,
            pod = %pod_name,
            "Port-forward established"
        );

        Ok(Self {
            local_port,
            shutdown_tx: Some(shutdown_tx),
            _handle: handle,
            cleanup_initiated: AtomicBool::new(false),
        })
    }

    /// Get the local port that is forwarding to the remote target
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    /// Stop the port-forward
    pub fn stop(&mut self) {
        if self.cleanup_initiated.swap(true, Ordering::SeqCst) {
            return;
        }

        tracing::debug!(local_port = self.local_port, "Stopping port-forward");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for PortForward {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Resolve a label selector to a pod name by finding a running pod
async fn resolve_selector_to_pod(
    client: &Client,
    namespace: &str,
    selector: &str,
) -> Result<String, PortForwardError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);

    tracing::debug!(label_selector = %selector, "Resolving selector to pod");

    let pod_list = pods.list(&ListParams::default().labels(selector)).await?;

    let pod = pod_list
        .items
        .into_iter()
        .find(|p| {
            // Find a running pod
            p.status
                .as_ref()
                .and_then(|s| s.phase.as_ref())
                .is_some_and(|phase| phase == "Running")
        })
        .ok_or_else(|| PortForwardError::NoPodsFound(selector.to_string()))?;

    pod.metadata
        .name
        .ok_or_else(|| PortForwardError::NoPodsFound(selector.to_string()))
}

/// Run the port forwarding loop
async fn run_port_forward(
    client: Client,
    namespace: &str,
    pod_name: &str,
    local_port: u16,
    remote_port: u16,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), PortForwardError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);

    // Bind to the local port
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", local_port)).await?;

    tracing::debug!(local_port = local_port, "Port forward listener started");

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::debug!("Port forward shutdown requested");
                break;
            }
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        tracing::trace!(client_addr = %addr, "New port forward connection");

                        // Clone for the connection handler
                        let pods = pods.clone();
                        let pod_name = pod_name.to_string();

                        tokio::spawn(async move {
                            if let Err(e) = handle_connection(
                                pods,
                                &pod_name,
                                remote_port,
                                stream
                            ).await {
                                tracing::warn!(error = %e, "Port forward connection error");
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Port forward accept error");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Handle a single port-forward connection
#[allow(clippy::indexing_slicing)] // Safe: n is always <= buf.len() after read()
async fn handle_connection(
    pods: Api<Pod>,
    pod_name: &str,
    remote_port: u16,
    mut local_stream: TcpStream,
) -> Result<(), PortForwardError> {
    // Create the port forward to the pod
    let mut pf = pods.portforward(pod_name, &[remote_port]).await?;

    // Get the stream for the port
    let upstream = pf
        .take_stream(remote_port)
        .ok_or(PortForwardError::TaskFailed)?;

    // Copy data bidirectionally
    let (mut local_read, mut local_write) = local_stream.split();
    let (mut upstream_read, mut upstream_write) = tokio::io::split(upstream);

    let client_to_server = async {
        let mut buf = [0u8; 8192];
        loop {
            let n = local_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            upstream_write.write_all(&buf[..n]).await?;
        }
        upstream_write.shutdown().await?;
        Ok::<_, std::io::Error>(())
    };

    let server_to_client = async {
        let mut buf = [0u8; 8192];
        loop {
            let n = upstream_read.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            local_write.write_all(&buf[..n]).await?;
        }
        local_write.shutdown().await?;
        Ok::<_, std::io::Error>(())
    };

    // Run both directions concurrently
    let _ = tokio::try_join!(client_to_server, server_to_client);

    // Join the port forward to clean up
    pf.join()
        .await
        .map_err(|e| PortForwardError::JoinError(e.to_string()))?;

    Ok(())
}

/// Find an available local port by binding to port 0
pub fn get_available_port() -> Result<u16, PortForwardError> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Helper to create port forwards for all pods in a ValkeyCluster
pub struct PodPortForwards {
    forwards: Vec<(String, PortForward)>,
}

impl PodPortForwards {
    /// Create port forwards for a list of pod names
    pub async fn new(
        client: Client,
        namespace: &str,
        pod_names: Vec<String>,
        remote_port: u16,
    ) -> Result<Self, PortForwardError> {
        let mut forwards = Vec::with_capacity(pod_names.len());

        for pod_name in pod_names {
            let pf = PortForward::start(
                client.clone(),
                namespace,
                PortForwardTarget::pod(&pod_name, remote_port),
                None,
            )
            .await?;

            forwards.push((pod_name, pf));
        }

        Ok(Self { forwards })
    }

    /// Get the local address (127.0.0.1:port) for a pod by name
    pub fn local_address(&self, pod_name: &str) -> Option<(String, u16)> {
        self.forwards
            .iter()
            .find(|(name, _)| name == pod_name)
            .map(|(_, pf)| ("127.0.0.1".to_string(), pf.local_port()))
    }

    /// Get all local addresses as a map from pod name to (host, port)
    pub fn all_local_addresses(&self) -> Vec<(String, String, u16)> {
        self.forwards
            .iter()
            .map(|(name, pf)| (name.clone(), "127.0.0.1".to_string(), pf.local_port()))
            .collect()
    }

    /// Get the number of port forwards
    pub fn len(&self) -> usize {
        self.forwards.len()
    }

    /// Check if there are no port forwards
    pub fn is_empty(&self) -> bool {
        self.forwards.is_empty()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
mod tests {
    use super::*;

    #[test]
    fn test_get_available_port() {
        let port = get_available_port().expect("should find port");
        assert!(port > 0);
    }

    #[test]
    fn test_port_forward_target_pod() {
        let target = PortForwardTarget::pod("my-pod-0", 6379);
        assert_eq!(target.remote_port(), 6379);
    }

    #[test]
    fn test_port_forward_target_selector() {
        let target = PortForwardTarget::label_selector("app=valkey", 6379);
        assert_eq!(target.remote_port(), 6379);
    }
}
