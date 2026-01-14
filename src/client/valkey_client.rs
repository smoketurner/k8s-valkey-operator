//! Valkey client wrapper using the fred crate.
//!
//! Provides a type-safe client for connecting to Valkey clusters with
//! TLS support, connection pooling, and proper error handling.

use std::time::Duration;

use fred::prelude::*;
use fred::types::InfoKind;
use fred::types::config::ClusterDiscoveryPolicy;
use fred::types::cluster::{ClusterFailoverFlag, ClusterResetFlag};
use thiserror::Error;
use tracing::{debug, instrument};

use crate::crd::ValkeyClusterSpec;

/// Errors that can occur during Valkey operations.
#[derive(Error, Debug)]
pub enum ValkeyError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Redis error: {0}")]
    Redis(#[from] fred::error::Error),

    #[error("Parse error: {0}")]
    Parse(#[from] crate::client::types::ParseError),

    #[error("Timeout after {duration:?}: {operation}")]
    Timeout { operation: String, duration: Duration },

    #[error("Cluster not ready: {0}")]
    ClusterNotReady(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

/// Configuration for connecting to a Valkey cluster.
#[derive(Clone, Debug)]
pub struct ValkeyClientConfig {
    /// List of host:port pairs for initial connection.
    pub hosts: Vec<(String, u16)>,
    /// TLS configuration.
    pub tls: Option<TlsClientConfig>,
    /// Connection timeout.
    pub connection_timeout: Duration,
    /// Command timeout.
    pub command_timeout: Duration,
    /// Password for authentication.
    pub password: Option<String>,
}

impl Default for ValkeyClientConfig {
    fn default() -> Self {
        Self {
            hosts: Vec::new(),
            tls: None,
            connection_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_secs(30),
            password: None,
        }
    }
}

/// TLS configuration for Valkey connections.
#[derive(Clone, Debug)]
pub struct TlsClientConfig {
    /// Path to CA certificate file.
    pub ca_cert_path: Option<String>,
    /// Path to client certificate file.
    pub cert_path: Option<String>,
    /// Path to client key file.
    pub key_path: Option<String>,
}

impl ValkeyClientConfig {
    /// Create a new configuration with hosts.
    pub fn new(hosts: Vec<(String, u16)>) -> Self {
        Self {
            hosts,
            ..Default::default()
        }
    }

    /// Set TLS configuration.
    pub fn with_tls(mut self, tls: TlsClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set password.
    pub fn with_password(mut self, password: String) -> Self {
        self.password = Some(password);
        self
    }

    /// Set connection timeout.
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set command timeout.
    pub fn with_command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Build configuration from CRD spec.
    pub fn from_spec(
        _spec: &ValkeyClusterSpec,
        hosts: Vec<(String, u16)>,
        password: Option<String>,
        tls_paths: Option<(String, String, String)>,
    ) -> Self {
        let tls = if let Some((ca_path, cert_path, key_path)) = tls_paths {
            Some(TlsClientConfig {
                ca_cert_path: Some(ca_path),
                cert_path: Some(cert_path),
                key_path: Some(key_path),
            })
        } else {
            None
        };

        Self {
            hosts,
            tls,
            password,
            connection_timeout: Duration::from_secs(10),
            command_timeout: Duration::from_secs(30),
        }
    }
}

/// Valkey client for cluster operations.
pub struct ValkeyClient {
    client: Client,
    config: ValkeyClientConfig,
}

impl ValkeyClient {
    /// Create and connect a new Valkey client.
    #[instrument(skip(config), fields(hosts = ?config.hosts))]
    pub async fn connect(config: ValkeyClientConfig) -> Result<Self, ValkeyError> {
        if config.hosts.is_empty() {
            return Err(ValkeyError::InvalidConfig(
                "No hosts provided".to_string(),
            ));
        }

        // Build server configuration
        let servers: Vec<Server> = config
            .hosts
            .iter()
            .map(|(host, port)| Server::new(host.clone(), *port))
            .collect();

        let server_config = ServerConfig::Clustered {
            hosts: servers,
            policy: ClusterDiscoveryPolicy::ConfigEndpoint,
        };

        // Build Redis configuration
        let mut redis_config = Config {
            server: server_config,
            ..Default::default()
        };

        // Set password if provided
        if let Some(ref password) = config.password {
            redis_config.password = Some(password.clone());
        }

        // Configure TLS if enabled
        if config.tls.is_some() {
            let tls_connector = TlsConnector::default_rustls()
                .map_err(|e| ValkeyError::Connection(format!("TLS error: {}", e)))?;
            redis_config.tls = Some(tls_connector.into());
        }

        // Create client with performance and connection configuration
        let command_timeout = config.command_timeout;
        let connection_timeout = config.connection_timeout;

        let client = Builder::from_config(redis_config)
            .with_performance_config(|perf| {
                perf.default_command_timeout = command_timeout;
            })
            .with_connection_config(|conn| {
                conn.connection_timeout = connection_timeout;
            })
            .build()?;

        // Connect
        debug!("Connecting to Valkey cluster");
        client.init().await?;
        debug!("Connected to Valkey cluster");

        Ok(Self { client, config })
    }

    /// Create a client for connecting to a single node (not clustered).
    /// Useful for initial cluster setup before CLUSTER MEET.
    #[instrument(skip_all, fields(host = %host, port = %port))]
    pub async fn connect_single(
        host: &str,
        port: u16,
        password: Option<&str>,
        tls: bool,
    ) -> Result<Self, ValkeyError> {
        let server_config = ServerConfig::Centralized {
            server: Server::new(host, port),
        };

        let mut redis_config = Config {
            server: server_config,
            ..Default::default()
        };

        if let Some(pass) = password {
            redis_config.password = Some(pass.to_string());
        }

        if tls {
            let tls_connector = TlsConnector::default_rustls()
                .map_err(|e| ValkeyError::Connection(format!("TLS error: {}", e)))?;
            redis_config.tls = Some(tls_connector.into());
        }

        let client = Builder::from_config(redis_config)
            .with_performance_config(|perf| {
                perf.default_command_timeout = Duration::from_secs(30);
            })
            .with_connection_config(|conn| {
                conn.connection_timeout = Duration::from_secs(10);
            })
            .build()?;

        client.init().await?;

        let config = ValkeyClientConfig {
            hosts: vec![(host.to_string(), port)],
            password: password.map(|s| s.to_string()),
            ..Default::default()
        };

        Ok(Self { client, config })
    }

    /// Get the underlying fred client.
    pub fn inner(&self) -> &Client {
        &self.client
    }

    /// Get the client configuration.
    pub fn config(&self) -> &ValkeyClientConfig {
        &self.config
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        self.client.is_connected()
    }

    /// Close the connection.
    pub async fn close(&self) -> Result<(), ValkeyError> {
        self.client.quit().await?;
        Ok(())
    }

    /// Ping the server.
    #[instrument(skip(self))]
    pub async fn ping(&self) -> Result<String, ValkeyError> {
        let response: String = self.client.ping(None).await?;
        Ok(response)
    }

    /// Get server info.
    #[instrument(skip(self))]
    pub async fn info(&self, section: Option<InfoKind>) -> Result<String, ValkeyError> {
        let response: String = self.client.info(section).await?;
        Ok(response)
    }

    /// Get cluster info.
    #[instrument(skip(self))]
    pub async fn cluster_info_raw(&self) -> Result<String, ValkeyError> {
        let response: String = self.client.cluster_info().await?;
        Ok(response)
    }

    /// Get cluster nodes.
    #[instrument(skip(self))]
    pub async fn cluster_nodes_raw(&self) -> Result<String, ValkeyError> {
        let response: String = self.client.cluster_nodes().await?;
        Ok(response)
    }

    /// Execute CLUSTER MEET to introduce a new node.
    #[instrument(skip(self))]
    pub async fn cluster_meet(&self, ip: &str, port: u16) -> Result<(), ValkeyError> {
        self.client.cluster_meet(ip, port).await?;
        Ok(())
    }

    /// Execute CLUSTER REPLICATE to make this node a replica of the specified master.
    #[instrument(skip(self))]
    pub async fn cluster_replicate(&self, master_node_id: &str) -> Result<(), ValkeyError> {
        self.client.cluster_replicate(master_node_id).await?;
        Ok(())
    }

    /// Execute CLUSTER ADDSLOTS to assign slots to this node.
    #[instrument(skip(self), fields(slot_count = slots.len()))]
    pub async fn cluster_add_slots(&self, slots: Vec<u16>) -> Result<(), ValkeyError> {
        self.client.cluster_add_slots(slots).await?;
        Ok(())
    }

    /// Execute CLUSTER ADDSLOTSRANGE to assign slot ranges to this node.
    #[instrument(skip(self))]
    pub async fn cluster_add_slots_range(
        &self,
        ranges: Vec<(u16, u16)>,
    ) -> Result<(), ValkeyError> {
        for (start, end) in ranges {
            let slots: Vec<u16> = (start..=end).collect();
            self.client.cluster_add_slots(slots).await?;
        }
        Ok(())
    }

    /// Execute CLUSTER FAILOVER for a replica to take over from its master.
    #[instrument(skip(self))]
    pub async fn cluster_failover(&self) -> Result<(), ValkeyError> {
        self.client.cluster_failover(None).await?;
        Ok(())
    }

    /// Execute CLUSTER FAILOVER FORCE.
    #[instrument(skip(self))]
    pub async fn cluster_failover_force(&self) -> Result<(), ValkeyError> {
        self.client.cluster_failover(Some(ClusterFailoverFlag::Force)).await?;
        Ok(())
    }

    /// Execute CLUSTER FAILOVER TAKEOVER.
    #[instrument(skip(self))]
    pub async fn cluster_failover_takeover(&self) -> Result<(), ValkeyError> {
        self.client.cluster_failover(Some(ClusterFailoverFlag::Takeover)).await?;
        Ok(())
    }

    /// Execute CLUSTER FORGET to remove a node from the cluster.
    #[instrument(skip(self))]
    pub async fn cluster_forget(&self, node_id: &str) -> Result<(), ValkeyError> {
        self.client.cluster_forget(node_id).await?;
        Ok(())
    }

    /// Execute CLUSTER RESET to reset the node.
    #[instrument(skip(self))]
    pub async fn cluster_reset(&self, hard: bool) -> Result<(), ValkeyError> {
        let mode = if hard {
            Some(ClusterResetFlag::Hard)
        } else {
            Some(ClusterResetFlag::Soft)
        };
        self.client.cluster_reset(mode).await?;
        Ok(())
    }

    /// Execute CLUSTER SAVECONFIG to save cluster config to disk.
    #[instrument(skip(self))]
    pub async fn cluster_saveconfig(&self) -> Result<(), ValkeyError> {
        self.client.cluster_saveconfig().await?;
        Ok(())
    }

    // TODO: Add bgsave when i-server feature is enabled
    // /// Execute BGSAVE to trigger a background RDB save.
    // #[instrument(skip(self))]
    // pub async fn bgsave(&self) -> Result<(), ValkeyError> {
    //     self.client.bgsave(false).await?;
    //     Ok(())
    // }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ValkeyClientConfig::default();
        assert!(config.hosts.is_empty());
        assert!(config.tls.is_none());
        assert!(config.password.is_none());
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.command_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_config_builder() {
        let config = ValkeyClientConfig::new(vec![("localhost".to_string(), 6379)])
            .with_password("secret".to_string())
            .with_connection_timeout(Duration::from_secs(5))
            .with_command_timeout(Duration::from_secs(15));

        assert_eq!(config.hosts.len(), 1);
        assert_eq!(config.hosts[0], ("localhost".to_string(), 6379));
        assert_eq!(config.password, Some("secret".to_string()));
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
        assert_eq!(config.command_timeout, Duration::from_secs(15));
    }
}
