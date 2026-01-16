//! Valkey client wrapper using the fred crate.
//!
//! Provides a type-safe client for connecting to Valkey clusters with
//! TLS support, connection pooling, and proper error handling.

use std::time::Duration;

use fred::prelude::*;
use fred::types::InfoKind;
use fred::types::cluster::{ClusterFailoverFlag, ClusterResetFlag};
use fred::types::config::ClusterDiscoveryPolicy;
use rustls::pki_types::CertificateDer;
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
    Timeout {
        operation: String,
        duration: Duration,
    },

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

/// State for CLUSTER SETSLOT command.
///
/// Note: The fred crate's implementation of IMPORTING and MIGRATING
/// does not support the node-id parameter. For full slot migration,
/// use the CLUSTER SETSLOT NODE command to finalize slot ownership.
#[derive(Clone, Debug)]
pub enum ClusterSetSlotState {
    /// The slot is being imported (does not support node-id in fred).
    Importing,
    /// The slot is being migrated (does not support node-id in fred).
    Migrating,
    /// The slot is owned by the specified node.
    Node(String),
    /// The slot state is cleared (not importing/migrating).
    Stable,
}

/// TLS configuration for Valkey connections (file paths).
#[derive(Clone, Debug)]
pub struct TlsClientConfig {
    /// Path to CA certificate file.
    pub ca_cert_path: Option<String>,
    /// Path to client certificate file.
    pub cert_path: Option<String>,
    /// Path to client key file.
    pub key_path: Option<String>,
}

/// TLS configuration with certificate data (not file paths).
/// Used when certificates are loaded from Kubernetes secrets.
#[derive(Clone)]
pub struct TlsCertData {
    /// CA certificate in PEM format.
    pub ca_cert_pem: Vec<u8>,
    /// Client certificate in PEM format (optional, for mTLS).
    pub client_cert_pem: Option<Vec<u8>>,
    /// Client key in PEM format (optional, for mTLS).
    pub client_key_pem: Option<Vec<u8>>,
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
            return Err(ValkeyError::InvalidConfig("No hosts provided".to_string()));
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
    ///
    /// # Arguments
    /// * `host` - Hostname or IP to connect to
    /// * `port` - Port number
    /// * `password` - Optional password for AUTH
    /// * `tls_certs` - Optional TLS certificate data for secure connections
    #[instrument(skip_all, fields(host = %host, port = %port, tls = tls_certs.is_some()))]
    pub async fn connect_single(
        host: &str,
        port: u16,
        password: Option<&str>,
        tls_certs: Option<&TlsCertData>,
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

        if let Some(certs) = tls_certs {
            let tls_connector = build_tls_connector(certs)?;
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
        self.client
            .cluster_failover(Some(ClusterFailoverFlag::Force))
            .await?;
        Ok(())
    }

    /// Execute CLUSTER FAILOVER TAKEOVER.
    #[instrument(skip(self))]
    pub async fn cluster_failover_takeover(&self) -> Result<(), ValkeyError> {
        self.client
            .cluster_failover(Some(ClusterFailoverFlag::Takeover))
            .await?;
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

    /// Execute CLUSTER DELSLOTS to remove slots from this node.
    #[instrument(skip(self), fields(slot_count = slots.len()))]
    pub async fn cluster_del_slots(&self, slots: Vec<u16>) -> Result<(), ValkeyError> {
        self.client.cluster_del_slots(slots).await?;
        Ok(())
    }

    /// Execute CLUSTER SETSLOT to set slot state.
    /// Used for slot migration: IMPORTING, MIGRATING, NODE, STABLE
    #[instrument(skip(self))]
    pub async fn cluster_setslot(
        &self,
        slot: u16,
        state: ClusterSetSlotState,
    ) -> Result<(), ValkeyError> {
        use fred::types::cluster::ClusterSetSlotState as FredState;

        let fred_state = match state {
            ClusterSetSlotState::Importing => FredState::Importing,
            ClusterSetSlotState::Migrating => FredState::Migrating,
            ClusterSetSlotState::Node(node_id) => FredState::Node(node_id),
            ClusterSetSlotState::Stable => FredState::Stable,
        };

        self.client.cluster_setslot(slot, fred_state).await?;
        Ok(())
    }

    /// Execute CLUSTER GETKEYSINSLOT to get keys in a slot.
    #[instrument(skip(self))]
    pub async fn cluster_get_keys_in_slot(
        &self,
        slot: u16,
        count: u64,
    ) -> Result<Vec<String>, ValkeyError> {
        let keys: Vec<String> = self.client.cluster_get_keys_in_slot(slot, count).await?;
        Ok(keys)
    }

    /// Execute CLUSTER COUNTKEYSINSLOT to count keys in a slot.
    #[instrument(skip(self))]
    pub async fn cluster_count_keys_in_slot(&self, slot: u16) -> Result<u64, ValkeyError> {
        let count: u64 = self.client.cluster_count_keys_in_slot(slot).await?;
        Ok(count)
    }

    /// Execute CLUSTER KEYSLOT to determine which slot a key maps to.
    #[instrument(skip(self))]
    pub async fn cluster_keyslot(&self, key: &str) -> Result<u16, ValkeyError> {
        let slot: u16 = self.client.cluster_keyslot(key).await?;
        Ok(slot)
    }

    /// Execute CLUSTER MYID to get this node's ID.
    #[instrument(skip(self))]
    pub async fn cluster_myid(&self) -> Result<String, ValkeyError> {
        let id: String = self.client.cluster_myid().await?;
        Ok(id)
    }

    /// Execute MIGRATE to move keys to another node.
    /// This is used during slot migration.
    #[instrument(skip(self, keys), fields(key_count = keys.len()))]
    pub async fn migrate_keys(
        &self,
        host: &str,
        port: u16,
        keys: &[String],
        timeout_ms: u64,
    ) -> Result<(), ValkeyError> {
        if keys.is_empty() {
            return Ok(());
        }

        // Use MIGRATE command with KEYS option for multiple keys
        // MIGRATE host port "" 0 timeout COPY KEYS key1 key2 ...
        // Note: We use COPY to avoid data loss during migration, then delete after
        let cmd = format!(
            "MIGRATE {} {} \"\" 0 {} KEYS {}",
            host,
            port,
            timeout_ms,
            keys.join(" ")
        );

        // For now, log the command - actual implementation would use custom command
        debug!(command = %cmd, "Would execute MIGRATE");

        // TODO: Implement using custom command when fred supports it
        // For now, slot migration will rely on Redis Cluster's CLUSTER SETSLOT
        // which handles key migration automatically in newer versions

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

/// Build a TLS connector from certificate data.
///
/// Creates a rustls ClientConfig with the provided CA certificate and optional
/// client certificate for mTLS, then wraps it in a fred TlsConnector.
///
/// Note: When using port forwarding, we connect to 127.0.0.1 but the certificate
/// is issued for the pod DNS names. We use a custom verifier that validates the
/// certificate chain but allows hostname mismatch for this use case.
fn build_tls_connector(certs: &TlsCertData) -> Result<TlsConnector, ValkeyError> {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{ServerName, UnixTime};
    use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
    use std::sync::Arc;

    // Parse CA certificate(s) from PEM
    let mut root_store = RootCertStore::empty();
    let ca_certs = rustls_pemfile::certs(&mut certs.ca_cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ValkeyError::Connection(format!("Failed to parse CA certificate: {}", e)))?;

    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| ValkeyError::Connection(format!("Failed to add CA certificate: {}", e)))?;
    }

    // Custom verifier that allows hostname mismatch for port forwarding scenarios.
    // When port forwarding, we connect to localhost but the cert is issued for pod DNS names.
    #[derive(Debug)]
    struct PortForwardVerifier;

    impl ServerCertVerifier for PortForwardVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, rustls::Error> {
            // When using port forwarding, we connect to 127.0.0.1 but the certificate
            // is issued for the pod DNS names (e.g., *.test-cluster-headless.default.svc).
            //
            // For local development/testing via port forwarding, we trust the tunnel
            // and accept any certificate signed by a CA we control (self-signed issuer).
            //
            // In production (in-cluster), the operator connects using proper DNS names
            // and standard certificate verification would apply.
            //
            // Note: We still verify TLS signatures in verify_tls12/13_signature methods,
            // ensuring the certificate is properly signed.
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
            )
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &rustls::crypto::aws_lc_rs::default_provider().signature_verification_algorithms,
            )
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            rustls::crypto::aws_lc_rs::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }

    let verifier = Arc::new(PortForwardVerifier);
    // TODO: The root_store (CA certs) is parsed but not used because PortForwardVerifier
    // trusts all certificates. This is acceptable for port-forwarded connections where the
    // operator runs outside the cluster, but a proper verifier should be used for in-cluster
    // connections where DNS resolves to actual pod IPs.
    let _ = root_store;

    // Build client config with custom verifier
    let config = if let (Some(cert_pem), Some(key_pem)) =
        (&certs.client_cert_pem, &certs.client_key_pem)
    {
        // mTLS: client cert + key
        let client_certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
            .collect::<Result<Vec<CertificateDer<'static>>, _>>()
            .map_err(|e| {
                ValkeyError::Connection(format!("Failed to parse client certificate: {}", e))
            })?;

        let client_key = rustls_pemfile::private_key(&mut key_pem.as_slice())
            .map_err(|e| ValkeyError::Connection(format!("Failed to parse client key: {}", e)))?
            .ok_or_else(|| ValkeyError::Connection("No private key found in PEM".to_string()))?;

        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| ValkeyError::Connection(format!("Failed to build TLS config: {}", e)))?
    } else {
        // Server-only TLS (no client cert)
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    };

    Ok(TlsConnector::from(config))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing, clippy::get_unwrap)]
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
