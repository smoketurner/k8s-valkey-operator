use std::collections::HashMap;

use kube::Client;
use tokio::sync::RwLock;
use tracing::info;

use crate::controller::cluster_init::{pod_dns_name, pod_name};
use crate::controller::cluster_topology::DEFAULT_VALKEY_PORT;
use crate::crd::PodOrdinal;
use crate::resources::port_forward::{PortForward, PortForwardTarget};

/// How the operator reaches a Valkey pod. The authoritative value is
/// computed once in `main.rs` from which `kube::Config` constructor
/// succeeds, then passed into `Context::new`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TransportMode {
    /// Operator runs inside the cluster. Dial the pod DNS name:6379.
    InCluster,
    /// Operator runs locally. Tunnel via a cached kube-rs SPDY port-forward.
    LocalForward,
}

impl TransportMode {
    /// Read the `VALKEY_OPERATOR_TRANSPORT_MODE` override.
    ///
    /// Returns `Some(InCluster)` for `in-cluster`, `Some(LocalForward)` for
    /// `local-forward`, `None` when unset. Any other value is a hard error.
    pub fn from_env() -> Result<Option<Self>, TransportError> {
        match std::env::var("VALKEY_OPERATOR_TRANSPORT_MODE") {
            Ok(v) => Self::parse_mode(&v).map(Some),
            Err(_) => Ok(None),
        }
    }

    fn parse_mode(value: &str) -> Result<Self, TransportError> {
        match value {
            "in-cluster" => Ok(Self::InCluster),
            "local-forward" => Ok(Self::LocalForward),
            other => Err(TransportError::InvalidMode(other.to_string())),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum TransportError {
    #[error("Pod {ordinal} not resolvable: {reason}")]
    NotResolvable {
        ordinal: PodOrdinal,
        reason: String,
    },

    #[error("Port forward to pod {ordinal} failed: {reason}")]
    PortForwardFailed {
        ordinal: PodOrdinal,
        reason: String,
    },

    #[error(
        "Invalid VALKEY_OPERATOR_TRANSPORT_MODE '{0}' \
         (expected 'in-cluster' or 'local-forward')"
    )]
    InvalidMode(String),
}

/// Resolved connection target for a single pod.
///
/// `host`/`port` are what the fred client dials. `tls_server_name`
/// is the pod DNS name the TLS verifier checks against the cert SAN.
#[derive(Clone, Debug)]
pub struct ResolvedEndpoint {
    pub host: String,
    pub port: u16,
    pub tls_server_name: String,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    namespace: String,
    cluster_name: String,
    ordinal: PodOrdinal,
}

struct CacheEntry {
    endpoint: ResolvedEndpoint,
    #[allow(dead_code)]
    port_forward: PortForward,
}

/// Per-cluster connection cache keyed by (namespace, cluster_name, ordinal).
///
/// Shared across all reconcile invocations via `Arc`. Interior
/// mutability via `tokio::sync::RwLock` on the cache map.
pub struct TransportPool {
    mode: TransportMode,
    client: Client,
    cache: RwLock<HashMap<CacheKey, CacheEntry>>,
}

impl TransportPool {
    pub fn new(mode: TransportMode, client: Client) -> Self {
        info!(?mode, "Transport pool created");
        Self {
            mode,
            client,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn mode(&self) -> TransportMode {
        self.mode
    }

    /// Resolve a connectable endpoint for the given pod.
    pub async fn resolve(
        &self,
        namespace: &str,
        cluster_name: &str,
        ordinal: impl Into<PodOrdinal>,
    ) -> Result<ResolvedEndpoint, TransportError> {
        let ordinal = ordinal.into();
        let dns_name = pod_dns_name(cluster_name, namespace, ordinal);

        if self.mode == TransportMode::InCluster {
            return Ok(ResolvedEndpoint {
                host: dns_name.clone(),
                port: DEFAULT_VALKEY_PORT,
                tls_server_name: dns_name,
            });
        }

        let key = CacheKey {
            namespace: namespace.into(),
            cluster_name: cluster_name.into(),
            ordinal,
        };

        // Fast path: read lock, return live cached endpoint.
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&key) {
                return Ok(entry.endpoint.clone());
            }
        }

        // Slow path: write lock, double-check, then create the forward.
        let mut cache = self.cache.write().await;
        if let Some(entry) = cache.get(&key) {
            return Ok(entry.endpoint.clone());
        }

        let pod = pod_name(cluster_name, ordinal);
        let pf = PortForward::start(
            self.client.clone(),
            namespace,
            PortForwardTarget::pod(&pod, DEFAULT_VALKEY_PORT),
            None,
        )
        .await
        .map_err(|e| TransportError::PortForwardFailed {
            ordinal,
            reason: e.to_string(),
        })?;

        let endpoint = ResolvedEndpoint {
            host: "127.0.0.1".into(),
            port: pf.local_port(),
            tls_server_name: dns_name,
        };

        cache.insert(
            key,
            CacheEntry {
                endpoint: endpoint.clone(),
                port_forward: pf,
            },
        );

        Ok(endpoint)
    }

    /// Evict a single pod's cached transport.
    pub async fn evict(
        &self,
        namespace: &str,
        cluster_name: &str,
        ordinal: impl Into<PodOrdinal>,
    ) {
        let key = CacheKey {
            namespace: namespace.into(),
            cluster_name: cluster_name.into(),
            ordinal: ordinal.into(),
        };
        let removed = self.cache.write().await.remove(&key);
        drop(removed);
    }

    /// Evict all cached transports for a cluster.
    pub async fn evict_cluster(&self, namespace: &str, cluster_name: &str) {
        self.cache.write().await.retain(|k, _| {
            k.namespace != namespace || k.cluster_name != cluster_name
        });
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
mod tests {
    use super::*;

    #[test]
    fn parse_mode_in_cluster() {
        let result = TransportMode::parse_mode("in-cluster").unwrap();
        assert_eq!(result, TransportMode::InCluster);
    }

    #[test]
    fn parse_mode_local_forward() {
        let result = TransportMode::parse_mode("local-forward").unwrap();
        assert_eq!(result, TransportMode::LocalForward);
    }

    #[test]
    fn parse_mode_invalid() {
        let result = TransportMode::parse_mode("bogus");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, TransportError::InvalidMode(ref v) if v == "bogus")
        );
    }

    #[test]
    fn cache_key_equality_and_hashing() {
        use std::collections::HashSet;

        let k1 = CacheKey {
            namespace: "ns1".into(),
            cluster_name: "c1".into(),
            ordinal: PodOrdinal::new(0),
        };
        let k2 = CacheKey {
            namespace: "ns1".into(),
            cluster_name: "c1".into(),
            ordinal: PodOrdinal::new(1),
        };
        let k3 = CacheKey {
            namespace: "ns2".into(),
            cluster_name: "c1".into(),
            ordinal: PodOrdinal::new(0),
        };

        let mut set = HashSet::new();
        set.insert(k1.clone());
        assert!(set.contains(&k1));
        assert!(!set.contains(&k2));
        assert!(!set.contains(&k3));
    }

    #[tokio::test]
    async fn in_cluster_resolve_returns_dns_name() {
        let client = kube::Client::try_default()
            .await
            .unwrap_or_else(|_| {
                // Tests may run without a cluster; build a dummy client
                // that is never used (InCluster resolve is pure).
                kube::Client::try_from(
                    kube::Config::new(
                        "https://localhost:6443".parse().unwrap(),
                    ),
                )
                .unwrap()
            });

        let pool = TransportPool::new(TransportMode::InCluster, client);
        let ep = pool.resolve("default", "my-cluster", 2).await.unwrap();

        assert_eq!(
            ep.host,
            "my-cluster-2.my-cluster-headless.default.svc.cluster.local"
        );
        assert_eq!(ep.port, 6379);
        assert_eq!(ep.tls_server_name, ep.host);
    }

    #[tokio::test]
    async fn in_cluster_resolve_is_deterministic() {
        let client = kube::Client::try_from(
            kube::Config::new(
                "https://localhost:6443".parse().unwrap(),
            ),
        )
        .unwrap();

        let pool = TransportPool::new(TransportMode::InCluster, client);
        let ep1 = pool.resolve("prod", "valkey", 0).await.unwrap();
        let ep2 = pool.resolve("prod", "valkey", 0).await.unwrap();

        assert_eq!(ep1.host, ep2.host);
        assert_eq!(ep1.port, ep2.port);
        assert_eq!(ep1.tls_server_name, ep2.tls_server_name);
    }

    #[tokio::test]
    async fn in_cluster_evict_is_noop() {
        let client = kube::Client::try_from(
            kube::Config::new(
                "https://localhost:6443".parse().unwrap(),
            ),
        )
        .unwrap();

        let pool = TransportPool::new(TransportMode::InCluster, client);

        // Evict should not panic on empty cache
        pool.evict("ns", "cluster", 0).await;
        pool.evict_cluster("ns", "cluster").await;

        // Resolve still works after eviction
        let ep = pool.resolve("ns", "cluster", 0).await.unwrap();
        assert_eq!(
            ep.host,
            "cluster-0.cluster-headless.ns.svc.cluster.local"
        );
    }

    #[test]
    fn transport_error_display() {
        let e = TransportError::InvalidMode("bogus".into());
        assert!(e.to_string().contains("bogus"));
        assert!(e.to_string().contains("in-cluster"));

        let e = TransportError::PortForwardFailed {
            ordinal: PodOrdinal::new(3),
            reason: "timeout".into(),
        };
        assert!(e.to_string().contains("3"));
        assert!(e.to_string().contains("timeout"));
    }
}
