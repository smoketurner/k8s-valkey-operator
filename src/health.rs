//! Health server for Kubernetes probes and Prometheus metrics.
//!
//! Provides:
//! - `/healthz` - Liveness probe (always returns 200 if server is running)
//! - `/readyz` - Readiness probe (returns 200 when ready to serve traffic)
//! - `/metrics` - Prometheus metrics endpoint

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabel, EncodeLabelSet, LabelSetEncoder};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use tokio::sync::RwLock;
use tracing::info;

/// Labels for reconciliation metrics (namespace + name)
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ReconcileLabels {
    pub namespace: String,
    pub name: String,
}

impl EncodeLabelSet for ReconcileLabels {
    fn encode(&self, encoder: &mut LabelSetEncoder<'_>) -> Result<(), std::fmt::Error> {
        ("namespace", self.namespace.as_str()).encode(encoder.encode_label())?;
        ("name", self.name.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

/// Labels for phase-based metrics
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PhaseLabels {
    pub phase: String,
}

impl EncodeLabelSet for PhaseLabels {
    fn encode(&self, encoder: &mut LabelSetEncoder<'_>) -> Result<(), std::fmt::Error> {
        ("phase", self.phase.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

/// Shared metrics for the operator
pub struct Metrics {
    /// Total reconciliations counter
    pub reconciliations_total: Family<ReconcileLabels, Counter>,
    /// Failed reconciliations counter
    pub reconciliation_errors_total: Family<ReconcileLabels, Counter>,
    /// Reconciliation duration histogram
    pub reconcile_duration_seconds: Family<ReconcileLabels, Histogram>,
    /// Total resources by phase
    pub resources_total: Family<PhaseLabels, Gauge>,
    /// Desired replicas per resource
    pub resource_replicas_desired: Family<ReconcileLabels, Gauge>,
    /// Ready replicas per resource
    pub resource_replicas_ready: Family<ReconcileLabels, Gauge>,
    /// Prometheus registry
    registry: Registry,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Create a new metrics instance with registered metrics
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let reconciliations_total = Family::<ReconcileLabels, Counter>::default();
        registry.register(
            "myoperator_reconciliations",
            "Total number of reconciliations",
            reconciliations_total.clone(),
        );

        let reconciliation_errors_total = Family::<ReconcileLabels, Counter>::default();
        registry.register(
            "myoperator_reconciliation_errors",
            "Total number of reconciliation errors",
            reconciliation_errors_total.clone(),
        );

        let reconcile_duration_seconds =
            Family::<ReconcileLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.001, 2.0, 15))
            });
        registry.register(
            "myoperator_reconcile_duration_seconds",
            "Duration of reconciliation in seconds",
            reconcile_duration_seconds.clone(),
        );

        let resources_total = Family::<PhaseLabels, Gauge>::default();
        registry.register(
            "myoperator_resources_total",
            "Total number of MyResource resources by phase",
            resources_total.clone(),
        );

        let resource_replicas_desired = Family::<ReconcileLabels, Gauge>::default();
        registry.register(
            "myoperator_resource_replicas_desired",
            "Desired number of replicas for each resource",
            resource_replicas_desired.clone(),
        );

        let resource_replicas_ready = Family::<ReconcileLabels, Gauge>::default();
        registry.register(
            "myoperator_resource_replicas_ready",
            "Number of ready replicas for each resource",
            resource_replicas_ready.clone(),
        );

        Self {
            reconciliations_total,
            reconciliation_errors_total,
            reconcile_duration_seconds,
            resources_total,
            resource_replicas_desired,
            resource_replicas_ready,
            registry,
        }
    }

    /// Record a successful reconciliation
    pub fn record_reconcile(&self, namespace: &str, name: &str, duration_secs: f64) {
        let labels = ReconcileLabels {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        self.reconciliations_total.get_or_create(&labels).inc();
        self.reconcile_duration_seconds
            .get_or_create(&labels)
            .observe(duration_secs);
    }

    /// Record a failed reconciliation
    pub fn record_error(&self, namespace: &str, name: &str) {
        let labels = ReconcileLabels {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        self.reconciliation_errors_total
            .get_or_create(&labels)
            .inc();
    }

    /// Update resource count by phase
    pub fn set_resources_by_phase(&self, phase: &str, count: i64) {
        let labels = PhaseLabels {
            phase: phase.to_string(),
        };
        self.resources_total.get_or_create(&labels).set(count);
    }

    /// Update resource replica metrics
    pub fn set_resource_replicas(&self, namespace: &str, name: &str, desired: i64, ready: i64) {
        let labels = ReconcileLabels {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        self.resource_replicas_desired
            .get_or_create(&labels)
            .set(desired);
        self.resource_replicas_ready
            .get_or_create(&labels)
            .set(ready);
    }

    /// Encode metrics to Prometheus text format
    pub fn encode(&self) -> String {
        let mut buffer = String::new();
        if encode(&mut buffer, &self.registry).is_err() {
            tracing::error!("Failed to encode metrics");
            return "# Error encoding metrics".to_string();
        }
        buffer
    }
}

/// Shared state for the health server
pub struct HealthState {
    /// Whether the operator is ready (acquired leadership and running controller)
    ready: RwLock<bool>,
    /// Metrics registry
    pub metrics: Metrics,
    /// Last successful reconcile timestamp (Unix epoch seconds)
    pub last_reconcile: AtomicU64,
}

impl Default for HealthState {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthState {
    /// Create a new health state (starts as not ready)
    pub fn new() -> Self {
        Self {
            ready: RwLock::new(false),
            metrics: Metrics::new(),
            last_reconcile: AtomicU64::new(0),
        }
    }

    /// Mark the operator as ready or not ready
    pub async fn set_ready(&self, ready: bool) {
        *self.ready.write().await = ready;
    }

    /// Check if the operator is ready
    pub async fn is_ready(&self) -> bool {
        *self.ready.read().await
    }
}

/// Liveness probe handler
///
/// Returns 200 OK if the process is alive.
/// This is a simple check - if we can respond, we're alive.
async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Readiness probe handler
///
/// Returns 200 OK if the operator is ready to serve.
/// Returns 503 Service Unavailable if not ready.
async fn readyz(State(state): State<Arc<HealthState>>) -> Response {
    if state.is_ready().await {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

/// Metrics handler
async fn metrics_handler(State(state): State<Arc<HealthState>>) -> impl IntoResponse {
    let body = state.metrics.encode();
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// Create the health server router
pub fn create_router(state: Arc<HealthState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_handler))
        .with_state(state)
}

/// Run the health server
///
/// Binds to 0.0.0.0:8080 and serves health/metrics endpoints.
pub async fn run_health_server(state: Arc<HealthState>) -> Result<(), std::io::Error> {
    let app = create_router(state);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], 8080));
    info!(port = 8080, "Starting health server");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();
        metrics.record_reconcile("default", "test-resource", 0.5);
        metrics.record_error("default", "test-resource");

        let encoded = metrics.encode();
        assert!(encoded.contains("myoperator_reconciliations"));
        assert!(encoded.contains("myoperator_reconciliation_errors"));
        assert!(encoded.contains("myoperator_reconcile_duration_seconds"));
    }

    #[test]
    fn test_phase_metrics() {
        let metrics = Metrics::new();

        // Set resource counts by phase
        metrics.set_resources_by_phase("Running", 5);
        metrics.set_resources_by_phase("Degraded", 1);
        metrics.set_resources_by_phase("Creating", 2);

        let encoded = metrics.encode();
        assert!(encoded.contains("myoperator_resources_total"));
    }

    #[test]
    fn test_replica_metrics() {
        let metrics = Metrics::new();

        // Set replica metrics for resources
        metrics.set_resource_replicas("default", "prod-app", 3, 3);
        metrics.set_resource_replicas("staging", "staging-app", 2, 1);

        let encoded = metrics.encode();
        assert!(encoded.contains("myoperator_resource_replicas_desired"));
        assert!(encoded.contains("myoperator_resource_replicas_ready"));
    }

    #[tokio::test]
    async fn test_health_state() {
        let state = HealthState::new();
        assert!(!state.is_ready().await);

        state.set_ready(true).await;
        assert!(state.is_ready().await);
    }
}
