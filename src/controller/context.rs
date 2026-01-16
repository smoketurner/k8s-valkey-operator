//! Shared context for the controller.
//!
//! The Context struct holds shared state that is passed to the reconciler,
//! including the Kubernetes client and event recorder.

use std::sync::Arc;

use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Api, Client, Resource, ResourceExt};

use crate::client::valkey_client::{TlsCertData, ValkeyClient};
use crate::controller::cluster_init;
use crate::controller::error::Error;
use crate::crd::{ValkeyCluster, ValkeyUpgrade};
use crate::health::HealthState;

/// Field manager name for the operator
pub const FIELD_MANAGER: &str = "valkey-operator";

/// Shared context for the controller
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Event reporter identity
    reporter: Reporter,
    /// Optional health state for metrics and readiness
    pub health_state: Option<Arc<HealthState>>,
}

impl Context {
    /// Create a new context
    pub fn new(client: Client, health_state: Option<Arc<HealthState>>) -> Self {
        Self {
            client,
            reporter: Reporter {
                controller: FIELD_MANAGER.into(),
                instance: std::env::var("POD_NAME").ok(),
            },
            health_state,
        }
    }

    /// Create an event recorder for publishing Kubernetes events
    fn recorder(&self) -> Recorder {
        Recorder::new(self.client.clone(), self.reporter.clone())
    }

    /// Publish a normal event for a ValkeyCluster resource
    pub async fn publish_normal_event(
        &self,
        resource: &ValkeyCluster,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        let recorder = self.recorder();
        let object_ref = resource.object_ref(&());
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: EventType::Normal,
                    reason: reason.into(),
                    note,
                    action: action.into(),
                    secondary: None,
                },
                &object_ref,
            )
            .await
        {
            tracing::warn!(reason = %reason, error = %e, "Failed to publish event");
        }
    }

    /// Publish a warning event for a ValkeyCluster resource
    pub async fn publish_warning_event(
        &self,
        resource: &ValkeyCluster,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        let recorder = self.recorder();
        let object_ref = resource.object_ref(&());
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: EventType::Warning,
                    reason: reason.into(),
                    note,
                    action: action.into(),
                    secondary: None,
                },
                &object_ref,
            )
            .await
        {
            tracing::warn!(reason = %reason, error = %e, "Failed to publish warning event");
        }
    }

    /// Publish a normal event for a ValkeyUpgrade resource
    pub async fn publish_upgrade_event(
        &self,
        resource: &ValkeyUpgrade,
        reason: &str,
        action: &str,
        note: Option<String>,
    ) {
        let recorder = self.recorder();
        let object_ref = resource.object_ref(&());
        if let Err(e) = recorder
            .publish(
                &Event {
                    type_: EventType::Normal,
                    reason: reason.into(),
                    note,
                    action: action.into(),
                    secondary: None,
                },
                &object_ref,
            )
            .await
        {
            tracing::warn!(reason = %reason, error = %e, "Failed to publish upgrade event");
        }
    }

    /// Retrieve password from the auth secret.
    pub async fn get_auth_password(
        &self,
        namespace: &str,
        secret_name: &str,
        secret_key: &str,
    ) -> Result<Option<String>, Error> {
        let secret_api: Api<k8s_openapi::api::core::v1::Secret> =
            Api::namespaced(self.client.clone(), namespace);

        match secret_api.get(secret_name).await {
            Ok(secret) => {
                if let Some(data) = secret.data
                    && let Some(password_bytes) = data.get(secret_key)
                {
                    let password = String::from_utf8(password_bytes.0.clone()).map_err(|e| {
                        Error::Validation(format!("Invalid password encoding: {}", e))
                    })?;
                    return Ok(Some(password));
                }
                Ok(None)
            }
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
            Err(e) => Err(Error::Kube(e)),
        }
    }

    /// Retrieve TLS certificates from the TLS secret created by cert-manager.
    pub async fn get_tls_certs(
        &self,
        namespace: &str,
        cluster_name: &str,
    ) -> Result<Option<TlsCertData>, Error> {
        let secret_name = format!("{}-tls", cluster_name);
        let secret_api: Api<k8s_openapi::api::core::v1::Secret> =
            Api::namespaced(self.client.clone(), namespace);

        match secret_api.get(&secret_name).await {
            Ok(secret) => {
                if let Some(data) = secret.data {
                    let ca_cert = match data.get("ca.crt") {
                        Some(bytes) => bytes.0.clone(),
                        None => {
                            tracing::warn!(secret = %secret_name, "ca.crt not found in TLS secret");
                            return Ok(None);
                        }
                    };
                    let client_cert = data.get("tls.crt").map(|b| b.0.clone());
                    let client_key = data.get("tls.key").map(|b| b.0.clone());

                    return Ok(Some(TlsCertData {
                        ca_cert_pem: ca_cert,
                        client_cert_pem: client_cert,
                        client_key_pem: client_key,
                    }));
                }
                Ok(None)
            }
            Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
            Err(e) => Err(Error::Kube(e)),
        }
    }

    /// Connect to a cluster node using the shared connection strategy.
    pub async fn connect_to_cluster(
        &self,
        cluster: &ValkeyCluster,
        namespace: &str,
        ordinal: i32,
    ) -> Result<ValkeyClient, Error> {
        let password = self
            .get_auth_password(
                namespace,
                &cluster.spec.auth.secret_ref.name,
                &cluster.spec.auth.secret_ref.key,
            )
            .await?;

        let tls_certs = self.get_tls_certs(namespace, &cluster.name_any()).await?;

        let strategy = cluster_init::create_connection_strategy(
            self.client.clone(),
            namespace,
            &cluster.name_any(),
        );
        let (connect_host, connect_port) = strategy
            .get_connection(ordinal)
            .await
            .map_err(|e| Error::Valkey(e.to_string()))?;

        ValkeyClient::connect_single(
            &connect_host,
            connect_port,
            password.as_deref(),
            tls_certs.as_ref(),
        )
        .await
        .map_err(|e| Error::Valkey(e.to_string()))
    }

    /// Connect to a specific host/port using the cluster's auth and TLS config.
    pub async fn connect_to_host(
        &self,
        cluster: &ValkeyCluster,
        namespace: &str,
        host: &str,
        port: u16,
    ) -> Result<ValkeyClient, Error> {
        let password = self
            .get_auth_password(
                namespace,
                &cluster.spec.auth.secret_ref.name,
                &cluster.spec.auth.secret_ref.key,
            )
            .await?;

        let tls_certs = self.get_tls_certs(namespace, &cluster.name_any()).await?;

        ValkeyClient::connect_single(host, port, password.as_deref(), tls_certs.as_ref())
            .await
            .map_err(|e| Error::Valkey(e.to_string()))
    }

    /// Build shared connection context for cluster operations.
    pub async fn connection_context(
        &self,
        cluster: &ValkeyCluster,
        namespace: &str,
    ) -> Result<
        (
            Option<String>,
            Option<TlsCertData>,
            cluster_init::ConnectionStrategy,
        ),
        Error,
    > {
        let password = self
            .get_auth_password(
                namespace,
                &cluster.spec.auth.secret_ref.name,
                &cluster.spec.auth.secret_ref.key,
            )
            .await?;
        let tls_certs = self.get_tls_certs(namespace, &cluster.name_any()).await?;
        let strategy = cluster_init::create_connection_strategy(
            self.client.clone(),
            namespace,
            &cluster.name_any(),
        );
        Ok((password, tls_certs, strategy))
    }
}
