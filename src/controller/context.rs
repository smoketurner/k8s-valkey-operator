//! Shared context for the controller.
//!
//! The Context struct holds shared state that is passed to the reconciler,
//! including the Kubernetes client and event recorder.

use std::sync::Arc;

use kube::runtime::events::{Event, EventType, Recorder, Reporter};
use kube::{Client, Resource};

use crate::crd::MyResource;
use crate::health::HealthState;

/// Field manager name for the operator
pub const FIELD_MANAGER: &str = "my-operator";

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

    /// Publish a normal event for a resource
    pub async fn publish_normal_event(
        &self,
        resource: &MyResource,
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

    /// Publish a warning event for a resource
    pub async fn publish_warning_event(
        &self,
        resource: &MyResource,
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
}
