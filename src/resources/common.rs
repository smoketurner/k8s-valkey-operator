//! Common resource generation utilities.
//!
//! Provides functions for creating standard Kubernetes resources with proper
//! labels, owner references, and configurations.

use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Service},
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use std::collections::BTreeMap;

use crate::crd::MyResource;

/// Standard labels applied to all managed resources
pub fn standard_labels(resource: &MyResource) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), resource.name_any());
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "my-operator".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "myresource".to_string(),
    );

    // Merge user-defined labels
    for (key, value) in &resource.spec.labels {
        labels.insert(key.clone(), value.clone());
    }

    labels
}

/// Create owner reference for a MyResource
pub fn owner_reference(resource: &MyResource) -> OwnerReference {
    OwnerReference {
        api_version: "myoperator.example.com/v1alpha1".to_string(),
        kind: "MyResource".to_string(),
        name: resource.name_any(),
        uid: resource.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Generate a ConfigMap for a MyResource
pub fn generate_configmap(resource: &MyResource) -> ConfigMap {
    let name = resource.name_any();
    let labels = standard_labels(resource);

    ConfigMap {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.clone()),
            namespace: resource.namespace(),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        data: Some({
            let mut data = BTreeMap::new();
            data.insert("message".to_string(), resource.spec.message.clone());
            data.insert("replicas".to_string(), resource.spec.replicas.to_string());
            data
        }),
        ..Default::default()
    }
}

/// Generate a Deployment for a MyResource
pub fn generate_deployment(resource: &MyResource) -> Deployment {
    let name = resource.name_any();
    let labels = standard_labels(resource);
    let replicas = resource.spec.replicas;

    Deployment {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.clone()),
            namespace: resource.namespace(),
            labels: Some(labels.clone()),
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::apps::v1::DeploymentSpec {
            replicas: Some(replicas),
            selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                match_labels: Some({
                    let mut selector = BTreeMap::new();
                    selector.insert("app.kubernetes.io/name".to_string(), name.clone());
                    selector
                }),
                ..Default::default()
            },
            template: k8s_openapi::api::core::v1::PodTemplateSpec {
                metadata: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(k8s_openapi::api::core::v1::PodSpec {
                    containers: vec![k8s_openapi::api::core::v1::Container {
                        name: "main".to_string(),
                        // Use a simple nginx image as placeholder
                        image: Some("nginx:alpine".to_string()),
                        ports: Some(vec![k8s_openapi::api::core::v1::ContainerPort {
                            container_port: 80,
                            name: Some("http".to_string()),
                            ..Default::default()
                        }]),
                        env: Some(vec![k8s_openapi::api::core::v1::EnvVar {
                            name: "MESSAGE".to_string(),
                            value: Some(resource.spec.message.clone()),
                            ..Default::default()
                        }]),
                        resources: Some(k8s_openapi::api::core::v1::ResourceRequirements {
                            limits: Some({
                                let mut limits = BTreeMap::new();
                                limits.insert(
                                    "cpu".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        "100m".to_string(),
                                    ),
                                );
                                limits.insert(
                                    "memory".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        "128Mi".to_string(),
                                    ),
                                );
                                limits
                            }),
                            requests: Some({
                                let mut requests = BTreeMap::new();
                                requests.insert(
                                    "cpu".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        "50m".to_string(),
                                    ),
                                );
                                requests.insert(
                                    "memory".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        "64Mi".to_string(),
                                    ),
                                );
                                requests
                            }),
                            ..Default::default()
                        }),
                        security_context: Some(k8s_openapi::api::core::v1::SecurityContext {
                            allow_privilege_escalation: Some(false),
                            read_only_root_filesystem: Some(true),
                            run_as_non_root: Some(true),
                            capabilities: Some(k8s_openapi::api::core::v1::Capabilities {
                                drop: Some(vec!["ALL".to_string()]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    security_context: Some(k8s_openapi::api::core::v1::PodSecurityContext {
                        run_as_non_root: Some(true),
                        run_as_user: Some(1000),
                        fs_group: Some(1000),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate a Service for a MyResource
pub fn generate_service(resource: &MyResource) -> Service {
    let name = resource.name_any();
    let labels = standard_labels(resource);

    Service {
        metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
            name: Some(name.clone()),
            namespace: resource.namespace(),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(resource)]),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::core::v1::ServiceSpec {
            type_: Some("ClusterIP".to_string()),
            selector: Some({
                let mut selector = BTreeMap::new();
                selector.insert("app.kubernetes.io/name".to_string(), name);
                selector
            }),
            ports: Some(vec![k8s_openapi::api::core::v1::ServicePort {
                port: 80,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        "http".to_string(),
                    ),
                ),
                name: Some("http".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}
