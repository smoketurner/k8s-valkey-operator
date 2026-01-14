//! Common resource generation utilities.
//!
//! Provides functions for creating standard Kubernetes resources with proper
//! labels, owner references, and configurations.
//!
//! NOTE: This is a placeholder implementation. Phase 6 will replace Deployment
//! with StatefulSet and add proper Valkey cluster configuration.

use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{ConfigMap, Service},
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::ResourceExt;
use std::collections::BTreeMap;

use crate::crd::{ValkeyCluster, total_pods};

/// Standard labels applied to all managed resources
pub fn standard_labels(resource: &ValkeyCluster) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app.kubernetes.io/name".to_string(), resource.name_any());
    labels.insert(
        "app.kubernetes.io/managed-by".to_string(),
        "valkey-operator".to_string(),
    );
    labels.insert(
        "app.kubernetes.io/component".to_string(),
        "valkeycluster".to_string(),
    );

    // Merge user-defined labels
    for (key, value) in &resource.spec.labels {
        labels.insert(key.clone(), value.clone());
    }

    labels
}

/// Create owner reference for a ValkeyCluster
pub fn owner_reference(resource: &ValkeyCluster) -> OwnerReference {
    OwnerReference {
        api_version: "valkeyoperator.smoketurner.com/v1alpha1".to_string(),
        kind: "ValkeyCluster".to_string(),
        name: resource.name_any(),
        uid: resource.uid().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

/// Generate a ConfigMap for a ValkeyCluster
///
/// NOTE: In Phase 6, this will be replaced with configuration via VALKEY_EXTRA_FLAGS env var.
pub fn generate_configmap(resource: &ValkeyCluster) -> ConfigMap {
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
            data.insert("masters".to_string(), resource.spec.masters.to_string());
            data.insert(
                "replicas_per_master".to_string(),
                resource.spec.replicas_per_master.to_string(),
            );
            data.insert(
                "total_pods".to_string(),
                total_pods(resource.spec.masters, resource.spec.replicas_per_master).to_string(),
            );
            data
        }),
        ..Default::default()
    }
}

/// Generate a Deployment for a ValkeyCluster
///
/// NOTE: Phase 6 will replace this with a StatefulSet for proper Valkey cluster operation.
pub fn generate_deployment(resource: &ValkeyCluster) -> Deployment {
    let name = resource.name_any();
    let labels = standard_labels(resource);
    let replicas = total_pods(resource.spec.masters, resource.spec.replicas_per_master);

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
                        name: "valkey".to_string(),
                        image: Some(format!(
                            "{}:{}",
                            resource.spec.image.repository, resource.spec.image.tag
                        )),
                        ports: Some(vec![
                            k8s_openapi::api::core::v1::ContainerPort {
                                container_port: 6379,
                                name: Some("client".to_string()),
                                ..Default::default()
                            },
                            k8s_openapi::api::core::v1::ContainerPort {
                                container_port: 16379,
                                name: Some("cluster-bus".to_string()),
                                ..Default::default()
                            },
                        ]),
                        resources: Some(k8s_openapi::api::core::v1::ResourceRequirements {
                            limits: Some({
                                let mut limits = BTreeMap::new();
                                limits.insert(
                                    "cpu".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        resource.spec.resources.limits.cpu.clone(),
                                    ),
                                );
                                limits.insert(
                                    "memory".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        resource.spec.resources.limits.memory.clone(),
                                    ),
                                );
                                limits
                            }),
                            requests: Some({
                                let mut requests = BTreeMap::new();
                                requests.insert(
                                    "cpu".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        resource.spec.resources.requests.cpu.clone(),
                                    ),
                                );
                                requests.insert(
                                    "memory".to_string(),
                                    k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                        resource.spec.resources.requests.memory.clone(),
                                    ),
                                );
                                requests
                            }),
                            ..Default::default()
                        }),
                        security_context: Some(k8s_openapi::api::core::v1::SecurityContext {
                            allow_privilege_escalation: Some(false),
                            read_only_root_filesystem: Some(false), // Valkey needs /data
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
                        run_as_user: Some(999), // Valkey user
                        fs_group: Some(999),
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

/// Generate a Service for a ValkeyCluster
pub fn generate_service(resource: &ValkeyCluster) -> Service {
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
                port: 6379,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::String(
                        "client".to_string(),
                    ),
                ),
                name: Some("client".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}
