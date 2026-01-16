//! Unit tests for valkey-operator.
//!
//! These tests run without a Kubernetes cluster and test individual
//! components in isolation.
//!
//! Test code is allowed to use expect() for error handling
#![allow(clippy::expect_used)]

#[path = "../common/mod.rs"]
mod common;

mod crd_tests {
    use valkey_operator::crd::{ClusterPhase, Condition};

    #[test]
    fn test_phase_display() {
        assert_eq!(ClusterPhase::Pending.to_string(), "Pending");
        assert_eq!(ClusterPhase::Creating.to_string(), "Creating");
        assert_eq!(ClusterPhase::Running.to_string(), "Running");
        assert_eq!(ClusterPhase::Updating.to_string(), "Updating");
        assert_eq!(ClusterPhase::Degraded.to_string(), "Degraded");
        assert_eq!(ClusterPhase::Failed.to_string(), "Failed");
        assert_eq!(ClusterPhase::Deleting.to_string(), "Deleting");
    }

    #[test]
    fn test_phase_default() {
        assert_eq!(ClusterPhase::default(), ClusterPhase::Pending);
    }

    #[test]
    fn test_condition_ready() {
        let condition = Condition::ready(true, "AllReady", "All components ready", Some(1));
        assert_eq!(condition.r#type, "Ready");
        assert_eq!(condition.status, "True");
        assert_eq!(condition.reason, "AllReady");
        assert_eq!(condition.message, "All components ready");
        assert_eq!(condition.observed_generation, Some(1));
    }

    #[test]
    fn test_condition_not_ready() {
        let condition = Condition::ready(false, "NotReady", "Components starting", None);
        assert_eq!(condition.status, "False");
    }

    #[test]
    fn test_condition_progressing() {
        let condition = Condition::progressing(true, "Reconciling", "Updating resources", Some(2));
        assert_eq!(condition.r#type, "Progressing");
        assert_eq!(condition.status, "True");
    }

    #[test]
    fn test_condition_degraded() {
        let condition = Condition::degraded(true, "PodFailed", "Pod in crash loop", Some(3));
        assert_eq!(condition.r#type, "Degraded");
        assert_eq!(condition.status, "True");
    }
}

mod state_machine_tests {
    use valkey_operator::controller::cluster_state_machine::{ClusterEvent, ClusterStateMachine};
    use valkey_operator::crd::ClusterPhase;

    #[test]
    fn test_valid_events_from_pending() {
        let sm = ClusterStateMachine::new();
        // Pending can transition via ResourcesApplied -> Creating
        assert!(sm.can_transition(&ClusterPhase::Pending, &ClusterEvent::ResourcesApplied));
        // Pending can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&ClusterPhase::Pending, &ClusterEvent::ReconcileError));
        // Pending can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&ClusterPhase::Pending, &ClusterEvent::DeletionRequested));
        // Pending cannot directly go to Running via AllReplicasReady
        assert!(!sm.can_transition(&ClusterPhase::Pending, &ClusterEvent::AllReplicasReady));
    }

    #[test]
    fn test_valid_events_from_creating() {
        let sm = ClusterStateMachine::new();
        // Creating can transition via AllReplicasReady -> Initializing (for cluster formation)
        assert!(sm.can_transition(&ClusterPhase::Creating, &ClusterEvent::AllReplicasReady));
        // Creating can transition via ReplicasDegraded -> Degraded
        assert!(sm.can_transition(&ClusterPhase::Creating, &ClusterEvent::ReplicasDegraded));
        // Creating can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&ClusterPhase::Creating, &ClusterEvent::ReconcileError));
        // Creating can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&ClusterPhase::Creating, &ClusterEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_running() {
        let sm = ClusterStateMachine::new();
        // Running can transition via SpecChanged -> Updating
        assert!(sm.can_transition(&ClusterPhase::Running, &ClusterEvent::SpecChanged));
        // Running can transition via ReplicasDegraded -> Degraded
        assert!(sm.can_transition(&ClusterPhase::Running, &ClusterEvent::ReplicasDegraded));
        // Running can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&ClusterPhase::Running, &ClusterEvent::DeletionRequested));
        // Running can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&ClusterPhase::Running, &ClusterEvent::ReconcileError));
    }

    #[test]
    fn test_valid_events_from_updating() {
        let sm = ClusterStateMachine::new();
        // Updating can transition via AllReplicasReady -> Running
        assert!(sm.can_transition(&ClusterPhase::Updating, &ClusterEvent::AllReplicasReady));
        // Updating can transition via ReplicasDegraded -> Degraded
        assert!(sm.can_transition(&ClusterPhase::Updating, &ClusterEvent::ReplicasDegraded));
        // Updating can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&ClusterPhase::Updating, &ClusterEvent::ReconcileError));
        // Updating can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&ClusterPhase::Updating, &ClusterEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_degraded() {
        let sm = ClusterStateMachine::new();
        // Degraded can transition via FullyRecovered -> Running
        assert!(sm.can_transition(&ClusterPhase::Degraded, &ClusterEvent::FullyRecovered));
        // Degraded can transition via AllReplicasReady -> Running
        assert!(sm.can_transition(&ClusterPhase::Degraded, &ClusterEvent::AllReplicasReady));
        // Degraded can transition via SpecChanged -> Updating
        assert!(sm.can_transition(&ClusterPhase::Degraded, &ClusterEvent::SpecChanged));
        // Degraded can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&ClusterPhase::Degraded, &ClusterEvent::ReconcileError));
        // Degraded can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&ClusterPhase::Degraded, &ClusterEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_failed() {
        let sm = ClusterStateMachine::new();
        // Failed can transition via RecoveryInitiated -> Pending
        assert!(sm.can_transition(&ClusterPhase::Failed, &ClusterEvent::RecoveryInitiated));
        // Failed can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&ClusterPhase::Failed, &ClusterEvent::DeletionRequested));
        // Failed cannot go directly to Running
        assert!(!sm.can_transition(&ClusterPhase::Failed, &ClusterEvent::AllReplicasReady));
    }

    #[test]
    fn test_deleting_is_terminal() {
        let sm = ClusterStateMachine::new();
        // Deleting cannot transition to any other state
        let valid_events = sm.valid_events(&ClusterPhase::Deleting);
        assert!(
            valid_events.is_empty(),
            "Deleting should have no valid events"
        );
    }

    #[test]
    fn test_deletion_from_all_states() {
        let sm = ClusterStateMachine::new();
        let states = vec![
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::Initializing,
            ClusterPhase::AssigningSlots,
            ClusterPhase::Running,
            ClusterPhase::Updating,
            ClusterPhase::Resharding,
            ClusterPhase::Degraded,
            ClusterPhase::Failed,
        ];

        for state in states {
            assert!(
                sm.can_transition(&state, &ClusterEvent::DeletionRequested),
                "Should be able to delete from {:?}",
                state
            );
        }
    }
}

mod error_tests {
    use valkey_operator::controller::error::Error;

    #[test]
    fn test_error_is_retryable() {
        let kube_err = Error::Kube(kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_string(),
            message: "timeout".to_string(),
            reason: "Timeout".to_string(),
            code: 504,
        }));
        assert!(kube_err.is_retryable());

        let validation_err = Error::Validation("invalid spec".to_string());
        assert!(!validation_err.is_retryable());

        let transient_err = Error::Transient("temporary failure".to_string());
        assert!(transient_err.is_retryable());
    }
}

mod status_tests {
    use valkey_operator::controller::status::{
        ConditionBuilder, get_condition_reason, is_condition_true,
    };
    use valkey_operator::crd::Condition;

    #[test]
    fn test_condition_builder() {
        let mut builder = ConditionBuilder::new();
        builder.ready(true, "AllReady", "Components ready", Some(1));
        builder.progressing(false, "ReconcileComplete", "Done", Some(1));
        let conditions = builder.build();

        assert_eq!(conditions.len(), 2);
        assert!(is_condition_true(&conditions, "Ready"));
        assert!(!is_condition_true(&conditions, "Progressing"));
    }

    #[test]
    fn test_condition_builder_updates_existing() {
        let mut builder = ConditionBuilder::new();
        builder.ready(false, "Starting", "Starting up", Some(1));
        builder.ready(true, "AllReady", "All ready now", Some(1));
        let conditions = builder.build();

        // Should only have one Ready condition (updated)
        assert_eq!(conditions.len(), 1);
        assert!(is_condition_true(&conditions, "Ready"));
    }

    #[test]
    fn test_is_condition_true_missing() {
        let conditions: Vec<Condition> = vec![];
        assert!(!is_condition_true(&conditions, "Ready"));
    }

    #[test]
    fn test_get_condition_reason() {
        let mut builder = ConditionBuilder::new();
        builder.ready(true, "AllReady", "Components ready", Some(1));
        builder.degraded(false, "Healthy", "All healthy", Some(1));
        let conditions = builder.build();

        assert_eq!(get_condition_reason(&conditions, "Ready"), Some("AllReady"));
        assert_eq!(
            get_condition_reason(&conditions, "Degraded"),
            Some("Healthy")
        );
        assert_eq!(get_condition_reason(&conditions, "Missing"), None);
    }

    #[test]
    fn test_condition_builder_default() {
        let builder = ConditionBuilder::default();
        let conditions = builder.build();
        assert!(conditions.is_empty());
    }

    #[test]
    fn test_condition_builder_all_types() {
        let mut builder = ConditionBuilder::new();
        builder.ready(true, "Ready", "Ready", Some(1));
        builder.progressing(true, "Progressing", "Progressing", Some(1));
        builder.degraded(false, "NotDegraded", "Not degraded", Some(1));
        let conditions = builder.build();

        assert_eq!(conditions.len(), 3);
        assert!(is_condition_true(&conditions, "Ready"));
        assert!(is_condition_true(&conditions, "Progressing"));
        assert!(!is_condition_true(&conditions, "Degraded"));
    }
}

mod builder_tests {
    use crate::common::fixtures::ValkeyClusterBuilder;
    use crate::common::fixtures::ValkeyUpgradeBuilder;

    #[test]
    fn test_valkey_cluster_builder_all_options() {
        let resource = ValkeyClusterBuilder::new("test-cluster")
            .namespace("test-ns")
            .masters(6)
            .replicas_per_master(2)
            .issuer_name("custom-issuer")
            .secret_name("custom-secret")
            .label("env", "test")
            .label("team", "platform")
            .annotation("description", "Test cluster")
            .generation(5)
            .uid("test-uid-123")
            .persistence(true, Some("10Gi"))
            .build();

        assert_eq!(resource.metadata.name.as_deref(), Some("test-cluster"));
        assert_eq!(resource.metadata.namespace.as_deref(), Some("test-ns"));
        assert_eq!(resource.spec.masters, 6);
        assert_eq!(resource.spec.replicas_per_master, 2);
        assert_eq!(resource.spec.tls.issuer_ref.name, "custom-issuer");
        assert_eq!(resource.spec.auth.secret_ref.name, "custom-secret");
        assert_eq!(resource.metadata.generation, Some(5));
        assert_eq!(resource.metadata.uid, Some("test-uid-123".to_string()));
        assert!(resource.spec.persistence.enabled);
        assert_eq!(resource.spec.persistence.size, "10Gi");
    }

    #[test]
    fn test_valkey_cluster_builder_persistence_default_size() {
        let resource = ValkeyClusterBuilder::new("test")
            .persistence(true, None::<String>)
            .build();

        assert!(resource.spec.persistence.enabled);
        assert_eq!(resource.spec.persistence.size, "1Gi");
    }

    #[test]
    fn test_valkey_cluster_builder_multiple_labels() {
        let mut labels = std::collections::BTreeMap::new();
        labels.insert("env".to_string(), "prod".to_string());
        labels.insert("region".to_string(), "us-east".to_string());

        let resource = ValkeyClusterBuilder::new("test")
            .labels(labels.clone())
            .build();

        assert_eq!(resource.metadata.labels, Some(labels));
    }

    #[test]
    fn test_valkey_upgrade_builder() {
        let upgrade = ValkeyUpgradeBuilder::new("test-upgrade")
            .namespace("test-ns")
            .cluster("my-cluster")
            .cluster_namespace("cluster-ns")
            .target_version("9.1.0")
            .replication_sync_timeout(600)
            .label("env", "test")
            .annotation("description", "Test upgrade")
            .build();

        assert_eq!(upgrade.metadata.name.as_deref(), Some("test-upgrade"));
        assert_eq!(upgrade.metadata.namespace.as_deref(), Some("test-ns"));
        assert_eq!(upgrade.spec.cluster_ref.name, "my-cluster");
        assert_eq!(
            upgrade.spec.cluster_ref.namespace,
            Some("cluster-ns".to_string())
        );
        assert_eq!(upgrade.spec.target_version, "9.1.0");
        assert_eq!(upgrade.spec.replication_sync_timeout_seconds, 600);
    }

    #[test]
    fn test_valkey_upgrade_builder_default_timeout() {
        let upgrade = ValkeyUpgradeBuilder::new("test-upgrade")
            .cluster("my-cluster")
            .target_version("9.1.0")
            .build();

        assert_eq!(upgrade.spec.replication_sync_timeout_seconds, 300);
    }
}

mod common_utils_tests {
    use valkey_operator::controller::common::extract_pod_name;

    #[test]
    fn test_extract_pod_name_full_dns() {
        assert_eq!(
            extract_pod_name("my-cluster-0.my-cluster-headless.default.svc.cluster.local:6379"),
            "my-cluster-0"
        );
    }

    #[test]
    fn test_extract_pod_name_short() {
        assert_eq!(extract_pod_name("my-cluster-0:6379"), "my-cluster-0");
    }

    #[test]
    fn test_extract_pod_name_no_port() {
        assert_eq!(extract_pod_name("my-cluster-0"), "my-cluster-0");
    }

    #[test]
    fn test_extract_pod_name_empty() {
        assert_eq!(extract_pod_name(""), "unknown");
    }

    #[test]
    fn test_extract_pod_name_invalid() {
        assert_eq!(extract_pod_name(":"), "unknown");
    }
}

mod crd_utils_tests {
    use valkey_operator::crd::total_pods;

    #[test]
    fn test_total_pods_basic() {
        assert_eq!(total_pods(3, 1), 6); // 3 masters + 3 replicas
        assert_eq!(total_pods(3, 2), 9); // 3 masters + 6 replicas
    }

    #[test]
    fn test_total_pods_no_replicas() {
        assert_eq!(total_pods(3, 0), 3); // 3 masters only
    }

    #[test]
    fn test_total_pods_large_cluster() {
        assert_eq!(total_pods(10, 2), 30); // 10 masters + 20 replicas
    }
}

mod resources_common_tests {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;
    use valkey_operator::crd::{AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyClusterSpec};
    use valkey_operator::resources::common::{
        owner_reference, pod_selector_labels, standard_annotations, standard_labels,
    };

    fn test_resource(name: &str) -> valkey_operator::crd::ValkeyCluster {
        valkey_operator::crd::ValkeyCluster {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("default".to_string()),
                uid: Some("test-uid".to_string()),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters: 3,
                replicas_per_master: 1,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "test-issuer".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-secret".to_string(),
                        ..Default::default()
                    },
                },
                labels: BTreeMap::new(),
                ..Default::default()
            },
            status: None,
        }
    }

    #[test]
    fn test_standard_labels_with_custom() {
        let mut resource = test_resource("my-cluster");
        resource
            .spec
            .labels
            .insert("env".to_string(), "prod".to_string());
        resource
            .spec
            .labels
            .insert("team".to_string(), "platform".to_string());

        let labels = standard_labels(&resource);

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(labels.get("env"), Some(&"prod".to_string()));
        assert_eq!(labels.get("team"), Some(&"platform".to_string()));
    }

    #[test]
    fn test_standard_annotations() {
        let mut resource = test_resource("my-cluster");
        resource
            .spec
            .annotations
            .insert("description".to_string(), "Test cluster".to_string());

        let annotations = standard_annotations(&resource);

        assert_eq!(
            annotations.get("description"),
            Some(&"Test cluster".to_string())
        );
    }

    #[test]
    fn test_pod_selector_labels() {
        let resource = test_resource("my-cluster");
        let labels = pod_selector_labels(&resource);

        assert_eq!(
            labels.get("app.kubernetes.io/name"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(
            labels.get("app.kubernetes.io/instance"),
            Some(&"my-cluster".to_string())
        );
        assert_eq!(labels.len(), 2); // Only selector labels, not all standard labels
    }

    #[test]
    fn test_owner_reference_uid() {
        let resource = test_resource("my-cluster");
        let owner_ref = owner_reference(&resource);

        assert_eq!(owner_ref.uid, "test-uid");
        assert_eq!(owner_ref.controller, Some(true));
        assert_eq!(owner_ref.block_owner_deletion, Some(true));
    }

    #[test]
    fn test_owner_reference_no_uid() {
        let mut resource = test_resource("my-cluster");
        resource.metadata.uid = None;

        let owner_ref = owner_reference(&resource);

        assert_eq!(owner_ref.uid, ""); // Should default to empty string
    }
}

mod validation_tests {
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;
    use valkey_operator::controller::cluster_validation::{
        MAX_REPLICAS_PER_MASTER, SpecDiff, generation_changed, validate_spec, validate_spec_change,
    };
    use valkey_operator::crd::{
        AuthSpec, IssuerRef, SecretKeyRef, TlsSpec, ValkeyCluster, ValkeyClusterSpec,
        ValkeyClusterStatus,
    };

    fn create_test_resource(masters: i32, replicas_per_master: i32) -> ValkeyCluster {
        ValkeyCluster {
            metadata: ObjectMeta {
                name: Some("test".to_string()),
                namespace: Some("default".to_string()),
                generation: Some(1),
                ..Default::default()
            },
            spec: ValkeyClusterSpec {
                masters,
                replicas_per_master,
                tls: TlsSpec {
                    issuer_ref: IssuerRef {
                        name: "test-issuer".to_string(),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                auth: AuthSpec {
                    secret_ref: SecretKeyRef {
                        name: "test-secret".to_string(),
                        ..Default::default()
                    },
                },
                labels: BTreeMap::new(),
                ..Default::default()
            },
            status: Some(ValkeyClusterStatus::default()),
        }
    }

    #[test]
    fn test_validate_replicas_per_master_negative() {
        let mut resource = create_test_resource(3, -1);
        resource.spec.replicas_per_master = -1;
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_replicas_per_master_too_high() {
        let mut resource = create_test_resource(3, MAX_REPLICAS_PER_MASTER + 1);
        resource.spec.replicas_per_master = MAX_REPLICAS_PER_MASTER + 1;
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_tls_required() {
        let mut resource = create_test_resource(3, 1);
        resource.spec.tls.issuer_ref.name = String::new();
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_validate_auth_required() {
        let mut resource = create_test_resource(3, 1);
        resource.spec.auth.secret_ref.name = String::new();
        assert!(validate_spec(&resource).is_err());
    }

    #[test]
    fn test_spec_diff_requires_update() {
        let old = create_test_resource(3, 1);
        let mut new = create_test_resource(3, 1);
        new.spec.image.tag = "9.1.0".to_string();

        let diff = validate_spec_change(&old, &new).expect("valid spec change should succeed");
        assert!(diff.image_changed);
        assert!(diff.requires_update());
    }

    #[test]
    fn test_spec_diff_resources_changed() {
        let old = create_test_resource(3, 1);
        let mut new = create_test_resource(3, 1);
        new.spec.resources.requests.cpu = "2".to_string();

        let diff = validate_spec_change(&old, &new).expect("valid spec change should succeed");
        assert!(diff.resources_changed);
        assert!(diff.requires_update());
    }

    #[test]
    fn test_spec_diff_labels_changed() {
        let old = create_test_resource(3, 1);
        let mut new = create_test_resource(3, 1);
        new.spec
            .labels
            .insert("env".to_string(), "prod".to_string());

        let diff = validate_spec_change(&old, &new).expect("valid spec change should succeed");
        assert!(diff.labels_changed);
        assert!(diff.requires_update());
    }

    #[test]
    fn test_spec_diff_scale_down_below_minimum() {
        let old = create_test_resource(3, 1);
        let new = create_test_resource(2, 1);

        assert!(validate_spec_change(&old, &new).is_err());
    }

    #[test]
    fn test_generation_changed_no_status() {
        let mut resource = create_test_resource(3, 1);
        resource.metadata.generation = Some(2);
        resource.status = None;

        assert!(generation_changed(&resource));
    }

    #[test]
    fn test_generation_changed_no_generation() {
        let mut resource = create_test_resource(3, 1);
        resource.metadata.generation = None;
        resource.status = Some(ValkeyClusterStatus {
            observed_generation: Some(1),
            ..Default::default()
        });

        assert!(!generation_changed(&resource));
    }

    #[test]
    fn test_spec_diff_total_pod_delta_scale_up() {
        let diff = SpecDiff::default();
        // Scale up: 3 masters + 1 replica = 6 pods -> 6 masters + 2 replicas = 18 pods
        let delta = diff.total_pod_delta(3, 1, 6, 2);
        assert_eq!(delta, 12);
    }

    #[test]
    fn test_spec_diff_total_pod_delta_scale_down() {
        let diff = SpecDiff::default();
        // Scale down: 6 masters + 2 replicas = 18 pods -> 3 masters + 1 replica = 6 pods
        let delta = diff.total_pod_delta(6, 2, 3, 1);
        assert_eq!(delta, -12);
    }
}
