// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

//! Property-based tests for my-operator.
//!
//! Uses proptest to generate random inputs and verify invariants.

use proptest::prelude::*;

use my_operator::controller::state_machine::{ResourceEvent, ResourceStateMachine};
use my_operator::crd::Phase;

/// Strategy for generating valid replica counts.
fn valid_replicas() -> impl Strategy<Value = i32> {
    1..=10i32
}

/// Strategy for generating valid messages.
fn valid_message() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9 ]{1,100}".prop_map(|s| s.to_string())
}

/// Strategy for generating random phases.
fn any_phase() -> impl Strategy<Value = Phase> {
    prop_oneof![
        Just(Phase::Pending),
        Just(Phase::Creating),
        Just(Phase::Running),
        Just(Phase::Updating),
        Just(Phase::Degraded),
        Just(Phase::Failed),
        Just(Phase::Deleting),
    ]
}

/// Strategy for generating random events.
fn any_event() -> impl Strategy<Value = ResourceEvent> {
    prop_oneof![
        Just(ResourceEvent::ResourcesApplied),
        Just(ResourceEvent::AllReplicasReady),
        Just(ResourceEvent::ReplicasDegraded),
        Just(ResourceEvent::SpecChanged),
        Just(ResourceEvent::ReconcileError),
        Just(ResourceEvent::DeletionRequested),
        Just(ResourceEvent::RecoveryInitiated),
        Just(ResourceEvent::FullyRecovered),
    ]
}

proptest! {
    /// Property: Replicas must be between 1 and 10.
    #[test]
    fn test_replica_bounds(replicas in valid_replicas()) {
        prop_assert!(replicas >= 1);
        prop_assert!(replicas <= 10);
    }

    /// Property: Messages are non-empty.
    #[test]
    fn test_message_non_empty(message in valid_message()) {
        prop_assert!(!message.is_empty());
    }

    /// Property: State machine transition checks are deterministic.
    /// Same (phase, event) pair always yields the same result.
    #[test]
    fn test_state_transitions_deterministic(
        phase in any_phase(),
        event in any_event()
    ) {
        let sm = ResourceStateMachine::new();
        let result1 = sm.can_transition(&phase, &event);
        let result2 = sm.can_transition(&phase, &event);
        prop_assert_eq!(result1, result2);
    }

    /// Property: Deleting phase cannot transition to anything.
    /// Once in Deleting, no events trigger a transition.
    #[test]
    fn test_deleting_is_terminal(event in any_event()) {
        let sm = ResourceStateMachine::new();
        let can_transition = sm.can_transition(&Phase::Deleting, &event);
        prop_assert!(!can_transition, "Deleting should not transition on {:?}", event);
    }

    /// Property: All phases can transition to Deleting via DeletionRequested.
    #[test]
    fn test_all_can_delete(phase in any_phase()) {
        let sm = ResourceStateMachine::new();
        let can_delete = sm.can_transition(&phase, &ResourceEvent::DeletionRequested);
        if phase == Phase::Deleting {
            // Deleting is terminal, no transitions out
            prop_assert!(!can_delete, "Deleting should not be able to transition");
        } else {
            prop_assert!(can_delete, "Phase {:?} should be able to transition to Deleting", phase);
        }
    }
}

#[cfg(test)]
mod crd_property_tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use my_operator::crd::{MyResource, MyResourceSpec};

    /// Strategy for generating valid MyResourceSpec.
    fn valid_spec() -> impl Strategy<Value = MyResourceSpec> {
        (valid_replicas(), valid_message()).prop_map(|(replicas, message)| MyResourceSpec {
            replicas,
            message,
            labels: std::collections::BTreeMap::new(),
        })
    }

    proptest! {
        /// Property: Valid specs can be serialized and deserialized.
        #[test]
        fn test_spec_roundtrip(spec in valid_spec()) {
            let json = serde_json::to_string(&spec).expect("Serialization should succeed");
            let parsed: MyResourceSpec = serde_json::from_str(&json).expect("Deserialization should succeed");
            prop_assert_eq!(spec.replicas, parsed.replicas);
            prop_assert_eq!(spec.message, parsed.message);
        }

        /// Property: MyResource with valid spec is valid.
        #[test]
        fn test_resource_with_valid_spec(spec in valid_spec()) {
            let resource = MyResource {
                metadata: ObjectMeta {
                    name: Some("test".to_string()),
                    namespace: Some("default".to_string()),
                    ..Default::default()
                },
                spec,
                status: None,
            };

            prop_assert!(resource.metadata.name.is_some());
            prop_assert!(resource.spec.replicas >= 1);
            prop_assert!(resource.spec.replicas <= 10);
        }
    }
}

#[cfg(test)]
mod webhook_property_tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use my_operator::crd::{MyResource, MyResourceSpec};
    use my_operator::webhooks::{AdmissionRequest, AdmissionResponse, AdmissionReview};
    use serde_json::{Value, json};

    /// Generate a valid UID string (UUID format)
    fn valid_uid() -> impl Strategy<Value = String> {
        "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}".prop_map(|s| s)
    }

    /// Generate an operation type
    fn operation() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("CREATE".to_string()),
            Just("UPDATE".to_string()),
            Just("DELETE".to_string()),
        ]
    }

    /// Strategy for generating valid MyResourceSpec
    fn valid_spec() -> impl Strategy<Value = MyResourceSpec> {
        (valid_replicas(), valid_message()).prop_map(|(replicas, message)| MyResourceSpec {
            replicas,
            message,
            labels: std::collections::BTreeMap::new(),
        })
    }

    /// Create a minimal valid spec for simpler tests
    fn minimal_spec() -> MyResourceSpec {
        MyResourceSpec {
            replicas: 1,
            message: "test".to_string(),
            labels: std::collections::BTreeMap::new(),
        }
    }

    /// Create a MyResource from a spec
    fn resource_from_spec(spec: MyResourceSpec) -> MyResource {
        MyResource {
            metadata: ObjectMeta {
                name: Some("test-resource".to_string()),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec,
            status: None,
        }
    }

    /// Build an AdmissionReview JSON request
    /// Includes all fields required by kube-rs AdmissionRequest
    fn build_admission_review(
        uid: &str,
        operation: &str,
        resource: &MyResource,
        old_resource: Option<&MyResource>,
    ) -> Value {
        let object = serde_json::to_value(resource).expect("serialize resource");
        let old_object =
            old_resource.map(|r| serde_json::to_value(r).expect("serialize old resource"));

        let mut request = json!({
            "uid": uid,
            "kind": {
                "group": "myoperator.example.com",
                "version": "v1alpha1",
                "kind": "MyResource"
            },
            "resource": {
                "group": "myoperator.example.com",
                "version": "v1alpha1",
                "resource": "myresources"
            },
            "requestKind": {
                "group": "myoperator.example.com",
                "version": "v1alpha1",
                "kind": "MyResource"
            },
            "requestResource": {
                "group": "myoperator.example.com",
                "version": "v1alpha1",
                "resource": "myresources"
            },
            "operation": operation,
            "namespace": resource.metadata.namespace,
            "name": resource.metadata.name.clone().unwrap_or_default(),
            "object": object,
            "dryRun": false,
            "userInfo": {
                "username": "test-user"
            }
        });

        if let Some(old) = old_object {
            request["oldObject"] = old;
        }

        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": request
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(50))]

        /// Property: AdmissionReview request deserializes correctly with camelCase fields
        /// Uses kube-rs AdmissionReview<MyResource> type
        #[test]
        fn prop_admission_review_deserializes(
            uid in valid_uid(),
            op in operation(),
            spec in valid_spec()
        ) {
            let resource = resource_from_spec(spec);
            let review_json = build_admission_review(&uid, &op, &resource, None);

            // Should deserialize without error using kube-rs typed AdmissionReview
            let review: Result<AdmissionReview<MyResource>, _> = serde_json::from_value(review_json);
            prop_assert!(review.is_ok(), "AdmissionReview should deserialize: {:?}", review.err());

            // Extract the request using try_into
            let request: Result<AdmissionRequest<MyResource>, _> = review.unwrap().try_into();
            prop_assert!(request.is_ok(), "Should extract request from review");

            let request = request.unwrap();
            prop_assert_eq!(request.uid, uid);
        }

        /// Property: AdmissionResponse from kube-rs serializes correctly
        #[test]
        fn prop_admission_response_serializes(
            uid in valid_uid(),
            allowed in proptest::bool::ANY,
            message in "[a-zA-Z ]{1,50}"
        ) {
            let resource = resource_from_spec(minimal_spec());
            let review_json = build_admission_review(&uid, "CREATE", &resource, None);

            // Deserialize to get a request
            let review: AdmissionReview<MyResource> = serde_json::from_value(review_json).unwrap();
            let request: AdmissionRequest<MyResource> = review.try_into().unwrap();

            // Create response using kube-rs API
            let response = if allowed {
                AdmissionResponse::from(&request)
            } else {
                AdmissionResponse::from(&request).deny(message.clone())
            };

            // Convert to review for serialization
            let review_response = response.into_review();

            // Serialize to JSON
            let json_result = serde_json::to_value(&review_response);
            prop_assert!(json_result.is_ok(), "Response should serialize");

            let json = json_result.unwrap();

            // Verify camelCase field names (Kubernetes requires these)
            prop_assert_eq!(json["apiVersion"].as_str(), Some("admission.k8s.io/v1"));
            prop_assert_eq!(json["kind"].as_str(), Some("AdmissionReview"));
            prop_assert_eq!(json["response"]["uid"].as_str(), Some(uid.as_str()));
            prop_assert_eq!(json["response"]["allowed"].as_bool(), Some(allowed));

            // When denied, status should have the message
            if !allowed {
                prop_assert!(json["response"]["status"].is_object(), "Status should be present when denied");
            }
        }

        /// Property: UID is always echoed back in response via kube-rs
        #[test]
        fn prop_uid_echoed_in_response(uid in valid_uid()) {
            let resource = resource_from_spec(minimal_spec());
            let review_json = build_admission_review(&uid, "CREATE", &resource, None);

            let review: AdmissionReview<MyResource> = serde_json::from_value(review_json).unwrap();
            let request: AdmissionRequest<MyResource> = review.try_into().unwrap();

            let response = AdmissionResponse::from(&request).into_review();
            let json: Value = serde_json::to_value(&response).unwrap();

            prop_assert_eq!(json["response"]["uid"].as_str(), Some(uid.as_str()), "UID must be echoed exactly");
        }

        /// Property: UPDATE operation includes oldObject for immutability checks
        #[test]
        fn prop_update_includes_old_object(
            uid in valid_uid(),
            old_spec in valid_spec(),
            new_spec in valid_spec()
        ) {
            let old_resource = resource_from_spec(old_spec);
            let new_resource = resource_from_spec(new_spec);
            let review_json = build_admission_review(&uid, "UPDATE", &new_resource, Some(&old_resource));

            let review: AdmissionReview<MyResource> = serde_json::from_value(review_json).unwrap();
            let request: AdmissionRequest<MyResource> = review.try_into().unwrap();

            prop_assert!(request.old_object.is_some(), "UPDATE should have oldObject");
            prop_assert!(request.object.is_some(), "UPDATE should have object");
        }

        /// Property: Response status includes message when denied via kube-rs
        #[test]
        fn prop_denied_has_status_message(
            uid in valid_uid(),
            message in "[a-zA-Z ]{1,30}"
        ) {
            let resource = resource_from_spec(minimal_spec());
            let review_json = build_admission_review(&uid, "CREATE", &resource, None);

            let review: AdmissionReview<MyResource> = serde_json::from_value(review_json).unwrap();
            let request: AdmissionRequest<MyResource> = review.try_into().unwrap();

            let response = AdmissionResponse::from(&request).deny(message.clone()).into_review();
            let json: Value = serde_json::to_value(&response).unwrap();

            // kube-rs deny() sets status.message when denied
            prop_assert!(json["response"]["status"]["message"].as_str().is_some(), "Denied responses should have status message");
            prop_assert_eq!(json["response"]["status"]["message"].as_str(), Some(message.as_str()), "Message should match");
        }
    }

    /// Verify the exact field names match Kubernetes API expectations using kube-rs types
    #[test]
    fn test_response_field_names_match_kubernetes_spec() {
        let resource = resource_from_spec(minimal_spec());
        let review_json = build_admission_review("test-uid", "CREATE", &resource, None);

        let review: AdmissionReview<MyResource> = serde_json::from_value(review_json).unwrap();
        let request: AdmissionRequest<MyResource> = review.try_into().unwrap();

        let response = AdmissionResponse::from(&request)
            .deny("Test message")
            .into_review();
        let json: Value = serde_json::to_value(&response).unwrap();

        // These exact field names are required by Kubernetes
        assert!(
            json.get("apiVersion").is_some(),
            "Must have apiVersion (camelCase)"
        );
        assert!(json.get("kind").is_some(), "Must have kind");
        assert!(json.get("response").is_some(), "Must have response");

        let resp = &json["response"];
        assert!(resp.get("uid").is_some(), "Response must have uid");
        assert!(resp.get("allowed").is_some(), "Response must have allowed");

        // These should NOT exist (snake_case would break the API)
        assert!(
            json.get("api_version").is_none(),
            "Must NOT have api_version (snake_case)"
        );

        // kube-rs includes status with message when denied
        assert!(
            resp.get("status").is_some(),
            "Status must be present when denied"
        );
        assert!(
            resp.get("status").unwrap().get("message").is_some(),
            "Status must have message"
        );
    }

    /// Verify AdmissionReview request can be round-tripped using kube-rs types
    #[test]
    fn test_admission_review_round_trip() {
        let resource = resource_from_spec(minimal_spec());
        let review_json = build_admission_review(
            "12345678-1234-1234-1234-123456789abc",
            "CREATE",
            &resource,
            None,
        );

        // Deserialize using kube-rs typed AdmissionReview
        let review: AdmissionReview<MyResource> =
            serde_json::from_value(review_json.clone()).expect("should deserialize");

        // Extract request
        let request: AdmissionRequest<MyResource> = review.try_into().expect("should have request");

        assert_eq!(request.uid, "12345678-1234-1234-1234-123456789abc");
        assert_eq!(request.kind.group, "myoperator.example.com");
        assert_eq!(request.kind.version, "v1alpha1");
        assert_eq!(request.kind.kind, "MyResource");
    }
}
