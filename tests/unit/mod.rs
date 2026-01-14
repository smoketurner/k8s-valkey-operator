//! Unit tests for my-operator.
//!
//! These tests run without a Kubernetes cluster and test individual
//! components in isolation.

mod crd_tests {
    use my_operator::crd::{Condition, Phase};

    #[test]
    fn test_phase_display() {
        assert_eq!(Phase::Pending.to_string(), "Pending");
        assert_eq!(Phase::Creating.to_string(), "Creating");
        assert_eq!(Phase::Running.to_string(), "Running");
        assert_eq!(Phase::Updating.to_string(), "Updating");
        assert_eq!(Phase::Degraded.to_string(), "Degraded");
        assert_eq!(Phase::Failed.to_string(), "Failed");
        assert_eq!(Phase::Deleting.to_string(), "Deleting");
    }

    #[test]
    fn test_phase_default() {
        assert_eq!(Phase::default(), Phase::Pending);
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
    use my_operator::controller::state_machine::{ResourceEvent, ResourceStateMachine};
    use my_operator::crd::Phase;

    #[test]
    fn test_valid_events_from_pending() {
        let sm = ResourceStateMachine::new();
        // Pending can transition via ResourcesApplied -> Creating
        assert!(sm.can_transition(&Phase::Pending, &ResourceEvent::ResourcesApplied));
        // Pending can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&Phase::Pending, &ResourceEvent::ReconcileError));
        // Pending can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&Phase::Pending, &ResourceEvent::DeletionRequested));
        // Pending cannot directly go to Running via AllReplicasReady
        assert!(!sm.can_transition(&Phase::Pending, &ResourceEvent::AllReplicasReady));
    }

    #[test]
    fn test_valid_events_from_creating() {
        let sm = ResourceStateMachine::new();
        // Creating can transition via AllReplicasReady -> Running
        assert!(sm.can_transition(&Phase::Creating, &ResourceEvent::AllReplicasReady));
        // Creating can transition via ReplicasDegraded -> Degraded
        assert!(sm.can_transition(&Phase::Creating, &ResourceEvent::ReplicasDegraded));
        // Creating can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&Phase::Creating, &ResourceEvent::ReconcileError));
        // Creating can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&Phase::Creating, &ResourceEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_running() {
        let sm = ResourceStateMachine::new();
        // Running can transition via SpecChanged -> Updating
        assert!(sm.can_transition(&Phase::Running, &ResourceEvent::SpecChanged));
        // Running can transition via ReplicasDegraded -> Degraded
        assert!(sm.can_transition(&Phase::Running, &ResourceEvent::ReplicasDegraded));
        // Running can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&Phase::Running, &ResourceEvent::DeletionRequested));
        // Running can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&Phase::Running, &ResourceEvent::ReconcileError));
    }

    #[test]
    fn test_valid_events_from_updating() {
        let sm = ResourceStateMachine::new();
        // Updating can transition via AllReplicasReady -> Running
        assert!(sm.can_transition(&Phase::Updating, &ResourceEvent::AllReplicasReady));
        // Updating can transition via ReplicasDegraded -> Degraded
        assert!(sm.can_transition(&Phase::Updating, &ResourceEvent::ReplicasDegraded));
        // Updating can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&Phase::Updating, &ResourceEvent::ReconcileError));
        // Updating can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&Phase::Updating, &ResourceEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_degraded() {
        let sm = ResourceStateMachine::new();
        // Degraded can transition via FullyRecovered -> Running
        assert!(sm.can_transition(&Phase::Degraded, &ResourceEvent::FullyRecovered));
        // Degraded can transition via AllReplicasReady -> Running
        assert!(sm.can_transition(&Phase::Degraded, &ResourceEvent::AllReplicasReady));
        // Degraded can transition via SpecChanged -> Updating
        assert!(sm.can_transition(&Phase::Degraded, &ResourceEvent::SpecChanged));
        // Degraded can transition via ReconcileError -> Failed
        assert!(sm.can_transition(&Phase::Degraded, &ResourceEvent::ReconcileError));
        // Degraded can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&Phase::Degraded, &ResourceEvent::DeletionRequested));
    }

    #[test]
    fn test_valid_events_from_failed() {
        let sm = ResourceStateMachine::new();
        // Failed can transition via RecoveryInitiated -> Pending
        assert!(sm.can_transition(&Phase::Failed, &ResourceEvent::RecoveryInitiated));
        // Failed can transition via DeletionRequested -> Deleting
        assert!(sm.can_transition(&Phase::Failed, &ResourceEvent::DeletionRequested));
        // Failed cannot go directly to Running
        assert!(!sm.can_transition(&Phase::Failed, &ResourceEvent::AllReplicasReady));
    }

    #[test]
    fn test_deleting_is_terminal() {
        let sm = ResourceStateMachine::new();
        // Deleting cannot transition to any other state
        let valid_events = sm.valid_events(&Phase::Deleting);
        assert!(
            valid_events.is_empty(),
            "Deleting should have no valid events"
        );
    }

    #[test]
    fn test_deletion_from_all_states() {
        let sm = ResourceStateMachine::new();
        let states = vec![
            Phase::Pending,
            Phase::Creating,
            Phase::Running,
            Phase::Updating,
            Phase::Degraded,
            Phase::Failed,
        ];

        for state in states {
            assert!(
                sm.can_transition(&state, &ResourceEvent::DeletionRequested),
                "Should be able to delete from {:?}",
                state
            );
        }
    }
}

mod error_tests {
    use my_operator::controller::error::Error;

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
    use my_operator::controller::status::{ConditionBuilder, is_condition_true};
    use my_operator::crd::Condition;

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
}
