//! Unit tests for valkey-operator.
//!
//! These tests run without a Kubernetes cluster and test individual
//! components in isolation.

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
    use valkey_operator::controller::status::{ConditionBuilder, is_condition_true};
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
}
