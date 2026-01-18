// Test code is allowed to panic on failure
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::string_slice
)]

//! Property-based tests for valkey-operator.
//!
//! Uses proptest to generate random inputs and verify invariants.

use proptest::prelude::*;

use valkey_operator::controller::cluster_state_machine::{ClusterEvent, ClusterStateMachine};
use valkey_operator::crd::ClusterPhase;

/// Strategy for generating valid master counts (3-100).
fn valid_masters() -> impl Strategy<Value = i32> {
    3..=100i32
}

/// Strategy for generating valid replicas per master (0-5).
fn valid_replicas_per_master() -> impl Strategy<Value = i32> {
    0..=5i32
}

/// Strategy for generating random phases.
fn any_phase() -> impl Strategy<Value = ClusterPhase> {
    prop_oneof![
        // Initial creation phases
        Just(ClusterPhase::Pending),
        Just(ClusterPhase::Creating),
        Just(ClusterPhase::WaitingForPods),
        Just(ClusterPhase::InitializingCluster),
        Just(ClusterPhase::AssigningSlots),
        Just(ClusterPhase::ConfiguringReplicas),
        // Running
        Just(ClusterPhase::Running),
        // Scale-up phases
        Just(ClusterPhase::ScalingUpStatefulSet),
        Just(ClusterPhase::WaitingForNewPods),
        Just(ClusterPhase::AddingNodesToCluster),
        Just(ClusterPhase::RebalancingSlots),
        Just(ClusterPhase::ConfiguringNewReplicas),
        // Scale-down phases
        Just(ClusterPhase::EvacuatingSlots),
        Just(ClusterPhase::RemovingNodesFromCluster),
        Just(ClusterPhase::ScalingDownStatefulSet),
        // Verification
        Just(ClusterPhase::VerifyingClusterHealth),
        // Problem states
        Just(ClusterPhase::Degraded),
        Just(ClusterPhase::Failed),
        // Terminal
        Just(ClusterPhase::Deleting),
    ]
}

/// Strategy for generating random events.
fn any_event() -> impl Strategy<Value = ClusterEvent> {
    prop_oneof![
        Just(ClusterEvent::ResourcesApplied),
        Just(ClusterEvent::PodsRunning),
        Just(ClusterEvent::ReplicasDegraded),
        Just(ClusterEvent::ReconcileError),
        Just(ClusterEvent::DeletionRequested),
        Just(ClusterEvent::RecoveryInitiated),
        Just(ClusterEvent::FullyRecovered),
        Just(ClusterEvent::PhaseComplete),
        Just(ClusterEvent::ScaleUpDetected),
        Just(ClusterEvent::ScaleDownDetected),
        Just(ClusterEvent::ClusterHealthy),
    ]
}

proptest! {
    /// Property: Masters must be between 3 and 100.
    #[test]
    fn test_master_bounds(masters in valid_masters()) {
        prop_assert!(masters >= 3);
        prop_assert!(masters <= 100);
    }

    /// Property: Replicas per master must be between 0 and 5.
    #[test]
    fn test_replicas_per_master_bounds(replicas in valid_replicas_per_master()) {
        prop_assert!(replicas >= 0);
        prop_assert!(replicas <= 5);
    }

    /// Property: Total pods calculation is correct.
    #[test]
    fn test_total_pods_calculation(
        masters in valid_masters(),
        replicas_per_master in valid_replicas_per_master()
    ) {
        let total = valkey_operator::crd::total_pods(masters, replicas_per_master);
        let expected = masters + (masters * replicas_per_master);
        prop_assert_eq!(total, expected);
    }

    /// Property: State machine transition checks are deterministic.
    /// Same (phase, event) pair always yields the same result.
    #[test]
    fn test_state_transitions_deterministic(
        phase in any_phase(),
        event in any_event()
    ) {
        let sm = ClusterStateMachine::new();
        let result1 = sm.can_transition(&phase, &event);
        let result2 = sm.can_transition(&phase, &event);
        prop_assert_eq!(result1, result2);
    }

    /// Property: Deleting phase cannot transition to anything.
    /// Once in Deleting, no events trigger a transition.
    #[test]
    fn test_deleting_is_terminal(event in any_event()) {
        let sm = ClusterStateMachine::new();
        let can_transition = sm.can_transition(&ClusterPhase::Deleting, &event);
        prop_assert!(!can_transition, "Deleting should not transition on {:?}", event);
    }

    /// Property: All phases can transition to Deleting via DeletionRequested.
    #[test]
    fn test_all_can_delete(phase in any_phase()) {
        let sm = ClusterStateMachine::new();
        let can_delete = sm.can_transition(&phase, &ClusterEvent::DeletionRequested);
        if phase == ClusterPhase::Deleting {
            // Deleting is terminal, no transitions out
            prop_assert!(!can_delete, "Deleting should not be able to transition");
        } else {
            prop_assert!(can_delete, "Phase {:?} should be able to transition to Deleting", phase);
        }
    }

    /// Property: Phase can be serialized and deserialized.
    #[test]
    fn test_phase_roundtrip(phase in any_phase()) {
        let serialized = serde_json::to_string(&phase).unwrap();
        let deserialized: ClusterPhase = serde_json::from_str(&serialized).unwrap();
        prop_assert_eq!(phase, deserialized);
    }
}

// TODO: Phase 16 - Add more comprehensive property tests for:
// - ValkeyClusterSpec serialization roundtrip
// - Webhook validation properties
// - Resource generation invariants
