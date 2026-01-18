//! Topology lifecycle tests for ValkeyCluster state machine.
//!
//! These tests verify that the state machine correctly traverses all expected
//! phases for different cluster topologies (minimal, standard, high-HA, large).

use crate::mock_state::{MockClusterState, expected_sequences};
use valkey_operator::crd::ClusterPhase;

// ============================================================================
// Initial Creation Tests
// ============================================================================

/// Test minimal cluster lifecycle: 3 masters, 0 replicas
/// Creation path: Pending → Creating → WaitingForPods → InitializingCluster →
///                AssigningSlots → VerifyingClusterHealth → Running
#[test]
fn test_minimal_cluster_lifecycle() {
    let mut state = MockClusterState::new("minimal", 3, 0);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::creation_without_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 3);
    assert!(state.slots_assigned);
}

/// Test standard cluster lifecycle: 3 masters, 1 replica per master
/// Creation path: Pending → Creating → WaitingForPods → InitializingCluster →
///                AssigningSlots → ConfiguringReplicas → VerifyingClusterHealth → Running
#[test]
fn test_standard_cluster_lifecycle() {
    let mut state = MockClusterState::new("standard", 3, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::creation_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas
    assert!(state.slots_assigned);
}

/// Test high-HA cluster lifecycle: 3 masters, 2 replicas per master
/// Same creation path as standard cluster.
#[test]
fn test_high_ha_cluster_lifecycle() {
    let mut state = MockClusterState::new("high-ha", 3, 2);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::creation_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 9); // 3 masters + 6 replicas
    assert!(state.slots_assigned);
}

/// Test large cluster lifecycle: 6 masters, 1 replica per master
/// Same creation path as standard cluster.
#[test]
fn test_large_cluster_lifecycle() {
    let mut state = MockClusterState::new("large", 6, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::creation_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 12); // 6 masters + 6 replicas
    assert!(state.slots_assigned);
}

/// Test large high-HA cluster lifecycle: 6 masters, 2 replicas per master
#[test]
fn test_large_ha_cluster_lifecycle() {
    let mut state = MockClusterState::new("large-ha", 6, 2);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::creation_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 18); // 6 masters + 12 replicas
    assert!(state.slots_assigned);
}

// ============================================================================
// Scale-Up Tests
// ============================================================================

/// Test scale-up from 3 to 6 masters with replicas.
/// Scale-up path: Running → ScalingUpStatefulSet → WaitingForNewPods →
///                AddingNodesToCluster → RebalancingSlots → ConfiguringNewReplicas →
///                VerifyingClusterHealth → Running
#[test]
fn test_scale_up_3_to_6_masters_with_replicas() {
    let mut state = MockClusterState::running("scale-up", 3, 1);

    // Set the spec to 6 masters
    state.set_scale_up(6, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_up_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 12); // 6 masters + 6 replicas
}

/// Test scale-up from 3 to 6 masters without replicas.
/// Scale-up path skips ConfiguringNewReplicas.
#[test]
fn test_scale_up_3_to_6_masters_without_replicas() {
    let mut state = MockClusterState::running("scale-up-no-replicas", 3, 0);

    // Set the spec to 6 masters
    state.set_scale_up(6, 0);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_up_without_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 6); // 6 masters, 0 replicas
}

/// Test scale-up from 3 to 9 masters (large scale-up).
#[test]
fn test_scale_up_3_to_9_masters() {
    let mut state = MockClusterState::running("large-scale-up", 3, 1);

    // Set the spec to 9 masters
    state.set_scale_up(9, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_up_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 9);
    assert_eq!(state.running_pods, 18); // 9 masters + 9 replicas
}

/// Test adding replicas (3m/0r → 3m/1r).
/// This is treated as a scale-up since the StatefulSet needs more pods.
#[test]
fn test_add_replicas() {
    let mut state = MockClusterState::running("add-replicas", 3, 0);

    // Set the spec to add replicas
    state.set_scale_up(3, 1);

    let phases = state.run_until_running(20);

    // Adding replicas follows the scale-up path
    assert_eq!(phases, expected_sequences::scale_up_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas
}

/// Test increasing replicas (3m/1r → 3m/2r).
#[test]
fn test_increase_replicas() {
    let mut state = MockClusterState::running("increase-replicas", 3, 1);

    // Set the spec to increase replicas
    state.set_scale_up(3, 2);

    let phases = state.run_until_running(20);

    // Increasing replicas follows the scale-up path
    assert_eq!(phases, expected_sequences::scale_up_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.running_pods, 9); // 3 masters + 6 replicas
}

// ============================================================================
// Scale-Down Tests
// ============================================================================

/// Test scale-down from 6 to 3 masters.
/// Scale-down path: Running → EvacuatingSlots → RemovingNodesFromCluster →
///                  ScalingDownStatefulSet → VerifyingClusterHealth → Running
#[test]
fn test_scale_down_6_to_3_masters() {
    let mut state = MockClusterState::running("scale-down", 6, 1);

    // Set the spec to 3 masters
    state.set_scale_down(3, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_down());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas
}

/// Test scale-down from 9 to 3 masters (large scale-down).
#[test]
fn test_scale_down_9_to_3_masters() {
    let mut state = MockClusterState::running("large-scale-down", 9, 1);

    // Set the spec to 3 masters
    state.set_scale_down(3, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_down());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6);
}

/// Test removing replicas (3m/2r → 3m/1r).
/// This follows the scale-down path since the StatefulSet needs fewer pods.
#[test]
fn test_remove_replicas() {
    let mut state = MockClusterState::running("remove-replicas", 3, 2);

    // Set the spec to remove replicas
    state.set_scale_down(3, 1);

    let phases = state.run_until_running(20);

    // Removing replicas follows the scale-down path
    assert_eq!(phases, expected_sequences::scale_down());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas
}

/// Test removing all replicas (3m/1r → 3m/0r).
#[test]
fn test_remove_all_replicas() {
    let mut state = MockClusterState::running("remove-all-replicas", 3, 1);

    // Set the spec to remove all replicas
    state.set_scale_down(3, 0);

    let phases = state.run_until_running(20);

    // Removing replicas follows the scale-down path
    assert_eq!(phases, expected_sequences::scale_down());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.running_pods, 3); // 3 masters, 0 replicas
}

// ============================================================================
// Combined Scale Tests
// ============================================================================

/// Test combined scale-up (3m/1r → 6m/2r).
/// Masters and replicas both increase.
#[test]
fn test_combined_scale_up() {
    let mut state = MockClusterState::running("combined-scale-up", 3, 1);

    // Set the spec to increase both
    state.set_scale_up(6, 2);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_up_with_replicas());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 18); // 6 masters + 12 replicas
}

/// Test combined scale-down (6m/2r → 3m/1r).
/// Masters and replicas both decrease.
#[test]
fn test_combined_scale_down() {
    let mut state = MockClusterState::running("combined-scale-down", 6, 2);

    // Set the spec to decrease both
    state.set_scale_down(3, 1);

    let phases = state.run_until_running(20);

    assert_eq!(phases, expected_sequences::scale_down());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas
}

// ============================================================================
// Recovery Tests
// ============================================================================

/// Test recovery from degraded state when replicas recover.
#[test]
fn test_degraded_to_running_recovery() {
    let mut state = MockClusterState::degraded("recovery", 3, 1, 4);

    // Simulate full recovery
    state.simulate_cluster_healthy();

    let phases = state.run_until_running(10);

    assert_eq!(phases, expected_sequences::degraded_to_running());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.ready_pods, 6);
}

/// Test recovery from failed state through degraded to running.
#[test]
fn test_failed_to_running_recovery() {
    let mut state = MockClusterState::failed("failed-recovery", 3, 1);

    // Simulate partial recovery first (some pods come back)
    state.simulate_partial_recovery(3);

    // Apply recovery initiated event
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::RecoveryInitiated,
    );
    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Degraded);
        }
        _ => panic!("Expected transition to Degraded"),
    }

    // Now simulate full recovery
    state.simulate_cluster_healthy();

    let phases = state.run_until_running(10);

    // Should go Degraded -> Running
    assert!(phases.contains(&ClusterPhase::Running));
    assert_eq!(state.phase, ClusterPhase::Running);
}

/// Test that failed state can directly recover to running if cluster becomes healthy.
#[test]
fn test_failed_direct_to_running() {
    let mut state = MockClusterState::failed("direct-recovery", 3, 1);

    // Simulate full recovery (all pods come back at once)
    state.simulate_cluster_healthy();

    // Apply ClusterHealthy event directly
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ClusterHealthy,
    );
    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Running);
        }
        _ => panic!("Expected transition to Running"),
    }
    assert_eq!(state.phase, ClusterPhase::Running);
}

// ============================================================================
// Deletion Tests
// ============================================================================

/// Test that deletion can be requested from any phase.
#[test]
fn test_deletion_from_running() {
    let mut state = MockClusterState::running("deletion", 3, 1);

    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::DeletionRequested,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
    assert_eq!(state.phase, ClusterPhase::Deleting);
}

/// Test that deletion can be requested during scale-up.
#[test]
fn test_deletion_during_scale_up() {
    let mut state = MockClusterState::running("deletion-scale", 3, 1);

    // Start scale-up
    state.set_scale_up(6, 1);
    state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ScaleUpDetected,
    );
    assert_eq!(state.phase, ClusterPhase::ScalingUpStatefulSet);

    // Request deletion during scale-up
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::DeletionRequested,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

/// Test that deletion can be requested during scale-down.
#[test]
fn test_deletion_during_scale_down() {
    let mut state = MockClusterState::running("deletion-scale-down", 6, 1);

    // Start scale-down
    state.set_scale_down(3, 1);
    state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ScaleDownDetected,
    );
    assert_eq!(state.phase, ClusterPhase::EvacuatingSlots);

    // Request deletion during scale-down
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::DeletionRequested,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

/// Test that deletion can be requested from degraded state.
#[test]
fn test_deletion_from_degraded() {
    let mut state = MockClusterState::degraded("deletion-degraded", 3, 1, 4);

    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::DeletionRequested,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

/// Test that deletion can be requested from failed state.
#[test]
fn test_deletion_from_failed() {
    let mut state = MockClusterState::failed("deletion-failed", 3, 1);

    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::DeletionRequested,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test that Running phase detects scale-up correctly.
#[test]
fn test_running_detects_scale_up() {
    let mut state = MockClusterState::running("detect-scale-up", 3, 1);

    // Change spec to trigger scale-up detection
    state.target_masters = 6;
    state.spec_changed = true;

    // Apply ScaleUpDetected event
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ScaleUpDetected,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::ScalingUpStatefulSet);
        }
        _ => panic!("Expected transition to ScalingUpStatefulSet"),
    }
}

/// Test that Running phase detects scale-down correctly.
#[test]
fn test_running_detects_scale_down() {
    let mut state = MockClusterState::running("detect-scale-down", 6, 1);

    // Change spec to trigger scale-down detection
    state.target_masters = 3;
    state.spec_changed = true;

    // Apply ScaleDownDetected event
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ScaleDownDetected,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::EvacuatingSlots);
        }
        _ => panic!("Expected transition to EvacuatingSlots"),
    }
}

/// Test that Running phase can detect degraded state.
#[test]
fn test_running_detects_degraded() {
    let mut state = MockClusterState::running("detect-degraded", 3, 1);

    // Simulate some pods failing
    state.ready_pods = 4;

    // The guard should allow transition to Degraded
    let ctx = state.to_transition_context();
    assert!(ctx.is_degraded());

    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ReplicasDegraded,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::Success {
            to,
            ..
        } => {
            assert_eq!(to, ClusterPhase::Degraded);
        }
        _ => panic!("Expected transition to Degraded"),
    }
}

/// Test guard prevents false degraded transition.
#[test]
fn test_degraded_guard_prevents_false_transition() {
    let mut state = MockClusterState::running("guard-test", 3, 1);

    // All pods are healthy, should not be able to go to Degraded
    let result = state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ReplicasDegraded,
    );

    match result {
        valkey_operator::controller::cluster_state_machine::TransitionResult::GuardFailed {
            ..
        } => {
            // Expected - guard prevents transition
        }
        _ => panic!("Expected guard to fail"),
    }
    // Phase should remain Running
    assert_eq!(state.phase, ClusterPhase::Running);
}

/// Test that Deleting is a terminal state.
#[test]
fn test_deleting_is_terminal() {
    let mut state = MockClusterState::running("terminal-test", 3, 1);

    // Transition to Deleting
    state.apply_event(
        valkey_operator::controller::cluster_state_machine::ClusterEvent::DeletionRequested,
    );
    assert_eq!(state.phase, ClusterPhase::Deleting);

    // Try to apply various events - all should fail
    let events = vec![
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ResourcesApplied,
        valkey_operator::controller::cluster_state_machine::ClusterEvent::PodsRunning,
        valkey_operator::controller::cluster_state_machine::ClusterEvent::PhaseComplete,
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ClusterHealthy,
        valkey_operator::controller::cluster_state_machine::ClusterEvent::ScaleUpDetected,
    ];

    for event in events {
        let result = state.apply_event(event.clone());
        match result {
            valkey_operator::controller::cluster_state_machine::TransitionResult::InvalidTransition { .. } => {
                // Expected - Deleting is terminal
            }
            _ => panic!("Expected invalid transition from Deleting state for event {:?}", event),
        }
    }

    // Phase should remain Deleting
    assert_eq!(state.phase, ClusterPhase::Deleting);
}
