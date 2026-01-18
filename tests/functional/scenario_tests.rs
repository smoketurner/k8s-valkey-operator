//! Complex multi-step scenario tests for ValkeyCluster state machine.
//!
//! These tests verify complex scenarios that span multiple operations,
//! such as rapid scaling sequences, recovery during operations, and
//! interruption handling.

use crate::mock_state::{MockClusterState, expected_sequences};
use valkey_operator::controller::cluster_state_machine::{ClusterEvent, TransitionResult};
use valkey_operator::crd::ClusterPhase;

// ============================================================================
// Rapid Scaling Sequence Tests
// ============================================================================

/// Test rapid scale sequence: 3 → 6 → 3 masters.
/// Verifies that scaling up and then down works correctly.
#[test]
fn test_rapid_scale_sequence_3_to_6_to_3() {
    let mut state = MockClusterState::running("rapid-scale", 3, 1);

    // Phase 1: Scale up 3 → 6
    state.set_scale_up(6, 1);
    let scale_up_phases = state.run_until_running(20);

    assert_eq!(
        scale_up_phases,
        expected_sequences::scale_up_with_replicas()
    );
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 12);

    // Phase 2: Scale down 6 → 3
    state.set_scale_down(3, 1);
    let scale_down_phases = state.run_until_running(20);

    assert_eq!(scale_down_phases, expected_sequences::scale_down());
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6);
}

/// Test rapid scale sequence: 3 → 9 → 3 masters (larger scale).
#[test]
fn test_rapid_scale_sequence_3_to_9_to_3() {
    let mut state = MockClusterState::running("large-rapid-scale", 3, 1);

    // Phase 1: Scale up 3 → 9
    state.set_scale_up(9, 1);
    let scale_up_phases = state.run_until_running(20);

    assert_eq!(
        scale_up_phases,
        expected_sequences::scale_up_with_replicas()
    );
    assert_eq!(state.current_masters, 9);
    assert_eq!(state.running_pods, 18);

    // Phase 2: Scale down 9 → 3
    state.set_scale_down(3, 1);
    let scale_down_phases = state.run_until_running(20);

    assert_eq!(scale_down_phases, expected_sequences::scale_down());
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6);
}

/// Test multiple scale-up operations: 3 → 4 → 5 → 6 masters.
#[test]
fn test_incremental_scale_up() {
    let mut state = MockClusterState::running("incremental", 3, 1);

    // Scale 3 → 4
    state.set_scale_up(4, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 4);
    assert_eq!(state.running_pods, 8);

    // Scale 4 → 5
    state.set_scale_up(5, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 5);
    assert_eq!(state.running_pods, 10);

    // Scale 5 → 6
    state.set_scale_up(6, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 12);
}

/// Test multiple scale-down operations: 6 → 5 → 4 → 3 masters.
#[test]
fn test_incremental_scale_down() {
    let mut state = MockClusterState::running("incremental-down", 6, 1);

    // Scale 6 → 5
    state.set_scale_down(5, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 5);
    assert_eq!(state.running_pods, 10);

    // Scale 5 → 4
    state.set_scale_down(4, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 4);
    assert_eq!(state.running_pods, 8);

    // Scale 4 → 3
    state.set_scale_down(3, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 6);
}

// ============================================================================
// Replica Change Scenarios
// ============================================================================

/// Test adding and removing replicas without changing masters.
#[test]
fn test_replica_change_sequence() {
    let mut state = MockClusterState::running("replica-change", 3, 1);

    // Add replicas: 3m/1r → 3m/2r
    state.set_scale_up(3, 2);
    state.run_until_running(20);
    assert_eq!(state.running_pods, 9); // 3 masters + 6 replicas

    // Remove replicas: 3m/2r → 3m/1r
    state.set_scale_down(3, 1);
    state.run_until_running(20);
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas

    // Remove all replicas: 3m/1r → 3m/0r
    state.set_scale_down(3, 0);
    state.run_until_running(20);
    assert_eq!(state.running_pods, 3); // 3 masters only
}

/// Test replica-only scale-down uses the correct path.
/// Should skip EvacuatingSlots since replicas don't hold slots.
#[test]
fn test_replica_scale_down_path() {
    let mut state = MockClusterState::running("replica-scale-down", 3, 2);
    assert_eq!(state.running_pods, 9); // 3 masters + 6 replicas

    // Remove replicas: 3m/2r → 3m/1r
    state.set_scale_down(3, 1);
    let phases = state.run_until_running(20);

    // Verify the correct path was taken (skipping EvacuatingSlots)
    assert_eq!(phases, expected_sequences::replica_scale_down());
    assert_eq!(state.running_pods, 6); // 3 masters + 3 replicas
    assert_eq!(state.current_masters, 3); // Masters unchanged
}

/// Test replica scale-down from degraded state.
#[test]
fn test_replica_scale_down_from_degraded() {
    let mut state = MockClusterState::degraded("replica-down-degraded", 3, 2, 7);

    // While degraded, request replica scale-down: 3m/2r → 3m/1r
    state.set_scale_down(3, 1);

    // Apply ReplicaScaleDownDetected from Degraded
    let result = state.apply_event(ClusterEvent::ReplicaScaleDownDetected);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::RemovingNodesFromCluster);
        }
        _ => panic!("Expected transition to RemovingNodesFromCluster"),
    }

    // Complete the scale-down
    state.simulate_cluster_healthy();
    state.run_until_running(20);
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.running_pods, 6);
}

/// Test combined master and replica changes.
#[test]
fn test_combined_master_and_replica_changes() {
    let mut state = MockClusterState::running("combined-changes", 3, 1);

    // Scale up both: 3m/1r → 6m/2r
    state.set_scale_up(6, 2);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 18); // 6 masters + 12 replicas

    // Scale down masters, keep replicas: 6m/2r → 3m/2r
    state.set_scale_down(3, 2);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 3);
    assert_eq!(state.running_pods, 9); // 3 masters + 6 replicas
}

// ============================================================================
// Recovery Scenarios
// ============================================================================

/// Test recovery while scaling is in progress.
/// Simulates pods failing during scale-up and then recovering.
#[test]
fn test_degraded_during_verification() {
    let mut state = MockClusterState::new("degraded-verify", 3, 1);

    // Run through creation to verification
    state.run_until_running(20);
    assert_eq!(state.phase, ClusterPhase::Running);

    // Now simulate degradation
    state.ready_pods = 4;
    state.running_pods = 4;

    // Apply degraded event
    let result = state.apply_event(ClusterEvent::ReplicasDegraded);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Degraded);
        }
        _ => panic!("Expected transition to Degraded"),
    }

    // Simulate recovery
    state.simulate_cluster_healthy();
    state.run_until_running(10);
    assert_eq!(state.phase, ClusterPhase::Running);
}

/// Test scale operation from degraded state.
#[test]
fn test_scale_from_degraded() {
    let mut state = MockClusterState::degraded("scale-degraded", 3, 1, 4);

    // While degraded, request scale-up
    state.set_scale_up(6, 1);

    // Apply ScaleUpDetected from Degraded
    let result = state.apply_event(ClusterEvent::ScaleUpDetected);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::ScalingUpStatefulSet);
        }
        _ => panic!("Expected transition to ScalingUpStatefulSet"),
    }

    // Complete the scale-up
    state.simulate_cluster_healthy();
    state.run_until_running(20);
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 6);
}

/// Test scale-down from degraded state.
#[test]
fn test_scale_down_from_degraded() {
    let mut state = MockClusterState::degraded("scale-down-degraded", 6, 1, 8);

    // While degraded, request scale-down
    state.set_scale_down(3, 1);

    // Apply ScaleDownDetected from Degraded
    let result = state.apply_event(ClusterEvent::ScaleDownDetected);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::EvacuatingSlots);
        }
        _ => panic!("Expected transition to EvacuatingSlots"),
    }

    // Complete the scale-down
    state.simulate_cluster_healthy();
    state.run_until_running(20);
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);
}

/// Test multiple recovery attempts.
#[test]
fn test_multiple_recovery_attempts() {
    let mut state = MockClusterState::failed("multi-recovery", 3, 1);

    // First recovery attempt - partial
    state.simulate_partial_recovery(2);
    let result = state.apply_event(ClusterEvent::RecoveryInitiated);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Degraded);
        }
        _ => panic!("Expected transition to Degraded"),
    }

    // Simulate falling back to failed (all pods fail again)
    state.ready_pods = 0;
    state.running_pods = 0;
    let result = state.apply_event(ClusterEvent::ReconcileError);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Failed);
        }
        _ => panic!("Expected transition to Failed"),
    }

    // Second recovery attempt - full
    state.simulate_cluster_healthy();
    let result = state.apply_event(ClusterEvent::ClusterHealthy);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Running);
        }
        _ => panic!("Expected transition to Running"),
    }
}

// ============================================================================
// Deletion Interrupt Scenarios
// ============================================================================

/// Test deletion interrupting scaling.
#[test]
fn test_deletion_interrupts_scaling() {
    let mut state = MockClusterState::running("deletion-scale", 3, 1);

    // Start scale-up
    state.set_scale_up(6, 1);
    state.apply_event(ClusterEvent::ScaleUpDetected);
    assert_eq!(state.phase, ClusterPhase::ScalingUpStatefulSet);

    // Move to next phase
    state.apply_event(ClusterEvent::PhaseComplete);
    assert_eq!(state.phase, ClusterPhase::WaitingForNewPods);

    // Deletion requested mid-scale
    let result = state.apply_event(ClusterEvent::DeletionRequested);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }

    assert_eq!(state.phase, ClusterPhase::Deleting);
}

/// Test deletion interrupting slot rebalancing.
#[test]
fn test_deletion_interrupts_rebalancing() {
    let mut state = MockClusterState::running("deletion-rebalance", 3, 1);

    // Start scale-up and progress to rebalancing
    state.set_scale_up(6, 1);
    state.apply_event(ClusterEvent::ScaleUpDetected);
    state.apply_event(ClusterEvent::PhaseComplete); // WaitingForNewPods
    state.simulate_new_pods_running();
    state.apply_event(ClusterEvent::PodsRunning); // AddingNodesToCluster
    state.simulate_nodes_added();
    state.apply_event(ClusterEvent::PhaseComplete); // RebalancingSlots
    assert_eq!(state.phase, ClusterPhase::RebalancingSlots);

    // Deletion requested during rebalancing
    let result = state.apply_event(ClusterEvent::DeletionRequested);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

/// Test deletion interrupting slot evacuation (scale-down).
#[test]
fn test_deletion_interrupts_evacuation() {
    let mut state = MockClusterState::running("deletion-evacuate", 6, 1);

    // Start scale-down
    state.set_scale_down(3, 1);
    state.apply_event(ClusterEvent::ScaleDownDetected);
    assert_eq!(state.phase, ClusterPhase::EvacuatingSlots);

    // Deletion requested during evacuation
    let result = state.apply_event(ClusterEvent::DeletionRequested);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

/// Test deletion from every phase in the creation path.
#[test]
fn test_deletion_from_creation_path() {
    // Test deletion from each creation phase
    let creation_phases = vec![
        ClusterPhase::Pending,
        ClusterPhase::Creating,
        ClusterPhase::WaitingForPods,
        ClusterPhase::InitializingCluster,
        ClusterPhase::AssigningSlots,
        ClusterPhase::ConfiguringReplicas,
        ClusterPhase::VerifyingClusterHealth,
    ];

    for phase in creation_phases {
        let mut state = MockClusterState::new("deletion-test", 3, 1);
        state.phase = phase;

        let result = state.apply_event(ClusterEvent::DeletionRequested);
        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Deleting, "Failed for phase {:?}", phase);
            }
            _ => panic!("Expected transition to Deleting from {:?}", phase),
        }
    }
}

// ============================================================================
// Full Lifecycle Scenarios
// ============================================================================

/// Test complete lifecycle: create → scale up → scale down → delete.
#[test]
fn test_complete_lifecycle() {
    let mut state = MockClusterState::new("full-lifecycle", 3, 1);

    // Phase 1: Create cluster
    let creation_phases = state.run_until_running(20);
    assert_eq!(
        creation_phases,
        expected_sequences::creation_with_replicas()
    );
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 3);

    // Phase 2: Scale up
    state.set_scale_up(6, 1);
    let scale_up_phases = state.run_until_running(20);
    assert_eq!(
        scale_up_phases,
        expected_sequences::scale_up_with_replicas()
    );
    assert_eq!(state.current_masters, 6);

    // Phase 3: Scale down
    state.set_scale_down(3, 1);
    let scale_down_phases = state.run_until_running(20);
    assert_eq!(scale_down_phases, expected_sequences::scale_down());
    assert_eq!(state.current_masters, 3);

    // Phase 4: Delete
    let result = state.apply_event(ClusterEvent::DeletionRequested);
    match result {
        TransitionResult::Success { to, .. } => {
            assert_eq!(to, ClusterPhase::Deleting);
        }
        _ => panic!("Expected transition to Deleting"),
    }
}

/// Test lifecycle with degradation and recovery.
#[test]
fn test_lifecycle_with_degradation() {
    let mut state = MockClusterState::new("degraded-lifecycle", 3, 1);

    // Create cluster
    state.run_until_running(20);
    assert_eq!(state.phase, ClusterPhase::Running);

    // Simulate degradation
    state.ready_pods = 4;
    state.running_pods = 4;
    state.apply_event(ClusterEvent::ReplicasDegraded);
    assert_eq!(state.phase, ClusterPhase::Degraded);

    // Recover
    state.simulate_cluster_healthy();
    state.run_until_running(10);
    assert_eq!(state.phase, ClusterPhase::Running);

    // Scale up
    state.set_scale_up(6, 1);
    state.run_until_running(20);
    assert_eq!(state.current_masters, 6);

    // Simulate degradation during operation
    // (in Running state after scale-up)
    state.ready_pods = 8;
    state.running_pods = 8;
    state.apply_event(ClusterEvent::ReplicasDegraded);
    assert_eq!(state.phase, ClusterPhase::Degraded);

    // Recover again
    state.simulate_cluster_healthy();
    state.run_until_running(10);
    assert_eq!(state.phase, ClusterPhase::Running);
}

/// Test lifecycle with failure and recovery.
#[test]
fn test_lifecycle_with_failure() {
    let mut state = MockClusterState::new("failed-lifecycle", 3, 1);

    // Create cluster
    state.run_until_running(20);
    assert_eq!(state.phase, ClusterPhase::Running);

    // Simulate complete failure (all pods down)
    state.ready_pods = 0;
    state.running_pods = 0;
    state.apply_event(ClusterEvent::ReplicasDegraded);

    // Hmm, this might go to degraded first if we have some pods
    // Let's verify the state machine behavior
    // Actually, ReplicasDegraded guard checks is_degraded which requires ready > 0
    // So we need to first go to degraded (partial), then to failed
    state.ready_pods = 2;
    state.running_pods = 2;
    state.apply_event(ClusterEvent::ReplicasDegraded);
    assert_eq!(state.phase, ClusterPhase::Degraded);

    // Now all pods fail
    state.ready_pods = 0;
    state.running_pods = 0;
    state.apply_event(ClusterEvent::ReconcileError);
    assert_eq!(state.phase, ClusterPhase::Failed);

    // Recovery through degraded
    state.ready_pods = 3;
    state.running_pods = 3;
    state.apply_event(ClusterEvent::RecoveryInitiated);
    assert_eq!(state.phase, ClusterPhase::Degraded);

    // Full recovery
    state.simulate_cluster_healthy();
    state.run_until_running(10);
    assert_eq!(state.phase, ClusterPhase::Running);
}

// ============================================================================
// Edge Cases
// ============================================================================

/// Test that we can create a cluster without replicas and then add them.
#[test]
fn test_create_without_replicas_then_add() {
    let mut state = MockClusterState::new("no-replicas-add", 3, 0);

    // Create cluster without replicas
    let creation_phases = state.run_until_running(20);
    assert_eq!(
        creation_phases,
        expected_sequences::creation_without_replicas()
    );
    assert_eq!(state.running_pods, 3);

    // Add replicas
    state.set_scale_up(3, 1);
    state.run_until_running(20);
    assert_eq!(state.running_pods, 6);
    assert_eq!(state.current_masters, 3);
}

/// Test that we can scale up and add replicas simultaneously.
#[test]
fn test_scale_up_with_new_replicas() {
    let mut state = MockClusterState::running("scale-add-replicas", 3, 0);

    // Scale up masters AND add replicas: 3m/0r → 6m/1r
    state.set_scale_up(6, 1);
    state.run_until_running(20);

    assert_eq!(state.current_masters, 6);
    assert_eq!(state.running_pods, 12); // 6 masters + 6 replicas
}

/// Test maximum topology (9 masters, 2 replicas).
#[test]
fn test_maximum_topology() {
    let mut state = MockClusterState::new("max-topology", 9, 2);

    // Create large cluster
    state.run_until_running(25);
    assert_eq!(state.phase, ClusterPhase::Running);
    assert_eq!(state.current_masters, 9);
    assert_eq!(state.running_pods, 27); // 9 masters + 18 replicas
}

/// Test that spec_changed flag works correctly.
#[test]
fn test_spec_changed_flag() {
    let mut state = MockClusterState::running("spec-changed", 3, 1);

    assert!(!state.spec_changed);

    // Change spec
    state.set_scale_up(6, 1);
    assert!(state.spec_changed);

    // Run through scale-up
    state.run_until_running(20);

    // After operation completes, verify state
    assert_eq!(state.current_masters, 6);
}

/// Test transition context helper methods.
#[test]
fn test_transition_context_helpers() {
    use valkey_operator::controller::cluster_state_machine::TransitionContext;

    // All ready
    let ctx = TransitionContext::new(6, 6);
    assert!(ctx.all_replicas_ready());
    assert!(!ctx.is_degraded());
    assert!(!ctx.no_replicas_ready());

    // Degraded
    let ctx = TransitionContext::new(3, 6);
    assert!(!ctx.all_replicas_ready());
    assert!(ctx.is_degraded());
    assert!(!ctx.no_replicas_ready());

    // Failed
    let ctx = TransitionContext::new(0, 6);
    assert!(!ctx.all_replicas_ready());
    assert!(!ctx.is_degraded());
    assert!(ctx.no_replicas_ready());
}

/// Test phase context helpers.
#[test]
fn test_phase_context_helpers() {
    use valkey_operator::controller::cluster_phases::ScaleDirection;

    let state = MockClusterState::running("helpers", 3, 1);
    let ctx = state.to_phase_context();

    assert_eq!(ctx.desired_replicas(), 6);
    assert_eq!(ctx.scale_direction(), ScaleDirection::None);

    // Modify for scale-up
    let mut state = MockClusterState::running("helpers-up", 3, 1);
    state.target_masters = 6;
    let ctx = state.to_phase_context();
    assert_eq!(ctx.scale_direction(), ScaleDirection::Up);

    // Modify for scale-down
    let mut state = MockClusterState::running("helpers-down", 6, 1);
    state.target_masters = 3;
    let ctx = state.to_phase_context();
    assert_eq!(ctx.scale_direction(), ScaleDirection::Down);
}
