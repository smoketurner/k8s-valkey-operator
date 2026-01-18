//! Phase handler tests for ValkeyCluster state machine.
//!
//! These tests verify individual phase handlers and pure logic functions
//! without requiring a live Kubernetes cluster.

use valkey_operator::controller::cluster_phases::{
    ClusterHealthResult, PhaseContext, ScaleDirection, compute_nodes_to_add,
    compute_nodes_to_remove, is_cluster_degraded, is_cluster_failed, is_cluster_healthy,
};
use valkey_operator::controller::cluster_state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult, determine_event,
};
use valkey_operator::crd::ClusterPhase;

// ============================================================================
// Pure Logic Function Tests
// ============================================================================

mod compute_nodes_tests {
    use super::*;

    #[test]
    fn test_compute_nodes_to_add_empty_when_no_scale() {
        let nodes = compute_nodes_to_add("test-cluster", 3, 3);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_add_empty_when_scale_down() {
        let nodes = compute_nodes_to_add("test-cluster", 6, 3);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_add_correct_when_scale_up() {
        let nodes = compute_nodes_to_add("test-cluster", 3, 6);
        assert_eq!(
            nodes,
            vec!["test-cluster-3", "test-cluster-4", "test-cluster-5"]
        );
    }

    #[test]
    fn test_compute_nodes_to_add_single_node() {
        let nodes = compute_nodes_to_add("vc", 3, 4);
        assert_eq!(nodes, vec!["vc-3"]);
    }

    #[test]
    fn test_compute_nodes_to_remove_empty_when_no_scale() {
        let nodes = compute_nodes_to_remove("test-cluster", 3, 3);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_remove_empty_when_scale_up() {
        let nodes = compute_nodes_to_remove("test-cluster", 3, 6);
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_compute_nodes_to_remove_correct_when_scale_down() {
        let nodes = compute_nodes_to_remove("test-cluster", 6, 3);
        assert_eq!(
            nodes,
            vec!["test-cluster-3", "test-cluster-4", "test-cluster-5"]
        );
    }

    #[test]
    fn test_compute_nodes_to_remove_single_node() {
        let nodes = compute_nodes_to_remove("test", 4, 3);
        assert_eq!(nodes, vec!["test-3"]);
    }
}

mod cluster_health_tests {
    use super::*;

    #[test]
    fn test_is_cluster_healthy_when_all_ready() {
        assert!(is_cluster_healthy(6, 6));
    }

    #[test]
    fn test_is_cluster_healthy_when_more_ready() {
        // Can happen during scale-down
        assert!(is_cluster_healthy(9, 6));
    }

    #[test]
    fn test_is_cluster_healthy_false_when_partial() {
        assert!(!is_cluster_healthy(3, 6));
    }

    #[test]
    fn test_is_cluster_healthy_false_when_none() {
        assert!(!is_cluster_healthy(0, 6));
    }

    #[test]
    fn test_is_cluster_healthy_false_when_zero_desired() {
        assert!(!is_cluster_healthy(0, 0));
    }

    #[test]
    fn test_is_cluster_degraded_when_partial() {
        assert!(is_cluster_degraded(3, 6));
    }

    #[test]
    fn test_is_cluster_degraded_when_one_ready() {
        assert!(is_cluster_degraded(1, 6));
    }

    #[test]
    fn test_is_cluster_degraded_false_when_all_ready() {
        assert!(!is_cluster_degraded(6, 6));
    }

    #[test]
    fn test_is_cluster_degraded_false_when_none_ready() {
        assert!(!is_cluster_degraded(0, 6));
    }

    #[test]
    fn test_is_cluster_failed_when_none_ready() {
        assert!(is_cluster_failed(0));
    }

    #[test]
    fn test_is_cluster_failed_false_when_some_ready() {
        assert!(!is_cluster_failed(1));
        assert!(!is_cluster_failed(3));
        assert!(!is_cluster_failed(6));
    }
}

// ============================================================================
// PhaseContext Tests
// ============================================================================

mod phase_context_tests {
    use super::*;

    fn make_context(
        target_masters: i32,
        target_replicas: i32,
        current_masters: i32,
        running_pods: i32,
    ) -> PhaseContext {
        PhaseContext {
            name: "test".to_string(),
            namespace: "default".to_string(),
            target_masters,
            target_replicas_per_master: target_replicas,
            current_masters,
            running_pods,
            ready_pods: running_pods,
            nodes_in_cluster: running_pods,
            spec_changed: false,
            generation: 1,
        }
    }

    #[test]
    fn test_desired_replicas_calculation() {
        let ctx = make_context(3, 1, 3, 6);
        assert_eq!(ctx.desired_replicas(), 6); // 3 * (1 + 1)

        let ctx = make_context(3, 2, 3, 9);
        assert_eq!(ctx.desired_replicas(), 9); // 3 * (1 + 2)

        let ctx = make_context(6, 1, 6, 12);
        assert_eq!(ctx.desired_replicas(), 12); // 6 * (1 + 1)

        let ctx = make_context(3, 0, 3, 3);
        assert_eq!(ctx.desired_replicas(), 3); // 3 * (1 + 0)
    }

    #[test]
    fn test_scale_direction_up() {
        let ctx = make_context(6, 1, 3, 6);
        assert_eq!(ctx.scale_direction(), ScaleDirection::Up);
    }

    #[test]
    fn test_scale_direction_down() {
        let ctx = make_context(3, 1, 6, 12);
        assert_eq!(ctx.scale_direction(), ScaleDirection::Down);
    }

    #[test]
    fn test_scale_direction_none_when_equal() {
        let ctx = make_context(3, 1, 3, 6);
        assert_eq!(ctx.scale_direction(), ScaleDirection::None);
    }

    #[test]
    fn test_scale_direction_replica_change() {
        // Masters same, but running pods differ from desired
        let ctx = make_context(3, 2, 3, 6);
        // Desired is 9 (3 * (1 + 2)), but running is 6
        assert_eq!(ctx.scale_direction(), ScaleDirection::ReplicaChange);
    }

    #[test]
    fn test_scale_direction_none_with_zero_current_masters() {
        // During initial creation, current_masters is 0
        let ctx = make_context(3, 1, 0, 0);
        // Should not report scale up/down, just replica change (or none if pods match)
        assert_eq!(ctx.scale_direction(), ScaleDirection::ReplicaChange);
    }
}

// ============================================================================
// TransitionContext Tests
// ============================================================================

mod transition_context_tests {
    use super::*;

    #[test]
    fn test_all_replicas_ready() {
        let ctx = TransitionContext::new(6, 6);
        assert!(ctx.all_replicas_ready());

        let ctx = TransitionContext::new(9, 6);
        assert!(ctx.all_replicas_ready());
    }

    #[test]
    fn test_all_replicas_ready_false() {
        let ctx = TransitionContext::new(3, 6);
        assert!(!ctx.all_replicas_ready());

        let ctx = TransitionContext::new(0, 6);
        assert!(!ctx.all_replicas_ready());
    }

    #[test]
    fn test_is_degraded() {
        let ctx = TransitionContext::new(3, 6);
        assert!(ctx.is_degraded());

        let ctx = TransitionContext::new(1, 6);
        assert!(ctx.is_degraded());
    }

    #[test]
    fn test_is_degraded_false() {
        let ctx = TransitionContext::new(6, 6);
        assert!(!ctx.is_degraded());

        let ctx = TransitionContext::new(0, 6);
        assert!(!ctx.is_degraded());
    }

    #[test]
    fn test_no_replicas_ready() {
        let ctx = TransitionContext::new(0, 6);
        assert!(ctx.no_replicas_ready());
    }

    #[test]
    fn test_no_replicas_ready_false() {
        let ctx = TransitionContext::new(1, 6);
        assert!(!ctx.no_replicas_ready());
    }

    #[test]
    fn test_with_spec_changed() {
        let ctx = TransitionContext::new(6, 6).with_spec_changed(true);
        assert!(ctx.spec_changed);

        let ctx = TransitionContext::new(6, 6).with_spec_changed(false);
        assert!(!ctx.spec_changed);
    }

    #[test]
    fn test_with_error() {
        let ctx = TransitionContext::new(6, 6).with_error("test error".to_string());
        assert_eq!(ctx.error_message, Some("test error".to_string()));
    }
}

// ============================================================================
// State Machine Transition Tests
// ============================================================================

mod state_machine_tests {
    use super::*;

    fn make_transition_ctx(ready: i32, desired: i32) -> TransitionContext {
        TransitionContext::new(ready, desired)
    }

    // === Creation Path Tests ===

    #[test]
    fn test_pending_to_creating() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(0, 6);

        let result = sm.transition(&ClusterPhase::Pending, ClusterEvent::ResourcesApplied, &ctx);

        match result {
            TransitionResult::Success { from, to, .. } => {
                assert_eq!(from, ClusterPhase::Pending);
                assert_eq!(to, ClusterPhase::Creating);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_creating_to_waiting_for_pods() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(0, 6);

        let result = sm.transition(
            &ClusterPhase::Creating,
            ClusterEvent::ResourcesApplied,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::WaitingForPods);
            }
            _ => panic!("Expected transition to WaitingForPods"),
        }
    }

    #[test]
    fn test_waiting_for_pods_to_initializing() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(
            &ClusterPhase::WaitingForPods,
            ClusterEvent::PodsRunning,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::InitializingCluster);
            }
            _ => panic!("Expected transition to InitializingCluster"),
        }
    }

    #[test]
    fn test_initializing_to_assigning_slots() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(
            &ClusterPhase::InitializingCluster,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::AssigningSlots);
            }
            _ => panic!("Expected transition to AssigningSlots"),
        }
    }

    #[test]
    fn test_assigning_slots_to_configuring_replicas() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(
            &ClusterPhase::AssigningSlots,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::ConfiguringReplicas);
            }
            _ => panic!("Expected transition to ConfiguringReplicas"),
        }
    }

    #[test]
    fn test_assigning_slots_skips_replicas_when_none() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(3, 3); // 3 masters, 0 replicas

        let result = sm.transition(
            &ClusterPhase::AssigningSlots,
            ClusterEvent::ClusterHealthy,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth);
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    #[test]
    fn test_configuring_replicas_to_verifying() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(
            &ClusterPhase::ConfiguringReplicas,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth);
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    #[test]
    fn test_verifying_to_running() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(
            &ClusterPhase::VerifyingClusterHealth,
            ClusterEvent::ClusterHealthy,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected transition to Running"),
        }
    }

    // === Scale-Up Path Tests ===

    #[test]
    fn test_running_to_scaling_up() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ScaleUpDetected, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::ScalingUpStatefulSet);
            }
            _ => panic!("Expected transition to ScalingUpStatefulSet"),
        }
    }

    #[test]
    fn test_scaling_up_to_waiting_for_new_pods() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 12);

        let result = sm.transition(
            &ClusterPhase::ScalingUpStatefulSet,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::WaitingForNewPods);
            }
            _ => panic!("Expected transition to WaitingForNewPods"),
        }
    }

    #[test]
    fn test_waiting_for_new_pods_to_adding_nodes() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::WaitingForNewPods,
            ClusterEvent::PodsRunning,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::AddingNodesToCluster);
            }
            _ => panic!("Expected transition to AddingNodesToCluster"),
        }
    }

    #[test]
    fn test_adding_nodes_to_rebalancing() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::AddingNodesToCluster,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::RebalancingSlots);
            }
            _ => panic!("Expected transition to RebalancingSlots"),
        }
    }

    #[test]
    fn test_rebalancing_to_configuring_new_replicas() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::RebalancingSlots,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::ConfiguringNewReplicas);
            }
            _ => panic!("Expected transition to ConfiguringNewReplicas"),
        }
    }

    #[test]
    fn test_rebalancing_skips_replicas_when_none() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6); // 6 masters, 0 replicas

        let result = sm.transition(
            &ClusterPhase::RebalancingSlots,
            ClusterEvent::ClusterHealthy,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth);
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    #[test]
    fn test_configuring_new_replicas_to_verifying() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::ConfiguringNewReplicas,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth);
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    // === Scale-Down Path Tests ===

    #[test]
    fn test_running_to_evacuating_slots() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::Running,
            ClusterEvent::ScaleDownDetected,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::EvacuatingSlots);
            }
            _ => panic!("Expected transition to EvacuatingSlots"),
        }
    }

    #[test]
    fn test_evacuating_to_removing_nodes() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::EvacuatingSlots,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::RemovingNodesFromCluster);
            }
            _ => panic!("Expected transition to RemovingNodesFromCluster"),
        }
    }

    #[test]
    fn test_removing_nodes_to_scaling_down() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(12, 12);

        let result = sm.transition(
            &ClusterPhase::RemovingNodesFromCluster,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::ScalingDownStatefulSet);
            }
            _ => panic!("Expected transition to ScalingDownStatefulSet"),
        }
    }

    #[test]
    fn test_scaling_down_to_verifying() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(
            &ClusterPhase::ScalingDownStatefulSet,
            ClusterEvent::PhaseComplete,
            &ctx,
        );

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::VerifyingClusterHealth);
            }
            _ => panic!("Expected transition to VerifyingClusterHealth"),
        }
    }

    // === Problem State Tests ===

    #[test]
    fn test_running_to_degraded() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(4, 6); // Degraded: some ready but not all

        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ReplicasDegraded, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Degraded);
            }
            _ => panic!("Expected transition to Degraded"),
        }
    }

    #[test]
    fn test_degraded_to_running_on_recovery() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::FullyRecovered, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected transition to Running"),
        }
    }

    #[test]
    fn test_degraded_to_running_on_healthy() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::ClusterHealthy, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected transition to Running"),
        }
    }

    #[test]
    fn test_failed_to_degraded_on_recovery() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(3, 6); // Some pods recovered

        let result = sm.transition(&ClusterPhase::Failed, ClusterEvent::RecoveryInitiated, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Degraded);
            }
            _ => panic!("Expected transition to Degraded"),
        }
    }

    #[test]
    fn test_failed_to_running_on_healthy() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(&ClusterPhase::Failed, ClusterEvent::ClusterHealthy, &ctx);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Running);
            }
            _ => panic!("Expected transition to Running"),
        }
    }

    // === Guard Tests ===

    #[test]
    fn test_degraded_guard_prevents_invalid_transition() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6); // All ready - not degraded

        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ReplicasDegraded, &ctx);

        match result {
            TransitionResult::GuardFailed { .. } => {
                // Expected - guard prevents invalid transition
            }
            _ => panic!("Expected guard to fail"),
        }
    }

    #[test]
    fn test_recovery_guard_prevents_invalid_transition() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(3, 6); // Not all ready

        let result = sm.transition(&ClusterPhase::Degraded, ClusterEvent::FullyRecovered, &ctx);

        match result {
            TransitionResult::GuardFailed { .. } => {
                // Expected - guard prevents invalid transition
            }
            _ => panic!("Expected guard to fail"),
        }
    }

    // === Deletion Tests ===

    #[test]
    fn test_deletion_from_all_phases() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let phases = vec![
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::WaitingForPods,
            ClusterPhase::InitializingCluster,
            ClusterPhase::AssigningSlots,
            ClusterPhase::ConfiguringReplicas,
            ClusterPhase::Running,
            ClusterPhase::ScalingUpStatefulSet,
            ClusterPhase::WaitingForNewPods,
            ClusterPhase::AddingNodesToCluster,
            ClusterPhase::RebalancingSlots,
            ClusterPhase::ConfiguringNewReplicas,
            ClusterPhase::EvacuatingSlots,
            ClusterPhase::RemovingNodesFromCluster,
            ClusterPhase::ScalingDownStatefulSet,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Degraded,
            ClusterPhase::Failed,
        ];

        for phase in phases {
            let result = sm.transition(&phase, ClusterEvent::DeletionRequested, &ctx);

            match result {
                TransitionResult::Success { to, .. } => {
                    assert_eq!(to, ClusterPhase::Deleting, "Failed for phase {:?}", phase);
                }
                _ => panic!("Expected transition to Deleting from {:?}", phase),
            }
        }
    }

    #[test]
    fn test_deleting_is_terminal() {
        let sm = ClusterStateMachine::new();
        let valid_events = sm.valid_events(&ClusterPhase::Deleting);
        assert!(valid_events.is_empty());
    }

    // === Invalid Transition Tests ===

    #[test]
    fn test_invalid_transition_returns_error() {
        let sm = ClusterStateMachine::new();
        let ctx = make_transition_ctx(6, 6);

        let result = sm.transition(&ClusterPhase::Running, ClusterEvent::ResourcesApplied, &ctx);

        match result {
            TransitionResult::InvalidTransition { .. } => {
                // Expected
            }
            _ => panic!("Expected invalid transition"),
        }
    }
}

// ============================================================================
// determine_event Tests
// ============================================================================

mod determine_event_tests {
    use super::*;

    #[test]
    fn test_deletion_takes_priority() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::Running, &ctx, true);
        assert_eq!(event, ClusterEvent::DeletionRequested);
    }

    #[test]
    fn test_pending_returns_resources_applied() {
        let ctx = TransitionContext::new(0, 6);
        let event = determine_event(&ClusterPhase::Pending, &ctx, false);
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }

    #[test]
    fn test_creating_returns_resources_applied() {
        let ctx = TransitionContext::new(0, 6);
        let event = determine_event(&ClusterPhase::Creating, &ctx, false);
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }

    #[test]
    fn test_waiting_for_pods_returns_pods_running_when_ready() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::WaitingForPods, &ctx, false);
        assert_eq!(event, ClusterEvent::PodsRunning);
    }

    #[test]
    fn test_waiting_for_pods_returns_resources_applied_when_not_ready() {
        let ctx = TransitionContext::new(3, 6);
        let event = determine_event(&ClusterPhase::WaitingForPods, &ctx, false);
        assert_eq!(event, ClusterEvent::ResourcesApplied);
    }

    #[test]
    fn test_initializing_returns_phase_complete() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::InitializingCluster, &ctx, false);
        assert_eq!(event, ClusterEvent::PhaseComplete);
    }

    #[test]
    fn test_running_returns_degraded_when_partial() {
        let ctx = TransitionContext::new(4, 6);
        let event = determine_event(&ClusterPhase::Running, &ctx, false);
        assert_eq!(event, ClusterEvent::ReplicasDegraded);
    }

    #[test]
    fn test_running_returns_healthy_when_all_ready() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::Running, &ctx, false);
        assert_eq!(event, ClusterEvent::ClusterHealthy);
    }

    #[test]
    fn test_failed_returns_error_when_some_ready() {
        // When only some pods are ready, determine_event returns ReconcileError
        // The reconciler then decides whether to attempt recovery
        let ctx = TransitionContext::new(3, 6);
        let event = determine_event(&ClusterPhase::Failed, &ctx, false);
        assert_eq!(event, ClusterEvent::ReconcileError);
    }

    #[test]
    fn test_failed_returns_recovery_when_all_ready() {
        // When all pods are ready, determine_event returns RecoveryInitiated
        // This goes to Degraded state for health verification
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::Failed, &ctx, false);
        assert_eq!(event, ClusterEvent::RecoveryInitiated);
    }

    #[test]
    fn test_degraded_returns_recovered_when_all_ready() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::Degraded, &ctx, false);
        assert_eq!(event, ClusterEvent::FullyRecovered);
    }

    #[test]
    fn test_degraded_returns_degraded_when_partial() {
        let ctx = TransitionContext::new(4, 6);
        let event = determine_event(&ClusterPhase::Degraded, &ctx, false);
        assert_eq!(event, ClusterEvent::ReplicasDegraded);
    }

    #[test]
    fn test_verifying_returns_healthy_when_ready() {
        let ctx = TransitionContext::new(6, 6);
        let event = determine_event(&ClusterPhase::VerifyingClusterHealth, &ctx, false);
        assert_eq!(event, ClusterEvent::ClusterHealthy);
    }

    #[test]
    fn test_verifying_returns_degraded_when_partial() {
        let ctx = TransitionContext::new(4, 6);
        let event = determine_event(&ClusterPhase::VerifyingClusterHealth, &ctx, false);
        assert_eq!(event, ClusterEvent::ReplicasDegraded);
    }
}

// ============================================================================
// ClusterHealthResult Tests
// ============================================================================

mod cluster_health_result_tests {
    use super::*;

    #[test]
    fn test_healthy_result() {
        let result = ClusterHealthResult {
            is_healthy: true,
            slots_assigned: 16384,
            healthy_masters: 3,
        };

        assert!(result.is_healthy);
        assert_eq!(result.slots_assigned, 16384);
        assert_eq!(result.healthy_masters, 3);
    }

    #[test]
    fn test_unhealthy_result() {
        let result = ClusterHealthResult {
            is_healthy: false,
            slots_assigned: 8192,
            healthy_masters: 2,
        };

        assert!(!result.is_healthy);
        assert_eq!(result.slots_assigned, 8192);
        assert_eq!(result.healthy_masters, 2);
    }
}
