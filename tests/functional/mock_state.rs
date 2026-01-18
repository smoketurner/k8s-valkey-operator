//! Mock infrastructure for simulating cluster state in functional tests.
//!
//! This module provides a `MockClusterState` struct that simulates the state
//! of a ValkeyCluster without requiring a live Kubernetes cluster.
//!
//! ## Design Philosophy
//!
//! Instead of duplicating production logic, this mock:
//! 1. Uses the actual `determine_event` function from production code
//! 2. Simulates only the external state changes (pods, slots, nodes)
//! 3. Delegates event determination to the real implementation
//!
//! This ensures tests stay in sync with production behavior automatically.

use valkey_operator::controller::cluster_phases::{PhaseContext, ScaleDirection};
use valkey_operator::controller::cluster_state_machine::{
    ClusterEvent, ClusterStateMachine, TransitionContext, TransitionResult, determine_event,
};
use valkey_operator::crd::{ClusterPhase, total_pods};

/// Mock infrastructure for simulating cluster state.
///
/// This struct represents the logical state of a ValkeyCluster and can be
/// used to test state machine transitions without requiring a real cluster.
#[derive(Debug, Clone)]
pub struct MockClusterState {
    /// Cluster name.
    pub name: String,
    /// Cluster namespace.
    pub namespace: String,
    /// Target number of masters from spec.
    pub target_masters: i32,
    /// Target replicas per master from spec.
    pub target_replicas_per_master: i32,
    /// Current number of masters in the cluster (from CLUSTER INFO).
    pub current_masters: i32,
    /// Number of running pods.
    pub running_pods: i32,
    /// Number of ready pods.
    pub ready_pods: i32,
    /// Number of nodes in the cluster.
    pub nodes_in_cluster: i32,
    /// Whether slots have been assigned.
    pub slots_assigned: bool,
    /// Current phase.
    pub phase: ClusterPhase,
    /// Generation counter.
    pub generation: i64,
    /// Whether the spec has changed.
    pub spec_changed: bool,
    /// Whether deletion has been requested.
    pub deletion_requested: bool,
}

impl MockClusterState {
    /// Create a new mock cluster state in Pending phase.
    pub fn new(name: &str, masters: i32, replicas_per_master: i32) -> Self {
        Self {
            name: name.to_string(),
            namespace: "default".to_string(),
            target_masters: masters,
            target_replicas_per_master: replicas_per_master,
            current_masters: 0,
            running_pods: 0,
            ready_pods: 0,
            nodes_in_cluster: 0,
            slots_assigned: false,
            phase: ClusterPhase::Pending,
            generation: 1,
            spec_changed: false,
            deletion_requested: false,
        }
    }

    /// Create a mock cluster state that's already running.
    pub fn running(name: &str, masters: i32, replicas_per_master: i32) -> Self {
        let total = total_pods(masters, replicas_per_master);
        Self {
            name: name.to_string(),
            namespace: "default".to_string(),
            target_masters: masters,
            target_replicas_per_master: replicas_per_master,
            current_masters: masters,
            running_pods: total,
            ready_pods: total,
            nodes_in_cluster: total,
            slots_assigned: true,
            phase: ClusterPhase::Running,
            generation: 1,
            spec_changed: false,
            deletion_requested: false,
        }
    }

    /// Create a mock cluster state in degraded state.
    pub fn degraded(name: &str, masters: i32, replicas_per_master: i32, ready: i32) -> Self {
        Self {
            name: name.to_string(),
            namespace: "default".to_string(),
            target_masters: masters,
            target_replicas_per_master: replicas_per_master,
            current_masters: masters,
            running_pods: ready,
            ready_pods: ready,
            nodes_in_cluster: ready,
            slots_assigned: true,
            phase: ClusterPhase::Degraded,
            generation: 1,
            spec_changed: false,
            deletion_requested: false,
        }
    }

    /// Create a mock cluster state in failed state.
    pub fn failed(name: &str, masters: i32, replicas_per_master: i32) -> Self {
        Self {
            name: name.to_string(),
            namespace: "default".to_string(),
            target_masters: masters,
            target_replicas_per_master: replicas_per_master,
            current_masters: masters,
            running_pods: 0,
            ready_pods: 0,
            nodes_in_cluster: 0,
            slots_assigned: true,
            phase: ClusterPhase::Failed,
            generation: 1,
            spec_changed: false,
            deletion_requested: false,
        }
    }

    /// Calculate desired total pods.
    pub fn desired_replicas(&self) -> i32 {
        total_pods(self.target_masters, self.target_replicas_per_master)
    }

    /// Convert to PhaseContext for use with phase handlers.
    pub fn to_phase_context(&self) -> PhaseContext {
        PhaseContext {
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            target_masters: self.target_masters,
            target_replicas_per_master: self.target_replicas_per_master,
            current_masters: self.current_masters,
            running_pods: self.running_pods,
            ready_pods: self.ready_pods,
            nodes_in_cluster: self.nodes_in_cluster,
            spec_changed: self.spec_changed,
            generation: self.generation,
        }
    }

    /// Convert to TransitionContext for use with state machine.
    pub fn to_transition_context(&self) -> TransitionContext {
        TransitionContext::new(self.ready_pods, self.desired_replicas())
            .with_spec_changed(self.spec_changed)
    }

    /// Get the scale direction based on current vs target masters.
    pub fn scale_direction(&self) -> ScaleDirection {
        self.to_phase_context().scale_direction()
    }

    /// Apply an event to the state machine and return the result.
    pub fn apply_event(&mut self, event: ClusterEvent) -> TransitionResult {
        let ctx = self.to_transition_context();
        let state_machine = ClusterStateMachine::new();
        let result = state_machine.transition(&self.phase, event, &ctx);

        // If transition was successful, update the phase
        if let TransitionResult::Success { to, .. } = &result {
            self.phase = *to;
        }

        result
    }

    /// Simulate pods becoming running.
    pub fn simulate_pods_running(&mut self) {
        self.running_pods = self.desired_replicas();
    }

    /// Simulate pods becoming ready.
    pub fn simulate_pods_ready(&mut self) {
        self.ready_pods = self.desired_replicas();
        self.running_pods = self.desired_replicas();
    }

    /// Simulate cluster initialization (CLUSTER MEET).
    pub fn simulate_cluster_meet(&mut self) {
        self.nodes_in_cluster = self.running_pods;
    }

    /// Simulate slot assignment.
    pub fn simulate_slots_assigned(&mut self) {
        self.slots_assigned = true;
        self.current_masters = self.target_masters;
    }

    /// Simulate replica configuration.
    pub fn simulate_replicas_configured(&mut self) {
        // Replicas are now configured, nothing special to track
    }

    /// Simulate new pods for scale-up.
    pub fn simulate_new_pods_running(&mut self) {
        self.running_pods = self.desired_replicas();
    }

    /// Simulate new pods ready for scale-up.
    pub fn simulate_new_pods_ready(&mut self) {
        self.ready_pods = self.desired_replicas();
        self.running_pods = self.desired_replicas();
    }

    /// Simulate nodes added to cluster during scale-up.
    pub fn simulate_nodes_added(&mut self) {
        self.nodes_in_cluster = self.desired_replicas();
    }

    /// Simulate slot rebalancing completion.
    pub fn simulate_slots_rebalanced(&mut self) {
        self.current_masters = self.target_masters;
    }

    /// Simulate slot evacuation for scale-down.
    pub fn simulate_slots_evacuated(&mut self) {
        // Slots are evacuated from nodes being removed
    }

    /// Simulate nodes removed from cluster during scale-down.
    pub fn simulate_nodes_removed(&mut self) {
        self.nodes_in_cluster = self.desired_replicas();
    }

    /// Simulate StatefulSet scale-down completion.
    pub fn simulate_statefulset_scaled_down(&mut self) {
        self.running_pods = self.desired_replicas();
        self.ready_pods = self.desired_replicas();
    }

    /// Simulate cluster becoming healthy.
    pub fn simulate_cluster_healthy(&mut self) {
        self.ready_pods = self.desired_replicas();
        self.running_pods = self.desired_replicas();
        self.nodes_in_cluster = self.desired_replicas();
        self.current_masters = self.target_masters;
        self.slots_assigned = true;
    }

    /// Set the spec to trigger scale-up.
    pub fn set_scale_up(&mut self, new_masters: i32, new_replicas: i32) {
        self.target_masters = new_masters;
        self.target_replicas_per_master = new_replicas;
        self.spec_changed = true;
    }

    /// Set the spec to trigger scale-down.
    pub fn set_scale_down(&mut self, new_masters: i32, new_replicas: i32) {
        self.target_masters = new_masters;
        self.target_replicas_per_master = new_replicas;
        self.spec_changed = true;
    }

    /// Simulate partial recovery (some pods ready).
    pub fn simulate_partial_recovery(&mut self, ready_count: i32) {
        self.ready_pods = ready_count;
        self.running_pods = ready_count;
        self.nodes_in_cluster = ready_count;
    }

    /// Run the state machine until it reaches a stable Running state.
    /// Returns the sequence of phases traversed.
    ///
    /// This method simulates the external conditions needed for each phase
    /// (pods becoming ready, slots being assigned, etc.) and then uses the
    /// production `determine_event` function to determine what event would
    /// naturally occur.
    ///
    /// A "stable Running state" means the cluster is Running AND:
    /// - No scale operation is pending (target == current masters)
    /// - All replicas are ready
    pub fn run_until_running(&mut self, max_iterations: usize) -> Vec<ClusterPhase> {
        let mut phases = Vec::new();
        phases.push(self.phase);

        for _ in 0..max_iterations {
            // Check if we've reached stable Running state
            if self.phase == ClusterPhase::Running && self.is_stable() {
                break;
            }

            // Simulate the appropriate external conditions for the current phase
            self.simulate_phase_completion();

            // Determine the event using the production function
            let event = self.determine_next_event();

            // Apply the event
            let result = self.apply_event(event.clone());

            match result {
                TransitionResult::Success { to, .. } => {
                    if to != *phases.last().unwrap_or(&ClusterPhase::Pending) {
                        phases.push(to);
                    }
                }
                TransitionResult::InvalidTransition { .. }
                | TransitionResult::GuardFailed { .. } => {
                    // Transition failed, break out
                    break;
                }
            }
        }

        phases
    }

    /// Check if the cluster is in a stable state (no pending operations).
    fn is_stable(&self) -> bool {
        // No scale operation pending
        let no_scale = self.target_masters == self.current_masters;
        // All replicas ready
        let all_ready = self.ready_pods >= self.desired_replicas();
        // Replica count matches
        let replica_count_matches = self.running_pods == self.desired_replicas();

        no_scale && all_ready && replica_count_matches
    }

    /// Simulate the external conditions that would cause a phase to complete.
    ///
    /// This only mutates state to simulate what would happen in a real cluster
    /// (pods starting, slots being assigned, etc.). It does NOT determine events.
    fn simulate_phase_completion(&mut self) {
        match self.phase {
            // Initial creation phases - simulate infrastructure becoming ready
            ClusterPhase::WaitingForPods => {
                self.simulate_pods_ready();
            }
            ClusterPhase::InitializingCluster => {
                self.simulate_cluster_meet();
            }
            ClusterPhase::AssigningSlots => {
                self.simulate_slots_assigned();
            }
            ClusterPhase::ConfiguringReplicas => {
                self.simulate_replicas_configured();
            }
            ClusterPhase::VerifyingClusterHealth => {
                self.simulate_cluster_healthy();
            }

            // Scale-up phases - simulate new infrastructure
            ClusterPhase::WaitingForNewPods => {
                self.simulate_new_pods_ready();
            }
            ClusterPhase::AddingNodesToCluster => {
                self.simulate_nodes_added();
            }
            ClusterPhase::RebalancingSlots => {
                self.simulate_slots_rebalanced();
            }
            ClusterPhase::ConfiguringNewReplicas => {
                // Just needs phase completion, no state simulation needed
            }

            // Scale-down phases - simulate infrastructure removal
            ClusterPhase::EvacuatingSlots => {
                self.simulate_slots_evacuated();
            }
            ClusterPhase::RemovingNodesFromCluster => {
                self.simulate_nodes_removed();
            }
            ClusterPhase::ScalingDownStatefulSet => {
                self.simulate_statefulset_scaled_down();
            }

            // Other phases don't need state simulation
            _ => {}
        }
    }

    /// Determine the next event using production logic.
    ///
    /// Uses:
    /// - `PhaseContext::scale_direction()` for scale detection (production code)
    /// - `determine_event` for replica-status-driven events (production code)
    /// - Phase-specific knowledge for phase-driven events (matches state machine design)
    ///
    /// **IMPORTANT: Keep in sync with production code!**
    ///
    /// The scale detection logic here must mirror the detection logic in:
    /// - `cluster_phases.rs:handle_running()` - Running state scale detection
    /// - `cluster_phases.rs:handle_degraded()` - Degraded state scale detection
    ///
    /// When adding new events or changing detection logic in production,
    /// update this function accordingly. See CLAUDE.md for details.
    fn determine_next_event(&self) -> ClusterEvent {
        // For Running and Degraded phases, use production scale_direction() for scale detection
        match self.phase {
            ClusterPhase::Running | ClusterPhase::Degraded => {
                // Use production scale_direction() method from PhaseContext
                let phase_ctx = self.to_phase_context();
                match phase_ctx.scale_direction() {
                    ScaleDirection::Up => return ClusterEvent::ScaleUpDetected,
                    ScaleDirection::Down => return ClusterEvent::ScaleDownDetected,
                    ScaleDirection::ReplicaChange => {
                        // Replica change without master change - determine direction by pod count
                        let current_pods = self.running_pods;
                        let desired_pods = phase_ctx.desired_replicas();
                        if desired_pods > current_pods {
                            // Replica scale-UP: uses the scale-up path
                            return ClusterEvent::ScaleUpDetected;
                        } else if desired_pods < current_pods {
                            // Replica scale-DOWN: goes directly to RemovingNodesFromCluster
                            // (skips EvacuatingSlots since replicas don't hold slots)
                            return ClusterEvent::ReplicaScaleDownDetected;
                        }
                        // If pods match, fall through to determine_event
                    }
                    ScaleDirection::None => {}
                }
            }
            // Phase-driven transitions return PhaseComplete
            // (these phases always complete via PhaseComplete per the state machine design)
            ClusterPhase::InitializingCluster
            | ClusterPhase::ConfiguringReplicas
            | ClusterPhase::ScalingUpStatefulSet
            | ClusterPhase::AddingNodesToCluster
            | ClusterPhase::ConfiguringNewReplicas
            | ClusterPhase::EvacuatingSlots
            | ClusterPhase::RemovingNodesFromCluster
            | ClusterPhase::ScalingDownStatefulSet => {
                return ClusterEvent::PhaseComplete;
            }
            // AssigningSlots and RebalancingSlots: skip replica config if no replicas
            // (this is encoded in the state machine transitions)
            ClusterPhase::AssigningSlots | ClusterPhase::RebalancingSlots => {
                if self.target_replicas_per_master == 0 {
                    return ClusterEvent::ClusterHealthy;
                }
                return ClusterEvent::PhaseComplete;
            }
            _ => {}
        }

        // Use production determine_event for all other cases
        let ctx = self.to_transition_context();
        determine_event(&self.phase, &ctx, self.deletion_requested)
    }

    /// Request deletion.
    pub fn request_deletion(&mut self) {
        self.deletion_requested = true;
    }
}

/// Result of a state machine simulation run.
#[derive(Debug, Clone)]
pub struct SimulationResult {
    /// The phases that were traversed.
    pub phases: Vec<ClusterPhase>,
    /// The final phase.
    pub final_phase: ClusterPhase,
    /// Whether the simulation completed successfully.
    pub success: bool,
}

/// Expected phase sequence for different topologies.
pub mod expected_sequences {
    use super::*;

    /// Expected phases for initial creation with replicas.
    pub fn creation_with_replicas() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::WaitingForPods,
            ClusterPhase::InitializingCluster,
            ClusterPhase::AssigningSlots,
            ClusterPhase::ConfiguringReplicas,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Running,
        ]
    }

    /// Expected phases for initial creation without replicas.
    pub fn creation_without_replicas() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Pending,
            ClusterPhase::Creating,
            ClusterPhase::WaitingForPods,
            ClusterPhase::InitializingCluster,
            ClusterPhase::AssigningSlots,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Running,
        ]
    }

    /// Expected phases for scale-up with replicas.
    pub fn scale_up_with_replicas() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Running,
            ClusterPhase::ScalingUpStatefulSet,
            ClusterPhase::WaitingForNewPods,
            ClusterPhase::AddingNodesToCluster,
            ClusterPhase::RebalancingSlots,
            ClusterPhase::ConfiguringNewReplicas,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Running,
        ]
    }

    /// Expected phases for scale-up without replicas.
    pub fn scale_up_without_replicas() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Running,
            ClusterPhase::ScalingUpStatefulSet,
            ClusterPhase::WaitingForNewPods,
            ClusterPhase::AddingNodesToCluster,
            ClusterPhase::RebalancingSlots,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Running,
        ]
    }

    /// Expected phases for scale-down (masters changing).
    pub fn scale_down() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Running,
            ClusterPhase::EvacuatingSlots,
            ClusterPhase::RemovingNodesFromCluster,
            ClusterPhase::ScalingDownStatefulSet,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Running,
        ]
    }

    /// Expected phases for replica-only scale-down (masters unchanged).
    /// Skips EvacuatingSlots since replicas don't hold slots.
    pub fn replica_scale_down() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Running,
            ClusterPhase::RemovingNodesFromCluster,
            ClusterPhase::ScalingDownStatefulSet,
            ClusterPhase::VerifyingClusterHealth,
            ClusterPhase::Running,
        ]
    }

    /// Expected phases for recovery from degraded to running.
    pub fn degraded_to_running() -> Vec<ClusterPhase> {
        vec![ClusterPhase::Degraded, ClusterPhase::Running]
    }

    /// Expected phases for recovery from failed through degraded to running.
    pub fn failed_to_running() -> Vec<ClusterPhase> {
        vec![
            ClusterPhase::Failed,
            ClusterPhase::Degraded,
            ClusterPhase::Running,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_cluster_state_new() {
        let state = MockClusterState::new("test", 3, 1);
        assert_eq!(state.name, "test");
        assert_eq!(state.target_masters, 3);
        assert_eq!(state.target_replicas_per_master, 1);
        assert_eq!(state.phase, ClusterPhase::Pending);
        assert_eq!(state.desired_replicas(), 6);
    }

    #[test]
    fn test_mock_cluster_state_running() {
        let state = MockClusterState::running("test", 3, 1);
        assert_eq!(state.phase, ClusterPhase::Running);
        assert_eq!(state.running_pods, 6);
        assert_eq!(state.ready_pods, 6);
        assert_eq!(state.current_masters, 3);
    }

    #[test]
    fn test_mock_cluster_state_degraded() {
        let state = MockClusterState::degraded("test", 3, 1, 4);
        assert_eq!(state.phase, ClusterPhase::Degraded);
        assert_eq!(state.ready_pods, 4);
    }

    #[test]
    fn test_to_phase_context() {
        let state = MockClusterState::running("test", 3, 1);
        let ctx = state.to_phase_context();
        assert_eq!(ctx.name, "test");
        assert_eq!(ctx.target_masters, 3);
        assert_eq!(ctx.current_masters, 3);
        assert_eq!(ctx.running_pods, 6);
    }

    #[test]
    fn test_to_transition_context() {
        let state = MockClusterState::running("test", 3, 1);
        let ctx = state.to_transition_context();
        assert_eq!(ctx.ready_replicas, 6);
        assert_eq!(ctx.desired_replicas, 6);
    }

    #[test]
    fn test_scale_direction_up() {
        let mut state = MockClusterState::running("test", 3, 1);
        state.target_masters = 6;
        assert_eq!(state.scale_direction(), ScaleDirection::Up);
    }

    #[test]
    fn test_scale_direction_down() {
        let mut state = MockClusterState::running("test", 6, 1);
        state.target_masters = 3;
        assert_eq!(state.scale_direction(), ScaleDirection::Down);
    }

    #[test]
    fn test_scale_direction_none() {
        let state = MockClusterState::running("test", 3, 1);
        assert_eq!(state.scale_direction(), ScaleDirection::None);
    }

    #[test]
    fn test_apply_event_success() {
        let mut state = MockClusterState::new("test", 3, 1);
        let result = state.apply_event(ClusterEvent::ResourcesApplied);

        match result {
            TransitionResult::Success { to, .. } => {
                assert_eq!(to, ClusterPhase::Creating);
                assert_eq!(state.phase, ClusterPhase::Creating);
            }
            _ => panic!("Expected successful transition"),
        }
    }

    #[test]
    fn test_simulation_helpers() {
        let mut state = MockClusterState::new("test", 3, 1);

        state.simulate_pods_running();
        assert_eq!(state.running_pods, 6);

        state.simulate_pods_ready();
        assert_eq!(state.ready_pods, 6);

        state.simulate_cluster_meet();
        assert_eq!(state.nodes_in_cluster, 6);

        state.simulate_slots_assigned();
        assert!(state.slots_assigned);
        assert_eq!(state.current_masters, 3);
    }
}
