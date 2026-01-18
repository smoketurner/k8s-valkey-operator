//! Slot migration planning - computes what needs to change, no I/O.
//!
//! This module provides pure functions for planning slot migrations.
//! It computes what slots need to move and where, without executing any commands.

use std::collections::{HashMap, HashSet};

use super::distribution::{TOTAL_SLOTS, calculate_distribution};

/// Identifies a node by its Valkey cluster node ID.
pub type NodeId = String;

/// Current ownership of slots in the cluster.
#[derive(Debug, Clone, Default)]
pub struct ClusterSlotState {
    /// Map from slot number to owning node ID.
    pub slot_to_node: HashMap<u16, NodeId>,
    /// Map from node ID to its ordinal (for ordering).
    pub node_ordinals: HashMap<NodeId, u16>,
    /// Set of node IDs that are masters.
    pub masters: HashSet<NodeId>,
}

impl ClusterSlotState {
    /// Create a new empty slot state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the node owning a slot, if any.
    pub fn owner_of(&self, slot: u16) -> Option<&NodeId> {
        self.slot_to_node.get(&slot)
    }

    /// Count total assigned slots.
    pub fn assigned_count(&self) -> usize {
        self.slot_to_node.len()
    }

    /// Get all unassigned slots.
    pub fn unassigned_slots(&self) -> Vec<u16> {
        (0..TOTAL_SLOTS)
            .filter(|s| !self.slot_to_node.contains_key(s))
            .collect()
    }

    /// Check if all slots are assigned.
    pub fn all_slots_assigned(&self) -> bool {
        self.slot_to_node.len() == TOTAL_SLOTS as usize
    }

    /// Add a master node with its slots.
    pub fn add_master(
        &mut self,
        node_id: NodeId,
        ordinal: u16,
        slots: impl IntoIterator<Item = u16>,
    ) {
        self.masters.insert(node_id.clone());
        self.node_ordinals.insert(node_id.clone(), ordinal);
        for slot in slots {
            self.slot_to_node.insert(slot, node_id.clone());
        }
    }
}

/// A planned slot migration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotMigration {
    /// The slot to migrate.
    pub slot: u16,
    /// The node currently owning the slot (None if unassigned).
    pub from_node: Option<NodeId>,
    /// The target node for the slot.
    pub to_node: NodeId,
}

impl SlotMigration {
    /// Create a new slot migration.
    pub fn new(slot: u16, from_node: Option<NodeId>, to_node: NodeId) -> Self {
        Self {
            slot,
            from_node,
            to_node,
        }
    }

    /// Check if this is a simple assignment (no source, unassigned slot).
    pub fn is_assignment(&self) -> bool {
        self.from_node.is_none()
    }
}

/// Complete migration plan.
#[derive(Debug, Clone, Default)]
pub struct MigrationPlan {
    /// Ordered list of slot migrations.
    pub migrations: Vec<SlotMigration>,
    /// Nodes being added (scale up).
    pub nodes_to_add: Vec<NodeId>,
    /// Nodes being removed (scale down).
    pub nodes_to_remove: Vec<NodeId>,
    /// Number of slots to move between nodes.
    pub slots_to_move: usize,
    /// Number of unassigned slots to assign.
    pub slots_to_assign: usize,
}

impl MigrationPlan {
    /// Create an empty migration plan.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Check if the plan is empty (no work to do).
    pub fn is_empty(&self) -> bool {
        self.migrations.is_empty()
    }

    /// Get total number of slot operations.
    pub fn total_operations(&self) -> usize {
        self.slots_to_move + self.slots_to_assign
    }
}

/// Compute a migration plan to reach target master count.
///
/// This is pure computation - no I/O involved.
///
/// # Arguments
/// * `current` - Current cluster slot state
/// * `target_master_count` - Desired number of masters
/// * `target_node_ids` - Node IDs for target masters, must be in ordinal order
pub fn plan_rebalance(
    current: &ClusterSlotState,
    target_master_count: u16,
    target_node_ids: &[NodeId],
) -> MigrationPlan {
    if target_master_count == 0 || target_node_ids.is_empty() {
        return MigrationPlan::empty();
    }

    let target_distribution = calculate_distribution(target_master_count);
    let mut migrations = Vec::new();
    let mut slots_to_move = 0;
    let mut slots_to_assign = 0;

    // For each target master, determine slots it should own
    for (range, target_node) in target_distribution.iter().zip(target_node_ids.iter()) {
        for slot in range.iter() {
            let current_owner = current.owner_of(slot);

            match current_owner {
                Some(owner) if owner == target_node => {
                    // Already correct, no migration needed
                }
                Some(owner) => {
                    // Slot needs to move
                    migrations.push(SlotMigration::new(
                        slot,
                        Some(owner.clone()),
                        target_node.clone(),
                    ));
                    slots_to_move += 1;
                }
                None => {
                    // Unassigned slot
                    migrations.push(SlotMigration::new(slot, None, target_node.clone()));
                    slots_to_assign += 1;
                }
            }
        }
    }

    // Determine nodes being added/removed
    let current_set: HashSet<&NodeId> = current.masters.iter().collect();
    let target_set: HashSet<&NodeId> = target_node_ids.iter().collect();

    let nodes_to_add: Vec<NodeId> = target_set
        .difference(&current_set)
        .map(|n| (*n).clone())
        .collect();

    let nodes_to_remove: Vec<NodeId> = current_set
        .difference(&target_set)
        .map(|n| (*n).clone())
        .collect();

    MigrationPlan {
        migrations,
        nodes_to_add,
        nodes_to_remove,
        slots_to_move,
        slots_to_assign,
    }
}

/// Plan initial slot assignment for a new cluster.
///
/// This is equivalent to `plan_rebalance` with an empty current state.
pub fn plan_initial_assignment(master_count: u16, node_ids: &[NodeId]) -> MigrationPlan {
    let empty_state = ClusterSlotState::new();
    plan_rebalance(&empty_state, master_count, node_ids)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::get_unwrap
)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_slot_state_new() {
        let state = ClusterSlotState::new();
        assert!(state.slot_to_node.is_empty());
        assert!(state.masters.is_empty());
        assert_eq!(state.assigned_count(), 0);
    }

    #[test]
    fn test_cluster_slot_state_add_master() {
        let mut state = ClusterSlotState::new();
        state.add_master("node-a".to_string(), 0, 0..100);

        assert_eq!(state.masters.len(), 1);
        assert!(state.masters.contains("node-a"));
        assert_eq!(state.assigned_count(), 100);
        assert_eq!(state.owner_of(0), Some(&"node-a".to_string()));
        assert_eq!(state.owner_of(99), Some(&"node-a".to_string()));
        assert_eq!(state.owner_of(100), None);
    }

    #[test]
    fn test_cluster_slot_state_unassigned_slots() {
        let mut state = ClusterSlotState::new();
        state.add_master("node-a".to_string(), 0, 0..5);

        let unassigned = state.unassigned_slots();
        assert_eq!(unassigned.len(), (TOTAL_SLOTS - 5) as usize);
        assert!(!unassigned.contains(&0));
        assert!(unassigned.contains(&5));
    }

    #[test]
    fn test_slot_migration_is_assignment() {
        let assignment = SlotMigration::new(100, None, "node-a".to_string());
        assert!(assignment.is_assignment());

        let migration = SlotMigration::new(100, Some("node-b".to_string()), "node-a".to_string());
        assert!(!migration.is_assignment());
    }

    #[test]
    fn test_plan_rebalance_empty_input() {
        let state = ClusterSlotState::new();
        let plan = plan_rebalance(&state, 0, &[]);
        assert!(plan.is_empty());

        let plan = plan_rebalance(&state, 3, &[]);
        assert!(plan.is_empty());
    }

    #[test]
    fn test_plan_initial_assignment_covers_all_slots() {
        let node_ids: Vec<NodeId> = vec!["node-a".into(), "node-b".into(), "node-c".into()];
        let plan = plan_initial_assignment(3, &node_ids);

        assert_eq!(plan.migrations.len(), TOTAL_SLOTS as usize);
        assert_eq!(plan.slots_to_assign, TOTAL_SLOTS as usize);
        assert_eq!(plan.slots_to_move, 0);

        // Verify all slots covered
        let assigned: HashSet<u16> = plan.migrations.iter().map(|m| m.slot).collect();
        assert_eq!(assigned.len(), TOTAL_SLOTS as usize);
    }

    #[test]
    fn test_plan_rebalance_no_op_when_balanced() {
        // Setup: 3-node cluster, all slots assigned correctly
        let node_ids: Vec<NodeId> = (0..3).map(|i| format!("node-{}", i)).collect();
        let mut current = ClusterSlotState::new();

        let dist = calculate_distribution(3);
        for (idx, range) in dist.iter().enumerate() {
            current.add_master(node_ids.get(idx).unwrap().clone(), idx as u16, range.iter());
        }

        // Rebalance with same masters - should be no-op
        let plan = plan_rebalance(&current, 3, &node_ids);
        assert!(plan.is_empty());
        assert_eq!(plan.slots_to_move, 0);
        assert_eq!(plan.slots_to_assign, 0);
    }

    #[test]
    fn test_plan_scale_up_redistributes_slots() {
        // Setup: 3-node cluster
        let old_nodes: Vec<NodeId> = (0..3).map(|i| format!("node-{}", i)).collect();
        let mut current = ClusterSlotState::new();

        let dist = calculate_distribution(3);
        for (idx, range) in dist.iter().enumerate() {
            current.add_master(
                old_nodes.get(idx).unwrap().clone(),
                idx as u16,
                range.iter(),
            );
        }

        // Scale to 6 nodes
        let new_nodes: Vec<NodeId> = (0..6).map(|i| format!("node-{}", i)).collect();
        let plan = plan_rebalance(&current, 6, &new_nodes);

        // Should move some slots to new nodes
        assert!(plan.slots_to_move > 0);
        assert_eq!(plan.nodes_to_add.len(), 3);
        assert!(plan.nodes_to_remove.is_empty());
    }

    #[test]
    fn test_plan_scale_down_evacuates_nodes() {
        // Setup: 6-node cluster
        let old_nodes: Vec<NodeId> = (0..6).map(|i| format!("node-{}", i)).collect();
        let mut current = ClusterSlotState::new();

        let dist = calculate_distribution(6);
        for (idx, range) in dist.iter().enumerate() {
            current.add_master(
                old_nodes.get(idx).unwrap().clone(),
                idx as u16,
                range.iter(),
            );
        }

        // Scale to 3 nodes
        let new_nodes: Vec<NodeId> = (0..3).map(|i| format!("node-{}", i)).collect();
        let plan = plan_rebalance(&current, 3, &new_nodes);

        // Should move slots from removed nodes
        assert!(plan.slots_to_move > 0);
        assert!(plan.nodes_to_add.is_empty());
        assert_eq!(plan.nodes_to_remove.len(), 3);
    }

    #[test]
    fn test_migration_plan_total_operations() {
        let mut plan = MigrationPlan::empty();
        plan.slots_to_move = 100;
        plan.slots_to_assign = 50;
        assert_eq!(plan.total_operations(), 150);
    }
}
