# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Kubernetes operator for managing Valkey Clusters** built in Rust using kube-rs. It manages ValkeyCluster and ValkeyUpgrade custom resources.

**For detailed documentation, see the `docs/` directory.**

## Version Requirements

| Component | Minimum Version |
|-----------|-----------------|
| Rust | 1.92+ (Edition 2024) |
| Kubernetes | 1.35+ |
| kube-rs | 2.x |
| Valkey | 9.x |

## Build & Test Commands

```bash
# Build
make build              # Build release binary
make docker-build       # Build Docker image
make docker-push        # Push Docker image

# Development
make run                # Run operator locally
make fmt                # Format code
make lint               # Run clippy lints
make check              # Run cargo check

# Testing
make test               # Run unit tests
make test-integration   # Run integration tests

# Installation
make install            # Install CRD and RBAC
make install-crd        # Install just CRD
make install-rbac       # Install just RBAC
make uninstall          # Uninstall from cluster

# Deployment
make deploy             # Deploy operator to cluster
make undeploy           # Undeploy operator
make deploy-sample      # Deploy sample ValkeyCluster
make delete-sample      # Delete sample ValkeyCluster

# Cleanup
make clean              # Clean build artifacts
make clean-all          # Uninstall and clean
```

## Architecture

### Source Files

| File | Purpose |
|------|---------|
| `src/main.rs` | Entry point, leader election, startup |
| `src/lib.rs` | Controller setup, stream configuration |
| `src/crd/mod.rs` | Custom Resource Definitions |
| `src/crd/valkey_cluster.rs` | ValkeyCluster CRD and ClusterPhase enum |
| `src/crd/valkey_upgrade.rs` | ValkeyUpgrade CRD and UpgradePhase enum |
| `src/controller/cluster_state_machine.rs` | Cluster phase transitions and events |
| `src/controller/cluster_phases.rs` | Phase handler implementations |
| `src/controller/cluster_reconciler.rs` | Cluster reconciliation logic |
| `src/controller/upgrade_state_machine.rs` | Upgrade phase transitions |
| `src/controller/upgrade_reconciler.rs` | Upgrade reconciliation logic |
| `src/controller/error.rs` | Error types and classification |
| `src/controller/status.rs` | Condition management |
| `src/controller/context.rs` | Shared context (client, recorder) |
| `src/resources/common.rs` | Resource generators |
| `src/webhooks/server.rs` | Admission webhook server |
| `src/health.rs` | Health endpoints and metrics |

### CRD Structure

- **API group**: `valkey-operator.smoketurner.com`
- **API version**: `v1alpha1`
- **Kinds**: `ValkeyCluster`, `ValkeyUpgrade`
- **Short name**: `vc` (for ValkeyCluster)

### Generated Resources

The operator creates these Kubernetes resources for each ValkeyCluster:
- **StatefulSet**: Valkey pods with stable identity
- **Headless Service**: Cluster discovery
- **Client Service**: Client access endpoint
- **Read Service** (optional): Read-only traffic distribution
- **PodDisruptionBudget**: Maintain quorum
- **Certificate**: TLS certs via cert-manager

---

## State Machine Architecture

The operator uses **event-driven finite state machines** for both cluster lifecycle and upgrades. Understanding these is critical for modifying reconciliation logic.

### Dual State Machine Design

| State Machine | CRD | File | Purpose |
|---------------|-----|------|---------|
| Cluster FSM | ValkeyCluster | `cluster_state_machine.rs` | Creation, scaling, recovery |
| Upgrade FSM | ValkeyUpgrade | `upgrade_state_machine.rs` | Rolling version upgrades |

### Key Concepts

1. **Phases**: Current state of the resource (stored in `status.phase`)
2. **Events**: Triggers that cause phase transitions (e.g., `PodsRunning`, `ScaleUpDetected`)
3. **Transitions**: Valid `(from_phase, event) → to_phase` mappings
4. **Guards**: Conditions that must be true for a transition to occur
5. **Handlers**: Functions that execute operations for each phase (`handle_<phase>`)

---

## ValkeyCluster State Machine

### Phases (18 total)

| Category | Phases |
|----------|--------|
| **Initial Creation** | `Pending` → `Creating` → `WaitingForPods` → `InitializingCluster` → `AssigningSlots` → `ConfiguringReplicas` |
| **Steady State** | `Running` |
| **Scale-Up** | `ScalingUpStatefulSet` → `WaitingForNewPods` → `AddingNodesToCluster` → `RebalancingSlots` → `ConfiguringNewReplicas` |
| **Scale-Down** | `EvacuatingSlots` → `RemovingNodesFromCluster` → `ScalingDownStatefulSet` |
| **Completion** | `VerifyingClusterHealth` |
| **Problem** | `Degraded`, `Failed` |
| **Terminal** | `Deleting` |

### Key Events

| Event | When Fired | Typical Transition |
|-------|------------|-------------------|
| `ResourcesApplied` | K8s resources created | Pending → Creating |
| `PodsRunning` | All pods Running | WaitingForPods → InitializingCluster |
| `PhaseComplete` | Phase handler finished | Most phase progressions |
| `ScaleUpDetected` | `target_masters > current_masters` | Running → ScalingUpStatefulSet |
| `ScaleDownDetected` | `target_masters < current_masters` | Running → EvacuatingSlots |
| `ReplicaScaleDownDetected` | Masters unchanged, replicas decreased | Running → RemovingNodesFromCluster |
| `ClusterHealthy` | All pods ready, slots assigned | VerifyingClusterHealth → Running |
| `ReplicasDegraded` | `0 < ready_pods < desired_pods` | Running → Degraded |
| `DeletionRequested` | Finalizer triggered | Any → Deleting |

### Operation Flows

**Initial Creation:**
```
Pending → Creating → WaitingForPods → InitializingCluster → AssigningSlots → ConfiguringReplicas → VerifyingClusterHealth → Running
```

**Scale-Up (e.g., 3→6 masters):**
```
Running → ScalingUpStatefulSet → WaitingForNewPods → AddingNodesToCluster → RebalancingSlots → ConfiguringNewReplicas → VerifyingClusterHealth → Running
```

**Scale-Down (e.g., 6→3 masters):**
```
Running → EvacuatingSlots → RemovingNodesFromCluster → ScalingDownStatefulSet → VerifyingClusterHealth → Running
```

**Replica Scale-Down (e.g., 3m/2r→3m/1r):**
```
Running → RemovingNodesFromCluster → ScalingDownStatefulSet → VerifyingClusterHealth → Running
```
*Note: Skips EvacuatingSlots because replicas don't hold slots.*

### Scale Detection Logic

Located in `cluster_phases.rs:handle_running()`:

```rust
// Master scale change
if target_masters > current_masters { return ScalingUpStatefulSet }
if target_masters < current_masters { return EvacuatingSlots }

// Replica-only change (masters unchanged)
if running_pods > desired_replicas { return RemovingNodesFromCluster }  // scale-down
if running_pods < desired_replicas { return ScalingUpStatefulSet }      // scale-up
```

---

## ValkeyUpgrade State Machine

### Upgrade Phases

| Phase | Description |
|-------|-------------|
| `Pending` | Upgrade not yet started |
| `PreChecks` | Verifying cluster health before starting |
| `InProgress` | Upgrade actively running |
| `Completed` | Upgrade finished successfully |
| `Failed` | Upgrade failed |
| `RollingBack` | Rolling back the upgrade |
| `RolledBack` | Rollback completed |

### Per-Shard States

Each shard (master + replicas) progresses through:

```
Pending → UpgradingReplicas → WaitingForSync → FailingOver → WaitingForClusterStable → UpgradingOldMaster → Completed
```

| State | What Happens | Valkey Commands |
|-------|--------------|-----------------|
| `UpgradingReplicas` | Delete replica pods (restart with new image) | - |
| `WaitingForSync` | Wait for replication offset match | `INFO REPLICATION` |
| `FailingOver` | Promote best replica to master | `WAIT`, `CLUSTER FAILOVER` |
| `WaitingForClusterStable` | Verify old master is now replica | `CLUSTER INFO`, `ROLE` |
| `UpgradingOldMaster` | Delete old master pod | - |

### Safety Features

- **Operation lock**: Prevents concurrent scale/upgrade operations
- **Upgrade protection**: `upgrade-in-progress` annotation blocks ValkeyCluster spec changes
- **Replica-first**: Replicas upgraded before master failover (no data loss)
- **Sync verification**: Replication offset must match before failover
- **Sequential processing**: One shard at a time

---

## ClusterTopology

The `ClusterTopology` struct (`cluster_reconciler.rs`) correlates Kubernetes pod info with Valkey cluster state:

- **IP → ordinal mapping**: Look up pod ordinal by IP address
- **ordinal → node_id mapping**: Find Valkey node ID by pod ordinal
- **Node role identification**: Determine if a node is master or replica
- **Orphan detection**: Find cluster nodes with no matching pod (used by `CLUSTER FORGET`)
- **Scale candidate identification**: Identify nodes for promotion or removal

Built fresh on each reconciliation by querying both Kubernetes API and `CLUSTER NODES`.

---

## Key Invariants

### Cluster Operations

1. **No data loss on scale-down**: Slots MUST be evacuated before nodes are removed
2. **Deletion from any state**: All phases (except Deleting) can transition to Deleting
3. **Operation locks**: Scaling and initialization acquire locks to prevent concurrent operations
4. **Phase encodes direction**: No `pending_changes` field needed - phase name indicates operation type

### Upgrades

1. **Sequential shard processing**: Only one shard upgraded at a time
2. **Replica-first**: Replicas upgraded before master failover
3. **Sync verification**: Replication offset must match before failover
4. **Cluster stability**: Must verify old master recognizes it's a replica before deleting

---

## Coding Standards

### Panic-Free Code

The operator must **never panic** in production code:

- **Never use** `unwrap()`, `expect()`, or `panic!()`
- **Always use** `Result<T, Error>` with `?` operator
- **For Options**, use `unwrap_or_default()`, `map()`, `and_then()`
- **Test code** may use `unwrap()` where panicking is acceptable

### Error Handling

- **Transient**: Retry with backoff (network issues)
- **Validation**: Fail fast, update status
- **Permanent**: Require intervention, set Failed phase

### Reconciliation

- All operations must be **idempotent**
- Use **server-side apply** for updates
- Track **generation** to skip redundant reconciliations

---

## Testing

| Directory | Purpose |
|-----------|---------|
| `tests/unit/` | Component tests (no cluster) |
| `tests/functional/` | State machine simulation tests |
| `tests/integration/` | End-to-end tests (requires cluster) |
| `tests/proptest/` | Property-based tests |
| `tests/common/fixtures.rs` | Test fixtures |

### Keeping Functional Tests in Sync

The functional tests in `tests/functional/` simulate cluster state transitions without a real Kubernetes cluster. Key files:

| File | Purpose |
|------|---------|
| `mock_state.rs` | Mock cluster state and event determination |
| `scenario_tests.rs` | Multi-step scenario tests |
| `topology_tests.rs` | Topology-specific tests |
| `phase_handler_tests.rs` | Phase handler unit tests |

**Important:** When modifying state machine logic in production code, you must also update:

1. **`mock_state.rs:determine_next_event()`** - This function determines which `ClusterEvent` to fire based on current state. It should mirror the detection logic in `cluster_phases.rs` (e.g., `handle_running`, `handle_degraded`).

2. **`mock_state.rs:expected_sequences`** - Update expected phase sequences if the state machine path changes.

3. **Existing tests** - Update any tests that assert on specific phase sequences.

**Design Goal:** The mock uses production code where possible (`determine_event`, `PhaseContext::scale_direction()`) but has custom logic for scale detection that must stay synchronized. Future refactoring could extract scale detection into a shared pure function to eliminate this duplication.

---

## Documentation

- `docs/state-machines.md`: Phase transition diagrams and event details
- `docs/cluster-operations.md`: Detailed operation flows with code references
- `docs/features.md`: Feature configuration details
