# State Machines

This document describes the formal finite state machines (FSMs) that govern the lifecycle of ValkeyCluster and ValkeyUpgrade resources.

## ValkeyCluster State Machine

The ValkeyCluster lifecycle is managed through a formal FSM with explicit state transitions, guards, and actions.

### Phases

| Phase | Description |
|-------|-------------|
| `Pending` | Initial state, waiting for reconciliation |
| `Creating` | Kubernetes resources (StatefulSet, Services, etc.) are being created |
| `Initializing` | Pods are running, executing `CLUSTER MEET` to form cluster |
| `AssigningSlots` | Initial hash slot assignment via `CLUSTER ADDSLOTS` |
| `Running` | Cluster is healthy and operational |
| `DetectingChanges` | Spec changed, analyzing required changes |
| `ScalingStatefulSet` | Updating StatefulSet replica count |
| `AddingNodes` | New nodes joining cluster via `CLUSTER MEET` |
| `MigratingSlots` | Hash slot migration for rebalancing |
| `RemovingNodes` | Nodes leaving cluster via `CLUSTER FORGET` |
| `VerifyingCluster` | Health verification after changes |
| `Degraded` | Cluster operational but with reduced capacity |
| `Failed` | Error state requiring intervention |
| `Deleting` | Resource deletion in progress (terminal) |

### State Diagram

```
                                    ┌─────────────────────────────────────────┐
                                    │                                         │
                                    │  DeletionRequested (from any state)     │
                                    │                                         │
                                    ▼                                         │
┌─────────┐  ResourcesApplied   ┌──────────┐  PodsRunning   ┌──────────────┐  │
│ Pending │────────────────────▶│ Creating │───────────────▶│ Initializing │  │
└─────────┘                     └──────────┘                └──────────────┘  │
     │                               │                            │           │
     │ ReconcileError                │ ReconcileError             │ CLUSTER   │
     ▼                               ▼                            │ MEET      │
┌─────────┐◀─────────────────────────────────────────────────────┘           │
│ Failed  │                          │ ResourcesApplied                       │
└─────────┘                          ▼                                        │
     │                         ┌───────────────┐  ClusterHealthy  ┌─────────┐ │
     │ RecoveryInitiated       │ AssigningSlots│─────────────────▶│ Running │◀┘
     │                         └───────────────┘                  └─────────┘
     │                                                                 │
     └─────────────────────────────▶ Pending                          │ SpecChanged
                                                                       │
                                                                       ▼
                                                              ┌─────────────────┐
                                                              │ DetectingChanges│
                                                              └─────────────────┘
                                                                       │
                                    ┌──────────────────────────────────┼─────────────────────┐
                                    │                                  │                     │
                                    │ ChangesDetected (scale-up)       │ NoChangesNeeded     │ SlotsMigrated (scale-down)
                                    ▼                                  ▼                     ▼
                           ┌───────────────────┐              ┌─────────┐            ┌───────────────┐
                           │ ScalingStatefulSet│              │ Running │            │ MigratingSlots│
                           └───────────────────┘              └─────────┘            └───────────────┘
                                    │                                                        │
                                    │ PodsRunning                                            │ SlotsMigrated
                                    ▼                                                        ▼
                           ┌─────────────┐                                          ┌───────────────┐
                           │ AddingNodes │                                          │ RemovingNodes │
                           └─────────────┘                                          └───────────────┘
                                    │                                                        │
                                    │ NodesAdded                                             │ NodesRemoved
                                    ▼                                                        ▼
                           ┌───────────────┐                                        ┌───────────────────┐
                           │ MigratingSlots│                                        │ ScalingStatefulSet│
                           └───────────────┘                                        └───────────────────┘
                                    │                                                        │
                                    │ ClusterHealthy                                         │ StatefulSetScaled
                                    ▼                                                        ▼
                           ┌──────────────────┐  ClusterHealthy                     ┌──────────────────┐
                           │ VerifyingCluster │─────────────────────────────────────│ VerifyingCluster │
                           └──────────────────┘                                     └──────────────────┘
                                    │                                                        │
                                    │ ClusterHealthy                                         │
                                    ▼                                                        ▼
                           ┌─────────┐                                              ┌─────────┐
                           │ Running │                                              │ Running │
                           └─────────┘                                              └─────────┘
```

### Events

| Event | Description |
|-------|-------------|
| `ResourcesApplied` | Kubernetes resources have been applied |
| `PodsRunning` | All desired pods are in Running state |
| `AllReplicasReady` | All pods are ready (passing readiness probes) |
| `ReplicasDegraded` | Some pods are ready but not all |
| `SpecChanged` | Resource spec has changed |
| `ReconcileError` | Error occurred during reconciliation |
| `DeletionRequested` | Deletion timestamp set on resource |
| `RecoveryInitiated` | Recovery from failed state started |
| `FullyRecovered` | Resource recovered from degraded state |
| `ChangesDetected` | Scale or config changes detected |
| `NoChangesNeeded` | No changes required |
| `StatefulSetScaled` | StatefulSet replica count updated |
| `NodesAdded` | New nodes added via CLUSTER MEET |
| `SlotsMigrated` | Hash slots migrated |
| `NodesRemoved` | Nodes removed via CLUSTER FORGET |
| `ClusterHealthy` | Cluster health verification passed |

### Transition Guards

Some transitions have guard conditions:

| Transition | Guard Condition |
|------------|-----------------|
| `* → Running` (AllReplicasReady) | `ready_replicas >= desired_replicas` |
| `* → Degraded` (ReplicasDegraded) | `0 < ready_replicas < desired_replicas` |
| `Degraded → Running` (FullyRecovered) | `ready_replicas >= desired_replicas` |

### Scale-Up Flow

1. `Running` → `DetectingChanges` (SpecChanged)
2. `DetectingChanges` → `ScalingStatefulSet` (ChangesDetected)
3. `ScalingStatefulSet` → `AddingNodes` (PodsRunning)
4. `AddingNodes` → `MigratingSlots` (NodesAdded)
5. `MigratingSlots` → `VerifyingCluster` (ClusterHealthy)
6. `VerifyingCluster` → `Running` (ClusterHealthy)

### Scale-Down Flow

1. `Running` → `DetectingChanges` (SpecChanged)
2. `DetectingChanges` → `MigratingSlots` (SlotsMigrated - evacuate nodes first)
3. `MigratingSlots` → `RemovingNodes` (SlotsMigrated)
4. `RemovingNodes` → `ScalingStatefulSet` (NodesRemoved)
5. `ScalingStatefulSet` → `VerifyingCluster` (StatefulSetScaled)
6. `VerifyingCluster` → `Running` (ClusterHealthy)

---

## ValkeyUpgrade State Machine

The ValkeyUpgrade resource manages rolling upgrades with coordinated failover.

### Upgrade Phases

| Phase | Description |
|-------|-------------|
| `Pending` | Upgrade not yet started |
| `PreChecks` | Running pre-upgrade health checks |
| `InProgress` | Upgrade is actively running |
| `Completed` | Upgrade finished successfully |
| `Failed` | Upgrade failed |
| `RollingBack` | Rolling back the upgrade |
| `RolledBack` | Rollback completed |

### Shard Upgrade States

Each shard (master + replicas) goes through these states:

| State | Description |
|-------|-------------|
| `Pending` | Shard not yet processed |
| `UpgradingReplicas` | Upgrading replica nodes first |
| `WaitingForSync` | Waiting for replication to catch up |
| `FailingOver` | Executing CLUSTER FAILOVER |
| `WaitingForClusterStable` | Verifying cluster stability post-failover |
| `UpgradingOldMaster` | Upgrading the old master (now a replica) |
| `Completed` | Shard upgrade completed |
| `Failed` | Shard upgrade failed |
| `Skipped` | Shard was skipped |

### Per-Shard Upgrade Flow

```
┌─────────┐
│ Pending │
└────┬────┘
     │
     ▼
┌──────────────────┐
│ UpgradingReplicas│──── Delete replica pods one by one
└────────┬─────────┘     New pods come up with new image
         │
         ▼
┌──────────────────┐
│ WaitingForSync   │──── Wait for INFO REPLICATION → master_link_status:up
└────────┬─────────┘     Timeout: replicationSyncTimeoutSeconds (default 300s)
         │
         ▼
┌──────────────────┐
│ FailingOver      │──── Execute CLUSTER FAILOVER on best replica
└────────┬─────────┘     Verify ROLE shows promotion
         │
         ▼
┌────────────────────────┐
│ WaitingForClusterStable│──── Verify cluster_state is "ok"
└────────┬───────────────┘     Old master recognizes it's now a replica
         │                     Replication link is up
         ▼
┌──────────────────┐
│ UpgradingOldMaster│──── Delete old master pod (now replica)
└────────┬─────────┘     New pod comes up with new image
         │
         ▼
┌───────────┐
│ Completed │
└───────────┘
```

### Upgrade Workflow

1. **Pre-checks**: Verify cluster is healthy before starting
2. **For each shard** (sequentially):
   a. Upgrade replicas (simple pod deletion, StatefulSet recreates with new image)
   b. Wait for replication sync
   c. Select best replica (lowest replication lag)
   d. Execute coordinated failover (`CLUSTER FAILOVER`)
   e. Verify cluster stability
   f. Upgrade old master (now a replica)
3. **Post-upgrade**: Verify all nodes running new version

### Failover Coordination

The operator uses `CLUSTER FAILOVER` for safe primary handoff:

1. **Sync Check**: `INFO REPLICATION` → verify `master_link_status:up`
2. **Consistency Check**: `CLUSTER NODES` on all nodes → verify consistent view
3. **Execute Failover**: `CLUSTER FAILOVER` on selected replica
4. **Verify Promotion**: `ROLE` command → confirm new master
5. **Wait for Stability**: cluster_state returns to "ok"

### Timeout Handling

- `replicationSyncTimeoutSeconds`: Time to wait for replication sync (default: 300s)
- If timeout exceeded, shard is marked `Failed`
- Progress is tracked via `syncStartedAt` and `syncElapsedSeconds` status fields

---

## Error Handling

### Transient Errors

Transient errors (network issues, temporary unavailability) trigger:
- Exponential backoff retry
- No phase transition (stays in current phase)
- Logged with retry count

### Validation Errors

Validation errors (invalid spec, missing dependencies) cause:
- Immediate transition to `Failed`
- Status condition with error message
- No automatic retry (requires spec fix)

### Permanent Errors

Permanent errors (irrecoverable state) cause:
- Transition to `Failed`
- Error message in status
- Manual intervention required

### Recovery

To recover from `Failed`:
1. Fix the underlying issue
2. Update the resource spec (triggers reconciliation)
3. Or delete and recreate the resource

The operator will transition: `Failed` → `Pending` → normal flow

---

## Status Conditions

Both resources maintain Kubernetes-standard conditions:

| Condition | Meaning |
|-----------|---------|
| `Ready` | Resource is fully operational |
| `Progressing` | Operation in progress |
| `Degraded` | Operating with reduced capacity |
| `Available` | At least partially available |

Condition structure:
```yaml
conditions:
  - type: Ready
    status: "True"
    reason: ClusterHealthy
    message: "All 6 nodes healthy, 16384/16384 slots assigned"
    lastTransitionTime: "2024-01-15T10:30:00Z"
    observedGeneration: 3
```

---

## Generation Tracking

The operator uses Kubernetes generation tracking to avoid redundant reconciliations:

1. `metadata.generation` increments on spec changes
2. `status.observedGeneration` tracks last processed generation
3. If equal, skip reconciliation (no changes)

This ensures efficient operation and prevents unnecessary work.
