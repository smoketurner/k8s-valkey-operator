# State Machines

This document describes the formal finite state machines (FSMs) that govern the lifecycle of ValkeyCluster and ValkeyUpgrade resources.

## ValkeyCluster State Machine

The ValkeyCluster lifecycle is managed through a formal FSM with explicit state transitions, guards, and actions.

### Phases

| Phase | Description |
|-------|-------------|
| `Pending` | Initial state, waiting for reconciliation |
| `Creating` | Kubernetes resources (StatefulSet, Services, etc.) are being created |
| `WaitingForPods` | Waiting for StatefulSet pods to become Running |
| `InitializingCluster` | Pods are running, executing `CLUSTER MEET` to form cluster |
| `AssigningSlots` | Initial hash slot assignment via `CLUSTER ADDSLOTS` |
| `ConfiguringReplicas` | Setting up replica nodes via `CLUSTER REPLICATE` |
| `Running` | Cluster is healthy and operational |
| `ScalingUpStatefulSet` | Increasing StatefulSet replica count for scale-up |
| `WaitingForNewPods` | Waiting for new pods to become Running after scale-up |
| `AddingNodesToCluster` | New nodes joining cluster via `CLUSTER MEET` |
| `RebalancingSlots` | Hash slot migration to new masters after scale-up |
| `ConfiguringNewReplicas` | Setting up replicas for new masters after scale-up |
| `EvacuatingSlots` | Migrating slots away from nodes before scale-down |
| `RemovingNodesFromCluster` | Nodes leaving cluster via `CLUSTER FORGET` |
| `ScalingDownStatefulSet` | Decreasing StatefulSet replica count for scale-down |
| `VerifyingClusterHealth` | Health verification after changes |
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
┌─────────┐  ResourcesApplied   ┌──────────┐  StatefulSetReady ┌─────────────────┐
│ Pending │────────────────────▶│ Creating │──────────────────▶│ WaitingForPods  │
└─────────┘                     └──────────┘                   └─────────────────┘
     │                               │                                │
     │ ReconcileError                │ ReconcileError                 │ PodsRunning
     ▼                               ▼                                ▼
┌─────────┐◀──────────────────────────────────────────────┐  ┌────────────────────┐
│ Failed  │                                                  │ InitializingCluster│
└─────────┘                                                  └────────────────────┘
     │                                                                │
     │ RecoveryInitiated                                              │ ClusterMeetComplete
     │                                                                ▼
     └───────────────────────────▶ Pending                   ┌───────────────┐
                                                             │ AssigningSlots│
                                                             └───────────────┘
                                                                      │
                                                                      │ SlotsAssigned
                                                                      ▼
                                                           ┌─────────────────────┐
                                                           │ ConfiguringReplicas │
                                                           └─────────────────────┘
                                                                      │
                                                                      │ ReplicasConfigured
                                                                      ▼
                                                           ┌─────────┐
                                                           │ Running │◀──────────────────────┐
                                                           └─────────┘                       │
                                                                │                            │
                                                                │ SpecChanged                │
                                                                ▼                            │
                                      ┌────────────────────────────────────────────┐         │
                                      │                                            │         │
                                      │ ScaleUp                         ScaleDown  │         │
                                      ▼                                            ▼         │
                           ┌─────────────────────┐                    ┌────────────────┐     │
                           │ ScalingUpStatefulSet│                    │ EvacuatingSlots│     │
                           └─────────────────────┘                    └────────────────┘     │
                                      │                                            │         │
                                      │ StatefulSetScaled                          │ SlotsMigrated
                                      ▼                                            ▼         │
                           ┌──────────────────┐                    ┌─────────────────────────┐
                           │ WaitingForNewPods│                    │ RemovingNodesFromCluster│
                           └──────────────────┘                    └─────────────────────────┘
                                      │                                            │
                                      │ PodsRunning                                │ NodesRemoved
                                      ▼                                            ▼
                           ┌──────────────────────┐                ┌───────────────────────┐
                           │ AddingNodesToCluster │                │ ScalingDownStatefulSet│
                           └──────────────────────┘                └───────────────────────┘
                                      │                                            │
                                      │ NodesAdded                                 │ StatefulSetScaled
                                      ▼                                            ▼
                           ┌─────────────────┐                        ┌──────────────────────┐
                           │ RebalancingSlots│                        │ VerifyingClusterHealth│
                           └─────────────────┘                        └──────────────────────┘
                                      │                                            │
                                      │ SlotsRebalanced                            │ ClusterHealthy
                                      ▼                                            │
                           ┌───────────────────────┐                               │
                           │ ConfiguringNewReplicas│                               │
                           └───────────────────────┘                               │
                                      │                                            │
                                      │ ReplicasConfigured                         │
                                      ▼                                            │
                           ┌──────────────────────┐                                │
                           │ VerifyingClusterHealth│───────────────────────────────┘
                           └──────────────────────┘
```

**Note:** The diagram above shows the primary paths. Additionally:
- **Replica-only scale-down** has a direct path: `Running` → `RemovingNodesFromCluster` (via `ReplicaScaleDownDetected`), skipping `EvacuatingSlots` since replicas don't hold slots.
- **Deletion** can occur from any phase (except `Deleting`) via `DeletionRequested`.

### Events

| Event | Description |
|-------|-------------|
| `ResourcesApplied` | Kubernetes resources have been applied |
| `StatefulSetReady` | StatefulSet created successfully |
| `PodsRunning` | All desired pods are in Running state |
| `ClusterMeetComplete` | All nodes connected via CLUSTER MEET |
| `SlotsAssigned` | Hash slots assigned to masters |
| `ReplicasConfigured` | Replicas connected to masters via CLUSTER REPLICATE |
| `SpecChanged` | Resource spec has changed |
| `ScaleUpDetected` | Scale-up detected (masters increased) |
| `ScaleDownDetected` | Scale-down detected (masters decreased) |
| `ReplicaScaleDownDetected` | Replica-only scale-down detected (masters unchanged, replicas decreased) |
| `StatefulSetScaled` | StatefulSet replica count updated |
| `NodesAdded` | New nodes added via CLUSTER MEET |
| `SlotsRebalanced` | Hash slots migrated to new masters |
| `SlotsMigrated` | Slots evacuated from nodes being removed |
| `NodesRemoved` | Nodes removed via CLUSTER FORGET |
| `ClusterHealthy` | Cluster health verification passed |
| `ReplicasDegraded` | Cluster degraded (0 < ready < desired) |
| `ReconcileError` | Error occurred during reconciliation |
| `DeletionRequested` | Deletion timestamp set on resource |
| `RecoveryInitiated` | Recovery from failed state started |

### Transition Guards

Some transitions have guard conditions:

| Transition | Guard Condition |
|------------|-----------------|
| `WaitingForPods → InitializingCluster` | All pods Running |
| `* → Running` | Cluster healthy, all slots assigned |
| `* → Degraded` | `0 < ready_replicas < desired_replicas` |
| `Degraded → Running` | `ready_replicas >= desired_replicas` |

### Scale-Up Flow (Masters)

1. `Running` → `ScalingUpStatefulSet` (ScaleUpDetected with increased masters)
2. `ScalingUpStatefulSet` → `WaitingForNewPods` (StatefulSetScaled)
3. `WaitingForNewPods` → `AddingNodesToCluster` (PodsRunning)
4. `AddingNodesToCluster` → `RebalancingSlots` (NodesAdded)
5. `RebalancingSlots` → `ConfiguringNewReplicas` (SlotsRebalanced)
6. `ConfiguringNewReplicas` → `VerifyingClusterHealth` (ReplicasConfigured)
7. `VerifyingClusterHealth` → `Running` (ClusterHealthy)

### Replica-Only Scale-Up Flow

When only replicas are being added (masters unchanged), the flow reuses the scale-up path but slot rebalancing is a no-op:

1. `Running` → `ScalingUpStatefulSet` (ScaleUpDetected with increased replicas)
2. `ScalingUpStatefulSet` → `WaitingForNewPods` (StatefulSetScaled)
3. `WaitingForNewPods` → `AddingNodesToCluster` (PodsRunning)
4. `AddingNodesToCluster` → `RebalancingSlots` (NodesAdded)
5. `RebalancingSlots` → `ConfiguringNewReplicas` (SlotsRebalanced - no-op, no new masters)
6. `ConfiguringNewReplicas` → `VerifyingClusterHealth` (ReplicasConfigured)
7. `VerifyingClusterHealth` → `Running` (ClusterHealthy)

### Scale-Down Flow (Masters)

1. `Running` → `EvacuatingSlots` (ScaleDownDetected with decreased masters)
2. `EvacuatingSlots` → `RemovingNodesFromCluster` (SlotsMigrated)
3. `RemovingNodesFromCluster` → `ScalingDownStatefulSet` (NodesRemoved)
4. `ScalingDownStatefulSet` → `VerifyingClusterHealth` (StatefulSetScaled)
5. `VerifyingClusterHealth` → `Running` (ClusterHealthy)

### Replica-Only Scale-Down Flow

When only replicas are being removed (masters unchanged), the flow skips slot evacuation since replicas don't hold slots:

1. `Running` → `RemovingNodesFromCluster` (ReplicaScaleDownDetected)
2. `RemovingNodesFromCluster` → `ScalingDownStatefulSet` (NodesRemoved)
3. `ScalingDownStatefulSet` → `VerifyingClusterHealth` (StatefulSetScaled)
4. `VerifyingClusterHealth` → `Running` (ClusterHealthy)

```
┌─────────┐  ReplicaScaleDownDetected  ┌───────────────────────────┐
│ Running │ ──────────────────────────►│ RemovingNodesFromCluster  │
└─────────┘                            └───────────────────────────┘
     ▲                                             │
     │                                      NodesRemoved
     │                                             ▼
     │  ClusterHealthy  ┌───────────────────────┐  StatefulSetScaled  ┌─────────────────────────┐
     └──────────────────│VerifyingClusterHealth │◄────────────────────│ ScalingDownStatefulSet  │
                        └───────────────────────┘                     └─────────────────────────┘
```

### Initial Creation Flow

1. `Pending` → `Creating` (Reconciliation started)
2. `Creating` → `WaitingForPods` (StatefulSetReady)
3. `WaitingForPods` → `InitializingCluster` (PodsRunning)
4. `InitializingCluster` → `AssigningSlots` (ClusterMeetComplete)
5. `AssigningSlots` → `ConfiguringReplicas` (SlotsAssigned)
6. `ConfiguringReplicas` → `VerifyingClusterHealth` (ReplicasConfigured)
7. `VerifyingClusterHealth` → `Running` (ClusterHealthy)

---

## ClusterTopology

The operator uses a centralized `ClusterTopology` struct to correlate Kubernetes pod information with Valkey cluster node state. This provides:

- **IP → ordinal mapping**: Look up pod ordinal by IP address
- **ordinal → node_id mapping**: Find Valkey node ID by pod ordinal
- **Node role identification**: Determine if a node is master or replica
- **Orphan detection**: Find cluster nodes with no matching pod
- **Scale candidate identification**: Identify nodes for promotion or removal

The topology is rebuilt on each reconciliation by querying:
1. Kubernetes API for current pod state
2. Valkey CLUSTER NODES for cluster membership

This eliminates scattered mapping logic and fixes bugs where IP address parsing failed (e.g., extracting ordinal "10" from IP "10.0.0.5").

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

The operator uses Kubernetes generation tracking to distinguish spec changes from transient failures:

1. `metadata.generation` increments on spec changes
2. `status.observedGeneration` tracks last processed generation
3. `spec_changed = (metadata.generation != status.observedGeneration)`

This is critical for scale detection:
- **Scale-down** is NEVER gated on `spec_changed` (data-loss invariant)
- **Scale-up** requires `spec_changed` to be true (prevents stuck RebalancingSlots when a master pod fails)
