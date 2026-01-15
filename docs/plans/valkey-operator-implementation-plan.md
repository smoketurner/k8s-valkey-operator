# Valkey Cluster Kubernetes Operator - Implementation Plan

## Overview

Create a production-grade Kubernetes operator to deploy and manage Valkey Clusters, including:
- **ValkeyCluster CRD** - Deploy and manage Valkey cluster topology
- **ValkeyUpgrade CRD** - Handle rolling upgrades with proper failover orchestration
- TLS support via cert-manager integration
- Target: Kubernetes 1.35+, Valkey 9 (`valkey/valkey:9`)

---

## Implementation Progress

| Phase | Task | Status | Files Modified |
|-------|------|--------|----------------|
| 1 | Rename project references | ✅ Complete | `Cargo.toml`, `src/**/*.rs`, `config/**/*.yaml` |
| 2 | Add `fred` dependency | ✅ Complete | `Cargo.toml` |
| 3 | ValkeyCluster CRD | ✅ Complete | `src/crd/valkey_cluster.rs` |
| 4 | Valkey client module (fred wrapper) | ✅ Complete | `src/valkey/*.rs` |
| 5 | State machine for cluster phases | ✅ Complete | `src/controller/state_machine.rs` |
| 6 | Resource generators | ✅ Complete | `src/resources/*.rs` |
| 7 | Basic reconciliation (create/delete) | ✅ Complete | `src/controller/cluster_reconciler.rs` |
| 8 | Cluster initialization logic | ✅ Complete | `src/controller/cluster_init.rs` |
| 9 | Health monitoring | ✅ Complete | `src/controller/cluster_reconciler.rs` |
| 10 | ValkeyUpgrade CRD | ✅ Complete | `src/crd/valkey_upgrade.rs` |
| 11 | Upgrade reconciler | ✅ Complete | `src/controller/upgrade_reconciler.rs`, `src/controller/upgrade_state_machine.rs` |
| 12 | TLS integration | ✅ Complete | `src/resources/certificate.rs`, `src/controller/cluster_reconciler.rs` |
| 13 | Scaling operations | ⏳ Pending | `src/controller/cluster_reconciler.rs` |
| 14 | Update manifests | ⏳ Pending | `config/**/*.yaml`, `charts/` |
| 15 | Integration tests | ⏳ Pending | `tests/integration/` |
| 16 | Property/fuzz tests | ⏳ Pending | `tests/proptest/`, `tests/fuzz/` |

**Current Status:** 12/16 phases complete (75%)

### Phase Details

#### Completed Phases

**Phase 1-2:** Project restructured and dependencies configured with exact version pinning.

**Phase 3:** ValkeyCluster CRD with:
- ValkeyClusterSpec (masters, replicas, TLS, auth, persistence, resources)
- ValkeyClusterStatus (phase, conditions, topology, connection info)
- Validation for minimum 3 masters

**Phase 4:** Valkey client module using `fred` crate:
- `ValkeyClient` wrapper with TLS support
- Cluster operations (CLUSTER MEET, ADDSLOTS, REPLICATE)
- CLUSTER INFO/NODES parsing

**Phase 5:** Cluster state machine:
- Phase transitions: Pending → Creating → Initializing → AssigningSlots → Running
- Degraded and Failed states for error handling

**Phase 6:** Resource generators:
- StatefulSet with pod anti-affinity, probes, security context
- Headless Service with publishNotReadyAddresses
- Client Service for external access
- PodDisruptionBudget for quorum protection

**Phase 7:** Basic reconciliation:
- Resource creation with server-side apply
- Phase-based reconciliation loop
- Event emission for phase transitions

**Phase 8:** Cluster initialization:
- DNS name generation for pods
- CLUSTER MEET execution across all nodes
- Slot assignment to masters (16384 slots distributed evenly)
- Replica setup with CLUSTER REPLICATE

**Phase 9:** Health monitoring:
- ClusterHealthStatus struct
- CLUSTER INFO parsing (cluster_state, slots coverage)
- CLUSTER NODES parsing for node count
- Status updates with health info

**Phase 10:** ValkeyUpgrade CRD:
- ValkeyUpgradeSpec (cluster ref, target version)
- ValkeyUpgradeStatus (phase, conditions, shard progress)
- Upgrade phases: Pending → PreChecks → InProgress → Completed
- Simplified spec with sensible defaults (removed config "footguns")

**Phase 11:** Upgrade reconciler:
- Per-shard rolling upgrades with failover orchestration
- State machine for upgrade phase transitions
- Automatic rollback on any shard failure
- Always wait for replication sync before failover (5 min timeout)

**Phase 12:** TLS integration:
- cert-manager Certificate resource generation
- DNS SANs for headless service wildcard and client service
- Certificate integrated into cluster reconciler via DynamicObject
- TLS secret name exposed in ValkeyCluster status

#### Remaining Phases

**Phase 13:** Scaling operations - slot rebalancing for scale up/down

**Phase 14:** Kubernetes manifests - RBAC, CRD YAML, Helm chart updates

**Phase 15:** Integration tests - end-to-end tests with real cluster

**Phase 16:** Property/fuzz tests - proptest and cargo-fuzz coverage

---

## Specialist Review Feedback (Incorporated)

### Platform Engineer (Primary Stakeholder)
- **Security by default**: TLS and auth REQUIRED, not optional
- **Friendly UX**: Sane defaults, minimal required config
- **Observability**: Include connection endpoint in status, PrometheusRules
- **Day-2 ops**: Need backup/restore story, runbooks for failure scenarios

### Rust Engineer
- **Use `fred` crate**: Native Rust client instead of `valkey-cli` pod exec
- **Extended errors**: Add `Fred`, `ValkeyCommand`, `Timeout` error variants
- **Timeouts**: Connection and command timeouts via fred config
- **Module structure**: Separate `src/valkey/` for client wrapper

### Kubernetes Specialist
- **Critical fix**: Two-phase probe strategy (PING during init, cluster_state:ok after)
- **Anti-affinity**: Required by default for masters across nodes
- **Graceful shutdown**: preStop hook with BGSAVE, 60s termination grace
- **StatefulSet**: Use `updateStrategy: OnDelete` for operator-controlled upgrades
- **Startup probe**: Required for AOF loading (up to 5 minutes)

## Architecture Summary

### Valkey Cluster Fundamentals
- **16384 hash slots** distributed across master nodes (CRC16 mod 16384)
- **Minimum 3 masters** for cluster quorum
- **Dual-port architecture**: client port (6379) + cluster bus port (16379)
- **Gossip protocol** for node discovery and failure detection
- **Asynchronous replication** with automatic failover

### Kubernetes Resources Generated
| Resource | Purpose |
|----------|---------|
| StatefulSet | Stable pod identity for cluster nodes |
| Headless Service | Cluster discovery (publishNotReadyAddresses) |
| Client Service | Client access endpoint |
| Secret | Authentication credentials (if generated) |
| Certificate | TLS certs via cert-manager |
| PodDisruptionBudget | Maintain quorum during disruptions |
| NetworkPolicy | Secure cluster bus traffic |

*Note: No ConfigMap needed - configuration via `VALKEY_EXTRA_FLAGS` env var*

---

## Phase 1: Project Restructuring

### 1.1 Rename References
Replace all `myresource`/`MyResource`/`my-operator` references:

| Old | New |
|-----|-----|
| `myoperator.example.com` | `valkeyoperator.smoketurner.com` |
| `MyResource` | `ValkeyCluster` |
| `my-operator` | `valkey-operator` |
| `myresources` | `valkeyclusters` |

### 1.2 Files to Modify
- `Cargo.toml` - Package name, description
- `src/crd/mod.rs` - CRD definitions
- `src/controller/*.rs` - All controller files
- `src/resources/common.rs` - Resource generators
- `src/lib.rs`, `src/main.rs` - Entry points
- `config/**/*.yaml` - All manifests
- `charts/my-operator/` → `charts/valkey-operator/`
- `Makefile` - Image names, targets
- `CLAUDE.md`, `GUIDE.md` - Documentation

---

## Phase 2: ValkeyCluster CRD

### 2.1 ValkeyClusterSpec

Design philosophy: **Secure-by-default with sane defaults**. Minimal required configuration.

```rust
#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "valkeyoperator.smoketurner.com",
    version = "v1alpha1",
    kind = "ValkeyCluster",
    plural = "valkeyclusters",
    shortname = "vc",
    status = "ValkeyClusterStatus",
    namespaced,
)]
pub struct ValkeyClusterSpec {
    // === Topology (minimal config) ===
    /// Number of master nodes (min 3, default 3)
    #[serde(default = "default_masters")]
    pub masters: i32,

    /// Number of replicas per master (default 1)
    #[serde(default = "default_replicas")]
    pub replicas_per_master: i32,

    // === Image ===
    /// Valkey image (default: valkey/valkey:9)
    #[serde(default)]
    pub image: ImageSpec,

    // === TLS - REQUIRED (secure-by-default) ===
    /// TLS configuration - must specify cert-manager issuer
    pub tls: TlsSpec,

    // === Authentication - REQUIRED (secure-by-default) ===
    /// Authentication - must reference password secret
    pub auth: AuthSpec,

    // === Storage ===
    /// Persistence configuration (default: enabled, 10Gi)
    #[serde(default)]
    pub persistence: PersistenceSpec,

    // === Resources (sensible defaults) ===
    /// Resource requests/limits (default: 256Mi request, 1Gi limit)
    #[serde(default)]
    pub resources: ResourceRequirementsSpec,

    // === Labels (cost allocation) ===
    /// Additional labels for cost tracking
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}
```

**Secure defaults applied automatically:**
- TLS enabled for client, cluster-bus, and replication traffic
- Non-root container (uid 999)
- Read-only root filesystem where possible
- Network policies restricting cluster-bus access
- Pod anti-affinity spreading masters across nodes
- PodDisruptionBudget maintaining quorum

### 2.2 ValkeyClusterStatus
```rust
pub struct ValkeyClusterStatus {
    pub phase: ClusterPhase,             // Pending→Creating→Initializing→Running
    pub ready_nodes: String,             // "6/6"
    pub ready_masters: i32,
    pub ready_replicas: i32,
    pub assigned_slots: String,          // "16384/16384"
    pub observed_generation: Option<i64>,
    pub conditions: Vec<Condition>,
    pub topology: Option<ClusterTopology>,
    pub valkey_version: Option<String>,

    // Connection info (Platform Engineer request)
    pub connection_endpoint: Option<String>,  // "valkey://my-cluster.ns.svc:6379"
    pub connection_secret: Option<String>,    // Secret with connection URL + password
    pub tls_secret: Option<String>,           // Secret with CA cert for clients
}
```

### 2.3 Cluster Phases
```
Pending → Creating → Initializing → Running
                         ↓              ↓
                    (CLUSTER MEET)  (health checks)
                         ↓              ↓
                  AssigningSlots → Updating/Scaling
                                       ↓
                                   Resharding
                                       ↓
                                   Degraded → Failed
```

### 2.4 Conditions
- `ClusterHealthy` - All nodes reachable, cluster consistent
- `SlotsAssigned` - All 16384 slots have owners
- `SlotsStable` - No migration in progress
- `TLSReady` - Certificates valid
- `PersistenceHealthy` - RDB/AOF functioning
- `ReplicasInSync` - All replicas caught up
- `Ready` - Cluster fully operational

---

## Phase 3: ValkeyUpgrade CRD

### 3.1 ValkeyUpgradeSpec
```rust
pub struct ValkeyUpgradeSpec {
    pub cluster_ref: ClusterReference,   // Target cluster
    pub target_version: Option<String>,  // or target_image
    pub target_image: Option<String>,
    pub strategy: UpgradeStrategy,       // RollingUpdate
    pub pre_upgrade_checks: PreUpgradeChecks,
    pub failover: FailoverConfig,        // Timeout, sync verification
    pub rollback: RollbackConfig,        // Auto-rollback on failure
    pub paused: bool,
}
```

### 3.2 Upgrade Workflow (Per Shard)
1. **Upgrade replicas first** (simple restart)
2. **For primary upgrade**:
   - Select best replica (lowest replication lag)
   - Wait for sync: `INFO REPLICATION` → `master_link_status:up`
   - Verify cluster consistency: `CLUSTER NODES` on all nodes
   - Execute: `CLUSTER FAILOVER` on replica
   - Verify: `ROLE` command shows promotion
   - Upgrade old primary (now replica)
3. Move to next shard

### 3.3 Upgrade Phases
```
Pending → PreChecks → InProgress → Completed
                          ↓
                       Paused
                          ↓
                       Failed → RollingBack → RolledBack
```

---

## Phase 4: Resource Generation

### 4.1 StatefulSet
```yaml
spec:
  replicas: masters + (masters * replicasPerMaster)
  serviceName: "{name}-headless"
  podManagementPolicy: Parallel
  updateStrategy:
    type: OnDelete  # Operator-controlled upgrades (K8s Specialist)
  template:
    spec:
      terminationGracePeriodSeconds: 60  # Allow BGSAVE to complete
      affinity:
        podAntiAffinity:  # REQUIRED: Masters on separate nodes
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchLabels:
                app.kubernetes.io/name: {name}
            topologyKey: kubernetes.io/hostname
      securityContext:
        runAsNonRoot: true
        runAsUser: 999
        fsGroup: 999
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: valkey
        image: valkey/valkey:9
        ports:
        - containerPort: 6379   # client
        - containerPort: 16379  # cluster-bus
        command: ["valkey-server", "/etc/valkey/valkey.conf"]
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: false  # Valkey needs /data
          capabilities:
            drop: ["ALL"]
        # Two-phase probe strategy (K8s Specialist fix)
        startupProbe:  # AOF loading can take minutes
          exec:
            command: ["valkey-cli", "ping"]
          failureThreshold: 60
          periodSeconds: 5
        livenessProbe:
          exec:
            command: ["valkey-cli", "ping"]
          periodSeconds: 10
        readinessProbe:  # Only ready when cluster formed
          exec:
            command: ["sh", "-c", "valkey-cli cluster info | grep cluster_state:ok"]
          periodSeconds: 5
        lifecycle:
          preStop:  # Graceful shutdown with BGSAVE
            exec:
              command: ["/bin/sh", "-c", "valkey-cli BGSAVE && sleep 5"]
```

### 4.2 Valkey Configuration via Environment Variable

Using `VALKEY_EXTRA_FLAGS` env var (from [valkey-container entrypoint](https://github.com/valkey-io/valkey-container/blob/mainline/9.0/alpine/docker-entrypoint.sh#L22)) instead of ConfigMap:

```yaml
env:
- name: VALKEY_EXTRA_FLAGS
  value: >-
    --cluster-enabled yes
    --cluster-config-file /data/nodes.conf
    --cluster-node-timeout 5000
    --cluster-require-full-coverage no
    --appendonly yes
    --appendfsync everysec
    --bind 0.0.0.0
    --tls-port 6379
    --port 0
    --tls-cluster yes
    --tls-replication yes
    --tls-cert-file /etc/valkey/certs/tls.crt
    --tls-key-file /etc/valkey/certs/tls.key
    --tls-ca-cert-file /etc/valkey/certs/ca.crt
```

**Benefits over ConfigMap:**
- No separate ConfigMap resource to manage
- Easier to construct dynamically based on CRD spec
- Changes trigger pod restart automatically (env change = new pod)
- Fewer files to mount

### 4.3 Headless Service
```yaml
spec:
  clusterIP: None
  publishNotReadyAddresses: true  # Critical for cluster formation
  ports:
  - port: 6379
    name: client
  - port: 16379
    name: cluster-bus
```

---

## Phase 5: Reconciliation Logic

### 5.1 Cluster Initialization
1. Apply K8s resources (StatefulSet, Services, ConfigMap)
2. Wait for all pods ready
3. Execute `CLUSTER MEET` from pod-0 to all others
4. Assign slots: `CLUSTER ADDSLOTS 0-5460` (distributed evenly)
5. Setup replicas: `CLUSTER REPLICATE <master-node-id>`
6. Verify: `CLUSTER INFO` shows `cluster_state:ok`

### 5.2 Valkey Commands (via `fred` crate - native Rust)

Using the `fred` crate with `i-cluster` and `enable-rustls` features instead of pod exec:

```rust
use fred::prelude::*;
use fred::interfaces::ClusterInterface;

// Create client connecting to cluster
let config = RedisConfig {
    server: ServerConfig::Clustered {
        hosts: vec![Server::new("my-cluster-0.my-cluster-headless", 6379)],
    },
    tls: Some(TlsConfig::default()),  // Uses rustls
    ..Default::default()
};
let client = Builder::from_config(config).build()?;
client.init().await?;

// CLUSTER MEET - connect nodes
client.cluster_meet("10.0.0.2", 6379).await?;

// CLUSTER ADDSLOTS - assign hash slots (supports ranges)
client.cluster_add_slots(vec![0, 1, 2, /* ... */ 5460]).await?;
// Or use ADDSLOTSRANGE for efficiency
client.cluster_add_slots_range(vec![(0, 5460)]).await?;

// CLUSTER REPLICATE - setup replication
client.cluster_replicate("node-id-of-master").await?;

// CLUSTER INFO - health check (returns HashMap)
let info: HashMap<String, String> = client.cluster_info().await?;
let state = info.get("cluster_state");  // "ok" or "fail"

// CLUSTER NODES - topology (returns String, parse to struct)
let nodes: String = client.cluster_nodes().await?;

// CLUSTER FAILOVER - manual failover for upgrades
client.cluster_failover(ClusterFailoverFlag::Default).await?;
```

**Benefits over pod exec:**
- Type-safe return values (no string parsing)
- Proper error types from the library
- TLS via rustls built-in
- Connection pooling and automatic reconnection
- Easier integration testing (connect directly, no pods needed)

### 5.3 Scaling
**Scale Up:**
1. Update StatefulSet replicas
2. Wait for new pods ready
3. `CLUSTER MEET` new nodes
4. Trigger slot rebalancing (migrate slots to new masters)

**Scale Down:**
1. Migrate slots away from nodes being removed
2. `CLUSTER FORGET` nodes
3. Scale down StatefulSet

---

## Phase 6: TLS Integration (cert-manager)

### 6.1 Certificate Resource
```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: {cluster-name}-tls
spec:
  secretName: {cluster-name}-tls
  issuerRef:
    name: {issuer-name}
    kind: ClusterIssuer
  dnsNames:
  - "*.{cluster-name}-headless.{namespace}.svc.cluster.local"
  - "{cluster-name}.{namespace}.svc.cluster.local"
```

### 6.2 Valkey TLS Config
```
tls-port 6379
port 0
tls-cert-file /etc/valkey/certs/tls.crt
tls-key-file /etc/valkey/certs/tls.key
tls-ca-cert-file /etc/valkey/certs/ca.crt
tls-cluster yes
tls-replication yes
tls-auth-clients optional  # or "yes" for mTLS
```

---

## Phase 7: Testing & Validation

### 7.1 Unit Tests
- CRD validation (masters >= 3, valid ports)
- State machine transitions
- Slot distribution algorithm
- Error classification

### 7.2 Integration Tests
- Cluster creation (3 masters, 1 replica each)
- Slot assignment verification
- Scale up/down
- Node failure and recovery
- TLS connectivity
- Upgrade workflow

### 7.3 Verification Commands
```bash
# Check cluster health
kubectl exec {pod} -- valkey-cli cluster info

# Check slot coverage
kubectl exec {pod} -- valkey-cli cluster slots

# Check node topology
kubectl exec {pod} -- valkey-cli cluster nodes

# Test client connectivity
kubectl exec {pod} -- valkey-cli -c set foo bar
kubectl exec {pod} -- valkey-cli -c get foo
```

---

## Code Organization (Rust Engineer Recommendation)

```
src/
  crd/
    mod.rs
    valkey_cluster.rs      # ValkeyCluster CRD
    valkey_upgrade.rs      # ValkeyUpgrade CRD
  controller/
    mod.rs
    cluster/               # Cluster reconciler
      mod.rs
      reconciler.rs
      state_machine.rs
    upgrade/               # Upgrade reconciler
      mod.rs
      reconciler.rs
      state_machine.rs
    shared/                # Common utilities
      context.rs
      error.rs
      status.rs
  valkey/                  # Valkey client module (uses fred crate)
    mod.rs
    client.rs              # Fred client wrapper with connection pooling
    cluster_ops.rs         # High-level cluster operations (meet, assign slots)
    types.rs               # Parsed CLUSTER NODES output types
  resources/
    mod.rs
    statefulset.rs       # Includes VALKEY_EXTRA_FLAGS env construction
    services.rs
    certificate.rs
    pdb.rs
    networkpolicy.rs
    secret.rs            # Auth credential generation (if needed)
```

## Implementation Order

| Phase | Task | Files |
|-------|------|-------|
| 1 | Rename project references | All files |
| 2 | Add `fred` dependency | `Cargo.toml` (i-cluster, enable-rustls) |
| 3 | ValkeyCluster CRD | `src/crd/valkey_cluster.rs` |
| 4 | Valkey client module (fred wrapper) | `src/valkey/*.rs` |
| 5 | State machine for cluster phases | `src/controller/cluster/state_machine.rs` |
| 6 | Resource generators | `src/resources/*.rs` |
| 7 | Basic reconciliation (create/delete) | `src/controller/cluster/reconciler.rs` |
| 8 | Cluster initialization logic | `src/valkey/cluster_ops.rs` |
| 9 | Health monitoring | `src/controller/cluster/reconciler.rs` |
| 10 | ValkeyUpgrade CRD | `src/crd/valkey_upgrade.rs` |
| 11 | Upgrade reconciler | `src/controller/upgrade/reconciler.rs` |
| 12 | TLS integration | `src/resources/certificate.rs` |
| 13 | Scaling operations | `src/controller/cluster/reconciler.rs` |
| 14 | Update manifests | `config/**/*.yaml`, `charts/` |
| 15 | Integration tests | `tests/integration/` (direct fred connection) |
| 16 | Property/fuzz tests | `tests/proptest/`, `tests/fuzz/` |

---

## Dependencies (Exact Version Pinning Required)

Per GUIDE.md, all dependencies must use exact version pinning (`=X.Y.Z`) for reproducible builds:

```toml
# Cargo.toml - exact version pinning
[dependencies]
# Core Kubernetes
kube = { version = "=2.0.1", default-features = false, features = [
    "client", "runtime", "derive", "rustls-tls"
] }
k8s-openapi = { version = "=0.26.1", features = ["v1_35"] }
kube-leader-election = "=0.42.0"

# Async runtime
tokio = { version = "=1.49.0", features = ["full"] }

# Valkey client (native Rust - no pod exec)
fred = { version = "=9.4.0", default-features = false, features = [
    "i-cluster",           # CLUSTER commands
    "enable-rustls",       # TLS support
    "replicas",            # Replica routing
] }

# Serialization
serde = { version = "=1.0.219", features = ["derive"] }
serde_json = "=1.0.140"
schemars = "=0.8.22"

# Error handling
thiserror = "=2.0.12"
anyhow = "=1.0.98"

# Logging & metrics
tracing = "=0.1.41"
tracing-subscriber = { version = "=0.3.19", features = ["env-filter", "json"] }
prometheus-client = "=0.23.1"

# HTTP server (health/webhooks)
axum = { version = "=0.8.4", features = ["tokio"] }

# TLS
rustls = "=0.23.27"

# Time
jiff = "=0.2.14"
```

---

## RBAC Requirements

```yaml
# Additional permissions needed (no pods/exec - using fred client)
- apiGroups: [""]
  resources: [persistentvolumeclaims]
  verbs: [get, list, watch, create, delete]
- apiGroups: [policy]
  resources: [poddisruptionbudgets]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [networking.k8s.io]
  resources: [networkpolicies]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [cert-manager.io]
  resources: [certificates]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [valkeyoperator.smoketurner.com]
  resources: [valkeyclusters, valkeyupgrades]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [valkeyoperator.smoketurner.com]
  resources: [valkeyclusters/status, valkeyupgrades/status]
  verbs: [get, update, patch]
```

---

## Extended Error Types (Rust Engineer)

```rust
#[derive(Error, Debug)]
pub enum Error {
    #[error("Kubernetes API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Fred/Valkey client error: {0}")]
    Fred(#[from] fred::error::RedisError),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout after {duration:?}: {operation}")]
    Timeout { operation: String, duration: Duration },

    #[error("Cluster inconsistent: {0}")]
    ClusterInconsistent(String),

    #[error("Migration in progress: {0}")]
    MigrationInProgress(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Transient error: {0}")]
    Transient(String),

    #[error("Permanent error: {0}")]
    Permanent(String),
}
```

Fred client configuration with timeouts:
```rust
let config = RedisConfig {
    server: ServerConfig::Clustered { hosts },
    tls: Some(TlsConfig::default()),
    ..Default::default()
};
let perf = PerformanceConfig {
    default_command_timeout: Duration::from_secs(30),
    ..Default::default()
};
let conn = ConnectionConfig {
    connection_timeout: Duration::from_secs(10),
    ..Default::default()
};
let client = Builder::from_config(config)
    .with_performance_config(perf)
    .with_connection_config(conn)
    .build()?;
```

---

## Patterns from k8s-postgres-operator

The following patterns are adopted from the reference implementation:

### Connection Management (like tokio-postgres)
```rust
// Fred client with Kubernetes service DNS
pub struct ValkeyConnection {
    client: fred::clients::RedisClient,
    config: RedisConfig,
}

impl ValkeyConnection {
    pub async fn connect(cluster: &ValkeyCluster) -> Result<Self, Error> {
        let hosts = Self::build_hosts(cluster);  // DNS-based discovery
        let config = RedisConfig {
            server: ServerConfig::Clustered { hosts },
            tls: Self::build_tls_config(cluster),
            ..Default::default()
        };
        let client = Builder::from_config(config).build()?;
        client.init().await?;
        Ok(Self { client, config })
    }
}
```

### Status Management (ConditionBuilder pattern)
```rust
// Condition updates only change transition_time when status changes
pub struct ConditionBuilder {
    r#type: String,
    status: String,
    reason: String,
    message: String,
}

impl ConditionBuilder {
    pub fn build(self, existing: Option<&Condition>) -> Condition {
        let transition_time = if existing.map(|c| &c.status) == Some(&self.status) {
            existing.unwrap().last_transition_time.clone()
        } else {
            jiff::Timestamp::now().to_string()
        };
        Condition { /* ... */ transition_time }
    }
}
```

### Optimized Reconciliation
```rust
// Skip full reconciliation when only status changed
async fn reconcile(obj: Arc<ValkeyCluster>, ctx: Arc<Context>) -> Result<Action, Error> {
    let observed_gen = obj.status.as_ref().and_then(|s| s.observed_generation);
    let current_gen = obj.metadata.generation;

    // Status-only update, skip heavy reconciliation
    if observed_gen == current_gen && !needs_health_check(&obj) {
        return Ok(Action::requeue(Duration::from_secs(60)));
    }

    // Full reconciliation path
    // ...
}
```

---

## Key Design Decisions

1. **StatefulSet over Deployment** - Stable network identity required for cluster node IDs
2. **Headless Service with publishNotReadyAddresses** - Enables cluster formation before all pods ready
3. **Parallel pod management** - Faster cluster startup
4. **`fred` crate for Valkey commands** - Native Rust client with TLS, no pod exec needed
5. **cert-manager integration** - Automatic certificate rotation
6. **Separate ValkeyUpgrade CRD** - Clean separation of concerns, explicit upgrade tracking
7. **Per-shard upgrade orchestration** - Minimize data loss risk with proper failover
8. **Secure-by-default** - TLS and auth required, network policies, non-root containers
9. **Sane defaults** - Minimal required config, sensible resource/storage defaults
10. **Two-phase probe strategy** - PING during init, cluster_state:ok after formation
11. **Type-safe cluster operations** - Fred returns typed values, minimal string parsing
12. **No escape hatches** - No pod_template or raw config overrides (prevent misuse)
13. **Exact version pinning** - All deps use `=X.Y.Z` for reproducible builds
14. **Server-side apply** - Use `Patch::Apply` not replace for resource updates
15. **Generation tracking** - Skip reconciliation when only status changed
16. **Exponential backoff with jitter** - Prevent thundering herd on retries
