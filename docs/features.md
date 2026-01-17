# Features

This document provides detailed configuration options for all ValkeyCluster features.

## Table of Contents

- [Core Configuration](#core-configuration)
- [TLS Configuration](#tls-configuration)
- [Authentication](#authentication)
- [ACL Support](#acl-support)
- [Persistence](#persistence)
- [Resource Management](#resource-management)
- [Scheduling](#scheduling)
- [Read Service](#read-service)
- [Metrics Exporter](#metrics-exporter)
- [Replication Configuration](#replication-configuration)

---

## Core Configuration

### Cluster Topology

```yaml
spec:
  masters: 3              # Number of master nodes (min 3, max 100)
  replicasPerMaster: 1    # Replicas per master (0-5)
```

**Total pods** = `masters * (1 + replicasPerMaster)`

| Masters | Replicas/Master | Total Pods | Notes |
|---------|-----------------|------------|-------|
| 3 | 0 | 3 | No HA (not recommended) |
| 3 | 1 | 6 | Standard HA setup |
| 6 | 1 | 12 | Larger cluster |
| 6 | 2 | 18 | High availability |

### Container Image

```yaml
spec:
  image:
    repository: "valkey/valkey"    # Default
    tag: "9-alpine"                # Default
    pullPolicy: "IfNotPresent"     # Always, IfNotPresent, Never
    pullSecrets:                   # Optional
      - my-registry-secret
```

---

## TLS Configuration

TLS is **required** for all ValkeyCluster deployments. The operator integrates with cert-manager to automatically provision and rotate certificates.

```yaml
spec:
  tls:
    issuerRef:
      name: my-issuer           # Required: cert-manager issuer name
      kind: ClusterIssuer       # Issuer or ClusterIssuer (default: ClusterIssuer)
      group: cert-manager.io    # Default
    duration: "2160h"           # Certificate validity (default: 90 days)
    renewBefore: "360h"         # Renewal time before expiry (default: 15 days)
```

### Prerequisites

1. Install cert-manager in your cluster
2. Create an Issuer or ClusterIssuer
3. Reference it in your ValkeyCluster spec

### Generated Certificate

The operator creates a Certificate resource with:
- DNS SANs for headless service (cluster discovery)
- DNS SANs for client service
- Wildcard SAN for all pod hostnames
- Server and client auth key usages

### Example ClusterIssuer

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-issuer
spec:
  selfSigned: {}
```

---

## Authentication

Basic password authentication is **required** for all ValkeyCluster deployments.

```yaml
spec:
  auth:
    secretRef:
      name: valkey-auth     # Required: Secret name
      key: password         # Key containing password (default: "password")
```

### Create the Secret

```bash
kubectl create secret generic valkey-auth \
  --from-literal=password='your-secure-password'
```

Or with YAML:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: valkey-auth
type: Opaque
stringData:
  password: "your-secure-password"
```

---

## ACL Support

Valkey 7+ Access Control Lists provide fine-grained user permissions. When enabled, ACL rules are loaded from a Secret.

```yaml
spec:
  auth:
    secretRef:
      name: valkey-auth
    acl:
      enabled: true              # Enable ACL (default: false)
      configSecretRef:
        name: valkey-acl         # Secret containing ACL rules
        key: acl.conf            # Key in secret (default: acl.conf)
      adminUser: "default"       # Admin user for operator access (default: "default")
```

### ACL File Format

Create a Secret with ACL rules:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: valkey-acl
type: Opaque
stringData:
  acl.conf: |
    # Admin user (used by operator for cluster management)
    user default on >your-password ~* &* +@all

    # Read-only user
    user readonly on >readonly-pass ~* &* +@read -@write -@admin

    # Application user with limited access
    user myapp on >app-password ~myapp:* &* +@read +@write -@admin

    # Replication user (for internal cluster communication)
    user replication on >repl-password ~* &* +psync +replconf +ping
```

### ACL Rule Syntax

```
user <username> <status> <password> <key-patterns> <channel-patterns> <commands>
```

| Field | Description | Examples |
|-------|-------------|----------|
| status | `on` (enabled) or `off` (disabled) | `on` |
| password | `>password` or `nopass` | `>secret123` |
| key-patterns | Key access patterns | `~*` (all), `~app:*` |
| channel-patterns | Pub/sub channels | `&*` (all) |
| commands | Command permissions | `+@all`, `-@admin`, `+get` |

### Command Categories

| Category | Commands |
|----------|----------|
| `@read` | GET, MGET, SCAN, etc. |
| `@write` | SET, DEL, LPUSH, etc. |
| `@admin` | CONFIG, DEBUG, SHUTDOWN |
| `@dangerous` | FLUSHALL, KEYS, DEBUG |
| `@all` | All commands |

---

## Persistence

Configure data persistence using PersistentVolumeClaims.

```yaml
spec:
  persistence:
    enabled: true                    # Enable persistence (default: true)
    storageClassName: "ssd"          # StorageClass (optional, uses default if omitted)
    size: "10Gi"                     # PVC size (default: 10Gi)
    aof:
      enabled: true                  # Enable AOF (default: true)
      fsync: "everysec"              # always, everysec, no (default: everysec)
    rdb:
      enabled: true                  # Enable RDB snapshots (default: true)
      saveRules:                     # RDB save rules
        - "900 1"                    # Save if 1 key changed in 900 seconds
        - "300 10"                   # Save if 10 keys changed in 300 seconds
        - "60 10000"                 # Save if 10000 keys changed in 60 seconds
```

### Persistence Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| AOF only | Append-only file, replay on restart | Best durability |
| RDB only | Point-in-time snapshots | Faster restarts |
| AOF + RDB | Both enabled | Recommended |
| None | `enabled: false` | Cache-only (data loss on restart) |

### fsync Policies

| Policy | Description | Trade-off |
|--------|-------------|-----------|
| `always` | fsync after every write | Most durable, slowest |
| `everysec` | fsync once per second | Good balance (default) |
| `no` | Let OS decide when to fsync | Fastest, least durable |

---

## Resource Management

Configure CPU and memory for Valkey pods.

```yaml
spec:
  resources:
    requests:
      cpu: "100m"           # Default
      memory: "256Mi"       # Default
    limits:
      cpu: "1"              # Default
      memory: "1Gi"         # Default
```

### Sizing Guidelines

| Workload | CPU Request | Memory Request | Notes |
|----------|-------------|----------------|-------|
| Development | 50m | 64Mi | Minimal resources |
| Small | 100m | 256Mi | Default |
| Medium | 500m | 1Gi | Production |
| Large | 1 | 4Gi | High throughput |

**Memory Tip**: Set `maxmemory` to ~75% of the memory limit to leave room for:
- Copy-on-write during BGSAVE
- Client buffers
- Replication buffers

---

## Scheduling

Control pod placement in your cluster.

```yaml
spec:
  scheduling:
    spreadMasters: true                    # Spread masters across nodes (default: true)
    topologyKey: "kubernetes.io/hostname"  # Spread topology key (default)
    nodeSelector:
      disktype: ssd
      dedicated: valkey
    tolerations:
      - key: "dedicated"
        operator: "Equal"
        value: "valkey"
        effect: "NoSchedule"
```

### Master Spreading

When `spreadMasters: true`, the operator creates a pod anti-affinity rule that prevents multiple masters from running on the same node (based on `topologyKey`).

### Zone Spreading

To spread across availability zones:

```yaml
spec:
  scheduling:
    spreadMasters: true
    topologyKey: "topology.kubernetes.io/zone"
```

---

## Read Service

Create a dedicated service for distributing read traffic across all pods (masters and replicas).

```yaml
spec:
  readService:
    enabled: true                    # Enable read service (default: false)
    serviceType: "ClusterIP"         # ClusterIP, NodePort, LoadBalancer
    annotations:                     # Optional annotations
      service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
```

### Use Cases

- **Read-heavy workloads**: Distribute reads across replicas
- **Geo-distributed reads**: Use LoadBalancer with cloud provider
- **Caching tier**: Serve stale reads from replicas

### Connection Strings

| Service | Purpose | DNS Name |
|---------|---------|----------|
| Client (`<name>`) | Primary access | `<name>.<namespace>.svc.cluster.local` |
| Read (`<name>-read`) | Read distribution | `<name>-read.<namespace>.svc.cluster.local` |
| Headless (`<name>-headless`) | Cluster discovery | `<name>-headless.<namespace>.svc.cluster.local` |

---

## Metrics Exporter

Deploy a Prometheus-compatible metrics exporter as a sidecar container.

```yaml
spec:
  metricsExporter:
    enabled: true                              # Enable exporter (default: false)
    image: "oliver006/redis_exporter:latest"   # Exporter image
    port: 9121                                 # Metrics port (default: 9121)
    resources:
      cpuRequest: "50m"                        # Default
      memoryRequest: "64Mi"                    # Default
      cpuLimit: "100m"                         # Default
      memoryLimit: "128Mi"                     # Default
    extraEnv:                                  # Additional env vars
      REDIS_EXPORTER_DEBUG: "true"
```

### Exported Metrics

The exporter exposes standard Redis/Valkey metrics:

| Metric | Description |
|--------|-------------|
| `redis_up` | Connection status |
| `redis_connected_clients` | Number of clients |
| `redis_memory_used_bytes` | Memory usage |
| `redis_commands_total` | Command counters |
| `redis_keyspace_keys` | Number of keys |
| `redis_cluster_*` | Cluster-specific metrics |

### Prometheus ServiceMonitor

Create a ServiceMonitor to scrape metrics:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: valkey-metrics
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: my-cluster
  endpoints:
    - port: metrics
      interval: 30s
```

### Grafana Dashboard

The exporter is compatible with standard Redis/Valkey Grafana dashboards. Import dashboard ID `763` from Grafana.com.

---

## Replication Configuration

Fine-tune master-replica replication behavior.

```yaml
spec:
  replication:
    disklessSync: true           # Enable diskless sync (default: false)
    disklessSyncDelay: 5         # Delay before diskless sync (default: 5 seconds)
    minReplicasToWrite: 1        # Min replicas to acknowledge writes (see below)
    minReplicasMaxLag: 10        # Max lag for replica health (default: 10 seconds)
```

### Diskless Synchronization

When enabled, masters stream RDB data directly to replicas without writing to disk first.

| Setting | Pros | Cons |
|---------|------|------|
| `disklessSync: false` | Less memory during sync | Slower, disk I/O |
| `disklessSync: true` | Faster sync, no disk I/O | More memory during sync |

**Use diskless sync when**:
- Disks are slow (HDD, network storage)
- Datasets are large
- Memory is available for streaming

### Replica Health Thresholds (Secure by Default)

The operator enforces durability by default when replicas are configured.

**Default behavior:**
- When `replicasPerMaster > 0`: `minReplicasToWrite` defaults to **1**
- When `replicasPerMaster = 0`: `minReplicasToWrite` defaults to **0**

This ensures that if you configure replicas for high availability, writes are only accepted when at least one replica can acknowledge them, preventing silent data loss.

```yaml
spec:
  replication:
    minReplicasToWrite: 1    # Reject writes if fewer than 1 replica is healthy
    minReplicasMaxLag: 10    # Replica is unhealthy if lag exceeds 10 seconds
```

| Setting | Value | Effect |
|---------|-------|--------|
| `minReplicasToWrite: 0` | Disabled | Writes always accepted (explicit opt-out) |
| `minReplicasToWrite: 1` | 1 replica required | **Default when replicas exist** |
| `minReplicasToWrite: 2` | 2 replicas required | Strong durability |

**To prioritize availability over durability**, explicitly set:

```yaml
spec:
  replication:
    minReplicasToWrite: 0  # Accept writes even with no healthy replicas
```

**Warning**: Setting `minReplicasToWrite` too high can cause write unavailability during replica failures.

---

## Full Example

A production-ready configuration with all features:

```yaml
apiVersion: valkey-operator.smoketurner.com/v1alpha1
kind: ValkeyCluster
metadata:
  name: production
  namespace: valkey
spec:
  # Topology
  masters: 6
  replicasPerMaster: 2

  # Image
  image:
    repository: valkey/valkey
    tag: "9.0.1-alpine"
    pullPolicy: IfNotPresent

  # TLS (required)
  tls:
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
    duration: "2160h"
    renewBefore: "360h"

  # Authentication
  auth:
    secretRef:
      name: valkey-auth
    acl:
      enabled: true
      configSecretRef:
        name: valkey-acl
        key: acl.conf

  # Persistence
  persistence:
    enabled: true
    storageClassName: ssd
    size: 100Gi
    aof:
      enabled: true
      fsync: everysec
    rdb:
      enabled: true

  # Resources
  resources:
    requests:
      cpu: "1"
      memory: "4Gi"
    limits:
      cpu: "2"
      memory: "8Gi"

  # Scheduling
  scheduling:
    spreadMasters: true
    topologyKey: topology.kubernetes.io/zone
    nodeSelector:
      dedicated: valkey
    tolerations:
      - key: dedicated
        operator: Equal
        value: valkey
        effect: NoSchedule

  # Services
  readService:
    enabled: true
    serviceType: ClusterIP

  # Observability
  metricsExporter:
    enabled: true
    port: 9121
    resources:
      cpuRequest: "50m"
      memoryRequest: "64Mi"

  # Replication
  replication:
    disklessSync: true
    disklessSyncDelay: 5
    minReplicasToWrite: 1
    minReplicasMaxLag: 10

  # Labels/Annotations
  labels:
    environment: production
    team: platform
  annotations:
    prometheus.io/scrape: "true"
```
