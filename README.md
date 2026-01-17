# Valkey Operator for Kubernetes

A production-ready Kubernetes operator for managing [Valkey](https://valkey.io) clusters, built in Rust using [kube-rs](https://kube.rs).

## Features

- **High Availability**: Automatic cluster formation with configurable masters and replicas
- **Secure by Default**: Required TLS (via cert-manager) and authentication
- **Horizontal Scaling**: Add/remove masters with automatic slot migration
- **Rolling Upgrades**: Zero-downtime upgrades via ValkeyUpgrade CRD with coordinated failover
- **Observability**: Optional Prometheus metrics exporter sidecar
- **ACL Support**: Fine-grained access control with Valkey 7+ ACL system
- **Panic-Free**: All production code paths handle errors gracefully

## Quick Start

### Prerequisites

- Kubernetes 1.35+
- Rust 1.92+ (for building from source)
- [cert-manager](https://cert-manager.io) installed in the cluster
- kubectl configured

### Installation

```bash
# Install CRDs
kubectl apply -f config/crd/

# Install RBAC
kubectl apply -f config/rbac/

# Deploy the operator
kubectl apply -f config/deploy/
```

Or using Helm:

```bash
helm install valkey-operator ./charts/valkey-operator
```

### Deploy a Valkey Cluster

1. Create a password secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: valkey-auth
type: Opaque
stringData:
  password: "your-secure-password"
```

2. Create a ValkeyCluster:

```yaml
apiVersion: valkey-operator.smoketurner.com/v1alpha1
kind: ValkeyCluster
metadata:
  name: my-cluster
spec:
  masters: 3
  replicasPerMaster: 1
  tls:
    issuerRef:
      name: my-cluster-issuer
      kind: ClusterIssuer
  auth:
    secretRef:
      name: valkey-auth
```

3. Watch the cluster come up:

```bash
kubectl get vc -w
```

## Configuration

### ValkeyCluster Spec

| Field | Description | Default |
|-------|-------------|---------|
| `masters` | Number of master nodes (min 3) | 3 |
| `replicasPerMaster` | Replicas per master | 1 |
| `tls.issuerRef` | cert-manager issuer reference | Required |
| `auth.secretRef` | Secret containing password | Required |
| `auth.acl` | ACL configuration for fine-grained access | Disabled |
| `persistence.enabled` | Enable persistent storage | true |
| `persistence.size` | PVC size | 10Gi |
| `readService.enabled` | Enable read-only service | false |
| `metricsExporter.enabled` | Enable Prometheus exporter sidecar | false |
| `replication.disklessSync` | Enable diskless replication | false |
| `replication.minReplicasToWrite` | Minimum replicas for write acknowledgment | 1 (when replicas exist) |

See [docs/features.md](docs/features.md) for detailed configuration options.

### Example with All Features

```yaml
apiVersion: valkey-operator.smoketurner.com/v1alpha1
kind: ValkeyCluster
metadata:
  name: production-cluster
spec:
  masters: 6
  replicasPerMaster: 2
  tls:
    issuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
  auth:
    secretRef:
      name: valkey-auth
    acl:
      enabled: true
      configSecretRef:
        name: valkey-acl
        key: acl.conf
  persistence:
    enabled: true
    size: 50Gi
    storageClassName: ssd
  resources:
    requests:
      cpu: "1"
      memory: "4Gi"
    limits:
      cpu: "2"
      memory: "8Gi"
  readService:
    enabled: true
  metricsExporter:
    enabled: true
    port: 9121
  replication:
    disklessSync: true
    minReplicasToWrite: 1
    minReplicasMaxLag: 10
```

## Rolling Upgrades

Create a ValkeyUpgrade resource to perform zero-downtime upgrades:

```yaml
apiVersion: valkey-operator.smoketurner.com/v1alpha1
kind: ValkeyUpgrade
metadata:
  name: upgrade-to-9.1
spec:
  clusterRef:
    name: my-cluster
  targetVersion: "9.1.0"
```

The operator performs per-shard rolling upgrades:
1. Upgrade replicas first
2. Wait for replication sync
3. Execute coordinated failover
4. Upgrade old master (now replica)
5. Move to next shard

See [docs/state-machines.md](docs/state-machines.md) for detailed upgrade workflow.

## Architecture

### Custom Resources

| CRD | Purpose |
|-----|---------|
| `ValkeyCluster` | Deploy and manage Valkey cluster topology |
| `ValkeyUpgrade` | Handle rolling upgrades with failover orchestration |

### Generated Resources

For each ValkeyCluster, the operator creates:
- **StatefulSet**: Valkey pods with stable identity
- **Headless Service**: Cluster discovery
- **Client Service**: Client access endpoint
- **Read Service** (optional): Read-only traffic distribution
- **PodDisruptionBudget**: Maintain quorum
- **Certificate**: TLS certs via cert-manager

## Development

### Build

```bash
make build              # Build release binary
make docker-build       # Build Docker image
make docker-push        # Push Docker image
```

### Test

```bash
make test               # Run unit tests
make test-integration   # Run integration tests
make lint               # Run clippy lints
```

### Run Locally

```bash
make install            # Install CRD and RBAC
make run                # Run operator locally
```

## Documentation

- [State Machines](docs/state-machines.md) - Lifecycle phases and transitions
- [Features](docs/features.md) - Detailed feature documentation
- [CLAUDE.md](CLAUDE.md) - AI assistant instructions

## Make Targets

| Target | Description |
|--------|-------------|
| `make build` | Build release binary |
| `make run` | Run operator locally |
| `make test` | Run unit tests |
| `make test-integration` | Run integration tests |
| `make lint` | Run clippy lints |
| `make fmt` | Format code |
| `make install` | Install CRD and RBAC |
| `make deploy` | Deploy to cluster |
| `make docker-build` | Build Docker image |

## Design Principles

### Panic-Free Code
All production code paths handle errors gracefully. Clippy lints deny `unwrap()`, `expect()`, and `panic!()`.

### Idempotent Reconciliation
Every reconcile operation can be safely retried. Uses server-side apply for atomic updates.

### Secure by Default
TLS and authentication are required, not optional. The operator enforces security best practices.

### State Machine Driven
Resource lifecycle is managed through formal FSMs with defined phase transitions. See [docs/state-machines.md](docs/state-machines.md).

## License

MIT
