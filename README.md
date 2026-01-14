# Kubernetes Operator Template (Rust)

A production-ready template for building Kubernetes operators in Rust using [kube-rs](https://kube.rs).

## Features

- **Working Skeleton**: Compiles and runs immediately with example `MyResource` CRD
- **Best Practices**: Panic-free code, server-side apply, finalizers, generation tracking
- **Full Stack**: CRD, controller, webhooks, health server, metrics
- **Testing**: Unit, integration, and property-based test scaffolding
- **CI/CD**: GitHub Actions for lint, test, build, and release
- **Deployment**: Kubernetes manifests and Helm chart included

## Quick Start

### Prerequisites

- Rust 1.92+ (install via [rustup](https://rustup.rs))
- Kubernetes cluster (local: [kind](https://kind.sigs.k8s.io), [minikube](https://minikube.sigs.k8s.io))
- kubectl configured

### Build and Run

```bash
# Clone the template
git clone https://github.com/YOUR_ORG/k8s-operator-template my-operator
cd my-operator

# Build the operator
make build

# Install CRD and RBAC
make install

# Run the operator locally
make run
```

### Deploy a Sample Resource

```bash
# In a separate terminal
make deploy-sample

# Watch the resource
kubectl get myresources -w
```

## Customization

### 1. Rename the Project

Update `Cargo.toml`:
```toml
[package]
name = "your-operator"
```

### 2. Define Your CRD

Edit `src/crd/mod.rs`:
```rust
#[derive(CustomResource, ...)]
#[kube(
    group = "yourcompany.com",
    version = "v1alpha1",
    kind = "YourResource",
    ...
)]
pub struct YourResourceSpec {
    // Your fields here
}
```

### 3. Implement Reconciliation

Customize `src/controller/reconciler.rs` with your business logic:
```rust
async fn reconcile(
    resource: Arc<YourResource>,
    ctx: Arc<Context>,
) -> Result<Action, Error> {
    // Your reconciliation logic
}
```

### 4. Update Kubernetes Manifests

- `config/deploy/` - Deployment manifests
- `config/rbac/` - RBAC permissions for your CRD
- `charts/my-operator/` - Helm chart

### 5. Update CI/CD

- `.github/workflows/ci.yml` - Update image name
- `.github/workflows/release.yml` - Configure registry

## Project Structure

```
├── src/
│   ├── main.rs              # Entry point with leader election
│   ├── lib.rs               # Controller setup
│   ├── health.rs            # Health server (metrics, probes)
│   ├── crd/                  # Custom Resource Definition
│   │   └── mod.rs
│   ├── controller/           # Reconciliation logic
│   │   ├── mod.rs
│   │   ├── reconciler.rs
│   │   ├── state_machine.rs
│   │   ├── error.rs
│   │   ├── status.rs
│   │   └── context.rs
│   ├── resources/            # Generated K8s resources
│   │   ├── mod.rs
│   │   └── common.rs
│   └── webhooks/             # Admission webhooks
│       ├── mod.rs
│       └── server.rs
├── tests/
│   ├── common/               # Test utilities
│   ├── unit/                 # Unit tests
│   ├── integration/          # Integration tests
│   └── proptest/             # Property-based tests
├── config/
│   ├── deploy/               # Deployment manifests
│   ├── rbac/                 # RBAC manifests
│   └── samples/              # Example resources
├── charts/
│   └── my-operator/          # Helm chart
└── .github/
    └── workflows/            # CI/CD pipelines
```

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

## Documentation

- **CLAUDE.md** - AI assistant instructions
- **GUIDE.md** - Detailed patterns and best practices

## Design Principles

### Panic-Free Code
All production code paths handle errors gracefully. The Cargo.toml enforces this via clippy lints that deny `unwrap()`, `expect()`, and `panic!()`.

### Idempotent Reconciliation
Every reconcile operation can be safely retried. Use server-side apply for atomic updates.

### Generation Tracking
Skip redundant reconciliations by comparing `metadata.generation` with `status.observedGeneration`.

### State Machine
Resource lifecycle is managed through a formal FSM with defined phase transitions:
- Pending → Creating → Running → Updating → Running
- Any state → Deleting (terminal)

## License

MIT
