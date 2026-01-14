# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Kubernetes operator for managing Valkey Clusters** built in Rust using kube-rs. It manages ValkeyCluster and ValkeyUpgrade custom resources.

**For detailed pattern explanations, see `GUIDE.md`.**

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
| `src/crd/mod.rs` | Custom Resource Definition (ValkeyCluster) |
| `src/controller/reconciler.rs` | Main reconciliation logic |
| `src/controller/state_machine.rs` | Phase transitions |
| `src/controller/error.rs` | Error types and classification |
| `src/controller/status.rs` | Condition management |
| `src/controller/context.rs` | Shared context (client, recorder) |
| `src/resources/common.rs` | Resource generators |
| `src/webhooks/server.rs` | Admission webhook server |
| `src/health.rs` | Health endpoints and metrics |

### CRD Structure

- **API group**: `valkeyoperator.smoketurner.com`
- **API version**: `v1alpha1`
- **Kinds**: `ValkeyCluster`, `ValkeyUpgrade`
- **Short name**: `vc` (for ValkeyCluster)

### Generated Resources

The operator creates these Kubernetes resources for each ValkeyCluster:
- **StatefulSet**: Valkey pods with stable identity
- **Headless Service**: Cluster discovery
- **Client Service**: Client access endpoint
- **PodDisruptionBudget**: Maintain quorum
- **Certificate**: TLS certs via cert-manager

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

## Testing

| Directory | Purpose |
|-----------|---------|
| `tests/unit/` | Component tests (no cluster) |
| `tests/integration/` | End-to-end tests (requires cluster) |
| `tests/proptest/` | Property-based tests |
| `tests/common/fixtures.rs` | Test fixtures |

## Documentation

For detailed patterns, rationale, and best practices, see **`GUIDE.md`**.
