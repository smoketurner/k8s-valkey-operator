# Rust Kubernetes Operator Development Guide

A comprehensive guide for building production-grade Kubernetes operators in Rust using kube-rs. This guide explains the patterns implemented in this template repository and provides the rationale for each design decision.

## How to Use This Guide

This guide complements the template code. Each section explains:
- **What**: The pattern or concept
- **Why**: The rationale and benefits
- **Reference**: Where to find the implementation in the template

For full implementations, refer to the source files directly. This guide focuses on understanding the "why" behind each pattern.

## Version Requirements

| Component | Minimum Version | Notes |
|-----------|-----------------|-------|
| Rust | 1.92+ | Edition 2024, MSRV enforced in `Cargo.toml` |
| Kubernetes | 1.35+ | Required for latest API features |
| kube-rs | 2.x | With k8s-openapi v1_34 feature |

---

## Table of Contents

1. [Project Setup](#1-project-setup)
2. [Coding Standards](#2-coding-standards)
3. [CRD Design](#3-crd-design)
4. [Controller Architecture](#4-controller-architecture)
5. [Reconciliation](#5-reconciliation)
6. [Error Handling](#6-error-handling)
7. [State Machine](#7-state-machine)
8. [Resource Generation](#8-resource-generation)
9. [Health Server](#9-health-server)
10. [Kubernetes 1.35+ Features](#10-kubernetes-135-features)
11. [Testing](#11-testing)
12. [Webhooks](#12-webhooks)
13. [RBAC & Security](#13-rbac--security)
14. [Deployment](#14-deployment)
15. [CI/CD](#15-cicd)

---

## 1. Project Setup

### 1.1 Cargo.toml Configuration

**What**: Dependencies, features, and lint configuration for the operator.

**Why**:
- **Exact version pinning** (`version = "=X.Y.Z"`) ensures reproducible builds across all environments
- **`default-features = false`** minimizes binary size and compile time
- **Panic-prevention lints** enforce safe code patterns at compile time

**Reference**: `Cargo.toml`

#### Key Dependencies

| Dependency | Purpose |
|------------|---------|
| `kube` | Kubernetes client and controller runtime |
| `k8s-openapi` | Kubernetes API type definitions |
| `kube-leader-election` | HA leader election for multi-replica deployments |
| `tokio` | Async runtime |
| `serde` / `schemars` | Serialization and JSON Schema generation |
| `thiserror` | Error type derivation |
| `tracing` | Structured logging |
| `axum` | Health server HTTP framework |
| `prometheus-client` | Metrics exposition |

### 1.2 Directory Structure

**What**: Organized layout separating concerns.

**Why**: Clear separation enables independent testing, easier navigation, and modular development.

```
src/
├── main.rs              # Entry point, leader election, startup
├── lib.rs               # Controller setup, stream configuration
├── crd/
│   └── mod.rs           # Custom Resource Definition
├── controller/
│   ├── mod.rs           # Module exports
│   ├── reconciler.rs    # Main reconciliation logic
│   ├── state_machine.rs # FSM for resource lifecycle
│   ├── error.rs         # Error types with classification
│   ├── status.rs        # Condition management
│   └── context.rs       # Shared context (client, recorder)
├── resources/
│   └── common.rs        # Resource generators (Deployment, etc.)
├── webhooks/
│   └── server.rs        # Admission webhook server
└── health.rs            # Health endpoints and metrics

tests/
├── unit/                # Unit tests (no cluster required)
├── integration/         # Integration tests (requires cluster)
├── proptest/            # Property-based tests
└── common/
    └── fixtures.rs      # Test fixtures and builders

config/
├── rbac/                # RBAC manifests
├── deploy/              # Deployment manifests
└── samples/             # Example CRs

charts/
└── my-operator/         # Helm chart
```

### 1.3 Clippy Configuration

**What**: Lint rules enforcing panic-free code.

**Why**: Operators must never crash. These lints catch potential panics at compile time.

**Reference**: `.clippy.toml`, `Cargo.toml` `[lints.clippy]` section

#### Enforced Lints

| Lint | Purpose |
|------|---------|
| `unwrap_used` | Prevent `.unwrap()` calls |
| `expect_used` | Prevent `.expect()` calls |
| `panic` | Prevent `panic!()` macro |
| `indexing_slicing` | Prevent `array[i]` (use `.get()` instead) |
| `exit` | Prevent `std::process::exit()` |

---

## 2. Coding Standards

### 2.1 Panic-Free Code Policy

**What**: Production code must never panic.

**Why**:
- Operators run continuously and must recover from all error states
- Panics terminate the process, losing in-flight work
- Kubernetes will restart the pod, but state may be inconsistent

**Reference**: All `src/` files demonstrate these patterns.

#### Do This / Not That

| Don't | Do |
|-------|-----|
| `option.unwrap()` | `option.unwrap_or_default()` |
| `result.expect("msg")` | `result?` or `result.map_err(...)` |
| `items[0]` | `items.first()` or `items.get(0)` |
| `panic!("error")` | `return Err(Error::...)` |

### 2.2 Rust 2024 Edition Patterns

**What**: Modern Rust idioms for cleaner code.

**Why**: Reduced nesting, better error handling, more expressive code.

#### let-else for Early Returns

```rust
let Some(name) = resource.metadata.name.as_ref() else {
    return Err(Error::MissingField("metadata.name"));
};
```

#### if-let Chains

```rust
if let Some(status) = resource.status.as_ref()
    && let Some(phase) = status.phase.as_ref()
    && *phase == Phase::Running
{
    // Handle running state
}
```

#### is_some_and / is_none_or

```rust
let has_finalizer = resource.finalizers().iter().any(|f| f == FINALIZER);
let should_reconcile = generation.is_none_or(|g| g != observed);
```

### 2.3 Safe Option/Result Handling

**What**: Patterns for extracting values without panicking.

**Why**: Graceful handling of missing or invalid data.

| Pattern | Use Case |
|---------|----------|
| `unwrap_or_default()` | When a sensible default exists |
| `unwrap_or("fallback")` | When you have a specific fallback |
| `ok_or(Error::...)` | Convert Option to Result |
| `map(\|v\| ...)` | Transform the inner value |
| `and_then(\|v\| ...)` | Chain Option-returning operations |
| `as_ref()` | Borrow without consuming |
| `as_deref()` | Get `&str` from `Option<String>` |

---

## 3. CRD Design

### 3.1 CustomResource Structure

**What**: The core API type users interact with.

**Why**:
- **Spec**: Declares desired state (user-controlled)
- **Status**: Reports observed state (operator-controlled)
- **Conditions**: Kubernetes-standard detailed status

**Reference**: `src/crd/mod.rs`

#### Field Organization

| Section | Purpose | Mutability |
|---------|---------|------------|
| `spec` | User-defined desired state | User can modify |
| `status` | Operator-reported observed state | Operator-only |
| `status.conditions` | Detailed status signals | Operator-only |
| `metadata` | Kubernetes object metadata | Managed by K8s |

### 3.2 Phase Lifecycle

**What**: High-level state indicator for the resource.

**Why**: Provides at-a-glance status for `kubectl get` output.

**Reference**: `Phase` enum in `src/crd/mod.rs`

#### Phase Definitions

| Phase | Meaning |
|-------|---------|
| `Pending` | Resource created, not yet processed |
| `Creating` | Initial resources being created |
| `Running` | All resources healthy and operational |
| `Updating` | Spec change being applied |
| `Degraded` | Partially functional (some replicas unhealthy) |
| `Failed` | Unrecoverable error (requires intervention) |
| `Deleting` | Cleanup in progress |

#### State Diagram

```
Pending → Creating → Running ⟷ Updating
              ↓         ↓
           Degraded ← ─ ┘
              ↓
           Failed → Pending (retry)

Any state → Deleting (on deletion)
```

### 3.3 Conditions

**What**: Kubernetes-standard detailed status reporting.

**Why**:
- Multiple independent status signals
- Each condition has type, status, reason, message, timestamp
- Tools like `kubectl wait` can watch specific conditions

**Reference**: Condition constants and builders in `src/crd/mod.rs`

#### Condition Types

| Type | True When | False When |
|------|-----------|------------|
| `Ready` | All replicas healthy | Any replica unhealthy |
| `Progressing` | Changes being applied | Stable state reached |
| `Degraded` | Partial functionality | Fully healthy or fully failed |

### 3.4 Print Columns

**What**: Custom columns in `kubectl get` output.

**Why**: Better UX - users see important status without `-o yaml`.

**Reference**: `#[kube(printcolumn = ...)]` attributes in `src/crd/mod.rs`

---

## 4. Controller Architecture

### 4.1 Entry Point & Leader Election

**What**: Main function setup with HA support.

**Why**:
- Multiple operator replicas prevent single point of failure
- Only one instance should reconcile at a time to prevent conflicts
- Leader election ensures exactly-once processing

**Reference**: `src/main.rs`

#### Startup Sequence

1. Initialize tracing (structured logging)
2. Create Kubernetes client
3. Start health server (probes work before leadership)
4. Acquire leader election lease (blocks until leader)
5. Start controller (only as leader)
6. Run lease renewal in background
7. Handle shutdown signals gracefully

### 4.2 Controller Stream Setup

**What**: Efficient event watching and filtering.

**Why**:
- **Reflector**: In-memory cache for fast lookups
- **Generation predicate**: Skip status-only updates
- **Metadata watchers**: Reduce memory for owned resources
- **Backoff**: Automatic retry with exponential delay

**Reference**: `src/lib.rs`

#### Optimizations Applied

| Optimization | Benefit |
|--------------|---------|
| `predicates::generation` | Skip reconciliation when only status changed |
| `metadata_watcher` | Less memory for owned resources (Service, ConfigMap) |
| `default_backoff()` | Automatic retry with exponential backoff |
| `any_semantic()` | More reliable in test environments |

### 4.3 Shared Context

**What**: Dependency injection for the reconciler.

**Why**:
- Single Kubernetes client shared across reconciliations
- Event recorder for publishing Kubernetes events
- Health state for readiness signaling
- Enables testing with mock clients

**Reference**: `src/controller/context.rs`

### 4.4 Graceful Shutdown

**What**: Clean termination on SIGTERM/SIGINT.

**Why**:
- Mark operator as not ready (stop receiving new work)
- Allow in-flight reconciliations to complete
- Prevent data corruption from abrupt termination

**Reference**: `shutdown_signal()` in `src/main.rs`

---

## 5. Reconciliation

### 5.1 Reconcile Function

**What**: The core logic that drives desired state.

**Why**:
- **Level-triggered**: React to current state, not events
- **Idempotent**: Safe to run multiple times
- **Declarative**: Describe what should exist, not how to create it

**Reference**: `src/controller/reconciler.rs`

#### Reconciliation Flow

```
┌─────────────────────────────────────────────┐
│              Reconcile Called               │
└─────────────────┬───────────────────────────┘
                  ↓
┌─────────────────────────────────────────────┐
│         Check deletion timestamp            │
│  (Has finalizer + deletion timestamp?)      │
└─────────────────┬───────────────────────────┘
         No      │      Yes
         ↓       │       ↓
┌────────────────┴──────────────────┐
│      Handle normal reconcile      │ → Handle deletion
└─────────────────┬─────────────────┘
                  ↓
┌─────────────────────────────────────────────┐
│          Ensure finalizer exists            │
└─────────────────┬───────────────────────────┘
                  ↓
┌─────────────────────────────────────────────┐
│       Apply owned resources (SSA)           │
│  (Deployment, ConfigMap, Service)           │
└─────────────────┬───────────────────────────┘
                  ↓
┌─────────────────────────────────────────────┐
│        Check resource readiness             │
└─────────────────┬───────────────────────────┘
                  ↓
┌─────────────────────────────────────────────┐
│         Update status & conditions          │
└─────────────────┬───────────────────────────┘
                  ↓
┌─────────────────────────────────────────────┐
│    Return Action (requeue or await)         │
└─────────────────────────────────────────────┘
```

### 5.2 Finalizer Pattern

**What**: Hook for cleanup before Kubernetes deletes the resource.

**Why**:
- Prevent orphaned resources (external resources not managed by owner refs)
- Graceful shutdown of workloads
- Audit logging of deletions

**Reference**: `FINALIZER` constant and `handle_deletion()` in `src/controller/reconciler.rs`

#### Lifecycle

1. **On Create**: Add finalizer to resource
2. **On Delete Request**: Kubernetes sets `deletionTimestamp` but doesn't delete
3. **Operator Cleanup**: Scale down, delete external resources
4. **Remove Finalizer**: Kubernetes can now delete the resource

### 5.3 Server-Side Apply

**What**: Declarative resource updates with conflict resolution.

**Why**:
- **Idempotent**: Same input always produces same result
- **Field ownership**: Multiple controllers can manage different fields
- **Conflict resolution**: Automatic handling of concurrent updates

**Reference**: `apply_resource()` usage in `src/controller/reconciler.rs`

#### Usage Pattern

```rust
let patch = Patch::Apply(&resource);
let params = PatchParams::apply(FIELD_MANAGER).force();
api.patch(name, &params, &patch).await?;
```

### 5.4 Generation Tracking

**What**: Track which spec version the status reflects.

**Why**:
- Skip reconciliation when only status changed
- Know if status is stale relative to spec
- Enable efficient change detection

**Reference**: `update_status()` in `src/controller/reconciler.rs`

---

## 6. Error Handling

### 6.1 Error Types

**What**: Structured error types with context.

**Why**:
- Different errors need different handling strategies
- Preserve context for debugging
- Enable smart retry decisions

**Reference**: `src/controller/error.rs`

#### Error Classification

| Category | Examples | Action |
|----------|----------|--------|
| **Transient** | Network timeout, API 503 | Retry with backoff |
| **Conflict** | Resource version mismatch | Retry immediately |
| **Validation** | Invalid spec values | Fail fast, update status |
| **Permanent** | Missing CRD, RBAC denied | Fail, require intervention |

### 6.2 Error Policy

**What**: Determines requeue behavior after errors.

**Why**:
- Transient errors should retry with exponential backoff
- Permanent errors should not retry indefinitely
- Not-found errors during deletion are expected

**Reference**: `error_policy()` in `src/controller/reconciler.rs`

### 6.3 Exponential Backoff

**What**: Increasing delays between retries.

**Why**:
- Prevents thundering herd on transient failures
- Gives external systems time to recover
- Jitter prevents synchronized retries

**Reference**: `BackoffConfig` in `src/controller/error.rs`

---

## 7. State Machine

### 7.1 Valid Transitions

**What**: Explicit rules for phase changes.

**Why**:
- Prevents invalid state combinations
- Documents expected lifecycle
- Enables testing of all transitions

**Reference**: `src/controller/state_machine.rs`

#### Transition Rules

| From | Event | To |
|------|-------|-----|
| Pending | ResourcesApplied | Creating |
| Creating | AllReplicasReady | Running |
| Running | SpecChanged | Updating |
| Running | ReplicasDegraded | Degraded |
| Updating | AllReplicasReady | Running |
| Degraded | AllReplicasReady | Running |
| Any | DeletionRequested | Deleting |

### 7.2 Transition Context

**What**: Guards that validate preconditions.

**Why**:
- Ensure transition only happens when appropriate
- Prevent premature state changes
- Provide clear failure reasons

**Reference**: `TransitionContext` in `src/controller/state_machine.rs`

---

## 8. Resource Generation

### 8.1 Standard Labels

**What**: Kubernetes-standard labels on all resources.

**Why**:
- Discoverability via `kubectl get -l`
- Integration with monitoring tools
- Cost attribution and ownership tracking

**Reference**: `resource_labels()` in `src/resources/common.rs`

#### Required Labels

| Label | Value | Purpose |
|-------|-------|---------|
| `app.kubernetes.io/name` | Resource name | Primary identifier |
| `app.kubernetes.io/component` | Component type | Filter by component |
| `app.kubernetes.io/managed-by` | `my-operator` | Identify managing controller |

### 8.2 Owner References

**What**: Link child resources to parent CRD.

**Why**:
- Automatic garbage collection when parent deleted
- Clear ownership chain
- Prevents orphaned resources

**Reference**: `owner_reference()` in `src/resources/common.rs`

### 8.3 Generated Resources

**What**: Kubernetes resources created for each CRD instance.

**Why**: Encapsulate the complexity of running workloads.

**Reference**: `generate_*` functions in `src/resources/common.rs`

| Resource | Purpose |
|----------|---------|
| Deployment | Manages pods for the workload |
| ConfigMap | Configuration data from spec |
| Service | Network access to pods |

---

## 9. Health Server

### 9.1 Endpoints

**What**: HTTP endpoints for probes and metrics.

**Why**:
- Kubernetes probes determine pod health
- Prometheus scrapes metrics for monitoring
- Enables rolling updates without downtime

**Reference**: `src/health.rs`

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `/healthz` | Liveness probe | 200 if process alive |
| `/readyz` | Readiness probe | 200 if ready to serve |
| `/metrics` | Prometheus metrics | Prometheus text format |

### 9.2 Dynamic Readiness

**What**: Readiness state changes during lifecycle.

**Why**:
- Not ready during startup (before leader election)
- Ready during normal operation
- Not ready during shutdown (drain traffic)

**Reference**: `HealthState` in `src/health.rs`

#### Readiness Lifecycle

```
Startup     → ready = false (not yet leader)
Leadership  → ready = true  (controller running)
Shutdown    → ready = false (draining)
Termination → process exits
```

---

## 10. Kubernetes 1.35+ Features

This template targets Kubernetes 1.35+ as the minimum version, enabling use of modern API features.

### 10.1 In-Place Resource Resizing

**What**: Modify CPU/memory requests without pod restart.

**Why**:
- Zero-downtime vertical scaling
- React to workload changes dynamically
- Better resource utilization

#### Resize Status Values

| Status | Meaning |
|--------|---------|
| `Proposed` | Resize requested, awaiting scheduler |
| `InProgress` | Kubelet applying the resize |
| `Deferred` | Cannot resize now, will retry |
| `Infeasible` | Cannot resize (node constraints) |

#### Pattern for Checking Resize

```rust
fn check_resize_status(pod: &Pod) -> Option<&str> {
    pod.status
        .as_ref()
        .and_then(|s| s.resize.as_deref())
}

fn get_allocated_resources(pod: &Pod) -> Option<&BTreeMap<String, Quantity>> {
    pod.status
        .as_ref()
        .and_then(|s| s.container_statuses.as_ref())
        .and_then(|cs| cs.first())
        .and_then(|c| c.allocated_resources.as_ref())
}
```

### 10.2 Pod Generation Tracking

**What**: Track when kubelet has applied spec changes.

**Why**:
- Know if pod spec changes are applied
- Avoid acting on stale pod state
- Enable precise rollout tracking

#### Pattern for Checking Sync

```rust
fn is_pod_synced(pod: &Pod) -> bool {
    let spec_gen = pod.metadata.generation;
    let observed_gen = pod.status.as_ref().and_then(|s| s.observed_generation);

    match (spec_gen, observed_gen) {
        (Some(spec), Some(observed)) => spec == observed,
        _ => true,  // Assume synced if missing (backwards compat)
    }
}
```

### 10.3 Feature Availability

| Feature | Minimum K8s Version |
|---------|---------------------|
| In-place resize | 1.27 (beta), 1.33 (GA) |
| Pod observedGeneration | 1.33+ |
| Sidecar containers | 1.28 (alpha), 1.33 (GA) |
| CEL validation | 1.25 (beta), 1.29 (GA) |

---

## 11. Testing

### 11.1 Test Organization

**What**: Structured test layout for different test types.

**Why**: Different tests have different requirements and run times.

**Reference**: `tests/` directory

| Directory | Purpose | Requirements |
|-----------|---------|--------------|
| `tests/unit/` | Component tests | None |
| `tests/integration/` | End-to-end tests | Kubernetes cluster |
| `tests/proptest/` | Property-based tests | None |
| `tests/common/` | Shared fixtures | None |

### 11.2 Test Fixtures

**What**: Builder pattern for creating test resources.

**Why**:
- Consistent, readable test setup
- Sensible defaults with override capability
- Type-safe construction

**Reference**: `tests/common/fixtures.rs`

### 11.3 Integration Test Utilities

**What**: Helpers for cluster-based tests.

**Why**:
- Namespace isolation prevents test interference
- Wait helpers handle async operations
- Cleanup ensures no leftover resources

**Reference**: `tests/integration/` modules

| Module | Purpose |
|--------|---------|
| `cluster.rs` | Shared cluster setup |
| `namespace.rs` | Test namespace management |
| `wait.rs` | Polling utilities |
| `assertions.rs` | Custom assertions |

### 11.4 Property-Based Testing

**What**: Generate random inputs to find edge cases.

**Why**:
- Finds bugs manual tests miss
- Tests invariants across input space
- Documents valid input ranges

**Reference**: `tests/proptest/mod.rs`

---

## 12. Webhooks

### 12.1 Validation Webhook

**What**: Admission controller that validates resources.

**Why**:
- Reject invalid resources before they're stored
- Enforce policies beyond what CEL can express
- Cross-resource validation

**Reference**: `src/webhooks/server.rs`

#### Validation Checks

| Check | Purpose |
|-------|---------|
| Replica bounds | Ensure 1 ≤ replicas ≤ 10 |
| Immutable fields | Prevent dangerous changes |
| Production requirements | Enforce HA in prod namespaces |

### 12.2 When to Use Webhooks

**What**: Decision guide for validation approach.

**Why**: Choose the right tool for the validation need.

| Use Webhooks When | Use CEL When |
|-------------------|--------------|
| Cross-resource validation | Single-resource validation |
| Complex business logic | Simple constraints |
| External system checks | Schema-level checks |
| Detailed error messages | Basic validation |

---

## 13. RBAC & Security

### 13.1 Minimal Privileges

**What**: Least-privilege RBAC design.

**Why**:
- Limit blast radius of compromise
- Principle of least privilege
- Easier security audits

**Reference**: `config/rbac/role.yaml`

#### RBAC Design Principles

1. **Separate rules by resource type** - easier to audit
2. **List explicit verbs** - avoid wildcards
3. **Status subresource separate** - different permissions
4. **Events: create/patch only** - no read needed
5. **Pods: read-only** - operator shouldn't modify pods directly

### 13.2 Secrets Handling

**What**: Safe practices for sensitive data.

**Why**: Secrets in logs or events are security incidents.

#### Guidelines

- **Never log secret values** - log that a secret was created, not its contents
- **Use secret references** - point to secrets, don't embed values
- **Validate secret existence** - check secrets exist before using
- **Don't include secrets in events** - events are visible to users

---

## 14. Deployment

### 14.1 Kubernetes Manifests

**What**: Raw Kubernetes manifests for deployment.

**Why**: Simple deployment without Helm dependency.

**Reference**: `config/deploy/` directory

| File | Purpose |
|------|---------|
| `namespace.yaml` | Operator namespace |
| `deployment.yaml` | Operator deployment |
| `networkpolicy.yaml` | Network security |

### 14.2 Helm Chart

**What**: Parameterized deployment via Helm.

**Why**: Customizable for different environments.

**Reference**: `charts/my-operator/`

#### Key Values

| Value | Purpose | Default |
|-------|---------|---------|
| `replicaCount` | Number of operator replicas | 2 |
| `image.repository` | Container image | `my-operator` |
| `image.tag` | Image version | `latest` |
| `resources` | CPU/memory limits | See values.yaml |

---

## 15. CI/CD

### 15.1 GitHub Actions

**What**: Automated build, test, and release pipeline.

**Why**: Consistent quality gates for all changes.

**Reference**: `.github/workflows/ci.yml`

#### Pipeline Stages

1. **Lint**: Format check, clippy (including panic-free lints)
2. **Test**: Unit tests, property tests
3. **Integration**: Tests against Kind cluster
4. **Build**: Docker image build and push

### 15.2 Make Targets

**What**: Development commands via Makefile.

**Why**: Consistent developer experience across environments.

**Reference**: `Makefile`

| Target | Purpose |
|--------|---------|
| `make build` | Build release binary |
| `make test` | Run unit tests |
| `make lint` | Run clippy |
| `make docker-build` | Build container image |
| `make install` | Install CRD and RBAC |
| `make run` | Run operator locally |

---

## Appendix: Quick Reference

### Pattern → File Mapping

| Pattern | File |
|---------|------|
| CRD definition | `src/crd/mod.rs` |
| Reconciliation | `src/controller/reconciler.rs` |
| State machine | `src/controller/state_machine.rs` |
| Error handling | `src/controller/error.rs` |
| Status updates | `src/controller/status.rs` |
| Shared context | `src/controller/context.rs` |
| Resource generation | `src/resources/common.rs` |
| Health server | `src/health.rs` |
| Webhooks | `src/webhooks/server.rs` |
| Leader election | `src/main.rs` |
| Controller setup | `src/lib.rs` |
| Test fixtures | `tests/common/fixtures.rs` |

### Key Links

- [kube-rs documentation](https://kube.rs)
- [k8s-openapi documentation](https://docs.rs/k8s-openapi)
- [Kubernetes API conventions](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md)
- [Controller runtime patterns](https://kubernetes.io/docs/concepts/architecture/controller/)

---

*This guide accompanies the k8s-operator-template repository. For implementation details, refer to the source files directly.*
