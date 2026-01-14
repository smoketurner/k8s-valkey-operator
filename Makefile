# Makefile for my-operator Kubernetes Operator

# Image configuration
IMG ?= my-operator:latest
NAMESPACE ?= my-operator-system

# Tool binaries
KUBECTL ?= kubectl
CARGO ?= cargo

.PHONY: all build test install uninstall deploy undeploy run clean help

all: build

##@ Build

build: ## Build the operator binary
	$(CARGO) build --release

docker-build: ## Build the Docker image
	docker build --platform linux/amd64 -t $(IMG) .

docker-push: ## Push the Docker image
	docker push $(IMG)

##@ Development

run: ## Run the operator locally (uses current kubeconfig)
	RUST_LOG=info $(CARGO) run

fmt: ## Format code
	$(CARGO) fmt --all

lint: ## Run clippy lints
	$(CARGO) clippy --all-targets --all-features -- -D warnings

check: ## Run cargo check
	$(CARGO) check

##@ Testing

test: ## Run unit tests
	$(CARGO) test --test unit

audit: ## Run security audit on dependencies
	$(CARGO) audit

test-integration: install ## Run integration tests (requires running cluster)
	$(CARGO) test --test integration -- --ignored

##@ Installation

install: install-crd install-rbac ## Install CRD and RBAC onto the cluster

install-crd: ## Install the CRDs
	$(KUBECTL) apply -f config/crd/

install-rbac: ## Install RBAC (creates namespace if needed)
	$(KUBECTL) apply -f config/deploy/namespace.yaml
	$(KUBECTL) apply -f config/rbac/

uninstall: uninstall-rbac uninstall-crd ## Uninstall CRD and RBAC from the cluster

uninstall-crd: ## Uninstall the CRDs
	$(KUBECTL) delete -f config/crd/ --ignore-not-found

uninstall-rbac: ## Uninstall RBAC
	$(KUBECTL) delete -f config/rbac/ --ignore-not-found
	$(KUBECTL) delete namespace $(NAMESPACE) --ignore-not-found

##@ Deployment

deploy: install docker-build ## Deploy the operator to the cluster
	$(KUBECTL) apply -f config/deploy/deployment.yaml
	$(KUBECTL) apply -f config/deploy/networkpolicy.yaml

undeploy: ## Undeploy the operator from the cluster
	$(KUBECTL) delete -f config/deploy/networkpolicy.yaml --ignore-not-found
	$(KUBECTL) delete -f config/deploy/deployment.yaml --ignore-not-found

##@ Samples

deploy-sample: ## Deploy a sample MyResource
	$(KUBECTL) apply -f config/samples/my-resource.yaml

delete-sample: ## Delete the sample MyResource
	$(KUBECTL) delete -f config/samples/my-resource.yaml --ignore-not-found

##@ Cleanup

clean: ## Clean build artifacts
	$(CARGO) clean

clean-all: uninstall clean ## Uninstall from cluster and clean build artifacts

##@ Help

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
