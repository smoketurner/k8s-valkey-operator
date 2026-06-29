# Makefile for valkey-operator Kubernetes Operator

# Image configuration
IMG ?= valkey-operator:latest
NAMESPACE ?= valkey-operator-system

# Operator version (used for the OLM bundle image tag and CSV version).
VERSION ?= 0.1.0

# Bundle image configuration. Sibling of the operator image at GHCR.
BUNDLE_IMG ?= ghcr.io/smoketurner/k8s-valkey-operator-bundle:v$(VERSION)

# operator-sdk version. CI pins this to the same value via the workflow env.
OPERATOR_SDK_VERSION ?= v1.42.2

# Tool binaries
KUBECTL ?= kubectl
CARGO ?= cargo
OPERATOR_SDK ?= operator-sdk

.PHONY: all build test install uninstall deploy undeploy run clean help \
        bundle-sync bundle-set-version bundle-validate bundle-build bundle-push

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

test-functional: ## Run functional tests (no cluster required)
	$(CARGO) test --test functional

test-all: test test-functional ## Run all non-integration tests

audit: ## Run security audit on dependencies
	$(CARGO) audit

test-integration: install ## Run integration tests (requires running cluster)
	RUST_MIN_STACK=16777216 $(CARGO) test --test integration -- --ignored

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

deploy-sample: ## Deploy a sample ValkeyCluster
	$(KUBECTL) apply -f config/samples/valkeycluster.yaml

delete-sample: ## Delete the sample ValkeyCluster
	$(KUBECTL) delete -f config/samples/valkeycluster.yaml --ignore-not-found

##@ OLM Bundle

bundle-sync: ## Refresh bundle/manifests/ CRDs from config/crd/ (run after CRD edits)
	@cp config/crd/valkeycluster.yaml bundle/manifests/valkeyclusters.valkey-operator.smoketurner.com.yaml
	@cp config/crd/valkeyupgrade.yaml bundle/manifests/valkeyupgrades.valkey-operator.smoketurner.com.yaml
	@echo "Bundle CRDs synced from config/crd/"

bundle-set-version: ## Set the CSV version (VERSION=X.Y.Z, no v prefix). Used by the release workflow.
	@test -n "$(VERSION)" || { echo "ERROR: VERSION is required (e.g., make bundle-set-version VERSION=0.1.0)"; exit 1; }
	@CSV=bundle/manifests/valkey-operator.clusterserviceversion.yaml; \
	  sed -i.bak \
	    -e "s|^  name: valkey-operator\.v[0-9][0-9.]*|  name: valkey-operator.v$(VERSION)|" \
	    -e "s|^  version: [0-9][0-9.]*|  version: $(VERSION)|" \
	    -e "s|k8s-valkey-operator:v[0-9][0-9.]*|k8s-valkey-operator:v$(VERSION)|g" \
	    "$$CSV" && rm -f "$$CSV.bak"
	@echo "CSV version set to $(VERSION)"

bundle-validate: ## Validate the OLM bundle (requires operator-sdk $(OPERATOR_SDK_VERSION))
	@command -v $(OPERATOR_SDK) >/dev/null 2>&1 || { \
		echo "ERROR: operator-sdk not found. Install $(OPERATOR_SDK_VERSION) from https://sdk.operatorframework.io/docs/installation/"; \
		exit 1; \
	}
	$(OPERATOR_SDK) bundle validate ./bundle --select-optional suite=operatorframework

bundle-build: ## Build the OLM bundle image
	docker build -f bundle.Dockerfile -t $(BUNDLE_IMG) .

bundle-push: ## Push the OLM bundle image
	docker push $(BUNDLE_IMG)

##@ Cleanup

clean: ## Clean build artifacts
	$(CARGO) clean

clean-all: uninstall clean ## Uninstall from cluster and clean build artifacts

##@ Help

help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
