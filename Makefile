# Image URL to use all building/pushing image targets
IMG ?= controller:latest
# YEAR defines the year value used for substituting the YEAR placeholder in the boilerplate header.
YEAR ?= $(shell date +%Y)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	"$(CONTROLLER_GEN)" rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	"$(CONTROLLER_GEN)" object:headerFile="hack/boilerplate.go.txt",year=$(YEAR) paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell "$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path)" CGO_ENABLED=0 go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# kubectl kuberc is disabled by default for test isolation; enable with:
# - KUBECTL_KUBERC=true
# CertManager is installed by default; skip with:
# - CERT_MANAGER_INSTALL_SKIP=true
KIND_CLUSTER ?= task-controller-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND=$(KIND) KIND_CLUSTER=$(KIND_CLUSTER) go test -tags=e2e ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	"$(GOLANGCI_LINT)" run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	"$(GOLANGCI_LINT)" run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	"$(GOLANGCI_LINT)" config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	CGO_ENABLED=0 go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# Override BASE_IMAGE to build from another registry, e.g.
# make docker-build IMG=<img> BASE_IMAGE=docker.io/library/golang:1.26
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name task-controller-builder
	$(CONTAINER_TOOL) buildx use task-controller-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) $(if $(BASE_IMAGE),--build-arg BASE_IMAGE=$(BASE_IMAGE)) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm task-controller-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" apply -f -; else echo "No CRDs to install; skipping."; fi

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	@out="$$( "$(KUSTOMIZE)" build config/crd 2>/dev/null || true )"; \
	if [ -n "$$out" ]; then echo "$$out" | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -; else echo "No CRDs to delete; skipping."; fi

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=${IMG}
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	"$(KUSTOMIZE)" build config/default | "$(KUBECTL)" delete --ignore-not-found=$(ignore-not-found) -f -

##@ Local demo (Docker + kind)

# Use :dev, never :latest. A :latest tag defaults to imagePullPolicy: Always,
# which sends kind to Docker Hub instead of using the image we loaded.
DEV_IMG ?= task-controller:dev
MONITORING_NS ?= monitoring
DEMO_NS ?= demo
CONTROLLER_NS ?= task-controller-system
CONTROLLER_DEPLOY ?= task-controller-controller-manager

.PHONY: docker-build-dev
docker-build-dev: ## Build the controller image for the local demo.
	# --provenance=false --sbom=false: buildx otherwise attaches attestation
	# manifests, which turn the result into a manifest list that
	# `kind load docker-image` cannot import.
	$(CONTAINER_TOOL) build --provenance=false --sbom=false -t $(DEV_IMG) .

.PHONY: kind-load
kind-load: ## Load the controller and task images into the kind cluster.
	# Loaded via a saved archive rather than `kind load docker-image`: with
	# Docker's containerd image store, the latter can fail to resolve layer
	# digests ("content digest ... not found"). Saving first is unambiguous.
	@mkdir -p $(LOCALBIN)
	$(CONTAINER_TOOL) save $(DEV_IMG) -o $(LOCALBIN)/task-controller-image.tar
	$(KIND) load image-archive $(LOCALBIN)/task-controller-image.tar --name $(KIND_CLUSTER)
	@rm -f $(LOCALBIN)/task-controller-image.tar
	# Pre-pull the task image into the node so demo pods never wait on Docker
	# Hub mid-run. Pulled node-side rather than loaded from the host, because
	# busybox is a multi-arch manifest list that does not survive save/load.
	# Best-effort: if it fails, the kubelet simply pulls it on first use.
	-@for node in $$($(KIND) get nodes --name $(KIND_CLUSTER)); do \
		$(CONTAINER_TOOL) exec $$node crictl pull busybox:1.36 >/dev/null 2>&1 || true; \
	done

.PHONY: prometheus-install
prometheus-install: ## Install kube-prometheus-stack into the cluster.
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update prometheus-community
	# serviceMonitorSelectorNilUsesHelmValues=false is what makes Prometheus
	# select our ServiceMonitor; without it the chart only picks up its own.
	helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
		-n $(MONITORING_NS) --create-namespace \
		--set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
		--set alertmanager.enabled=false \
		--set grafana.enabled=false \
		--wait --timeout 10m

.PHONY: deploy-dev
deploy-dev: manifests kustomize ## Deploy with the local dev overlay (plain HTTP metrics on :8080).
	cd config/manager && "$(KUSTOMIZE)" edit set image controller=$(DEV_IMG)
	"$(KUSTOMIZE)" build config/dev | $(KUBECTL) apply -f -

.PHONY: samples
samples: ## Apply the sample Tasks.
	$(KUBECTL) apply -k config/samples/

.PHONY: up
up: setup-test-e2e prometheus-install docker-build-dev kind-load deploy-dev samples ## Full local demo, end to end.
	$(KUBECTL) -n $(CONTROLLER_NS) rollout status deploy/$(CONTROLLER_DEPLOY) --timeout=180s
	@echo
	@echo "Up. Watch the Tasks with:   kubectl get tk -A -w"
	@echo "Live state:                 make demo"
	@echo "Metrics:                    make metrics"

.PHONY: redeploy
redeploy: docker-build-dev kind-load ## Rebuild, reload and restart after a code change.
	# Reloading the same tag does not restart the pod on its own.
	$(KUBECTL) -n $(CONTROLLER_NS) rollout restart deploy/$(CONTROLLER_DEPLOY)
	$(KUBECTL) -n $(CONTROLLER_NS) rollout status deploy/$(CONTROLLER_DEPLOY) --timeout=180s

.PHONY: demo
demo: ## Show the live state of the demo.
	@echo "== Tasks =="
	@$(KUBECTL) get tk -A
	@echo
	@echo "== Jobs in $(DEMO_NS) =="
	@$(KUBECTL) get jobs -n $(DEMO_NS) 2>/dev/null || true
	@echo
	@echo "== Run history for hello =="
	@$(KUBECTL) get tk -n $(DEMO_NS) hello -o jsonpath='{.status.recentRuns}' 2>/dev/null || true
	@echo

.PHONY: metrics
metrics: ## Port-forward and print the controller's own metrics.
	@$(KUBECTL) -n $(CONTROLLER_NS) port-forward svc/$(CONTROLLER_DEPLOY)-metrics-service 18080:8080 >/dev/null 2>&1 & \
		PF=$$!; sleep 3; \
		curl -s localhost:18080/metrics | grep '^task_controller' || true; \
		kill $$PF 2>/dev/null || true

.PHONY: metrics-example
metrics-example: ## Capture docs/metrics-example.txt from the running controller.
	@mkdir -p docs
	@$(KUBECTL) -n $(CONTROLLER_NS) port-forward svc/$(CONTROLLER_DEPLOY)-metrics-service 18080:8080 >/dev/null 2>&1 & \
		PF=$$!; sleep 3; \
		{ echo "# Example /metrics output, captured from a live kind run."; \
		  echo "# Regenerate with: make metrics-example"; \
		  echo; \
		  curl -s localhost:18080/metrics | grep -E '^(task_controller|# (HELP|TYPE) task_controller)'; } > docs/metrics-example.txt; \
		kill $$PF 2>/dev/null || true
	@echo "Wrote docs/metrics-example.txt"

.PHONY: prometheus-ui
prometheus-ui: ## Port-forward the Prometheus UI to localhost:9090.
	$(KUBECTL) -n $(MONITORING_NS) port-forward svc/kube-prometheus-stack-prometheus 9090

.PHONY: down
down: kustomize ## Remove the samples and the controller, leaving the cluster up.
	-$(KUBECTL) delete -k config/samples/ --ignore-not-found=true
	-"$(KUSTOMIZE)" build config/dev | $(KUBECTL) delete --ignore-not-found=true -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p "$(LOCALBIN)"

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.8.1
CONTROLLER_TOOLS_VERSION ?= v0.22.0

#ENVTEST_VERSION is the controller-runtime version to use for setup-envtest, derived from go.mod
ENVTEST_VERSION ?= $(shell v='$(call gomodver,sigs.k8s.io/controller-runtime)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_VERSION manually (controller-runtime replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v")

#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell v='$(call gomodver,k8s.io/api)'; \
  [ -n "$$v" ] || { echo "Set ENVTEST_K8S_VERSION manually (k8s.io/api replace has no tag)" >&2; exit 1; }; \
  printf '%s\n' "$$v" | sed -E 's/^v?[0-9]+\.([0-9]+).*/1.\1/')

GOLANGCI_LINT_VERSION ?= v2.13.1
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) --bin-dir "$(LOCALBIN)" -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))
	@test -f .custom-gcl.yml && { \
		echo "Building custom golangci-lint with plugins..." && \
		CGO_ENABLED=0 $(GOLANGCI_LINT) custom --destination $(LOCALBIN) --name golangci-lint-custom && \
		mv -f $(LOCALBIN)/golangci-lint-custom $(GOLANGCI_LINT); \
	} || true

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f "$(1)" ;\
GOBIN="$(LOCALBIN)" CGO_ENABLED=0 go install $${package} ;\
mv "$(LOCALBIN)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

define gomodver
$(shell go list -m -f '{{if .Replace}}{{.Replace.Version}}{{else}}{{.Version}}{{end}}' $(1) 2>/dev/null)
endef
