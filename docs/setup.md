# Setup

Local environment for building and running the controller: **Docker + kind**.
The install commands below are Homebrew, for macOS; nothing else here is
macOS-specific.

## Prerequisites

| Tool | Install | Notes |
|---|---|---|
| Docker | [Docker Desktop](https://www.docker.com/products/docker-desktop/) or `brew install --cask docker` | Must be running before anything else |
| kind | `brew install kind` | Creates the local cluster |
| kubectl | `brew install kubectl` | |
| Helm | `brew install helm` | Only for the Prometheus stack |
| Go | `brew install go` | 1.26 or newer (see `go.mod`) |


Give Docker a few GB of memory — the demo runs a kind node plus the
kube-prometheus-stack. The first run pulls images, so it needs network access.

Verify:

```bash
docker info --format '{{.ServerVersion}} {{.Architecture}}'   # daemon is up
kind version
kubectl version --client
go version
```

## Quickest path

`make up` does everything in this document and then deploys the controller and
samples — see [running.md](running.md). The steps below are the same work done
one piece at a time.

## Create the cluster

```bash
make setup-test-e2e        # creates kind cluster "task-controller-test-e2e"
kubectl config use-context kind-task-controller-test-e2e
kubectl get nodes
```

The cluster name is `KIND_CLUSTER` in the Makefile; override it if you want a
different one. `make cleanup-test-e2e` deletes it.

## First build

```bash
make docker-build-dev      # builds task-controller:dev
make kind-load             # loads it into the cluster
```

## Teardown

```bash
make down                  # remove samples + controller, keep the cluster
make cleanup-test-e2e      # delete the kind cluster entirely
```
