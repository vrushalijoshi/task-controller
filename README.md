# task-controller

A Kubernetes controller that manages a custom `Task` resource: each `Task`
describes a command and a cron schedule, and the controller turns every cron slot
into exactly one `batch/v1` Job, folds the Job's outcome back into `Task.status`,
deletes the Job, and exposes Prometheus metrics about the whole population.

```yaml
apiVersion: custom.io/v1alpha1
kind: Task
metadata:
  name: hello
  namespace: demo
spec:
  command: "echo Hello World"
  schedule: "*/5 * * * *"
```

```
$ kubectl get tk -A
NAMESPACE   NAME       SCHEDULE      SUSPEND   STATE       LAST EXECUTION   NEXT                   SUCCEEDED   FAILED   AGE
demo        bad-cron   0 0 30 2 *              Failed                                                                   4m
demo        fail       * * * * *               Failed      23s              2026-10-13T03:09:00Z               20       4m
demo        hello      * * * * *               Completed   23s              2026-10-13T03:09:00Z   20                   4m
demo        slow       */2 * * * *             Running     23s              2026-10-13T03:10:00Z   4                    4m
```

## Quickstart

Requires Docker, kind, kubectl, Helm and Go — see [docs/setup.md](docs/setup.md).

```bash
make up                    # cluster, Prometheus, build, load, deploy, samples
kubectl get tk -A -w
make metrics               # the controller's own metrics
make down                  # tear down, keep the cluster
```

## What it does

- **Watches Tasks** and launches a Job per cron slot, running
  `/bin/sh -c <spec.command>` in `spec.image` (default `busybox:1.36`).
- **Tracks state** across `Pending → Running → Completed / Failed`, where `state`
  describes the *most recent execution* — between runs a Task shows its last
  outcome rather than reverting to `Pending`.
- **Keeps a bounded run history** (`status.recentRuns`, last 3) that survives the
  deleted Jobs.
- **Deletes finished Jobs** once their outcome is durable, with a TTL as a safety
  net.
- **Validates schedules properly**: rejects not just unparseable cron but also
  schedules that parse yet can never fire (`0 0 30 2 *` — February 30th) and
  sub-minute schedules that would collide on Job names.
- **Cleans up gracefully**: a finalizer records why an in-flight run ended,
  deletes the Jobs, and only then releases the Task — so `kubectl delete tk`
  returning means the Jobs really are gone.
- **Exposes Prometheus metrics** and a `ServiceMonitor`.

## Metrics

| Metric | Type | Notes |
|---|---|---|
| `task_controller_tasks` | gauge | Total Tasks |
| `task_controller_tasks_by_state` | gauge | Per state, zero-filled over all four |
| `task_controller_task_failures_total` | counter | Workload failures only |
| `task_controller_task_successes_total` | counter | |
| `task_controller_jobs_created_total` | counter | |
| `task_controller_task_runs_skipped_total` | counter | by `reason` |
| `task_controller_task_runs_aborted_total` | counter | by `reason` — deletions, not failures |
| `task_controller_task_state_collector_up` | gauge | 0 if the gauges could not be computed |

## Documentation

| Document | Contents |
|---|---|
| [docs/design.md](docs/design.md) | Architecture, API design, correctness model, RBAC, trade-offs |
| [docs/setup.md](docs/setup.md) | Prerequisites and local environment |
| [docs/running.md](docs/running.md) | Build, deploy, demo and test commands |

## Layout

```
api/v1alpha1/task_types.go     CRD schema and markers
internal/controller/
  task_controller.go           Reconcile — eight steps, finalization
  schedule.go                  Slot maths and schedule validation 
  job.go                       Job template and outcome classification
internal/metrics/metrics.go    Counters + scrape-time state collector
cmd/main.go                    Manager wiring
config/                        CRD, RBAC, manager, prometheus, samples, dev overlay
test/e2e/                      End-to-end specs against a real kind cluster
```

## Development

```bash
make test        # unit + envtest
make test-e2e    # end-to-end on kind
make lint        # golangci-lint
make redeploy    # rebuild, reload, restart after a code change
```

After editing `api/v1alpha1/task_types.go` or any marker, run
`make manifests generate`.

## Security

The controller runs in `task-controller-system` under its own ServiceAccount with
least privilege, and **creating a Task is equivalent to running arbitrary commands
in that namespace** — the same power as `create jobs`, so grant Task RBAC
accordingly. The permissions and the reasoning are in
[docs/design.md](docs/design.md#rbac-and-security).