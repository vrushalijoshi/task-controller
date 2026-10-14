# Design

What the controller does is in the [README](../README.md); this document is the
*why* — the assumptions taken from the exercise, the API shape, and the
correctness model.

Everything here serves one guarantee:

> **At most one Job per cron slot, and each finished Job counted exactly once in
> status.**

Three mechanisms carry it, and most other decisions follow from them:

| Mechanism | What it prevents |
|---|---|
| **Deterministic Job names** — `<task>-<unix-seconds/60>` | A duplicate run after a lost status write. `Create` becomes idempotent: `AlreadyExists` is success |
| **An idempotency key on status** — `lastFinishedScheduleTime` | An outcome being counted twice if the Job delete fails |
| **State gauges counted at scrape time**, by listing the Tasks already in the controller's cache | Metrics drifting out of step with the real state, and leftover values for Tasks that no longer exist |

---

## Assumptions

The exercise fixes the shape of the resource and leaves the operational
semantics open. The readings taken here, and the reasoning behind each, are
worth a conversation — these are the choices, stated plainly:

| Question the exercise leaves open | Reading taken |
|---|---|
| What does `state` describe? | The **most recent execution**. Between runs a Task shows its last outcome rather than reverting to `Pending`, so `kubectl get tk` always shows how the last run went |
| Recurring or one-shot? | Recurring. `spec.schedule` is a cron expression, so a Task behaves like a CronJob, not a one-time Job |
| What happens if a run overruns its next slot? | The next slot is skipped (`Forbid`), with an Event and a counter. `Allow` and `Replace` are named as extensions |
| What happens to slots missed while the controller is down? | They collapse into a single run, rather than a backlog burst on restart |
| What counts as a failure? | Only the workload failing. A Task or Job deleted by an operator is a third outcome, `Aborted`, kept out of the failure counter |
| Version | `v1alpha1`, not the `v1` in the example — the API has not earned a stable version |
| Timezone | UTC throughout |

Beyond the required `command` and `schedule`, the spec adds `image`, `suspend`,
`startingDeadlineSeconds`, `backoffLimit` and `activeDeadlineSeconds` — each
small, and each there to make a real failure mode controllable rather than to
grow the API.

---

## API: `custom.io/v1alpha1` Task

Namespaced, short name `tk`, status subresource enabled so spec and status have
separate write paths — users write spec, only the controller writes status.

```yaml
apiVersion: custom.io/v1alpha1
kind: Task
metadata:
  name: hello
  namespace: demo
spec:
  command: "echo Hello World"    # required; run as /bin/sh -c
  schedule: "*/5 * * * *"        # required; 5-field cron or @hourly / @every 10m, UTC
  image: busybox:1.36            # default
  suspend: false
  startingDeadlineSeconds: 120   # optional: skip slots missed by more than this
  backoffLimit: 0                # default 0, so failures surface immediately
  activeDeadlineSeconds: 300     # optional: per-run timeout
status:
  state: Completed               # Pending | Running | Completed | Failed
  lastExecutionTime: "2026-10-13T03:05:00Z"
  lastScheduleTime: "2026-10-13T03:05:00Z"
  nextScheduleTime: "2026-10-13T03:10:00Z"
  lastFinishedScheduleTime: "2026-10-13T03:05:00Z"   # idempotency key
  activeJob: ""
  succeededCount: 12
  failedCount: 1
  recentRuns: [...]              # last 3, newest first — history outlives the Jobs
  conditions:
    - type: ScheduleValid
      status: "True"
```

Every string field is bounded with `maxLength`, and the Task name is capped by a
CEL rule so generated Job names stay inside the 63-character `job-name` label.
`spec.schedule` carries a regex that rejects shape errors at admission; CEL
cannot parse cron, so the real validation happens in the controller.

---

## Reconcile

One pass, eight steps, no per-Task goroutines and no background scheduler — the
requeue *is* the scheduler:

```
0  finalize or adopt      7  requeue at the next slot
1  observe                6  clean up finished Jobs
2  record outcomes  ──►   5  persist status
3  derive state           4  schedule the due slot
```

The anchor for slot maths is `status.lastScheduleTime`, falling back to
`creationTimestamp`, so a Task created at 10:02 with `*/5` first runs at 10:05
rather than immediately. The due slot is the newest slot in `(anchor, now]`.

Order matters: outcomes are recorded before status is written, status is written
before Jobs are deleted, and Prometheus counters move only after the status write
succeeds — so the counters only ever describe facts that are already durable.

### Schedules that parse but can never run

"Parses successfully" is not the same as "can ever run". A Task that sits idle
forever with no explanation is the worst failure mode, so three cases are
rejected up front with `ScheduleValid=False` and a specific reason:
`ParseError`, `Unsatisfiable` (`0 0 30 2 *` — February 30th), and
`IntervalTooShort`, which is load-bearing rather than fussy: sub-minute slots
would collide in the deterministic Job name.

### Races the design has to survive

Lost status writes, a stale cache, a Job deleted out from under the controller, a
Task deleted mid-run, two replicas during leader-election hand-off, and a
controller restarted after hours of downtime. Each is handled by the three
mechanisms above plus a finalizer, owner-reference checks on UID, and a single
uncached read in the one case where status and cache imply opposite actions.

---

## Metrics

The full list is in the [README](../README.md#metrics): a total gauge, a
per-state gauge, and counters for successes, failures, Jobs created, skipped runs
and aborted runs — plus controller-runtime's built-ins
(`controller_runtime_reconcile_*`, `workqueue_*`) on the same endpoint, scraped
via a `ServiceMonitor`.

The gauges are computed at scrape time by listing the informer cache, rather than
maintained with `Inc()`/`Dec()` on transitions — a list cannot drift, needs no
cleanup for deleted Tasks, and costs microseconds. States are zero-filled so
alerts need no `absent()` trick. Cardinality is deliberate: no per-Task or
per-namespace labels, since that detail belongs in `kubectl get tk` and Events.

---

## RBAC and security

Least privilege, with a few choices worth naming:

- **No `update` on Jobs** — the controller creates and deletes Jobs; it does not
  enforce their spec.
- **`update`/`patch` on Tasks is for the finalizer only.** Spec is the user's;
  status has its own subresource.
- **No access to Pods.** Running-vs-pending is read from `Job.status.ready`,
  which removes an informer and a permission.
- **No Secrets, and no `create`/`delete` on Tasks.**
- Leader-election leases are a namespaced Role, not a ClusterRole.

The controller runs distroless and nonroot with a read-only root filesystem and
all capabilities dropped; Task pods run as UID 65534 with no service-account
token mounted, compatible with Pod Security Admission `restricted`.

**Trust boundary:** creating a Task is equivalent to running arbitrary commands
in that namespace — the same power as `create jobs` — so Task RBAC belongs only
where a tenant could already create Jobs.

---

## Trade-offs

- **Delete Jobs immediately vs keep history.** The exercise asks for deletion, so
  pod logs are gone once the outcome is recorded. `status.recentRuns` and Events
  preserve the history that matters without keeping API objects around.
- **Cron validated in the controller, not at admission.** A validating webhook
  would drag cert-manager into a local demo for a check the controller can do.
- **A finalizer, despite the cost.** Finalizers risk stuck deletions; owner
  references alone would give the controller no say in teardown.
- **Scale.** Work is per-key and cache-backed, and the cache is scoped by label
  to this controller's own Jobs. The default client and workqueue rate limits are
  raised and exposed as flags.

## Out of scope (named, not built)

Admission webhook for cron validation, `concurrencyPolicy: Allow`/`Replace`,
`timeZone`, Job history retention beyond `recentRuns`, multi-container tasks, and
sharding across controller replicas.
