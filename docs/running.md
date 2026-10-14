# Running this codebase

Every command is a Make target; `make help` lists them all. Prerequisites
(Docker, kind, kubectl, Helm, Go) are in [setup.md](setup.md).

## Bring it up

```bash
make up
```

That one target creates the kind cluster, installs kube-prometheus-stack, builds
and loads the image, deploys the controller with the dev overlay, and applies the
sample Tasks.

## Watch it work

```bash
kubectl get tk -A -w         # the main view
make demo                    # Tasks, Jobs, and a run history
```

Within about two minutes all four samples reach their states:

```
NAMESPACE   NAME       SCHEDULE      SUSPEND   STATE       LAST EXECUTION   NEXT                   SUCCEEDED   FAILED   AGE
demo        bad-cron   0 0 30 2 *              Failed                                                                   4m
demo        fail       * * * * *               Failed      23s              2026-10-13T03:09:00Z               20       4m
demo        hello      * * * * *               Completed   23s              2026-10-13T03:09:00Z   20                   4m
demo        slow       */2 * * * *             Running     23s              2026-10-13T03:10:00Z   4                    4m
```

The four samples cover the happy path, a failing command, a long-running command
under the Forbid concurrency policy, and a cron expression that is valid but can
never fire. Finished Jobs are deleted; each Task keeps its own run history in
`status.recentRuns`.

## Metrics

```bash
make metrics                 # port-forward + print the task_controller_* metrics
make prometheus-ui           # Prometheus UI on http://localhost:9090
```

## Tests

```bash
make test        # unit + envtest (downloads envtest binaries on first run)
make test-e2e    # end-to-end against the kind cluster
make lint        # golangci-lint
```

`make test` is fast: the unit suite drives all time-based behaviour with a fake
clock, so the full scheduling matrix runs in milliseconds.

> `make test-e2e` deploys the default (secure `:8443`) overlay and so replaces a
> `make up` deployment. Re-run `make deploy-dev` afterwards.

## Iterating on the code

```bash
make redeploy    # rebuild, reload, rollout restart
kubectl -n task-controller-system logs -f deploy/task-controller-controller-manager
make manifests generate   # after editing api/v1alpha1/task_types.go or any marker
```

## Deploying outside the demo

`config/dev` is for local work only — it serves metrics over plain HTTP with no
authentication. Anywhere else, use the default overlay, which serves metrics over
HTTPS on `:8443` behind authn/authz:

```bash
make docker-build docker-push IMG=<registry>/task-controller:<tag>
make deploy IMG=<registry>/task-controller:<tag>
```

## Cleanup

```bash
make down                  # remove samples + controller, cluster stays
make cleanup-test-e2e      # delete the kind cluster
```
