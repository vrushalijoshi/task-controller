/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/robfig/cron/v3"
	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
	"github.com/vrushalijoshi/task-controller/internal/metrics"
)

// jobOwnerKey indexes Jobs by their controller owner, so a Task's Jobs can be
// listed from the cache without a label selector scan.
const jobOwnerKey = ".metadata.controller"

// minRequeue is the floor for any requeue delay.
//
// ctrl.Result{RequeueAfter: d} with d <= 0 does not mean "requeue immediately":
// controller-runtime treats it as no requeue at all and forgets the item, so a
// Task whose computed delay came out zero or negative would never wake again.
// Every non-terminal return clamps through this.
const minRequeue = 100 * time.Millisecond

// Event reasons.
const (
	eventJobCreated      = "JobCreated"
	eventRunSucceeded    = "RunSucceeded"
	eventRunFailed       = "RunFailed"
	eventRunAborted      = "RunAborted"
	eventJobDisappeared  = "JobDisappeared"
	eventSlotSkipped     = "SlotSkipped"
	eventTooManyMissed   = "TooManyMissed"
	eventInvalidSchedule = "InvalidSchedule"
)

// TaskReconciler reconciles a Task object.
type TaskReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// APIReader is an uncached reader straight to the API server. It is used
	// for exactly one ambiguity (see step 3) and deliberately nowhere else:
	// routine uncached reads would put a per-reconcile load on the API server
	// that the cache exists to avoid.
	APIReader client.Reader

	Recorder events.EventRecorder

	// Clock is injected so time-based behaviour can be driven by a fake clock
	// in tests, which runs the whole scheduling matrix in milliseconds.
	Clock clock.Clock

	Metrics *metrics.Recorder

	//defines how many simultaneous reconciliation loops can run for a single controller
	MaxConcurrentReconciles int

	// ReconcileQPS and ReconcileBurst size the workqueue's rate limiter.
	//
	// controller-runtime's default limiter caps the whole controller at 10
	// reconciles/second -- the bucket is global to the queue, not per item -- so
	// at tens of thousands of Tasks a full sweep would take over an hour.
	ReconcileQPS   float64
	ReconcileBurst int
}

// +kubebuilder:rbac:groups=custom.io,resources=tasks,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=custom.io,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=custom.io,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile converges one Task.
//
// It is level-triggered: it never trusts which event woke it, re-derives the
// whole picture from (Task, owned Jobs, clock) every pass, and writes status at
// most once. That single write is the commit point, so a pass that fails part
// way can never leave status half-updated.
//
// The guarantee is: at most one Job per cron slot, and each finished Job
// counted exactly once in status.
func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var task customv1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		// NotFound means the Task is gone and its finalizer already drained.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Step 0: finalize or adopt.
	if task.DeletionTimestamp != nil {
		return r.finalizeTask(ctx, &task)
	}
	if !controllerutil.ContainsFinalizer(&task, customv1alpha1.CleanupFinalizer) {
		// Added before any Job can exist, which closes the window where a Task
		// is deleted mid-creation and leaves an orphan.
		patch := client.MergeFrom(task.DeepCopy())
		controllerutil.AddFinalizer(&task, customv1alpha1.CleanupFinalizer)
		if err := r.Patch(ctx, &task, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("adding finalizer: %w", err)
		}
		log.Info("Added finalizer to Task")
		// Requeue explicitly rather than relying on the watch event from this
		// patch. Adding a finalizer changes only metadata, so the resulting
		// update carries the same generation, is not a deletion, and now does
		// have the finalizer -- which means taskPredicate filters it out and
		// nothing would wake this Task until its spec changed. Driving the next
		// pass from here keeps the behaviour independent of predicate subtleties.
		return ctrl.Result{RequeueAfter: minRequeue}, nil
	}

	// patchBase is a full copy of the object as fetched. The status patch is
	// computed as the diff between it and the mutated task, which yields only
	// the status fields that actually changed.
	//
	// It must be a copy of the whole Task, not a synthetic object holding only
	// status: a merge patch against a partial base also emits every field the
	// base is missing, including metadata.resourceVersion -- which silently
	// turns every status write into an optimistically locked one and makes it
	// fail with a conflict whenever anything else touched the object.
	patchBase := task.DeepCopy()
	originalStatus := task.Status.DeepCopy()
	now := r.Clock.Now()

	// Step 1: observe.
	active, finished, err := r.observeJobs(ctx, &task)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Step 2: record outcomes, exactly once.
	outcomes := r.recordOutcomes(&task, finished)

	// Step 3: derive state.
	if err := r.deriveState(ctx, &task, active, outcomes); err != nil {
		return ctrl.Result{}, err
	}

	// Step 4: schedule. A failure to create the Job is held rather than returned
	// immediately, so that outcomes already recorded in recordOutcome are still
	// persisted below; the error is returned at the end so the workqueue retries
	// the slot.
	sched, scheduleResult, scheduleErr := r.scheduleSlot(ctx, &task, active, finished, now)

	// Step 5: persist. Counters move only after this succeeds, so they only
	// ever count facts that are already durable.
	task.Status.ObservedGeneration = task.Generation
	if !equality.Semantic.DeepEqual(originalStatus, &task.Status) {
		if err := r.Status().Patch(ctx, &task, client.MergeFrom(patchBase)); err != nil {
			// Nothing is counted and nothing is deleted; the retry re-derives.
			return ctrl.Result{}, fmt.Errorf("patching status: %w", err)
		}
	}
	outcomes.publish(r.Metrics)
	scheduleResult.publish(r.Metrics)

	// Step 6: clean up, now that the outcomes are durable.
	if err := r.cleanupJobs(ctx, finished); err != nil {
		return ctrl.Result{}, err
	}

	// Step 7: requeue at the next slot. This is the scheduler; there is no
	// timer goroutine.
	if scheduleErr != nil {
		// Status and cleanup are already done; retry the slot with backoff.
		return ctrl.Result{}, scheduleErr
	}
	if scheduleResult.terminal || sched == nil {
		// Invalid schedule or suspended: only a spec edit can change the
		// outcome, and that wakes us through GenerationChangedPredicate.
		return ctrl.Result{}, nil
	}
	return ctrl.Result{RequeueAfter: r.requeueDelay(&task, now)}, nil
}

// requeueDelay returns how long until the next slot.
//
// The 100ms offset avoids waking up early: the delay is measured on a
// monotonic timer while cron slots are wall-clock instants, so a small clock
// adjustment is enough to land just before the slot, find nothing due, and burn
// a no-op reconcile. At scale that is a wasted pass per Task per slot.
func (r *TaskReconciler) requeueDelay(task *customv1alpha1.Task, now time.Time) time.Duration {
	if task.Status.NextScheduleTime == nil {
		return minRequeue
	}
	d := task.Status.NextScheduleTime.Sub(now) + minRequeue
	if d < minRequeue {
		return minRequeue
	}
	return d
}

// observeJobs lists the Task's Jobs from the cache and splits them into active
// and finished, newest slot last.
func (r *TaskReconciler) observeJobs(
	ctx context.Context,
	task *customv1alpha1.Task,
) (active, finished []jobWithSlot, err error) {
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs,
		client.InNamespace(task.Namespace),
		client.MatchingFields{jobOwnerKey: string(task.UID)},
	); err != nil {
		return nil, nil, fmt.Errorf("listing Jobs: %w", err)
	}

	log := logf.FromContext(ctx)
	for i := range jobs.Items {
		job := &jobs.Items[i]
		// Guards against a Task deleted and recreated with the same name:
		// ownership is by UID, not by name.
		if !metav1.IsControlledBy(job, task) {
			continue
		}
		slot, err := JobSlot(job)
		if err != nil {
			log.Error(err, "Skipped Job with an unreadable schedule annotation", "job", job.Name)
			continue
		}
		done, succeeded, message := JobOutcome(job)
		entry := jobWithSlot{job: job, slot: slot, succeeded: succeeded, message: message}
		if done {
			finished = append(finished, entry)
		} else {
			active = append(active, entry)
		}
	}

	slices.SortFunc(active, func(a, b jobWithSlot) int { return a.slot.Compare(b.slot) })
	slices.SortFunc(finished, func(a, b jobWithSlot) int { return a.slot.Compare(b.slot) })
	return active, finished, nil
}

// jobWithSlot pairs a Job with the cron slot it belongs to.
type jobWithSlot struct {
	job       *batchv1.Job
	slot      time.Time
	succeeded bool
	message   string
}

// outcomeTally accumulates what step 2 recorded, so the counters can be moved
// in one place after the status write succeeds.
type outcomeTally struct {
	succeeded int
	failed    int
	aborted   map[string]int
	recorded  bool
}

func (t *outcomeTally) publish(m *metrics.Recorder) {
	if m == nil {
		return
	}
	for range t.succeeded {
		m.Successes.Inc()
	}
	for range t.failed {
		m.Failures.Inc()
	}
	for reason, n := range t.aborted {
		for range n {
			m.RunsAborted.WithLabelValues(reason).Inc()
		}
	}
}

func (t *outcomeTally) abort(reason string) {
	if t.aborted == nil {
		t.aborted = map[string]int{}
	}
	t.aborted[reason]++
}

// recordOutcomes folds each finished Job into status exactly once.
func (r *TaskReconciler) recordOutcomes(
	task *customv1alpha1.Task,
	finished []jobWithSlot,
) *outcomeTally {
	tally := &outcomeTally{}

	for _, f := range finished {
		// The idempotency key. If delete failed and this Job is seen
		// again, it is skipped rather than counted twice.
		if key := task.Status.LastFinishedScheduleTime; key != nil && !f.slot.After(key.Time) {
			continue
		}

		result := customv1alpha1.TaskRunSucceeded
		state := customv1alpha1.TaskStateCompleted
		eventType, reason := corev1.EventTypeNormal, eventRunSucceeded
		if f.succeeded {
			task.Status.SucceededCount++
			tally.succeeded++
		} else {
			result = customv1alpha1.TaskRunFailed
			state = customv1alpha1.TaskStateFailed
			eventType, reason = corev1.EventTypeWarning, eventRunFailed
			task.Status.FailedCount++
			tally.failed++
		}

		task.Status.State = state
		task.Status.Message = truncate(fmt.Sprintf("Job %s %s", f.job.Name, lowerResult(result)))
		task.Status.RecentRuns = prependRun(task.Status.RecentRuns,
			NewTaskRun(f.job, f.slot, result, f.message))
		slot := metav1.NewTime(f.slot.UTC())
		task.Status.LastFinishedScheduleTime = &slot
		if f.job.Status.CompletionTime != nil {
			task.Status.LastCompletionTime = f.job.Status.CompletionTime.DeepCopy()
		} else {
			now := metav1.NewTime(r.Clock.Now().UTC())
			task.Status.LastCompletionTime = &now
		}

		// Clearing activeJob here is not optional. If it is left pointing at a
		// Job is about to delete, the next pass finds a name the
		// cache cannot resolve, takes the api server check NotFound branch, and records a
		// bogus Aborted run over a job that actually succeeded. A fast command
		// such as `echo` hits that on essentially every run.
		if task.Status.ActiveJob == f.job.Name {
			task.Status.ActiveJob = ""
		}

		r.event(task, eventType, reason, "RecordOutcome", "Job %s for slot %s %s",
			f.job.Name, f.slot.UTC().Format(time.RFC3339), lowerResult(result))
		tally.recorded = true
	}

	return tally
}

func lowerResult(r customv1alpha1.TaskRunResult) string {
	switch r {
	case customv1alpha1.TaskRunSucceeded:
		return "succeeded"
	case customv1alpha1.TaskRunFailed:
		return "failed"
	default:
		return "was aborted"
	}
}

// deriveState sets status.state from the observed Jobs.
//
// Precedence is the whole subtlety here, so it is explicit rather than implied
// by ordering: an active Job wins over an outcome just recorded by recordOutcomes,
// because it is the more recent execution; otherwise a recorded outcome stands;
// otherwise a Task that has never run is Pending.
func (r *TaskReconciler) deriveState(
	ctx context.Context,
	task *customv1alpha1.Task,
	active []jobWithSlot,
	tally *outcomeTally,
) error {
	if len(active) > 0 {
		newest := active[len(active)-1]
		task.Status.ActiveJob = newest.job.Name
		if JobIsRunning(newest.job) {
			task.Status.State = customv1alpha1.TaskStateRunning
		} else {
			// A fast command finishes before the Job ever reports a ready pod,
			// so it goes Pending -> Completed and never shows Running. Running
			// is reported only when it is actually observed.
			task.Status.State = customv1alpha1.TaskStatePending
		}
		return nil
	}

	// No active Job in the cache. If status still names one, resolve the
	// ambiguity rather than guessing: guessing one way strands the Task in
	// Running forever, the other way declares a live run dead.
	if task.Status.ActiveJob != "" {
		return r.resolveMissingActiveJob(ctx, task, tally)
	}

	if tally.recorded {
		// recordOutcomes set the state; do not stomp it back to Pending.
		return nil
	}
	if task.Status.State == "" {
		task.Status.State = customv1alpha1.TaskStatePending
	}
	return nil
}

// resolveMissingActiveJob handles status naming a Job the cache cannot see.
func (r *TaskReconciler) resolveMissingActiveJob(
	ctx context.Context,
	task *customv1alpha1.Task,
	tally *outcomeTally,
) error {
	log := logf.FromContext(ctx)
	name := task.Status.ActiveJob

	// Check the idempotency key before spending an uncached read. If this
	// run's slot has already been recorded, the Job is simply one that got
	// deleted, and there is nothing to resolve. This is what keeps an instant
	// `echo` Task from logging a false Aborted on every single run, and it
	// holds even if the recordOutcomes clear was lost to a failed status write.
	if task.Status.LastScheduleTime != nil && task.Status.LastFinishedScheduleTime != nil &&
		!task.Status.LastScheduleTime.After(task.Status.LastFinishedScheduleTime.Time) {
		task.Status.ActiveJob = ""
		return nil
	}

	var job batchv1.Job
	err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: task.Namespace, Name: name}, &job)
	switch {
	case err == nil:
		// The cache is merely lagging our own Create. We already hold the
		// authoritative object, so derive state from it now rather than
		// requeueing and hoping the cache has caught up.
		if JobIsRunning(&job) {
			task.Status.State = customv1alpha1.TaskStateRunning
		} else {
			task.Status.State = customv1alpha1.TaskStatePending
		}
		return nil

	case apierrors.IsNotFound(err):
		// Deleted out from under us. Recover the slot from the Job's own name
		// rather than from status.lastScheduleTime: that anchor also advances on
		// skipped slots, so after a Forbid skip it names a slot this Job never
		// belonged to -- which would both mis-record the history entry and
		// advance the idempotency key past a slot that never ran.
		var slot *metav1.Time
		if parsed, ok := SlotFromJobName(task.Name, name); ok {
			t := metav1.NewTime(parsed)
			slot = &t
		} else if task.Status.LastScheduleTime != nil {
			slot = task.Status.LastScheduleTime
		} else {
			now := metav1.NewTime(r.Clock.Now().UTC())
			slot = &now
		}
		task.Status.State = customv1alpha1.TaskStateFailed
		task.Status.Message = truncate(fmt.Sprintf("Job %s was deleted before it finished", name))
		task.Status.RecentRuns = prependRun(task.Status.RecentRuns, customv1alpha1.TaskRun{
			ScheduledTime: *slot,
			JobName:       name,
			Result:        customv1alpha1.TaskRunAborted,
			Message:       "Job was deleted before it finished",
		})
		task.Status.LastFinishedScheduleTime = slot
		task.Status.ActiveJob = ""
		tally.abort(metrics.ReasonDeletedExternally)
		r.event(task, corev1.EventTypeWarning, eventJobDisappeared, "RecordOutcome",
			"Job %s was deleted before it finished", name)
		log.Info("Active Job disappeared", "job", name)
		return nil

	default:
		return fmt.Errorf("confirming active Job %s: %w", name, err)
	}
}

// scheduleOutcome records what step 4 decided, so its counters can be moved
// after the status write.
type scheduleOutcome struct {
	created  bool
	skipped  map[string]int
	terminal bool // invalid schedule or suspended: do not requeue
}

func (s *scheduleOutcome) publish(m *metrics.Recorder) {
	if m == nil {
		return
	}
	if s.created {
		m.JobsCreated.Inc()
	}
	for reason, n := range s.skipped {
		for range n {
			m.RunsSkipped.WithLabelValues(reason).Inc()
		}
	}
}

func (s *scheduleOutcome) skip(reason string) {
	if s.skipped == nil {
		s.skipped = map[string]int{}
	}
	s.skipped[reason]++
}

// scheduleSlot parses the schedule, decides which slot is due, and acts on it.
func (r *TaskReconciler) scheduleSlot(
	ctx context.Context,
	task *customv1alpha1.Task,
	active, finished []jobWithSlot,
	now time.Time,
) (cron.Schedule, *scheduleOutcome, error) {
	log := logf.FromContext(ctx)
	out := &scheduleOutcome{}

	sched, err := ValidateSchedule(task.Spec.Schedule, now)
	if err != nil {
		se, _ := AsScheduleError(err)
		reason := string(ReasonParseError)
		if se != nil {
			reason = string(se.Reason)
		}
		r.setCondition(task, metav1.ConditionFalse, reason, err.Error())
		task.Status.State = customv1alpha1.TaskStateFailed
		task.Status.Message = truncate(err.Error())
		// Leaving a stale nextScheduleTime would promise a run that can never
		// happen.
		task.Status.NextScheduleTime = nil
		r.event(task, corev1.EventTypeWarning, eventInvalidSchedule, "ValidateSchedule", "%s", err.Error())
		log.Info("Rejected schedule", "schedule", task.Spec.Schedule, "reason", reason)
		out.terminal = true
		return nil, out, nil
	}
	r.setCondition(task, metav1.ConditionTrue, "Valid", "Schedule parses and can run")

	if task.Spec.Suspend != nil && *task.Spec.Suspend {
		task.Status.NextScheduleTime = nil
		out.terminal = true
		return sched, out, nil
	}

	// The anchor is the last slot acted on, or the creation time for a Task
	// that has never run, so a Task created at 10:02 with */5 first runs at
	// 10:05 rather than immediately.
	anchor := task.CreationTimestamp.Time
	if task.Status.LastScheduleTime != nil {
		anchor = task.Status.LastScheduleTime.Time
	}

	decision, err := DueSlot(sched, anchor, now, task.Spec.StartingDeadlineSeconds)
	if err != nil {
		// A pathological anchor: reset it rather than walking forever.
		reset := metav1.NewTime(now.UTC())
		task.Status.LastScheduleTime = &reset
		r.event(task, corev1.EventTypeWarning, eventTooManyMissed, "ResetAnchor",
			"More than %d missed slots; schedule anchor reset to now", maxMissedSlots)
		log.Info("Reset schedule anchor after too many missed slots")
		decision = SlotDecision{}
	}

	var slotErr error
	switch {
	case decision.Due:
		slotErr = r.actOnSlot(ctx, task, active, finished, decision.Slot, out)
	case decision.SkippedByDeadline:
		// A slot was genuinely due but startingDeadlineSeconds had elapsed.
		// Counted and evented so the drop is visible rather than silent.
		out.skip(metrics.ReasonStartingDeadline)
		r.event(task, corev1.EventTypeWarning, eventSlotSkipped, "SkipSlot",
			"Skipped a slot missed by more than startingDeadlineSeconds (%ds)",
			*task.Spec.StartingDeadlineSeconds)
	}

	next := sched.Next(now)
	if !next.IsZero() {
		n := metav1.NewTime(next.UTC())
		task.Status.NextScheduleTime = &n
	}
	return sched, out, slotErr
}

// actOnSlot creates, adopts or skips the run for a due slot.
func (r *TaskReconciler) actOnSlot(
	ctx context.Context,
	task *customv1alpha1.Task,
	active, finished []jobWithSlot,
	slot time.Time,
	out *scheduleOutcome,
) error {
	log := logf.FromContext(ctx)
	name := JobName(task, slot)
	markScheduled := func() {
		s := metav1.NewTime(slot.UTC())
		task.Status.LastScheduleTime = &s
	}

	// A Job for this exact slot already exists: the previous pass created it
	// but its status write was lost. Adopt it rather than creating again.
	for _, a := range active {
		if a.job.Name == name {
			markScheduled()
			task.Status.ActiveJob = name
			// Deliberately logged rather than recorded as an Event. Adoption
			// happens both when recovering a lost status write and, far more
			// often, when this pass simply read our own status before the cache
			// caught up with it. The two are indistinguishable here, and Owns()
			// fires several times per Job lifecycle, so an Event each time would
			// be noise proportional to Job churn -- and Events are API writes.
			log.V(1).Info("Adopted existing Job for slot", "name", name,
				"slot", slot.UTC().Format(time.RFC3339))
			return nil
		}
	}
	// Already finished and recorded: the slot is done, just advance the anchor.
	for _, f := range finished {
		if f.job.Name == name {
			markScheduled()
			return nil
		}
	}

	// Concurrency policy is Forbid: runs of one Task never overlap.
	if len(active) > 0 {
		markScheduled()
		out.skip(metrics.ReasonConcurrencyForbid)
		r.event(task, corev1.EventTypeWarning, eventSlotSkipped, "SkipSlot",
			"Skipped slot %s because Job %s is still running",
			slot.UTC().Format(time.RFC3339), active[len(active)-1].job.Name)
		return nil
	}

	job, err := BuildJob(task, slot, r.Scheme)
	if err != nil {
		return fmt.Errorf("building Job for slot %s: %w", slot.UTC().Format(time.RFC3339), err)
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// The deterministic name made Create idempotent: a duplicate
			// reconcile or a leader hand-off cannot cause a second run.
			//
			// Confirm we actually own it first. Job names key on Task name, but
			// ownership is by UID, so a Job left behind by a same-named
			// predecessor would otherwise be adopted here and reported as this
			// Task's run.
			var existing batchv1.Job
			if getErr := r.APIReader.Get(ctx,
				types.NamespacedName{Namespace: task.Namespace, Name: name}, &existing); getErr == nil {
				if !metav1.IsControlledBy(&existing, task) {
					return fmt.Errorf(
						"job %s already exists but is not controlled by this Task; "+
							"it is left over from a previous Task of the same name", name)
				}
			}
			markScheduled()
			task.Status.ActiveJob = name
			return nil
		}
		// Returned, not swallowed: without this the slot is lost outright, with
		// no retry and nothing but a log line to show for it.
		return fmt.Errorf("creating Job %s for slot %s: %w",
			name, slot.UTC().Format(time.RFC3339), err)
	}

	markScheduled()
	execTime := metav1.NewTime(slot.UTC())
	task.Status.LastExecutionTime = &execTime
	task.Status.ActiveJob = name
	if task.Status.State != customv1alpha1.TaskStateRunning {
		task.Status.State = customv1alpha1.TaskStatePending
	}
	out.created = true
	r.event(task, corev1.EventTypeNormal, eventJobCreated, "CreateJob",
		"Created Job %s for slot %s", name, slot.UTC().Format(time.RFC3339))
	log.Info("Created Job", "name", name, "slot", slot.UTC().Format(time.RFC3339))
	return nil
}

// cleanupJobs deletes finished Jobs, only once their outcomes are durable.
func (r *TaskReconciler) cleanupJobs(ctx context.Context, finished []jobWithSlot) error {
	log := logf.FromContext(ctx)
	policy := metav1.DeletePropagationBackground
	for _, f := range finished {
		if !f.job.DeletionTimestamp.IsZero() {
			continue
		}
		if err := r.Delete(ctx, f.job, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("deleting Job %s: %w", f.job.Name, err)
		}
		log.Info("Deleted finished Job", "name", f.job.Name)
	}
	return nil
}

// finalizeTask tears a deleting Task down gracefully.
//
// Owner references alone would let the garbage collector delete the Jobs, but
// asynchronously and with no say from the controller: an in-flight run would
// vanish with no final status, no Event and no history entry. This records what
// happened first, then deletes, then releases the Task.
func (r *TaskReconciler) finalizeTask(
	ctx context.Context,
	task *customv1alpha1.Task,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	if !controllerutil.ContainsFinalizer(task, customv1alpha1.CleanupFinalizer) {
		return ctrl.Result{}, nil
	}

	patchBase := task.DeepCopy()
	originalStatus := task.Status.DeepCopy()
	active, finished, err := r.observeJobs(ctx, task)
	if err != nil {
		return ctrl.Result{}, err
	}

	// A run that completed just before the delete is still counted.
	tally := r.recordOutcomes(task, finished)

	// Cut short anything still running, with a record of why.
	//
	// Guarded by the same idempotency key as recordOutcomes. Finalization
	// requeues every second while a Job remains, and Owns() wakes it on each Job
	// status update, so a Job that is deleting but still visible in the cache
	// would otherwise be recorded, counted and evented on every one of those
	// passes.
	for _, a := range active {
		if key := task.Status.LastFinishedScheduleTime; key != nil && !a.slot.After(key.Time) {
			continue
		}
		task.Status.RecentRuns = prependRun(task.Status.RecentRuns,
			NewTaskRun(a.job, a.slot, customv1alpha1.TaskRunAborted, "Task deleted"))
		slot := metav1.NewTime(a.slot.UTC())
		task.Status.LastFinishedScheduleTime = &slot
		tally.abort(metrics.ReasonTaskDeleted)
		r.event(task, corev1.EventTypeWarning, eventRunAborted, "AbortRun",
			"Aborted Job %s because the Task is being deleted", a.job.Name)
	}
	task.Status.ActiveJob = ""

	if !equality.Semantic.DeepEqual(originalStatus, &task.Status) {
		if err := r.Status().Patch(ctx, task, client.MergeFrom(patchBase)); err != nil {
			return ctrl.Result{}, fmt.Errorf("patching status during finalization: %w", err)
		}
	}
	tally.publish(r.Metrics)

	policy := metav1.DeletePropagationBackground
	remaining := 0
	for _, j := range append(append([]jobWithSlot{}, active...), finished...) {
		if !j.job.DeletionTimestamp.IsZero() {
			remaining++
			continue
		}
		if err := r.Delete(ctx, j.job, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return ctrl.Result{}, fmt.Errorf("deleting Job %s during finalization: %w", j.job.Name, err)
		}
		remaining++
	}

	// Release the Task only once every Job it owned is confirmed gone, so
	// `kubectl delete tk` returning means the Jobs really are deleted rather
	// than racing the garbage collector.
	if remaining > 0 {
		log.Info("Waiting for owned Jobs to be deleted", "count", remaining)
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	patch := client.MergeFrom(task.DeepCopy())
	controllerutil.RemoveFinalizer(task, customv1alpha1.CleanupFinalizer)
	if err := r.Patch(ctx, task, patch); err != nil {
		return ctrl.Result{}, fmt.Errorf("removing finalizer: %w", err)
	}
	log.Info("Finalized Task")
	return ctrl.Result{}, nil
}

// setCondition sets the ScheduleValid condition.
func (r *TaskReconciler) setCondition(
	task *customv1alpha1.Task,
	status metav1.ConditionStatus,
	reason, message string,
) {
	now := metav1.NewTime(r.Clock.Now())
	cond := metav1.Condition{
		Type:               customv1alpha1.ConditionScheduleValid,
		Status:             status,
		Reason:             reason,
		Message:            truncate(message),
		ObservedGeneration: task.Generation,
	}
	for i := range task.Status.Conditions {
		if task.Status.Conditions[i].Type != cond.Type {
			continue
		}
		existing := &task.Status.Conditions[i]
		if existing.Status == cond.Status && existing.Reason == cond.Reason &&
			existing.Message == cond.Message && existing.ObservedGeneration == cond.ObservedGeneration {
			return
		}
		// LastTransitionTime only moves when the status actually flips.
		if existing.Status == cond.Status {
			cond.LastTransitionTime = existing.LastTransitionTime
		} else {
			cond.LastTransitionTime = now
		}
		task.Status.Conditions[i] = cond
		return
	}
	cond.LastTransitionTime = now
	task.Status.Conditions = append(task.Status.Conditions, cond)
}

// event emits an Event if a recorder is configured.
//
// The action argument of the events.k8s.io API is the verb describing what the
// controller did; reason stays the machine-readable cause, as on the legacy API.
func (r *TaskReconciler) event(
	task *customv1alpha1.Task,
	eventType, reason, action, format string,
	args ...any,
) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(task, nil, eventType, reason, action, format, args...)
}

// taskPredicate decides which Task events wake the controller.
//
// GenerationChangedPredicate alone is not safe once a finalizer is involved. It
// passes only events where metadata.generation changed, which covers spec edits
// and the deletion itself, but not metadata-only updates -- and adding or
// removing a finalizer is exactly that. The finalizer patch in step 0 would be
// filtered out and the Task would sit unreconciled until its next slot.
//
// The second branch therefore passes every event for a Task that is terminating
// or whose finalizer is not yet set. Status writes still do not re-trigger us,
// because they change neither generation nor either of those conditions.
func taskPredicate() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.NewPredicateFuncs(func(o client.Object) bool {
			return o.GetDeletionTimestamp() != nil ||
				!controllerutil.ContainsFinalizer(o, customv1alpha1.CleanupFinalizer)
		}),
	)
}

// SetupIndexes registers the owner field index. It is shared by the manager and
// the tests so there is exactly one definition of the index.
func SetupIndexes(ctx context.Context, idx client.FieldIndexer) error {
	return idx.IndexField(ctx, &batchv1.Job{}, jobOwnerKey, func(o client.Object) []string {
		owner := metav1.GetControllerOf(o)
		if owner == nil || owner.Kind != "Task" {
			return nil
		}
		return []string{string(owner.UID)}
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Clock == nil {
		r.Clock = clock.RealClock{}
	}
	if r.MaxConcurrentReconciles <= 0 {
		r.MaxConcurrentReconciles = 4
	}
	if r.ReconcileQPS <= 0 {
		r.ReconcileQPS = 500
	}
	if r.ReconcileBurst <= 0 {
		r.ReconcileBurst = 1000
	}
	if err := SetupIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&customv1alpha1.Task{}, builder.WithPredicates(taskPredicate())).
		// Wakes the Task when a pod becomes ready, a Job finishes, or a Job is
		// deleted -- including during finalization, so teardown does not wait
		// on a timer.
		Owns(&batchv1.Job{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: r.MaxConcurrentReconciles,
			// The per-item exponential backoff is kept for failures; only the
			// queue-wide bucket is raised off its 10 qps default.
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter(
				workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
					5*time.Millisecond, 1000*time.Second),
				&workqueue.TypedBucketRateLimiter[reconcile.Request]{
					Limiter: rate.NewLimiter(rate.Limit(r.ReconcileQPS), r.ReconcileBurst),
				},
			),
		}).
		Named("task").
		Complete(r)
}
