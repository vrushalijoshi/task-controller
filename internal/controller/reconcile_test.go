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
	"encoding/json"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
	"github.com/vrushalijoshi/task-controller/internal/metrics"
)

// slotTime is the first */5 slot after `base` (17:02), i.e. 17:05.
var slotTime = time.Date(2026, 10, 7, 17, 5, 0, 0, time.UTC)

// The finalizer must be added before any Job can exist, which closes the window
// where a Task is deleted mid-creation and leaves an orphan.
func TestFinalizerAddedBeforeAnyJob(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	task.Finalizers = nil
	f := newFixture(t, base, task)

	f.reconcile("hello")

	got := f.task("hello")
	if !controllerutil.ContainsFinalizer(got, customv1alpha1.CleanupFinalizer) {
		t.Fatal("first reconcile did not add the cleanup finalizer")
	}
	if n := len(f.jobs()); n != 0 {
		t.Errorf("created %d Jobs before the first slot; want 0", n)
	}
}

// The finalizer-add pass must requeue itself.
//
// Adding a finalizer changes only metadata, so the resulting watch event has an
// unchanged generation, is not a deletion, and does have the finalizer, which
// means taskPredicate filters it out. Without an explicit requeue the Task would
// stall after its finalizer was added and never schedule anything.
func TestFinalizerAddRequeuesItself(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	task.Finalizers = nil
	f := newFixture(t, slotTime, task)

	res := f.reconcile("hello")
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %v after adding the finalizer; the Task would stall", res.RequeueAfter)
	}

	// The requeued pass must then do the real work.
	f.reconcile("hello")
	if n := len(f.jobs()); n != 1 {
		t.Errorf("created %d Jobs on the pass after the finalizer was added; want 1", n)
	}
}

// taskPredicate must pass the events the finalizer flow depends on, and still
// filter the controller's own status writes.
func TestTaskPredicate(t *testing.T) {
	pred := taskPredicate()

	withFinalizer := newTask("hello", "echo hi", everyFiveMinutes)
	withoutFinalizer := withFinalizer.DeepCopy()
	withoutFinalizer.Finalizers = nil
	deleting := withFinalizer.DeepCopy()
	now := metav1.NewTime(base)
	deleting.DeletionTimestamp = &now
	nextGeneration := withFinalizer.DeepCopy()
	nextGeneration.Generation = 2

	tests := []struct {
		name string
		old  client.Object
		new  client.Object
		want bool
	}{
		{name: "spec edit", old: withFinalizer, new: nextGeneration, want: true},
		{name: "finalizer not yet set", old: withoutFinalizer, new: withoutFinalizer, want: true},
		{name: "terminating", old: withFinalizer, new: deleting, want: true},
		// The controller's own status writes must not re-trigger it, or every
		// status patch would schedule another reconcile.
		{name: "status-only update", old: withFinalizer, new: withFinalizer.DeepCopy(), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pred.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new})
			if got != tc.want {
				t.Errorf("predicate = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewTaskBeforeFirstSlot(t *testing.T) {
	f := newFixture(t, base, newTask("hello", "echo hi", everyFiveMinutes))

	res := f.reconcile("hello")

	task := f.task("hello")
	if task.Status.State != customv1alpha1.TaskStatePending {
		t.Errorf("state = %q, want Pending", task.Status.State)
	}
	if n := len(f.jobs()); n != 0 {
		t.Errorf("created %d Jobs before the slot was due; want 0", n)
	}
	if task.Status.NextScheduleTime == nil {
		t.Fatal("nextScheduleTime is unset")
	}
	if !task.Status.NextScheduleTime.Time.Equal(slotTime) {
		t.Errorf("nextScheduleTime = %v, want %v", task.Status.NextScheduleTime.Time, slotTime)
	}
	// A Task that is merely waiting must still be requeued, or it would never
	// wake again.
	if res.RequeueAfter <= 0 {
		t.Errorf("RequeueAfter = %v, want a positive delay", res.RequeueAfter)
	}
}

func TestSlotReachedCreatesOneJob(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo Hello World", everyFiveMinutes))

	f.reconcile("hello")

	jobs := f.jobs()
	if len(jobs) != 1 {
		t.Fatalf("created %d Jobs for one slot; want exactly 1", len(jobs))
	}
	job := jobs[0]
	task := f.task("hello")

	wantName := JobName(task, slotTime)
	if job.Name != wantName {
		t.Errorf("job name = %q, want the deterministic name %q", job.Name, wantName)
	}
	if task.Status.ActiveJob != wantName {
		t.Errorf("activeJob = %q, want %q", task.Status.ActiveJob, wantName)
	}
	if task.Status.State != customv1alpha1.TaskStatePending {
		t.Errorf("state = %q, want Pending", task.Status.State)
	}
	if task.Status.LastExecutionTime == nil {
		t.Error("lastExecutionTime is unset after creating a Job")
	}
	if task.Status.LastScheduleTime == nil || !task.Status.LastScheduleTime.Time.Equal(slotTime) {
		t.Errorf("lastScheduleTime = %v, want %v", task.Status.LastScheduleTime, slotTime)
	}
	if got := f.counter("task_controller_jobs_created_total", nil); got != 1 {
		t.Errorf("jobs_created_total = %v, want 1", got)
	}
}

func TestJobReadyShowsRunning(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 75", everyFiveMinutes))
	f.reconcile("slow")

	f.markJobReady(f.jobs()[0].Name)
	f.reconcile("slow")

	if got := f.task("slow").Status.State; got != customv1alpha1.TaskStateRunning {
		t.Errorf("state = %q, want Running once the Job reports a ready pod", got)
	}
}

func TestJobSucceeds(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	f.reconcile("hello")
	jobName := f.jobs()[0].Name

	f.finishJob(jobName, true)
	f.reconcile("hello")

	task := f.task("hello")
	if task.Status.State != customv1alpha1.TaskStateCompleted {
		t.Errorf("state = %q, want Completed", task.Status.State)
	}
	if task.Status.SucceededCount != 1 {
		t.Errorf("succeededCount = %d, want 1", task.Status.SucceededCount)
	}
	// The exercise requires finished Jobs to be deleted to avoid resource bloat.
	if n := len(f.jobs()); n != 0 {
		t.Errorf("%d Jobs remain after the outcome was recorded; want 0", n)
	}
	if len(task.Status.RecentRuns) != 1 {
		t.Fatalf("recentRuns has %d entries, want 1", len(task.Status.RecentRuns))
	}
	if got := task.Status.RecentRuns[0].Result; got != customv1alpha1.TaskRunSucceeded {
		t.Errorf("recentRuns[0].result = %q, want Succeeded", got)
	}
	if got := f.counter("task_controller_task_successes_total", nil); got != 1 {
		t.Errorf("successes_total = %v, want 1", got)
	}
}

func TestJobFails(t *testing.T) {
	f := newFixture(t, slotTime, newTask("fail", "exit 1", everyFiveMinutes))
	f.reconcile("fail")
	f.finishJob(f.jobs()[0].Name, false)
	f.reconcile("fail")

	task := f.task("fail")
	if task.Status.State != customv1alpha1.TaskStateFailed {
		t.Errorf("state = %q, want Failed", task.Status.State)
	}
	if task.Status.FailedCount != 1 {
		t.Errorf("failedCount = %d, want 1", task.Status.FailedCount)
	}
	if got := f.counter("task_controller_task_failures_total", nil); got != 1 {
		t.Errorf("failures_total = %v, want 1", got)
	}
}

// The idempotency key must make an outcome count exactly once even if the Job
// is seen again because the delete did not take effect.
func TestFinishedJobCountedExactlyOnceWhenDeleteFails(t *testing.T) {
	f := newFixture(t, slotTime, newTask("fail", "exit 1", everyFiveMinutes))
	f.reconcile("fail")
	jobName := f.jobs()[0].Name
	f.finishJob(jobName, false)

	// First pass records the outcome and deletes the Job.
	f.reconcile("fail")
	task := f.task("fail")
	if task.Status.FailedCount != 1 {
		t.Fatalf("failedCount after the first pass = %d, want 1", task.Status.FailedCount)
	}

	// Simulate the delete failing: the same finished Job is visible again.
	readdJob(t, f, task, jobName, slotTime, false)
	f.reconcile("fail")

	task = f.task("fail")
	if task.Status.FailedCount != 1 {
		t.Errorf("failedCount = %d after seeing the same Job twice; want 1", task.Status.FailedCount)
	}
	if got := f.counter("task_controller_task_failures_total", nil); got != 1 {
		t.Errorf("failures_total = %v after two passes; want 1, not 2", got)
	}
	if len(task.Status.RecentRuns) != 1 {
		t.Errorf("recentRuns has %d entries; want 1, the history must not duplicate either",
			len(task.Status.RecentRuns))
	}
}

// Concurrency policy is Forbid: runs of one Task never overlap.
func TestSlotDueWhileRunIsActive(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 600", everyFiveMinutes))
	f.reconcile("slow")
	firstJob := f.jobs()[0].Name
	f.markJobReady(firstJob)

	// Advance to the next slot while the first run is still active.
	f.advance(5 * time.Minute)
	f.reconcile("slow")

	if n := len(f.jobs()); n != 1 {
		t.Errorf("have %d Jobs; want 1, the overlapping slot must be skipped", n)
	}
	task := f.task("slow")
	if !task.Status.LastScheduleTime.Time.Equal(slotTime.Add(5 * time.Minute)) {
		t.Errorf("lastScheduleTime = %v, want the skipped slot to still advance the anchor",
			task.Status.LastScheduleTime)
	}
	got := f.counter("task_controller_task_runs_skipped_total",
		map[string]string{reasonLabel: metrics.ReasonConcurrencyForbid})
	if got != 1 {
		t.Errorf("runs_skipped_total{concurrency_forbid} = %v, want 1", got)
	}
}

// If the status write is lost after Create, the next pass must adopt the Job
// its deterministic name identifies rather than creating a second one.
func TestStatusWriteLostAfterCreate(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	f.reconcile("hello")
	jobName := f.jobs()[0].Name

	// Roll status back to before the write, leaving the Job in place.
	task := f.task("hello")
	task.Status = customv1alpha1.TaskStatus{}
	if err := f.client.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("resetting status: %v", err)
	}

	f.reconcile("hello")

	jobs := f.jobs()
	if len(jobs) != 1 {
		t.Fatalf("have %d Jobs after a lost status write; want exactly 1", len(jobs))
	}
	if jobs[0].Name != jobName {
		t.Errorf("job name = %q, want the original %q to have been adopted", jobs[0].Name, jobName)
	}
}

func TestStartingDeadlineExceeded(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	task.Spec.StartingDeadlineSeconds = ptrInt64(60)
	f := newFixture(t, base.Add(3*time.Hour), task)

	f.reconcile("hello")

	if n := len(f.jobs()); n != 0 {
		t.Errorf("created %d Jobs for a slot missed by more than the deadline; want 0", n)
	}
}

// Three hours of downtime must produce one run, not a burst.
func TestMissedSlotsCollapseIntoOneRun(t *testing.T) {
	f := newFixture(t, base.Add(3*time.Hour), newTask("hello", "echo hi", everyFiveMinutes))

	f.reconcile("hello")

	if n := len(f.jobs()); n != 1 {
		t.Errorf("created %d Jobs after a three-hour gap; want 1, missed slots collapse", n)
	}
}

func TestInvalidSchedules(t *testing.T) {
	tests := []struct {
		name       string
		schedule   string
		wantReason string
	}{
		{name: "parse error", schedule: tooFewCronFields, wantReason: string(ReasonParseError)},
		// Parses cleanly but can never fire. Without the check this Task would
		// sit idle forever with nothing reporting why.
		{name: "February 30th", schedule: "0 0 30 2 *", wantReason: string(ReasonUnsatisfiable)},
		// Would collide on the minute-granularity Job name.
		{name: "sub-minute", schedule: "@every 30s", wantReason: string(ReasonIntervalTooShort)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, slotTime, newTask("bad", "echo hi", tc.schedule))

			res := f.reconcile("bad")

			task := f.task("bad")
			if task.Status.State != customv1alpha1.TaskStateFailed {
				t.Errorf("state = %q, want Failed", task.Status.State)
			}
			cond := findCondition(task, customv1alpha1.ConditionScheduleValid)
			if cond == nil {
				t.Fatal("ScheduleValid condition is missing")
			}
			if cond.Status != metav1.ConditionFalse {
				t.Errorf("ScheduleValid = %q, want False", cond.Status)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", cond.Reason, tc.wantReason)
			}
			if n := len(f.jobs()); n != 0 {
				t.Errorf("created %d Jobs for an invalid schedule; want 0", n)
			}
			// A stale nextScheduleTime would promise a run that cannot happen.
			if task.Status.NextScheduleTime != nil {
				t.Errorf("nextScheduleTime = %v, want it cleared for an unusable schedule",
					task.Status.NextScheduleTime)
			}
			// Only a spec edit can fix this, and that wakes us via the predicate.
			if res.RequeueAfter != 0 {
				t.Errorf("RequeueAfter = %v, want no requeue for a terminal state", res.RequeueAfter)
			}
		})
	}
}

// The no-requeue path for an invalid schedule must not be a dead end.
func TestInvalidScheduleRecoversAfterEdit(t *testing.T) {
	f := newFixture(t, slotTime, newTask("bad", "echo hi", "0 0 30 2 *"))
	f.reconcile("bad")

	task := f.task("bad")
	task.Spec.Schedule = everyFiveMinutes
	task.Generation = 2
	if err := f.client.Update(context.Background(), task); err != nil {
		t.Fatalf("updating the schedule: %v", err)
	}

	f.reconcile("bad")

	task = f.task("bad")
	cond := findCondition(task, customv1alpha1.ConditionScheduleValid)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("ScheduleValid = %v, want True after the schedule was corrected", cond)
	}
	if len(f.jobs()) != 1 {
		t.Errorf("have %d Jobs; want scheduling to resume after the fix", len(f.jobs()))
	}
}

func TestSuspendedTaskCreatesNothing(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	suspend := true
	task.Spec.Suspend = &suspend
	f := newFixture(t, slotTime, task)

	res := f.reconcile("hello")

	if n := len(f.jobs()); n != 0 {
		t.Errorf("created %d Jobs for a suspended Task; want 0", n)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want no requeue while suspended", res.RequeueAfter)
	}
}

// A Task deleted and recreated with the same name must not adopt its
// predecessor's Jobs: ownership is by UID, not by name.
func TestRecreatedTaskDoesNotAdoptOldJobs(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	f.reconcile("hello")
	oldJob := f.jobs()[0].Name

	// Re-point the Task at a new UID, as a delete-and-recreate would.
	task := f.task("hello")
	task.UID = "uid-hello-recreated"
	task.Status = customv1alpha1.TaskStatus{}
	if err := f.client.Update(context.Background(), task); err != nil {
		t.Fatalf("recreating the Task: %v", err)
	}

	f.reconcile("hello")

	task = f.task("hello")
	if task.Status.SucceededCount != 0 || task.Status.FailedCount != 0 {
		t.Error("the recreated Task adopted outcomes from its predecessor's Jobs")
	}
	if len(task.Status.RecentRuns) != 0 {
		t.Errorf("recentRuns has %d entries; a recreated Task must start clean",
			len(task.Status.RecentRuns))
	}
	_ = oldJob
}

func findCondition(task *customv1alpha1.Task, condType string) *metav1.Condition {
	for i := range task.Status.Conditions {
		if task.Status.Conditions[i].Type == condType {
			return &task.Status.Conditions[i]
		}
	}
	return nil
}

// readdJob puts a finished Job back, simulating a delete that did not take.
func readdJob(t *testing.T, f *fixture, task *customv1alpha1.Task, name string, slot time.Time, succeeded bool) {
	t.Helper()
	job, err := BuildJob(task, slot, f.scheme)
	if err != nil {
		t.Fatalf("BuildJob: %v", err)
	}
	job.Name = name
	if err := f.client.Create(context.Background(), job); err != nil {
		t.Fatalf("re-adding Job: %v", err)
	}
	f.finishJob(name, succeeded)
}

var _ = strings.TrimSpace
var _ client.Object = &batchv1.Job{}

// Adopting an existing Job must not emit an Event.
//
// Adoption fires both on a genuine recovery and, far more often, when a pass
// reads our own status before the cache has caught up. Owns() delivers several
// events per Job lifecycle, so an Event per adoption would be noise
// proportional to Job churn, and Events are API writes.
func TestAdoptionIsSilentAndCreatesNoDuplicate(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	f.reconcile("hello")
	jobName := f.jobs()[0].Name
	f.recordedEvents() // drain JobCreated

	// Simulate the cache lagging our own status write: the Job exists, but
	// status does not yet know the slot was scheduled.
	task := f.task("hello")
	task.Status.LastScheduleTime = nil
	task.Status.ActiveJob = ""
	if err := f.client.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("rewinding status: %v", err)
	}

	f.reconcile("hello")

	if n := len(f.jobs()); n != 1 {
		t.Errorf("have %d Jobs; adoption must never create a duplicate", n)
	}
	if f.jobs()[0].Name != jobName {
		t.Errorf("job = %q, want the existing %q adopted", f.jobs()[0].Name, jobName)
	}
	for _, e := range f.recordedEvents() {
		if strings.Contains(e, "Adopted") || strings.Contains(e, "JobCreated") {
			t.Errorf("adoption emitted an Event: %q", e)
		}
	}
}

// The status patch must contain only status fields.
//
// client.MergeFrom computes the patch as a diff against the base object, so a
// base that holds only status would also emit every field it is missing --
// including metadata.resourceVersion, which silently makes every status write
// optimistically locked and fail with a conflict whenever anything else touched
// the object. The fake client does not enforce optimistic concurrency, so the
// patch body itself is asserted.
func TestStatusPatchCarriesOnlyStatus(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	recorder := &patchRecorder{}
	f.client.onStatusPatch = recorder.record

	f.reconcile("hello")

	if len(recorder.patches) == 0 {
		t.Fatal("no status patch was issued")
	}
	for _, body := range recorder.patches {
		var fields map[string]any
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Fatalf("status patch is not valid JSON: %v", err)
		}
		if _, ok := fields["spec"]; ok {
			t.Errorf("status patch carries spec: %s", body)
		}
		if meta, ok := fields["metadata"].(map[string]any); ok {
			if _, bad := meta["resourceVersion"]; bad {
				t.Errorf("status patch carries metadata.resourceVersion, which makes "+
					"every status write optimistically locked: %s", body)
			}
		}
		if _, ok := fields["status"]; !ok {
			t.Errorf("status patch has no status stanza: %s", body)
		}
	}
}

// patchRecorder captures status patch bodies.
type patchRecorder struct {
	patches [][]byte
}

func (p *patchRecorder) record(body []byte) {
	p.patches = append(p.patches, body)
}
