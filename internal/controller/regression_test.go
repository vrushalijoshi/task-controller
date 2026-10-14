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
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
	"github.com/vrushalijoshi/task-controller/internal/metrics"
)

// A transient Job Create failure must surface as a reconcile error so the
// workqueue retries the slot.
//
// Swallowing it loses the slot outright: DueSlot collapses missed slots into the
// newest one, so the next wake is at the *next* slot and the failed one is gone
// for good, with nothing but a log line to show for it. For an @daily Task that
// is a lost day.
func TestJobCreateFailureIsRetried(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	f.client.onCreate = func(client.Object) error {
		return apierrors.NewInternalError(errors.New("etcd unavailable"))
	}

	err := f.reconcileExpectingError("hello")
	if !strings.Contains(err.Error(), "creating Job") {
		t.Errorf("error = %q, want it to name the failed Create", err)
	}
	if n := len(f.jobs()); n != 0 {
		t.Fatalf("have %d Jobs after a failed Create; want 0", n)
	}
	if got := f.counter("task_controller_jobs_created_total", nil); got != 0 {
		t.Errorf("jobs_created_total = %v after a failed Create; want 0", got)
	}

	// The retry must still run the slot rather than skipping past it.
	f.client.onCreate = nil
	f.reconcile("hello")

	jobs := f.jobs()
	if len(jobs) != 1 {
		t.Fatalf("have %d Jobs after the retry; want the slot to have run", len(jobs))
	}
	task := f.task("hello")
	if want := JobName(task, slotTime); jobs[0].Name != want {
		t.Errorf("job = %q, want the originally-due slot %q", jobs[0].Name, want)
	}
}

// Outcomes already recorded in step 2 must survive a later Create failure in the
// same pass, or a successful run would be lost because the next slot errored.
func TestOutcomeIsPersistedEvenIfCreateFails(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", "* * * * *"))
	f.reconcile("hello")
	f.finishJob(f.jobs()[0].Name, true)

	// Next slot is due and its Create fails, in the same pass that records the
	// previous run's success.
	f.advance(time.Minute)
	f.client.onCreate = func(client.Object) error {
		return apierrors.NewInternalError(errors.New("etcd unavailable"))
	}
	_ = f.reconcileExpectingError("hello")

	task := f.task("hello")
	if task.Status.SucceededCount != 1 {
		t.Errorf("succeededCount = %d; the recorded outcome must survive the Create failure",
			task.Status.SucceededCount)
	}
	if len(task.Status.RecentRuns) != 1 {
		t.Errorf("recentRuns has %d entries; the recorded run must still be persisted",
			len(task.Status.RecentRuns))
	}
}

// Finalization requeues every second while a Job remains, and Owns() wakes it on
// every Job status update. Without an idempotency guard each of those passes
// would append another Aborted entry, bump the counter and emit another Event.
func TestFinalizationRecordsEachAbortOnce(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 600", everyFiveMinutes))
	f.reconcile("slow")
	jobName := f.jobs()[0].Name
	f.markJobReady(jobName)

	if err := f.client.Delete(context.Background(), f.task("slow")); err != nil {
		t.Fatalf("deleting the Task: %v", err)
	}

	// First finalize pass: records the abort and deletes the Job.
	f.reconcile("slow")

	// Simulate the Job still being visible: deleting, but not yet gone from the
	// cache, which is exactly what happens before the delete propagates.
	task := f.taskOrNil("slow")
	if task == nil {
		t.Skip("the Task drained in one pass; nothing to re-observe")
	}
	readdJob(t, f, task, jobName, slotTime, false)
	markJobActive(t, f, jobName)

	// Second and third passes see the same active Job again.
	f.reconcile("slow")
	f.reconcile("slow")

	if task = f.taskOrNil("slow"); task == nil {
		return // released; nothing further to assert
	}
	aborted := 0
	for _, run := range task.Status.RecentRuns {
		if run.Result == customv1alpha1.TaskRunAborted && run.JobName == jobName {
			aborted++
		}
	}
	if aborted > 1 {
		t.Errorf("recorded %d Aborted entries for Job %s; want exactly 1", aborted, jobName)
	}
	if got := f.counter("task_controller_task_runs_aborted_total",
		map[string]string{reasonLabel: metrics.ReasonTaskDeleted}); got > 1 {
		t.Errorf("runs_aborted_total{task_deleted} = %v; want 1, not one per requeue", got)
	}
}

// After a Forbid skip, status.lastScheduleTime names a slot the active Job does
// not belong to, so it must not be used to attribute an aborted run.
func TestAbortedRunUsesTheJobsOwnSlot(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 600", "* * * * *"))
	f.reconcile("slow")
	jobName := f.jobs()[0].Name
	f.markJobReady(jobName)

	// Next slot arrives while the run is active: skipped, but the anchor still
	// advances past the slot this Job belongs to.
	f.advance(time.Minute)
	f.reconcile("slow")

	task := f.task("slow")
	if task.Status.LastScheduleTime.Equal(&metav1.Time{Time: slotTime}) {
		t.Fatal("precondition failed: the skip did not advance lastScheduleTime")
	}

	// Now the Job is deleted out from under the controller.
	if err := f.client.Delete(context.Background(), f.job(jobName)); err != nil {
		t.Fatalf("deleting the Job: %v", err)
	}
	f.reconcile("slow")

	task = f.task("slow")
	if len(task.Status.RecentRuns) == 0 {
		t.Fatal("the aborted run was not recorded")
	}
	got := task.Status.RecentRuns[0]
	if !got.ScheduledTime.Time.Equal(slotTime) {
		t.Errorf("recentRuns[0].scheduledTime = %v, want the Job's own slot %v",
			got.ScheduledTime.Time, slotTime)
	}
	// Advancing the key past a slot that never ran would make a later outcome
	// for that slot silently unrecordable.
	if key := task.Status.LastFinishedScheduleTime; key != nil && key.After(slotTime) {
		t.Errorf("lastFinishedScheduleTime = %v, advanced past a slot that never ran", key)
	}
}

func TestSlotFromJobName(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	name := JobName(task, slotTime)

	got, ok := SlotFromJobName("hello", name)
	if !ok {
		t.Fatalf("SlotFromJobName(%q) failed", name)
	}
	if !got.Equal(slotTime.Truncate(time.Minute)) {
		t.Errorf("slot = %v, want %v", got, slotTime)
	}

	for _, bad := range []string{"other-123", "hello-notanumber", "hello", ""} {
		if _, ok := SlotFromJobName("hello", bad); ok {
			t.Errorf("SlotFromJobName accepted %q", bad)
		}
	}
}

// A Job left behind by a same-named predecessor must not be adopted: Job names
// key on Task name, but ownership is by UID.
func TestForeignJobWithTheSameNameIsNotAdopted(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	f := newFixture(t, slotTime, task)

	// A Job with the name this slot will produce, owned by a different Task UID.
	foreign, err := BuildJob(task, slotTime, f.scheme)
	if err != nil {
		t.Fatalf("BuildJob: %v", err)
	}
	foreign.OwnerReferences[0].UID = "uid-a-previous-task"
	if err := f.client.Create(context.Background(), foreign); err != nil {
		t.Fatalf("creating the foreign Job: %v", err)
	}

	err = f.reconcileExpectingError("hello")
	if !strings.Contains(err.Error(), "not controlled by this Task") {
		t.Errorf("error = %q, want it to report the foreign Job", err)
	}
	if got := f.task("hello").Status.ActiveJob; got != "" {
		t.Errorf("activeJob = %q; a Job owned by another Task must not be adopted", got)
	}
}

// A slot dropped because startingDeadlineSeconds elapsed must be counted and
// evented, not dropped silently.
func TestStartingDeadlineSkipIsReported(t *testing.T) {
	task := newTask("hello", "echo hi", everyFiveMinutes)
	task.Spec.StartingDeadlineSeconds = ptrInt64(60)
	f := newFixture(t, base.Add(3*time.Hour), task)

	f.reconcile("hello")

	if n := len(f.jobs()); n != 0 {
		t.Errorf("created %d Jobs for a slot past its deadline; want 0", n)
	}
	if got := f.counter("task_controller_task_runs_skipped_total",
		map[string]string{reasonLabel: metrics.ReasonStartingDeadline}); got != 1 {
		t.Errorf("runs_skipped_total{starting_deadline_exceeded} = %v, want 1", got)
	}
	assertEvent(t, f, "SlotSkipped")
}

// Three finished Jobs folded in one pass must advance the counter by three.
func TestMultipleOutcomesInOnePassCountOnceEach(t *testing.T) {
	task := newTask("hello", "echo hi", "* * * * *")
	f := newFixture(t, slotTime, task)

	// Three finished Jobs for three consecutive slots, all visible at once.
	for i := range 3 {
		slot := slotTime.Add(time.Duration(i) * time.Minute)
		job, err := BuildJob(task, slot, f.scheme)
		if err != nil {
			t.Fatalf("BuildJob: %v", err)
		}
		if err := f.client.Create(context.Background(), job); err != nil {
			t.Fatalf("creating Job: %v", err)
		}
		f.finishJob(job.Name, true)
	}

	f.advance(3 * time.Minute)
	f.reconcile("hello")

	if got := f.counter("task_controller_task_successes_total", nil); got != 3 {
		t.Errorf("successes_total = %v, want 3 — one per outcome folded in the pass", got)
	}
	if got := f.task("hello").Status.SucceededCount; got != 3 {
		t.Errorf("succeededCount = %d, want 3", got)
	}
}

// A failed status write must leave counters untouched and Jobs undeleted, so the
// retry re-derives from a consistent picture.
func TestStatusWriteFailureLeavesNothingCounted(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", everyFiveMinutes))
	f.reconcile("hello")
	jobName := f.jobs()[0].Name
	f.finishJob(jobName, true)

	f.client.onStatusPatch = func([]byte) {}
	f.client.failStatusPatch = apierrors.NewConflict(
		schema.GroupResource{Group: "custom.io", Resource: "tasks"}, "hello",
		errors.New("conflict"))

	_ = f.reconcileExpectingError("hello")

	if got := f.counter("task_controller_task_successes_total", nil); got != 0 {
		t.Errorf("successes_total = %v after a failed status write; want 0", got)
	}
	if n := len(f.jobs()); n != 1 {
		t.Errorf("have %d Jobs; the Job must not be deleted before its outcome is durable", n)
	}
}

// markJobActive clears the finished conditions so the Job reads as active again.
func markJobActive(t *testing.T, f *fixture, name string) {
	t.Helper()
	job := f.job(name)
	job.Status.Conditions = nil
	ready := int32(1)
	job.Status.Ready = &ready
	if err := f.client.Status().Update(context.Background(), job); err != nil {
		t.Fatalf("resetting Job status: %v", err)
	}
}

var _ = batchv1.Job{}
var _ = metav1.Now
