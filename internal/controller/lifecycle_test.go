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
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
	"github.com/vrushalijoshi/task-controller/internal/metrics"
)

// An instant command such as `echo` runs the whole create -> finish -> delete
// cycle inside two reconciles. If activeJob is not cleared when the outcome is
// recorded, the following pass finds a name the cache cannot resolve and
// records a bogus Aborted run over a job that actually succeeded.
func TestFastCommandDoesNotRecordSpuriousAbort(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo Hello World", "*/5 * * * *"))

	f.reconcile("hello")
	jobName := f.jobs()[0].Name
	f.finishJob(jobName, true)
	f.reconcile("hello") // records the outcome and deletes the Job

	// Run to a fixed point: no later pass may invent an outcome.
	for range 3 {
		f.reconcile("hello")
	}

	task := f.task("hello")
	if task.Status.State != customv1alpha1.TaskStateCompleted {
		t.Errorf("state = %q, want Completed", task.Status.State)
	}
	if task.Status.ActiveJob != "" {
		t.Errorf("activeJob = %q, want it cleared once the run finished", task.Status.ActiveJob)
	}
	if len(task.Status.RecentRuns) != 1 {
		t.Fatalf("recentRuns has %d entries, want exactly 1", len(task.Status.RecentRuns))
	}
	if got := task.Status.RecentRuns[0].Result; got != customv1alpha1.TaskRunSucceeded {
		t.Errorf("recentRuns[0].result = %q, want Succeeded, not an invented abort", got)
	}
	if got := f.counter("task_controller_task_runs_aborted_total",
		map[string]string{reasonLabel: metrics.ReasonDeletedExternally}); got != 0 {
		t.Errorf("runs_aborted_total = %v for a clean fast run; want 0", got)
	}
	if task.Status.FailedCount != 0 {
		t.Errorf("failedCount = %d for a successful run; want 0", task.Status.FailedCount)
	}
}

// Defence in depth: even if the activeJob clear is lost to a failed status
// write, the idempotency key must stop the next pass inventing an abort.
func TestFastCommandSurvivesLostActiveJobClear(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", "*/5 * * * *"))
	f.reconcile("hello")
	jobName := f.jobs()[0].Name
	f.finishJob(jobName, true)
	f.reconcile("hello")

	// Put activeJob back, as a lost status write would leave it.
	task := f.task("hello")
	task.Status.ActiveJob = jobName
	if err := f.client.Status().Update(context.Background(), task); err != nil {
		t.Fatalf("restoring activeJob: %v", err)
	}

	f.reconcile("hello")

	task = f.task("hello")
	if len(task.Status.RecentRuns) != 1 {
		t.Errorf("recentRuns has %d entries; the key must suppress a duplicate",
			len(task.Status.RecentRuns))
	}
	if task.Status.State == customv1alpha1.TaskStateFailed {
		t.Error("state flipped to Failed for a run that succeeded")
	}
	if task.Status.ActiveJob != "" {
		t.Errorf("activeJob = %q, want it cleared", task.Status.ActiveJob)
	}
}

// A run shorter than the time it takes the Job to report a ready pod goes
// straight from Pending to Completed. Running is only shown when observed.
func TestFastCommandSkipsRunningState(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", "*/5 * * * *"))

	f.reconcile("hello")
	if got := f.task("hello").Status.State; got != customv1alpha1.TaskStatePending {
		t.Fatalf("state after create = %q, want Pending", got)
	}

	f.finishJob(f.jobs()[0].Name, true)
	f.reconcile("hello")

	if got := f.task("hello").Status.State; got != customv1alpha1.TaskStateCompleted {
		t.Errorf("state = %q, want Completed without ever showing Running", got)
	}
}

// Someone runs `kubectl delete job` on an active run.
func TestActiveJobDeletedExternally(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 600", "*/5 * * * *"))
	f.reconcile("slow")
	jobName := f.jobs()[0].Name
	f.markJobReady(jobName)
	f.reconcile("slow")

	// Delete it out from under the controller.
	if err := f.client.Delete(context.Background(), f.job(jobName)); err != nil {
		t.Fatalf("deleting the Job: %v", err)
	}

	f.reconcile("slow")

	task := f.task("slow")
	if task.Status.State != customv1alpha1.TaskStateFailed {
		t.Errorf("state = %q, want Failed when the run is destroyed", task.Status.State)
	}
	if task.Status.ActiveJob != "" {
		t.Errorf("activeJob = %q, want it cleared", task.Status.ActiveJob)
	}
	if len(task.Status.RecentRuns) != 1 {
		t.Fatalf("recentRuns has %d entries, want 1", len(task.Status.RecentRuns))
	}
	if got := task.Status.RecentRuns[0].Result; got != customv1alpha1.TaskRunAborted {
		t.Errorf("result = %q, want Aborted; an external delete is not a workload failure", got)
	}
	// An operator-initiated delete must not look like the workload failing, or
	// every teardown would spike the failure alert.
	if got := f.counter("task_controller_task_failures_total", nil); got != 0 {
		t.Errorf("failures_total = %v, want 0 for an aborted run", got)
	}
	if got := f.counter("task_controller_task_runs_aborted_total",
		map[string]string{reasonLabel: metrics.ReasonDeletedExternally}); got != 1 {
		t.Errorf("runs_aborted_total{deleted_externally} = %v, want 1", got)
	}
	assertEvent(t, f, "JobDisappeared")
}

// After an external delete the Task must resume, not strand.
func TestTaskResumesAfterExternalJobDelete(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 600", "*/5 * * * *"))
	f.reconcile("slow")
	if err := f.client.Delete(context.Background(), f.job(f.jobs()[0].Name)); err != nil {
		t.Fatalf("deleting the Job: %v", err)
	}
	f.reconcile("slow")

	f.advance(5 * time.Minute)
	f.reconcile("slow")

	if n := len(f.jobs()); n != 1 {
		t.Errorf("have %d Jobs at the next slot; the Task must resume normally", n)
	}
}

func TestDeletingTaskWithNoJobsDrainsImmediately(t *testing.T) {
	task := newTask("hello", "echo hi", "*/5 * * * *")
	now := metav1.NewTime(base)
	task.DeletionTimestamp = &now
	f := newFixture(t, base, task)

	f.reconcile("hello")

	if got := f.taskOrNil("hello"); got != nil &&
		controllerutil.ContainsFinalizer(got, customv1alpha1.CleanupFinalizer) {
		t.Error("finalizer still present with no Jobs to clean up")
	}
}

// Deleting a Task mid-run must record why the run ended, delete the Job, and
// only then release the Task.
func TestDeletingTaskAbortsActiveRun(t *testing.T) {
	f := newFixture(t, slotTime, newTask("slow", "sleep 600", "*/5 * * * *"))
	f.reconcile("slow")
	jobName := f.jobs()[0].Name
	f.markJobReady(jobName)

	// A real delete: the finalizer makes the API server set deletionTimestamp
	// and keep the object, rather than removing it outright.
	if err := f.client.Delete(context.Background(), f.task("slow")); err != nil {
		t.Fatalf("deleting the Task: %v", err)
	}

	// First finalize pass: record, abort, delete, and hold the finalizer.
	res := f.reconcile("slow")
	if res.RequeueAfter <= 0 {
		t.Error("finalization did not requeue while a Job was still being deleted")
	}

	task := f.taskOrNil("slow")
	if task == nil {
		t.Fatal("the Task was released before its Job was confirmed gone")
	}
	if len(task.Status.RecentRuns) != 1 {
		t.Fatalf("recentRuns has %d entries, want the aborted run recorded",
			len(task.Status.RecentRuns))
	}
	if got := task.Status.RecentRuns[0].Result; got != customv1alpha1.TaskRunAborted {
		t.Errorf("result = %q, want Aborted", got)
	}
	if got := f.counter("task_controller_task_failures_total", nil); got != 0 {
		t.Errorf("failures_total = %v; deleting a Task is not a workload failure", got)
	}
	if got := f.counter("task_controller_task_runs_aborted_total",
		map[string]string{reasonLabel: metrics.ReasonTaskDeleted}); got != 1 {
		t.Errorf("runs_aborted_total{task_deleted} = %v, want 1", got)
	}
	assertEvent(t, f, "RunAborted")

	// Second pass, with the Job now gone: release the Task.
	f.reconcile("slow")
	if task := f.taskOrNil("slow"); task != nil &&
		controllerutil.ContainsFinalizer(task, customv1alpha1.CleanupFinalizer) {
		t.Error("finalizer still held after every Job was deleted")
	}
	if n := len(f.jobs()); n != 0 {
		t.Errorf("%d Jobs left behind after finalization; want 0", n)
	}
}

// A run that completed just before the delete must still be counted.
func TestDeletingTaskRecordsUnrecordedOutcome(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", "*/5 * * * *"))
	f.reconcile("hello")
	f.finishJob(f.jobs()[0].Name, true)

	if err := f.client.Delete(context.Background(), f.task("hello")); err != nil {
		t.Fatalf("deleting the Task: %v", err)
	}

	f.reconcile("hello")

	if got := f.counter("task_controller_task_successes_total", nil); got != 1 {
		t.Errorf("successes_total = %v; an outcome that landed before the delete must still count", got)
	}
}

// A terminating Task must never start new work.
func TestTerminatingTaskCreatesNoJob(t *testing.T) {
	task := newTask("hello", "echo hi", "*/5 * * * *")
	now := metav1.NewTime(slotTime)
	task.DeletionTimestamp = &now
	f := newFixture(t, slotTime, task)

	f.reconcile("hello")

	if n := len(f.jobs()); n != 0 {
		t.Errorf("created %d Jobs for a terminating Task; want 0", n)
	}
}

// Only the last MaxRecentRuns are kept, newest first.
func TestRunHistoryIsBoundedAndNewestFirst(t *testing.T) {
	f := newFixture(t, slotTime, newTask("hello", "echo hi", "*/5 * * * *"))

	names := make([]string, 0, 4)
	for i := range 4 {
		f.reconcile("hello")
		jobs := f.jobs()
		if len(jobs) == 0 {
			t.Fatalf("iteration %d created no Job", i)
		}
		names = append(names, jobs[0].Name)
		f.finishJob(jobs[0].Name, true)
		f.reconcile("hello")
		f.advance(5 * time.Minute)
	}

	task := f.task("hello")
	if len(task.Status.RecentRuns) != customv1alpha1.MaxRecentRuns {
		t.Fatalf("recentRuns has %d entries, want %d",
			len(task.Status.RecentRuns), customv1alpha1.MaxRecentRuns)
	}
	// Newest first: the most recent run is the last one created.
	if got, want := task.Status.RecentRuns[0].JobName, names[len(names)-1]; got != want {
		t.Errorf("recentRuns[0] = %q, want the newest run %q", got, want)
	}
	if task.Status.SucceededCount != 4 {
		t.Errorf("succeededCount = %d, want 4; the durable total is not truncated",
			task.Status.SucceededCount)
	}
}

// Every non-terminal return must requeue, or the Task would be forgotten by the
// queue: RequeueAfter <= 0 means no requeue at all, not "immediately".
func TestNonTerminalReturnsAlwaysRequeue(t *testing.T) {
	cases := []struct {
		name string
		when time.Time
	}{
		{name: "waiting for the first slot", when: base},
		{name: "slot just reached", when: slotTime},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.when, newTask("hello", "echo hi", "*/5 * * * *"))
			res := f.reconcile("hello")
			if res.RequeueAfter <= 0 {
				t.Errorf("RequeueAfter = %v, want a positive delay", res.RequeueAfter)
			}
		})
	}
}

// A settled Task with nothing to do must not write status at all, or every
// reconcile would cost an API write.
func TestSteadyStateWritesNothing(t *testing.T) {
	f := newFixture(t, base, newTask("hello", "echo hi", "*/5 * * * *"))
	f.reconcile("hello") // first pass settles status

	f.client.statusWrites = 0
	f.reconcile("hello")

	if f.client.statusWrites != 0 {
		t.Errorf("a settled reconcile made %d status writes; want 0", f.client.statusWrites)
	}
}

// The ScheduleValid condition must not be rewritten on every pass, or Events
// and status writes would loop.
func TestInvalidScheduleEventEmittedOnTransitionOnly(t *testing.T) {
	f := newFixture(t, slotTime, newTask("bad", "echo hi", "0 0 30 2 *"))

	f.reconcile("bad")
	f.recordedEvents() // drain the first, expected InvalidSchedule
	f.client.statusWrites = 0

	f.reconcile("bad")

	if f.client.statusWrites != 0 {
		t.Errorf("reconciling an unchanged invalid schedule wrote status %d times; want 0",
			f.client.statusWrites)
	}
}

func assertEvent(t *testing.T, f *fixture, reason string) {
	t.Helper()
	for _, e := range f.recordedEvents() {
		if strings.Contains(e, reason) {
			return
		}
	}
	t.Errorf("no %s Event was emitted", reason)
}

var _ client.Client = &countingClient{}
