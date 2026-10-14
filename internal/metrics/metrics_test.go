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

package metrics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
)

// stubLister serves a fixed list, or an error.
type stubLister struct {
	tasks []customv1alpha1.Task
	err   error
}

func (s *stubLister) List(_ context.Context, out *customv1alpha1.TaskList) error {
	if s.err != nil {
		return s.err
	}
	out.Items = s.tasks
	return nil
}

func task(name string, state customv1alpha1.TaskState) customv1alpha1.Task {
	return customv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
		Status:     customv1alpha1.TaskStatus{State: state},
	}
}

// Every state must be present even at zero, so alerts need no absent() handling
// and the per-state values always sum to the total.
func TestCollectZeroFillsEveryState(t *testing.T) {
	c := NewStateCollector(&stubLister{tasks: []customv1alpha1.Task{
		task("a", customv1alpha1.TaskStateCompleted),
		task("b", customv1alpha1.TaskStateCompleted),
		task("c", customv1alpha1.TaskStateFailed),
		task("d", customv1alpha1.TaskStateRunning),
	}}, nil)

	expected := `
# HELP task_controller_tasks Total number of Tasks, excluding those being deleted.
# TYPE task_controller_tasks gauge
task_controller_tasks 4
# HELP task_controller_tasks_by_state Number of Tasks in each state. Zero-filled over all states, so alerts need no absent() handling and the values always sum to task_controller_tasks.
# TYPE task_controller_tasks_by_state gauge
task_controller_tasks_by_state{state="Pending"} 0
task_controller_tasks_by_state{state="Running"} 1
task_controller_tasks_by_state{state="Completed"} 2
task_controller_tasks_by_state{state="Failed"} 1
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"task_controller_tasks", "task_controller_tasks_by_state"); err != nil {
		t.Error(err)
	}
}

// A Task that has not been reconciled yet counts as Pending, so the per-state
// values still sum to the total.
func TestCollectCountsUnreconciledAsPending(t *testing.T) {
	c := NewStateCollector(&stubLister{tasks: []customv1alpha1.Task{
		task("fresh", ""),
		task("done", customv1alpha1.TaskStateCompleted),
	}}, nil)

	expected := `
# HELP task_controller_tasks_by_state Number of Tasks in each state. Zero-filled over all states, so alerts need no absent() handling and the values always sum to task_controller_tasks.
# TYPE task_controller_tasks_by_state gauge
task_controller_tasks_by_state{state="Pending"} 1
task_controller_tasks_by_state{state="Running"} 0
task_controller_tasks_by_state{state="Completed"} 1
task_controller_tasks_by_state{state="Failed"} 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"task_controller_tasks_by_state"); err != nil {
		t.Error(err)
	}
}

// A terminating Task is on its way out and must not hold a slot in a gauge.
func TestCollectSkipsTerminatingTasks(t *testing.T) {
	deleting := task("going", customv1alpha1.TaskStateCompleted)
	now := metav1.Now()
	deleting.DeletionTimestamp = &now

	c := NewStateCollector(&stubLister{tasks: []customv1alpha1.Task{
		task("staying", customv1alpha1.TaskStateCompleted),
		deleting,
	}}, nil)

	expected := `
# HELP task_controller_tasks Total number of Tasks, excluding those being deleted.
# TYPE task_controller_tasks gauge
task_controller_tasks 1
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"task_controller_tasks"); err != nil {
		t.Error(err)
	}
}

// On failure the collector reports up=0 and no gauges at all. Publishing zeros
// instead would be indistinguishable from "there are genuinely no Tasks", which
// is exactly the wrong signal for an alert.
func TestCollectReportsDownOnListError(t *testing.T) {
	c := NewStateCollector(&stubLister{err: errors.New("cache unavailable")}, nil)

	expected := `
# HELP task_controller_task_state_collector_up 1 if the state gauges in this scrape were computed successfully, 0 if the cache was unavailable or the List failed.
# TYPE task_controller_task_state_collector_up gauge
task_controller_task_state_collector_up 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"task_controller_task_state_collector_up"); err != nil {
		t.Error(err)
	}
	if n := testutil.CollectAndCount(c, "task_controller_tasks_by_state"); n != 0 {
		t.Errorf("emitted %d state gauges on a failed List; want none", n)
	}
}

// A replica that lost the leader election still serves /metrics but its cache
// never starts. It must not publish all-zero gauges that disagree with the
// leader's.
func TestCollectReportsDownWhenCacheNotSynced(t *testing.T) {
	c := NewStateCollector(
		&stubLister{tasks: []customv1alpha1.Task{task("a", customv1alpha1.TaskStateCompleted)}},
		func() bool { return false },
	)

	if n := testutil.CollectAndCount(c, "task_controller_tasks"); n != 0 {
		t.Errorf("a standby replica published %d total gauges; want none", n)
	}
	expected := `
# HELP task_controller_task_state_collector_up 1 if the state gauges in this scrape were computed successfully, 0 if the cache was unavailable or the List failed.
# TYPE task_controller_task_state_collector_up gauge
task_controller_task_state_collector_up 0
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"task_controller_task_state_collector_up"); err != nil {
		t.Error(err)
	}
}

// The gauges must be derived from the cache, so a Task that disappears drops
// out of the metrics with no cleanup and no leaked series.
func TestCollectTracksDeletedTasksWithoutLeaking(t *testing.T) {
	lister := &stubLister{tasks: []customv1alpha1.Task{
		task("a", customv1alpha1.TaskStateCompleted),
		task("b", customv1alpha1.TaskStateCompleted),
	}}
	c := NewStateCollector(lister, nil)

	assertTotal := func(want int) {
		t.Helper()
		expected := fmt.Sprintf(`
# HELP task_controller_tasks Total number of Tasks, excluding those being deleted.
# TYPE task_controller_tasks gauge
task_controller_tasks %d
`, want)
		if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
			"task_controller_tasks"); err != nil {
			t.Error(err)
		}
	}

	assertTotal(2)

	// The next scrape simply sees one fewer Task: no cleanup hook, no stale series.
	lister.tasks = lister.tasks[:1]
	assertTotal(1)
}

// At tens of thousands of Tasks the collector runs on every scrape, so its cost
// is worth measuring rather than assuming.
func BenchmarkCollect(b *testing.B) {
	states := customv1alpha1.AllTaskStates
	tasks := make([]customv1alpha1.Task, 50000)
	for i := range tasks {
		tasks[i] = task(fmt.Sprintf("task-%d", i), states[i%len(states)])
	}
	c := NewStateCollector(&stubLister{tasks: tasks}, nil)

	b.ReportAllocs()
	for b.Loop() {
		testutil.CollectAndCount(c, "task_controller_tasks_by_state")
	}
}

// A CounterVec emits nothing until its first observation, which would make
// increase(...) return no data rather than 0. Every known reason must exist at
// zero from startup, for the same reason the state gauges are zero-filled.
func TestCounterVecsAreZeroInitialised(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewRecorder(reg)

	for _, tc := range []struct {
		metric  string
		reasons []string
	}{
		{
			metric:  "task_controller_task_runs_skipped_total",
			reasons: []string{ReasonConcurrencyForbid, ReasonStartingDeadline},
		},
		{
			metric:  "task_controller_task_runs_aborted_total",
			reasons: []string{ReasonDeletedExternally, ReasonTaskDeleted},
		},
	} {
		for _, reason := range tc.reasons {
			got := testutil.ToFloat64(mustCollector(t, reg, tc.metric, reason))
			if got != 0 {
				t.Errorf("%s{reason=%q} = %v, want 0", tc.metric, reason, got)
			}
		}
	}
}

// mustCollector finds a single labelled series in the registry.
func mustCollector(t *testing.T, reg *prometheus.Registry, name, reason string) prometheus.Collector {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gathering: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					c := prometheus.NewCounter(prometheus.CounterOpts{Name: "shim", Help: "shim"})
					c.Add(m.GetCounter().GetValue())
					return c
				}
			}
		}
	}
	t.Fatalf("%s{reason=%q} is not present; the series must exist at 0", name, reason)
	return nil
}
