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

// Package metrics exposes the controller's Prometheus instrumentation.
//
// It has two halves with deliberately different mechanics:
//
//   - Counters, incremented by the reconciler, but only ever after the status
//     write for the same facts has succeeded. A counter must never describe
//     something that is not already durable.
//
//   - Gauges, computed at scrape time by StateCollector from the shared
//     informer cache rather than maintained with Inc/Dec on transitions.
//     Inc/Dec double-counts on reconcile retries, loses transitions across
//     restarts, and leaks a series for every deleted Task. A List over the
//     source of truth cannot drift, needs no finalizer to clean up, and costs
//     microseconds.
package metrics

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
)

// Reasons for the skipped and aborted counters.
const (
	ReasonConcurrencyForbid = "concurrency_forbid"
	ReasonStartingDeadline  = "starting_deadline_exceeded"
	ReasonDeletedExternally = "deleted_externally"
	ReasonTaskDeleted       = "task_deleted"
)

// Recorder holds the event counters incremented by the reconciler.
type Recorder struct {
	Failures    prometheus.Counter
	Successes   prometheus.Counter
	JobsCreated prometheus.Counter
	RunsSkipped *prometheus.CounterVec
	RunsAborted *prometheus.CounterVec
}

// NewRecorder builds the counters and registers them with the given registerer.
func NewRecorder(reg prometheus.Registerer) *Recorder {
	r := &Recorder{
		Failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_controller_task_failures_total",
			Help: "Total number of Task runs that failed. Runs aborted by a " +
				"Task or Job deletion are counted separately, in " +
				"task_controller_task_runs_aborted_total.",
		}),
		Successes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_controller_task_successes_total",
			Help: "Total number of Task runs that succeeded.",
		}),
		JobsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "task_controller_jobs_created_total",
			Help: "Total number of Jobs created by the controller.",
		}),
		RunsSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_controller_task_runs_skipped_total",
			Help: "Total number of scheduled slots that did not run.",
		}, []string{"reason"}),
		RunsAborted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "task_controller_task_runs_aborted_total",
			Help: "Total number of runs cut short by something other than the " +
				"workload, such as the Task or its Job being deleted.",
		}, []string{"reason"}),
	}
	reg.MustRegister(r.Failures, r.Successes, r.JobsCreated, r.RunsSkipped, r.RunsAborted)

	// Pre-create every label combination so the series exists at 0. A CounterVec
	// emits nothing at all until its first observation, which would make
	// increase(...runs_skipped_total[10m]) return no data rather than 0 and force
	// alerts into absent() handling. This is the same reasoning as zero-filling
	// the state gauges.
	for _, reason := range []string{ReasonConcurrencyForbid, ReasonStartingDeadline} {
		r.RunsSkipped.WithLabelValues(reason)
	}
	for _, reason := range []string{ReasonDeletedExternally, ReasonTaskDeleted} {
		r.RunsAborted.WithLabelValues(reason)
	}

	return r
}

// NewRecorderForManager registers the counters with controller-runtime's
// registry, so they are served on the same endpoint as the built-in
// controller_runtime_* and workqueue_* series.
func NewRecorderForManager() *Recorder {
	return NewRecorder(ctrlmetrics.Registry)
}

// TaskLister is the slice of the informer cache the collector needs. Narrowing
// it to this keeps the collector unit-testable without a manager.
type TaskLister interface {
	List(ctx context.Context, out *customv1alpha1.TaskList) error
}

// StateCollector reports Task counts, computed at scrape time from the cache.
type StateCollector struct {
	lister TaskLister

	// synced reports whether the cache has started. A replica that lost the
	// leader election still serves /metrics, but its cache and controllers
	// never start; without this it would publish all-zero gauges and Prometheus
	// would scrape two disagreeing targets for the same data.
	synced func() bool

	total   *prometheus.Desc
	byState *prometheus.Desc
	up      *prometheus.Desc
}

var _ prometheus.Collector = &StateCollector{}

// NewStateCollector builds a collector over the given lister. synced may be nil,
// in which case the cache is assumed ready.
func NewStateCollector(lister TaskLister, synced func() bool) *StateCollector {
	if synced == nil {
		synced = func() bool { return true }
	}
	return &StateCollector{
		lister: lister,
		synced: synced,
		total: prometheus.NewDesc(
			"task_controller_tasks",
			"Total number of Tasks, excluding those being deleted.",
			nil, nil),
		byState: prometheus.NewDesc(
			"task_controller_tasks_by_state",
			"Number of Tasks in each state. Zero-filled over all states, so "+
				"alerts need no absent() handling and the values always sum to "+
				"task_controller_tasks.",
			[]string{"state"}, nil),
		up: prometheus.NewDesc(
			"task_controller_task_state_collector_up",
			"1 if the state gauges in this scrape were computed successfully, "+
				"0 if the cache was unavailable or the List failed.",
			nil, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *StateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.total
	ch <- c.byState
	ch <- c.up
}

// Collect implements prometheus.Collector. It lists Tasks from the cache and
// derives the gauges, so they cannot drift from the source of truth.
//
// On failure it reports only collector_up 0 and emits no gauge samples at all.
// Publishing zeros instead would be indistinguishable from "there are genuinely
// no Tasks", which is exactly the wrong signal to hand an alert.
func (c *StateCollector) Collect(ch chan<- prometheus.Metric) {
	if !c.synced() {
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}

	var list customv1alpha1.TaskList
	if err := c.lister.List(context.Background(), &list); err != nil {
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 0)
		return
	}

	// Zero-fill every state so each series exists even at 0.
	counts := make(map[customv1alpha1.TaskState]int, len(customv1alpha1.AllTaskStates))
	for _, s := range customv1alpha1.AllTaskStates {
		counts[s] = 0
	}

	total := 0
	for i := range list.Items {
		task := &list.Items[i]
		// A terminating Task is on its way out; it should not hold a slot in a
		// state gauge while its finalizer drains.
		if task.DeletionTimestamp != nil {
			continue
		}
		total++
		state := task.Status.State
		if state == "" {
			// Not reconciled yet. Counting it as Pending keeps the per-state
			// values summing to the total.
			state = customv1alpha1.TaskStatePending
		}
		counts[state]++
	}

	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(total))
	for _, s := range customv1alpha1.AllTaskStates {
		ch <- prometheus.MustNewConstMetric(
			c.byState, prometheus.GaugeValue, float64(counts[s]), string(s))
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
}

// RegisterStateCollector registers the collector with controller-runtime's registry.
func RegisterStateCollector(c *StateCollector) error {
	return ctrlmetrics.Registry.Register(c)
}
