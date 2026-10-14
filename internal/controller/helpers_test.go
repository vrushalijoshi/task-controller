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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
	"github.com/vrushalijoshi/task-controller/internal/metrics"
)

const (
	testNamespace = "demo"

	// Shared fixtures, so the same literals are not repeated across test files.
	testImage        = "busybox:1.36"
	everyFiveMinutes = "*/5 * * * *"
	tooFewCronFields = "*/5 * *"
	reasonLabel      = "reason"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("adding client-go scheme: %v", err)
	}
	if err := customv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding Task scheme: %v", err)
	}
	return s
}

// newTask builds a Task with the finalizer already present, which is the steady
// state after the first reconcile. Tests that exercise the finalizer-add path
// clear it explicitly.
func newTask(name, command, schedule string) *customv1alpha1.Task {
	backoff := int32(0)
	return &customv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         testNamespace,
			UID:               types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(base),
			Generation:        1,
			Finalizers:        []string{customv1alpha1.CleanupFinalizer},
		},
		Spec: customv1alpha1.TaskSpec{
			Command:      command,
			Schedule:     schedule,
			Image:        testImage,
			BackoffLimit: &backoff,
		},
	}
}

// countingClient records status writes so a test can assert the "at most one
// status write per reconcile" invariant directly rather than by inspection.
type countingClient struct {
	client.Client
	statusWrites int
	// onCreate, when set, can fail a Create, so the error paths are testable.
	onCreate func(client.Object) error
	// failStatusPatch, when set, fails every status patch.
	failStatusPatch error
	// onStatusPatch, when set, receives the serialised patch body so a test can
	// assert what the patch actually contains.
	onStatusPatch func([]byte)
}

func (c *countingClient) Create(
	ctx context.Context,
	obj client.Object,
	opts ...client.CreateOption,
) error {
	if c.onCreate != nil {
		if err := c.onCreate(obj); err != nil {
			return err
		}
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *countingClient) Status() client.SubResourceWriter {
	return &countingStatusWriter{SubResourceWriter: c.Client.Status(), parent: c}
}

type countingStatusWriter struct {
	client.SubResourceWriter
	parent *countingClient
}

func (w *countingStatusWriter) Patch(
	ctx context.Context,
	obj client.Object,
	patch client.Patch,
	opts ...client.SubResourcePatchOption,
) error {
	w.parent.statusWrites++
	if w.parent.failStatusPatch != nil {
		return w.parent.failStatusPatch
	}
	if w.parent.onStatusPatch != nil {
		body, err := patch.Data(obj)
		if err != nil {
			return err
		}
		w.parent.onStatusPatch(body)
	}
	return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func (w *countingStatusWriter) Update(
	ctx context.Context,
	obj client.Object,
	opts ...client.SubResourceUpdateOption,
) error {
	w.parent.statusWrites++
	return w.SubResourceWriter.Update(ctx, obj, opts...)
}

// fixture bundles everything a reconcile test needs.
type fixture struct {
	t        *testing.T
	client   *countingClient
	reconc   *TaskReconciler
	clock    *clocktesting.FakeClock
	registry *prometheus.Registry
	events   *events.FakeRecorder
	scheme   *runtime.Scheme
}

// newFixture builds a reconciler over a fake client with the status subresource
// and the owner index registered, driven by a fake clock so time-based
// behaviour runs in milliseconds and deterministically.
func newFixture(t *testing.T, now time.Time, objs ...client.Object) *fixture {
	t.Helper()
	s := testScheme(t)

	builder := fake.NewClientBuilder().
		WithScheme(s).
		WithStatusSubresource(&customv1alpha1.Task{}).
		WithIndex(&batchv1.Job{}, jobOwnerKey, func(o client.Object) []string {
			owner := metav1.GetControllerOf(o)
			if owner == nil || owner.Kind != "Task" {
				return nil
			}
			return []string{string(owner.UID)}
		})
	for _, o := range objs {
		builder = builder.WithObjects(o)
	}
	base := builder.Build()

	counting := &countingClient{Client: base}
	registry := prometheus.NewRegistry()
	eventRecorder := events.NewFakeRecorder(100)
	fakeClock := clocktesting.NewFakeClock(now)

	return &fixture{
		t:        t,
		client:   counting,
		clock:    fakeClock,
		registry: registry,
		events:   eventRecorder,
		scheme:   s,
		reconc: &TaskReconciler{
			Client: counting,
			Scheme: s,
			// The fake client serves uncached reads too, which is the right
			// model: the test controls what exists, and the APIReader path is
			// exercised by removing the Job from the tracker.
			APIReader: base,
			Recorder:  eventRecorder,
			Clock:     fakeClock,
			Metrics:   metrics.NewRecorder(registry),
		},
	}
}

// reconcile runs one pass and asserts the write budget.
func (f *fixture) reconcile(name string) ctrl.Result {
	f.t.Helper()
	f.client.statusWrites = 0
	res, err := f.reconc.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name},
	})
	if err != nil {
		f.t.Fatalf("Reconcile returned an error: %v", err)
	}
	if f.client.statusWrites > 1 {
		f.t.Fatalf("Reconcile made %d status writes; the invariant is at most one per pass",
			f.client.statusWrites)
	}
	return res
}

// reconcileExpectingError runs one pass and requires it to fail, so error paths
// can assert both the error and what the pass still managed to persist.
//
//nolint:unparam // name is part of the fixture's shape, mirroring reconcile().
func (f *fixture) reconcileExpectingError(name string) error {
	f.t.Helper()
	f.client.statusWrites = 0
	_, err := f.reconc.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: name},
	})
	if err == nil {
		f.t.Fatal("Reconcile succeeded, want an error")
	}
	if f.client.statusWrites > 1 {
		f.t.Fatalf("Reconcile made %d status writes; the invariant is at most one per pass",
			f.client.statusWrites)
	}
	return err
}

func (f *fixture) task(name string) *customv1alpha1.Task {
	f.t.Helper()
	var task customv1alpha1.Task
	err := f.client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: name}, &task)
	if err != nil {
		f.t.Fatalf("getting Task %s: %v", name, err)
	}
	return &task
}

func (f *fixture) taskOrNil(name string) *customv1alpha1.Task {
	f.t.Helper()
	var task customv1alpha1.Task
	err := f.client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: name}, &task)
	if err != nil {
		return nil
	}
	return &task
}

func (f *fixture) jobs() []batchv1.Job {
	f.t.Helper()
	var list batchv1.JobList
	if err := f.client.List(context.Background(), &list, client.InNamespace(testNamespace)); err != nil {
		f.t.Fatalf("listing Jobs: %v", err)
	}
	return list.Items
}

func (f *fixture) job(name string) *batchv1.Job {
	f.t.Helper()
	var job batchv1.Job
	err := f.client.Get(context.Background(),
		types.NamespacedName{Namespace: testNamespace, Name: name}, &job)
	if err != nil {
		f.t.Fatalf("getting Job %s: %v", name, err)
	}
	return &job
}

// finishJob marks a Job complete or failed, as the real Job controller would.
func (f *fixture) finishJob(name string, succeeded bool) {
	f.t.Helper()
	job := f.job(name)
	cond := batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}
	if !succeeded {
		cond = batchv1.JobCondition{
			Type:   batchv1.JobFailed,
			Status: corev1.ConditionTrue,
			Reason: "BackoffLimitExceeded",
		}
	}
	completion := metav1.NewTime(f.clock.Now())
	job.Status.Conditions = append(job.Status.Conditions, cond)
	job.Status.CompletionTime = &completion
	zero := int32(0)
	job.Status.Ready = &zero
	if err := f.client.Status().Update(context.Background(), job); err != nil {
		f.t.Fatalf("updating Job status: %v", err)
	}
}

// markJobReady simulates the Job reporting a running pod.
func (f *fixture) markJobReady(name string) {
	f.t.Helper()
	job := f.job(name)
	ready := int32(1)
	job.Status.Ready = &ready
	if err := f.client.Status().Update(context.Background(), job); err != nil {
		f.t.Fatalf("updating Job status: %v", err)
	}
}

// counter reads a counter's current value out of the test registry.
func (f *fixture) counter(name string, labels map[string]string) float64 {
	f.t.Helper()
	families, err := f.registry.Gather()
	if err != nil {
		f.t.Fatalf("gathering metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			if !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			return m.GetCounter().GetValue()
		}
	}
	return 0
}

func labelsMatch(got []*dto.LabelPair, want map[string]string) bool {
	if len(want) == 0 {
		return true
	}
	for k, v := range want {
		found := false
		for _, l := range got {
			if l.GetName() == k && l.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// recordedEvents drains the fake recorder.
func (f *fixture) recordedEvents() []string {
	var out []string
	for {
		select {
		case e := <-f.events.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func (f *fixture) advance(d time.Duration) {
	f.clock.Step(d)
}
