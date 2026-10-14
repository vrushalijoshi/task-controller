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
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
)

// The deterministic name is what makes Create idempotent, so it must depend on
// the slot and nothing else.
func TestJobNameIsDeterministicPerSlot(t *testing.T) {
	task := newTask("hello", "echo hi", "*/5 * * * *")
	slot := time.Date(2026, 10, 7, 17, 5, 0, 0, time.UTC)

	first := JobName(task, slot)
	second := JobName(task, slot)
	if first != second {
		t.Fatalf("JobName is not deterministic: %q then %q", first, second)
	}

	other := JobName(task, slot.Add(5*time.Minute))
	if first == other {
		t.Errorf("different slots produced the same Job name %q", first)
	}

	if len(first) > 63 {
		t.Errorf("Job name %q is %d characters, over the 63-character limit", first, len(first))
	}
}

// A 52-character Task name is the documented maximum; the Job name it produces
// must still fit the 63-character job-name label.
func TestJobNameFitsLabelLimitAtMaxTaskName(t *testing.T) {
	task := newTask(strings.Repeat("a", 52), "echo hi", "*/5 * * * *")
	name := JobName(task, time.Date(2026, 10, 7, 17, 5, 0, 0, time.UTC))
	if len(name) > 63 {
		t.Errorf("Job name is %d characters, over the 63-character limit: %q", len(name), name)
	}
}

func TestBuildJob(t *testing.T) {
	task := newTask("hello", "echo Hello World", "*/5 * * * *")
	task.Spec.Image = testImage
	task.Spec.ActiveDeadlineSeconds = ptrInt64(300)
	slot := time.Date(2026, 10, 7, 17, 5, 0, 0, time.UTC)

	job, err := BuildJob(task, slot, testScheme(t))
	if err != nil {
		t.Fatalf("BuildJob: %v", err)
	}

	if got, want := job.Name, JobName(task, slot); got != want {
		t.Errorf("name = %q, want %q", got, want)
	}
	if got, want := job.Namespace, task.Namespace; got != want {
		t.Errorf("namespace = %q, want %q", got, want)
	}

	container := job.Spec.Template.Spec.Containers[0]
	wantCmd := []string{"/bin/sh", "-c", "echo Hello World"}
	if len(container.Command) != 3 || container.Command[2] != wantCmd[2] ||
		container.Command[0] != wantCmd[0] || container.Command[1] != wantCmd[1] {
		t.Errorf("command = %v, want %v", container.Command, wantCmd)
	}
	if container.Image != testImage {
		t.Errorf("image = %q, want busybox:1.36", container.Image)
	}

	// The slot annotation is what the reconciler indexes by, so it must be the
	// slot rather than a wall-clock creation time.
	if got, want := job.Annotations[AnnotationScheduledTime], slot.Format(time.RFC3339); got != want {
		t.Errorf("scheduled-time annotation = %q, want %q", got, want)
	}
	// The managed-by label scopes the manager's Job cache; without it the Job
	// would be invisible to the controller.
	if got := job.Labels[LabelManagedBy]; got != ManagedByValue {
		t.Errorf("managed-by label = %q, want %q", got, ManagedByValue)
	}
	if got := job.Labels[LabelTaskName]; got != task.Name {
		t.Errorf("task-name label = %q, want %q", got, task.Name)
	}

	owner := metav1.GetControllerOf(job)
	if owner == nil || owner.Name != task.Name || owner.UID != task.UID {
		t.Errorf("owner reference = %+v, want controller ref to %s/%s", owner, task.Name, task.UID)
	}

	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %v, want 0 so failures surface immediately", job.Spec.BackoffLimit)
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 300 {
		t.Errorf("activeDeadlineSeconds = %v, want 300", job.Spec.ActiveDeadlineSeconds)
	}
	if job.Spec.TTLSecondsAfterFinished == nil {
		t.Error("ttlSecondsAfterFinished is unset; it is the safety net if the controller is down")
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", job.Spec.Template.Spec.RestartPolicy)
	}

	// Task pods run untrusted commands, so the restricted posture is part of
	// the contract rather than a nicety.
	podSec := job.Spec.Template.Spec.SecurityContext
	if podSec == nil || podSec.RunAsNonRoot == nil || !*podSec.RunAsNonRoot {
		t.Error("pod does not set runAsNonRoot")
	}
	if am := job.Spec.Template.Spec.AutomountServiceAccountToken; am == nil || *am {
		t.Error("pod mounts a service-account token; task pods should not have one")
	}
	if c := container.SecurityContext; c == nil || len(c.Capabilities.Drop) == 0 {
		t.Error("container does not drop capabilities")
	}
}

func TestJobSlotRoundTrip(t *testing.T) {
	task := newTask("hello", "echo hi", "*/5 * * * *")
	slot := time.Date(2026, 10, 7, 17, 5, 0, 0, time.UTC)

	job, err := BuildJob(task, slot, testScheme(t))
	if err != nil {
		t.Fatalf("BuildJob: %v", err)
	}
	got, err := JobSlot(job)
	if err != nil {
		t.Fatalf("JobSlot: %v", err)
	}
	if !got.Equal(slot) {
		t.Errorf("JobSlot = %v, want %v", got, slot)
	}
}

func TestJobSlotRejectsBadAnnotation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		annotations map[string]string
	}{
		{name: "missing", annotations: nil},
		{name: "unparseable", annotations: map[string]string{AnnotationScheduledTime: "not-a-time"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "j", Annotations: tc.annotations}}
			if _, err := JobSlot(job); err == nil {
				t.Error("JobSlot accepted an invalid annotation")
			}
		})
	}
}

func TestJobOutcome(t *testing.T) {
	tests := []struct {
		name          string
		conditions    []batchv1.JobCondition
		wantFinished  bool
		wantSucceeded bool
	}{
		{name: "still running", conditions: nil},
		{
			name: "complete",
			conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
			wantFinished: true, wantSucceeded: true,
		},
		{
			name: "failed",
			conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
			},
			wantFinished: true, wantSucceeded: false,
		},
		{
			// A condition present but False must not be read as finished.
			name: "condition present but false",
			conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: batchv1.JobStatus{Conditions: tc.conditions}}
			finished, succeeded, _ := JobOutcome(job)
			if finished != tc.wantFinished || succeeded != tc.wantSucceeded {
				t.Errorf("JobOutcome = (%v, %v), want (%v, %v)",
					finished, succeeded, tc.wantFinished, tc.wantSucceeded)
			}
		})
	}
}

func TestJobIsRunning(t *testing.T) {
	ready := int32(1)
	none := int32(0)
	tests := []struct {
		name string
		jobs *int32
		want bool
	}{
		{name: "ready unset", jobs: nil, want: false},
		{name: "no ready pods", jobs: &none, want: false},
		{name: "one ready pod", jobs: &ready, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: batchv1.JobStatus{Ready: tc.jobs}}
			if got := JobIsRunning(job); got != tc.want {
				t.Errorf("JobIsRunning = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPrependRun(t *testing.T) {
	run := func(name string) customv1alpha1.TaskRun {
		return customv1alpha1.TaskRun{JobName: name}
	}

	tests := []struct {
		name    string
		history []customv1alpha1.TaskRun
		add     customv1alpha1.TaskRun
		want    []string
	}{
		{name: "empty history", add: run("a"), want: []string{"a"}},
		{
			name:    "newest first",
			history: []customv1alpha1.TaskRun{run("b"), run("c")},
			add:     run("a"),
			want:    []string{"a", "b", "c"},
		},
		{
			// The cap is what keeps status bounded regardless of how long a
			// Task has been running.
			name:    "truncates to MaxRecentRuns",
			history: []customv1alpha1.TaskRun{run("b"), run("c"), run("d")},
			add:     run("a"),
			want:    []string{"a", "b", "c"},
		},
		{
			name:    "already over the cap",
			history: []customv1alpha1.TaskRun{run("b"), run("c"), run("d"), run("e")},
			add:     run("a"),
			want:    []string{"a", "b", "c"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := prependRun(tc.history, tc.add)
			if len(got) > customv1alpha1.MaxRecentRuns {
				t.Fatalf("history has %d entries, over the cap of %d", len(got), customv1alpha1.MaxRecentRuns)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("history = %d entries, want %d", len(got), len(tc.want))
			}
			for i, want := range tc.want {
				if got[i].JobName != want {
					t.Errorf("entry %d = %q, want %q", i, got[i].JobName, want)
				}
			}
		})
	}
}

// A long Job failure message must not make the status write exceed the CRD's
// MaxLength and stall the Task.
func TestTruncate(t *testing.T) {
	if got := truncate("short"); got != "short" {
		t.Errorf("truncate shortened a short message to %q", got)
	}
	long := strings.Repeat("x", maxMessageLength*2)
	got := truncate(long)
	if len(got) > maxMessageLength {
		t.Errorf("truncate returned %d characters, over the %d limit", len(got), maxMessageLength)
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("truncated message does not end in an ellipsis: %q", got[len(got)-10:])
	}
}
