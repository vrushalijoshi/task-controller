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
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
)

const (
	// AnnotationScheduledTime records which cron slot a Job belongs to. The
	// slot, not the wall-clock creation time, is what the reconciler indexes
	// and compares against the idempotency key.
	AnnotationScheduledTime = "custom.io/scheduled-time"

	// LabelTaskName identifies the owning Task.
	LabelTaskName = "custom.io/task-name"

	// LabelManagedBy scopes the manager's Job informer. Caching only Jobs with
	// this label means controller memory scales with our Jobs rather than with
	// every Job in the cluster.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// ManagedByValue is the value of LabelManagedBy on Jobs we own.
	ManagedByValue = "task-controller"

	// jobTTLSeconds is a safety net, not the primary cleanup path: the
	// reconciler deletes finished Jobs itself once their outcome is durable.
	// This only matters if the controller is down for hours.
	jobTTLSeconds = int32(21600) // 6h

	// taskPodUID runs task pods as nobody.
	taskPodUID = int64(65534)

	// maxMessageLength matches the MaxLength on the status message fields.
	// Job failure messages can be long, and a status write rejected for length
	// would stall the Task, so messages are truncated before being stored.
	maxMessageLength = 1024
)

// JobName returns the deterministic Job name for a slot.
//
// Determinism is what makes Create idempotent: if a status write is lost or the
// controller crashes between creating the Job and recording it, the next pass
// computes the same name, Create returns AlreadyExists, and the existing Job is
// adopted. That single property removes the entire class of double-run bugs.
//
// The minute granularity here is why ValidateSchedule rejects schedules faster
// than MinScheduleInterval: two slots in one minute would collide on this name.
// The two must change together.
func JobName(task *customv1alpha1.Task, slot time.Time) string {
	return fmt.Sprintf("%s-%d", task.Name, slot.Unix()/60)
}

// BuildJob renders the Job for one slot of a Task.
func BuildJob(
	task *customv1alpha1.Task,
	slot time.Time,
	scheme *runtime.Scheme,
) (*batchv1.Job, error) {
	backoffLimit := int32(0)
	if task.Spec.BackoffLimit != nil {
		backoffLimit = *task.Spec.BackoffLimit
	}
	ttl := jobTTLSeconds
	uid := taskPodUID
	noPrivilegeEscalation := false
	runAsNonRoot := true
	automount := false

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      JobName(task, slot),
			Namespace: task.Namespace,
			Labels: map[string]string{
				LabelTaskName:  task.Name,
				LabelManagedBy: ManagedByValue,
			},
			Annotations: map[string]string{
				AnnotationScheduledTime: slot.UTC().Format(time.RFC3339),
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   task.Spec.ActiveDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						LabelTaskName:  task.Name,
						LabelManagedBy: ManagedByValue,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &automount,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &runAsNonRoot,
						RunAsUser:    &uid,
						RunAsGroup:   &uid,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:    "task",
						Image:   task.Spec.Image,
						Command: []string{"/bin/sh", "-c", task.Spec.Command},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivilegeEscalation,
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
							},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("10m"),
								corev1.ResourceMemory: resource.MustParse("16Mi"),
							},
						},
					}},
				},
			},
		},
	}

	// The owner reference is what lets the garbage collector clean up Jobs if
	// the finalizer is ever force-removed, and what the owner field index keys
	// on when listing a Task's Jobs.
	if err := ctrl.SetControllerReference(task, job, scheme); err != nil {
		return nil, fmt.Errorf("setting controller reference: %w", err)
	}
	return job, nil
}

// SlotFromJobName recovers the slot encoded in a Job name by JobName.
//
// Needed when only the name survives -- notably when status names an active Job
// that has since been deleted, so the object (and its scheduled-time annotation)
// can no longer be read. Using status.lastScheduleTime as a stand-in would be
// wrong, because that advances on skipped slots too.
func SlotFromJobName(taskName, jobName string) (time.Time, bool) {
	prefix := taskName + "-"
	if !strings.HasPrefix(jobName, prefix) {
		return time.Time{}, false
	}
	minutes, err := strconv.ParseInt(strings.TrimPrefix(jobName, prefix), 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(minutes*60, 0).UTC(), true
}

// JobSlot returns the cron slot a Job was created for.
func JobSlot(job *batchv1.Job) (time.Time, error) {
	raw, ok := job.Annotations[AnnotationScheduledTime]
	if !ok {
		return time.Time{}, fmt.Errorf("job %s has no %s annotation", job.Name, AnnotationScheduledTime)
	}
	slot, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("job %s has an invalid %s annotation %q: %w",
			job.Name, AnnotationScheduledTime, raw, err)
	}
	return slot, nil
}

// JobOutcome reports whether a Job has finished and, if so, whether it
// succeeded. It reads the Job's conditions rather than counting pods, which
// keeps the controller free of a Pod informer and Pod RBAC.
func JobOutcome(job *batchv1.Job) (finished bool, succeeded bool, message string) {
	for i := range job.Status.Conditions {
		c := &job.Status.Conditions[i]
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, true, truncate(c.Message)
		case batchv1.JobFailed:
			msg := c.Message
			if msg == "" {
				msg = c.Reason
			}
			return true, false, truncate(msg)
		}
	}
	return false, false, ""
}

// JobIsRunning reports whether the Job has at least one ready pod. Reading
// Job.status.ready rather than listing pods removes a whole informer and a
// permission.
func JobIsRunning(job *batchv1.Job) bool {
	return job.Status.Ready != nil && *job.Status.Ready > 0
}

// NewTaskRun builds the history record for a finished run.
func NewTaskRun(
	job *batchv1.Job,
	slot time.Time,
	result customv1alpha1.TaskRunResult,
	message string,
) customv1alpha1.TaskRun {
	run := customv1alpha1.TaskRun{
		ScheduledTime: metav1.NewTime(slot.UTC()),
		JobName:       job.Name,
		Result:        result,
		Message:       truncate(message),
	}
	if job.Status.StartTime != nil {
		run.StartTime = job.Status.StartTime.DeepCopy()
	}
	if job.Status.CompletionTime != nil {
		run.CompletionTime = job.Status.CompletionTime.DeepCopy()
	}
	return run
}

// prependRun adds a run to the front of the history and truncates to
// MaxRecentRuns, so status.recentRuns stays newest-first and bounded.
func prependRun(history []customv1alpha1.TaskRun, run customv1alpha1.TaskRun) []customv1alpha1.TaskRun {
	out := make([]customv1alpha1.TaskRun, 0, customv1alpha1.MaxRecentRuns)
	out = append(out, run)
	for i := 0; i < len(history) && len(out) < customv1alpha1.MaxRecentRuns; i++ {
		out = append(out, history[i])
	}
	return out
}

// truncate bounds a message to the CRD's MaxLength, so an unusually long Job
// failure message cannot cause the status write to be rejected and stall the
// Task.
func truncate(s string) string {
	if len(s) <= maxMessageLength {
		return s
	}
	const ellipsis = "..."
	return s[:maxMessageLength-len(ellipsis)] + ellipsis
}
