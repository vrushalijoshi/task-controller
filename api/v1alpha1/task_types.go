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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// CleanupFinalizer blocks deletion of a Task until the controller has
	// deleted the Jobs it owns and recorded their outcomes. Owner references
	// remain as the backstop if this finalizer is ever force-removed.
	CleanupFinalizer = "custom.io/cleanup-jobs"

	// MaxRecentRuns caps status.recentRuns. Fixed rather than configurable:
	// three records stay well under a kilobyte of status at any Task count,
	// and a knob would add a default, a bound and a test path for no gain.
	// Promoting it to spec.historyLimit later is backwards compatible.
	MaxRecentRuns = 3

	// ConditionScheduleValid reports whether spec.schedule can ever produce a
	// run. It is False for a schedule that fails to parse, one that can never
	// be satisfied (February 30th), and one that fires faster than once a
	// minute. See internal/controller/schedule.go.
	ConditionScheduleValid = "ScheduleValid"
)

// TaskState is the state of the most recent execution of a Task.
//
// state describes the most recent execution, not "what is happening right
// now": between scheduled runs a Task shows the last outcome (Completed or
// Failed) rather than reverting to Pending. Pending therefore means either
// "never run yet" or "Job created, pod not running yet". This keeps the last
// outcome visible in `kubectl get tk`, which is what an operator wants.
type TaskState string

const (
	// TaskStatePending means the Task has never run, or its current Job has
	// not yet reported a running pod.
	TaskStatePending TaskState = "Pending"
	// TaskStateRunning means a Job for this Task has at least one ready pod.
	TaskStateRunning TaskState = "Running"
	// TaskStateCompleted means the most recent execution succeeded.
	TaskStateCompleted TaskState = "Completed"
	// TaskStateFailed means the most recent execution failed, was aborted, or
	// the schedule is invalid.
	TaskStateFailed TaskState = "Failed"
)

// AllTaskStates is the zero-fill set for the per-state metric gauge, so that
// e.g. tasks_by_state{state="Failed"} exists at 0 and alerts need no absent().
var AllTaskStates = []TaskState{
	TaskStatePending,
	TaskStateRunning,
	TaskStateCompleted,
	TaskStateFailed,
}

// TaskRunResult is the outcome of one finished execution.
type TaskRunResult string

const (
	// TaskRunSucceeded means the Job completed successfully.
	TaskRunSucceeded TaskRunResult = "Succeeded"
	// TaskRunFailed means the Job's own workload failed.
	TaskRunFailed TaskRunResult = "Failed"
	// TaskRunAborted means the run was cut short by something other than the
	// workload: the Task was deleted, or the Job was deleted externally. Kept
	// distinct from Failed so an operator-initiated delete does not land in
	// task_failures_total and spike failure alerts on every teardown.
	TaskRunAborted TaskRunResult = "Aborted"
)

// TaskSpec defines the desired state of Task.
type TaskSpec struct {
	// command is executed as `/bin/sh -c <command>`. It is intended to be a
	// one-liner; longer logic belongs in spec.image or a mounted ConfigMap.
	// The 4096 bound keeps Task objects small (cache memory scales with it)
	// and stays far below MAX_ARG_STRLEN, past which exec would fail at
	// runtime rather than at admission.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=4096
	Command string `json:"command"`

	// schedule is a 5-field cron expression or a descriptor (@hourly, @daily,
	// @every 10m), interpreted in UTC. The pattern below is a cheap shape gate
	// only: CEL cannot parse cron, so the authoritative parse and the
	// satisfiability and minimum-interval checks happen in the controller and
	// surface on the ScheduleValid condition.
	// +required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +kubebuilder:validation:Pattern=`^(@(annually|yearly|monthly|weekly|daily|midnight|hourly)|@every +[0-9]+(ns|us|ms|s|m|h)|([^ ]+ +){4}[^ ]+)$`
	Schedule string `json:"schedule"`

	// image is the container image each run executes in.
	// +optional
	// +kubebuilder:default="busybox:1.36"
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	// +kubebuilder:validation:Pattern=`^\S+$`
	Image string `json:"image,omitempty"`

	// suspend pauses scheduling without deleting the Task or its history.
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// startingDeadlineSeconds bounds how late a missed slot may still run,
	// which keeps a restart after downtime from running a stale slot. Unset
	// means the most recent missed slot runs however late it is. Minimum is 1
	// because 0 is a degenerate "deadline of zero" that silently means never.
	// +optional
	// +kubebuilder:validation:Minimum=1
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// backoffLimit is the Job retry count. It defaults to 0, unlike the Job
	// default of 6, so a failure surfaces immediately instead of minutes
	// later; capped at 10 since surfacing failures fast is the point.
	// +optional
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// activeDeadlineSeconds bounds a single run, so a hung execution cannot
	// block every later slot under the Forbid concurrency policy.
	// +optional
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=86400
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`
}

// TaskRun is a high-level record of one finished execution. The Job and its
// pods are deleted once the outcome is recorded, so this is what survives.
type TaskRun struct {
	// scheduledTime is the cron slot this run belongs to.
	// +required
	ScheduledTime metav1.Time `json:"scheduledTime"`

	// jobName is the Job that ran this slot. The Job itself is deleted once
	// the outcome is recorded.
	// +required
	// +kubebuilder:validation:MaxLength=63
	JobName string `json:"jobName"`

	// result is the outcome of the run.
	// +required
	// +kubebuilder:validation:Enum=Succeeded;Failed;Aborted
	Result TaskRunResult `json:"result"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// message explains the outcome, e.g. "BackoffLimitExceeded".
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`
}

// TaskStatus defines the observed state of Task.
type TaskStatus struct {
	// state is the state of the most recent execution. See TaskState.
	// +optional
	// +kubebuilder:validation:Enum=Pending;Running;Completed;Failed
	State TaskState `json:"state,omitempty"`

	// lastExecutionTime is when the controller last created a Job.
	// +optional
	LastExecutionTime *metav1.Time `json:"lastExecutionTime,omitempty"`

	// lastScheduleTime is the most recent slot acted on. It anchors
	// missed-slot math and is deliberately separate from lastExecutionTime,
	// because a Job can be created late for an earlier slot.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// lastCompletionTime is when the most recent run finished.
	// +optional
	LastCompletionTime *metav1.Time `json:"lastCompletionTime,omitempty"`

	// nextScheduleTime is when the next run is due. Cleared when the schedule
	// is invalid, so status never promises a run that cannot happen.
	// +optional
	NextScheduleTime *metav1.Time `json:"nextScheduleTime,omitempty"`

	// lastFinishedScheduleTime is the idempotency key for outcome recording:
	// a finished Job whose slot is at or before this has already been counted,
	// so it is skipped and only deleted. This is what makes a Job's outcome
	// recorded exactly once even if the delete fails and it is seen again.
	// +optional
	LastFinishedScheduleTime *metav1.Time `json:"lastFinishedScheduleTime,omitempty"`

	// activeJob is the Job currently executing, if any.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	ActiveJob string `json:"activeJob,omitempty"`

	// succeededCount and failedCount are durable totals. Prometheus counters
	// reset when the process restarts; these do not.
	// +optional
	// +kubebuilder:validation:Minimum=0
	SucceededCount int32 `json:"succeededCount,omitempty"`

	// +optional
	// +kubebuilder:validation:Minimum=0
	FailedCount int32 `json:"failedCount,omitempty"`

	// recentRuns holds the most recent executions, newest first, capped at
	// MaxRecentRuns. The Jobs themselves are deleted as soon as their outcome
	// is durable, so this is the surviving run history.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=3
	RecentRuns []TaskRun `json:"recentRuns,omitempty"`

	// message is a human-readable note about the most recent transition.
	// +optional
	// +kubebuilder:validation:MaxLength=1024
	Message string `json:"message,omitempty"`

	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// conditions represent the current state of the Task resource.
	// ScheduleValid reports whether spec.schedule can ever produce a run.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tk
// +kubebuilder:validation:XValidation:rule="self.metadata.name.size() <= 52",message="Task name must be 52 characters or fewer; Job names append a slot suffix"
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=".spec.schedule"
// +kubebuilder:printcolumn:name="Suspend",type=boolean,JSONPath=".spec.suspend"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Last Execution",type=date,JSONPath=".status.lastExecutionTime"
// Next is type=string, not type=date: kubectl renders a date column as elapsed
// time since the timestamp, which for a future instant prints "<invalid>".
// +kubebuilder:printcolumn:name="Next",type=string,JSONPath=".status.nextScheduleTime"
// +kubebuilder:printcolumn:name="Succeeded",type=integer,JSONPath=".status.succeededCount"
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=".status.failedCount"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="Command",type=string,priority=1,JSONPath=".spec.command"
// +kubebuilder:printcolumn:name="Image",type=string,priority=1,JSONPath=".spec.image"
// +kubebuilder:printcolumn:name="Active Job",type=string,priority=1,JSONPath=".status.activeJob"
// +kubebuilder:printcolumn:name="Last Completion",type=date,priority=1,JSONPath=".status.lastCompletionTime"
// +kubebuilder:printcolumn:name="Schedule Valid",type=string,priority=1,JSONPath=".status.conditions[?(@.type=='ScheduleValid')].status"
// +kubebuilder:printcolumn:name="Message",type=string,priority=1,JSONPath=".status.message"

// Task is the Schema for the tasks API.
type Task struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of Task
	// +required
	Spec TaskSpec `json:"spec"`

	// status defines the observed state of Task
	// +optional
	Status TaskStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TaskList contains a list of Task
type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Task{}, &TaskList{})
		return nil
	})
}
