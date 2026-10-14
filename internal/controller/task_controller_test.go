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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
)

// These specs run against a real API server and etcd (envtest), so they cover
// what the fake client cannot: schema validation, defaulting, the status
// subresource split, and the generated print columns.
var _ = Describe("Task CRD", func() {
	const ns = "default"

	newTask := func(name string) *customv1alpha1.Task {
		return &customv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: customv1alpha1.TaskSpec{
				Command:  "echo Hello World",
				Schedule: everyFiveMinutes,
			},
		}
	}

	Context("validation", func() {
		It("accepts a minimal Task and applies the declared defaults", func() {
			task := newTask("defaults")
			Expect(k8sClient.Create(ctx, task)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, task) })

			var got customv1alpha1.Task
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "defaults", Namespace: ns}, &got)).To(Succeed())
			Expect(got.Spec.Image).To(Equal("busybox:1.36"),
				"image should default so samples stay two lines")
			Expect(got.Spec.BackoffLimit).NotTo(BeNil())
			Expect(*got.Spec.BackoffLimit).To(BeEquivalentTo(0),
				"backoffLimit should default to 0 so failures surface immediately")
		})

		It("requires command and schedule", func() {
			task := newTask("no-command")
			task.Spec.Command = ""
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())

			task = newTask("no-schedule")
			task.Spec.Schedule = ""
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())
		})

		It("rejects a Task name longer than 52 characters", func() {
			// Job names append a slot suffix and must fit the 63-character
			// job-name label, which is the same limit CronJob enforces.
			task := newTask(strings.Repeat("a", 53))
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())

			ok := newTask(strings.Repeat("b", 52))
			Expect(k8sClient.Create(ctx, ok)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, ok) })
		})

		It("rejects a schedule that fails the shape gate", func() {
			task := newTask("too-few-fields")
			task.Spec.Schedule = tooFewCronFields
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())
		})

		It("rejects a command over the length bound", func() {
			task := newTask("too-long")
			task.Spec.Command = strings.Repeat("x", 4097)
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())
		})

		It("rejects out-of-range numeric fields", func() {
			task := newTask("bad-backoff")
			over := int32(11)
			task.Spec.BackoffLimit = &over
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())

			task = newTask("bad-deadline")
			zero := int64(0)
			task.Spec.StartingDeadlineSeconds = &zero
			Expect(k8sClient.Create(ctx, task)).NotTo(Succeed(),
				"0 is a degenerate deadline that silently means never run")
		})
	})

	Context("status subresource", func() {
		It("only accepts status through the status subresource", func() {
			task := newTask("status-split")
			Expect(k8sClient.Create(ctx, task)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, task) })

			// A write to the main resource must not persist status.
			task.Status.State = customv1alpha1.TaskStateCompleted
			Expect(k8sClient.Update(ctx, task)).To(Succeed())

			var got customv1alpha1.Task
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "status-split", Namespace: ns}, &got)).To(Succeed())
			Expect(got.Status.State).To(BeEmpty(),
				"users write spec; only the controller writes status")

			got.Status.State = customv1alpha1.TaskStateCompleted
			Expect(k8sClient.Status().Update(ctx, &got)).To(Succeed())
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "status-split", Namespace: ns}, &got)).To(Succeed())
			Expect(got.Status.State).To(Equal(customv1alpha1.TaskStateCompleted))
		})

		It("rejects an invalid state and a history longer than the cap", func() {
			task := newTask("status-bounds")
			Expect(k8sClient.Create(ctx, task)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, task) })

			task.Status.State = customv1alpha1.TaskState("Bogus")
			Expect(k8sClient.Status().Update(ctx, task)).NotTo(Succeed())

			var got customv1alpha1.Task
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "status-bounds", Namespace: ns}, &got)).To(Succeed())
			for i := range customv1alpha1.MaxRecentRuns + 1 {
				got.Status.RecentRuns = append(got.Status.RecentRuns, customv1alpha1.TaskRun{
					ScheduledTime: metav1.Now(),
					JobName:       fmt.Sprintf("j-%d", i),
					Result:        customv1alpha1.TaskRunSucceeded,
				})
			}
			Expect(k8sClient.Status().Update(ctx, &got)).NotTo(Succeed(),
				"recentRuns must stay bounded so status cannot grow without limit")
		})
	})

	Context("print columns", func() {
		// A wrong JSONPath in a marker fails silently as an empty cell rather
		// than an error, so the column set is asserted rather than eyeballed.
		It("exposes the default columns in order", func() {
			task := newTask("columns")
			Expect(k8sClient.Create(ctx, task)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, task) })

			cols, err := taskTableColumns(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(cols).To(Equal([]string{
				"Name", "Schedule", "Suspend", "State",
				"Last Execution", "Next", "Succeeded", "Failed", "Age",
			}), "kubectl adds Name automatically but not Age, so Age is declared explicitly")
		})
	})
})

// taskTableColumns asks the API server for the Table representation, which is
// what kubectl renders.
func taskTableColumns(ctx context.Context) ([]string, error) {
	result := restClient.Get().
		AbsPath("/apis/custom.io/v1alpha1/namespaces/default/tasks").
		SetHeader("Accept", "application/json;as=Table;g=meta.k8s.io;v=v1").
		Do(ctx)
	raw, err := result.Raw()
	if err != nil {
		return nil, err
	}
	var table struct {
		ColumnDefinitions []struct {
			Name     string `json:"name"`
			Priority int32  `json:"priority"`
		} `json:"columnDefinitions"`
	}
	if err := jsonUnmarshal(raw, &table); err != nil {
		return nil, err
	}
	var names []string
	for _, c := range table.ColumnDefinitions {
		if c.Priority == 0 { // priority > 0 is -o wide only
			names = append(names, c.Name)
		}
	}
	return names, nil
}
