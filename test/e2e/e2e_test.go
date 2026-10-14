//go:build e2e
// +build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/vrushalijoshi/task-controller/test/utils"
)

// namespace where the project is deployed in
const namespace = "task-controller-system"

// serviceAccountName created for the project
const serviceAccountName = "task-controller-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "task-controller-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "task-controller-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		// Tolerates an existing namespace: both top-level containers ensure the
		// controller is deployed, and Ginkgo randomises their order.
		cmd := exec.Command("kubectl", "get", "ns", namespace)
		if _, getErr := utils.Run(cmd); getErr != nil {
			cmd = exec.Command("kubectl", "create", "ns", namespace)
			_, createErr := utils.Run(cmd)
			Expect(createErr).NotTo(HaveOccurred(), "Failed to create namespace")
		}

		var err error

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			// Cluster-scoped, so it outlives the namespace and a previous run may
			// have left one behind. Remove it first to keep the spec re-runnable.
			_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding",
				metricsRoleBindingName, "--ignore-not-found=true"))

			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=task-controller-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput, err := getMetricsOutput()
		// Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// scrapeMetrics reads the controller's /metrics through a short-lived pod in the
// controller namespace, granting the metrics-reader role first. Self-contained
// so any spec can call it regardless of container ordering.
func scrapeMetrics(podName string) string {
	GinkgoHelper()

	By("granting the service account the metrics-reader role")
	_, _ = utils.Run(exec.Command("kubectl", "delete", "clusterrolebinding",
		metricsRoleBindingName, "--ignore-not-found=true"))
	_, err := utils.Run(exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
		"--clusterrole=task-controller-metrics-reader",
		fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName)))
	Expect(err).NotTo(HaveOccurred(), "Failed to bind the metrics-reader role")

	token, err := serviceAccountToken()
	Expect(err).NotTo(HaveOccurred(), "Failed to mint a service-account token")

	_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", podName,
		"-n", namespace, "--ignore-not-found=true"))

	By("curling the metrics endpoint from inside the cluster")
	_, err = utils.Run(exec.Command("kubectl", "run", podName,
		"--restart=Never", "--namespace", namespace,
		"--image=curlimages/curl:latest",
		"--overrides", fmt.Sprintf(`{
			"spec": {
				"containers": [{
					"name": "curl",
					"image": "curlimages/curl:latest",
					"command": ["/bin/sh", "-c"],
					"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
					"securityContext": {
						"allowPrivilegeEscalation": false,
						"capabilities": {"drop": ["ALL"]},
						"runAsNonRoot": true,
						"runAsUser": 1000,
						"seccompProfile": {"type": "RuntimeDefault"}
					}
				}],
				"serviceAccount": "%s"
			}
		}`, token, metricsServiceName, namespace, serviceAccountName)))
	Expect(err).NotTo(HaveOccurred(), "Failed to start the metrics scrape pod")

	Eventually(func(g Gomega) {
		out, err := utils.Run(exec.Command("kubectl", "get", "pods", podName,
			"-n", namespace, "-o", "jsonpath={.status.phase}"))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(out).To(Equal("Succeeded"), "metrics scrape pod did not complete")
	}, 3*time.Minute, 5*time.Second).Should(Succeed())

	out, err := utils.Run(exec.Command("kubectl", "logs", podName, "-n", namespace))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() {
		_, _ = utils.Run(exec.Command("kubectl", "delete", "pod", podName,
			"-n", namespace, "--ignore-not-found=true"))
	})
	return out
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}

// These specs exercise the controller against a real cluster: real Jobs, real
// pods, real timing. They cover the behaviour that unit tests with a fake clock
// cannot prove, namely that the whole loop converges end to end.
var _ = Describe("Task lifecycle", Ordered, func() {
	const (
		taskNamespace = "task-e2e"
		// Every minute, so a full cycle is observable within a spec timeout.
		everyMinute = "* * * * *"
	)

	applyTask := func(name, command, schedule string) {
		GinkgoHelper()
		manifest := fmt.Sprintf(`apiVersion: custom.io/v1alpha1
kind: Task
metadata:
  name: %s
  namespace: %s
spec:
  command: %q
  schedule: %q
`, name, taskNamespace, command, schedule)
		cmd := exec.Command("kubectl", "apply", "-f", "-")
		cmd.Stdin = strings.NewReader(manifest)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to apply Task %s", name)
	}

	taskField := func(name, jsonpath string) string {
		cmd := exec.Command("kubectl", "get", "task", name, "-n", taskNamespace,
			"-o", "jsonpath="+jsonpath)
		out, err := utils.Run(cmd)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(out)
	}

	BeforeAll(func() {
		// Ginkgo randomises the order of top-level containers, so this suite
		// cannot assume the Manager container has already installed the CRD and
		// deployed the controller. Both steps are idempotent, so running them
		// here makes this suite order-independent rather than implicitly
		// coupled to another container's BeforeAll.
		By("ensuring the CRD is installed")
		_, err := utils.Run(exec.Command("make", "install"))
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("ensuring the controller is deployed")
		_, err = utils.Run(exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage)))
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller")

		By("waiting for the controller to become available")
		_, err = utils.Run(exec.Command("kubectl", "wait", "--for=condition=Available",
			"deployment/task-controller-controller-manager", "-n", namespace, "--timeout=180s"))
		Expect(err).NotTo(HaveOccurred(), "The controller did not become available")

		_, _ = utils.Run(exec.Command("kubectl", "create", "ns", taskNamespace))
	})

	AfterAll(func() {
		cmd := exec.Command("kubectl", "delete", "ns", taskNamespace, "--ignore-not-found=true")
		_, _ = utils.Run(cmd)
	})

	It("should add the cleanup finalizer before running anything", func() {
		applyTask("hello", "echo Hello World", everyMinute)

		Eventually(func() string {
			return taskField("hello", "{.metadata.finalizers[0]}")
		}, 2*time.Minute, 2*time.Second).Should(Equal("custom.io/cleanup-jobs"))
	})

	It("should run a Task, record the outcome and delete the Job", func() {
		Eventually(func() string {
			return taskField("hello", "{.status.state}")
		}, 3*time.Minute, 5*time.Second).Should(Equal("Completed"))

		By("recording a durable success count")
		Expect(taskField("hello", "{.status.succeededCount}")).NotTo(Equal("0"))

		By("keeping the run in history even though the Job is deleted")
		Expect(taskField("hello", "{.status.recentRuns[0].result}")).To(Equal("Succeeded"))

		By("deleting the finished Job, as the exercise requires")
		Eventually(func() string {
			cmd := exec.Command("kubectl", "get", "jobs", "-n", taskNamespace,
				"-o", "jsonpath={.items[*].metadata.name}")
			out, _ := utils.Run(cmd)
			return strings.TrimSpace(out)
		}, 2*time.Minute, 5*time.Second).ShouldNot(ContainSubstring("hello-"))

		By("leaving activeJob empty at rest, so no spurious abort is recorded")
		Expect(taskField("hello", "{.status.activeJob}")).To(BeEmpty())
		Expect(taskField("hello", "{.status.recentRuns[0].result}")).NotTo(Equal("Aborted"))
	})

	It("should surface a failing command as Failed", func() {
		applyTask("failing", "exit 1", everyMinute)

		Eventually(func() string {
			return taskField("failing", "{.status.state}")
		}, 3*time.Minute, 5*time.Second).Should(Equal("Failed"))
		Expect(taskField("failing", "{.status.recentRuns[0].result}")).To(Equal("Failed"))
	})

	It("should reject a schedule that parses but can never run", func() {
		// February 30th: valid cron, impossible date.
		applyTask("impossible", "echo never", "0 0 30 2 *")

		Eventually(func() string {
			return taskField("impossible",
				`{.status.conditions[?(@.type=="ScheduleValid")].reason}`)
		}, 2*time.Minute, 2*time.Second).Should(Equal("Unsatisfiable"))

		Expect(taskField("impossible", "{.status.state}")).To(Equal("Failed"))
		By("clearing nextScheduleTime, so status never promises a run that cannot happen")
		Expect(taskField("impossible", "{.status.nextScheduleTime}")).To(BeEmpty())
	})

	It("should reject a sub-minute schedule that would collide on Job names", func() {
		applyTask("too-fast", "echo hi", "@every 30s")

		Eventually(func() string {
			return taskField("too-fast",
				`{.status.conditions[?(@.type=="ScheduleValid")].reason}`)
		}, 2*time.Minute, 2*time.Second).Should(Equal("IntervalTooShort"))
	})

	It("should delete a Task gracefully, leaving no orphan Jobs", func() {
		cmd := exec.Command("kubectl", "delete", "task", "hello", "-n", taskNamespace, "--timeout=90s")
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Deleting the Task should not hang on its finalizer")

		By("confirming the Task is really gone, not stuck Terminating")
		Eventually(func() string {
			return taskField("hello", "{.metadata.name}")
		}, time.Minute, 2*time.Second).Should(BeEmpty())

		By("confirming no Jobs were orphaned")
		cmd = exec.Command("kubectl", "get", "jobs", "-n", taskNamespace,
			"-o", "jsonpath={.items[*].metadata.name}")
		out, _ := utils.Run(cmd)
		Expect(out).NotTo(ContainSubstring("hello-"))
	})

	It("should expose zero-filled state gauges on /metrics", func() {
		// Scraped through a pod of this suite's own, so the spec does not
		// depend on another randomly-ordered container having built one.
		metrics := scrapeMetrics("curl-task-metrics")

		By("reporting the collector as healthy")
		Expect(metrics).To(ContainSubstring("task_controller_task_state_collector_up 1"))

		By("zero-filling every state, so alerts need no absent() handling")
		for _, state := range []string{"Pending", "Running", "Completed", "Failed"} {
			Expect(metrics).To(ContainSubstring(
				fmt.Sprintf("task_controller_tasks_by_state{state=%q}", state)))
		}

		By("exposing the totals the exercise asks for")
		Expect(metrics).To(ContainSubstring("task_controller_tasks "))
		Expect(metrics).To(ContainSubstring("task_controller_task_failures_total"))
	})
})
