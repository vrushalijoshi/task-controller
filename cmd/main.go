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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"net/http"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	customv1alpha1 "github.com/vrushalijoshi/task-controller/api/v1alpha1"
	"github.com/vrushalijoshi/task-controller/internal/controller"
	"github.com/vrushalijoshi/task-controller/internal/metrics"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")

	// cacheSyncTimeout bounds the cache-sync checks used by the readiness probe
	// and the metrics collector. Without a deadline a probe arriving before the
	// caches sync would block the handler rather than reporting not-ready.
	cacheSyncTimeout = 5 * time.Second
)

// cachedTaskLister adapts the manager's cache to the narrow interface the
// metrics collector needs, which keeps the collector testable without a manager.
type cachedTaskLister struct {
	reader client.Reader
}

func (l *cachedTaskLister) List(ctx context.Context, out *customv1alpha1.TaskList) error {
	return l.reader.List(ctx, out)
}

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(customv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var webhookPort int
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "Port the webhook server listens on. "+
		"Defaults to 9443. Set -1 to disable the webhook server.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	var maxConcurrentReconciles int
	var reconcileQPS float64
	var reconcileBurst int
	var watchNamespace string
	var kubeAPIQPS float64
	var kubeAPIBurst int
	var leaseDuration, renewDeadline, retryPeriod time.Duration
	flag.IntVar(&maxConcurrentReconciles, "max-concurrent-reconciles", 4,
		"Number of Tasks reconciled in parallel. The workqueue serialises per Task, so "+
			"this only parallelises across different Tasks.")
	flag.Float64Var(&reconcileQPS, "reconcile-qps", 500,
		"Queue-wide reconcile rate limit. controller-runtime's default bucket caps the "+
			"whole controller at 10 reconciles/second, which is far too low at scale.")
	flag.IntVar(&reconcileBurst, "reconcile-burst", 1000, "Queue-wide reconcile burst.")
	flag.StringVar(&watchNamespace, "watch-namespace", "",
		"Restrict the cache and the controller to a single namespace. Empty means all "+
			"namespaces. Useful for sharding across several controller deployments.")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", 50,
		"Client-side QPS limit to the API server. The client-go default of 20 throttles "+
			"Job creates and status writes well before the API server does.")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", 100, "Client-side burst limit to the API server.")
	flag.DurationVar(&leaseDuration, "leader-elect-lease-duration", 15*time.Second,
		"How long a non-leader waits before attempting to acquire leadership.")
	flag.DurationVar(&renewDeadline, "leader-elect-renew-deadline", 10*time.Second,
		"How long the leader retries refreshing leadership before giving up. Must be less "+
			"than the lease duration: the old leader stops reconciling here, strictly before "+
			"a new leader can acquire.")
	flag.DurationVar(&retryPeriod, "leader-elect-retry-period", 2*time.Second,
		"How long clients wait between leader election actions.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
		Port:    webhookPort,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	restConfig := ctrl.GetConfigOrDie()
	// client-go defaults to QPS 20 / burst 30, which throttles the controller
	// long before the API server would and shows up only as client-side latency.
	restConfig.QPS = float32(kubeAPIQPS)
	restConfig.Burst = kubeAPIBurst

	cacheOpts := cache.Options{
		// Scope the Job informer to Jobs we own, so controller memory scales
		// with our Jobs rather than with every Job in the cluster.
		ByObject: map[client.Object]cache.ByObject{
			&batchv1.Job{}: {
				Label: labels.SelectorFromSet(labels.Set{
					controller.LabelManagedBy: controller.ManagedByValue,
				}),
			},
		},
		// managedFields is often a third of an object's bytes and is never read
		// here. Stripping it matters at tens of thousands of Tasks.
		DefaultTransform: cache.TransformStripManagedFields(),
		// A dropped watch event would otherwise strand a Task until its next
		// slot, which for @daily is 24 hours. An explicit period makes that
		// bound a decision rather than a library default.
		SyncPeriod: ptr.To(30 * time.Minute),
	}
	if watchNamespace != "" {
		cacheOpts.DefaultNamespaces = map[string]cache.Config{watchNamespace: {}}
		setupLog.Info("Watching a single namespace", "namespace", watchNamespace)
	}

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		Cache:                  cacheOpts,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "task-controller.custom.io",
		LeaseDuration:          &leaseDuration,
		RenewDeadline:          &renewDeadline,
		RetryPeriod:            &retryPeriod,
		// The binary exits as soon as the manager stops, so releasing the lease
		// on shutdown is safe and makes a rollout hand over in ~2s instead of
		// waiting out the full lease duration.
		LeaderElectionReleaseOnCancel: true,
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	metricsRecorder := metrics.NewRecorderForManager()

	if err := (&controller.TaskReconciler{
		Client:                  mgr.GetClient(),
		Scheme:                  mgr.GetScheme(),
		APIReader:               mgr.GetAPIReader(),
		Recorder:                mgr.GetEventRecorder("task-controller"),
		Clock:                   clock.RealClock{},
		Metrics:                 metricsRecorder,
		MaxConcurrentReconciles: maxConcurrentReconciles,
		ReconcileQPS:            reconcileQPS,
		ReconcileBurst:          reconcileBurst,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "task")
		os.Exit(1)
	}

	// State gauges are computed at scrape time from the same cache the
	// reconciler reads, so they are derived from the source of truth and cannot
	// drift or leak series for deleted Tasks.
	//
	// The synced check matters with more than one replica: a standby that lost
	// the leader election still serves /metrics, but its cache never starts.
	// Without this it would publish all-zero gauges and Prometheus would scrape
	// two disagreeing targets for the same data.
	collector := metrics.NewStateCollector(
		&cachedTaskLister{reader: mgr.GetCache()},
		func() bool {
			ctx, cancel := context.WithTimeout(context.Background(), cacheSyncTimeout)
			defer cancel()
			return mgr.GetCache().WaitForCacheSync(ctx)
		},
	)
	if err := metrics.RegisterStateCollector(collector); err != nil {
		setupLog.Error(err, "Failed to register the Task state collector")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", func(req *http.Request) error {
		ctx, cancel := context.WithTimeout(req.Context(), cacheSyncTimeout)
		defer cancel()
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return errors.New("informer caches have not synced")
		}
		return nil
	}); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
