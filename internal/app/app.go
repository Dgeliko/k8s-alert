package app

import (
	"context"
	"fmt"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"k8s-alert/internal/notify"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

type App struct {
	Config *config.Config
	Client kubernetes.Interface
}

func (a *App) Run(ctx context.Context) error {
	cfg := a.Config
	metrics := NewMetrics()
	engine := monitor.NewEngine(cfg)
	detector := monitor.NewDetector(cfg)
	delivery := notify.NewManager(cfg)
	diag := NewDiagnostics(a.Client, cfg)
	delivery.Enrich = diag.Enrich
	delivery.Ack = engine.Ack
	delivery.Fail = engine.Fail
	delivery.Terminal = engine.Reject
	delivery.OnResult = func(s string) { metrics.Inc("delivery_" + s) }
	metrics.Incidents = engine.Count
	metrics.Queue = delivery.Depth
	if err := delivery.Sender.LoadCredentials(cfg.Destinations); err != nil {
		return err
	}
	diag.literals = delivery.Sender.Secrets()

	server := &http.Server{Addr: cfg.Listen, Handler: metrics.Handler(), ReadHeaderTimeout: 5 * time.Second}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(c)
	}()
	namespaces := cfg.ObservationNamespaces()
	type observedStore struct {
		kind      string
		namespace string
		informer  cache.SharedIndexInformer
	}
	var stores []observedStore
	var factories []informers.SharedInformerFactory
	for _, namespace := range namespaces {
		factory := informers.NewSharedInformerFactoryWithOptions(a.Client, 0, informers.WithNamespace(namespace), informers.WithTransform(stripCacheMetadata))
		factories = append(factories, factory)
		for kind, inf := range map[string]cache.SharedIndexInformer{
			"Pod":         factory.Core().V1().Pods().Informer(),
			"Deployment":  factory.Apps().V1().Deployments().Informer(),
			"ReplicaSet":  factory.Apps().V1().ReplicaSets().Informer(),
			"StatefulSet": factory.Apps().V1().StatefulSets().Informer(),
			"DaemonSet":   factory.Apps().V1().DaemonSets().Informer(),
			"Job":         factory.Batch().V1().Jobs().Informer(),
			"CronJob":     factory.Batch().V1().CronJobs().Informer(),
			"Event":       factory.Core().V1().Events().Informer(),
		} {
			stores = append(stores, observedStore{kind, namespace, inf})
		}
	}
	var verificationMu sync.Mutex
	var verified *monitor.Inventory
	var verifiedPods []*corev1.Pod
	var verifiedAt time.Time
	for _, store := range stores {
		inf := store.informer
		inf.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
			verificationMu.Lock()
			verified = nil
			verifiedPods = nil
			verificationMu.Unlock()
			metrics.Inc("watch_errors")
			slog.Warn("Kubernetes watch failed", "kind", store.kind, "namespace", store.namespace)
		})
	}
	for _, factory := range factories {
		factory.Start(ctx.Done())
	}
	for _, store := range stores {
		if !cache.WaitForCacheSync(ctx.Done(), store.informer.HasSynced) {
			return fmt.Errorf("required informer cache failed to sync: %s in namespace %q", store.kind, store.namespace)
		}
	}
	metrics.Ready.Store(true)
	state := Persistence{Client: a.Client, Name: cfg.Persistence.Name, Namespace: cfg.Persistence.Namespace, Cluster: cfg.Cluster}
	if state.Namespace == "" {
		state.Namespace = os.Getenv("POD_NAMESPACE")
	}
	if cfg.Persistence.Enabled && state.Namespace == "" {
		return fmt.Errorf("persistence.namespace or POD_NAMESPACE required")
	}
	if cfg.Persistence.Enabled {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		cp, err := state.Load(c)
		cancel()
		if err != nil {
			metrics.StateDegraded.Store(true)
			metrics.Inc("state_load_failures")
			slog.Warn("state load failed; monitoring continues with empty state", "error", err)
		} else {
			engine.Restore(cp.Incidents)
			delivery.Restore(cp.Jobs)
			for _, job := range cp.Jobs {
				if job != nil {
					if _, ok := delivery.Destinations[job.Destination]; ok {
						engine.Resume(job.Request, job.Destination)
					}
				}
			}
		}
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); diag.Run(workerCtx) }()
	go func() { defer workers.Done(); delivery.Run(workerCtx) }()
	defer func() {
		metrics.Ready.Store(false)
		stopWorkers()
		workers.Wait()
		if cfg.Persistence.Enabled {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := state.Save(c, Checkpoint{Incidents: engine.Export(), Jobs: delivery.Export()}); err != nil {
				slog.Error("final checkpoint failed", "error", err)
			}
		}
	}()
	// Events are coalesced; informer callbacks never perform API I/O.
	wake := make(chan struct{}, 1)
	for _, store := range stores {
		store.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{AddFunc: func(any) {
			select {
			case wake <- struct{}{}:
			default:
			}
		}, UpdateFunc: func(any, any) {
			select {
			case wake <- struct{}{}:
			default:
			}
		}, DeleteFunc: func(any) {
			select {
			case wake <- struct{}{}:
			default:
			}
		}})
	}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	checkpoint := time.NewTicker(10 * time.Second)
	defer checkpoint.Stop()
	probe := time.NewTicker(15 * time.Second)
	defer probe.Stop()
	// Fresh full observations, bounded by deadline, confirm recovery.
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-probe.C:
				if engine.Count() == 0 {
					verificationMu.Lock()
					verified = nil
					verifiedPods = nil
					verificationMu.Unlock()
					continue
				}
				// Each additional namespace adds seven requests on a rate-limited client.
				timeout := max(10*time.Second, time.Duration(len(namespaces))*time.Second+5*time.Second)
				c, cancel := context.WithTimeout(workerCtx, timeout)
				v, pods, err := freshInventory(c, a.Client, namespaces)
				cancel()
				verificationMu.Lock()
				if err != nil {
					verified = nil
					verifiedPods = nil
					metrics.Inc("verification_errors")
				} else {
					verified = v
					verifiedPods = pods
					verifiedAt = time.Now()
				}
				verificationMu.Unlock()
			}
		}
	}()
	reconcile := func() {
		now := time.Now()
		v := monitor.NewInventory()
		var pods []*corev1.Pod
		var events []*corev1.Event
		for _, store := range stores {
			for _, obj := range store.informer.GetStore().List() {
				r, ok := obj.(runtime.Object)
				if !ok {
					continue
				}
				if store.kind == "Event" {
					if e, ok := obj.(*corev1.Event); ok {
						events = append(events, e)
					}
					continue
				}
				v.Add(store.kind, r)
				if p, ok := obj.(*corev1.Pod); ok {
					pods = append(pods, p)
				}
			}
		}
		verificationMu.Lock()
		fresh := verified
		freshPods := verifiedPods
		known := fresh != nil && now.Sub(verifiedAt) < 30*time.Second
		verificationMu.Unlock()
		engine.RecoveryCheck = func(s monitor.Signal) (bool, bool) {
			if !known {
				return false, false
			}
			return monitor.Recovery(fresh, freshPods, s)
		}
		var signals []monitor.Signal
		for _, p := range pods {
			signals = append(signals, detector.Pod(p, v, now)...)
		}
		signals = detector.ApplyProbeContext(signals, events, now)
		signals = append(signals, detector.Workloads(v, now)...)
		signals = append(signals, detector.Events(events, v, now)...)
		for i := range signals {
			signals[i] = diag.Sanitize(signals[i])
		}
		for _, s := range signals {
			if s.NewTermination || s.Container != "" {
				diag.Prefetch(s)
			}
		}
		for _, r := range engine.Reconcile(signals, known, now) {
			delivery.Enqueue(r)
		}
		detector.Cleanup(signals)
		detector.Prune(v)
	}
	reconcile()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-serverErr:
			if err != http.ErrServerClosed {
				return fmt.Errorf("HTTP server: %w", err)
			}
		case <-tick.C:
			reconcile()
		case <-wake:
			reconcile()
		case <-checkpoint.C:
			if cfg.Persistence.Enabled {
				c, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := state.Save(c, Checkpoint{Incidents: engine.Export(), Jobs: delivery.Export()})
				cancel()
				metrics.StateDegraded.Store(err != nil)
				if err != nil {
					metrics.Inc("state_save_failures")
					slog.Warn("checkpoint failed", "error", err)
				}
			}
		}
	}
}

// Informers own incoming objects before the transform runs. Keep only the
// metadata needed for detection and ownership resolution in the cache.
func stripCacheMetadata(obj any) (any, error) {
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	m.SetManagedFields(nil)
	m.SetAnnotations(nil)
	return obj, nil
}
