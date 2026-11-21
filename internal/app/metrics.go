package app

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

type Metrics struct {
	Ready         atomic.Bool
	StateDegraded atomic.Bool
	mu            sync.Mutex
	Counts        map[string]uint64
	Incidents     func() int
	Queue         func() int
}

func NewMetrics() *Metrics         { return &Metrics{Counts: map[string]uint64{}} }
func (m *Metrics) Inc(name string) { m.mu.Lock(); m.Counts[name]++; m.mu.Unlock() }
func (m *Metrics) Handler() http.Handler {
	h := http.NewServeMux()
	h.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	h.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !m.Ready.Load() {
			http.Error(w, "caches not ready", 503)
			return
		}
		w.Write([]byte("ready\n"))
	})
	h.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		m.mu.Lock()
		counts := make(map[string]uint64, len(m.Counts))
		for k, v := range m.Counts {
			counts[k] = v
		}
		m.mu.Unlock()
		for k, v := range counts {
			fmt.Fprintf(w, "k8s_alert_%s_total %d\n", k, v)
		}
		if m.Incidents != nil {
			fmt.Fprintf(w, "k8s_alert_incidents %d\n", m.Incidents())
		}
		if m.Queue != nil {
			fmt.Fprintf(w, "k8s_alert_delivery_queue %d\n", m.Queue())
		}
		n := 0
		if m.StateDegraded.Load() {
			n = 1
		}
		fmt.Fprintf(w, "k8s_alert_state_degraded %d\n", n)
	})
	return h
}
