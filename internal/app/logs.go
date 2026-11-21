package app

import (
	"context"
	"fmt"
	"io"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"k8s-alert/internal/notify"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"regexp"
	"strings"
	"sync"
	"time"
)

type cachedLog struct {
	Log monitor.Log
	At  time.Time
}
type Diagnostics struct {
	Client   kubernetes.Interface
	Config   *config.Config
	mu       sync.Mutex
	Cache    map[string]cachedLog
	Pending  map[string]bool
	Queue    chan monitor.Signal
	patterns []*regexp.Regexp
	literals []string
}

func NewDiagnostics(c kubernetes.Interface, cfg *config.Config) *Diagnostics {
	d := &Diagnostics{Client: c, Config: cfg, Cache: map[string]cachedLog{}, Pending: map[string]bool{}, Queue: make(chan monitor.Signal, 128)}
	for _, p := range append([]string{`(?i)authorization["\s:=]+(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+`, `(?i)(password|passwd|token|authorization|api[_-]?key)(["'\s:=]+)[^\s,"']+`, `eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`, `AKIA[0-9A-Z]{16}`, `[a-zA-Z][a-zA-Z0-9+.-]*://[^\s/@]+:[^\s/@]+@`}, cfg.Logs.Redact...) {
		d.patterns = append(d.patterns, regexp.MustCompile(p))
	}
	return d
}
func (d *Diagnostics) redact(s string) string {
	for _, v := range d.literals {
		if v != "" {
			s = strings.ReplaceAll(s, v, "[REDACTED]")
		}
	}
	for _, r := range d.patterns {
		s = r.ReplaceAllString(s, "[REDACTED]")
	}
	return s
}
func diagnosticKey(s monitor.Signal) string {
	if s.TerminationID != "" {
		return s.TerminationID
	}
	return s.PodUID + "/" + s.Container + "/" + s.Class
}
func (d *Diagnostics) Prefetch(s monitor.Signal) {
	if !d.Config.Logs.Enabled || s.Container == "" || s.PodName == "" || s.Class == "ImagePullFailure" {
		return
	}
	key := diagnosticKey(s)
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.Cache[key]; ok || d.Pending[key] {
		return
	}
	select {
	case d.Queue <- s:
		d.Pending[key] = true
	default:
	}
}
func (d *Diagnostics) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case s := <-d.Queue:
					l := d.collect(ctx, s)
					d.mu.Lock()
					key := diagnosticKey(s)
					d.Cache[key] = cachedLog{l, time.Now()}
					delete(d.Pending, key)
					if len(d.Cache) > 1000 {
						var oldest string
						var at time.Time
						for k, v := range d.Cache {
							if oldest == "" || v.At.Before(at) {
								oldest = k
								at = v.At
							}
						}
						delete(d.Cache, oldest)
					}
					d.mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
}
func (d *Diagnostics) collect(ctx context.Context, s monitor.Signal) monitor.Log {
	l := monitor.Log{Pod: s.PodName, Container: s.Container, Source: "previous"}
	if s.CurrentLogs {
		l.Source = "current"
	}
	ctx, cancel := context.WithTimeout(ctx, d.Config.Logs.Timeout.D())
	defer cancel()
	// A name can be reused (StatefulSet); never collect another Pod UID's logs.
	p, err := d.Client.CoreV1().Pods(s.Target.Namespace).Get(ctx, s.PodName, metav1.GetOptions{})
	if err != nil || string(p.UID) != s.PodUID {
		l.Error = "pod unavailable or replaced"
		return l
	}
	n := d.Config.Logs.TailLines
	limit := d.Config.Logs.MaxBytes
	stream, err := d.Client.CoreV1().Pods(s.Target.Namespace).GetLogs(s.PodName, &corev1.PodLogOptions{Container: s.Container, Previous: !s.CurrentLogs, Timestamps: true, TailLines: &n, LimitBytes: &limit}).Stream(ctx)
	if err != nil {
		l.Error = "container logs unavailable"
		return l
	}
	defer stream.Close()
	b, err := io.ReadAll(io.LimitReader(stream, limit+1))
	if err != nil {
		l.Error = "container logs read failed"
		return l
	}
	l.Truncated = int64(len(b)) >= limit
	l.Text = notify.Clip(d.redact(string(b)), int(limit))
	return l
}
func (d *Diagnostics) Enrich(ctx context.Context, r monitor.Request) monitor.Payload {
	p := r.Payload
	p.Events = append([]string(nil), p.Events...)
	p.Message = d.redact(p.Message)
	for i := range p.Events {
		p.Events[i] = notify.Clip(d.redact(p.Events[i]), 2048)
	}
	if p.Status == "resolved" || !d.Config.Logs.Enabled || r.Signal.Container == "" {
		return p
	}
	sources := r.Sources
	if len(sources) == 0 {
		sources = []monitor.Signal{r.Signal}
	}
	if len(sources) > d.Config.Logs.MaxPods {
		sources = sources[:d.Config.Logs.MaxPods]
	}
	for _, s := range sources {
		d.Prefetch(s)
	}
	deadline := time.NewTimer(d.Config.Logs.Timeout.D() + time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	finish := func() monitor.Payload {
		for _, s := range sources {
			key := diagnosticKey(s)
			d.mu.Lock()
			v, ok := d.Cache[key]
			d.mu.Unlock()
			if ok {
				p.Logs = append(p.Logs, v.Log)
			} else {
				p.Logs = append(p.Logs, monitor.Log{Pod: s.PodName, Container: s.Container, Error: "diagnostic unavailable or timed out"})
			}
		}
		return p
	}
	for {
		pending := false
		for _, s := range sources {
			d.mu.Lock()
			pending = pending || d.Pending[diagnosticKey(s)]
			d.mu.Unlock()
		}
		if !pending {
			return finish()
		}
		select {
		case <-ctx.Done():
			return finish()
		case <-deadline.C:
			return finish()
		case <-tick.C:
		}
	}
}
func (d *Diagnostics) String() string {
	return fmt.Sprintf("diagnostics for %s", strings.TrimSpace(d.Config.Cluster))
}

func (d *Diagnostics) Sanitize(s monitor.Signal) monitor.Signal {
	s.Events = append([]string(nil), s.Events...)
	for i := range s.Events {
		s.Events[i] = notify.Clip(d.redact(s.Events[i]), 2048)
	}
	s.Reason = notify.Clip(d.redact(s.Reason), 256)
	return s
}
