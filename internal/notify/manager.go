package notify

import (
	"context"
	"encoding/json"
	"errors"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

type Job struct {
	Request       monitor.Request `json:"request"`
	Destination   string          `json:"destination"`
	Created, Next time.Time
	Attempts      int
	InFlight      bool `json:"-"`
}
type Manager struct {
	mu           sync.Mutex
	Jobs         map[string]*Job
	Destinations map[string]config.Destination
	Sender       *Sender
	Enrich       func(context.Context, monitor.Request) monitor.Payload
	Ack          func(string, string, time.Time)
	Fail         func(string, string)
	Terminal     func(string, string, time.Time)
	OnResult     func(string)
}

func NewManager(c *config.Config) *Manager {
	m := &Manager{Jobs: map[string]*Job{}, Destinations: map[string]config.Destination{}, Sender: NewSender()}
	for _, d := range c.Destinations {
		if d.Enabled {
			m.Destinations[d.Name] = d
		}
	}
	return m
}
func (m *Manager) Enqueue(r monitor.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, name := range r.Destinations {
		key := r.ID + "/" + name
		if _, ok := m.Jobs[key]; ok {
			continue
		}
		if _, ok := m.Destinations[name]; !ok {
			if m.Fail != nil {
				m.Fail(r.ID, name)
			}
			continue
		}
		if len(m.Jobs) >= 256 {
			if m.OnResult != nil {
				m.OnResult("queue_full")
			}
			if m.Fail != nil {
				m.Fail(r.ID, name)
			}
			continue
		}
		m.Jobs[key] = &Job{Request: r, Destination: name, Created: time.Now(), Next: time.Now()}
	}
}
func (m *Manager) Run(ctx context.Context) {
	tasks := make(chan string, 4)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range tasks {
				m.send(ctx, key)
			}
		}()
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	defer func() { close(tasks); wg.Wait() }()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.mu.Lock()
			for key, j := range m.Jobs {
				if j.InFlight || now.Before(j.Next) {
					continue
				}
				select {
				case tasks <- key:
					j.InFlight = true
				default:
				}
			}
			m.mu.Unlock()
		}
	}
}
func (m *Manager) send(ctx context.Context, key string) {
	m.mu.Lock()
	j := m.Jobs[key]
	if j == nil {
		m.mu.Unlock()
		return
	}
	cp := *j
	d, ok := m.Destinations[j.Destination]
	m.mu.Unlock()
	if !ok {
		return
	}
	payload := cp.Request.Payload
	if m.Enrich != nil && cp.Attempts == 0 {
		payload = m.Enrich(ctx, cp.Request)
	}
	// Keep the exact rendered diagnostics for retry/checkpoint.
	m.mu.Lock()
	if live := m.Jobs[key]; live != nil {
		live.Request.Payload = payload
	}
	m.mu.Unlock()
	err := m.Sender.Send(ctx, d, payload)
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	j = m.Jobs[key]
	if j == nil {
		return
	}
	j.InFlight = false
	if ctx.Err() != nil {
		return
	}
	p := j.Request.Payload
	fields := []any{"destination", d.Name, "notification_id", p.NotificationID, "incident_id", p.IncidentID, "status", p.Status, "namespace", p.Namespace, "resource_kind", p.Resource.Kind, "resource", p.Resource.Name, "workload_kind", p.Workload.Kind, "workload", p.Workload.Name, "container", p.Container, "reason", p.Reason, "failure_class", p.Class, "attempt", j.Attempts + 1}
	if err == nil {
		delete(m.Jobs, key)
		if m.Ack != nil {
			m.Ack(j.Request.ID, j.Destination, now)
		}
		if m.OnResult != nil {
			m.OnResult("success")
		}
		slog.Info("notification sent", fields...)
		return
	}
	var de *DeliveryError
	retry := errors.As(err, &de) && de.Retry
	if !retry || now.Sub(j.Created) > time.Hour {
		delete(m.Jobs, key)
		if m.Terminal != nil {
			m.Terminal(j.Request.ID, j.Destination, now)
		}
		if m.OnResult != nil {
			m.OnResult("terminal")
		}
		slog.Error("notification permanently failed", append(fields, "error", err)...)
		return
	}
	j.Attempts++
	delay := time.Second * time.Duration(1<<min(j.Attempts, 8))
	delay += time.Duration(rand.IntN(1000)) * time.Millisecond
	if de.After > delay {
		delay = de.After
	}
	j.Next = now.Add(delay)
	if m.OnResult != nil {
		m.OnResult("retry")
	}
	slog.Warn("notification retry scheduled", append(fields, "delay", delay, "error", err)...)
}
func (m *Manager) Export() map[string]*Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _ := json.Marshal(m.Jobs)
	var out map[string]*Job
	json.Unmarshal(b, &out)
	return out
}
func (m *Manager) Restore(jobs map[string]*Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, j := range jobs {
		if j == nil {
			continue
		}
		if _, ok := m.Destinations[j.Destination]; ok {
			j.InFlight = false
			m.Jobs[k] = j
		} else if m.Fail != nil {
			m.Fail(j.Request.ID, j.Destination)
		}
	}
}
func (m *Manager) Depth() int { m.mu.Lock(); defer m.mu.Unlock(); return len(m.Jobs) }
