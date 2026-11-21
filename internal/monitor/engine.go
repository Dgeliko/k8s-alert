package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"k8s-alert/internal/config"
	"sort"
	"sync"
	"time"
)

type Engine struct {
	mu            sync.Mutex
	Config        *config.Config
	Incidents     map[string]*Incident
	RecoveryCheck func(Signal) (healthy, gone bool)
}

func NewEngine(c *config.Config) *Engine {
	return &Engine{Config: c, Incidents: map[string]*Incident{}}
}
func digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:12]) }
func (e *Engine) Reconcile(signals []Signal, known bool, now time.Time) []Request {
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := map[string]bool{}
	for _, s := range signals {
		if !s.Active || !e.Config.Allows(s.Target) {
			continue
		}
		seen[s.Key] = true
		enabled, _ := e.Config.Detector(s.Class)
		if !enabled {
			continue
		}
		in := e.Incidents[s.Key]
		if in == nil || in.Resolved {
			if s.ContinueOnly {
				continue
			}
			in = &Incident{Signal: s, FirstSeen: now, Failures: map[string]time.Time{}, LastSent: map[string]time.Time{}, Pending: map[string]string{}}
			e.Incidents[s.Key] = in
		}
		if in.Sources == nil {
			in.Sources = map[string]Signal{}
		}
		if s.Container != "" {
			in.Sources[s.PodUID+"/"+s.Container] = s
		}
		if len(in.Sources) > e.Config.Logs.MaxPods {
			var oldest string
			var at time.Time
			for k, src := range in.Sources {
				if oldest == "" || src.Since.Before(at) {
					oldest = k
					at = src.Since
				}
			}
			delete(in.Sources, oldest)
		}
		in.Signal = s
		in.LastSeen = now
		in.HealthySince = time.Time{}
		if s.NewTermination && s.TerminationID != "" {
			if _, ok := in.Failures[s.TerminationID]; !ok {
				in.Failures[s.TerminationID] = now
				in.Occurrences++
			}
		}
	}
	var requests []Request
	for key, in := range e.Incidents {
		if !e.Config.Allows(in.Signal.Target) {
			delete(e.Incidents, key)
			continue
		}
		policy := e.Config.PolicyFor(in.Signal.Target)
		// Keep recent unique terminations only. Resyncs never increase count.
		for id, t := range in.Failures {
			if now.Sub(t) > policy.Window {
				delete(in.Failures, id)
			}
		}
		if !known {
			in.HealthySince = time.Time{}
			if !seen[key] {
				continue
			}
		}
		active := seen[key]
		if !active && !in.Resolved {
			healthy, gone := true, false
			if e.RecoveryCheck != nil {
				healthy, gone = e.RecoveryCheck(in.Signal)
			}
			if !healthy {
				in.HealthySince = time.Time{}
				continue
			}
			if in.HealthySince.IsZero() {
				in.HealthySince = now
			}
			if !gone && now.Sub(in.HealthySince) < policy.Stable {
				continue
			}
			if !in.Firing {
				if len(in.Failures) == 0 {
					delete(e.Incidents, key)
				}
				continue
			}
			in.Resolved = true
			if gone {
				in.Signal.Reason = "resource_deleted"
			}
		}
		if in.Resolved {
			if !policy.Recovery || len(in.LastSent) == 0 {
				if len(in.Pending) > 0 {
					continue
				}
				delete(e.Incidents, key)
				continue
			}
			r := e.request(key, in, "resolved", now)
			if len(r.Destinations) > 0 {
				requests = append(requests, r)
			}
			all := true
			for _, name := range e.Config.EnabledNames() {
				if _, sent := in.LastSent[name]; sent {
					all = false
				}
			}
			if all && len(in.Pending) == 0 {
				delete(e.Incidents, key)
			}
			continue
		}
		if !active {
			continue
		}
		if !in.Firing {
			enabled, delay := e.Config.Detector(in.Signal.Class)
			if !enabled {
				continue
			}
			age := now.Sub(in.FirstSeen)
			if !in.Signal.Since.IsZero() {
				age = now.Sub(in.Signal.Since)
			}
			switch in.Signal.Class {
			case "ContainerCrash", "OOMKilled", "ProbeFailure":
				loopEnabled, loopDelay := e.Config.Detector("CrashLoop")
				if len(in.Failures) < policy.Threshold && !(loopEnabled && in.Signal.Reason == "CrashLoopBackOff" && age >= loopDelay) {
					continue
				}
			default:
				if age < delay {
					continue
				}
			}
			in.Firing = true
		}
		r := e.request(key, in, "firing", now)
		if len(r.Destinations) > 0 || len(e.Config.EnabledNames()) == 0 {
			requests = append(requests, r)
		}
	}
	return requests
}
func (e *Engine) request(key string, in *Incident, status string, now time.Time) Request {
	id := digest(key + in.FirstSeen.UTC().Format(time.RFC3339Nano) + status + now.UTC().Format(time.RFC3339Nano))
	var names []string
	p := e.Config.PolicyFor(in.Signal.Target)
	for _, name := range e.Config.EnabledNames() {
		if in.Pending[name] != "" {
			continue
		}
		last, ok := in.LastSent[name]
		if attempt, tried := in.LastAttempt[name]; tried && (!ok || attempt.After(last)) {
			last = attempt
			if status != "resolved" {
				ok = true
			}
		}
		if status == "resolved" {
			if !ok {
				continue
			}
		} else if ok && now.Sub(last) < p.Repeat {
			continue
		}
		names = append(names, name)
		in.Pending[name] = id
		if in.PendingStatus == nil {
			in.PendingStatus = map[string]string{}
		}
		in.PendingStatus[name] = status
	}
	s := in.Signal
	message := fmt.Sprintf("%s %s/%s: %s (%s), container=%s, pod=%s, occurrences=%d", status, s.Target.Namespace, s.Workload.Name, s.Class, s.Reason, s.Container, s.PodName, in.Occurrences)
	if status == "resolved" && s.Reason == "resource_deleted" {
		message = "Resource deleted: " + s.Target.Namespace + "/" + s.Workload.Name
	}
	resource := s.Workload
	if s.PodName != "" {
		resource = Resource{Kind: "Pod", Name: s.PodName, UID: s.PodUID, Namespace: s.Target.Namespace}
	}
	sources := []Signal{}
	for _, src := range in.Sources {
		sources = append(sources, src)
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].PodName < sources[j].PodName })
	return Request{Sources: sources, ID: id, Destinations: names, Signal: s, Payload: Payload{SchemaVersion: 1, IncidentID: digest(key + in.FirstSeen.UTC().Format(time.RFC3339Nano)), NotificationID: id, Status: status, Cluster: e.Config.Cluster, Env: e.Config.Env, Namespace: s.Target.Namespace, Resource: resource, Workload: s.Workload, Container: s.Container, Reason: s.Reason, Class: s.Class, ExitCode: s.ExitCode, RestartCount: s.RestartCount, FirstSeen: in.FirstSeen, LastSeen: in.LastSeen, Occurrences: in.Occurrences, Message: message, Logs: []Log{}, Events: s.Events}}
}
func (e *Engine) Ack(id, name string, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, in := range e.Incidents {
		if in.Pending[name] == id {
			delete(in.Pending, name)
			if in.PendingStatus[name] == "resolved" {
				delete(in.LastSent, name)
			} else {
				in.LastSent[name] = now
			}
			delete(in.PendingStatus, name)
			return
		}
	}
}
func (e *Engine) Fail(id, name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, in := range e.Incidents {
		if in.Pending[name] == id {
			delete(in.Pending, name)
		}
	}
}
func (e *Engine) Export() map[string]*Incident {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, _ := json.Marshal(e.Incidents)
	var out map[string]*Incident
	json.Unmarshal(b, &out)
	return out
}
func (e *Engine) Restore(s map[string]*Incident) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s == nil {
		return
	}
	for k, in := range s {
		if in == nil {
			continue
		}
		if in.Failures == nil {
			in.Failures = map[string]time.Time{}
		}
		if in.LastSent == nil {
			in.LastSent = map[string]time.Time{}
		}
		if in.Pending == nil {
			in.Pending = map[string]string{}
		}
		in.HealthySince = time.Time{}
		e.Incidents[k] = in
	}
}
func (e *Engine) Count() int { e.mu.Lock(); defer e.mu.Unlock(); return len(e.Incidents) }

// Reject records a terminal delivery attempt to avoid retrying a permanent
// HTTP 4xx on every reconcile. A future reminder may try again after policy.Repeat.
func (e *Engine) Reject(id, name string, now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, in := range e.Incidents {
		if in.Pending[name] == id {
			delete(in.Pending, name)
			if in.PendingStatus[name] == "resolved" {
				delete(in.LastSent, name)
			} else {
				if in.LastAttempt == nil {
					in.LastAttempt = map[string]time.Time{}
				}
				in.LastAttempt[name] = now
			}
			delete(in.PendingStatus, name)
			return
		}
	}
}

func (e *Engine) Resume(r Request, name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if in := e.Incidents[r.Signal.Key]; in != nil {
		if in.Pending == nil {
			in.Pending = map[string]string{}
		}
		in.Pending[name] = r.ID
		if in.PendingStatus == nil {
			in.PendingStatus = map[string]string{}
		}
		in.PendingStatus[name] = r.Payload.Status
	}
}
