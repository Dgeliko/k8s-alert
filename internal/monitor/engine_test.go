package monitor

import (
	"k8s-alert/internal/config"
	"testing"
	"time"
)

func cfg(t *testing.T, env string) *config.Config {
	t.Helper()
	c, e := config.Load([]byte("version: 1\ncluster: test\nenv-type: " + env + "\nnotifications:\n  repeat-interval: 30m\n  recovery-stable-for: 2m\ndestinations:\n  - name: pachca\n    type: pachca\n    enabled: true\n    url-file: /test/url\n"))
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func crash(pod, term string) Signal {
	return Signal{Key: "deploy-uid/app/ContainerCrash", Class: "ContainerCrash", Reason: "Error", Container: "app", PodName: pod, PodUID: pod, Workload: Resource{Kind: "Deployment", Name: "api", UID: "deploy-uid"}, Target: config.Target{Namespace: "default", PodName: pod, Kinds: []string{"Pod", "ReplicaSet", "Deployment"}}, TerminationID: term, NewTermination: true, Active: true}
}
func TestDevThresholdAndWorkloadMute(t *testing.T) {
	e := NewEngine(cfg(t, "dev"))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, term := range []string{"one", "two", "three"} {
		out := e.Reconcile([]Signal{crash("pod-a", term)}, true, now.Add(time.Duration(i)*time.Second))
		if (len(out) > 0) != (i == 2) {
			t.Fatalf("step %d requests %d", i, len(out))
		}
		if i == 2 {
			e.Ack(out[0].ID, "pachca", now.Add(2*time.Second))
		}
	}
	out := e.Reconcile([]Signal{crash("pod-b", "four")}, true, now.Add(time.Minute))
	if len(out) != 0 {
		t.Fatal("replacement bypassed cooldown")
	}
	out = e.Reconcile([]Signal{crash("pod-b", "four")}, true, now.Add(31*time.Minute))
	if len(out) != 1 {
		t.Fatal("timer reminder missing")
	}
}
func TestProdRecoveryRequiresStableHealth(t *testing.T) {
	e := NewEngine(cfg(t, "prod"))
	now := time.Now()
	out := e.Reconcile([]Signal{crash("pod", "one")}, true, now)
	if len(out) != 1 {
		t.Fatal("no initial alert")
	}
	e.Ack(out[0].ID, "pachca", now)
	e.Reconcile(nil, false, now.Add(time.Hour))
	if len(e.Incidents) != 1 {
		t.Fatal("unknown resolved incident")
	}
	if len(e.Reconcile(nil, true, now.Add(2*time.Hour))) != 0 {
		t.Fatal("premature recovery")
	}
	out = e.Reconcile(nil, true, now.Add(2*time.Hour+3*time.Minute))
	if len(out) != 1 || out[0].Payload.Status != "resolved" {
		t.Fatal("missing stable recovery")
	}
}
func TestRepeatedObservationDoesNotReachThreshold(t *testing.T) {
	e := NewEngine(cfg(t, "dev"))
	now := time.Now()
	s := crash("pod", "one")
	for i := 0; i < 5; i++ {
		if len(e.Reconcile([]Signal{s}, true, now.Add(time.Duration(i)*time.Second))) != 0 {
			t.Fatal("counted same termination twice")
		}
	}
}
func TestRestoreRetainsCooldown(t *testing.T) {
	c := cfg(t, "prod")
	e := NewEngine(c)
	now := time.Now()
	r := e.Reconcile([]Signal{crash("pod", "one")}, true, now)
	e.Ack(r[0].ID, "pachca", now)
	n := NewEngine(c)
	n.Restore(e.Export())
	if len(n.Reconcile([]Signal{crash("pod-b", "two")}, true, now.Add(time.Minute))) != 0 {
		t.Fatal("restart forgot cooldown")
	}
}

func TestDisabledCrashLoopDoesNotOverrideDevThreshold(t *testing.T) {
	c := cfg(t, "dev")
	c.Detectors = map[string]config.Detector{"crash-loop": {Enabled: false}}
	e := NewEngine(c)
	now := time.Now()
	s := crash("p", "one")
	s.Reason = "CrashLoopBackOff"
	e.Reconcile([]Signal{s}, true, now)
	if len(e.Reconcile([]Signal{s}, true, now.Add(3*time.Minute))) != 0 {
		t.Fatal("disabled crash-loop bypassed threshold")
	}
}
func TestTerminalRejectionDoesNotSendRecovery(t *testing.T) {
	e := NewEngine(cfg(t, "prod"))
	now := time.Now()
	r := e.Reconcile([]Signal{crash("p", "one")}, true, now)
	e.Reject(r[0].ID, "pachca", now)
	e.Reconcile(nil, true, now.Add(time.Minute))
	if len(e.Reconcile(nil, true, now.Add(4*time.Minute))) != 0 {
		t.Fatal("resolve sent for failed notification")
	}
}

func TestUnknownResetsRecoveryClock(t *testing.T) {
	e := NewEngine(cfg(t, "prod"))
	now := time.Now()
	r := e.Reconcile([]Signal{crash("p", "one")}, true, now)
	e.Ack(r[0].ID, "pachca", now)
	e.Reconcile(nil, true, now.Add(time.Second))
	e.Reconcile(nil, false, now.Add(5*time.Minute))
	if len(e.Reconcile(nil, true, now.Add(6*time.Minute))) != 0 {
		t.Fatal("unknown interval counted as healthy")
	}
}
func TestRecoveryWaitsForPendingFiringDelivery(t *testing.T) {
	e := NewEngine(cfg(t, "prod"))
	now := time.Now()
	r := e.Reconcile([]Signal{crash("p", "one")}, true, now)
	e.Reconcile(nil, true, now.Add(time.Minute))
	e.Reconcile(nil, true, now.Add(4*time.Minute))
	if e.Count() != 1 {
		t.Fatal("lost incident while firing still pending")
	}
	e.Ack(r[0].ID, "pachca", now.Add(5*time.Minute))
	out := e.Reconcile(nil, true, now.Add(6*time.Minute))
	if len(out) != 1 || out[0].Payload.Status != "resolved" {
		t.Fatal("late firing success did not get recovery")
	}
}
func TestFailedRecoveryIsNotRetriedEveryReconcile(t *testing.T) {
	e := NewEngine(cfg(t, "prod"))
	now := time.Now()
	r := e.Reconcile([]Signal{crash("p", "one")}, true, now)
	e.Ack(r[0].ID, "pachca", now)
	e.Reconcile(nil, true, now.Add(time.Minute))
	out := e.Reconcile(nil, true, now.Add(4*time.Minute))
	e.Reject(out[0].ID, "pachca", now.Add(4*time.Minute))
	if len(e.Reconcile(nil, true, now.Add(4*time.Minute+time.Second))) != 0 {
		t.Fatal("terminal recovery retried immediately")
	}
}
func TestRestoreResetsRecoveryClock(t *testing.T) {
	e := NewEngine(cfg(t, "prod"))
	now := time.Now()
	r := e.Reconcile([]Signal{crash("p", "one")}, true, now)
	e.Ack(r[0].ID, "pachca", now)
	e.Reconcile(nil, true, now.Add(time.Second))
	n := NewEngine(cfg(t, "prod"))
	n.Restore(e.Export())
	if len(n.Reconcile(nil, true, now.Add(time.Hour))) != 0 {
		t.Fatal("downtime counted as healthy")
	}
}
