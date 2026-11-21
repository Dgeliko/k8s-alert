package app

import (
	"context"
	"k8s-alert/internal/monitor"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
)

func TestCheckpointRoundTrip(t *testing.T) {
	p := Persistence{Client: fake.NewSimpleClientset(), Namespace: "alerts", Name: "state", Cluster: "test"}
	c := Checkpoint{Incidents: map[string]*monitor.Incident{"one": {Occurrences: 3}}}
	if err := p.Save(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	out, err := p.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if out.Incidents["one"].Occurrences != 3 {
		t.Fatal("lost state")
	}
	p.Cluster = "other"
	if _, err = p.Load(context.Background()); err == nil {
		t.Fatal("accepted other cluster checkpoint")
	}
}
