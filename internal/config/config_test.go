package config

import "testing"

func TestProfilesAndDisabledTelegram(t *testing.T) {
	for _, tc := range []struct {
		env   string
		count int
	}{{"dev", 3}, {"prod", 1}} {
		c, err := Load([]byte("version: 1\ncluster: test\nenv-type: " + tc.env + "\ndestinations:\n  - name: telegram\n    type: telegram\n    enabled: false\n"))
		if err != nil {
			t.Fatal(err)
		}
		if got := c.PolicyFor(Target{}).Threshold; got != tc.count {
			t.Fatalf("%s threshold=%d", tc.env, got)
		}
	}
}
func TestStrictConfig(t *testing.T) {
	for _, s := range []string{"unknown: true", "version: 1\ncluster: x\nenv-type: typo", "version: 1\ncluster: x\nlogs:\n  tail-lines: -1"} {
		if _, err := Load([]byte(s)); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
func TestFiltersInspectOwnerChain(t *testing.T) {
	c, err := Load([]byte("version: 1\ncluster: test\nfilters:\n  kinds:\n    exclude: [Job, CronJob]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Allows(Target{Namespace: "default", PodName: "task-123", Kinds: []string{"Pod", "Job", "CronJob"}}) {
		t.Fatal("job pod accepted")
	}
	if !c.Allows(Target{Namespace: "default", PodName: "api-123", Kinds: []string{"Pod", "ReplicaSet", "Deployment"}}) {
		t.Fatal("deployment rejected")
	}
}
func TestOverrideAndExclusion(t *testing.T) {
	c, err := Load([]byte("version: 1\ncluster: test\nfilters:\n  namespaces:\n    include: [team-*]\n    exclude: [team-ignore]\noverrides:\n  - match:\n      namespaces: [team-dev]\n    restarts:\n      threshold: 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Allows(Target{Namespace: "team-ignore"}) {
		t.Fatal("exclude lost")
	}
	if c.PolicyFor(Target{Namespace: "team-dev"}).Threshold != 7 {
		t.Fatal("override missing")
	}
}
