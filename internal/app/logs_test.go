package app

import (
	"k8s-alert/internal/config"
	"strings"
	"testing"
)

func TestRedactBearer(t *testing.T) {
	cfg, err := config.Load([]byte("version: 1\ncluster: test\n"))
	if err != nil {
		t.Fatal(err)
	}
	d := NewDiagnostics(nil, cfg)
	got := d.redact("Authorization: Bearer abc-super-secret")
	if strings.Contains(got, "abc-super-secret") {
		t.Fatal("bearer leaked")
	}
}
