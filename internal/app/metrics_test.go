package app

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetricsDoesNotHoldLockAcrossCallbacks(t *testing.T) {
	m := NewMetrics()
	m.Queue = func() int { m.Inc("callback"); return 0 }
	done := make(chan struct{})
	go func() {
		m.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("metrics deadlocked")
	}
}
