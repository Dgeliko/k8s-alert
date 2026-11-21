package notify

import (
	"bytes"
	"context"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeliveryLogDescribesNotificationWithoutSensitivePayload(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	srv := memoryServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	f := filepath.Join(t.TempDir(), "url")
	if err := os.WriteFile(f, []byte(srv.URL+"/secret-webhook"), 0600); err != nil {
		t.Fatal(err)
	}
	m := NewManager(&config.Config{Destinations: []config.Destination{{Name: "pachca", Type: "pachca", Enabled: true, URLFile: f}}})
	m.Sender = srv.Sender
	m.Enqueue(monitor.Request{ID: "id-1", Destinations: []string{"pachca"}, Payload: monitor.Payload{Status: "firing", Namespace: "team01", Resource: monitor.Resource{Kind: "Pod", Name: "api-123"}, Reason: "OOMKilled", Message: "private message", Logs: []monitor.Log{{Text: "private log"}}}})
	m.send(context.Background(), "id-1/pachca")
	for _, value := range []string{"notification sent", "team01", "api-123", "OOMKilled", "firing", "pachca"} {
		if !strings.Contains(logs.String(), value) {
			t.Errorf("missing %q in %s", value, logs.String())
		}
	}
	for _, value := range []string{"secret-webhook", "private message", "private log"} {
		if strings.Contains(logs.String(), value) {
			t.Errorf("sensitive content %q logged", value)
		}
	}
}

func TestDestinationFailureDoesNotReplaySuccess(t *testing.T) {
	counts := map[string]int{}
	srv := memoryServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts[r.URL.Path]++
		if r.URL.Path == "/retry" && counts[r.URL.Path] == 1 {
			w.WriteHeader(500)
		} else {
			w.Write([]byte("OK"))
		}
	}))
	var ds []config.Destination
	for _, name := range []string{"ok", "retry"} {
		f := filepath.Join(t.TempDir(), "url")
		os.WriteFile(f, []byte(srv.URL+"/"+name), 0600)
		ds = append(ds, config.Destination{Name: name, Type: "generic", Enabled: true, URLFile: f})
	}
	m := NewManager(&config.Config{Destinations: ds})
	m.Sender = srv.Sender
	m.Enqueue(monitor.Request{ID: "one", Destinations: []string{"ok", "retry"}, Payload: monitor.Payload{Message: "failure"}})
	m.send(context.Background(), "one/ok")
	m.send(context.Background(), "one/retry")
	if m.Depth() != 1 {
		t.Fatal("successful destination not removed")
	}
	m.send(context.Background(), "one/retry")
	if m.Depth() != 0 || counts["/ok"] != 1 || counts["/retry"] != 2 {
		t.Fatal(counts)
	}
}
func TestDisabledDestinationRequiresNoCredentials(t *testing.T) {
	s := NewSender()
	if err := s.LoadCredentials([]config.Destination{{Type: "telegram", Enabled: false, TokenFile: "/nonexistent"}}); err != nil {
		t.Fatal(err)
	}
}
func TestCredentialsAreSnapshotUntilRestart(t *testing.T) {
	var path string
	srv := memoryServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { path = r.URL.Path; w.Write([]byte("OK")) }))
	f := filepath.Join(t.TempDir(), "url")
	os.WriteFile(f, []byte(srv.URL+"/initial"), 0600)
	d := config.Destination{Type: "generic", Enabled: true, URLFile: f}
	if err := srv.Sender.LoadCredentials([]config.Destination{d}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(f, []byte(srv.URL+"/changed"), 0600)
	if err := srv.Sender.Send(context.Background(), d, monitor.Payload{}); err != nil {
		t.Fatal(err)
	}
	if path != "/initial" {
		t.Fatal("credential changed without restart")
	}
}
