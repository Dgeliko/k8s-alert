package notify

import (
	"context"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

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
