package notify

import (
	"context"
	"encoding/json"
	"io"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestPachcaPayloadHasSeparateLogs(t *testing.T) {
	var body map[string]any
	srv := memoryServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		w.Write([]byte("OK"))
	}))
	defer srv.Close()
	f := filepath.Join(t.TempDir(), "url")
	os.WriteFile(f, []byte(srv.URL), 0600)
	lines := []string{`error at "startup"`, `path C:\app`}
	err := srv.Sender.Send(context.Background(), config.Destination{Name: "p", Type: "pachca", URLFile: f}, monitor.Payload{
		Message: "failure",
		Logs:    []monitor.Log{{Pod: "api-1", Container: "app", Text: strings.Join(lines, "\n") + "\n"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if body["logs"] == nil || body["message"] == nil {
		t.Fatal(body)
	}
	rendered, ok := body["logs_json"].(string)
	if !ok {
		t.Fatal("missing JSON for the message template")
	}
	var logs []struct {
		Pod   string
		Lines []string
	}
	if err := json.Unmarshal([]byte(rendered), &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].Pod != "api-1" || !slices.Equal(logs[0].Lines, lines) {
		t.Fatalf("unexpected JSON logs: %s", rendered)
	}
}
func TestCredentialNotLeaked(t *testing.T) {
	f := filepath.Join(t.TempDir(), "url")
	os.WriteFile(f, []byte("https://user:secret@127.0.0.1:1/hook/token"), 0600)
	err := NewSender().Send(context.Background(), config.Destination{Type: "pachca", URLFile: f}, monitor.Payload{})
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "/hook/token") {
		t.Fatal("credential leak")
	}
}
func TestTelegramBodyAndRateLimit(t *testing.T) {
	f := filepath.Join(t.TempDir(), "token")
	os.WriteFile(f, []byte("fake-token"), 0600)
	srv := memoryServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		json.NewDecoder(r.Body).Decode(&b)
		if b["chat_id"] != "-1001" {
			t.Error(b)
		}
		if len([]rune(b["text"].(string))) > 4096 {
			t.Fatal("oversize")
		}
		w.Write([]byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":3}}`))
	}))
	defer srv.Close()
	s := srv.Sender
	s.TelegramBase = srv.URL
	err := s.Send(context.Background(), config.Destination{Type: "telegram", TokenFile: f, ChatID: "-1001"}, monitor.Payload{Message: strings.Repeat("x", 5000)})
	re, ok := err.(*DeliveryError)
	if !ok || !re.Retry || re.After.Seconds() != 3 {
		t.Fatalf("%v", err)
	}
}

func TestLargePachcaPayloadIsBounded(t *testing.T) {
	var size int
	srv := memoryServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		size = len(b)
		w.Write([]byte("OK"))
	}))
	defer srv.Close()
	f := filepath.Join(t.TempDir(), "url")
	os.WriteFile(f, []byte(srv.URL), 0600)
	p := monitor.Payload{Message: "failure", Logs: []monitor.Log{{Text: strings.Repeat("x", 16384)}, {Text: strings.Repeat("x", 16384)}, {Text: strings.Repeat("x", 16384)}}}
	err := srv.Sender.Send(context.Background(), config.Destination{Type: "pachca", URLFile: f}, p)
	if err != nil || size > 65536 {
		t.Fatalf("size=%d err=%v", size, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type memoryHTTP struct {
	URL    string
	Sender *Sender
}

func (m *memoryHTTP) Close() {}
func memoryServer(h http.Handler) *memoryHTTP {
	s := NewSender()
	s.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Result(), nil
	})
	return &memoryHTTP{URL: "http://memory.test", Sender: s}
}
