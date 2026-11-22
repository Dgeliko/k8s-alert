package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"k8s-alert/internal/config"
	"k8s-alert/internal/monitor"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type DeliveryError struct {
	Retry   bool
	After   time.Duration
	Code    int
	Message string
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("delivery failed: %s (status %d)", e.Message, e.Code)
}

type Sender struct {
	mu           sync.Mutex
	credentials  map[string]string
	Client       *http.Client
	TelegramBase string
}

func NewSender() *Sender {
	return &Sender{Client: &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, TelegramBase: "https://api.telegram.org"}
}
func credential(file string) (string, error) {
	b, e := os.ReadFile(file)
	if e != nil {
		return "", &DeliveryError{Message: "credential file unavailable", Retry: true}
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "", &DeliveryError{Message: "credential empty"}
	}
	return s, nil
}
func Clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	n -= len("\n[truncated]")
	if n < 0 {
		return ""
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "\n[truncated]"
}
func Text(p monitor.Payload, limit int) string {
	s := p.Message
	for _, l := range p.Logs {
		if l.Text != "" {
			s += "\n\nLogs " + l.Pod + "/" + l.Container + " (" + l.Source + "):\n" + l.Text
		} else if l.Error != "" {
			s += "\nLogs unavailable: " + l.Error
		}
	}
	r := []rune(s)
	if len(r) > limit {
		s = string(r[:limit-15]) + "\n[truncated]"
	}
	return s
}
func (s *Sender) Send(ctx context.Context, d config.Destination, p monitor.Payload) error {
	var endpoint string
	var body any
	switch d.Type {
	case "telegram":
		token, e := s.secret(d.TokenFile)
		if e != nil {
			return e
		}
		endpoint = strings.TrimRight(s.TelegramBase, "/") + "/bot" + url.PathEscape(token) + "/sendMessage"
		b := map[string]any{"chat_id": d.ChatID, "text": Text(p, 4096)}
		if d.ThreadID > 0 {
			b["message_thread_id"] = d.ThreadID
		}
		body = b
	case "pachca", "generic":
		u, e := s.secret(d.URLFile)
		if e != nil {
			return e
		}
		parsed, e := url.Parse(u)
		if e != nil || parsed.Host == "" || !config.ValidURLScheme(u) {
			return &DeliveryError{Message: "invalid webhook URL"}
		}
		endpoint = u

		body = p
	default:
		return &DeliveryError{Message: "unknown destination"}
	}
	data, e := json.Marshal(body)
	if d.Type == "pachca" || d.Type == "generic" {
		data, e = webhookBody(p, d.Type)
	}
	if e != nil {
		return &DeliveryError{Message: "encode payload"}
	}
	if len(data) > 65536 {
		return &DeliveryError{Message: "payload exceeds 64 KiB"}
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if e != nil {
		return &DeliveryError{Message: "invalid request"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "k8s-alert")
	req.Header.Set("X-Notification-ID", p.NotificationID)
	res, e := s.Client.Do(req)
	if e != nil {
		return &DeliveryError{Message: "network or timeout", Retry: true}
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 16384))
	code := res.StatusCode
	after := time.Duration(0)
	if n, e := strconv.Atoi(res.Header.Get("Retry-After")); e == nil && n > 0 {
		after = time.Duration(n) * time.Second
	} else if t, e := http.ParseTime(res.Header.Get("Retry-After")); e == nil {
		after = time.Until(t)
	}
	if d.Type == "telegram" {
		var ans struct {
			OK         bool `json:"ok"`
			Code       int  `json:"error_code"`
			Parameters struct {
				RetryAfter int `json:"retry_after"`
			} `json:"parameters"`
		}
		if e := json.Unmarshal(b, &ans); e != nil && code >= 200 && code < 300 {
			return &DeliveryError{Message: "invalid Telegram response", Retry: true}
		}
		if ans.Parameters.RetryAfter > 0 {
			after = time.Duration(ans.Parameters.RetryAfter) * time.Second
		}
		if !ans.OK && code >= 200 && code < 300 {
			code = ans.Code
			if code == 0 {
				code = 502
			}
		}
	}
	if code < 200 || code >= 300 {
		return &DeliveryError{Code: code, Message: "remote rejected request", Retry: code == 408 || code == 429 || code >= 500, After: after}
	}
	return nil
}

// webhookBody bounds the encoded JSON, including JSON escaping and Pachca's
// rendered message, rather than bounding only the unencoded log strings.
func webhookBody(p monitor.Payload, provider string) ([]byte, error) {
	p.Logs = append([]monitor.Log(nil), p.Logs...)
	p.Events = append([]string(nil), p.Events...)
	base := Clip(p.Message, 12000)
	for i := 0; i < 40; i++ {
		p.Message = base
		var logsJSON string
		if provider == "pachca" {
			p.Message = Clip(Text(p, 24000), 24000)
			if len(p.Logs) > 0 {
				rendered, err := renderJSONLogs(p.Logs)
				if err != nil {
					return nil, err
				}
				logsJSON = rendered
			}
		}
		b, e := json.Marshal(struct {
			monitor.Payload
			LogsJSON string `json:"logs_json,omitempty"`
		}{Payload: p, LogsJSON: logsJSON})
		if e != nil {
			return nil, e
		}
		if len(b) <= 65536 {
			return b, nil
		}
		largest := -1
		for j := range p.Logs {
			if largest < 0 || len(p.Logs[j].Text) > len(p.Logs[largest].Text) {
				largest = j
			}
		}
		if largest >= 0 && len(p.Logs[largest].Text) > 64 {
			p.Logs[largest].Text = Clip(p.Logs[largest].Text, len(p.Logs[largest].Text)/2)
			p.Logs[largest].Truncated = true
		} else if len(p.Events) > 0 {
			p.Events = p.Events[:len(p.Events)-1]
		} else if len(base) > 128 {
			base = Clip(base, len(base)/2)
		} else {
			return nil, &DeliveryError{Message: "payload metadata exceeds 64 KiB"}
		}
	}
	return nil, &DeliveryError{Message: "cannot bound webhook payload"}
}

// Render the same redacted, size-limited logs as a JSON block for Liquid.
func renderJSONLogs(logs []monitor.Log) (string, error) {
	type entry struct {
		Pod       string   `json:"pod"`
		Container string   `json:"container"`
		Source    string   `json:"source"`
		Lines     []string `json:"lines"`
		Truncated bool     `json:"truncated"`
		Error     string   `json:"error,omitempty"`
	}
	entries := make([]entry, 0, len(logs))
	for _, log := range logs {
		lines := []string{}
		if log.Text != "" {
			lines = strings.Split(strings.TrimSuffix(log.Text, "\n"), "\n")
		}
		entries = append(entries, entry{
			Pod: log.Pod, Container: log.Container, Source: log.Source,
			Lines: lines, Truncated: log.Truncated, Error: log.Error,
		})
	}
	b, err := json.MarshalIndent(entries, "", "  ")
	return string(b), err
}

func (s *Sender) secret(file string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.credentials[file]; ok {
		return v, nil
	}
	v, e := credential(file)
	if e != nil {
		return "", e
	}
	if s.credentials == nil {
		s.credentials = map[string]string{}
	}
	s.credentials[file] = v
	return v, nil
}
func (s *Sender) LoadCredentials(ds []config.Destination) error {
	for _, d := range ds {
		if !d.Enabled {
			continue
		}
		f := d.URLFile
		if d.Type == "telegram" {
			f = d.TokenFile
		}
		v, e := s.secret(f)
		if e != nil {
			return e
		}
		if d.Type != "telegram" {
			u, e := url.Parse(v)
			if e != nil || u.Host == "" || !config.ValidURLScheme(v) {
				return &DeliveryError{Message: "invalid webhook URL"}
			}
		}
	}
	return nil
}
func (s *Sender) Secrets() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, v := range s.credentials {
		out = append(out, v)
	}
	return out
}
