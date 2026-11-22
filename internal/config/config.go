package config

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/labels"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, e := time.ParseDuration(n.Value)
	if e != nil {
		return e
	}
	*d = Duration(v)
	return nil
}
func (d Duration) D() time.Duration { return time.Duration(d) }

type Selection struct {
	Include []string `yaml:"include"`
	Exclude []string `yaml:"exclude"`
}
type Filters struct {
	Namespaces    Selection `yaml:"namespaces"`
	PodNames      Selection `yaml:"pod-names"`
	Kinds         Selection `yaml:"kinds"`
	LabelSelector string    `yaml:"label-selector"`
}
type Restarts struct {
	Threshold *int     `yaml:"threshold"`
	Window    Duration `yaml:"window"`
}
type Notifications struct {
	GroupBy  string   `yaml:"group-by"`
	Repeat   Duration `yaml:"repeat-interval"`
	Recovery bool     `yaml:"recovery"`
	Stable   Duration `yaml:"recovery-stable-for"`
}
type LogConfig struct {
	Enabled   bool     `yaml:"enabled"`
	TailLines int64    `yaml:"tail-lines"`
	MaxBytes  int64    `yaml:"max-bytes"`
	MaxPods   int      `yaml:"max-pods"`
	Timeout   Duration `yaml:"timeout"`
	Redact    []string `yaml:"redact-patterns"`
}
type Detector struct {
	Enabled bool     `yaml:"enabled"`
	For     Duration `yaml:"for"`
}
type Match struct {
	Namespaces    []string `yaml:"namespaces"`
	PodNames      []string `yaml:"pod-names"`
	WorkloadNames []string `yaml:"workload-names"`
	Kinds         []string `yaml:"kinds"`
	Labels        string   `yaml:"label-selector"`
}
type NotifyPatch struct {
	Repeat   *Duration `yaml:"repeat-interval"`
	Stable   *Duration `yaml:"recovery-stable-for"`
	Recovery *bool     `yaml:"recovery"`
}
type Override struct {
	Match         Match       `yaml:"match"`
	Restarts      Restarts    `yaml:"restarts"`
	Notifications NotifyPatch `yaml:"notifications"`
}
type Destination struct {
	Name           string `yaml:"name"`
	Type           string `yaml:"type"`
	Enabled        bool   `yaml:"enabled"`
	ExistingSecret string `yaml:"existingSecret"`
	URLKey         string `yaml:"url-key"`
	TokenKey       string `yaml:"token-key"`
	URLFile        string `yaml:"url-file"`
	TokenFile      string `yaml:"token-file"`
	ChatID         string `yaml:"chat-id"`
	ThreadID       int64  `yaml:"message-thread-id"`
}
type Persistence struct {
	Enabled   bool   `yaml:"enabled"`
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}
type Config struct {
	Version       int                 `yaml:"version"`
	Cluster       string              `yaml:"cluster"`
	Env           string              `yaml:"env-type"`
	Filters       Filters             `yaml:"filters"`
	Restarts      Restarts            `yaml:"restarts"`
	Notifications Notifications       `yaml:"notifications"`
	Logs          LogConfig           `yaml:"logs"`
	Detectors     map[string]Detector `yaml:"detectors"`
	Overrides     []Override          `yaml:"overrides"`
	Destinations  []Destination       `yaml:"destinations"`
	Persistence   Persistence         `yaml:"persistence"`
	Listen        string              `yaml:"listen"`
}
type Target struct {
	Namespace, PodName, WorkloadName string
	Kinds                            []string
	Labels                           map[string]string
	OwnerUnknown                     bool
}
type Policy struct {
	Threshold              int
	Window, Repeat, Stable time.Duration
	Recovery               bool
}

var DetectorDefaults = map[string]time.Duration{"ContainerCrash": 0, "OOMKilled": 0, "ProbeFailure": 0, "CrashLoop": 2 * time.Minute, "ImagePullFailure": time.Minute, "ContainerConfigFailure": time.Minute, "PodPending": 240 * time.Second, "PodNotReady": 120 * time.Second, "WorkloadUnavailable": 120 * time.Second, "RolloutFailed": 30 * time.Second, "ReplicaSetFailedCreate": 30 * time.Second}
var DetectorNames = map[string]string{"ContainerCrash": "container-crash", "OOMKilled": "oom-killed", "ProbeFailure": "probe-failure", "CrashLoop": "crash-loop", "ImagePullFailure": "image-pull-failure", "ContainerConfigFailure": "container-config-failure", "PodPending": "pod-pending", "PodNotReady": "pod-not-ready", "WorkloadUnavailable": "workload-unavailable", "RolloutFailed": "rollout-failed", "ReplicaSetFailedCreate": "replicaset-failed-create"}

func Load(b []byte) (*Config, error) {
	c := &Config{Version: 1, Env: "dev", Listen: ":8080", Restarts: Restarts{Window: Duration(5 * time.Minute)}, Notifications: Notifications{GroupBy: "workload", Repeat: Duration(30 * time.Minute), Recovery: true, Stable: Duration(2 * time.Minute)}, Logs: LogConfig{Enabled: true, TailLines: 15, MaxBytes: 16384, MaxPods: 3, Timeout: Duration(5 * time.Second)}, Persistence: Persistence{Name: "k8s-alert-state"}}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if e := d.Decode(c); e != nil {
		return nil, e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return nil, fmt.Errorf("exactly one YAML document required")
	}
	if e := c.Validate(); e != nil {
		return nil, e
	}
	return c, nil
}
func (c *Config) Validate() error {
	if c.Version != 1 || c.Cluster == "" {
		return fmt.Errorf("version must be 1 and cluster must be nonempty")
	}
	if c.Env != "dev" && c.Env != "prod" {
		return fmt.Errorf("env-type must be dev or prod")
	}
	if c.Restarts.Threshold != nil && *c.Restarts.Threshold < 1 {
		return fmt.Errorf("restarts.threshold must be >=1")
	}
	if c.Restarts.Window <= 0 || c.Notifications.Repeat <= 0 || c.Notifications.Stable <= 0 {
		return fmt.Errorf("restarts/notifications durations must be positive")
	}
	if c.Notifications.GroupBy != "workload" && c.Notifications.GroupBy != "pod" {
		return fmt.Errorf("notifications.group-by must be workload or pod")
	}
	if c.Logs.TailLines < 0 || c.Logs.TailLines > 10000 || c.Logs.MaxBytes < 1 || c.Logs.MaxBytes > 65536 || c.Logs.MaxPods < 1 || c.Logs.MaxPods > 10 || c.Logs.Timeout <= 0 {
		return fmt.Errorf("logs limits invalid: lines 0..10000, bytes 1..65536, pods 1..10, timeout >0")
	}
	for _, s := range []Selection{c.Filters.Namespaces, c.Filters.PodNames, c.Filters.Kinds} {
		for _, pat := range append(append([]string{}, s.Include...), s.Exclude...) {
			if _, e := path.Match(pat, ""); e != nil {
				return fmt.Errorf("invalid glob %q", pat)
			}
		}
	}
	if _, e := labels.Parse(c.Filters.LabelSelector); e != nil {
		return fmt.Errorf("filters.label-selector: %w", e)
	}
	for _, r := range c.Logs.Redact {
		if _, e := regexp.Compile(r); e != nil {
			return fmt.Errorf("logs.redact-patterns: %w", e)
		}
	}
	known := map[string]bool{}
	for _, n := range DetectorNames {
		known[n] = true
	}
	for n, d := range c.Detectors {
		if !known[n] || d.For < 0 {
			return fmt.Errorf("invalid detector %q", n)
		}
	}
	for _, o := range c.Overrides {
		if o.Restarts.Threshold != nil && *o.Restarts.Threshold < 1 {
			return fmt.Errorf("override threshold must be >=1")
		}
		if o.Restarts.Window < 0 {
			return fmt.Errorf("override window must be positive")
		}
		if _, e := labels.Parse(o.Match.Labels); e != nil {
			return e
		}
		for _, ps := range [][]string{o.Match.Namespaces, o.Match.PodNames, o.Match.WorkloadNames, o.Match.Kinds} {
			for _, pat := range ps {
				if _, e := path.Match(pat, ""); e != nil {
					return e
				}
			}
		}
		if o.Notifications.Repeat != nil && *o.Notifications.Repeat <= 0 {
			return fmt.Errorf("override repeat must be >0")
		}
		if o.Notifications.Stable != nil && *o.Notifications.Stable <= 0 {
			return fmt.Errorf("override stable must be >0")
		}
	}
	names := map[string]bool{}
	validName := regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	for i := range c.Destinations {
		d := &c.Destinations[i]
		if !validName.MatchString(d.Name) || names[d.Name] {
			return fmt.Errorf("destination name invalid or duplicate")
		}
		names[d.Name] = true
		if d.Type != "pachca" && d.Type != "generic" && d.Type != "telegram" {
			return fmt.Errorf("unknown destination type %q", d.Type)
		}
		if !d.Enabled {
			continue
		}
		if d.ThreadID < 0 {
			return fmt.Errorf("message-thread-id must be >=0")
		}
		if d.Type == "telegram" {
			if d.ChatID == "" {
				return fmt.Errorf("telegram chat-id required")
			}
			if d.TokenKey == "" {
				d.TokenKey = "bot-token"
			}
			if filepath.Base(d.TokenKey) != d.TokenKey {
				return fmt.Errorf("token-key must be a filename")
			}
			if d.TokenFile == "" && d.ExistingSecret != "" {
				d.TokenFile = filepath.Join("/var/run/k8s-alert/destinations", d.Name, d.TokenKey)
			}
			if d.TokenFile == "" {
				return fmt.Errorf("telegram token-file or existingSecret required")
			}
		} else {
			if d.URLKey == "" {
				d.URLKey = "url"
			}
			if filepath.Base(d.URLKey) != d.URLKey {
				return fmt.Errorf("url-key must be a filename")
			}
			if d.URLFile == "" && d.ExistingSecret != "" {
				d.URLFile = filepath.Join("/var/run/k8s-alert/destinations", d.Name, d.URLKey)
			}
			if d.URLFile == "" {
				return fmt.Errorf("webhook url-file or existingSecret required")
			}
		}
	}
	return nil
}
func matches(ps []string, v string) bool {
	for _, p := range ps {
		if ok, _ := path.Match(p, v); ok {
			return true
		}
	}
	return false
}
func selection(s Selection, vs []string) bool {
	inc := len(s.Include) == 0
	for _, v := range vs {
		if matches(s.Exclude, v) {
			return false
		}
		if matches(s.Include, v) {
			inc = true
		}
	}
	return inc
}

// ObservationNamespaces returns the API scopes needed by the namespace filter.
// An empty string denotes a cluster-wide watch for dynamic namespace patterns.
func (c *Config) ObservationNamespaces() []string {
	s := c.Filters.Namespaces
	if len(s.Include) == 0 {
		return []string{""}
	}
	names := make([]string, 0, len(s.Include))
	seen := make(map[string]bool, len(s.Include))
	for _, name := range s.Include {
		if name == "" || strings.ContainsAny(name, `*?[\`) {
			return []string{""}
		}
		if !seen[name] && !matches(s.Exclude, name) {
			names = append(names, name)
			seen[name] = true
		}
	}
	return names
}

func (c *Config) Allows(t Target) bool {
	if t.OwnerUnknown && (len(c.Filters.Kinds.Include) > 0 || len(c.Filters.Kinds.Exclude) > 0) {
		return false
	}
	if !selection(c.Filters.Namespaces, []string{t.Namespace}) || !selection(c.Filters.PodNames, []string{t.PodName}) || !selection(c.Filters.Kinds, t.Kinds) {
		return false
	}
	sel, _ := labels.Parse(c.Filters.LabelSelector)
	return sel.Matches(labels.Set(t.Labels))
}
func (m Match) Matches(t Target) bool {
	if len(m.Namespaces) > 0 && !matches(m.Namespaces, t.Namespace) {
		return false
	}
	if len(m.PodNames) > 0 && !matches(m.PodNames, t.PodName) {
		return false
	}
	if len(m.WorkloadNames) > 0 && !matches(m.WorkloadNames, t.WorkloadName) {
		return false
	}
	if len(m.Kinds) > 0 {
		ok := false
		for _, k := range t.Kinds {
			ok = ok || matches(m.Kinds, k)
		}
		if !ok {
			return false
		}
	}
	sel, _ := labels.Parse(m.Labels)
	return sel.Matches(labels.Set(t.Labels))
}
func (c *Config) PolicyFor(t Target) Policy {
	n := 3
	if c.Env == "prod" {
		n = 1
	}
	if c.Restarts.Threshold != nil {
		n = *c.Restarts.Threshold
	}
	p := Policy{n, c.Restarts.Window.D(), c.Notifications.Repeat.D(), c.Notifications.Stable.D(), c.Notifications.Recovery}
	for _, o := range c.Overrides {
		if !o.Match.Matches(t) {
			continue
		}
		if o.Restarts.Threshold != nil {
			p.Threshold = *o.Restarts.Threshold
		}
		if o.Restarts.Window > 0 {
			p.Window = o.Restarts.Window.D()
		}
		if o.Notifications.Repeat != nil {
			p.Repeat = o.Notifications.Repeat.D()
		}
		if o.Notifications.Stable != nil {
			p.Stable = o.Notifications.Stable.D()
		}
		if o.Notifications.Recovery != nil {
			p.Recovery = *o.Notifications.Recovery
		}
		break
	}
	return p
}
func (c *Config) Detector(class string) (bool, time.Duration) {
	n := DetectorNames[class]
	d, ok := c.Detectors[n]
	if ok {
		return d.Enabled, d.For.D()
	}
	v, ok := DetectorDefaults[class]
	return ok, v
}
func (c *Config) EnabledNames() []string {
	var r []string
	for _, d := range c.Destinations {
		if d.Enabled {
			r = append(r, d.Name)
		}
	}
	return r
}
func (c *Config) CredentialFiles() []string {
	var r []string
	for _, d := range c.Destinations {
		if !d.Enabled {
			continue
		}
		if d.Type == "telegram" {
			r = append(r, d.TokenFile)
		} else {
			r = append(r, d.URLFile)
		}
	}
	return r
}
func ValidURLScheme(s string) bool {
	return strings.HasPrefix(s, "https://") || strings.HasPrefix(s, "http://")
}
