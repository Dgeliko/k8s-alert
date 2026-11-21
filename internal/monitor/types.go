package monitor

import (
	"k8s-alert/internal/config"
	"time"
)

type Resource struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
	Namespace string `json:"namespace,omitempty"`
}
type Log struct {
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Source    string `json:"source"`
	Text      string `json:"text"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}
type Payload struct {
	SchemaVersion  int       `json:"schema_version"`
	IncidentID     string    `json:"incident_id"`
	NotificationID string    `json:"notification_id"`
	Status         string    `json:"status"`
	Cluster        string    `json:"cluster"`
	Env            string    `json:"env_type"`
	Namespace      string    `json:"namespace"`
	Resource       Resource  `json:"resource"`
	Workload       Resource  `json:"workload"`
	Container      string    `json:"container,omitempty"`
	Reason         string    `json:"reason"`
	Class          string    `json:"failure_class"`
	ExitCode       int32     `json:"exit_code,omitempty"`
	RestartCount   int32     `json:"restart_count"`
	FirstSeen      time.Time `json:"first_seen"`
	LastSeen       time.Time `json:"last_seen"`
	Occurrences    int       `json:"occurrences"`
	Message        string    `json:"message"`
	Logs           []Log     `json:"logs"`
	Events         []string  `json:"events,omitempty"`
}
type Signal struct {
	Key, Class, Reason, Container, PodName, PodUID string
	Workload                                       Resource
	Target                                         config.Target
	TerminationID                                  string
	NewTermination, Active, ContinueOnly           bool
	Since                                          time.Time
	ExitCode, RestartCount                         int32
	Events                                         []string
	CurrentLogs                                    bool
}
type Request struct {
	Sources      []Signal `json:"sources,omitempty"`
	ID           string   `json:"id"`
	Payload      Payload  `json:"payload"`
	Signal       Signal   `json:"signal"`
	Destinations []string `json:"destinations,omitempty"`
}
type Incident struct {
	PendingStatus                     map[string]string
	Sources                           map[string]Signal `json:"sources,omitempty"`
	Signal                            Signal            `json:"signal"`
	FirstSeen, LastSeen, HealthySince time.Time
	Failures                          map[string]time.Time
	Occurrences                       int
	Firing, Resolved                  bool
	LastSent                          map[string]time.Time
	Pending                           map[string]string
	LastAttempt                       map[string]time.Time
}
