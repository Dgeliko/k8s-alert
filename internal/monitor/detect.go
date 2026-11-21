package monitor

import (
	"fmt"
	"k8s-alert/internal/config"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"time"
)

type Detector struct {
	Config       *config.Config
	Terminations map[string]string
	Since        map[string]time.Time
	Initial      bool
}

func NewDetector(c *config.Config) *Detector {
	return &Detector{Config: c, Terminations: map[string]string{}, Since: map[string]time.Time{}, Initial: true}
}
func (d *Detector) Pod(p *corev1.Pod, v *Inventory, now time.Time) []Signal {
	root, kinds, unknown := v.Resolve("Pod", p)
	target := config.Target{Namespace: p.Namespace, PodName: p.Name, WorkloadName: root.Name, Kinds: kinds, Labels: p.Labels, OwnerUnknown: unknown}
	if !d.Config.Allows(target) {
		return nil
	}
	var out []Signal
	native := map[string]bool{}
	for _, c := range p.Spec.InitContainers {
		native[c.Name] = c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
	}
	jobFinishing := p.Status.Phase == corev1.PodSucceeded
	jobOwned := false
	for _, k := range kinds {
		jobOwned = jobOwned || k == "Job" || k == "CronJob"
	}
	if jobOwned && len(p.Status.ContainerStatuses) > 0 {
		all := true
		for _, st := range p.Status.ContainerStatuses {
			all = all && st.State.Terminated != nil && st.State.Terminated.ExitCode == 0
		}
		jobFinishing = jobFinishing || all
	}
	if d.Config.Notifications.GroupBy == "pod" {
		root = Resource{Kind: "Pod", Name: p.Name, UID: string(p.UID), Namespace: p.Namespace}
	}
	base := Signal{PodName: p.Name, PodUID: string(p.UID), Workload: root, Target: target, Active: true}
	statuses := append([]corev1.ContainerStatus{}, p.Status.InitContainerStatuses...)
	statuses = append(statuses, p.Status.ContainerStatuses...)
	for _, st := range statuses {
		s := base
		s.Container = st.Name
		s.RestartCount = st.RestartCount
		term := st.State.Terminated
		current := term != nil
		if term == nil {
			term = st.LastTerminationState.Terminated
		}
		marker := ""
		if term != nil {
			marker = fmt.Sprintf("%s/%s/%s/%d", p.UID, st.Name, term.ContainerID, term.FinishedAt.UnixNano())
		}
		slot := string(p.UID) + "/" + st.Name
		prev, seen := d.Terminations[slot]
		d.Terminations[slot] = marker
		fresh := term != nil && marker != "" && prev != marker && (!d.Initial || current || st.State.Running == nil)
		if !seen && d.Initial && st.State.Running != nil {
			fresh = false
		}
		if term != nil {
			s.TerminationID = marker
			s.NewTermination = fresh
			s.ExitCode = term.ExitCode
			s.CurrentLogs = current
			beforeDelete := p.DeletionTimestamp == nil || (!term.FinishedAt.IsZero() && term.FinishedAt.Before(p.DeletionTimestamp))
			sidecarStop := native[st.Name] && jobFinishing && term.Reason != "OOMKilled" && (term.ExitCode == 143 || term.ExitCode == 137 || term.Signal == 15 || term.Signal == 9)
			expectedStop := sidecarStop || term.ExitCode == 0 || ((term.ExitCode == 143 || term.Signal == 15) && p.DeletionTimestamp != nil)
			teardown := p.Status.Phase == corev1.PodFailed && (p.Status.Reason == "Evicted" || p.Status.Reason == "Shutdown" || p.Status.Reason == "DeadlineExceeded")
			if !expectedStop && beforeDelete && !teardown {
				if term.Reason == "OOMKilled" {
					s.Class = "OOMKilled"
					s.Reason = "OOMKilled"
				} else if term.ExitCode != 0 {
					s.Class = "ContainerCrash"
					s.Reason = term.Reason
					if s.Reason == "" {
						s.Reason = fmt.Sprintf("Exit%d", term.ExitCode)
					}
				}
				if s.Class != "" && (fresh || current || st.State.Waiting != nil) {
					s.Key = Key(root, st.Name, s.Class)
					out = append(out, s)
				}
			}
		}
		if p.DeletionTimestamp != nil {
			continue
		}
		if st.State.Waiting != nil {
			reason := st.State.Waiting.Reason
			class := ""
			switch reason {
			case "CrashLoopBackOff":
				class = "ContainerCrash"
			case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "ErrImageNeverPull":
				class = "ImagePullFailure"
			case "CreateContainerConfigError", "CreateContainerError", "RunContainerError":
				class = "ContainerConfigFailure"
			}
			if class != "" {
				if reason == "CrashLoopBackOff" && term != nil && term.Reason == "OOMKilled" {
					class = "OOMKilled"
				}
				s.Class = class
				s.Reason = reason
				s.Key = Key(root, st.Name, class)
				s.Since = d.since(s.Key+"/waiting", now)
				out = append(out, s)
			}
		}
	}
	if p.DeletionTimestamp == nil && p.Status.Phase != corev1.PodSucceeded && p.Status.Phase != corev1.PodFailed {
		s := base
		if p.Status.Phase == corev1.PodPending {
			s.Class = "PodPending"
			s.Reason = "Pending"
		} else if !PodReady(p) {
			s.Class = "PodNotReady"
			s.Reason = "NotReady"
		}
		if s.Class != "" {
			s.Key = Key(root, "", s.Class)
			s.Since = d.since(s.Key, now)
			out = append(out, s)
		}
	}
	return out
}
func (d *Detector) since(key string, now time.Time) time.Time {
	if t, ok := d.Since[key]; ok {
		return t
	}
	d.Since[key] = now
	return now
}
func PodReady(p *corev1.Pod) bool {
	if p.DeletionTimestamp != nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
func (d *Detector) Workloads(v *Inventory, now time.Time) []Signal {
	var out []Signal
	for key, obj := range v.Objects {
		kind := strings.SplitN(key, "/", 2)[0]
		if kind == "Pod" || kind == "Job" || kind == "CronJob" {
			continue
		}
		root, kinds, unknown := v.Resolve(kind, obj)
		t := config.Target{Namespace: obj.GetNamespace(), WorkloadName: root.Name, Kinds: kinds, Labels: obj.GetLabels(), OwnerUnknown: unknown}
		if !d.Config.Allows(t) {
			continue
		}
		base := Signal{Workload: root, Target: t, Active: true}
		emit := func(class, reason string) {
			s := base
			s.Class = class
			s.Reason = reason
			s.Key = Key(root, "", class)
			s.Since = d.since(s.Key, now)
			out = append(out, s)
		}
		switch o := obj.(type) {
		case *appsv1.Deployment:
			if o.Status.ObservedGeneration < o.Generation {
				continue
			}
			for _, c := range o.Status.Conditions {
				if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == "ProgressDeadlineExceeded" {
					emit("RolloutFailed", c.Reason)
				}
			}
			desired := int32(1)
			if o.Spec.Replicas != nil {
				desired = *o.Spec.Replicas
			}
			availableKnown, available := false, false
			for _, condition := range o.Status.Conditions {
				if condition.Type == appsv1.DeploymentAvailable {
					availableKnown = true
					available = condition.Status == corev1.ConditionTrue
				}
			}
			if desired > 0 && ((availableKnown && !available) || (!availableKnown && o.Status.AvailableReplicas < desired)) {
				emit("WorkloadUnavailable", "InsufficientAvailableReplicas")
			}
		case *appsv1.ReplicaSet:
			for _, c := range o.Status.Conditions {
				if c.Type == appsv1.ReplicaSetReplicaFailure && c.Status == corev1.ConditionTrue {
					emit("ReplicaSetFailedCreate", c.Reason)
				}
			}
		case *appsv1.StatefulSet:
			desired := int32(1)
			if o.Spec.Replicas != nil {
				desired = *o.Spec.Replicas
			}
			if o.Status.ObservedGeneration >= o.Generation && desired > 0 && o.Status.ReadyReplicas < desired {
				emit("WorkloadUnavailable", "ReplicasNotReady")
			}
		case *appsv1.DaemonSet:
			if o.Status.ObservedGeneration >= o.Generation && o.Status.NumberReady < o.Status.DesiredNumberScheduled {
				emit("WorkloadUnavailable", "ReplicasNotReady")
			}
		}
	}
	return out
}
func (d *Detector) Events(events []*corev1.Event, v *Inventory, now time.Time) []Signal {
	var out []Signal
	for _, ev := range events {
		if ev.Type != "Warning" || ev.Reason != "FailedCreate" || ev.InvolvedObject.Kind != "ReplicaSet" {
			continue
		}
		t := ev.LastTimestamp.Time
		if t.IsZero() {
			t = ev.CreationTimestamp.Time
		}
		if now.Sub(t) > 5*time.Minute {
			continue
		}
		obj := v.Objects[objectKey("ReplicaSet", ev.Namespace, ev.InvolvedObject.Name)]
		if obj == nil || string(obj.GetUID()) != string(ev.InvolvedObject.UID) {
			continue
		}
		root, kinds, unknown := v.Resolve("ReplicaSet", obj)
		target := config.Target{Namespace: ev.Namespace, WorkloadName: root.Name, Kinds: kinds, Labels: obj.GetLabels(), OwnerUnknown: unknown}
		s := Signal{Key: Key(root, "", "ReplicaSetFailedCreate"), Class: "ReplicaSetFailedCreate", Reason: "FailedCreate", Workload: root, Target: target, Active: true, Since: d.since(Key(root, "", "ReplicaSetFailedCreate"), now), Events: []string{ev.Message}}
		out = append(out, s)
	}
	return out
}
func (d *Detector) Cleanup(signals []Signal) {
	active := map[string]bool{}
	for _, s := range signals {
		active[s.Key] = true
		active[s.Key+"/waiting"] = true
	}
	for k := range d.Since {
		if !active[k] {
			delete(d.Since, k)
		}
	}
	d.Initial = false
}
func Recovery(v *Inventory, pods []*corev1.Pod, s Signal) (bool, bool) {
	obj := v.Objects[objectKey(s.Workload.Kind, s.Workload.Namespace, s.Workload.Name)]
	if obj == nil || string(obj.GetUID()) != s.Workload.UID {
		return true, true
	}
	count := 0
	containerHealthy := true
	ownersUnknown := false
	for _, p := range pods {
		r, _, u := v.Resolve("Pod", p)
		if s.Workload.Kind == "Pod" {
			r = Resource{UID: string(p.UID)}
		}
		if u {
			ownersUnknown = true
			continue
		}
		if r.UID != s.Workload.UID {
			continue
		}
		if p.DeletionTimestamp != nil {
			continue
		}
		count++
		if s.Container != "" {
			found := false
			for _, st := range p.Status.ContainerStatuses {
				if st.Name == s.Container {
					found = true
					containerHealthy = containerHealthy && st.Ready && st.State.Running != nil
				}
			}
			for _, st := range p.Status.InitContainerStatuses {
				if st.Name == s.Container {
					found = true
					containerHealthy = containerHealthy && (st.State.Terminated != nil && st.State.Terminated.ExitCode == 0 || st.State.Running != nil && st.Ready)
				}
			}
			containerHealthy = containerHealthy && found
		} else if !PodReady(p) {
			return false, false
		}
	}
	if s.Container != "" && (count == 0 || !containerHealthy || ownersUnknown) {
		return false, false
	}
	switch o := obj.(type) {
	case *appsv1.Deployment:
		if o.Status.ObservedGeneration < o.Generation {
			return false, false
		}
		n := int32(1)
		if o.Spec.Replicas != nil {
			n = *o.Spec.Replicas
		}
		return o.Status.AvailableReplicas >= n, false
	case *appsv1.StatefulSet:
		n := int32(1)
		if o.Spec.Replicas != nil {
			n = *o.Spec.Replicas
		}
		return o.Status.ObservedGeneration >= o.Generation && o.Status.ReadyReplicas >= n, false
	case *appsv1.DaemonSet:
		return o.Status.ObservedGeneration >= o.Generation && o.Status.NumberReady >= o.Status.DesiredNumberScheduled, false
	}
	return count > 0, false
}

// ObjectMetadata keeps callers independent of concrete workload types.
func ObjectMetadata(obj any) metav1.Object { m, _ := meta.Accessor(obj); return m }

// Attribute liveness-caused kills while keeping the same crash identity.
func (d *Detector) ApplyProbeContext(signals []Signal, events []*corev1.Event, now time.Time) []Signal {
	for i := range signals {
		s := &signals[i]
		if s.Class != "ContainerCrash" || s.ExitCode != 137 {
			continue
		}
		for _, ev := range events {
			if string(ev.InvolvedObject.UID) != s.PodUID || ev.Reason != "Unhealthy" || !strings.Contains(strings.ToLower(ev.Message), "liveness probe failed") {
				continue
			}
			at := ev.LastTimestamp.Time
			if ev.Series != nil {
				at = ev.Series.LastObservedTime.Time
			}
			if now.Sub(at) > 2*time.Minute {
				continue
			}
			if field := ev.InvolvedObject.FieldPath; field != "" && !strings.Contains(field, "{"+s.Container+"}") {
				continue
			}
			s.Class = "ProbeFailure"
			s.Reason = "LivenessProbeFailed"
			s.Events = append(s.Events, ev.Message)
			break
		}
	}
	return signals
}

func (d *Detector) Prune(v *Inventory) {
	uids := map[string]bool{}
	for key, obj := range v.Objects {
		if strings.HasPrefix(key, "Pod/") {
			uids[string(obj.GetUID())] = true
		}
	}
	for k := range d.Terminations {
		uid := strings.SplitN(k, "/", 2)[0]
		if !uids[uid] {
			delete(d.Terminations, k)
		}
	}
}
