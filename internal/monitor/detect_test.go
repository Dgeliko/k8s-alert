package monitor

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"time"
)

func own(kind, name, uid string) metav1.OwnerReference {
	b := true
	return metav1.OwnerReference{Kind: kind, Name: name, UID: types.UID(uid), Controller: &b}
}
func TestOwnerChainCronJobExclusion(t *testing.T) {
	c := cfg(t, "prod")
	c.Filters.Kinds.Exclude = []string{"CronJob"}
	v := NewInventory()
	cron := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cron", UID: "cron"}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "job", UID: "job", OwnerReferences: []metav1.OwnerReference{own("CronJob", "cron", "cron")}}}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "task", UID: "pod", OwnerReferences: []metav1.OwnerReference{own("Job", "job", "job")}}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}}}}}
	v.Add("CronJob", cron)
	v.Add("Job", job)
	v.Add("Pod", p)
	if len(NewDetector(c).Pod(p, v, time.Now())) != 0 {
		t.Fatal("CronJob pod leaked")
	}
}
func TestDeletionDoesNotBecomeCrash(t *testing.T) {
	c := cfg(t, "prod")
	now := metav1.Now()
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p", UID: "p", DeletionTimestamp: &now}, Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 143, FinishedAt: now}}}}}}
	v := NewInventory()
	v.Add("Pod", p)
	if len(NewDetector(c).Pod(p, v, time.Now())) != 0 {
		t.Fatal("shutdown alerted")
	}
}
func TestInitialRunningHistoryIsNotNewCrash(t *testing.T) {
	c := cfg(t, "prod")
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p", UID: "p"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", RestartCount: 10, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}}}}
	v := NewInventory()
	v.Add("Pod", p)
	if len(NewDetector(c).Pod(p, v, time.Now())) != 0 {
		t.Fatal("historical crash sent")
	}
}
func TestInitFailureAndSeparateContainers(t *testing.T) {
	c := cfg(t, "prod")
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p", UID: "p"}, Status: corev1.PodStatus{Phase: corev1.PodPending, InitContainerStatuses: []corev1.ContainerStatus{{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}}}}}
	v := NewInventory()
	v.Add("Pod", p)
	got := NewDetector(c).Pod(p, v, time.Now())
	found := false
	for _, s := range got {
		found = found || s.Container == "init" && s.Class == "ContainerCrash"
	}
	if !found {
		t.Fatal("init failure missed")
	}
	if Key(Resource{UID: "u"}, "a", "ContainerCrash") == Key(Resource{UID: "u"}, "b", "ContainerCrash") {
		t.Fatal("containers collide")
	}
}
func TestReplicaSetFailureWithoutPod(t *testing.T) {
	c := cfg(t, "prod")
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "default", UID: "rs"}, Status: appsv1.ReplicaSetStatus{Conditions: []appsv1.ReplicaSetCondition{{Type: appsv1.ReplicaSetReplicaFailure, Status: corev1.ConditionTrue, Reason: "FailedCreate"}}}}
	v := NewInventory()
	v.Add("ReplicaSet", rs)
	s := NewDetector(c).Workloads(v, time.Now())
	if len(s) != 1 || s[0].Class != "ReplicaSetFailedCreate" {
		t.Fatal("missing FailedCreate")
	}
}

func TestNativeSidecarJobCompletionIsNormal(t *testing.T) {
	c := cfg(t, "prod")
	always := corev1.ContainerRestartPolicyAlways
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "jobpod", UID: "p"}, Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "sidecar", RestartPolicy: &always}}}, Status: corev1.PodStatus{Phase: corev1.PodSucceeded, InitContainerStatuses: []corev1.ContainerStatus{{Name: "sidecar", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 143}}}}}}
	v := NewInventory()
	v.Add("Pod", p)
	if len(NewDetector(c).Pod(p, v, time.Now())) != 0 {
		t.Fatal("native sidecar shutdown alerted")
	}
}
func TestAvailableDeploymentDuringRolloutIsNotFailure(t *testing.T) {
	c := cfg(t, "prod")
	n := int32(4)
	o := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "api", UID: "d", Generation: 2}, Spec: appsv1.DeploymentSpec{Replicas: &n}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2, AvailableReplicas: 3, Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue}}}}
	v := NewInventory()
	v.Add("Deployment", o)
	if len(NewDetector(c).Workloads(v, time.Now())) != 0 {
		t.Fatal("normal rollout alerted")
	}
}
func TestReplacementGapIsNotRecovery(t *testing.T) {
	n := int32(1)
	o := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "api", UID: "d"}, Spec: appsv1.DeploymentSpec{Replicas: &n}, Status: appsv1.DeploymentStatus{AvailableReplicas: 1}}
	v := NewInventory()
	v.Add("Deployment", o)
	healthy, _ := Recovery(v, nil, Signal{Container: "app", Workload: Resource{Kind: "Deployment", Namespace: "default", Name: "api", UID: "d"}})
	if healthy {
		t.Fatal("zero observed pods counted as recovered")
	}
}
