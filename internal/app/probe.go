package app

import (
	"context"
	"fmt"
	"k8s-alert/internal/monitor"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// freshInventory is only used to verify recoveries while incidents exist.
// Never turn a successful connectivity probe into proof of a fresh cache.
func freshInventory(ctx context.Context, c kubernetes.Interface) (*monitor.Inventory, []*corev1.Pod, error) {
	v := monitor.NewInventory()
	o := metav1.ListOptions{}
	var pods []*corev1.Pod
	ps, e := c.CoreV1().Pods("").List(ctx, o)
	if e != nil {
		return nil, nil, fmt.Errorf("list Pods: %w", e)
	}
	for i := range ps.Items {
		p := &ps.Items[i]
		v.Add("Pod", p)
		pods = append(pods, p)
	}
	ds, e := c.AppsV1().Deployments("").List(ctx, o)
	if e != nil {
		return nil, nil, e
	}
	for i := range ds.Items {
		v.Add("Deployment", &ds.Items[i])
	}
	rs, e := c.AppsV1().ReplicaSets("").List(ctx, o)
	if e != nil {
		return nil, nil, e
	}
	for i := range rs.Items {
		v.Add("ReplicaSet", &rs.Items[i])
	}
	ss, e := c.AppsV1().StatefulSets("").List(ctx, o)
	if e != nil {
		return nil, nil, e
	}
	for i := range ss.Items {
		v.Add("StatefulSet", &ss.Items[i])
	}
	daemon, e := c.AppsV1().DaemonSets("").List(ctx, o)
	if e != nil {
		return nil, nil, e
	}
	for i := range daemon.Items {
		v.Add("DaemonSet", &daemon.Items[i])
	}
	jobs, e := c.BatchV1().Jobs("").List(ctx, o)
	if e != nil {
		return nil, nil, e
	}
	for i := range jobs.Items {
		v.Add("Job", &jobs.Items[i])
	}
	cron, e := c.BatchV1().CronJobs("").List(ctx, o)
	if e != nil {
		return nil, nil, e
	}
	for i := range cron.Items {
		v.Add("CronJob", &cron.Items[i])
	}
	return v, pods, nil
}
