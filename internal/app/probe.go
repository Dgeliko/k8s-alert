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
func freshInventory(ctx context.Context, c kubernetes.Interface, namespaces []string) (*monitor.Inventory, []*corev1.Pod, error) {
	v := monitor.NewInventory()
	var pods []*corev1.Pod
	for _, namespace := range namespaces {
		var err error
		pods, err = appendNamespaceInventory(ctx, c, namespace, v, pods)
		if err != nil {
			return nil, nil, fmt.Errorf("list recovery inventory in namespace %q: %w", namespace, err)
		}
	}
	return v, pods, nil
}

func appendNamespaceInventory(ctx context.Context, c kubernetes.Interface, namespace string, v *monitor.Inventory, pods []*corev1.Pod) ([]*corev1.Pod, error) {
	o := metav1.ListOptions{}
	ps, e := c.CoreV1().Pods(namespace).List(ctx, o)
	if e != nil {
		return nil, fmt.Errorf("list Pods: %w", e)
	}
	for i := range ps.Items {
		p := &ps.Items[i]
		v.Add("Pod", p)
		pods = append(pods, p)
	}
	ds, e := c.AppsV1().Deployments(namespace).List(ctx, o)
	if e != nil {
		return nil, e
	}
	for i := range ds.Items {
		v.Add("Deployment", &ds.Items[i])
	}
	rs, e := c.AppsV1().ReplicaSets(namespace).List(ctx, o)
	if e != nil {
		return nil, e
	}
	for i := range rs.Items {
		v.Add("ReplicaSet", &rs.Items[i])
	}
	ss, e := c.AppsV1().StatefulSets(namespace).List(ctx, o)
	if e != nil {
		return nil, e
	}
	for i := range ss.Items {
		v.Add("StatefulSet", &ss.Items[i])
	}
	daemon, e := c.AppsV1().DaemonSets(namespace).List(ctx, o)
	if e != nil {
		return nil, e
	}
	for i := range daemon.Items {
		v.Add("DaemonSet", &daemon.Items[i])
	}
	jobs, e := c.BatchV1().Jobs(namespace).List(ctx, o)
	if e != nil {
		return nil, e
	}
	for i := range jobs.Items {
		v.Add("Job", &jobs.Items[i])
	}
	cron, e := c.BatchV1().CronJobs(namespace).List(ctx, o)
	if e != nil {
		return nil, e
	}
	for i := range cron.Items {
		v.Add("CronJob", &cron.Items[i])
	}
	return pods, nil
}
