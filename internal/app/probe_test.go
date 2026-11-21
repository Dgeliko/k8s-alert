package app

import (
	"context"
	"fmt"
	"k8s-alert/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"reflect"
	"testing"
)

func TestObservationScope(t *testing.T) {
	for _, tc := range []struct {
		selection config.Selection
		want      []string
	}{
		{config.Selection{}, []string{metav1.NamespaceAll}},
		{config.Selection{Include: []string{"team*"}}, []string{metav1.NamespaceAll}},
		{config.Selection{Include: []string{"team01", "team02", "team01"}, Exclude: []string{"*02"}}, []string{"team01"}},
		{config.Selection{Include: []string{"team01"}, Exclude: []string{"team*"}}, []string{}},
	} {
		cfg := &config.Config{Filters: config.Filters{Namespaces: tc.selection}}
		if got := cfg.ObservationNamespaces(); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("scope for %+v = %v, want %v", tc.selection, got, tc.want)
		}
	}
}

func TestFreshInventoryOnlyListsSelectedNamespaces(t *testing.T) {
	c := fake.NewSimpleClientset(
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "team01"}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ignored", Namespace: "team02"}},
	)
	_, pods, err := freshInventory(context.Background(), c, []string{"team01"})
	if err != nil || len(pods) != 1 || pods[0].Name != "api" {
		t.Fatalf("unexpected snapshot: %v, %v", pods, err)
	}
	for _, a := range c.Actions() {
		if a.GetNamespace() != "team01" {
			t.Fatalf("unexpected API scope: %s %s in %q", a.GetVerb(), a.GetResource().Resource, a.GetNamespace())
		}
	}
	c.PrependReactor("list", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		return a.GetNamespace() == "team03", nil, fmt.Errorf("offline")
	})
	v, pods, err := freshInventory(context.Background(), c, []string{"team01", "team03"})
	if err == nil || v != nil || pods != nil {
		t.Fatal("partial observations must not confirm recovery")
	}
}

func TestFailedVerificationDoesNotReturnHealthySnapshot(t *testing.T) {
	c := fake.NewSimpleClientset()
	c.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("offline") })
	v, _, err := freshInventory(context.Background(), c, []string{metav1.NamespaceAll})
	if err == nil || v != nil {
		t.Fatal("failed API accepted as fresh")
	}
}
