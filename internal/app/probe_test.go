package app

import (
	"context"
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"testing"
)

func TestFailedVerificationDoesNotReturnHealthySnapshot(t *testing.T) {
	c := fake.NewSimpleClientset()
	c.PrependReactor("list", "pods", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("offline") })
	v, _, err := freshInventory(context.Background(), c)
	if err == nil || v != nil {
		t.Fatal("failed API accepted as fresh")
	}
}
