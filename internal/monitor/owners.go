package monitor

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Inventory struct{ Objects map[string]metav1.Object }

func NewInventory() *Inventory               { return &Inventory{Objects: map[string]metav1.Object{}} }
func objectKey(kind, ns, name string) string { return kind + "/" + ns + "/" + name }
func (v *Inventory) Add(kind string, obj runtime.Object) {
	m, e := meta.Accessor(obj)
	if e == nil {
		v.Objects[objectKey(kind, m.GetNamespace(), m.GetName())] = m
	}
}
func (v *Inventory) Resolve(kind string, obj metav1.Object) (Resource, []string, bool) {
	cur := obj
	r := Resource{Kind: kind, Name: cur.GetName(), Namespace: cur.GetNamespace(), UID: string(cur.GetUID())}
	kinds := []string{kind}
	seen := map[string]bool{}
	for depth := 0; depth < 12; depth++ {
		var owner *metav1.OwnerReference
		for _, o := range cur.GetOwnerReferences() {
			if o.Controller != nil && *o.Controller {
				cp := o
				owner = &cp
				break
			}
		}
		if owner == nil {
			return r, kinds, false
		}
		kinds = append(kinds, owner.Kind)
		key := objectKey(owner.Kind, cur.GetNamespace(), owner.Name)
		if seen[key] {
			return r, kinds, true
		}
		seen[key] = true
		m, ok := v.Objects[key]
		if !ok || m.GetUID() != owner.UID {
			return r, kinds, true
		}
		cur = m
		r = Resource{Kind: owner.Kind, Name: owner.Name, Namespace: m.GetNamespace(), UID: string(owner.UID)}
	}
	return r, kinds, true
}
func Key(r Resource, container, class string) string {
	if class == "ProbeFailure" {
		class = "ContainerCrash"
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s", r.Namespace, r.Kind, r.UID, container, class)
}
