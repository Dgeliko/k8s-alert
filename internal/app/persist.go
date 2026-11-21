package app

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"k8s-alert/internal/monitor"
	"k8s-alert/internal/notify"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
)

type Checkpoint struct {
	Version   int                          `json:"version"`
	Cluster   string                       `json:"cluster"`
	Incidents map[string]*monitor.Incident `json:"incidents"`
	Jobs      map[string]*notify.Job       `json:"jobs"`
}
type Persistence struct {
	Client                   kubernetes.Interface
	Namespace, Name, Cluster string
}

func encodeState(c Checkpoint) (string, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return "", e
	}
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, e = w.Write(b); e != nil {
		return "", e
	}
	if e = w.Close(); e != nil {
		return "", e
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}
func (p Persistence) Save(ctx context.Context, c Checkpoint) error {
	c.Version = 1
	c.Cluster = p.Cluster
	data, err := encodeState(c)
	if err != nil {
		return err
	}
	if len(data) > 700*1024 {
		for _, j := range c.Jobs {
			j.Request.Payload.Logs = nil
			j.Request.Payload.Events = nil
		}
		data, err = encodeState(c)
		if err != nil {
			return err
		}
	}
	if len(data) > 700*1024 {
		return fmt.Errorf("checkpoint exceeds 700 KiB; previous checkpoint retained")
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "k8s-alert"}}, Data: map[string]string{"state.gz.b64": data}}
	old, err := p.Client.CoreV1().ConfigMaps(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = p.Client.CoreV1().ConfigMaps(p.Namespace).Create(ctx, cm, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	cm.ResourceVersion = old.ResourceVersion
	cm.Labels = old.Labels
	cm.Annotations = old.Annotations
	_, err = p.Client.CoreV1().ConfigMaps(p.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	return err
}
func (p Persistence) Load(ctx context.Context) (Checkpoint, error) {
	var c Checkpoint
	cm, e := p.Client.CoreV1().ConfigMaps(p.Namespace).Get(ctx, p.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(e) {
		return c, nil
	}
	if e != nil {
		return c, e
	}
	b, e := base64.StdEncoding.DecodeString(cm.Data["state.gz.b64"])
	if e != nil {
		return c, e
	}
	z, e := gzip.NewReader(bytes.NewReader(b))
	if e != nil {
		return c, e
	}
	defer z.Close()
	b, e = io.ReadAll(io.LimitReader(z, 16*1024*1024+1))
	if e != nil {
		return c, e
	}
	if len(b) > 16*1024*1024 {
		return c, fmt.Errorf("checkpoint too large")
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return c, e
	}
	if c.Version != 1 || c.Cluster != p.Cluster {
		return c, fmt.Errorf("checkpoint version/cluster mismatch")
	}
	for _, in := range c.Incidents {
		if in != nil {
			for name := range in.Pending {
				delete(in.Pending, name)
			}
		}
	}
	for _, j := range c.Jobs {
		if j != nil {
			j.InFlight = false
			if j.Next.Before(time.Now()) {
				j.Next = time.Now()
			}
		}
	}
	return c, nil
}
