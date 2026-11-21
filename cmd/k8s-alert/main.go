package main

import (
	"context"
	"flag"
	"fmt"
	"k8s-alert/internal/app"
	"k8s-alert/internal/config"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("k8s-alert stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	file := flag.String("config", "/etc/k8s-alert/config.yaml", "configuration file")
	kubeconfig := flag.String("kubeconfig", "", "external kubeconfig path")
	validate := flag.Bool("validate", false, "validate configuration without connecting")
	ver := flag.Bool("version", false, "print version")
	flag.Parse()
	if *ver {
		fmt.Println(version)
		return nil
	}
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	cfg, err := config.Load(b)
	if err != nil {
		return err
	}
	if *validate {
		fmt.Println("configuration valid")
		return nil
	}
	var rc *rest.Config
	if *kubeconfig != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return err
	}
	rc.UserAgent = "k8s-alert/" + version
	rc.QPS = 10
	rc.Burst = 20
	client, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	slog.Info("k8s-alert starting", "version", version, "cluster", cfg.Cluster, "env", cfg.Env)
	return (&app.App{Config: cfg, Client: client}).Run(ctx)
}
