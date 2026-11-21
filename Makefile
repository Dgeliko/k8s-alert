VERSION ?= dev
IMAGE ?= ghcr.io/dgeliko/k8s-alert

.PHONY: test build chart image release-binaries

test:
	go test -race ./...
	go vet ./...

build:
	go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o bin/k8s-alert ./cmd/k8s-alert

chart:
	helm lint charts/k8s-alert
	helm template k8s-alert charts/k8s-alert -n k8s-alert -f examples/dev.yaml > /dev/null
	helm template k8s-alert charts/k8s-alert -n k8s-alert -f examples/prod.yaml > /dev/null

image:
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) --push .

release-binaries:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o dist/k8s-alert-linux-amd64 ./cmd/k8s-alert
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w -X main.version=$(VERSION)" -o dist/k8s-alert-linux-arm64 ./cmd/k8s-alert
