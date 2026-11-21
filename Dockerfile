# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine AS build
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -mod=readonly -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/k8s-alert ./cmd/k8s-alert
FROM alpine:3.22 AS certificates
RUN apk add --no-cache ca-certificates
FROM scratch
ARG VERSION=dev
LABEL org.opencontainers.image.source="https://github.com/Dgeliko/k8s-alert" \
      org.opencontainers.image.version="$VERSION"
COPY --from=certificates /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/k8s-alert /k8s-alert
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/k8s-alert"]
