# syntax=docker/dockerfile:1.7
FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/vibedb ./cmd/vibedb && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/vibedb-shard ./cmd/vibedb-shard && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/vibedb-gateway ./cmd/vibedb-gateway && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/vibedb-operator ./cmd/vibedb-operator && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/vibedb-kube-qualify ./cmd/vibedb-kube-qualify

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/vibedb"]
