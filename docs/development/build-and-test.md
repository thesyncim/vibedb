# Build and test

[Documentation](../README.md) / [Developer guide](README.md)

Run commands from the repository root with Go 1.27+ (`go.mod`). Use the
standard `go env GOCACHE`.

## Make targets

`Makefile` exports `GOEXPERIMENT=simd` (override with `GOEXPERIMENT=nosimd` for
portable builds):

```sh
make build
make vet
make test PACKAGES=./distribution TEST_FLAGS='-run ^TestName$ -count=1'
make test GOEXPERIMENT=nosimd PACKAGES=./distribution TEST_FLAGS='-short -count=1'
make bench                # curated public-op benchmarks at -cpu=1
make docs-check           # Markdown links, anchors, and structure
```

For full root runs, use `make test TEST_FLAGS='-p=1 -timeout=25m'` or run CI
shards directly. Nested modules (`bench/competitive`, `integration/pgclient`,
`integration/pgcompat`, `x/vitessroute`) must be tested from their own
directories ([Contributing](../../CONTRIBUTING.md#nested-modules)).

## CI shards and race lanes

`scripts/ci/test-shard.sh <shard>` partitions `go list ./...` into `durable`,
`durable-churn`, `durable-large-cache`, `sql`, `process`, and `core` (plus
`go vet ./...`):

```sh
scripts/ci/test-shard.sh sql
scripts/ci/storage-race.sh all                                   # storage race lane
GOEXPERIMENT=simd go test -race -p=4 -count=1 -timeout=30m \
  ./gateway ./internal/multiraft ./internal/snapshottransfer \
  ./internal/raftservice ./internal/replicatedstate              # distributed race
CGO_ENABLED=0 GOOS=windows GOARCH=386 GOEXPERIMENT=simd \
  go test -run '^$' -exec=true ./...                             # 32-bit cross-compile
python3 -m unittest discover -s scripts/ci -p '*_test.py'        # CI script self-tests
```

## Linux-only and process qualification gates

Many durable and multi-process tests carry `//go:build linux` or require Linux
`fallocate`/`/proc` (on macOS, filtered runs may report `[no tests to run]`).
Expensive process tests skip unless their `VIBEDB_*_E2E=1` gate is set:

| Gate | Package |
| --- | --- |
| `VIBEDB_DURABLE_SQL_RF3_E2E=1`, `VIBEDB_RF3_QUORUM_QUALIFICATION=1` | `internal/raftservice` |
| `VIBEDB_REPLICA_REPLACEMENT_E2E=1`, `VIBEDB_HOT_SHARD_MUTATION_E2E=1`, `VIBEDB_DURABLE_RF3_PROCESS_E2E=1`, `VIBEDB_READ_BATCH_RF3_PROCESS_E2E=1`, `VIBEDB_DURABLE_RF3_MULTIRELATION_E2E=1`, `VIBEDB_FUSED_RF3_PROCESS_E2E=1`, `VIBEDB_SEAMLESS_SCALE_E2E=1`, `VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE_E2E=1` | `internal/gatewayruntime` |
| `VIBEDB_WAL_RETENTION_E2E=1`, `VIBEDB_RESTORE_RF3_PROCESS_E2E=1` | `cmd/vibedb-shard` |
| `VIBEDB_RESTORE_ACTIVATION_E2E=1` / `VIBEDB_AUTH_TRANSPORT_PROCESS_E2E=1` / `VIBEDB_APPLY_10M=1` | `internal/clusterrestore` / `gateway` / `internal/replicatedstate` |
| `VIBEDB_TEST_PSQL=1` | `integration/pgclient` |

Always run gated tests with `-count=1`.

## Reproduce Linux CI from macOS

Mount caches and `TMPDIR` on named Docker volumes (ext4 in the Linux VM, so
strict allocation works):

```sh
docker run --rm \
  -v "$PWD":/src:ro -w /src \
  -e GOEXPERIMENT=simd -e GOFLAGS=-buildvcs=false -e TMPDIR=/work \
  -v vibedb-gomod:/go/pkg/mod \
  -v vibedb-gocache:/root/.cache/go-build \
  -v vibedb-work:/work \
  -e VIBEDB_HOT_SHARD_MUTATION_E2E=1 \
  golang:1.27.0 \
  go test -count=1 -timeout=6m -run '^TestGatewayHotShardMutationProcesses$' -v ./internal/gatewayruntime
```

## Pre-push checklist

```sh
git diff --check
go vet ./...
go run ./bench/gate
go generate ./internal/buildgate ./internal/featurestate && git diff --exit-code
make docs-check
```

See [qualification workflows](../qualification/README.md) and
[STYLE.md](../STYLE.md#verify-changes).

## Source map

- [Makefile](../../Makefile) · [scripts/ci/test-shard.sh](../../scripts/ci/test-shard.sh) · [storage-race.sh](../../scripts/ci/storage-race.sh) · [.github/workflows/ci.yml](../../.github/workflows/ci.yml)
