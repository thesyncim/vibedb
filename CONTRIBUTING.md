# Contributing to VibeDB

VibeDB is unreleased development software. Breaking a contract deliberately is
fine when code, tests, fixtures, generators, and docs are updated together.
Read [stability](docs/status.md) and the [developer guide](docs/development/README.md)
before making large changes.

## Build, test, and benchmark

Requires Go 1.27+ (`go.mod`). `Makefile` enables `GOEXPERIMENT=simd` by
default; pass `GOEXPERIMENT=nosimd` for portable checks, and set
`GOEXPERIMENT=simd` explicitly for raw `go` commands.

```sh
make build
make vet
make test PACKAGES=./path/to/pkg TEST_FLAGS='-run ^TestName$ -count=1'
make test PACKAGES=./path/to/pkg TEST_FLAGS='-race -run ^TestName$ -count=1'
make test GOEXPERIMENT=nosimd PACKAGES='./distribution ./internal/rangesplit ./sql/driver'
```

For full root runs use `make test TEST_FLAGS='-p=1 -timeout=25m'`, or run CI
shards directly (`scripts/ci/test-shard.sh <shard>`). Process qualifications
skip unless their `VIBEDB_*_E2E=1` gate is set (most are Linux-only; always use
`-count=1`). See [build and test](docs/development/build-and-test.md).

## Nested modules

`go test ./...` at the root does not test nested modules. Run them directly when
affected:

```sh
(cd bench/competitive && GOEXPERIMENT=simd go test ./...)
(cd integration/pgclient && GOEXPERIMENT=simd go test -timeout=2m ./...)
(cd integration/pgcompat && GOEXPERIMENT=simd go test ./...)
(cd x/vitessroute && GOEXPERIMENT=simd go test ./...) # not run by CI
```

## Match evidence to the change

| Change | Required evidence |
| --- | --- |
| Codec / disk format | Golden round trip, truncation, semantic corruption, bound checks; update `internal/storeio/testdata/format0` and [format.md](docs/format.md) |
| Journal / root ordering | Fault injection before/after each write/barrier/sync + reopen |
| Snapshot / cache | Held generation, pressure, release, retry, and `-race` tests |
| Index / query / SQL | Recheck, reopen, heap vs durable differential, budget & cancellation tests |
| Distributed protocol | Stale fence, authz, byte bounds, unknown outcome, exact-identity retry |
| Optimized / `unsafe` | Portable oracle, `testing.AllocsPerRun`, `-race`/`checkptr` |

## Generated contracts

Never hand-edit generated blocks (`docs/capabilities.md`,
`docs/distributed-feature-state.md`, `bench/competitive/COVERAGE.md`,
`UNSAFE.md`). Regenerate from source (enforced by CI `repository contracts`):

```sh
go generate ./internal/buildgate ./internal/featurestate ./internal/conformance
(cd bench/competitive && go generate .)
go test ./internal/buildgate ./internal/featurestate ./internal/conformance
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

## Allocation gate and documentation checks

```sh
go run ./bench/gate                                      # allocs/op & B/op vs base
make docs-check                                          # links, anchors, structure
python3 -m unittest discover -s scripts/docs -p 'test_*.py'
git diff --check
```

Follow [docs/STYLE.md](docs/STYLE.md#verify-changes) when editing docs. Before
pushing, verify `git status --short` contains no caches, credentials, or
stray artifacts.
