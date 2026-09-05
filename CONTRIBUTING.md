# Contributing to VibeDB

> [!CAUTION]
> VibeDB is unreleased. Compatibility may be broken deliberately, but a change
> must replace the current contract coherently across code, tests, fixtures,
> generators, and documentation. “Development” is not permission to make
> correctness or failure behavior implicit.

## Before you change code

1. Read [current status](docs/status.md) and capture a clean baseline.
2. Find the narrowest owning package and its failure/reopen tests.
3. State the invariant, resource bound, ownership rule, and error outcome that
   the change affects.
4. Keep unrelated working-tree changes intact.

The repository targets the Go version in `go.mod`. Use focused tests while
iterating, then run the root and nested-module checks that match the change.

## Default build, test, and benchmark commands

Repository commands default to `GOEXPERIMENT=simd` on Go 1.27:

```sh
make build
make test
make bench
make info
```

`make bench` selects a small set of public-operation benchmarks for fast
feedback. Narrow tests with `PACKAGES` and `TEST_FLAGS`; choose benchmark
packages and rows with `BENCH_PACKAGES` and `BENCH`. `BENCHTIME` defaults to
250 ms and `COUNT` to one; use `COUNT=10` for repeated measurements. Override
`GO` to select a specific compiler. Run benchmarks alone after correctness
checks; building or testing concurrently invalidates timing comparisons.

Portable checks are explicit: `make test GOEXPERIMENT=nosimd` or
`make bench GOEXPERIMENT=nosimd`. CI and standalone test/benchmark scripts use
SIMD by default and retain named portable parity checks. For raw Go commands
and nested modules, set `GOEXPERIMENT=simd` in the command or shell environment;
`go.mod` cannot enable compiler experiments.

## Fast feedback

```bash
make test PACKAGES=./path/to/package TEST_FLAGS='-run ^TestName$ -count=1'
make test PACKAGES=./path/to/package TEST_FLAGS='-race -run ^TestConcurrentName$ -count=1'
```

Use fault injection and reopen oracles for persistence work. Use real process
or transport tests when the contract crosses a process, TLS, or filesystem
boundary. A benchmark is never a correctness test.

## Root checks

```bash
make build
make vet
make test TEST_FLAGS='-p=1 -timeout=25m'
make test GOEXPERIMENT=nosimd PACKAGES='./distribution ./internal/rangesplit ./sql/driver'
git diff --check
```

Check [current status](docs/status.md) for any recorded baseline failure. Do not
silently attribute an old failure to your change or hide a new one as
“pre-existing.” Record the exact command and comparison.

## Nested modules

```bash
(cd bench/competitive && GOEXPERIMENT=simd go test ./...)
(cd integration/pgclient && GOEXPERIMENT=simd go test -timeout=2m ./...)
(cd integration/pgcompat && GOEXPERIMENT=simd go test ./...)
(cd x/vitessroute && GOEXPERIMENT=simd go test ./...)
```

Java/JDBC, stock `psql`, Linux fault, Docker, and Kind lanes are opt-in or
environment-specific. Their individual docs state prerequisites and what a
pass proves.

## Match evidence to the change

| Change | Minimum evidence |
| --- | --- |
| Codec or page grammar | Golden round trip, truncation, checksum-valid semantic corruption, reserved-field and bound checks |
| Journal/root ordering | Failure before and after each write/barrier/sync, then reopen |
| Snapshot or cache | Held old generation, pressure, release, retry, and race coverage |
| Index | Collision candidates, exact recheck, mutation maintenance, reopen, scan differential |
| Query/SQL | Heap and durable differential tests; cancellation and result-budget cases |
| Distributed protocol | Stale identity, authorization, byte bounds, cancellation, ambiguous response, exact retry |
| Optimized/unsafe path | Portable oracle, allocation/escape check when promised, race/checkptr/lifetime tests |

## Change the development disk format

Format 0 has no compatibility ladder. An intentional change replaces the
current grammar; it does not add a reader for every obsolete image.

In the same commit:

1. update codecs and semantic validation;
2. replace affected `internal/storeio/testdata/format0` fixtures;
3. retain malformed-old-layout rejection;
4. update [the format reference](docs/format.md), [architecture](docs/architecture.md),
   and [durability](docs/durability.md);
5. regenerate build grammar identities when the manifest contract changes;
6. add crash/reopen evidence for the new publication order.

## Generated contracts

Run the generators after changing their source:

```bash
go generate ./internal/buildgate
go generate ./internal/featurestate
go generate ./internal/conformance
(cd bench/competitive && go generate .)
go test ./internal/buildgate ./internal/featurestate ./internal/conformance
(cd bench/competitive && GOEXPERIMENT=simd go test ./internal/coverage)
```

Refresh the unsafe inventory when a production import changes:

```bash
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

Do not hand-edit generated blocks in `UNSAFE.md`, `docs/capabilities.md`,
`docs/distributed-feature-state.md`, or `bench/competitive/COVERAGE.md`.

## Measure performance honestly

```bash
go run ./bench/gate
```

The allocation gate compares allocations and bytes against a Git base; it does
not gate time. A publishable claim requires the complete provenance, raw rows,
repetitions, and validator scope in [performance evidence](docs/performance.md).
No selective number or CI artifact becomes a product claim by being copied into
prose.

## Documentation changes

Follow [the documentation style](docs/STYLE.md). Keep the README concise,
procedures runnable, and design explanations separate from dated research.
Update the appropriate index when adding a page. Preserve historical evidence
and regenerate generated pages through their source.

After installing [the checker dependencies](docs/STYLE.md#check-a-documentation-change):

```sh
make docs-check
python3 -m unittest discover -s scripts/docs -p 'test_*.py'
git diff --check
```

Set `PYTHON` to the virtual environment's Python when needed. Compile complete
Go examples and run changed procedures against disposable data. For a prose
change, these targeted checks are sufficient; run package, process, or fault
tests when the change depends on an unverified behavior. Record the actual
validation in the pull request rather than adding a running test log to the
README or status page.

## Final review

```bash
git status --short
git diff --check
git diff --stat
```

Confirm that the diff contains no caches, benchmark binaries, credentials,
generated cluster state, evidence directories, or unrelated user work.
