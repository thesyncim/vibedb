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

## Fast feedback

```bash
go test ./path/to/package -run '^TestName$' -count=1
go test -race ./path/to/package -run '^TestConcurrentName$' -count=1
```

Use fault injection and reopen oracles for persistence work. Use real process
or transport tests when the contract crosses a process, TLS, or filesystem
boundary. A benchmark is never a correctness test.

## Root checks

```bash
go build ./...
go vet ./...
go test -p=1 -timeout=25m ./...
git diff --check
```

Check [current status](docs/status.md) for any recorded baseline failure. Do not
silently attribute an old failure to your change or hide a new one as
“pre-existing.” Record the exact command and comparison.

## Nested modules

```bash
(cd bench/competitive && go test ./...)
(cd integration/pgclient && go test -timeout=2m ./...)
(cd integration/pgcompat && go test ./...)
(cd x/vitessroute && go test ./...)
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
(cd bench/competitive && go test ./internal/coverage)
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

Follow [the documentation standard](docs/STYLE.md). Lead with maturity and
failure boundaries. Keep tutorials runnable, reference tables exact, and
implementation evidence in a compact source map. Never use “PostgreSQL
compatible,” “production,” “shipped,” or “bounded memory” without the precise
scope that source and tests establish.

## Final review

```bash
git status --short
git diff --check
git diff --stat
```

Confirm that the diff contains no caches, benchmark binaries, credentials,
generated cluster state, evidence directories, or unrelated user work.
