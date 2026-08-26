# Contributing

VibeDB changes must preserve correctness, isolation, durability, bounded
resource use, and format validation. Do not trade one of these contracts for a
benchmark result.

The repository targets Go 1.26. Contracts can change between tested commits.

## Start with a focused test

Run the smallest package and test while you work:

```bash
go test ./internal/storeio -run '^TestName$' -count=1
go test ./store/durable -run '^TestName$' -count=1
go test ./query -run '^TestName$' -count=1
```

Use `-race` for concurrency, snapshot, cache, lease, or publication changes.
Use fault injection and reopen oracles for a persistence change.

## Run the root checks

```bash
go build ./...
go vet ./...
go test -p=1 -timeout=25m ./...
git diff --check
```

CI runs build, vet, and tests on Linux amd64 and arm64. It also cross-compiles
root and PostgreSQL-client packages for 32-bit Linux and Windows.

The focused race lane is:

```bash
go test -race -timeout=25m \
  -skip 'Qualification' \
  -run 'Primary|BufferedInplace|Committer|PageCache|WriteTransaction' \
  ./internal/storeio/ ./store/durable/
```

CI also runs a focused cross-layer LATERAL race suite.

## Run nested-module checks

The competitive harness is a separate module:

```bash
cd bench/competitive
go test ./...
```

Do not add competitor dependencies to the root `go.mod`.

The PostgreSQL client integration is also a separate module:

```bash
cd integration/pgclient
go test -timeout=2m ./...
```

## Match tests to the change

- A codec change needs byte-exact fixtures, truncation checks, checksum-valid
  semantic corruption, reserved-zero validation, and bounds checks.
- A root or copy-on-write change needs injected failure before and after each
  write and barrier, followed by reopen.
- A journal change needs append, sync, recycle, torn-tail, and identity tests.
- A snapshot change needs an old held generation, pressure, release, retry, and
  race tests.
- An index change needs collision candidates, exact-value rechecks, mutation
  maintenance, reopen, and scan differential tests.
- An optimized path needs a correctness oracle and an allocation or route
  assertion when the API promises that property.
- A query change needs heap and durable differential coverage.
- A distributed change needs stale identity, admission precedence, bounded
  fanout, cancellation, and partial-failure tests.

A benchmark is not a correctness test.

## Change the on-disk format

Format 0 is an unreleased development format. A change replaces the current
grammar and its golden images. Do not add a compatibility decoder for an
obsolete development image.

In the same change:

1. Update codecs and validation.
2. Replace affected fixtures in `internal/storeio/testdata/format0`.
3. Add malformed and truncated input tests.
4. Update [the format reference](docs/format.md).
5. Update [the architecture](docs/architecture.md) when the root graph changes.
6. Update [durability](docs/durability.md) when an acknowledgement or recovery
   boundary changes.

## Measure performance

Run the allocation gate for a hot-path change:

```bash
go run ./bench/gate
```

The gate compares the working tree and base on the same machine. It gates
allocations and bytes, not time.

Follow [the performance measurement rules](docs/performance.md) for a published
number. Keep raw rows and exact provenance.

## Change unsafe code

Read [the unsafe-code boundary](UNSAFE.md). Keep pointer-free storage free of Go
pointers. Keep every backing owner alive for the full borrow.

Regenerate the inventory after an unsafe import changes:

```bash
go test ./internal/unsafeaudit -run TestUnsafeFileListMatchesSource -update
```

## Change generated documentation

Regenerate benchmark coverage from the nested module:

```bash
cd bench/competitive
go generate .
go test -run '^TestBenchmarkCoverage' -count=1 ./internal/coverage
```

Do not hand-edit the generated capability matrix or benchmark coverage table.
The related golden tests compare exact bytes.

Use [the documentation language guide](docs/STYLE.md). Keep industry terms and
exact code identifiers. Add implementation references for an internal contract.

## Review the final diff

```bash
git status --short
git diff --check
git diff --stat
```

Confirm that the change does not include local caches, benchmark binaries,
temporary data, or unrelated user work.
