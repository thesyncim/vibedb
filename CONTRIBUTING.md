# Contributing

Changes must preserve correctness, ownership, portability, and the documented
allocation contract before they improve a benchmark.

## Toolchains

Use the latest Go 1.26 patch release for the stable portable lane. SIMD changes
also require the exact development compiler built by:

```sh
./scripts/bootstrap-gotip.sh "$HOME/sdk/vibejson-gotip"
```

Stable Go builds the portable implementation. The pinned development compiler
must pass both portable and `GOEXPERIMENT=simd` modes on amd64 and arm64;
unvalidated compiler families keep the portable source set.

## Required local checks

Start with the stable lane:

```sh
GOTOOLCHAIN=local go test ./...
GOTOOLCHAIN=local go vet ./...
```

Then run the pinned compiler in both source modes:

```sh
export GOTIP="$HOME/sdk/vibejson-gotip/bin/go"
GOTOOLCHAIN=local "$GOTIP" test ./...
GOTOOLCHAIN=local GOEXPERIMENT=simd "$GOTIP" test ./...
GOTOOLCHAIN=local "$GOTIP" vet ./...
```

Before committing:

```sh
go generate ./...
go mod tidy
go run ./internal/cmd/testcontracts -check
git diff --check
```

Generated output belongs in the same commit as its generator or source change.

## Correctness

Add the smallest permanent test that proves the changed contract:

- parser and codec behavior needs differential coverage where
  `encoding/json` has the same semantics;
- stream changes need fragmented-I/O, boundary, and terminal-state coverage;
- ownership changes need retained-result, forced-GC, and stack-growth coverage;
- persistence changes need fault injection, reopen, and previous-generation
  recovery coverage;
- optimized routes need portable/accelerated parity and a malformed-input path.

`internal/cmd/testcontracts/contracts.txt` is the machine-checked ownership map
for test files, fuzz targets, and checked-in corpus seeds. It is checker input,
not user documentation.

## Unsafe and external memory

Unsafe code is permitted only for a bounded, measured path that ordinary Go
cannot express without violating a maintained contract.

Do not hide a Go pointer in `uintptr`, depend on a private runtime layout, or
place Go pointers in external memory.

After changing an unsafe scope:

```sh
"$GOTIP" run ./internal/cmd/unsafeinventory -write UNSAFE.md
"$GOTIP" run ./internal/cmd/unsafeinventory -check UNSAFE.md
"$GOTIP" test -race -skip 'Alloc|ZeroCost|StaysOnStack' ./...
GOEXPERIMENT=simd "$GOTIP" test \
  -gcflags=all=-d=checkptr=2 \
  -skip 'Alloc|ZeroCost|StaysOnStack' ./...
```

Review the affected bounds, GC visibility, ownership, aliases, and fallback
behavior in [UNSAFE.md](UNSAFE.md).

## Performance

Compare the change with its merge base using the same compiler, CPU, operating
system, input, and benchmark duration. Report time, bytes, and allocations.
Inspect retained memory and generated code when the change affects either.

Use `scripts/bench-gate.sh` for maintained root benchmarks:

```sh
BENCH_GO="$(command -v go)" BENCH_GOEXPERIMENT= \
  ./scripts/bench-gate.sh -b HEAD~1 -c 63
```

The gate rejects statistically significant time regressions above 2% and any
significant bytes/op or allocations/op increase. A targeted run must record its
exact selector and row count.

A synthetic kernel result does not justify a specialization by itself. Keep a
portable implementation and add a route test that proves when the optimized
path is selected.

### Competitive benchmarks

`bench/competitive` measures `store` and `store/durable` against bbolt, Badger,
Pebble, and pure-Go SQLite on one shared JSON corpus. It is a **separate Go
module** with its own `go.mod` that replaces vibejson with `../..`, because the
root module's dependency set is maintained deliberately: `golang.org/x/sys` and
nothing else. Never add a competitor dependency to the root `go.mod`;
`git diff go.mod` must stay empty when this harness changes, and the `competitive`
CI job fails if any import outside `golang.org/x/sys` reaches the root module.

```sh
cd bench/competitive
go test -run TestFullEquivalence -v .                      # every engine must agree, on every key
go test -run '^$' -bench=. -count=6 -timeout=180m . | tee bench.txt
go build -o /tmp/footprint ./cmd/footprint                 # memory and disk, one engine per process
/tmp/footprint -engine=baseline -header
for e in $(/tmp/footprint -list); do /tmp/footprint -engine="$e"; done
```

`TestFullEquivalence` is the test that licenses every number the harness
produces: it checks all 100,000 keys through `Get` byte for byte and checks that
a scan visits every key exactly once with those same bytes. It is what CI runs.
A performance number taken from a tree where it does not pass is not a
measurement.

Report medians of `-count=6`, never a single run, and state the machine, the Go
version, and the corpus variant. Every measured figure belongs in
[bench/competitive/RESULTS.md](bench/competitive/RESULTS.md) and nowhere else;
prose refers to it rather than quoting it, because both sides of this comparison
change and a number pasted into a paragraph goes stale silently.

Rules that exist because the harness previously broke each of them and published
a wrong comparative number:

- **Disk is reported as apparent size and allocated blocks, and every
  comparison uses allocated.** Several engines create sparse or preallocated
  files; summing `st_size` overstated one engine's footprint by 9.7x.
- **Every disk figure is published for both corpus variants.** The shipped
  corpus is ~92% redundant and only `store/durable`'s bulk writer exploits that.
  A single-corpus disk column credits vibejson for the corpus.
- **`store/durable`'s bulk and `Put`-loop footprints are separate rows.** They
  are different artifacts; only the bulk path emits the compact representation.
- **A scan column is labelled "iteration only" unless it reads every byte**, and
  stands beside an all-bytes column.
- **`store` is in-memory and is not a competitor.** Its numbers get their own
  table, captioned as an upper bound on what removing durability buys.

Durability must be matched across every row of a write comparison — on darwin
that means checking whether an engine reaches `F_FULLFSYNC` or only
`fsync`/`msync`, which differ by three orders of magnitude. Any non-default
setting applied to any engine, vibejson's included, must be recorded in that
engine's `Tuning()` string with its reason, and any setting that changes a call
shape must be revertible through `Config.Untuned` so `BenchmarkTuning` can
measure what the correction was worth. A tuning claim that is not a benchmark
row is not evidence. See
[bench/competitive/README.md](bench/competitive/README.md) for the full caveats.

## Documentation

Update one canonical document:

- [README.md](README.md) for the product surface;
- [docs/store.md](docs/store.md) for storage and durability behavior;
- [docs/format.md](docs/format.md) for the durable on-disk byte format;
- [CONTRIBUTING.md](CONTRIBUTING.md) for build, compiler, test, and benchmark
  policy;
- [docs/provenance.md](docs/provenance.md) or [UNSAFE.md](UNSAFE.md) for their
  machine-checked inventories.

Do not add historical implementation journals or a second roadmap beside the
current contract. Describe implemented behavior, measured conditions, and known
limits. Do not publish product comparisons, unmeasured superlatives, or a
partial memory counter as total database size.

Comparative figures have exactly one home:
[bench/competitive/RESULTS.md](bench/competitive/RESULTS.md), where every number
carries its machine, Go version, corpus variant, repetition count, and run mode.
Nothing in the documents above may quote one; they link. This is not
bureaucracy — the documents above outlive any particular measurement, and a
figure pasted into a paragraph keeps asserting itself after the code underneath
it has changed. The same rule applies to `bench/competitive/README.md`, which
describes the harness and states no results.
