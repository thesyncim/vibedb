# Allocation regression gate

`bench/gate` runs a curated set of benchmarks on two revisions and fails when a
hot path allocates more than it did before. It gates on **allocs/op** and
**B/op** only, never on **ns/op**: allocation and byte counts are reproducible
across machine load and CPU, so a difference is a real change in the code path;
wall-clock time is not, so it cannot separate a regression from a busy runner.

## What it checks

For every curated benchmark the gate compares the working (head) revision to a
base revision and fails when, on the same machine and in the same run:

- a **gated allocs/op** count is higher on head than on base — any increase at
  all, because an integer allocation count that was stable and then rose is a
  concrete new allocation; or
- a **gated B/op** count is more than **5%** higher on head than on base.

The gate prints a per-benchmark table on every run, pass or fail, so a green run
is a record of exactly what was measured. Exit code `0` means every gated metric
held, `1` means a gated metric regressed, `2` means the gate could not run
(unresolvable base, a curated benchmark that failed to execute, or output
without `-benchmem` metrics). It never fails open.

## Why A/B instead of a checked-in baseline

There is no baseline file. Allocation counts legitimately differ across `GOOS`:
for example the durable write path hole-punches on darwin and takes a different
fallback on linux, and those paths allocate differently. A baseline captured on
one platform would misjudge another. Instead the gate measures the base revision
and the working revision itself, back to back on one machine, and compares those
two numbers. The base revision is checked out into a throwaway `git worktree`
(detached on the base commit), so the gate never stashes, resets, or otherwise
disturbs uncommitted work in the primary tree.

## Curated benchmarks

Each entry was verified to exist, and — where its allocs/op is gated — to
produce an identical count across repeated runs at the listed `-benchtime`.
Setup is amortized outside the measured loop in every case, but a fixed
per-benchmark iteration count still had to be chosen so that residual one-time
allocations do not perturb the integer average.

| Benchmark | Package | `-benchtime` | allocs/op | B/op |
| --- | --- | --- | --- | --- |
| `BenchmarkFileStoreCreateFromFloor` | `store/durable` | `5x` | reported, not gated | gated (5%) |
| `BenchmarkUnifiedScanAllBytesLowCardinality` | `store/durable` | `5x` | gated | gated (5%) |
| `BenchmarkUnifiedScanAllBytesHighCardinality` | `store/durable` | `5x` | gated | gated (5%) |
| `BenchmarkCompactPrimaryStripePointRead` | `internal/storeio` | `20x` | gated | gated (5%) |
| `BenchmarkStoreQueryIndexedProjection` | `query` | `20x` | gated | gated (5%) |
| `BenchmarkStoreQueryIndexedCount` | `query` | `20x` | gated | gated (5%) |
| `BenchmarkApplyKernelLeftExactMemoizationWarm` | `query` | `20x` | gated | gated (5%) |

Every gated benchmark except `BenchmarkFileStoreCreateFromFloor` reports exactly
`0` allocs/op and `0` B/op on the current tree; those are the guards that turn
any new heap allocation on a warm read, scan, or apply path into a hard failure.

Benchmarks sharing a package and `-benchtime` run in one `go test` invocation so
the expensive per-package fixtures build once.

### Measured exclusions

Two benchmarks named as candidates are deliberately not gated on allocs/op. Both
exclusions are measured, not assumed.

- **`BenchmarkFileStoreCreateFromFloor` allocs/op is reported but not gated.**
  Its bulk-build allocs/op is fractional: it measured `124` at `-benchtime=20x`
  but `123`–`124` at `-benchtime=50x`, i.e. a small fixed setup overhead
  amortizes as the iteration count rises and the integer average straddles a
  boundary. A straddling metric would produce phantom failures with no code
  change, so the gate reports the value for context and gates the benchmark on
  B/op only. Across repeated samples its B/op was observed between roughly
  `1,677,800` and `1,679,400` bytes — a spread near `0.1%`, run-to-run jitter
  rather than a trend, and well inside the 5% tolerance. B/op still catches a
  gross regression in the sequential write floor.
- **`BenchmarkCompactPrimaryStripeCountryScan` is not in the set.** On the
  current tree it does not complete: `CountDictionaryHoleEqual` returns
  `ok=false` for the `/country` column of the shared corpus, so the benchmark
  calls `b.Fatal("compact dictionary scan")` on the first iteration and produces
  no measurement. The corpus is deterministic and the compact encoder is pure
  Go, so this reproduces on every platform. A gate cannot compare a number it
  never obtains; its sibling `BenchmarkCompactPrimaryStripePointRead` is gated in
  its place. Never running benchmarks is exactly how they rot, which is the gap
  this gate closes — fixing the country-scan benchmark is a change for the code
  that owns it.

## Run it locally

One command, from the module root:

```sh
go run ./bench/gate
```

With no arguments the base is the merge-base of `HEAD` and `main` — the fork
point the working tree diverged from — so the comparison isolates your changes
from unrelated commits that have since landed on `main`. Name a different base
explicitly when you need one:

```sh
go run ./bench/gate -base <ref>     # any git revision: a branch, tag, or SHA
GATE_BASE=<ref> go run ./bench/gate # same, via the environment CI uses
```

Add `-keep` to leave the base worktree in place for inspection instead of
removing it. A full run stays well under ten minutes because the base worktree
shares the module and build caches; when base and head compile the same package
sources, the base side is a cache hit.

## CI behavior

`.github/workflows/bench-gate.yml` runs the gate on every pull request on
`ubuntu-latest`, with the Go toolchain taken from `go.mod`. The workflow checks
out with full history so the base commit is present, then runs
`go run ./bench/gate` with `GATE_BASE` set to the pull request's base commit.
The job fails the pull request when a gated allocation or byte count regresses
and prints the same table you see locally.
