# Competitive benchmark harness

This directory is a separate Go module. It keeps competitor dependencies out
of VibeDB's root module.

## Start with correctness

```bash
cd bench/competitive
go test ./...
```

The engine adapters must pass the common correctness oracle before you publish
a comparison.

## Use the coverage matrix

`COVERAGE.md` is generated from an executable manifest. It identifies each
required measurement cell as implemented, diagnostic, or a gap. It also gives
the exact evidence commands.

Regenerate it after the manifest changes:

```bash
go generate .
go test -run '^TestBenchmarkCoverage' -count=1 ./internal/coverage
```

An implemented measurement shape is not a measured result.

## Commands

The module includes these command harnesses:

- `cmd/mixed` runs one mixed workload process.
- `cmd/mixedsuite` runs isolated child processes in a Latin-square order.
- `cmd/footprint` measures apparent disk, allocated blocks, Go heap, and RSS.
- `cmd/churndisk` samples storage during a bounded fixed-live-set mutation run.
- `cmd/speedprobe` records a focused speed diagnostic.

Run a command with `-h` to inspect its current flags. Use the exact commands in
`COVERAGE.md` for a publication shape.

## Isolation

Run one engine in each process for footprint and mixed-suite publication. RSS
and Go heap metrics are process-global. A multi-engine process would mix
retained state from different adapters.

`mixedsuite` records raw child rows and robust summaries. A publishable suite
uses at least nine recorded repetitions and no forced checkpoint cadence. The
default uses ten repetitions and one discarded conditioning pass.

## Corpus and storage profiles

Record corpus size, cardinality, and document shape. Low-cardinality and
high-cardinality variants are shape- and length-matched but have different
value entropy. The `inline`, `mixed`, and `overflow-heavy` shapes use exact
bounded lengths and byte-native `vibejson` validation.

Record `-exact-indexes=0` through `3`; the standard cells use `0`, `1`, and
`3`. VibeDB and SQLite receive the same ordered country, tier, and region
definitions. Mixed output includes p99.9 and
maximum acknowledgement latency. It also reports submitted logical mutation
bytes. `write-known=true` means the adapter exposes a native device-byte
counter and makes `device/logical` meaningful; do not compare a zero from an
adapter that reports `write-known=false`.

Record `intrinsic` or `production` storage profile. The output must state the
resolved compression policy and its provenance.

Record the selected durability mode. Do not compare a volatile acknowledgement
with a power-safe acknowledgement as if they were equal.

## Results

Put publication-grade results in [RESULTS.md](RESULTS.md). Keep raw rows and
complete metadata with the summary. Do not publish a number from a dirty tree
without marking it dirty.

The current replacement documentation publishes no benchmark result.
