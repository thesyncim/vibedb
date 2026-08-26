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
- `cmd/lifecycle` measures clean, hot, cold, and crash-recovery opens in isolated children.
- `cmd/outofram` streams, stores, and scans a logical dataset larger than host RAM under hard memory and write bounds.
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
bytes. `durability-payload-known=true` means the adapter exposes a monotonic
counter of bytes it handed to its durability device and makes
`durability-payload/logical` meaningful. This is not filesystem metadata,
block-layer, or physical-media write accounting. Do not compare a zero from an
adapter that reports `durability-payload-known=false`.

Record `intrinsic` or `production` storage profile. The output must state the
resolved compression policy and its provenance.

Record the selected durability mode. Do not compare a volatile acknowledgement
with a power-safe acknowledgement as if they were equal.

## Reopen and recovery control

`cmd/lifecycle` creates one populated, checkpointed image outside the measured
interval. A fresh child process then times only `Factory.Open`; process startup,
the correctness scan, exact-index proof, and `Close` are excluded. The modes
have deliberately narrow meanings:

- `open` leaves OS cache state uncontrolled and must not be cited as hot or cold;
- `hot` fully scans and closes the image in a conditioning child before the
  measured child;
- `cold` performs a synchronous Linux global cache drop through
  `/proc/sys/vm/drop_caches` before the measured child; and
- `recovery` acknowledges one ordinary-sync mutation in a child that exits
  without `Close`, then verifies the recovered canonical bytes after timed open.

Cold mode fails closed unless it can perform the Linux global cache drop. It
usually requires root. Darwin has no supported whole-host drop control: advisory
per-file eviction is not treated as proof that mapped, metadata, journal, and
directory pages are cold. Run hot/open/recovery on Darwin, but do not relabel
them as cold evidence.

Every lifecycle row records durability and exact-index count. Cross-engine
claims require the same values for both; unsupported engine/index combinations
are rejected. Linux `/proc/self/io` `write_bytes` is reported as process
storage-layer write bytes, not filesystem metadata, device, or media bytes.
The command enforces explicit peak-RSS and physical-write bounds where those
measurements are available.

## Larger than RAM

`cmd/outofram` never builds `[]Doc` for the complete dataset. It streams the
same byte-native `vibejson`-validated corpus used by resident fixtures through
a bounded batch, checkpoints it, and performs a full-byte scan. A row is
emitted only if exact logical key-plus-document bytes exceed measured physical
RAM. The command fails before loading when its conservative logical lower bound
does not exceed RAM or when free disk is below twice that bound.

The loader-batch byte ceiling, process peak-RSS ceiling, and Linux process
`write_bytes` ceiling are hard errors, not annotations. Defaults cap the loader
at 8 MiB and peak RSS below 75% of physical RAM. Use one process per engine and
keep durability, exact-index count, cardinality, shape, checkpoint cadence, and
all hard bounds identical. Loading time is setup evidence, not a bulk-load
performance claim; the output labels the combined load-and-scan wall time only
so an interrupted evidence run is diagnosable.

## Results

Put publication-grade results in [RESULTS.md](RESULTS.md). Keep raw rows and
complete metadata with the summary. Do not publish a number from a dirty tree
without marking it dirty.

The current replacement documentation publishes no benchmark result.
