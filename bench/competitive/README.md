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
- `cmd/saturation` runs a matched, isolated client sweep and applies one fixed throughput-plateau rule.
- `cmd/snapshotpressure` compares matched unpinned and explicitly pinned durable-snapshot phases.
- `cmd/sqlsurface` runs one matched SQL workload through database/sql or a real loopback pgwire client.
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

## Saturation

`cmd/saturation` conditions and then records one isolated `cmd/mixed` child per
client level. The publication shape uses seven cyclic-order repetitions at 1,
2, 4, 8, 16, 32, and 64 clients. It reports saturation only after two
consecutive median-throughput gains are at or below 500 basis points. If the
fixed sweep never meets that rule, the command preserves its canonical TSV and
returns an error. The decision is specific to the recorded host and workload;
it is not an engine-wide or environment-independent capacity number.

Every child receives the same durability, checkpoint cadence, exact-index
count, corpus, cardinality, and document shape. Run one engine per saturation
command and repeat the exact flags for another engine before making a
comparison.

## Bounded long churn and process writes

The publishable `cmd/churndisk` shape is exactly 200,000 acknowledged state
changes over a fixed 100,000-document live set, sampled every 5,000 mutations.
It verifies all final key/value bytes and rejects automatic checkpoints. Hard
limits cover peak RSS, live allocated filesystem bytes, and Linux process
`write_bytes`. Logical write bytes count every submitted key and document byte,
including both key submissions in delete-plus-restore churn, so
`physical-write/logical` has an exact denominator.

Linux `/proc/self/io` `write_bytes` is process-attributed storage-layer traffic,
not filesystem metadata, device, or media accounting. Publishable churn fails
closed when that counter is unavailable or regresses. Darwin can run an
explicitly diagnostic churn shape with `-allow-diagnostic`, but cannot produce
this process-write qualification.

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

`verify` and `repack` are VibeDB-format lifecycle modes. Verify times only
`durable.Verify` in a fresh child and requires a clean report. Repack times only
the out-of-place rewrite, verifies its output afterward, then separately times
the benchmark's primary/journal two-rename cutover and reopens the complete
corpus. That cutover is explicitly non-atomic and is not a production
publication protocol. Publication commands require Linux process-write
accounting and fail closed elsewhere.

## Cache and snapshot pressure

The overflow-heavy 10,000-document mixed corpus has exact logical
key-plus-document bytes above the common 64 MiB engine cache. This is a matched
cross-engine cache-pressure shape, but does not imply cold OS caches or a data
set larger than RAM.

`cmd/snapshotpressure` holds an actual durable snapshot lease across the pinned
phase. Its control and pinned rows share one image, durability, exact-index
count, operation count, and checkpoint cadence, and report p99.9/maximum
acknowledgement latency, allocated bytes, RSS, and process writes. An adapter
without an explicit snapshot hook is rejected rather than approximated.

## SQL interfaces

`cmd/sqlsurface` uses the same deterministic inline documents and 50/50 point
read/update trace for each surface. The database/sql lane supports VibeDB and
SQLite with each engine's strongest synchronous durability (VibeDB
`DurabilitySync`, SQLite WAL `synchronous=FULL` plus `fullfsync=1`) and one
physical exact index. Native SQL
statement spellings differ because VibeDB stores whole documents while SQLite
uses an explicit document column; output and documentation keep that boundary
visible.

The pgwire lane starts a real loopback VibeDB server and speaks PostgreSQL
startup and simple-query messages over TCP. It is matched to the VibeDB
database/sql lane for corpus, trace, durability, indexing, correctness, and
resource bounds. It measures interface overhead, not another PostgreSQL server.

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

For a complete embedded plus RF3 evidence bundle, use the repository runner on
a dedicated Linux benchmark host:

```bash
scripts/bench/run-publishable-evidence.sh \
  /absolute/path/to/new-evidence-directory OUT_OF_RAM_DOCUMENTS
```

Choose `OUT_OF_RAM_DOCUMENTS` so the exact overflow-heavy logical corpus is
larger than physical RAM; the loader verifies that inequality and fails before
publishing when it is false. The runner requires a clean tree and creates no
result row itself. It records ten isolated embedded repetitions at the strongest
common durability contract (`ordinary-sync`), plus a separate matched
`power-safe` VibeDB/SQLite lane. It also records per-engine
space/churn/above-RAM cuts, nine isolated RF3 latency/counter matrices, and nine
external RF3 fault runs. The RF3 matrices cover read, write, and mixed workloads
at 1, 8, and 32 clients. The fault artifact supplies the independently bounded
WAL and waiter-RSS evidence that the in-process latency matrix does not claim to
measure. bbolt, Badger, and Pebble are excluded from the power-safe row because
their adapters reject that guarantee instead of silently weakening it.

The runner finishes by invoking `cmd/publishcheck`. That validator rejects a
dirty or revision-mismatched bundle, fewer than nine repetitions, diagnostic
embedded rows, missing p50/p99/p99.9/maximum latency, absent apparent or
allocated space, unknown Linux process writes, invalid RF3 counter cuts,
missing network/device/logical-byte counters, and failed WAL/RSS fault bounds.
Only then does it create `VALIDATED.tsv`, containing SHA-256 digests of every
accepted raw artifact. A validation receipt proves evidence completeness; it is
not a claim that VibeDB won a comparison.

The current replacement documentation publishes no benchmark result.
