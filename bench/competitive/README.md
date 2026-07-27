# Competitive benchmarks

This separate Go module compares vibedb's in-memory and durable stores with
bbolt, Badger, Pebble, and pure-Go SQLite on one deterministic JSON corpus.
Measured values live only in [RESULTS.md](RESULTS.md). The repository-wide
publication and interpretation rules live in
[docs/performance.md](../../docs/performance.md).

| Engine | Kind |
| --- | --- |
| [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) | B+tree key/value |
| [`github.com/dgraph-io/badger/v4`](https://github.com/dgraph-io/badger) | LSM key/value |
| [`github.com/cockroachdb/pebble`](https://github.com/cockroachdb/pebble) | LSM key/value |
| [`modernc.org/sqlite`](https://modernc.org/sqlite) | SQLite with JSON1 |

## Why a separate module

Competitor dependencies belong in `bench/competitive/go.mod`. The root module
must not acquire them. The nested module replaces vibedb with `../..`, so
changes in the working tree are benchmarked directly.

## Correctness first

```sh
cd bench/competitive
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' \
  -count=1 -timeout=60m .
```

`TestFullEquivalence` checks all 100,000 keys byte for byte and verifies that
each scan visits every key exactly once with the same bytes.
`TestCorpusVariantsAreShapeMatched` verifies that the low- and
high-cardinality corpora have identical document lengths and predicate
selectivity. A cross-engine number is not publishable unless both pass.

## Benchmark surfaces

| Workload | What it measures |
| --- | --- |
| `BenchmarkBulkLoad` | whole-corpus batch construction, with and without a secondary index |
| `BenchmarkBulkLoadVariants` | durable verbatim bulk, compact bulk, mutation replay, and untuned replay at three sizes |
| `BenchmarkPointRead` | one document by key |
| `BenchmarkPointWrite` | replacement with a growing value |
| `BenchmarkPointWriteSameSize` | replacement without length or indexed-value change |
| `BenchmarkDeleteRestore` | random delete plus exact reinsertion |
| `BenchmarkMixedWorkload` | deterministic Zipfian YCSB A/B/F, churn, indexed churn, and scan-under-write |
| `BenchmarkScan` | ordered iteration; touches one byte per value |
| `BenchmarkScanAllBytes` | ordered scan reading every value byte |
| `BenchmarkFilter` | approximately 1% JSON predicate without a secondary index |
| `BenchmarkIndexedFilter` | the same predicate with a native index |
| `BenchmarkTuning` | every call-shape tuning against that engine's default |
| `BenchmarkParse` | JSON extraction without storage |

The in-memory engine is an upper-bound diagnostic, not a durable competitor.
Verbatim bulk, compact bulk, and `Put` replay are distinct artifacts.

## Core runs

```sh
go test -run '^$' \
  -bench='BenchmarkBulkLoad$|BenchmarkPointWrite|BenchmarkBulkLoadVariants|BenchmarkTuning' \
  -count=6 -timeout=180m . | tee bench.txt

go test -run '^$' \
  -bench='BenchmarkMixedWorkload|BenchmarkDeleteRestore' \
  -count=6 -timeout=180m .

go test -run '^$' \
  -bench='^Benchmark(PointRead|Scan|ScanAllBytes)/(vibejson-durable|bbolt|badger|pebble|sqlite)$' \
  -benchtime=2s -count=3 -timeout=30m .
```

## Isolated mixed suites

`cmd/mixed` runs one engine per process and reports per-operation latency,
throughput, retained Go memory, peak RSS, apparent disk bytes, and allocated
blocks. `cmd/mixedsuite` executes children sequentially in deterministic
Latin-square rotations so every engine occupies each order position.

```sh
go build -o /tmp/vibedb-mixed ./cmd/mixed
go build -o /tmp/vibedb-mixedsuite ./cmd/mixedsuite

for workload in ycsb-b ycsb-a ycsb-f churn scan; do
  /tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
    -workload="$workload" -durability=buffered-visible \
    -checkpoint-mutations=64 \
    -output="mixed-${workload}-buffered.tsv"
done
```

Output contains:

- `meta`: Git commit and dirty fingerprint, binary hash/build information,
  machine, platform, filesystem, workload, seed, and engine order;
- `raw`: every child row with repetition and position;
- `summary`: sample count, median, MAD, Q1/Q3, IQR, minimum, and maximum.

The output path is exclusive. The default conditioning pass is discarded.
Recorded engines never run concurrently. A publishable suite uses complete
rotation blocks, at least the required repetition count, and no unrequested
forced checkpoint.

## Durability lanes

The resolved mode is printed in every row:

- `buffered-visible`: reader-visible volatile admission plus scheduled
  checkpoints;
- `async-stable-in-flight`: a bounded background stable-persistence pipeline;
- `ordinary-sync`: per-mutation ordinary filesystem synchronization;
- `power-safe`: the strongest platform boundary.

Unsupported engine/mode pairs fail. Periodic and final checkpoints are inside
measured throughput. `forced-cp` reports boundaries caused by staging pressure;
a same-cadence row with a nonzero value is diagnostic, not publishable. The
exact vibedb contracts are in
[docs/durability.md](../../docs/durability.md).

## Corpus variants

The low-cardinality corpus repeats several string fields. The high-cardinality
control replaces those fields with per-document random values of the exact same
length while preserving shape and filter selectivity:

```sh
go test -run '^$' -bench=. -cardinality=high .
```

Neither corpus is declared realistic. Together they expose how much a
dictionary-based representation depends on redundancy. Disk results always
show both, and explicitly compact output remains separate from verbatim output.

## Footprint

Memory is process-global, so the tool loads one engine per process:

```sh
go build -o /tmp/vibedb-footprint ./cmd/footprint
/tmp/vibedb-footprint -engine=baseline -header
for cardinality in low high; do
  for engine in $(/tmp/vibedb-footprint -list); do
    /tmp/vibedb-footprint -engine="$engine" -cardinality="$cardinality"
  done
  /tmp/vibedb-footprint -engine=vibejson-durable \
    -putloop -cardinality="$cardinality"
  /tmp/vibedb-footprint -engine=vibejson-durable \
    -compact -cardinality="$cardinality"
done
```

`heap-MiB`, runtime-resident memory, and peak RSS measure different scopes.
Disk output includes both apparent size and allocated blocks.

## Sustained churn

`cmd/churndisk` keeps a fixed live set while replacing or deleting/restoring
uniformly selected keys. It samples apparent and allocated bytes between
buffered-visible checkpoints and runs each engine's documented maintenance
floor afterward.

```sh
go build -o /tmp/vibedb-churndisk ./cmd/churndisk
/tmp/vibedb-churndisk -engine=vibejson-durable -cardinality=low
```

Use `-allow-diagnostic` only for investigation; its short or forced-checkpoint
output is explicitly non-publishable.

## Publishing a refresh

Before editing [RESULTS.md](RESULTS.md), apply every rule in
[docs/performance.md](../../docs/performance.md): matched semantics and
durability, repeated isolated samples, explicit machine/commit/lane
provenance, both scan meanings, both disk meanings, cardinality controls,
separate durable representations, and all tuning disclosed.
