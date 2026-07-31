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
| `BenchmarkBulkLoadVariants` | durable unified bulk, mutation replay, and untuned replay at three sizes |
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
Unified bulk and `Put` replay are distinct construction artifacts over the
same durable format.

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

Neither corpus is declared realistic. Together they expose how much the
unified grammar's per-leaf dictionary benefits from redundancy. Disk results
always show both; there is no separate compact/verbatim store-mode axis.

## Storage profiles

Footprint and sustained-churn disk runs have two explicit storage profiles:

| `-storage-profile` | Badger | Pebble | vibejson durable, bbolt, SQLite |
| --- | --- | --- | --- |
| `intrinsic` (default) | SST compression forced off | SST compression forced off | no optional compression switch; labelled `unsupported/no-op` |
| `production` | Snappy SST blocks | Snappy SST blocks | no optional compression switch; labelled `unsupported/no-op` |

`intrinsic` compares each engine's intrinsic representation without adding an
optional block codec.
`production` asks the separate operational question, “how much disk does the
pinned engine use with its recommended built-in compression enabled?” Badger
v4.9.5 and Pebble v1.1.5 both default to Snappy, so the harness selects that
algorithm explicitly rather than inheriting a dependency default silently.

Every footprint row and churn TSV sample prints `storage-profile`,
`compression`, and `compression-provenance`. The last field names the exact
dependency version and API setting. For Badger, enabling compression moves the
same `CacheBytes` budget from its index cache to its block cache, following
Badger's compressed-block cache guidance without granting it more memory.

The scope matters: Badger and Pebble compress SST blocks, not every byte in
their directories. Value logs, WALs, manifests, preallocation, and sparse-file
effects remain in both apparent and allocated totals. Do not mix storage rows
from the two profiles in one ranking, and do not attribute the production
profile's space saving to an intrinsic format. The throughput suites remain on
the default intrinsic profile; a production-space result does not claim that
compression CPU was included in those latency rows.

Badger can keep the 100k bulk corpus in its mutable table because the harness
retains Badger's 64 MiB memtable. `Sync` is a durability fence, not an SST
flush, and Badger exposes no ordinary public flush operation. Consequently a
pre-close bulk-footprint row may contain few or no compressible SST bytes even
though Snappy is configured; `-files` makes that physical state visible. The
churn samples and post-`Flatten` maintenance-floor row exercise rotated SSTs.
The harness does not shrink the production profile's memtable merely to make
compression look better, because that would change memory and LSM geometry in
the supposed space-only comparison.

## Footprint

Memory is process-global, so the tool loads one engine per process:

```sh
go build -o /tmp/vibedb-footprint ./cmd/footprint
/tmp/vibedb-footprint -engine=baseline -header
for profile in intrinsic production; do
  for cardinality in low high; do
    for engine in $(/tmp/vibedb-footprint -list); do
      /tmp/vibedb-footprint -engine="$engine" \
        -cardinality="$cardinality" -storage-profile="$profile"
    done
    /tmp/vibedb-footprint -engine=vibejson-durable \
      -putloop -cardinality="$cardinality" -storage-profile="$profile"
  done
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
/tmp/vibedb-churndisk -engine=badger -cardinality=low \
  -storage-profile=intrinsic
/tmp/vibedb-churndisk -engine=badger -cardinality=low \
  -storage-profile=production
```

Use `-allow-diagnostic` only for investigation; its short or forced-checkpoint
output is explicitly non-publishable.

## Publishing a refresh

Before editing [RESULTS.md](RESULTS.md), apply every rule in
[docs/performance.md](../../docs/performance.md): matched semantics and
durability, repeated isolated samples, explicit machine/commit/lane
provenance, both scan meanings, both disk meanings, cardinality controls,
one durable representation at both cardinalities, separate bulk/replay
construction results, and all tuning disclosed.
