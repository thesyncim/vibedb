# Competitive benchmarks

A cross-engine harness that measures vibejson's `store` (in-memory) and
`store/durable` (page-file) against four pure-Go embedded engines on one shared
JSON corpus:

| Engine | Kind |
| --- | --- |
| [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) | B+tree key/value |
| [`github.com/dgraph-io/badger/v4`](https://github.com/dgraph-io/badger) | LSM key/value |
| [`github.com/cockroachdb/pebble`](https://github.com/cockroachdb/pebble) | LSM key/value (CockroachDB's engine) |
| [`modernc.org/sqlite`](https://modernc.org/sqlite) | SQLite, pure-Go translation, with JSON1 |

**Every measured figure is in [RESULTS.md](RESULTS.md).** This file describes
what is measured and how to read it, and deliberately contains no numbers: the
engines on both sides of this comparison are under active change, and a number
in a paragraph goes stale without the paragraph noticing.

## Why this is a separate module

The root vibejson module depends on **`golang.org/x/sys` and nothing else**, and
that is a property the project maintains deliberately. Every dependency in the
table above lives in `bench/competitive/go.mod`, which `replace`s
`github.com/thesyncim/vibejson` with `../..`. Nothing here can reach the root
`go.mod`; the `competitive` CI job fails if it ever does. Building or testing the
root module never downloads any of it.

## Running

```sh
cd bench/competitive

# Correctness first, and it is not a formality. TestFullEquivalence checks
# every one of the 100,000 keys through Get, byte for byte, and checks that a
# scan visits every key exactly once with those same bytes. Every performance
# number in RESULTS.md is licensed by this test and by nothing else.
go test -run TestFullEquivalence -v .

# The measurement runs. See RESULTS.md "Run modes" for why the read
# benchmarks are one process per engine.
go test -run '^$' -bench='BenchmarkBulkLoad$|BenchmarkPointWrite|BenchmarkBulkLoadVariants|BenchmarkTuning' \
  -count=6 -timeout=180m . | tee bench.txt

# Mixed throughput in the Go benchmark harness.
go test -run '^$' -bench='BenchmarkMixedWorkload|BenchmarkDeleteRestore' \
  -count=6 -timeout=180m .

# One engine per process: per-operation p50/p95/p99, total throughput,
# retained Go memory, peak process RSS, apparent disk bytes, and allocated
# disk blocks.
go build -o /tmp/mixedbench ./cmd/mixed
/tmp/mixedbench -header -engine=vibejson-durable -workload=churn \
  -durability=async-stable-in-flight -checkpoint-mutations=1024

# Publishable cross-engine sampling: sequential isolated processes in a
# deterministic shuffled Latin-square order. Defaults are YCSB-A,
# buffered-visible, checkpoint every 64 mutations, one discarded conditioning
# pass, and 10 recorded repetitions.
go build -o /tmp/mixedsuite ./cmd/mixedsuite
/tmp/mixedsuite -mixed-bin=/tmp/mixedbench -output=mixed-ycsb-a.tsv

# Sustained fixed-live-set disk churn, one engine per process.
go build -o /tmp/churndisk ./cmd/churndisk
/tmp/churndisk -engine=vibejson-durable -cardinality=low
```

The default resolves to `volatile` for the heap and `buffered-visible` for
every file-backed engine, including vibejson durable. The mandatory
`durability` output column still prints that resolution, and explicit
`async-stable-in-flight`, `ordinary-sync`, and `power-safe` runs remain
separate contracts. `-checkpoint-mutations=N` counts successful state changes
(delete+restore counts as two), includes periodic and final checkpoint time in
total throughput, and emits a separate checkpoint latency row. Zero means one
checkpoint after the measured mutations. `forced-cp` reports persistence
boundaries triggered internally by bounded staging pressure; a publishable
same-cadence row must report zero.

`-count=6` and medians are not optional. Several of these engines have
multi-millisecond tail operations (an fsync, an LSM flush, a B+tree remap) and
a single run of a bulk load is not a measurement.

### Reproducible mixed suites

`cmd/mixedsuite` is the cross-engine runner. It never runs competitors
concurrently: every `cmd/mixed` child exits before the next one starts, so
process-global RSS and runtime memory stay isolated. It deterministically
shuffles one base engine order from `-seed`, then uses cyclic Latin-square
rotations. Every complete block of five repetitions puts every default engine
in every position once. The default 10 repetitions are therefore two complete
blocks; changing the engine list changes the block size.

The conditioning pass runs every engine once and discards those measurements.
Recorded output is TSV with three record types:

- `meta` captures the Git commit and dirty-state fingerprint, mixed-binary
  SHA-256 and Go build information, suite toolchain, host/CPU/platform, temp
  filesystem, resolved workload settings, seed, and every engine order.
- `raw` preserves every operation row from every child with its repetition,
  position, and requested engine.
- `summary` reports the sample count, median, median absolute deviation,
  linearly interpolated Q1/Q3 and IQR, minimum, and maximum for every numeric
  column. Min/max remain visible deliberately; a median must not hide a
  machine-state outlier.

The output path is created exclusively, so an old result cannot be overwritten
accidentally. Use a separate file for every workload and durability lane:

```sh
for workload in ycsb-b ycsb-a ycsb-f churn scan; do
  /tmp/mixedsuite -mixed-bin=/tmp/mixedbench \
    -workload="$workload" -output="mixed-${workload}.tsv"
done
```

### Sustained-churn live disk

`cmd/churndisk` keeps the corpus live-set fixed while uniformly selected keys
are replaced or deleted and immediately reinserted, sampling both apparent
bytes and allocated blocks between ordinary buffered-visible checkpoints. Its
post-run maintenance floor invokes Pebble manual `Compact`; Badger `Flatten`
followed by value-log garbage collection at the recommended discard ratio
until no rewrite remains; SQLite a truncated WAL checkpoint, `VACUUM` on its
single adapter connection, and another truncated checkpoint; no compaction for
bbolt because it exposes none; and vibejson `Flush` through the ordinary
mutation representation. A vibejson row with a forced checkpoint is marked
non-publishable.

A run with `forced-cp` above zero did not maintain the requested checkpoint
cadence and is not a publishable same-cadence comparison. The runner rejects
that result by default. It likewise requires at least nine recorded
repetitions. `-allow-diagnostic` retains shorter or forced-checkpoint output,
marks `publishable-suite=false`, and must not be used for a release table.

A smaller corpus makes iteration quick:

```sh
go test -run '^$' -bench=BenchmarkFilter -corpus=10000 .
```

### Corpus variants

```sh
go test -run '^$' -bench=. -cardinality=high .    # the control corpus
```

The shipped corpus is highly redundant: `note` is drawn from four fixed
sentences, `tier` from four values, `region` from five, `tags` from a pool of
eight. `gzip -9` compresses it to under a tenth of its size. `store/durable`
can exploit that only in the explicitly selected compact bulk row; its default
bulk and Put paths are verbatim. A disk table must therefore include both
cardinality variants and both vibejson representations.

`-cardinality=high` generates the control: document for document the same shape
and the same byte length, with every repeated string field replaced by a
per-document random value of that field's exact length. `country` is left drawn
from the same hundred-value alphabet in both, because the filter workloads
depend on its 1% selectivity. `TestCorpusVariantsAreShapeMatched` asserts the
two are length- and selectivity-identical, so any difference between them is
attributable to value entropy and to nothing else.

Neither variant is "the realistic one". Real data sits between them, and the
pair brackets the range a dictionary-based writer can occupy. Both are always
published side by side.

### Memory and disk footprint

Go-heap residency and process RSS are process-global, so measuring six engines
inside one benchmark binary would report the sum of all of them. The footprint
tool loads **one** engine per process:

```sh
go build -o /tmp/footprint ./cmd/footprint
/tmp/footprint -engine=baseline -header
for c in low high; do
  for e in $(/tmp/footprint -list); do /tmp/footprint -engine="$e" -cardinality="$c"; done
  /tmp/footprint -engine=vibejson-durable -putloop -cardinality="$c"
  /tmp/footprint -engine=vibejson-durable -compact -cardinality="$c"
done

/tmp/footprint -corpus-stats -cardinality=low      # the redundancy line
/tmp/footprint -engine=badger -files              # which file is sparse, and by how much
```

## What is measured

One corpus of 100,000 documents, generated deterministically, ~230-270 bytes
each: a few scalars, one nested object, one small array, one prose field. Keys
are `doc:%08d`. The predicate field `country` is drawn uniformly from a
hundred-value alphabet, so `country = "PT"` selects ~1% of the corpus.

| Workload | What it does |
| --- | --- |
| `BenchmarkBulkLoad` | Whole corpus through each engine's batch path, with and without a secondary index |
| `BenchmarkBulkLoadVariants` | store/durable verbatim bulk, compact bulk, mutation replay, and untuned replay at three corpus sizes |
| `BenchmarkPointRead` | One document by key |
| `BenchmarkPointWrite` | Replace one existing document with a growing value |
| `BenchmarkPointWriteSameSize` | Replace bytes without changing document length or indexed value |
| `BenchmarkDeleteRestore` | Random delete plus exact reinsertion; reports cost per storage mutation |
| `BenchmarkMixedWorkload` | Deterministic Zipfian YCSB A/B/F, delete churn, indexed churn, and ordered scan-under-write mixes |
| `BenchmarkPointWriteDurableDefaults` | store/durable tuned vs. its own defaults |
| `BenchmarkScan` | Every document once, **iteration only** — see below |
| `BenchmarkScanAllBytes` | Every document once with every byte of every value read |
| `BenchmarkFilter` | ~1% predicate with **no** secondary index |
| `BenchmarkIndexedFilter` | Same predicate with the engine's index |
| `BenchmarkTuning` | Every call-shape tuning applied to a competitor, against that competitor's default |
| `BenchmarkParse` | The JSON extraction alone, no storage underneath |

`cmd/mixed` measures the same mixes one engine per process and emits one row
per operation kind with p50/p95/p99 latency. YCSB A/B/F preserve the standard
50/50 read/update, 95/5 read/update, and 50/50 read/read-modify-write ratios.
Keys come from Go's finite Zipf generator with `s=1.01`, and hot ranks are
deterministically scattered over the keyspace. That is a close analogue, not a
byte-for-byte port of YCSB's scrambled generator with `theta=0.99`. Operation
types are deterministically shuffled within each exact 1,000-choice cycle;
they are not run as a read phase followed by a write phase. The corpus remains
this harness's exact JSON corpus rather than YCSB's synthetic field records.
`churn` adds 5% delete+restore cycles; `scan` adds one ordered, all-bytes scan
per 1,000 foreground choices. A delete+restore or read-modify-write is one user
choice and two engine calls, and the benchmark reports both counts.

The memory columns have deliberately different meanings. `heap-MiB` and
`runtime-MiB` are retained readings after the corpus and latency traces are
released and the Go runtime returns unused spans. They cannot see mmap or
engine-managed off-heap storage. `peak-rss-MiB` sees those allocations, but is
the process high-water mark and therefore includes transient bulk-load memory.
It is not a steady-state RSS reading.

Every mixed row carries its corpus cardinality, document count, measured
operation count, warmup count, resolved durability mode, and checkpoint
interval. The load and warmup are checkpointed before timing; measured
periodic and final checkpoint stalls are included in total throughput and
reported separately.
`vibejson-durable/bulk-verbatim`, `vibejson-durable/bulk-compact`, and
`vibejson-durable/put` remain distinct engine names. The first and third both
use verbatim document pages but follow different construction paths; compact is
a separate representation with a material read-speed tradeoff. Never combine
their disk figures.

## How the results must be presented

These rules exist because the harness previously violated each of them and
published a wrong comparative number as a result.

1. **One table for the durable engines only.** `store` is in-memory and is not
   a competitor to a durable store. It gets its own table, captioned as an upper
   bound on what removing durability buys, and its numbers are never placed in
   the same column as a durable engine's.
2. **Disk is reported apparent *and* allocated, and comparisons use allocated.**
   Never one number.
3. **A corpus-cardinality line sits next to every disk column**, with the
   high-cardinality figures alongside the shipped-corpus ones.
4. **The scan column is labelled "iteration only"** and stands beside the
   all-bytes column. Never alone.
5. **`store/durable`'s verbatim bulk, compact bulk, and `Put`-loop disk figures
   are separate rows.** Presenting one representation or construction path as
   the engine's sole footprint is not an honest comparison.

## Caveats — read these before quoting a number

**Correctness is checked exhaustively, and it did not use to be.** The previous
guard verified one document — `docs[42]` — and validated a scan by its returned
count. An engine that returned correct bytes for `docs[42]` and garbage for the
other 99,999, or the right number of wrong documents, passed it, and its
benchmark numbers would have been published. `TestFullEquivalence` checks every
key through `Get` byte for byte, and checks that a scan visits every key exactly
once with those same bytes. All six engines pass. Do not quote a number from a
tree where it does not.

**`BenchmarkScan` is not a throughput measurement and must not be presented as
one.** It touches `value[0]` of each document. On 248-byte documents it reported
a per-document time that works out to well above this machine's memory
bandwidth, which is only possible because the bytes were never read. It measures
iteration and lookup, which is a real thing to measure — an engine that walks
its own index quickly is doing something useful — but it is not "how fast can
this engine hand me the corpus". `BenchmarkScanAllBytes` reads every byte of
every value through one shared fold, so the per-byte cost is identical across
the table and the differences between rows are storage differences. Publish the
two columns together.

**Durability is reported, not assumed from a shared flag.** `-durability`
selects one named contract: `buffered-visible`,
`async-stable-in-flight`, `ordinary-sync`, or `power-safe`. Unsupported
engine/mode pairs fail instead of silently changing strength. The historical
default remains available, but resolves to a concrete mode before the engine
opens and that resolved value is printed in every output row.

Vibejson `DurabilityBufferedVisible` is the peer buffered lane: it publishes
bounded fresh-COW pages to readers and ordinary admission does not wake the
device worker. Bounded staging pressure may force an earlier checkpoint;
comparative runs must expose that event or verify their interval stays below
the pressure bound. `Checkpoint` groups the captured generation cut under one
alternate root.
Crash before that boundary recovers the prior root; crash after it recovers the
checkpointed root. The adapter explicitly selects
`CheckpointFilesystem`, a two-phase ordinary `fsync` checkpoint; the store's
zero-value checkpoint strength remains power-safe. Vibejson
`DurabilityAsyncVisible` is the distinct
`async-stable-in-flight`: it may acknowledge a generation in a private queue,
but its bounded worker continuously writes that queue through ordered stable
barriers. Pebble `NoSync`, bbolt `NoSync`, Badger `SyncWrites=false`, and
SQLite `synchronous=OFF` are `buffered-visible`: they make writes visible
without a comparable per-generation stable barrier. These contracts never
share a leaderboard row. `Engine.Durability()` reports the exact platform
guarantee. On Darwin, only vibejson `DurabilitySync` and SQLite with
`fullfsync=1` form the native `power-safe` pair.

The fair replacement has three distinct lanes:

1. Buffered-visible writes with the same periodic checkpoint schedule, with
   checkpoint stalls included in total throughput.
2. Per-mutation ordinary filesystem synchronization (`fsync`, `fdatasync`, or
   `msync`), compared only where the resulting guarantees match.
3. Per-mutation power-loss-safe persistence; on Darwin the current native
   comparable pair is vibejson `DurabilitySync` and SQLite
   `synchronous=FULL,fullfsync=1`.

The first lane now uses the real vibejson buffered/checkpoint mode.
`DurabilityAsyncVisible` remains separate because it deliberately keeps the
strong background commit pipeline running.

**The default mixed command is a short, single-client, warm burst.** The
published 10,000-document corpus is much smaller than the configured 64 MiB
caches and Pebble memtable, and 20,000 measured operations do not force a
sustained LSM flush/compaction/GC cycle. One goroutine issues one blocking
operation at a time. The framework now checkpoints the loaded baseline and
warmup before timing and includes requested periodic plus final checkpoint
stalls in throughput, but the short corpus still cannot establish sustained
engine capacity. A defensible leaderboard also needs larger-than-cache
steady-state and 1/8/64 writer lanes.

**Existing-key mutation work is matched explicitly.** The harness owns the
database and guarantees that the primary mixed lane targets an existing key.
Every adapter is charged an existence resolution before replace/delete. This
matters because vibejson resolves the row as part of document materialization
and SQLite checks the affected-row count, while an LSM `Set/Delete` can
otherwise append blindly. This lane is not a claim that every adapter exposes
an atomic conditional-replace API under outside concurrent writers. Blind
upsert/delete is a useful separate lane, but its numbers must not be mixed with
the existing-key lane.

Warmup and measurement consume consecutive portions of the deterministic key
trace. Measurement does not restart at trace position zero and replay the
exact warmup key sequence.

On darwin, "fsync" is not one thing, and this nearly wrecked the comparison:

- Plain `fsync` asks the device to accept the data but does not drain its
  volatile write cache. bbolt and Pebble use that weaker boundary on Darwin.
  vibejson `DurabilitySync` explicitly issues `F_FULLFSYNC`.
- `modernc.org/sqlite` is a translation of SQLite's C source, so its VFS calls
  plain `fsync()`, which on macOS returns once the data reaches the drive
  cache. `PRAGMA synchronous=FULL` alone therefore measured an order of
  magnitude faster than the same store with `PRAGMA fullfsync=1` — an apparent
  win that was really a missing flush. The harness sets `fullfsync=1`.
- **Badger cannot join the crash-safe pair.** Its log files are mmapped and its sync is
  `unix.Msync(MS_SYNC)`, which does not force the drive cache, and it exposes
  no option to request `F_FULLFSYNC`. Its `ordinary-sync` number buys weaker
  durability and must not be read as a like-for-like win.

None of this applies on Linux, where `fsync()` is the real thing.

**Every engine was tuned, the tuning is stated, and the corrections are
measured.** Each engine's `Engine.Tuning()` string records what was changed from
its defaults and why. `Config.Untuned` reverts the call-shape choices and
`BenchmarkTuning` reports every tuned/defaults pair, so no tuning claim has to
be taken on trust. The headline choices:

- All engines get a **64 MiB read-cache budget** — the default
  `store/durable` `ResidentBytes`. Pebble's default is 8 MiB; Badger's budget
  is moved wholesale to its index cache, which is Badger's own guidance with
  compression off.
- **Compression off everywhere.** Badger and Pebble compress with Snappy by
  default; bbolt, SQLite, and both vibejson modes never compress. Leaving it on
  would make the bytes-on-disk column incomparable and charge two engines CPU
  the others do not pay.
- **Badger's value log** is 64 MiB, not the 1 GiB default, which would mmap a
  gigabyte of address space for a 25 MiB corpus.
- **Badger's `PrefetchValues` is off on every scan.** Its default is on, and on
  this corpus the default was the single largest competitor-side distortion in
  the harness — a multiple, not a percentage. The prefetch worker pool pays for
  large value-log-resident values and these are ~250 bytes inline in the LSM
  tables.
- **bbolt and Badger point reads hold one read transaction** instead of opening
  and discarding one per `Get`. `store/durable`'s `AppendRaw` takes no
  transaction at all, so charging them per-`Get` transaction setup measured a
  difference the APIs do not have. Both write paths drop the transaction before
  writing, which is correctness and not hygiene: a held bbolt transaction pins
  pages the writer cannot reuse, and a held Badger transaction pins a read
  timestamp, so a read through either after a write would return the pre-write
  value.
- **SQLite scans through `sql.RawBytes`**, not `[]byte`, which avoids a
  `database/sql` copy per row that no other engine here was charged.
- **store/durable gets `BufferCount=1024`.** Its default normalises to ~128, at
  which a serial writer blocks on the commit-buffer pool and pays a durability
  fence per `Put`. It costs about 30 MiB of off-heap buffers.
  `BenchmarkPointWriteDurableDefaults` measures what that buys. **It currently
  buys far less than the 25-35x this tuning was justified by**, and until that
  is explained the 30 MiB is unexplained spend rather than an established
  trade — see RESULTS.md section 4. This is the only vibejson-side call-shape
  tuning in the harness, and it is not the largest tuning effect in it; the
  correction owed to Badger is larger.
- **bbolt could not be given a cache budget at all.** It mmaps the file and
  the OS page cache is its cache. It is therefore allowed more resident memory
  than everyone else, and its `HeapAlloc` reads near zero because its working
  set is not on the Go heap. Both facts flatter it.

Two tuning hypotheses were **checked and rejected**, and are recorded in the
relevant `Tuning()` strings so nobody re-litigates them as obvious wins:

- **A Pebble bloom filter does not help here.** Every key this harness probes
  exists, and a bloom filter can only save the read of a table that does not
  contain the key. Adding one measured slightly slower.
- **SQLite `mmap_size` does not help here.** `modernc.org/sqlite`'s VFS does not
  implement the memory-mapped read path the pragma exists to enable.

SQLite is competently configured, and `EXPLAIN QUERY PLAN` was checked: the
indexed query reaches `SEARCH docs USING INDEX idx_country` and the unindexed
one is a true `SCAN docs`. Neither row is accidentally measuring the wrong plan.

**Index build cost is charged to every engine that has an index.**
`BenchmarkBulkLoad` runs with `indexed=false` and `indexed=true`. It used to run
only unindexed, which meant the cost of building the index whose benefit
`BenchmarkIndexedFilter` reports was charged to nobody. Read an engine's indexed
filter against its own indexed load, never against its unindexed one.

**Bulk-load variants are measured at three corpus sizes.**
`BenchmarkBulkLoadVariants` used to run a 5,000-document build and invite
comparison with the 100,000-document `BenchmarkBulkLoad` row, on the strength of
a comment asserting that ns/doc was stable at 5,000. It is not: `store/durable`'s
bulk builder has large per-build fixed costs and its ns/doc falls by more than
3x between those two sizes. The sizes are now a measured column.

**The key/value stores are flattered on the filter workload.** They cannot see
inside a value, so counting documents by a field means scanning everything and
parsing each one. The harness hands them a precompiled RFC 6901 pointer, an
early-exit scan, and the non-revalidating trusted parser — the fastest
extraction vibejson has, and faster than anything they would get by default.
`BenchmarkParse` shows what the same predicate costs through `encoding/json`,
which is what an application would realistically reach for. Add that difference
back before concluding a key/value store is competitive at filtering.

**`HeapAlloc` is not RSS and neither is the whole story, and neither is a
database size.** bbolt's working set is an mmap; `modernc.org/sqlite` allocates
through its own page allocator outside the Go heap; Pebble's block cache is
manually managed off-heap. All three read far lower on `HeapAlloc` than they
cost. `MaxRSS` sees them but is a high-water mark that includes the load's
transient peak, and its unit differs by platform (bytes on darwin, KiB on Linux;
`Footprint.MaxRSSBytes` normalises). Neither number alone is a verdict, and a
partial memory counter is never a total database size.

**`store` is not durable and is not a competitor.** The in-memory engine has no
fsync setting because it has no file. Its numbers belong in their own table,
captioned as an upper bound on what removing durability buys — not in a column
beside a durable engine's.

**store/durable's bulk path materialises the collection first, and its open is
inside the measurement.** `Load` builds a heap collection and then calls
`durable.CreateFrom`, which is the documented bulk path and the fair counterpart
of the other engines' batch APIs. The in-memory build is inside the measurement.
So is the `durable.Open` that follows it, and that is a **stated asymmetry in
vibejson's disfavour**: every other engine opens its database in
`Factory.New`, which the bulk-load benchmark stops the timer around, while
`store/durable` cannot open a file it has not yet created. The gap is bounded
recovery, not a corpus scan, so it is small against a full bulk load — but it is
not zero and the bulk-load row is not exactly like-for-like because of it.
Verbatim bulk, explicit compact bulk, and a `Put` loop are always reported as
separate rows. `CreateFrom` does not silently select compact: the harness must
request it.

**One machine, one platform.** Conditions for the checked-in figures are stated
at the top of [RESULTS.md](RESULTS.md). The harness builds and passes
correctness on linux/amd64 too, but LSM and fsync behaviour is
filesystem-dependent and these numbers do not transfer.
