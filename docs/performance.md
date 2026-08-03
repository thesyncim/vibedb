# Performance

The write, concurrency, space, and bulk tables were regenerated on 2026-08-01
from clean engine commit `7fe67691dd889a34951682d2522661c7741d8720`.
The scan-mix row and CPU/scan gates were refreshed the same day from clean
commit `b5702bc9ed951b7d88591e8f9a5eebe826fe1fa0` on the same Apple M4 Max.
The complete tables, exact protocol, dependency versions, caveats, and
reproduction commands are in
[bench/competitive/RESULTS.md](../bench/competitive/RESULTS.md). This page is
the short reading guide.

## What changed

The former write gap is closed in the measured buffered-visible CP64 lane.
With one client, vibedb is 2.52–2.79× Badger on YCSB-A, YCSB-B, YCSB-F, and
churn. Across 1, 8, and 32 clients it is 2.35–2.50× Badger on existing-key
replacement and 2.73–2.93× on mixed churn.

The former scan-mix deficit is also closed: the refreshed row is 1.58× Badger,
or 57.7% ahead. That aggregate result does not hide the remaining latency gap:
the individual ordered full-scan p50 is 7.0% slower than Badger, while p99 is
effectively tied. Throughput scaling also flattens after eight clients. Those
are the next two measured performance targets.

The log-filter path has a fused execution lane for `COUNT(*)` equality queries.
On the 100,000-row low-cardinality log-like probe, the current fair adapter
measured about 0.8 µs for a random point read, 0.069 ms for an unindexed
equality count, and 5.2 µs for the same count through an exact `country` index.
The unindexed result is a complete 100,000-row scan (945 matches, about
0.69 ns/document), not candidate pruning: the storage cursor resolves the
field once per durable leaf template and compares every packed scalar ID
without reconstructing each JSON document. A sequential bit reservoir carries
unused ID bits between rows; seven-bit dictionaries additionally consume eight
IDs per exact seven-byte block. Neither path consults value postings or skips
encoded IDs. Rows that cannot be decided from tokens are rendered individually
and reported through `TokenFilterFallbackRows`; `RowsScanned` still reports the
complete corpus and `IndexBounded` remains false.

The point-read path acquires the same immutable page-cache lease as before, but
now opens the compact payload from the lease's already validated header and
payload view. It no longer hashes the entire cached leaf a second time on every
lookup. Compact grammar and logical identity are still checked, and cold page
admission still verifies the complete page checksum. The public probe improved
from about 4.1 µs to 0.78 µs with zero allocations, without changing routing,
leaf size, physical order, or adding an index.

The fair adapter retains one compiled query per repeated filter value and reuses
its `Exec`, matching a prepared-query workload. The complete warmed public path
is now zero-allocation; changing the filter value rebuilds the cached query once.
The same token lane also handles
nested paths and exact-decimal numeric equality: `1`, `1.0`, and `1e0` compare
equal without float64 rounding. On the same 100,000-row storage corpus, a
nested string equality measured about 0.084 ms and a decimal `500.0` needle over
integer-spelled values measured about 0.057 ms; both scanned every row with zero
fallback rows and zero warm allocations. Integer-valued decimal needles compile
once to an exact int64 and then scan packed frame-of-reference offsets or delta
varints directly, without rendering JSON or rounding through float64. Ten-bit
FOR lanes consume four offsets per exact five-byte block. A non-integral
`500.5` needle over the same integer stream also measured about 0.057 ms and
zero matches: it consumes every packed offset against an impossible encoded
value instead of using type metadata to skip the stream. Dictionary and
front-coded decimal spellings use `JSONNumberEqual` directly over scalar
values, including exponent and signed-zero forms. The
indexed lane uses the durable spanned-term representation for large
low-cardinality postings. These are machine-specific probe numbers, not API
promises; reproduce them with `bench/competitive/cmd/speedprobe` and separate
warm-up from steady-state allocation measurements.

Codec choice no longer forces exact-spelling filters onto whole-document
rendering. Dictionary, front-coded, alphabet-packed, FOR, delta,
Gregorian-date, and single-numeric-run streams all have complete encoded-value
scan lanes. On the
shape-identical high-cardinality corpus, an absent, alphabet-representable
`/note` string of a populated value length scanned all 100,000
alphabet-packed values in about 0.48 ms (4.8 ns/document), with zero fallback
rows and zero warm allocations. The needle deliberately cannot be rejected
from alphabet metadata. The prior format took about 0.79 ms on the identical
gate. The general container-valued render
fallback measured about 67 ms on the same harness; it remains disclosed as a
different physical path rather than being used as an unindexed result.

The latest high-cardinality alphabet layout packs codes continuously within
each bounded 64-row restart block. Besides removing per-value byte padding,
its sequential decoder improved the high-cardinality all-bytes scan from about
58.8 ms to 52.2 ms (roughly 477 MB/s). Every packed character remains
independently reconstructible; this is not dictionary substitution or value
inference.

The same public probe's ordered all-bytes scan now reconstructs and consumes
24.88 MB of canonical JSON in about 39.7 ms (roughly 627 MB/s), also with zero
warm allocations. Sequential cursors carry per-shape ordinals and restart-coded
integer state across rows; point reads retain bounded restart decoding. A
constant-time proleptic-Gregorian transform renders date ordinals without a
per-value year search or month loop. Together these changes reduced the
low-cardinality all-bytes scan from about 54.7 ms without changing the compact
bytes or callback semantics.

Compact bulk planning now tries the complete 4,096-row window before binary
searching for a smaller extent. Log-like windows normally fit, so they are
encoded once rather than once per search step. Bulk reservation, exact-index
sizing, and construction also share that immutable leaf plan instead of
rediscovering its boundaries. A selected encoded payload is staged directly
when it is no larger than the row descriptors it replaces; larger payloads
retain only bounded descriptors and use the original foreground re-encode
path, so very large documents never create a second corpus-sized plan.
Already-canonical sources also borrow their values without reserving an unused
whole-corpus rewrite arena.

Compact stripe construction gathers and encodes one scalar-hole column at a
time from a reusable vector instead of retaining every column simultaneously.
Bulk snapshot traversal fills the final borrowed record plan directly instead
of allocating an intermediate chunk/slot list and then walking the source a
second time.
Oversized windows now select the actual largest fitting prefix. After the first
exact search, neighbouring leaves probe the preceding exact row count, expand
until they prove one fitting and one failing bound, and binary-search only that
bracket. A density phase change expands the bracket; it never substitutes a
heuristic boundary. On the 100,000-row high-cardinality corpus this reduced the
bulk load from about 3.59 s to 2.08 s and the leaf count from 195 to 129.
Scalar codec planning now measures dictionary cost from borrowed values and
only sorts and packs the dictionary if it wins; deterministic ordering and the
existing dictionary-first tie rule are unchanged. Avoiding discarded
dictionary work reduced the same high-cardinality bulk load again to about
1.51 s, while the low-cardinality load improved from about 0.30 s to 0.24 s.
Both complete database files remain exactly the same size.
The borrowed-value census now uses a bounded generation-stamped hash table, so
successive probes neither clear a map nor retain per-key objects; collisions
still compare complete scalar bytes. Dictionaries below 16 values stay on a
direct comparison lane. This reduced the high-cardinality load again to about
1.21 s while keeping the low-cardinality load at about 0.24 s.
Exact fit probes now share one immutable parse and canonical extraction for
their complete 4,096-row planning window. Each candidate still serializes its
exact prefix, and prepared-prefix tests compare those bytes with independent
one-shot builds. This reduced the high-cardinality load again to about 1.02 s
without changing the low-cardinality result or retained allocation.
Front coding is also sized exactly before materialization and now copies its
stream only when it wins the adaptive codec choice. The high-cardinality load
fell again to about 0.97 s and the low-cardinality load to about 0.23 s; each
build retained about 0.4 MB less allocation, with unchanged serialized bytes.
Alphabet streams likewise separate exact sizing from bit packing, performing
the per-character pack only if that candidate wins. This reduced the
high-cardinality load again to about 0.94 s while keeping low cardinality at
about 0.23 s and preserving the exact stream representation.
The 20,000-document `BenchmarkFileStoreCreateFromFloor` improved from about
289 ms to 29 ms (roughly 10×, or 14.45 µs to 1.45 µs per document); allocated
setup bytes fell from about 15.62 MB to 2.90 MB. It still selects identical leaf
boundaries and writes identical compact bytes. High-cardinality windows that do
not fit still use the exact largest-prefix search.

A local ClickHouse control over the same flattened 100,000-row corpus measured
about 1.49 ms with `ORDER BY key` and no secondary data-skipping index. The
current warmed VibeDB public count is about 21.6× faster while performing the
same full-corpus logical scan and using no filter index. For context,
ClickHouse measured about 1.34 ms with the primary layout aligned as
`ORDER BY (country, key)`, and about 1.97 ms with a mixed-value `set` skipping
index. Those indexed/layout-assisted rows are disclosed separately because
they do not represent the same physical work as the full-scan row.

### Direct ClickHouse space/speed control

The same local ClickHouse 25.8 run also exposes an important current deficit.
These are per-table apparent bytes after the single inserted part was complete;
they exclude ClickHouse's shared server installation and logs, just as the
VibeDB's row is the complete database file and excludes unrelated process
files, matching the ClickHouse per-table accounting.

| representation | bytes | bytes/document | unindexed `country = 'PT'` count |
| --- | ---: | ---: | ---: |
| ClickHouse typed, `ORDER BY key`, no skipping index | **2,713,077** | **27.13** | about **1.49 ms** |
| ClickHouse typed, `ORDER BY (country, key)` | 2,992,629 | 29.93 | about 1.34 ms |
| ClickHouse raw JSON, `ORDER BY key` | 4,565,499 | 45.65 | not measured in this control |
| VibeDB compact default, complete database file | 1,019,904 | **10.20** | about **0.069 ms**, zero allocations, through the warmed public query API |

The compact immutable VibeDB online footprint is 2.66× smaller than the fair
typed ClickHouse table. Ordinary buffered-visible bulk creation does not mint
its lazy mutation journal, so this comparison omits no required VibeDB sidecar.
Its warmed public query path is about 21.6× faster in this control while
scanning all 100,000 field IDs; it uses no index, candidate list, or data
skipping. The aligned ClickHouse row is kept separate because
`country` participates in its sparse primary index and physical order. The
raw-JSON row is a useful representation control, but its filter speed must not
be borrowed from the typed table.

The retired class-5 leaf census explained the former gap. Its 66.93 physical
leaf bytes per document split into 41.77 row-token bytes, 12.00 key bytes, 5.18 structural
bytes, 4.19 dictionary bytes, 2.07 template bytes, and only 1.71 bytes of
extent slack. Independent per-leaf DEFLATE reaches 30.67 bytes/document at its
fast setting and 27.52 at its densest setting. That is a feasibility bound,
not a stored-format result, and even the slow setting does not beat the typed
ClickHouse table. Packing or a generic codec alone is consequently not enough.

The compact stream is now the unconditional development-format primary leaf
for empty creation, bulk loading, point reads, ordered scans, exact-index
posting enumeration, mutation folds, verification, and salvage. The same
100,000-row low-cardinality corpus occupies 905,216 physical leaf bytes
(9.05 bytes/document); catalog, tablet, root, and allocator pages bring the
complete file to 10.20 bytes/document. The shape-identical high-cardinality
variant occupies 82.94 bytes/document as a complete file, preventing the low
cardinality result from hiding an irreversible or corpus-eliding encoding.
That is 7.910 MiB, below the same JSON corpus's 8.041 MiB gzip-9 output even
though the database file also retains all keys and structural metadata. Every
key and scalar codec is decoded byte-for-byte in the format tests.

The winning ingredients are generic: template-hole transposition; bit-packed
dictionaries and frame-of-reference integers; block-packed delta integers;
12-byte self-delimiting stream headers with 16-bit in-leaf dictionary ends;
validated Gregorian date packing; bounded-restart front coding; a reversible
single-numeric-run string codec; and adaptive alphabet packing for scalar
streams with at most 64 distinct bytes. Alphabet codes are continuous within
each 64-row restart block, avoiding per-value byte padding while retaining
bounded point reads. The numeric-run codec applies equally
to keys such as
`doc:00001234` and values such as `"user-1234"`. It does not infer one field
from another, omit values, add a filter index, prune rows, or derive one field
from another.

Compact storage does not introduce a background compactor. Mutations use a
per-stripe bounded delta overlay. Crossing its deterministic threshold folds
the delta into one replacement stripe on the foreground checkpoint path,
publishes the replacement through the existing atomic-root protocol, and makes
the old extents logically reusable immediately; bounded foreground hole
punching then returns physical blocks where the filesystem supports it. The
online-space gate charges the old stripe, maximum admitted delta, replacement,
and recovery bytes simultaneously, so a benchmark cannot report only the
post-fold floor while hiding asynchronous compaction debt.

## Single-client workloads

Total operations per second, median of ten isolated repetitions on an Apple
M4 Max. Every workload has 10,000 documents, one client, buffered-visible
durability, and a CP64 acknowledged-mutation threshold.

| workload | vibedb | Badger | SQLite | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: |
| YCSB-A | **770,327** | 305,605 | 111,375 | **2.52×** |
| YCSB-B | **2,704,197.5** | 1,027,076.5 | 330,809 | **2.63×** |
| YCSB-F | **732,317** | 277,999 | 99,433 | **2.63×** |
| Churn | **1,089,310** | 390,140 | 143,698 | **2.79×** |
| Scan mix | **390,929.5** | 247,961.5 | 111,617.5 | **1.58×** |

Vibedb point-read p50 is 0.125 µs in all five workloads. In the first four,
update p50 is 1.83–1.90 µs, delete+restore p50 is 2.21 µs, checkpoint p50 is
30.1–33.0 µs, and checkpoint p99 is 40.6–44.8 µs. In the refreshed scan
mix, update, delete+restore, and checkpoint p50/p99 are 2.083/32.396,
2.417/41.562, and 32.396/78.188 µs. Its full-scan p50/p99 is
1.703/1.887 ms; Badger's is 1.592/1.886 ms. The aggregate win comes from the
whole operation mix, not from claiming the lowest raw full-scan p50.

## Concurrent writes

Median total operations per second:

| workload | clients | vibedb | Badger | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: |
| 100% replacement | 1 | **408,753.5** | 173,801 | **2.35×** |
| 100% replacement | 8 | **623,980.5** | 249,857.5 | **2.50×** |
| 100% replacement | 32 | **648,988.5** | 272,191.5 | **2.38×** |
| mixed churn | 1 | **1,087,408.5** | 396,310.5 | **2.74×** |
| mixed churn | 8 | **1,621,065.5** | 594,384 | **2.73×** |
| mixed churn | 32 | **1,730,922.5** | 590,288 | **2.93×** |

The concurrent primary path uses at most 32 fixed, preallocated writer-scratch
contexts; 4,096 stripes hashed by complete bucket/leaf identity; parallel
canonicalization, routing, and leaf-local inspection; and a bounded
flat-combining publisher for generation assignment and overlay publication.
Its current qualification lane is buffered-visible, schemaless, unindexed,
and inline. Structural operations, overflow, split, and other unsupported
cases retain exclusive fencing and fall back to the general path.

That design improves useful concurrency without claiming lock-free commits.
From one to 32 clients, vibedb scales 1.59× on both workloads. It gains another
4.0% (replacement) and 6.8% (churn) from 8 to 32, rather than scaling linearly.

## Online space under churn

The churn harness keeps 100,000 documents live through 200,000 acknowledged
state changes: 80% replacements and 20% indivisible delete+reinsert pairs.
Every final key/value is verified outside the timed interval, and every Vibe
run reports zero pressure-forced checkpoints.

Cells are **apparent / allocated MiB**.

| measurement | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb online after churn | **22.075 / 16.020** | 54.841 / 36.070 |
| production Badger online after churn | 273.948 / 31.820 | 279.414 / 37.285 |
| production Pebble online after churn, median of 3 | 79.133 / 81.129 | 84.244 / 86.055 |
| SQLite online after churn | 28.109 / 28.109 | **28.109 / 28.109** |
| vibedb offline Repack floor | **9.001 / 9.520** | **18.767 / 19.520** |

The Vibe online row is the important result. Physical checkpoint completion
performs bounded, work-conserving foreground hole punching, so obsolete extents
become reusable without a background compactor, an offline pass, or an
unbounded cleanup spike. The amount of reclaim work per durable generation is
fixed; unsupported filesystems safely retain logical reuse without the
allocated-byte optimization.

Low-cardinality Vibe is the smallest online image by both measures. At high
cardinality, SQLite is still smaller, while Vibe uses 5.10× fewer apparent
bytes and 3.3% fewer allocated bytes than production Badger. Offline
out-of-place `durable.Repack` is shown as a separate lower bound, not as a
requirement for stable online space.

## Bulk footprint

Both 100,000-document corpora contain 24,881,153 JSON bytes (23.729 MiB) plus
1,200,000 key bytes (1.144 MiB), for a key-inclusive logical payload of
26,081,153 bytes (24.873 MiB). Gzip-9 is an explicitly JSON-only entropy
control: low cardinality compresses to 1.837 MiB and the shape-identical high
cardinality corpus to 8.041 MiB. Database cells are **apparent / allocated
MiB**. The unified bulk image has not been mutated, so its ordinary
buffered-visible lazy journal does not exist and the row is the complete online
footprint. Point-put builds mutate the store and therefore include their bounded
sibling journal.

| engine/profile | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk, immutable | **0.973 / 0.973** | **7.910 / 7.910** |
| vibedb point-put build | 16.341 / 16.379 | 28.606 / 29.250 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| Pebble with Snappy | 33.978 / 34.000 | 40.993 / 41.027 |
| Badger with Snappy configured | 257.000 / 26.621 | 257.000 / 26.621 |

Badger's bulk image has not yet materialized its mutable table into compressed
SSTs, so use the churn table—not bulk—for a production-compressed comparison.
Vibedb's compactness comes from structural sharing inside the canonical leaf
format: repeated JSON skeletons and scalar spellings are shared, while shapes
that do not save bytes remain verbatim.

## CPU and scan gates

Five-sample medians from clean commit `b5702bc`:

| gate | result |
| --- | ---: |
| stable native checkpoint leaf fold | **1.914 µs**, 0 allocs |
| full render/replan/encode | 256.121 µs, 0 allocs |
| ordered scan, 100k three-scalar documents | **23.49 ns/document**, 0 allocs |
| competitive full scan, low/high cardinality | **91.57 / 94.21 ns/document**, 0 allocs |
| masked scan, one occupied row per live posting tile | **178.4 ns/selected document**, 0 allocs |

The certified native fold is about 134× faster than full replanning. Historical
masked-density sweeps are not carried forward because the current named
benchmark reproduces one occupied row per live posting tile, not the former
1/4/16/dense matrix.

## Parser and internal threshold provenance

Machine-specific microbenchmarks are recorded here, not in exported Go
documentation. They are regression and tuning evidence, not portable API
promises. The following historical measurements were taken on an Apple M4 Max
with Go 1.26; rerun the named benchmarks on the intended deployment machine
before changing a threshold.

### SQL parser

A warmed `sql.Parser` refills retained arenas and measured zero allocations per
parse. The simple SELECT measured about 118 ns, while the join and grouped
aggregate shapes each measured about 730 ns, roughly 180 MB/s of statement
text. The owning package-level `sql.Parse` convenience path measured about 11
allocations for the simple SELECT.

```sh
go test ./sql -run '^$' \
  -bench 'BenchmarkParse($|/)|BenchmarkParseStatement|BenchmarkParseOneShot' \
  -benchmem -count=10
```

### Join strategy thresholds

`BenchmarkJoinThresholdSweep` measured nanoseconds per outer row for the two
heap strategies:

| inner matches | membership | membership + index | lookup |
| ---: | ---: | ---: | ---: |
| 16 | 76.2 | 4.2 | 175.2 |
| 256 | 121.0 | 23.6 | 176.7 |
| 1,024 | 153.4 | 101.2 | 183.2 |
| 2,048 | 175.3 | 134.0 | 180.8 |
| 3,072 | 188.5 | 172.6 | 184.9 |
| 4,096 | 210.9 | 208.1 | 183.4 |
| 16,384 | 381.2 | 664.5 | 191.7 |
| 65,536 | 1,196.3 | 8,090.0 | 235.4 |

That crossover is the provenance for `joinMembershipMax = 2048`. The count is
used instead of byte size because comparison work and cache footprint scale
with entries. The unindexed crossover is the conservative one; the benchmark
did not justify a second index-dependent threshold.

`BenchmarkJoinCostModel` separately measured the heap semi-join filter inputs:

| inner collection | inner row scanned | outer row probed | probe / scan |
| ---: | ---: | ---: | ---: |
| 1,000 | 44.8 | 199.8 | 4.5 |
| 10,000 | 44.7 | 229.5 | 5.1 |
| 100,000 | 47.4 | 389.2 | 8.2 |
| 400,000 | 52.4 | 546.3 | 10.4 |

The floor of the measured break-even range is the provenance for
`joinBloomScanRatio = 4`. This gate compares exact materialized counts; it is
not a cardinality estimator.

The durable filter needs a separate threshold because its filter-building scan
is serial while an ordinary durable scan is parallel. At 1% inner selectivity,
`BenchmarkDurableJoinFilterCrossover` measured:

| inner / outer | unfiltered | forced filter | outcome |
| ---: | ---: | ---: | ---: |
| 0.5 | 270.7 | 91.2 | filter 2.97× faster |
| 1.0 | 252.3 | 129.2 | filter 1.95× faster |
| 2.0 | 255.2 | 208.5 | filter 1.22× faster |
| 4.0 | 260.9 | 369.1 | filter 1.41× slower |

The crossing near 2.6 is the provenance for `joinFileBloomScanRatio = 2`.
Parallelizing the bind scan would invalidate this result and requires both a
new sweep and a new balance proof for the reused file-worker channels.

```sh
go test ./query -run '^$' \
  -bench 'BenchmarkJoinThresholdSweep|BenchmarkJoinCostModel|BenchmarkDurableJoin(CostModel|FilterCrossover)' \
  -benchmem -count=10
```

### Bulk construction and value dictionaries

On the 100,000-row, 24 MiB bulk corpus, `store.Builder` plus `Build` measured
821 allocated bytes and 0.19 allocations per row; repeated `Collection.Put`
measured about 7.8 KiB and 14 allocations per row. Builder construction peaked
at 77 MiB of live Go heap and 119 MiB of `MemStats.HeapSys`, versus 3.9 MiB of
steady-state `HeapAlloc` after publication. These values explain the stable API
guidance: use Builder for bulk load, and size the process for load high-water,
not only the final collection.

That guidance applies when the desired result is an in-memory
`store.Collection`. Durable callers that already own the input batch use
`durable.CreateFromRecords`, which bypasses this intermediate heap collection
and feeds borrowed rows directly to the canonical compact-graph planner.

For the same class of loaded collection, one 100,000-document, 24 MiB corpus
measured 3.9 MiB of Go `HeapAlloc` against 165 MiB of peak process RSS because
published pointer-free blocks live outside the Go heap. Use `store.Stats` for
off-heap collection bytes and process metrics for total residency; neither
`HeapAlloc` alone nor a post-load sample describes the high-water footprint.

The executable allocation gates are:

```sh
go test ./store -run 'TestStore(BuilderPageArenaAllocation|BuilderBulkLoadAllocation|CollectionReplaceAllocation)' -count=10
```

Value dictionaries are an at-rest lever, not a live-heap lever. On the long
repeated-enum corpus they added 36 B per document to a Segment and 64 B per
document to a chunked Collection while modeling 103 B per document of
persisted-source saving. In the length-floor sweep, lowering the floor from 16
to 8 increased modeled saving from 38.2 to 47.2 B per document but live cost
from 19.7 to 36.6 B per document. That tradeoff is the provenance for the
16-byte default; the algebraic sighting economics remain executable in
`TestValueDictSightingEconomics`.

```sh
go test ./store -run 'TestValueDict(SightingEconomics|FloorKeepsShortInline)' -count=10
```

Other implementation constants retain named benchmark coverage beside the
code. Their machine results should be published here when used to change a
default; source comments describe only the stable invariant and name the
benchmark that must be rerun.

### Migrated local optimization records

The following historical observations explain current implementation shapes.
They were moved out of permanent source/API comments so they can be refreshed
without changing package documentation:

- Reusing the durable execution pool removed a fixed 18 allocations and about
  2.3 KiB per warm execution. Building temporary per-batch postings in the
  favorable 1%-selectivity filtered-count case cost 4.2 allocations per scanned
  document and ran 1.9× slower, so pushdown uses persistent snapshot indexes and
  each admitted batch takes one columnar pass.
- Per-batch arena generations replaced a scheduler-dependent rewind condition.
  Under the race detector, the old condition retained about 1.0 MiB for a
  `LIMIT 10` over 50,000 documents when workers stayed busy, versus about 18 KiB
  when they happened to idle.
- Build-and-retry for containment tapes measured 134.5 ns for `BuildIndex` on
  the 245-byte fixture, versus 257 ns for an exact pre-count before the build.
- The parallel scan sweep on 16 processors measured the following ns/document
  for a 1% filter; it is the provenance for the 1,024-row split floor and
  256-row-per-worker policy:

  | rows | narrow w=1 | narrow w=2 | narrow w=4 | wide w=1 | wide w=2 | wide w=4 |
  | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
  | 256 | 31.4 | 39.7 | 36.6 | 60.8 | 70.2 | 46.9 |
  | 512 | 31.0 | 34.8 | 24.3 | 58.5 | 49.1 | 40.4 |
  | 1,024 | 30.9 | 27.7 | 19.4 | 57.3 | 38.6 | 31.5 |
  | 4,096 | 29.9 | 19.7 | 13.2 | 58.4 | 30.2 | 16.4 |

- Passing SipHash state by value enabled inlining and changed the 14-byte-key
  hash profile from about 84 ns to about 20 ns. The resident primary router's
  packed fence prefix addressed a separate roughly 90 ns byte-search cost at
  100,000 keys.
- Flat pooled exact-term build scratch removed about 630 KiB of transient
  allocation per 10,000-posting build; avoiding per-posting identity strings
  accounted for roughly 220 KiB of that class. Passing a just-derived chunk-0
  mask directly into leaf construction avoided duplicated codec selection that
  had been about half of checkpoint-fold encode CPU on the 10,000-shape fixture.
- Primary-leaf placement on the 100,000-mutation, 513-leaf churn fixture held
  adjacency near 25× for about 7% file-size cost. An 8 KiB stride reached about
  50×; 12 KiB reached about 5× for about 19% file-size cost. Rerun
  `BenchmarkFilePrimaryChurn` before changing the stride.
- The former free-space tree lost reclaimed extents across reopen: identical
  write volume occupied 6.3 MiB in one session and 23.9 MiB across eight. The
  self-describing segmented free log exists to make the durable and in-memory
  free sets identical after every commit and reopen.
- Raising durability `GroupLimit` from 2 to 64 did not change achieved group
  size or throughput in the measured writer shapes; it caps a group already
  formed. `CommitCoalesce`, not `GroupLimit`, controls intentional waiting.

Relevant local gates can be rerun with:

```sh
go test ./query -run '^$' \
  -bench 'BenchmarkSegmentParallel|BenchmarkNarrowRowParallel|BenchmarkJoinBloomPrefilter' \
  -benchmem -count=10
go test ./store/durable -run '^$' \
  -bench 'BenchmarkFilePrimaryChurn|BenchmarkPrimaryExactProbe|BenchmarkFileStoreCommitGrouping' \
  -benchmem -count=10
```

## Reading the numbers honestly

- Compare only equal durability lanes, checkpoint cadence, operation mix, and
  client count.
- Throughput cells are ten-sample medians from isolated child processes;
  footprint is one isolated run except where a three-run median is labeled.
- `online` includes all requested checkpoints and bounded foreground reclaim.
  `offline Repack` is always a separate row.
- Apparent and allocated bytes answer different questions and are both shown.
- Intrinsic/uncompressed storage is never ranked as if it were the same profile
  as a production-compressed LSM.
- Microbenchmarks are regression gates, not substitutes for cross-engine
  workload results.

See the [competitive harness guide](../bench/competitive/README.md) for the
full contract and [competitive results](../bench/competitive/RESULTS.md) for
reproduction commands.
