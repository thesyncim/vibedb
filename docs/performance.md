# Performance

The current buffered-visible snapshot was measured on 2026-07-31 host-local
time from clean engine commit
`0a593f513663b66e631853d1352edf1a74b7a8a6`. Disk and footprint were
regenerated from clean benchmark-verification commit
`3fbcd8a8c79f076adce682a121131b1fbc11fe54`.

The summary tables, competitor versions, commits, corpus definitions, and
command templates live in
[bench/competitive/RESULTS.md](../bench/competitive/RESULTS.md). This page is
the short reading guide.

## Published snapshot

Throughput is total operations per second, median of ten isolated repetitions
on an Apple M4 Max. Every workload uses 10,000 documents and a CP64
acknowledged-mutation threshold.

Current buffered-visible medians:

| Workload | vibedb | Badger | SQLite |
| --- | ---: | ---: | ---: |
| YCSB-A | 278,553 | **313,682** | 114,072 |
| YCSB-B | **1,697,367** | 1,092,690 | 326,279 |
| YCSB-F | 269,938 | **289,320** | 100,899 |
| Churn | **445,005** | 401,636 | 147,831 |
| Scan mix | 276,420 | **309,899** | 131,560 |

vibedb is 2.1-5.2x SQLite, leads YCSB-B by 55% and churn by 10.8%,
and trails Badger by 6.7-11.2% on the other three aggregate mixes. Point
read p50 is 0.333-0.334 µs, update p50 is 0.916-0.958 µs, and
delete+restore p50 is 1.25 µs. Checkpoint p50 is 28.2-34.3 µs with zero
forced checkpoints; workload median p99 ranges from 38 µs to 1.03 ms.

## Current unified-format space

The strict class-5 churn harness keeps 100,000 documents live through 200,000
acknowledged state changes. Eighty percent of random choices are one-change
replacements and the rest are indivisible two-change delete+reinsert pairs.
Checkpoint and sample cadences are thresholds, so a pair may cross one by a
single change. The harness verifies every final key and byte outside the timed
mutation interval and requires zero pressure-forced checkpoints.

Disk cells are **apparent / allocated MiB**.

| Measurement | Low cardinality | High cardinality |
| --- | ---: | ---: |
| vibedb paired bulk | **7.50 / 8.02** | **17.27 / 18.02** |
| sustained churn, steady plateau | **15.48 / 16.02** | 37.00 / 37.02 |
| after offline repack | **7.50 / 8.02** | **17.27 / 18.02** |
| SQLite after VACUUM | 26.23 / 26.23 | 26.23 / 26.23 |
| Snappy Pebble after compaction (median of 3) | 58.57 / 55.25 | 78.32 / 75.12 |

The high-cardinality steady Vibe high-water is larger than SQLite before
maintenance, but it is flat rather than growing. It is reusable online
capacity; offline out-of-place repack lowers both apparent and allocated bytes
and produces the smallest maintenance floor in both cardinalities. The
benchmark's source-pair removal is not a production crash-atomic cutover.

The shape-matched corpora are both 23.73 MiB raw. Their gzip-9 sizes are
1.84 MiB and 8.04 MiB. The full vibedb pair includes its 1 MiB-capacity
recovery journal plus a 1 KiB dual header; the core class-5 graph is 68.16 and
170.56 bytes/document before that pairing overhead.

Why compactness is strong: each leaf stores repeated canonical JSON skeletons
once, dictionaries repeated value spellings, and represents each templated
row as typed tokens for the skeleton's holes. Keys and row boundaries remain
in the succinct ordered envelope. A shape templates only when doing so saves
bytes; otherwise it remains a trivial row. This is one adaptive representation,
not a compact/verbatim store-mode choice and not a generic gzip claim.

Optional compression is reported separately. With pinned Snappy defaults,
Pebble's bulk image is 34.0/41.0 MiB low/high and its post-compaction churn
median is 58.57/78.32 MiB apparent; vibedb remains 7.50/17.27 MiB without
adding a block codec.

## Current CPU and scan gates

The leaf-codec CPU regression is closed by a plan-stability certificate that
patches an admitted class-5 page only when the result is byte-identical to a
full planner fold. Its leaf microbenchmark is 2.12–2.13 µs with zero
allocations, versus 240–242 µs for render/replan/encode (about 113× faster).
The common checkpoint appends one bounded journal batch and ordinary sync over
the unchanged physical root. Journal recycle now preserves the requested
checkpoint strength: ordinary filesystem mode no longer pays an accidental
Darwin `F_FULLFSYNC`, while power-safe mode still does.

Local scan microbenchmarks on the Apple M4 Max:

| Gate | Result |
| --- | ---: |
| Ordered class-5 scan, 100k three-scalar documents | 24.58 ns/document, 0 allocs |
| Competitive ~250 B scan, low / high cardinality | 88.07 / 89.48 ns/document, 0 allocs |
| Masked scan, 1 / 4 / 16 selected rows per leaf | 163 ns / 443–448 ns / 1.47 µs |
| Masked scan, dense 153-row leaf | 10.88–11.10 µs, within 2% of sequential |

The masked visitor decodes and renders only selected stable slots below the
measured 75% density crossover; dense masks switch to the sequential leaf
drain. The full scan fuses succinct-boundary decoding with the admitted
class-5 renderer and retains its splice buffer across passes.

## Publishing rules

A replacement table must:

1. name the exact commit, machine, OS, Go and competitor versions;
2. run timing/throughput engines in isolated processes and publish
   repeated-sample medians; label disk/footprint sample counts and any medians
   explicitly;
3. validate corpus equivalence and final state outside timed intervals, and
   make timed scans consume every returned byte;
4. match mutation semantics, durability boundaries, checkpoint cadence, and
   client count;
5. include requested checkpoint and maintenance stalls in elapsed time;
6. report both apparent and allocated disk bytes, with low- and
   high-cardinality corpora side by side;
7. name the storage profile and effective compression configuration for every
   disk row, and never rank intrinsic/uncompressed rows together with
   production-compressed rows; and
8. keep measured database results separate from microbenchmarks and
   projections.

The detailed rationale is in the
[competitive harness guide](../bench/competitive/README.md).

## Reproduction

From `bench/competitive`:

```sh
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' \
  -count=1 -timeout=60m .

go build -o /tmp/vibedb-mixed ./cmd/mixed
go build -o /tmp/vibedb-mixedsuite ./cmd/mixedsuite

/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -workload=ycsb-a -durability=buffered-visible \
  -checkpoint-mutations=64 -repetitions=10 -output=mixed.tsv
```

Use the complete command matrix in
[RESULTS.md](../bench/competitive/RESULTS.md#reproduction) for a publishable
refresh.
