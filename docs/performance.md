# Performance

The current competitive snapshot was measured on 2026-07-30 from clean commit
`d714d63e1c48fc7c8e3021cf27675712d08a04fa`.

The complete source tables, competitor versions, commits, corpus definitions,
and commands live in
[bench/competitive/RESULTS.md](../bench/competitive/RESULTS.md). This page is
the short reading guide.

## Published snapshot

Throughput is total operations per second, median of ten isolated repetitions
on an Apple M4 Max. Every workload uses 10,000 documents and checkpoints after
64 acknowledged mutations.

Each result cell is **vibedb / SQLite**:

| Workload | Buffered-visible | Ordinary-sync | Power-safe |
| --- | ---: | ---: | ---: |
| YCSB-A | 182,590 / 94,134 | 10,138 / 28,790 | 394 / 382 |
| YCSB-B | 1,105,691 / 275,950 | 177,810 / 181,062 | 4,175 / 3,728 |
| YCSB-F | 171,480 / 83,182 | 9,860 / 34,737 | 436 / 392 |
| Churn | 244,114 / 118,515 | 18,016 / 51,866 | 607 / 518 |
| Scan mix | 248,194 / 107,244 | 18,380 / 62,592 | 829 / 765 |

The columns are separate durability lanes:

- **Buffered-visible:** mutations are visible immediately and become durable at
  the scheduled checkpoint. vibedb is 1.9-4.0x SQLite.
- **Ordinary-sync:** every successful mutation crosses an ordinary filesystem
  boundary. vibedb is near SQLite on read-heavy YCSB-B and trails on the other
  mixes; Badger leads this lane.
- **Power-safe:** every successful mutation crosses the strongest Darwin
  device-cache boundary. Only vibedb and SQLite make that promise in this
  harness; vibedb leads SQLite by 3-17%.

Do not compare values across lanes as if durability were unchanged. The current
single-fence synchronous path is included in this snapshot.

## Current unified-format development gates

The strict class-5 churn harness keeps 100,000 documents live through 200,000
uniform random mutations, checkpoints every 64 acknowledged mutations, samples
every 5,000 mutations, verifies the final corpus, and requires zero
pressure-forced checkpoints. These rows are current development measurements;
they are not mixed into the older clean cross-engine publication above.

Disk cells are **apparent / allocated MiB**.

| Measurement | Result |
| --- | ---: |
| Sustained churn, low cardinality | 11.12 / 11.52, flat through the final maintenance floor |
| Sustained churn, high cardinality | 24.59 / 25.52, flat through the final maintenance floor |
| Unified primary bulk, low / high cardinality | 6.50 / — · 16.27 / — |

The shape-matched low- and high-cardinality corpora are both 23.73 MiB raw.
The unified primary graph measures 68.16 and 170.56 bytes/document,
respectively, and is the only production leaf representation. Allocated-file
cells await the next complete competitive publication.
The replay-through-`Put` diagnostic still reaches the primary macro-tablet
split limit at 100,000 newly inserted keys; it is not presented as a footprint
result.

The leaf-codec CPU regression is closed by a plan-stability certificate that
patches an admitted class-5 page only when the result is byte-identical to a
full planner fold. Its leaf microbenchmark is 2.12–2.13 µs with zero
allocations, versus 240–242 µs for render/replan/encode (about 113× faster).
The bounded journal-delta checkpoint removes the common physical leaf/root
copy: on three isolated CP64 churn runs, checkpoint p50 is 76.8 µs at low
cardinality and 65.5 µs at high cardinality, with zero pressure-forced
persistence checkpoints. Median throughput is 42,667 and 40,710 ops/s.
The remaining tail is explicit rather than hidden: p95 is 7.56 and 7.93 ms
when a non-aligned overlay fold requires the physical fallback. Eliminating
that fallback tail is the next checkpoint gate.

The churn footprint includes the paired preallocated checkpoint journal, while
the bulk row above is the core store-file image. The low/high corpora remain
23.73 MiB raw.

## Unified scan development gates

These are local codec/runtime microbenchmarks, not cross-engine published
results. On the Apple M4 Max used for the 2026-07-30 unification work:

| Gate | Result |
| --- | ---: |
| Ordered class-5 scan, 100k three-scalar documents | 24.58 ns/document, 0 allocs |
| Competitive ~250 B scan, low / high cardinality | 98.60 / 101.1 ns/document, 0 allocs |
| Masked scan, 1 / 4 / 16 selected rows per leaf | 163 ns / 443–448 ns / 1.47 µs |
| Masked scan, dense 153-row leaf | 10.88–11.10 µs, within 2% of sequential |

The masked visitor decodes and renders only selected stable slots below the
measured 75% density crossover; dense masks switch to the sequential leaf
drain. The full scan fuses succinct-boundary decoding with the admitted
class-5 renderer and retains its splice buffer across passes.

## Publishing rules

A replacement table must:

1. name the exact commit, machine, OS, Go and competitor versions;
2. run each engine in an isolated process and publish repeated-sample medians;
3. verify every key and scanned byte before measurement;
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

Use the complete lane matrix in
[RESULTS.md](../bench/competitive/RESULTS.md#reproduction) for a publishable
refresh.
