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

## Space snapshot

The same published run recorded:

Disk cells are **apparent / allocated MiB**.

| Measurement | Result |
| --- | ---: |
| Sustained churn, 100k live documents | 35.1 / 35.4, flat |
| Verbatim primary bulk, low / high cardinality | 28.1 / 29.0 · 28.1 / 28.1 |
| Compact primary bulk, low / high cardinality | 7.8 / 8.0 · 17.6 / 18.0 |

The shape-matched low- and high-cardinality corpora are both 23.73 MiB raw.
The compact primary graph is the smallest measured representation in both.
The replay-through-`Put` diagnostic still reaches the primary macro-tablet
split limit at 100,000 newly inserted keys; it is not presented as a footprint
result.

## Publishing rules

A replacement table must:

1. name the exact commit, machine, OS, Go and competitor versions;
2. run each engine in an isolated process and publish repeated-sample medians;
3. verify every key and scanned byte before measurement;
4. match mutation semantics, durability boundaries, checkpoint cadence, and
   client count;
5. include requested checkpoint and maintenance stalls in elapsed time;
6. report both apparent and allocated disk bytes, with low- and
   high-cardinality corpora side by side; and
7. keep measured database results separate from microbenchmarks and
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
