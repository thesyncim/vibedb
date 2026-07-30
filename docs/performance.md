# Performance

The latest checked-in competitive snapshot was measured on 2026-07-28. It is a
reproducible baseline, not a claim about current `main`: the primary graph,
single-fence synchronous journal, compact primary leaves, exact-index overlay,
spanned postings, and online index creation changed after the measured commits.
Refresh the suite before attributing these numbers to the current engine.

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
| YCSB-A | 195,351 / 104,263 | 17,949 / 27,871 | 378 / 431 |
| YCSB-B | 1,010,312 / 280,248 | 176,264 / 202,783 | 3,663 / 3,904 |
| YCSB-F | 179,986 / 86,000 | 18,519 / 32,560 | 370 / 402 |
| Churn | 265,137 / 127,557 | 17,860 / 49,881 | 533 / 589 |
| Scan mix | 265,362 / 112,967 | 21,176 / 62,369 | 757 / 828 |

The columns are separate durability lanes:

- **Buffered-visible:** mutations are visible immediately and become durable at
  the scheduled checkpoint. In this snapshot vibedb was 1.9–3.6× SQLite.
- **Ordinary-sync:** every successful mutation crosses an ordinary filesystem
  boundary. The measured vibedb build trailed SQLite and Badger.
- **Power-safe:** every successful mutation crosses the strongest Darwin
  device-cache boundary. Only vibedb and SQLite make that promise in this
  harness; the measured vibedb build trailed SQLite by roughly 6–13%.

Do not compare values across lanes as if durability were unchanged. The current
single-fence synchronous journal landed after this snapshot and has not yet
received a publishable competitive refresh.

## Space snapshot

The same published run recorded:

| Measurement | Result |
| --- | ---: |
| Sustained churn, 100k live documents | 36.0 MiB allocated, flat; no maintenance |
| Verbatim primary bulk, 100k documents | 28.1 MiB allocated |
| Historical compact chunk bulk, low / high cardinality | 13.9 / 26.1 MiB allocated |

The compact row is retained only as a format-history result: it measured the
deleted chunk layout. Primary-graph compact and unified leaves need a new
cross-engine footprint run before replacing it.

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
