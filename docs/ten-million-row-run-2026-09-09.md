# Cancelled 10M-row comparison: insertion scaling

The user stopped this run during VibeDB loading. The client acknowledged
4,871,616 rows in 930.79 seconds (15m31s), averaging 5,234 rows/s.
CockroachDB and the measured read trials never started. This is not a
completed 10M comparison and establishes no space advantage over CockroachDB.

## Measured insertion slowdown

The same client generated deterministic, per-row `varied-v1` 256-byte payloads
and issued one autocommit INSERT per 64 rows. The schema was
`id TEXT PRIMARY KEY, bucket INTEGER, score INTEGER, payload TEXT`, with no
secondary indexes. Three VibeDB replicas ran in one Linux/arm64 Docker fixture
with a shared 12-CPU, 24-GiB cap, including the client. Ordinary production
binaries used the default qualified-clock read-authority policy. Durability
was not disabled.

| Acknowledged rows | Cumulative seconds | Rows/s since previous boundary |
| ---: | ---: | ---: |
| 1,048,576 | 109.76 | 9,554 |
| 2,031,616 | 211.58 | 9,655 |
| 3,014,656 | 461.37 | 3,935 |
| 4,063,232 | 702.27 | 4,353 |
| 4,871,616 | 930.79 | 3,538 |

Progress samples occur every 65,536 rows, so these are observed boundaries,
not interpolated million-row measurements. The last row uses final client
acknowledgement accounting: 76,119 successful batches. Cancellation terminated
the next request with `unexpected EOF`; the raw client therefore records a
failed setup. The cancellation record here distinguishes that deliberate stop
from an autonomous database failure. Full post-load verification did not run.

## Concrete structural issue

The current [resident router](../internal/storeio/resident_primary_router.go)
performs work proportional to the collection's entire leaf count during a
local leaf split. `SplitLeafPartition` scans every resident route to validate
tablet identities, allocates a new global image, copies every routing entry
and fence, and rebuilds all search keys. The scalar `SplitLeaf` path also
copies the global image. This cost increases as the table grows even when
the corresponding on-disk change is local.

The [batch topology path](../store/durable/store_file_primary_batch_topology.go)
first calls `flushPendingForStructural`. Its
[structural transaction implementation](../store/durable/store_file_primary_structural.go)
checkpoints pending canonical changes before the split and flushes the
structural publication afterward. These barriers currently protect durable
root and retirement ordering; simply deleting them would be incorrect.

Retained insertion profiling from an earlier `ccf` revision supports the
router diagnosis: `SplitLeafPartition` consumed 0.95 seconds of 1.20 seconds
in topology preparation. That profile also includes startup work. It cannot
establish the fraction of this cancelled run spent in the router or explain
the entire CockroachDB performance gap. The present evidence identifies a
concrete size-dependent flaw; it is not a fresh CPU attribution.

The required design direction is bounded routing updates with safe publication
for concurrent readers, followed by reducing structural checkpoint scope while
preserving journal, durable-root and reclamation ordering. No such production
fix is included in this report.

## Space, shutdown and provenance

At the stopped partial-load boundary, aggregate allocated bytes were
6,026,260,480 and apparent bytes were 5,791,215,095. These include all three
replicas, cover fewer than 5M rows, and are neither a settled 10M footprint nor
an estimate of the eventual size.

The runner stopped all candidate shards and did not launch CockroachDB. Its
default cleanup removed this run's temporary container and database volume;
the logs and timing evidence remain. Older retained data and protected
containers were untouched.

- Candidate code: `23f884e14b5a8c43c773255eeb5486740771a42d`; all 40 CI checks
  passed, with two optional checks skipped. This run did not enable compression.
- Client source: `bce6064d02f11e8d7364166b3c06ec54bc89f9b5`.
- Frozen runner SHA-256:
  `c90c6b0747173194fae107ad266fa69bd8ddffcd0c1465ca6d0ed63579f56c57`.
- [Machine-readable partial results and evidence hashes](benchmarks/ten-million-2026-09-09/cancelled-summary.json).
- Local raw evidence: `/private/tmp/vibedb-10m-final-23f884e-sep8`.
- Earlier profile:
  `/private/tmp/vibedb-insert-rf3-scale-ccf-20260908-retry1/tail/leader-structural.cpu.top.txt`.

The earlier [20K update results](batch-slot-preservation-results.md) measure a
different operation. They do not establish improved large-table insert speed.
