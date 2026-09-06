# Raft persistence and transport improvements

Implemented with Luna at max reasoning and reviewed by Astra at high reasoning.
Baseline: `400add9228c9deb5ab48fb8e3251983e2cd94a32`. The candidate is the
working-tree change described here, not a published revision.

## Changes

- **Leading commit-only completions:** the submission sequencer ends a wave
  before adding durable work to a leading commit-only prefix. A wave that
  starts with durable work still batches durable / hint / durable submissions.
  Ticket order and the store's authoritative validation remain intact. This
  does **not** let new hints bypass a durable wave that is already running.
- **One Ready aggregation/digest pass:** persistence retains preflight results
  for mapping instead of hashing and aggregating the same Ready series again.
  A bounded pointer arena keeps different groups' aggregates independent;
  borrowed references are cleared on success and failure. It adds
  `MaxSegmentEvents * sizeof(pointer)` resident memory per node store, or
  256 KiB with default options on a 64-bit machine.
- **Direct singleton transport writes:** owned outbound buffers reserve the
  existing four-byte stream prefix, allowing one-frame batches to avoid the
  copy into a second write buffer. Multiple frames still use a contiguous
  coalesced write. Memory accounting includes the prefix and capacity class;
  frames remain owned through partial writes and retries.

## Measurements

These are component benchmarks, not RF3 throughput or durable-write latency
claims. Both comparisons use Go 1.27 on Darwin/arm64 (Apple M4 Max), with the
same benchmark source on baseline and candidate and baseline / candidate /
candidate / baseline order. Other local tasks paused heavy work for timing;
ordinary host and filesystem variation was not excluded.

The 32 KiB singleton transport benchmark includes framing, encoding, queues,
and an allocation-free fake writer; it excludes TLS and network latency.
Ten samples per revision, five one-second samples per arm:

| Operation | Baseline median | Candidate median | Reduction |
| --- | ---: | ---: | ---: |
| 32 KiB singleton send | 1,557.5 ns | 986.2 ns | 36.7% |

Small singleton frames and four-frame heartbeat batches showed little change.
All measured transport cases retain zero allocations per operation.

The persistence benchmark disables the data-sync hook but still includes
namespace checks, mapping, encryption and page-cache writes. It overwrites one
uncommitted entry across a series of logical Readies. Six samples per revision,
three 150 ms samples per arm, `GOMAXPROCS=1`:

| Payload per Ready / series length | Baseline median | Candidate median | Reduction |
| --- | ---: | ---: | ---: |
| 32 KiB / 1 | 90.8 us | 80.7 us | 11.1% |
| 32 KiB / 4 | 158.1 us | 113.9 us | 28.0% |
| 32 KiB / 16 | 425.6 us | 248.6 us | 41.6% |

All measured persistence cases retain zero allocations per operation. Small
payload results have substantial filesystem variance and do not establish a
reliable gain. Raw samples and ranges are retained in
[persistence-summary.json](persistence-summary.json); the table alone must not
be interpreted as a database speedup.

## Validation

- Astra's final full-diff review found no remaining correctness blockers.
- Full `internal/raftstore` suite and full package race run passed.
- Full `internal/rafttransport` suite and full package race run passed.
- Focused multi-group reopen, suffix replacement, scratch cleanup and
  persistence-failure tests passed normally and under the race detector.
- The new leading-hint regression fails promptly against the baseline with
  `hint.Poll done=false`. Durable/hint/durable batching and fatal-failure
  propagation have separate deterministic regressions.
- Linux/arm64 shipped node-log restart, shipped node fault/failover and WAL
  pressure qualifications each passed three candidate repetitions, nine
  checks total. Each also passed once on the baseline. These use real child
  processes in a single-host Docker fixture and are correctness checks.
- Full Raft member (100.3 s), Multi-Raft (4.2 s) and Raft service (84.2 s)
  suites passed on the final candidate; see
  [integration-final-summary.json](integration-final-summary.json).
- Rebuilding the final Linux candidate reproduced the exact SHA-256 of the
  binary used for process qualification; see [provenance.json](provenance.json).

An initial baseline Raft-service invocation failed before production changes;
the rerun passed. The first invocation's complete diagnostic output was not
retained, so its cause is unclassified. No test timeout or threshold was
relaxed.

All Go builds and tests use the project environment wrapper and an
agent-specific `CODEX_AGENT_ID`, sharing the cache derived from the Git common
directory. Full local logs and test binaries are retained at
`/tmp/vibedb-raft-impact-20260906`; this directory retains benchmark arms,
compact process evidence, the baseline regression failure, and checksums.

Benchmark selectors:

```text
BenchmarkOrdinaryTransportFullWritePathSingleton32KiB
BenchmarkNodeStorePersistReadySeriesHotPath
```
