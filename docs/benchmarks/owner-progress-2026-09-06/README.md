# Bounded owner progress and isolated performance evidence

This checkpoint fixes starvation, not an established throughput bottleneck.
A continuously runnable Host previously kept the owner from admitting queued
reads or retrying deferred reads. After at most 16 busy loop turns, the owner
services a bounded admission turn and deferred reads, and offers both a pending
async wakeup and a pending tick. Cancellation is checked on every loop turn.
Campaign, membership, and schema controls retain their Ready ordering barrier.

The regression test keeps unrelated protocol work continuously runnable. A
queued read is admitted within 16 turns; a read initially blocked by pending
Ready is admitted within 32 turns. A separate regression requires a pending
async notification and tick to receive service in the same quantum.

Validation on updated main including the isolated result-buffer change:

- Full `internal/raftservice` suite passed (105.495 seconds).
- Full `internal/multiraft` suite passed (9.124 seconds).
- All `TestOwner` tests passed under the race detector (1.782 seconds), including
  control barriers, backpressure, deferred reads, cancellation and new fairness
  regressions.

## Performance attribution remains open

A separate commit-hint scheduling experiment passed the full raftstore suite
and focused sequencer race tests. Its deterministic blocked-persistence test
shows that metadata-only candidates can complete before an independent durable
wave while preserving durable batching and failure handling. That experiment
is not included in this checkpoint and has no RF3 performance verdict.

The attempted same-client ABBA comparison stopped during the unchanged
`02b75683` baseline, before running any candidate. Six trials completed with
zero errors; mixed_uniform with eight clients failed during the first repetition
with one error and failed verification. The client reported no reachable leader.
The server log contains elections around 10:31:25–10:31:26 UTC, repeated stale
serving fence errors, and replica-health revision-controller errors. Those
observations do not establish a root cause or prove permanent unavailability.
No competing container start/exec activity was recorded; host contention was
not excluded. No throughput comparison or attribution to owner scheduling is
valid from this incomplete campaign.

[The evidence archive](baseline-failure.tar.gz) retains the manifest, frozen
binary hashes, controller sources, commands, container event log, client and
server logs, and diagnostic snapshots. Published database volumes and binaries
are omitted. [SHA256SUMS](SHA256SUMS) identifies the archive.

The preceding independent result-buffer checkpoint is `049760fd`. Its full
Linux SQL driver suite and focused query tests passed. Previous same-query
microbenchmarks measured range-256 allocations at 95,312 versus 17,520 bytes per
operation; this allocation result is not an RF3 latency or CRDB speedup claim.
