# Read coalescing: fast path and update-regression follow-up

The solo-read optimization and diagnostic counter are merged at `b8fb9c38`.
A new RF3 campaign did not reproduce the original update slowdown, but the
large run-order variation still prevents declaring that regression resolved.
The original measurements remain in the [PR 194 report](../read-coalesce-2026-09-06/README.md).

## Shipped improvement

When the primary read is already the newest admitted request, no later read
can safely join its quorum round. The owner now avoids registering a sharing
map entry in this case; a lookup no longer allocates an empty map. Existing
pre-issue admission, generation, term, cancellation and capacity checks remain.
The existing ReadIndexShared counter is now exported as `read_index_shared` in
RF3 diagnostic snapshots, so later campaigns can measure actual sharing.

Full raftservice tests passed (68.360 seconds), as did focused owner/read and
diagnostic race tests (1.653 and 1.726 seconds). The solo-state regression test
fails against the unchanged baseline and passes with the optimization.

The owner-only microbenchmark ran baseline/candidate/candidate/baseline, with
five samples per arm, 100,000 iterations per sample, and GOMAXPROCS=1 on an
Apple M4 Max / Go 1.27.0. The same test sources were compiled against both
production revisions. It measures publication, admission and barrier
settlement, excluding network quorum, SQL and state-machine execution.

| Owner operation | Before ns/op | After ns/op | Reduction | Before/after allocations |
|---|---:|---:|---:|---:|
| Solo read | 1,519 | 1,455 | 4.2% | 3 / 3 |
| Eight-read batch | 12,109 | 11,972.5 | 1.1% | 28 / 28 |

These are medians of ten samples per revision. Steady-state bytes are unchanged
at 2,896 per solo read and 23,288 per batch. The batch timing difference is small.
The 64-ns solo reduction is not an RF3 speedup claim or a fix for the earlier
millisecond-scale update timing difference.

## Separate RF3 regression check

This compares the same frozen **pre-PR `38409c7a` and corrected PR `e8cf5617`
binaries** used in the original report. It does not include the new solo fast
path, and therefore cannot attribute a change to that optimization.

Three physical Linux ARM64 processes, RF3, four 8,192-row logical tables, one
client, uniform update_existing traffic, identical frozen client, authority
caching disabled, 12 CPUs / 24 GiB. Fresh container and volume per arm.
Baseline/candidate/candidate/baseline order; three trials per arm, with a fixed
8,000-operation warmup and 8,000 timed updates per trial. The narrower workload
and longer trials differ from the first campaign; this is not a warmup-only
causal experiment.

All **12 trials / 96,000 timed updates** were verified with zero errors. An
additional 96,000 warmup updates were untimed. Binary hashes remained unchanged
and no competing container activity was recorded; host contention was not
excluded and leader placement was not pinned.

| Comparison | Before updates/s | After updates/s | Observed change |
|---|---:|---:|---:|
| First pair | 1,026.7 | 1,037.6 | +1.1% |
| Reverse pair | 639.6 | 1,022.2 | +59.8% |
| Pooled six-trial medians | 714.4 | 1,027.5 | +43.8% |

The first pair is approximately flat; the reverse pair shows a much larger
difference. This does not support claiming the pooled gain as causal. The
original 24.6% slowdown did not repeat under this workload, but that is not
proof of its absence in the original mixed campaign.

Retained diagnostic deltas show nearly the same 8,000 foreground proposals per
trial. The slowest final baseline spends about 11.4 seconds per node in Ready
persistence, versus roughly 3 seconds in faster trials. These are aggregate
persistence-region times, not separately measured device fsync latency. They
locate the timing variation but do not establish its cause. Storage latency and
background/placement effects remain the next investigation, alongside cold-run
and hot-shard tail behavior.

## CI status at capture

Earlier main `a545a6a5` failed a grouped LATERAL protocol test with a pipe read
timeout and a hot-shard mutation qualification with p99 8.913 seconds above its
5-second limit. They have not been classified as fixed or harmless. Hosted CI
verification was requested on the latest code; local passing tests above do not
replace those gates. No timeout or performance threshold was relaxed.

## Evidence

[Warm summary](warm-summary.json), [per-node persistence observations](persistence-observations.json),
[raw warm campaign](warm-evidence.tar.gz), [microbenchmark summary](micro-summary.json),
[checksums](SHA256SUMS). Microbenchmark logs are retained alongside the summary.
The archive retains all four reports, per-operation samples, diagnostics,
controllers, commands, hashes and container event logs; published database
volumes and binaries are omitted. Internal label `scheduler` denotes PR 194.
