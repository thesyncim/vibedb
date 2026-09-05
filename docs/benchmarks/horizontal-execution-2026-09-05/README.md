# Horizontal execution investigation, 2026-09-05

The 2× CockroachDB throughput target remains unmet. This branch starts at main
`e1784046dd883ce9b050beae87b119d8855f3dea`. Changes preserve RF3 replication,
quorum reads, authenticated serving fences, exact retry identities, and the
existing durable acknowledgement boundary. No novelty claim is established.

## Implemented execution changes

- `f6f7aa5e`: schedule only groups with completed asynchronous work and synchronize
  membership authority only for the group that progressed. Startup still checks
  every group. The completion queue is allocation-free and coalesces repeated
  signals without losing a subsequent completion.
- `45f5a001`: defer a read whose Raft input window is busy without stopping the
  entire owner lane. The read retains its original admission charge and delivery;
  retries recheck the full fence. Peer messages and other groups keep progressing.
- `4ef63f59`: notify the submission that completed instead of broadcasting every
  durable wave to every cell. Admission-pressure retries and fatal failures still
  receive the required global notifications.
- `362c1b78`: fix catalog startup reads that span an atomic publication. Retry only
  when a second head read proves the generation advanced. A stable head with an
  invalid witness remains a hard error.
- `f1edcc60`: reuse authenticated health connections within one bounded catalog
  sweep. Every frame is independently authorized and its response identity checked.
  Only one sweep owns a cache, at most half the shared opener capacity is retained,
  and all cached connections close at sweep end. Each stream is capped at 256
  health frames. Old peers that close after one response remain supported.
- `9594d4b2`: yield one scheduler turn before freezing a singleton Ready wave,
  allowing already-runnable owners to contribute. This is a bounded scheduling
  experiment with no timer or wait for a missing group.
- `9e0bfee6`: publish durable first/last/commit coordinates separately from the
  mutex held during disk I/O. Warm Raft coordinate reads can observe the preceding
  durable cut while another wave syncs. Publication follows successful sync and
  namespace proof and precedes Ready completion. Close and both node/log failure
  signals invalidate the serving path. Checkpoints and suffix replacements update
  the coordinates; restart reconstructs them from recovered metadata.

- `b5c3dcaa`: publish a fixed 64-entry recent-term ring with the durable
  coordinates. Exact index tags prevent ring collisions. Replacement invalidates
  the old suffix, checkpoint boundaries retain their exact term, and older terms
  use the original storage lookup. Warm reads and persistence remain allocation-free.

## Matched complete SQL campaigns

Each cell uses 16 independent table groups, three physical node processes, RF3,
512 rows/table, 256-byte payloads, C8, a uniform mixed point-read/existing-update
workload, 500 warmups and three repetitions of 8,000 measured operations. Both
orders run on freshly created fixtures. Every campaign below verified all
144,000 measured operations with zero errors.

Values are medians of the three trial throughputs, in operations/second. These
are paired campaigns, not a sequence whose percentage improvements can be
multiplied. Leader placement and host conditions differ between fresh runs.

| Campaign | Order | Before | After | CRDB | After/before |
|---|---|---:|---:|---:|---:|
| main → `45f5a001` | before first | 1,957.2 | 1,955.8 | 9,766.5 | 1.00× |
| main → `45f5a001` | after first | 1,956.2 | 1,992.6 | 9,710.4 | 1.02× |
| main → `362c1b78` | before first | 1,740.9 | 1,994.9 | 9,364.8 | 1.15× |
| main → `362c1b78` | after first | 1,883.2 | 1,941.3 | 9,448.8 | 1.03× |
| `362c1b78` → `f1edcc60` | before first | 1,763.5 | 1,937.6 | 6,896.0 | 1.10× |
| `362c1b78` → `f1edcc60` | after first | 1,741.3 | 2,058.0 | 7,388.1 | 1.18× |
| `f1edcc60` → `9594d4b2` | before first | 1,983.2 | 2,218.3 | 9,127.8 | 1.12× |
| `f1edcc60` → `9594d4b2` | after first | 2,127.8 | 2,293.5 | 9,688.2 | 1.08× |

Health-reuse p95 medians were 11.320 → 11.760 ms in the first order and
12.213 → 10.191 ms in the reverse order. Yield p95 medians were
11.285 → 9.958 ms and 9.624 → 9.196 ms. Throughput improvements therefore do not
imply a uniform latency improvement in every campaign.

Coordinate-only results are inconclusive: 1,952.1 → 2,052.6 ops/s in the
before-first order, and 2,229.5 → 2,108.7 in reverse. CRDB reached 9,954.3 and
9,780.9 respectively. All 144,000 measured operations verified. p95 medians were
11.411 → 11.712 ms and 9.910 → 11.699 ms. This establishes no repeatable
competitive gain from coordinates alone. Raft's adjacent `Term` call still takes
the device mutex, motivating the recent-term follow-up.

The recent-term campaign (`9e0bfee6` → `b5c3dcaa`) also does not establish a
competitive breakthrough. Before-first throughput was 2,109.3 → 2,170.7 ops/s,
with CRDB at 4,522.9. Reverse throughput was 562.2 → 592.7, with CRDB at 5,617.5.
p95 medians were 10.557 → 11.976 ms and 46.437 → 48.077 ms. All 144,000 measured
operations verified. The reverse candidate's three trials reached 478.0, 592.7,
and 2,258.2 ops/s; average node persistence service dropped from 2.6–3.4 ms in the
first two trials to 0.71–0.74 ms in the third. Those slow trials remain included.
The paired arms improved only 3–5%, with worse p95 medians, amid substantial
storage-service variation. A separate profile is being used to choose the next
change; these data must not be represented as a 2× CRDB result.

## Mechanism evidence

The sparse-completion microbenchmark at 256 registered groups fell from a median
14,396 ns to 72.78 ns, with zero allocations in both versions. At one group the
medians were 70.21 and 72.77 ns. This isolates scheduler scaling and is not a SQL
throughput result.

Separate CPU/trace runs on `362c1b78` and `f1edcc60` include setup and verification
and have different durations. They are diagnostic only. The health-observation
caller accounted for 25.50% of frontend sampled CPU before reuse and 2.78% after.
Protocol tests independently show 300 health requests using two bounded streams
on a new peer or 300 one-shot connections on an old peer.

The post-health trace exposed 12.97 seconds of accumulated mutex wait at
`GroupView.FirstIndex` and 7.56 seconds at `LastIndex`. These values sum waits
across goroutines and do not represent elapsed workload time. Both methods held
the same mutex as disk persistence. A deterministic regression holds data sync
open: main cannot read coordinates until the sync is released; the new code
returns the previous durable coordinates and publishes the new ones afterward.

In one unprofiled post-health repetition, each node persisted about 6,300 waves
in 3.89 seconds. Average persistence service was 0.49–0.52 ms/wave and each wave
contained about 1.4 logical batches. This motivates removing scheduler/storage
coupling; it does not justify weakening durability or claiming storage is the
only remaining bottleneck.

## Method and retained failures

Runner: `scripts/bench/run-horizontal-scheduler-comparison.py`, reusing the
existing fused-node fixture with identical layout in both VibeDB arms. The
runner builds immutable revisions, captures binary hashes and build settings,
retains topology inventories, and runs both orders against pinned CockroachDB.
Go 1.27 with `GOEXPERIMENT=simd`; Linux/arm64 containers; 12 CPU/24 GiB budget
including the client. No builds, tests, or profiles run concurrently with timed
SQL trials.

These are single-host, multi-process measurements. They do not establish scaling
across independent machines, multi-region performance, failover parity, or full
SQL feature parity. The requested 2× result is still an open target.

The original 3/6-node scheduler campaign is incomplete: its six-node main arm
failed startup on the catalog head/witness race. A diagnostic profile startup
failed the same way. Both failures are retained and excluded from complete
comparisons. The catalog regression fails on main and passes on this branch.

Working evidence roots (raw samples, manifests, diagnostics, resource/storage
inventories, logs, and immutable source hashes):

- `/private/tmp/vibedb-horizontal-evidence` — incomplete original 3/6-node run.
- `/private/tmp/vibedb-horizontal-execution-evidence` — main versus read isolation.
- `/private/tmp/vibedb-horizontal-targeted-evidence` — main versus targeted wakes.
- `/private/tmp/vibedb-horizontal-health-evidence` — health transport reuse.
- `/private/tmp/vibedb-horizontal-yield-evidence` — bounded scheduler yield.
- `/private/tmp/vibedb-horizontal-coordinate-evidence` — coordinate publication.
- `/private/tmp/vibedb-horizontal-terms-evidence` — bounded recent terms.
- `/private/tmp/vibedb-horizontal-profile-targeted` — pre-health diagnostic profile.
- `/private/tmp/vibedb-horizontal-profile-health` — post-health diagnostic profile.
