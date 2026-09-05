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

- `8f8300cf`: serve recent persisted entry payloads from one fixed 8 MiB physical
  node arena. Payloads up to 4 KiB use 2,048 generational slots; per-group references
  are limited to the existing 64-entry window. Larger entries and misses use the
  original authenticated storage path. Private staging precedes frame-buffer
  reuse; references publish only after successful sync and namespace proof.
  Reads return detached payloads and scalar fields. Staging and warmed persistence
  allocate nothing.

## Frozen a2ac5fd8 point checkpoint

The baseline-to-frozen-G point checkpoint is retained in
[`point-assessment/README.md`](point-assessment/README.md). It freezes M at
`5160e0f6` and G at `a2ac5fd8`, with read authority disabled in both arms. N3
retry2 completed 72 verified report cells with zero workload errors. N6 has 60
verified report cells; the reverse-order M arm failed before reporting because
startup hit a replicated catalog compare-and-publish conflict, while its valid
G/CRDB pairs remain archived. Complete G/M median-throughput ratios span
0.478–1.197×, and valid G/CRDB ratios span 0.569–0.935× in N3 and
0.663–1.051× in N6 before-first, with 0.663–0.825× in the valid N6
after-first G/CRDB-only rows. These are fixed-host diagnostic comparisons; they do not
complete the nine-workload assessment or establish a 2× or final enabled-feature
claim.

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

The payload-cache campaign (`b5c3dcaa` → `8f8300cf`) verified all 144,000 measured
operations. Before-first medians were 1,307.6 → 2,118.5 ops/s, with CRDB at 5,271.1.
Reverse medians were 1,453.2 → 2,077.7, with CRDB at 5,377.6. That is a paired
43–62% throughput improvement, with p95 falling 15.508 → 9.921 ms and
13.867 → 9.941 ms. The candidate reaches only 0.39–0.40× CRDB throughput here.
This is a measured incremental improvement, not achievement of the 2× target
and not a main-to-final comparison.

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

The next separate profile confirms the entry-cache mechanism: frontend accumulated
mutex wait at `GroupView.Entries` fell from approximately 4.86 seconds in the
recent-term diagnostic to approximately 0.01 seconds in the payload-cache
diagnostic. These are different diagnostic runs, not normalized latency or
throughput ratios. The deterministic held-sync test independently fails on main
and succeeds with the payload cache.

Validation includes nine integration packages (raftstore, seglog, raftmember,
multiraft, raftservice, rafttransport, gateway, gatewayruntime, and vibedb-shard),
repeated focused race tests, cold recovery, checkpoint and replacement tests,
slot eviction and oversized-entry fallback, exact protobuf byte limits, and
zero-allocation warmed persistence. No durability fault is converted to success.

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
- `/private/tmp/vibedb-horizontal-entry-evidence` — bounded durable payload cache.
- `/private/tmp/vibedb-horizontal-profile-targeted` — pre-health diagnostic profile.
- `/private/tmp/vibedb-horizontal-profile-health` — post-health diagnostic profile.


The adjacent `.tar.gz` archives retain the raw trials and control evidence from
these campaigns, including the incomplete first campaign. `profile-terms.tar.gz`
and `profile-entries.tar.gz` retain the relevant diagnostic traces and profiles.
`artifacts.json` records archive hashes and sizes. Compiled binaries are excluded; the original manifests retain their hashes,
build settings, and immutable source identities. Numeric binary CRDB WAL files
are omitted; storage inventories and measurements remain. Profile archives keep
the frontend trace and all CPU profiles. Each archive's `omitted-files.json`
records hashes of omitted WAL files and other node traces, which remain in the
original working evidence roots. The archive does not replace those explicit source identities with
the later documentation commit.

## Remaining write-barrier diagnostic

`3d65357c` adds trace-only classification of sequencer waves. `raft.persist.metadata`
requires every logical Ready to have `MustSync=false`, no entries, and no snapshot;
all other waves are `raft.persist.required`. Both classes still execute the same
synchronous persistence path. Trace-disabled execution does not construct regions
or format metadata. Focused sequencer and allocation tests passed.

A verified 16,000-operation diagnostic run retained 578 completed metadata waves
and 1,597 required waves in the frontend flight recorder. Their accumulated region
times were 3,646.7 ms and 6,877.4 ms: metadata accounts for 26.6% of completed waves
and 34.7% of these region times. One required-wave end was unmatched. The retained
interval is incomplete and includes other activity; these fractions are not an
end-to-end throughput estimate. Disk service was markedly slower than in the
previous profile. The diagnostic does not replace the unprofiled paired results.

The next architectural investigation is deferring commit-progress persistence
while preserving durable data, term/vote, exact Ready identities, checkpoint cuts,
and crash recovery. At that diagnostic cut, no such deferral was implemented. `StableStore.Persist`
also protects HardState and retry-safe Ready IDs, so merely skipping `syncData`
on metadata waves would have bypassed those protections. The implementation
below extends the existing legacy WAL commit-hint exception explicitly. Required evidence includes
bounded pending-state ownership, sequence folding, control-boundary flushing,
and crash/restart and retry fault tests before any throughput claim.

`profile-waves.tar.gz` retains the immutable build identity, driver, frontend trace,
all CPU profiles, verification and diagnostics. `validation/wave-regions.json`
contains the region summary. Like the other profile archives, it is diagnostic
rather than a comparative benchmark.

## Shared-node commit-progress folding

`1f6a7b89` extends the legacy per-group WAL's volatile commit-only path to the
shared physical-node log. A hint requires unchanged term/vote, no entries or
snapshot, `MustSync=false`, and a monotone commit within already-durable log
bounds. All constituents of a submitted series are validated before acceptance.
The node copies scalar state and the exact caller retry digest; it retains no
borrowed protobuf fields. Pending state is bounded to 15 logical Readies per
registered group. The next durable batch folds both the live commit and pending
Ready span into the existing authenticated frame grammar. The final caller's
retry identity remains valid after reopening. Larger incoming series flush the
previously acknowledged pending span first. Checkpoint, incarnation and graceful
close boundaries flush hints. `9dfde26b` ensures an exact large-series retry does
not unnecessarily force that flush.

`seglog.Engine.PersistWave` is unchanged: every frame it accepts still completes
its original data sync before publication. Entries, snapshots, term/vote changes,
and explicit barriers always use that path. Live `InitialState` and `LogBounds`
may include commit knowledge; published durable coordinates retain a separate
persisted commit. On process loss, volatile knowledge can disappear, while the
already-durable entries remain available. Existing Raft quorum recovery and the
state machine's durable publication establish the committed prefix. This is an
extension of an existing recovery model, not a novel consensus claim.

Matched incremental results, `0d17b442` → `06684d33`:

| Order | Before ops/s | After ops/s | CRDB ops/s | After/before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|---:|
| before first | 2,096.0 | 3,124.4 | 8,125.2 | 1.49× | 10.292 | 9.017 |
| after first | 3,064.7 | 3,329.7 | 9,438.4 | 1.09× | 8.120 | 8.198 |

All 144,000 operations verified. Candidate throughput is only 0.35–0.38× CRDB;
the 2× target remains unmet. The first candidate's individual throughputs were
1,073.5, 3,448.0, and 3,124.4 ops/s; the slow trial is retained. Per-node append
barriers were approximately 2,200–2,500 per trial versus 3,000–3,300 before.
Persistence service conditions varied markedly, so the paired throughput change
must not be treated as a precise hardware-independent effect size. Reverse-order
p95 did not improve.

Validation includes the full storage, runtime, owner, transport, gateway and
shard package suite; repeated race checks; zero-allocation warmed hint/fold
pairs; detached-field ownership; exact retries before and after reopen; bounded
span overflow; committed-suffix protection; invalid multi-group/series atomic
preflight; namespace failure; ambiguous sync failure; and checkpoint/incarnation
recovery. Linux-specific shared-node restart and segment-capacity recovery tests
passed on a Docker volume. The first attempt on the container overlay filesystem
failed strict-allocation qualification and is retained as an environment failure.

`06684d33` adds an immutable client oracle and a read-only recovery phase to the
SQL benchmark client. `3990ae3b` adds a driver that uses the exact benchmark
candidate binaries, exports the client's independently tracked expected scores,
and kills the supervisor and all three RF3 servers with SIGKILL. Two restart
cycles each verified all 8,192 rows across 16 tables, including all four fields,
without reseeding or resetting scores. Observed readiness times were 1.56 s and
1.31 s. This is process-loss evidence, not a host power-loss test; the separate
storage tests prove that a volatile hint performs no new log write or sync and
that WAL-only recovery retains the previously durable entry bytes.

The post-change frontend trace contains 2,758 metadata waves averaging 0.010 ms
and 5,031 required waves averaging 0.706 ms. Metadata-wave regions accumulated
27.9 ms versus 3,550.3 ms for required waves. Direct SQL write execution averaged
2.950 ms (p50 2.362 ms); prepare averaged 0.218 ms, and quorum reads 0.527 ms.
These are diagnostic retained-region wall times, not comparative SQL throughput.
There were no unmatched region edges in this retained summary. The remaining
write gap is larger than local persistence alone; the next investigation needs
to separate proposal admission, quorum completion and response transport.

`hints.tar.gz`, `hint-crash-recovery.tar.gz`, and `profile-hints.tar.gz` retain the
campaign, restart verification, and frontend trace respectively. Their hashes
and omitted-file inventories follow the same artifact policy as prior runs.

## Proposal-stage diagnostic

`29e6e07b` separates leader discovery, gateway round trip, owner admission and
proposal completion in execution traces. Targeted owner/proposal tests passed.
A fully verified 16,000-operation diagnostic run used immutable Linux/arm64
executables from that revision; it is not an additional throughput comparison.

The frontend trace contains 3,681 matched owner admissions averaging 0.055 ms
(p95 0.099 ms) and 3,681 proposal-completion waits averaging 3.225 ms (p50 2.448 ms,
p95 8.165 ms). Gateway leader discovery averaged 0.000489 ms across 7,299 matched
regions; proposal round trips averaged 3.425 ms across 7,299 matched regions.
Required persistence waves averaged 0.797 ms, while metadata waves averaged
0.00989 ms. Owner regions cover locally led proposals, and gateway regions also
cover remote leaders: their different populations must not be subtracted as if
paired observations. One completion end and two round-trip ends were unmatched.

The evidence puts the dominant wait inside replication/application rather than
leader discovery or owner admission. Source inspection confirms ordinary leader
MsgApp traffic is already emitted through RawNode's asynchronous message path;
the local self-ack remains behind durability. A future experiment should measure
replication delivery and durable append completion before attributing this gap
to either transport scheduling or storage. A possible bounded experiment is
fusing Linux frame write and data-integrity completion, retaining the existing
barrier and outcome-unknown semantics; it has not been implemented or measured.

`profile-proposals.tar.gz` retains the frontend trace, all CPU profiles, build
identities, run verification and controls. The region summary and targeted test
output are retained under `validation/`.


## Append-completion and apply diagnostic

`c14d1ac0` adds logical append and peer-message trace events and apply execution
regions. A fully verified 16,000-operation diagnostic used immutable binaries
from that revision. This is not a comparative throughput result.

The frontend trace contains 4,528 paired entry-bearing append submissions and
owner-consumed completions, averaging 1.439 ms (p50 1.042 ms, p95 3.605 ms), with
no unmatched append edges. Required persistence waves averaged 0.766 ms across
2,738 regions. Apply execution averaged 0.178 ms across 4,511 regions, with two
unmatched ends. These populations differ and must not be subtracted as paired
measurements. This revision omitted empty append batches, so this run cannot
measure commit-hint queue latency. `ee92ecf2` extends instrumentation and the
parser to separate empty hint candidates, explicit syncs, and snapshots.

`profile-peers.tar.gz` retains the frontend trace, all CPU profiles, verified
report, and build identities. Parsed summaries are under `validation/`; archive
hashes and omitted-file inventories follow the policy above. The parser's three
focused tests and targeted raftmember tests passed.
