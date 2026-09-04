# Distributed SQL bottleneck investigation, 2026-09-04

The measured update bottleneck spans the gateway and storage publication path.
There is no evidence that replacing Raft itself is the first useful change.

## Diagnostic profile before the point-read fix

Run: `/private/tmp/vibedb-sql-profile-prepared`, 1,024 rows, 1,024 point/update
operations, 128 scan operations, 32 warmups, one repetition, clients 1 and 8.
Both engines completed with every result verified. CPU profiling and execution
tracing were enabled on VibeDB, so these timings are **diagnostic only**, not a
competitive benchmark. The run used an uncommitted development build preceding
`b533bae8`; its manifest records binary hashes and dirty status. Its tracked-file
patch does not include then-untracked files, so it is not a complete source archive.

Matched Go trace regions (2,128 completed writes, including warmups and setup):

| Region | Count | Mean milliseconds | Median milliseconds | p95 milliseconds |
|---|---:|---:|---:|---:|
| Prepare exact direct mutation | 2,128 | 5.172 | 4.911 | 7.393 |
| Execute replicated direct mutation | 2,128 | 3.389 | 3.128 | 4.666 |
| Save gateway outbox, each save | 4,259 | 1.008 | 0.855 | 2.001 |

There are normally two outbox saves per direct write. Region totals do not include
all SQL/client overhead. `pg.write` includes waiting for the per-table gate and
cannot be added to the inner regions. The profile run reached 93.7 updates/s with
both one and eight clients: the table outbox serialized all eight clients.

10.66 seconds of accumulated gateway preparation network wait came from
`readReplicatedSQLDocument`/`ReadPoint`. The data leader trace attributed 3.03 seconds
of accumulated syscall wait to `PointReadInto` -> `SnapshotCollectionsInto` ->
checkpointing. Additional checkpoint waits run on the storage committer goroutine,
so syscall time on the request goroutine is not the complete checkpoint latency.
Neither this number nor the 3.389 ms execution region isolates Raft network latency
from durable apply, scheduling and response transport.

Gateway CPU sampled 7.42 CPU-seconds over 27.31 seconds. TLS client handshakes
consumed 2.07 CPU-seconds cumulatively (27.9%), principally repeated background
replica-control health observations. This is a CPU attribution, not a claim that
27.9% of SQL latency is TLS or that eliminating it gives an equal throughput gain.

## Implemented corrections

`b533bae8` separates direct and coordinated issuer sequences and fsyncs the exact
conditional mutation recipe before direct execution. This fixes the earlier
insert/update sequence gap and permits a single guarded data-group proposal for
an existing-row primary-key update. Unknown outcomes replay the same recipe.

It also replaces physical snapshot capture in native point reads with current
primary-router reads under the state machine's publication lock. Intent checks,
applied-index floors, ownership checks, quorum reads and durable write publication
remain. Detached scan snapshots still use their existing capture protocol.

The regression test `TestSingleParticipantPointReadDoesNotForceCheckpoint` fails
against the original implementation: its first read advances the checkpoint
applied index from 2 to 3 and adds three journal syncs, one certificate sync,
four barriers and three physical collection checkpoints. With the fix, reads
following each of 32 replicated writes leave all checkpoint counters unchanged.

## Sustained run exposed a second checkpoint trigger

The full 8,192-row attempt on `b533bae8` completed its first 20,000-operation
single-client update repetition at 69.7 ops/s with verified results, then was
intentionally interrupted to address another checkpoint call site. The VibeDB
client exited 143; its JSON was not finalized, so the run is incomplete and must
not be summarized as a complete comparison. Logs remain in
`/private/tmp/vibedb-crdb-sql-prepared-b533bae8`.

Inspection found `BeginCompletionLookupBatch` still captured a physical hidden
collection snapshot. Removing point-read capture alone therefore allowed another
mandatory result lookup to force checkpointing. The follow-up uses live hidden
state under the already-held publication lock for completion lookup too. The
checkpoint regression test now covers both point reads and exact completion
lookups after every write. This is why isolated call-site improvements cannot
substitute for a sustained end-to-end measurement.

## Remaining measured or code-supported targets

- The per-table outbox still serializes independent writes. Bounded concurrent
  issuers and grouped durable outbox publication need explicit recovery semantics.
- Each successful direct mutation still includes replicated log/apply durability;
  measure and batch those barriers rather than disabling them.
- SQL reads reserve 40 MiB each from a default 112 MiB shared frame budget,
  limiting active SQL execution to two requests in the maximum-result profile.
  Query/session preparation is repeated. Native point-read improvements do not
  remove SQL snapshot or planning work.
- Health observations establish a new authenticated control connection per call.
  Reuse requires server protocol support and bounded connection ownership.
- Strong reads currently use a quorum read-index check. Lease-based reads require
  a proved leadership/clock/failure contract, not a cached leader flag.

## Current research relevant to the next protocol work

CockroachDB's SIGMOD 2026 leader-lease design uses node-level liveness checks and
Raft leader fortification to avoid per-range lease maintenance and serve reads
without coordinating each request. That is a relevant comparison for VibeDB's
per-request read-index checks and repeated health handshakes, not evidence that
VibeDB already has equivalent leases. [Cockroach Labs, May 2026](https://www.cockroachlabs.com/blog/distributed-database-leader-leases/).

LeaseGuard provides a TLA+-specified Raft lease algorithm and evaluates failover
read/write availability. Its result motivates modelling lease failure behavior
before changing read admission. [LeaseGuard, SIGMOD 2026](https://arxiv.org/abs/2512.15659).

Bodega explores local linearizable reads at multiple responder replicas through
roster leases and responder-covering write quorums. Its reported WAN gains are
from a research key-value system, not CockroachDB SQL or VibeDB; locality benefits
must be measured against the extra quorum and failure constraints here.
[Bodega](https://arxiv.org/abs/2509.07158).

The 2x CockroachDB target is not established. These changes also do not establish
full SQL feature, isolation, failover or multi-region guarantee parity.
