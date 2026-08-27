# Distributed clock contract

Status: **Internal qualification contract**

This page records the clock assumptions that must be tested before the
distributed runtime can claim resilience to clock skew or process suspension.
It is not a claim that those gates currently pass. The generated
[distributed feature state](../distributed-feature-state.md) remains the
evidence ledger.

VibeDB does not currently expose a globally ordered transaction timestamp or a
single-time cross-group MVCC snapshot. Within one RF3 group, Raft term, log
index, and applied index order state. A linearizable read uses Raft
`ReadIndex` with `ReadOnlySafe`. It does not use a leader lease derived from
wall time. A multi-group exact-key read obtains one such cut per group and
returns the sorted vector of route and applied-index observations. Replicated
prepare, decision, apply, and terminal records determine transaction outcome.

Those facts remove synchronized wall time from the RF3 commit and read-order
protocols. They do **not** make every distributed subsystem independent of
time.

## Clock domains

The word “clock” covers several different inputs. Their contracts must not be
collapsed into one cluster-skew number.

| Domain | Current use | Consequence of skew, pause, or jump |
| --- | --- | --- |
| Raft term, log index, and applied index | Replicated ordering, durable cuts, route observations, and index-based planning leases | These are protocol counters, not wall time. They are safety-critical and advance only through their owning replicated protocol. |
| Raft logical ticks | Election and heartbeat progress | Tick rate and delivery affect leader detection and availability. Raft safety does not depend on voters sampling equal wall times. A paused or slow member can delay elections or lose leadership. |
| Go monotonic time attached to local `time.Time` values | In-process timers, context and socket deadlines, retry backoff, idle eviction, and static read-fence leases | These are normally liveness and resource bounds. Process suspension can delay expiry and recovery. Static read fences are a separate correctness-sensitive lease contract and require their own suspend/overrun gate. |
| UTC wall time | TLS certificate validity and replicated session deadline construction | TLS time is security-admission-critical. Session timestamps are replicated values, but a producer that asserts elapsed time still needs a trustworthy authority contract. Transaction recovery, execution pins, and pressure cooldown do not use UTC authority. |
| Catalog generations, ownership epochs, route generations, request identities, and transaction identities | Topology fencing, replay, and exact retry | These are explicit identities or monotonic protocol values. They must never be synthesized from wall time. |

“Liveness-only” means that changing the clock can change when work retries,
times out, or elects a leader, but cannot authorize a conflicting replicated
state transition. It does not mean the behavior is operationally harmless.
An early deadline can reject useful work. A late deadline can retain memory,
locks, pins, or unavailable transactions longer than intended.

## RF3 read and write safety

`internal/raftmodel.NewConfig` pins `ReadOnlyOption` to
`raft.ReadOnlySafe`, enables quorum checking and pre-vote, and disables
proposal forwarding. A leader read therefore needs quorum-confirmed Raft
authority and an applied `ReadIndex` cut. A former leader isolated from a
quorum cannot turn a favorable local clock into read authority.

One group has one Raft order. Multiple groups do not. `read_batch` first pins
one catalog and then obtains a cut from every involved group. It returns no
partial result and reports an observation vector rather than inventing a
scalar timestamp. The vector proves the per-group cuts that were actually
read. It does not provide external consistency or historical reads at a common
instant.

Transaction IDs, request IDs, catalog generations, route generations,
ownership epochs, Raft terms, indexes, and commit decisions do not derive
their ordering from UTC. Static and RF3 transaction recovery append a fixed
number of ordered durable pulses before an incomplete staging state can abort.
The legacy field name `RecoveryDeadline` encodes that pulse bound. Wall time can
schedule a pulse attempt. It cannot satisfy, skip, or widen the bound.

Execution-pin acquire, renew, recover, release, and expire transitions use a
controller epoch, expected lease revision, and `LeaseAppliedThrough` fence. A
recovery or expiry command is too early unless its own catalog-group apply index
crosses the prior fence. The grammar admits no wall-clock observation. Hot-shard
qualification similarly uses catalog generations, replicated authority
revisions, and logical cooldown state.

## Security and lease caveats

TLS verification deliberately checks X.509 validity through a required local
`Now` function. An incorrect UTC clock can reject an otherwise valid peer or
accept a certificate outside the operator's intended real-time validity
window. Raft term/index fencing does not repair that security property.
Production operation therefore still needs clock monitoring, certificate
lifetime margin, and a fail-closed policy for an untrustworthy clock.

Static coherent-read fences use short-lived in-process `time.Time` leases.
The normal Go path retains a monotonic component while the process remains
alive, so an ordinary wall-clock correction is not intended to shorten such a
lease. Process suspension, lease overrun, platform timer behavior, and a read
that outlives its admitted lease still require an end-to-end gate. Until that
exists, the static vector-cut path is not qualified for arbitrary suspend or
clock-fault claims.

Replicated session apply compares exact deadline scalars and never samples a
replica-local clock. The current gateway constructs those UTC deadline values.
The durable-request contract binds its fixed logical recovery-pulse count into
`ClockContractDigest`, preventing a retry from silently changing that bound.
The public gateway constructs this service, and its structured RF3 write path
uses the same applied-index execution-pin authority. Missing pin or ledger
authority fails startup instead of selecting a wall-clock or process-local
fallback.

An applied-index lease is deliberately a progress lease, not a duration. A quiet
catalog group does not grant takeover merely because real time passed. A
controller must advance the group log beyond the certified fence. This removes
trusted elapsed time from the authority decision, but it still needs operational
qualification for write amplification, stalled progress, controller churn, and
index exhaustion boundaries.

The shipped durable-request default reserves one successor log position and
refreshes the certificate at each unfinished wave. Opening a replacement's
session and applying an exact lease-recovery CAS therefore advances past an
abandoned lease without unrelated traffic. At an observation equal to the old
fence, recovery may be proposed, but only a strictly later replicated apply can
grant the new controller authority; the old certificate is never borrowed.

## Comparison contract

This comparison defines semantics only. It is not a performance or superiority
claim.

| System model | Ordering/read contract | Clock assumption exposed by that contract |
| --- | --- | --- |
| Google Spanner | Globally meaningful commit timestamps and external consistency, implemented with TrueTime uncertainty and commit wait | Correctness uses a bounded time-uncertainty service backed by redundant time sources. This is more specific than “atomic NTP.” |
| CockroachDB | Hybrid logical timestamps and distributed MVCC reads, with uncertainty handling and a configured maximum clock offset | Physical-clock offset is part of the timestamp/uncertainty and node-admission contract. |
| Current VibeDB RF3 lane | Per-group Raft order, quorum-confirmed `ReadIndex`, explicit applied-index follower fences, replicated transaction decisions, logical recovery pulses, execution-pin progress leases, and cross-group vector cuts | Synchronized UTC is not an input to RF3 log order, transaction recovery authority, execution-pin takeover, or the vector-cut read proof. The system does not offer Spanner- or CockroachDB-equivalent global timestamp semantics. TLS and session deadline construction remain separate clock obligations. |

An application that requires “read the entire database as of timestamp T,”
globally ordered commit timestamps, bounded-staleness reads stated in seconds,
or external consistency across groups cannot infer those properties from the
current vector-cut API.

## Required qualification gates

Clock resilience remains unqualified until automated gates cover the shipped
process composition and assert both safety and bounded degradation:

1. Run sustained authenticated RF3 reads, exact retries, and multi-group
   transactions while independently stepping each node's UTC clock forward
   and backward by minutes and hours. Prove one terminal transaction outcome,
   former-leader refusal, and replica convergence.
2. Stagger, freeze, and burst logical Raft ticks independently. Combine each
   schedule with leader isolation and quorum restoration. Prove
   `ReadOnlySafe` reads never succeed on the minority and that progress resumes
   without manual state repair.
3. Suspend and resume gateways and RF3 members across socket deadlines,
   election windows, static read-fence leases, logical recovery pulses,
   and slow-client response retention. Prove no partial read, mixed decision,
   or leaked unbounded admission ownership.
4. Exercise certificates that are valid, not-yet-valid, expired, and near both
   validity boundaries under skew. Prove the exact configured X.509 policy,
   trust domain, node identity, and traffic class remain fail closed. Document
   the operator alarm and renewal margin.
5. Crash and restart at every static and RF3 recovery phase while wall clocks
   disagree and pulse scheduling stalls or bursts. Prove that no schedule can
   convert a durable commit to abort or expose a partially applied transaction.
6. Drive execution-pin acquire, renew, takeover, expiry, and release across
   controller replacement and catalog-group restart. Prove that no transition
   crosses its applied-index fence early and measure the progress cost required
   to cross a quiet lease.
7. Audit encoded identities and replicated state so UTC cannot enter Raft
   ordering, transaction or request identity, ownership epochs, route
   generations, catalog generations, GC floors, or snapshot/split cut
   selection. Treat any future wall-time dependency as a contract change.
8. Repeat the benchmark and chaos matrix with clock injection disabled and
   enabled under identical durability, TLS, replication, data, and client
   settings. Publish abort/retry rates, unavailable intervals, and tail latency
   alongside the safety assertions.

Narrow unit tests with injected certificate time and staggered logical ticks
are useful evidence, but they do not satisfy these process-level gates.

## Implementation references

- `internal/raftmodel/config.go` and `config_test.go`
- `internal/multiraft/host.go`
- `gateway/replicated_sql_read.go`
- `gateway/replicated_transaction_recover.go`
- `gateway/recovery.go`
- `shardservice/read_fence.go`
- `internal/servicetls/server.go`
- `internal/rafttransport/identity.go`
- `internal/executionpin/command.go` and `transition.go`
- `gateway/replicated_request_ledger_contract.go`
- `cmd/vibedb-gateway/durable_request_runtime.go`
