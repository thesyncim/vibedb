# Fused physical-node runtime: structural implementation contract

Planned by Sol with max reasoning at the user's explicit request. Implement
with Luna max, then independently review and fix with Sol max. Base is main
`fff5d6892e27db30f919c3ff7081b291cf71e4a1`; the inconclusive query experiment
`d1756055` and its evidence `97200d59` stay on their separate branch.

At the user's subsequent request, main `0128e794` was merged cleanly at
`22bd378b` before any tranche measurement. It adds lazy collection transaction
cuts, shared conflict-clock validation, parallel disjoint collection commits
and zero-copy transaction puts. Both parent and candidate retain these upstream
changes; the corrected comparison baseline is recorded below.
Sol max review then reproduced and fixed fractured read-only cuts, missing
validation for no-write transactions, and permanent handles created by rejected
collection names. Five regression tests fail on `0128e794` and pass with the
fix; the full root race suite passes. The reviewed fix is on main at
`37a171521459199b8cea9fe5f3ad50ce0b325597`. The active branch includes it at merge
`f3281140`. At the next requested refresh, main `ea8dfc0f` added striped conflict
tracking, smaller commit allocations, and cached collection handles. Sol review
then reproduced and fixed lost conflict history during quiescence/overflow,
stale pruning boundaries, counter exhaustion, and retained finished handles.
The corrected main and final comparison parent is
`82ea6abfcf51de01745a99609d5ffb0cbbb828d0`. Its clean root race suite passes;
[regressions and raw validation](qualification/sharded-clock-2026-09-04/README.md)
are retained. Both engines keep these upstream improvements. No tranche timing
preceded this baseline change, and the promotion thresholds remain unchanged.

The full objective remains at least 2× CockroachDB on a representative matched
read/write matrix with comparable durability/consistency, substantially better
space efficiency, horizontal scalability and nonblocking schema evolution.
This tranche must make structural progress; it does not redefine completion.

## Architecture and scope

Each physical node owns its SQL frontend/coordinator, multiple Raft groups,
one NodeStore, one NodeSubmissionSequencer and one NodeCheckpointCoordinator.
A common authenticated dispatcher calls local groups directly and uses the
existing authenticated transport for remote groups. Physical consolidation
and embedding the frontend with semantic local dispatch are both required.
Merely launching fewer processes or implementing a dev-only fast path is not
the requested change.

Current runtime already supports multigroup owner lanes and a shared node log,
but fresh development provisioning creates replicas×3 role processes and wraps
only one group per prepared node log. The SQL path additionally encodes an
inner SQL frame inside an outer RF3 request and reverses that at the server.
The redesign removes local coordinator/owner socket and framing boundaries,
and shares transport, scheduling and maintenance resources across groups.

Point reads still require their existing quorum proof; writes still require
their existing durable replicated commit. No leases or new clock assumptions
are introduced here. The measured single-key UPDATE already uses PrepareDirect,
an exact PK preimage, MutationPutDigestEqual and DirectMutate in one consensus
entry. Preserve direct issuers, reservations, receipts, retry retirement, exact
recipe retention on unknown outcomes and native outbox recovery. General and
multi-key operations keep their coordinated path.

Storage representation is unchanged. Do not carry over d1756055. Do not buffer
commit notifications or alter any append/commit/checkpoint/apply/acknowledgement
durability fence. A prior automatic approval review rejected buffering commit
notifications because acknowledged-write recovery semantics could change.

## 1. Transport-neutral service dispatch

Factor the existing execution in shardservice/replicated_server.go and
replicated_query.go into a single authenticated semantic dispatcher. Essential
API shape (names may follow repository conventions):

```go
type ReplicatedCall struct {
    Request ReplicatedRequest
    SQL *ShardRequest
}
type ReplicatedReply struct {
    Response ReplicatedResponse
    SQL *ShardResponse
}
type ReplicatedReplyLease interface {
    Reply() *ReplicatedReply
    Release()
}
```

The gateway uses one semantic transport abstraction. Its authenticated remote
adapter encodes/decodes once at the boundary. Its node adapter selects local
dispatch only for the exact physical NodeID and delegates remote destinations
to the remote adapter. Move SQL encoding out of QuerySQL into that adapter;
both paths invoke the same operation and result validation. Do not duplicate
SQL handlers, use a memory socket as the local path, or bypass service checks
with a bare executeReplicatedAuthenticated call.

The local capability must be bound to a validated service principal and trust
domain. A caller-supplied NodeID does not grant authentication. Preserve:

- Request grammar, field exclusions, sizes and every complete serving fence.
- Minimum caller/attempt/server/SQL deadlines and cancellation cleanup.
- Shared byte admission, SQL quotas, native/control headroom and bounded waits.
- Delegate authorization, actor/capability generation checks and the ledger
  requirement that Authority.Node equals the authenticated peer.
- Live and transitional authority, group generation acquisition, applied floor,
  quorum ReadIndex and durable publication barriers.
- Group, member, store, incarnation, allocation, term and command identity
  validation after physical-node selection. Address equality is insufficient.
- Existing refusal/hint invalidation/retry behavior and exact command bytes.
- Private ownership of SubmitOwned bytes before work can outlive cancellation;
  callers may reuse their buffers immediately after return.
- Reply lease lifetime: network replies retain it through encoding; local
  consumers detach or transfer owned results before release. No returned bytes
  may alias a snapshot cut, reused workspace, caller buffer or released reply.

Keep the existing limits initially. Lower copy count is not evidence that
admission reservations can be shrunk or concurrency increased safely. The
remaining socket server handles authentication, bounded decode/encode and
connection lifecycle around the same dispatcher.

## 2. Production fused service composition

Add `vibedb-shard serve-node -manifest PATH` using a prepared node manifest and
explicit embedded gateway configuration. It must work outside cluster dev.
Reuse RF3 serving instead of introducing another owner implementation.

Extract reusable gateway assembly into an internal package such as
internal/gatewayruntime. Move implementations instead of copying them. Sources
include cmd/vibedb-gateway/serve.go catalog setup/route-seed recovery,
durable_request_runtime.go, durable_request_adapter.go, pgwire.go,
pgwire_write_tables.go, pgwire_direct_pool.go, durable writer/journal code and
required recovery/public-service/controller construction. Standalone gateway
serving must use the same implementation.

Inject the semantic transport and expose explicit Open/Serve/Drain/Close
lifecycles. Startup authenticates manifests, identities, roots, log, TLS and
policy; constructs owners/dispatcher; starts peer/control/native services;
waits for owners; opens frontend catalog/session/issuer state; then starts
public endpoints and publishes readiness. Never await catalog quorum before
peer/native endpoints are reachable.

Every embedded frontend has a distinct persisted gateway service identity,
catalog session, direct-issuer reservation journal, fallback journals and
execution-pin journals. Storage identity and gateway principal may remain
distinct credentials in one process. Do not grant storage nodes delegation by
consolidation. Preserve configured control-plane ownership; additional SQL
frontends must not start competing schema/split/membership controllers.

Shutdown stops public admission, drains/cancels frontend work and reply leases,
settles existing recovery obligations, stops native/control users, then drains
owners/checkpoints/sequencer and closes the node store. A frontend startup
failure after Raft starts triggers orderly node shutdown.

## 3. Physical nodes and replica placement

Update cluster_dev.go, cluster_dev_node.go, cluster_dev_tables.go,
cluster_dev_ddl.go, prepare_node_rf3.go, manifests, reload, inventories,
diagnostics and process fixtures. Three physical nodes mean three serving OS
processes and three node logs containing catalog, ledger and data groups.
Retain group-specific SQL roots and store/member identities while sharing node
listeners, credentials, owner and maintenance. New groups enter the correct
node manifest and live sequencer.

Replace prepare_node_rf3.go's requirement that every group have the same entire
RF3 roster. Each group must independently satisfy RF3 rules, have exactly one
local member matching the node, and contribute consistent NodeID/address pairs
to the roster union. Shared local listeners/TLS/policy must agree. Membership
in one group never authorizes another group's traffic. Reuse and qualify the
parser/runtime's existing roster-union support.

Separate physical-node count from replication factor. Support deterministic
3- and 6-node fixtures with multiple active data groups, overlapping placements
and different RF3 subsets. The six-node fixture cycles the three initial
subsets `{0,1,2}`, `{2,3,4}`, and `{4,5,0}`; each target therefore already has
the complete peer roster needed by an appended group. Reload rejects a new
physical peer outside the running transport roster. Arbitrary new-peer
placement/rebalance needs a separately qualified enrollment protocol. This
establishes a scaling test seam, not automatic balancing. Reject incompatible old fresh-cluster manifests clearly; no legacy
data migration framework is required for this unreleased redesign.

## 4. Correctness and failure gates

Adapt existing assertions; do not replace real checks with easier mocks.
Differentially exercise local and TLS paths for point hit/miss/empty values,
scans, grouped SQL, direct writes, coordinated fallback and typed refusals.
Cover authorization revocation and rotation; wrong node/store/member/incarnation;
stale schema/route/allocation; leader change and transitional authority;
oversized input/output; admission saturation and native/control progress;
cancellation/shutdown; request mutation after unknown return; response retention
across reuse and schema retirement; slow consumer leases and zero leaked credits.

Relevant existing gates include TestReplicatedServerHoldsReadLeaseUntilSlowClientAcceptsFrame,
TestReplicatedServerRequestDeadlineBoundsOwnerAndReleasesFrame,
TestReplicatedServerLiveServingAuthorityGatesEveryRequest,
TestSQLExecutionQuotaPreservesNativeCapacity,
TestNodeSubmissionSequencerFusesRealNodeStoreEngineCalls and
TestNodeSubmissionSequencerBackpressureAndCloseDrain.

Extend the Linux filesystem-backed process campaign to three fused processes
with multiple simultaneously active groups. Acknowledge writes across groups,
kill a node, elect replacements, restart all, and verify acknowledged contents
and retry retirement. Lose direct replies after apply and retry via another
frontend. Partition peers while public/local services remain reachable; an
isolated local owner cannot serve successful linearizable reads or writes.
Interrupt live admission and verify descriptor/root/incarnation agreement.
Pause one group's apply/checkpoint preparation and check unrelated groups make
progress (a shared device stall may legitimately affect durability). Prove real
shared append waves and that closing/replacing one group preserves the owner.
Run relevant Go1.27 suites, focused race checks and a real Linux process run.

## 5. Mechanism, measurement and promotion

Add bounded diagnostics for local/remote dispatch, encoded bytes, admission,
owner/quorum wait, SQL execution, append-wave group count/syncs and checkpoint
delay. Record per-trial UTC anchors/start offsets to correlate tail events.

Before throughput claims, prove three processes/logs for three nodes; local
leader calls avoid gateway-to-shard networking and nested SQL frames; remote
and failover calls use authenticated transport; multigroup writes share real
durability waves; acknowledged operations retain their durability barriers.

Compare pinned parent and candidate on fresh fixtures in AB and BA order.
Retain every repeat, error and latency sample, process inventory, total resource
allocation, logical/allocated bytes and leader placement. Run all five existing
C1/C8 workloads, then multigroup uniform/skewed/mixed traffic at 3 and 6 physical
nodes. Separate local/remote leader cases diagnostically, but use ordinary
disclosed endpoint routing for promotion; do not favor VibeDB leader placement
over CockroachDB's routing. No competing builds/tests/profile processing during
timed runs.

Registered tranche gate: at least 1.25× parent geometric-mean throughput across
the existing matrix, no per-cell median throughput regression above 5%, and no
per-cell median p99 regression above 10%, reproduced in both orderings. Require
demonstrable multigroup benefit and all correctness/failure gates. Uncertain
results are inconclusive; retain them. These are promotion gates for this
tranche, not replacements for the full ≥2× CockroachDB objective. Do not merge
a performance claim merely because RPC/process counts fell.

Versioned visibility, interactive serializable transactions, nonblocking schema
publication, balancing and safe lease/fencing remain subsequent structural
work under the same full goal.

## Current measured checkpoint (2026-09-04)

The first shortened comparison is retained in
[the independent audit](benchmarks/fused-node-short-2026-09-04/audit-summary.md).
Candidate `27b89cd6` achieved 1.081x and 1.244x the `82ea6abf` parent
throughput geomean in the two orders, and 0.418x and 0.534x CockroachDB.
Its observed gateway locality differed between orders. Writes showed no
reproducible gain. The 1,024-row, one-table run does not qualify this tranche.
Main `fc7548a7` was merged after measurement and is excluded from those results.

The subsequent [isolated health-observation comparison](benchmarks/fused-health-2026-09-04/README.md)
includes that main revision in both fused arms. Before `494cf2aa` and after
`2184dca3` share the same catalog-publication race fix; only the health path
differs. All VibeDB and shared-client binaries prove Go 1.27 with
`GOEXPERIMENT=simd`, Linux/ARM64, and clean recorded revisions. CockroachDB runs
on the same Linux/ARM64 Docker kernel and matched aggregate resource limits.
After/before throughput geomeans are 1.068x and 1.136x; after/CockroachDB is
0.607x and 0.450x. C1 update gains are 16–23%; C8 gains are 11–89%, with substantial
order variation. Both arms are local for the first order and remote for the
second. Two first-order p99 cells regress beyond 10%. All 120 trials / 153,600
samples validate, but this remains a shortened single-host diagnostic and
does not pass the tranche or full objective. Runtime health warnings remain
in the retained logs even though health publication is active in every arm.

Main `ea866504` (window-count and sorting optimizations) was then merged at
`0383eb86` and passed the focused query tests and independent review. A fresh
[SIMD write profile](benchmarks/fused-health-2026-09-04/write-profile-0383eb86/README.md)
verifies 16,000 updates with zero errors. Compressed leaf reconstruction takes
12.6–25.8% of each node's sampled CPU; proposal/apply and node-log durability
waits remain substantial. These instrumented timings are not a new speedup
claim. The next investigations are private guarded preimage reads and compact
column patching during batch application.

The [compact batch experiment](benchmarks/fused-storage-2026-09-04/README.md)
at `7aa4f496` passed focused correctness/race tests but failed its SQL performance
comparison: after/before geomeans 0.884x / 0.852x, with inconsistent write results.
Reverse-order locality differs; first-order local-read slow bursts are unresolved.
All 120 trials validate, and all regressions are retained. Revert `d6374d33`
removes the experiment. The separately reviewed guarded-preimage change at
`7b8efb88` is not included in those timings.

Its [independent guarded-update comparison](benchmarks/fused-guarded-update-2026-09-04/README.md)
measures C8 update throughput at 1.946x / 1.997x baseline, p99 at 0.489x / 0.461x,
and cluster append barriers/update about 49% lower in both orders. These writes
remain 0.633x / 0.645x CockroachDB. Overall after/before geomeans are 1.791x /
0.890x, and after/CockroachDB is 0.577x / 0.495x. C1 gains are unstable, unchanged
reads have slow bursts, the first baseline records a term transition, and reverse
order locality differs. All 120 trials validate with SIMD Linux/ARM64 binaries
and matched resources, but the reverse order fails five throughput and seven p99
guardrails. No stable overall gain or promotion qualification is established.
The reviewed guarded path remains on the redesign branch. Next resolve baseline
stalls and C1 proposal/durability costs, together with the outstanding process
correctness gate; do not infer further structural benefits from these short runs.

Main `c0675b8a` (PR #134, ARM64 SIMD packed equality counts) was merged next at
`7b00ed3b`. It is upstream work and is absent from all SQL comparison timings
above. Future comparison builds continue to require Go 1.27 with the SIMD
experiment and verify Linux/ARM64 in each executable's build metadata.

At `7dc21395`, the real Linux physical3/physical6 process test was a failing gate.
Both layouts create three tables and validate topology and identities.
Physical3 acknowledges 18 writes and passes the owner PG content oracle,
then native frontend2 returns an internal error. Physical6 reports missing
table placement in its pinned catalog during PG verification. The latter
failure did not record which frontend returned it. Crash/restart was not
reached. That baseline established neither recovery qualification nor complete
frontend visibility; its failures remain recorded below.

Main `f4644b8e` (PR #133, bounded scans and packed string/date rendering) was
merged at `38e5ff72`. Independent source review found no blocking issue in
seek priming, half-open bounds, overflow re-entry or immutable snapshot use.
Focused `internal/storeio` tests passed on Darwin ARM64 with Go 1.27 and
`GOEXPERIMENT=simd`. The new alphabet SIMD kernel is explicitly Darwin-only;
Linux ARM64 selects its scalar fallback. The bounded cursor, seek and date
changes also apply on Linux. This upstream change is absent from every SQL
comparison reported above and receives no performance credit from those runs.
The durable bounded-scan binary-prefix, overflow and held-snapshot regression
also passed with SIMD enabled from a frozen archive of `38e5ff72`, excluding
the active row-overlay experiment.

The [catalog refresh qualification](qualification/fused-catalog-refresh-2026-09-04/README.md)
at frozen `6402842c` now passes both physical3 and physical6 on Go 1.27 SIMD,
Linux ARM64. Each layout checks 18 acknowledged rows across three tables,
SIGKILLs every exact serving child, reopens the same roots and verifies topology,
identities and row contents through all configured PG and native frontends.
The full gate takes 76.137s; no subtest skips. The catalog fix refreshes once on
definite local table absence while preserving durable replay and existing-table
preparation. No batch overlay or new diagnostics are included in this source.
This clears the previously failing gate, not the wider fault/scalability or
performance objective.
