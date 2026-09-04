# Fused physical-node runtime: structural implementation contract

Planned by Sol with max reasoning at the user's explicit request. Implement
with Luna max, then independently review and fix with Sol max. Base is main
`fff5d6892e27db30f919c3ff7081b291cf71e4a1`; the inconclusive query experiment
`d1756055` and its evidence `97200d59` stay on their separate branch.

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
and different RF3 subsets. This establishes a scaling test seam, not automatic
balancing. Reject incompatible old fresh-cluster manifests clearly; no legacy
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
