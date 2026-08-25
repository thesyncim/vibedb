# Static distributed sharding

The shipped distributed service uses static shard ownership. It does not start
Raft replication or automatic movement.

## Components

- A gateway catalog defines distributions, table placements, manifests, and
  endpoints.
- `vibedb-gateway` pins one catalog generation, plans a route, and merges
  results.
- `vibedb-shard` serves one local SQL catalog with one static ownership
  identity.

## Ownership identity

Each shard request carries distribution, shard ID, allocation generation,
routing version, and ownership epoch. The shard validates these fields before
SQL execution.

The service makes a durable local serving claim. The claim prevents a stale
restart on the same store. It does not revoke a process that serves a copied
store.

## Placement

A manifest covers the complete 64-bit keyspace with ordered adjacent half-open
ranges. A shard has a unique nonzero allocation generation and at least one
leader endpoint.

The router selects the first leader only. It does not balance or fail over
between endpoint entries.

A targeted route uses a bounded leading placement prefix. A shorter prefix can
map to the complete keyspace. Admission policy decides whether scatter is
permitted.

## Non-serving range splits

`autosplit.PlanSplit` validates one desired split manifest. The plan has at
most three child ranges. One child keeps the source allocation. The target
routing version must be the exact successor because the ownership seal advances
all mutable coordinates once.

The target manifest is a copy-on-write successor: it keeps new contiguous
shard and range-start arrays for the routing hot path, shares only immutable
backing from untouched shards, and defensively copies the replacement children.
The bounded replacement validates coverage, adjacency, IDs, allocation
generations, and endpoints without a hash map or per-untouched-shard clone.

`internal/rangesplit` implements the non-serving data plane for that plan. It
uses one source scan and one compiled `vibejson` index per row. It sends each
borrowed key and document to one child and can skip the retained child copy.

The package writes bounded hash-chained artifacts for the other children. Its
verifier checks the complete artifact chain, strict key order, and the
placement of every document before a durable callback receives a chunk.

The tail translator accepts consecutive exact before-and-after transitions.
It emits one digested batch for every child at every source entry. A
shard-key move becomes a delete from the old child and a put to the new child.
An entry with no row change still advances every child.

A child stage applies verified rows and tail batches to a caller-owned durable
collection. It can resume an artifact after a persisted verified prefix. Before
tail catch-up, it rebuilds the deterministic artifact from the destination and
requires the exact manifest digest. A fixed-size digest-protected cursor records
artifact and tail progress. On Unix, the cursor store uses a writer lease, file
sync, atomic replacement, and directory sync.

An optional source capture stores exact before-and-after transitions in a
private collection. The replicated state machine includes each capture record
in the same transaction as the source state and row changes. Recovery verifies
the complete digest and publication chain before capture resumes.

The final source gap closes with one captured ownership-fence entry. It advances
ownership epoch, routing version, and route generation together and carries no
row changes. Every child persists the exact empty batch and enters a terminal
sealed phase. Sealing hashes the complete ordered child image. Reopening a
sealed stage rescans it and rejects changed files. Certification rereads and
verifies the capture record, recomputes all child batch digests, binds every
non-retained image digest, and refuses a source head that advanced past the
seal. A sealed stage can initialize the standard replicated-state snapshot base
in place without rewriting user rows. Its authenticated image proof is reused
to admit one canonical state preparation; that preparation audits the image in
one pass, and the compact seeded manifest and finish path do not scan or
serialize it again. The checkpoint group certifies the one-row hidden-state
seed before returning. Raft must install the exact returned base before
proposal, apply, lookup, snapshot export, or SQL serving is admitted.

The SQL adapter stages rows directly into the final bound user collection. It
holds an exclusive connector claim, so SQL sessions cannot observe the image
while artifact or tail work is active. Activation creates or requalifies the
seeded checkpoint group, writes only the replicated state row, and transfers
the same claim to `ReplicatedApply`. Crashes before the seed certificate or in
the certified seed-only interval reopen only through the exact sealed-stage
resume policy. Once transaction 2 durably certifies the snapshot-base binding,
an exact-identity ordinary reopen is serving-safe; a later uncertified suffix
rolls back to that certified cut.

The destination does not need a provisional WAL. `BindingForNewWAL` derives a
non-serving SQL binding from the intended immutable member identity.
`CreateStagedChildWAL` later requires the activated apply cut, child artifact
manifest, snapshot-base state, planned SQL binding, and created live WAL to
agree. It then returns the WAL without minting a node incarnation. The ordinary
Raft runtime remains the only activation owner.

The resulting certificate does not publish the catalog. Retained cleanup is a
bounded, resumable sequence of ordinary replicated deletes. It checkpoints a
batch before proposal, verifies the exact captured transition after apply, and
finishes with a fresh retained-image digest. The gateway accepts the terminal
proof only when the next manifest replaces exactly the planned source and
leaves every unrelated shard unchanged. Publication then reuses the durable
and in-memory catalog generation compare-and-swap operations.

`internal/splitcontroller` derives one safe next operation from these durable
authorities. Its fixed plan binds the source catalog generation and each
non-retained child to its final first-leader SQL and Raft identity. The control
loop does not write a second progress journal. A restart can reconstruct
progress from the caller-retained plan, capture head, child stage cursors, SQL
apply profile, WAL binding, runtime identity, prune proof, and catalog.
After catalog publication, recovery collapses the exact published child
sequence back into the original source manifest and revalidates the transition.
It does not require the old catalog snapshot.

The reconciler rejects skipped routing or catalog generations. It also requires
the cutover route generation to equal the exact catalog successor. It does not
execute an action or provide a runnable service controller.

The plan can encode the source ownership seal directly into the existing fixed
binary transition grammar only when the supplied source state exactly matches
the validated, unsealed tail cursor. A reused maximum-size buffer keeps this
step allocation-free. The plan also constructs the certified unpublished
catalog successor, but the existing durable and in-memory generation CAS
operations remain the publication authority.

An observed source publication may legitimately be ahead of a durable tail
cursor while captured writes or the ownership seal await translation. The
controller returns bounded tail catch-up in that window; it rejects a regressed
capture head, an unknown fence tuple, or a sealed cursor paired with an
unsealed source. Child activation, WAL creation, and runtime adoption use one
monotonic phase byte, so skipped phases and premature later-phase evidence fail
closed.

Up to 64 disjoint splits prepared from the same catalog generation can share
one successor publication. Each certificate, retained-image proof, and exact
one-source manifest transition is validated independently before composition.
Distinct sources in the same distribution are merged in range order; distinct
distributions are replaced in one catalog clone. Duplicate sources, allocation
identity collisions, skipped generations, or stale proofs fail before the
single catalog CAS. This permits parallel hot-shard data preparation without
serial full-catalog rebuilds or weakened cutover fencing.

Before preparation, `internal/topologyscheduler` can select a bounded cut from
as many as 4,096 hot-range recommendations. Selection reuses an 8 KiB
caller-owned workspace, allocates no heap memory on the warm path, and returns
only fixed-width candidate ordinals. It requires the exact catalog generation
and exact current source allocation, using an O(log shard-count) range-index
lookup. Deterministic priority favors benefit and pressure, then lower movement
cost; policy caps the batch, each distribution's share, and aggregate migration
bytes.

This admission cut never groups work by tenant. The range allocation is the
scheduling and fencing unit, while virtual-bucket mapping permits one tenant's
data to span physical shards when its placement key has sufficient entropy or
additional placement fields. `BuildSplitPlanBatch` binds selected ordinals to
caller-prepared destinations only after it rechecks the catalog cut, exact
source allocation, durable per-distribution allocation high-water, endpoint
directory, and cross-plan identity uniqueness. It does not assign those
resources or publish topology. Placement reservations and every later data
proof remain separate prerequisites.

The scheduler's optional 1,024-entry feedback table prevents the same exact
source incarnation from being admitted while work is in flight. Retryable
outcomes use capped exponential delay measured in new evidence windows, not
wall time. The fixed-width fingerprint table has a compact open-addressed index
and stores no topology strings. It is advisory, single-owner state: restart
safety still comes from durable child/capture proofs and exact generation
fences, not from recovering this table.

Capacity placement consumes exact-catalog-generation reports for up to 4,096
nodes. It uses SABLE's seven resource dimensions plus migration ingress and
receive concurrency. A bounded pointer-free workspace orders at most 128 child
allocations by dominant cluster share, then places as many as five replicas by
minimum projected dominant pressure. Source-leader exclusion, numeric
failure-domain anti-affinity, per-node primary/replica limits, and a physical
migration cap are hard constraints. Sibling reservations immediately affect
later scores, so equal-capacity children spread without a tenant affinity key.

The fixed cut contains node ordinals, not a serialized plan. Its bridge rechecks
catalog/node generations and endpoint membership before using the existing
allocation-lineage-fenced split builder. It does not create Raft membership,
move bytes, or publish topology.

## Non-serving replica movement

`internal/topologyscheduler` also selects capacity-relieving replica moves from
as many as 1,024 exact allocation candidates and 4,096 node reports. The warm
path is allocation-free and returns at most 64 fixed-width candidate/source/
target ordinals. A candidate is fenced to the exact catalog, range, allocation,
routing version, and ownership epoch; its seven-resource demand must be
physically present in the current first leader's report.

The scheduler excludes every current replica endpoint, enforces receive and
migration ingress capacity, and can require a destination failure domain
different from every replica that will remain. It scores the maximum projected
dominant pressure after both source relief and target load. Reservations are
netted across the whole cut, including when one node is both a source and a
target, while bounded per-source and per-target counts limit concentration.
No tenant affinity is introduced: the physical range allocation remains the
unit of evidence, placement, and fencing.

The fixed cut exposes endpoints only after rechecking the complete candidate,
node, and policy fingerprint. An external membership owner must still attach
Raft group and member identities. `internal/rebalance` then drives the existing
learner, certified snapshot, catch-up, promotion, leadership, ownership,
catalog-CAS, drain, and source-removal sequence one proof-checked action at a
time. Its manifest cutover copies only the shard array and changed leader set;
the immutable range index and untouched leader storage remain shared. This is
not a runnable automatic rebalancing controller or snapshot transport.

## Security boundary

The gateway and shard commands use the shared TLS 1.3 service profiles for
client-to-gateway and gateway-to-shard traffic. Those profiles bind traffic
class, peer identity, trust roots, and connection limits. One canonical
`vibejson` policy binds exact certificate principals and a policy generation to
data-read, data-write, schema, or delegation authority. The gateway forwards
the original application authority and the shard checks it independently.
Plaintext is available only through the explicit loopback development mode; it
is not the default cross-host security contract.

See [the operating guide](../operations/distributed.md) for exact commands and
[the distributed design](distributed-system.md) for the internal RF3 and
shipped-command boundary.

## Implementation references

- `distribution/manifest.go`, `placement.go`, and `router.go`
- `gateway/catalog.go` and `executor.go`
- `shardservice/admit.go` and `server.go`
- `cmd/vibedb-gateway` and `cmd/vibedb-shard`
- `autosplit/action.go`
- `internal/topologyscheduler/admission.go`, `feedback.go`, `planning.go`,
  `capacity_placement.go`, and `replica_move.go`
- `internal/rangesplit/partition.go`, `artifact.go`, `tail.go`, and `stage.go`
- `internal/rangesplit/stage_cursor.go` and `stage_cursor_store.go`
- `internal/rangesplit/source_capture.go` and `internal/replicatedstate/capture.go`
- `internal/rangesplit/cutover.go`
- `internal/rangesplit/stage_image.go` and `activate.go`
- `sql/driver/replicated_child_stage.go`
- `internal/raftmember/staged_child.go`
- `internal/splitcontroller/reconcile.go`
- `internal/splitcontroller/execute.go`
- `internal/rebalance/plan.go` and `reconcile.go`
