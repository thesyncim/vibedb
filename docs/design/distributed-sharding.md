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
in place without rewriting user rows. Raft must install that base before
serving activation.

The SQL adapter stages rows directly into the final bound user collection. It
holds an exclusive connector claim, so SQL sessions cannot observe the image
while artifact or tail work is active. Activation publishes the hidden apply
participant, writes only the replicated state row, and transfers the same
claim to `ReplicatedApply`. A crash after hidden-participant publication can
settle and retry with the exact apply identity and sealed stage cursor.

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
additional placement fields. Selection does not assign child endpoints or
allocation generations and does not publish topology. Those placement
reservations and every later data proof remain separate prerequisites.

## Security boundary

The gateway and shard commands accept loopback listeners only. Their protocols
have no authentication or TLS.

See [the operating guide](../operations/distributed.md) for exact commands and
[the distributed design](distributed-system.md) for the non-serving kernel
boundary.

## Implementation references

- `distribution/manifest.go`, `placement.go`, and `router.go`
- `gateway/catalog.go` and `executor.go`
- `shardservice/admit.go` and `server.go`
- `cmd/vibedb-gateway` and `cmd/vibedb-shard`
- `autosplit/action.go`
- `internal/topologyscheduler/admission.go`
- `internal/rangesplit/partition.go`, `artifact.go`, `tail.go`, and `stage.go`
- `internal/rangesplit/stage_cursor.go` and `stage_cursor_store.go`
- `internal/rangesplit/source_capture.go` and `internal/replicatedstate/capture.go`
- `internal/rangesplit/cutover.go`
- `internal/rangesplit/stage_image.go` and `activate.go`
- `sql/driver/replicated_child_stage.go`
- `internal/raftmember/staged_child.go`
- `internal/splitcontroller/reconcile.go`
- `internal/splitcontroller/execute.go`
