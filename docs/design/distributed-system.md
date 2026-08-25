# Distributed system design

VibeDB has a runnable static-shard command layer and a fixed-RF3 serving
composition. `vibedb-shard serve-rf3` constructs the latter for externally
prepared exact member artifacts. The public gateway connects canonical point
`get` requests to the native RF3 endpoint when its catalog authority is RF3.
The gateway does not route SQL, writes, scatter reads, or multi-table reads to
that endpoint. No command initializes or changes the RF3 topology.

## Runnable static layer

The runnable layer has these components:

1. A catalog snapshot defines distributions, placements, shards, and endpoints.
2. A gateway pins one catalog generation for each attempt.
3. The router derives target shards from SQL constraints and placement tuples.
4. A shard service admits the request against its static ownership identity.
5. The shard runs SQL against one local VibeDB catalog.
6. The gateway merges complete shard results or returns an error.

The gateway has no authoritative row state. The shard store has the row state.
The static SQL path always selects the first leader endpoint in the manifest.
Multiple endpoint entries do not provide automatic load balancing or failover
for that path.

Each physical manifest covers the full unsigned 64-bit range. Ranges are
half-open, ordered, and adjacent. Each shard has a unique nonzero allocation
generation.

A distribution tuple has 1 through 8 fields. The current mapper maps the tuple
to a 64-bit value. Virtual bucket width is 8 through 24 bits. Zero selects 20
bits, which gives 1,048,576 virtual buckets.

## Execution lanes

The gateway has three execution lanes:

- A single-shard read contacts one shard. A targeted read can contact more
  than one, but not all, active shards. An ordinary write must prove one owner.
- A scatter read contacts every active shard or follows an unknown route. The
  fanout executor contacts the admitted bounded shard set and merges results.
- A repartitioned grouped aggregate uses bounded worker exchange.

The last lane uses loopback exchange by default. Cross-host exchange needs an
injected trusted dialer. The shipped gateway CLI does not supply one.

The canonical RF3 point-read lane is separate from these SQL lanes. It accepts
one table and one canonical ordered primary key. The replicated table profile
binds that pair to an exact dense relation, schema generation,
relation-manifest digest, and three-replica route.

A linearizable read follows the current leader and completes a Raft
`ReadIndex`. An `at_least_applied` read supplies the exact `RouteID` and applied
index returned by an earlier operation. The gateway rejects a route mismatch
before I/O and can select a follower that has reached the requested index.
Successful reads return the exact route lineage and applied index.

One bounded authenticated native pool is shared by catalog and data reads. Its
physical key is the authenticated node and address, so unrelated shard fences
do not fragment connections. A bounded four-way cache retains exact leader
hints. A delayed failure cannot remove a newer term. Discovery, `NotLeader`,
transport failure, and retry remain bounded by the executor profile.

The point-read lane never falls back to the static SQL service. Public RF3
writes, scatter reads, multi-table reads, and a common distributed read
timestamp are not implemented.

The merge layer supports global limits, ordered results, aggregates, and
grouped partial aggregates. It cancels remaining calls after a hard error or a
total result-bound breach.

## Topology safety

One gateway attempt uses one immutable catalog generation. A stale response
can cause a retry only after a strictly newer generation is available.

The shard admission order is stable:

1. Distribution and shard
2. Allocation generation
3. Routing version
4. Ownership epoch
5. Read policy and position

This order makes stale topology errors deterministic. It also prevents SQL
execution before the service accepts the ownership coordinates.

Catalog publication is crash-safe for cooperating writers. `SaveSnapshotAfter`
also checks an exact current generation. This check is the topology compare and
swap. Publication cannot protect against a process that ignores the lock.

## Coherent multi-shard reads

The gateway creates a random 128-bit fence ID. It acquires the same scoped
lease on all routed shards. If a shard is busy, the gateway releases partial
leases and retries with a new ID.

A writer has priority after it registers. Reacquisition cannot extend a read
fence ahead of that writer. Disjoint bucket scopes can proceed concurrently.

The fence is not durable. It expires if a gateway abandons it. This protocol
establishes a scoped vector cut. It does not assign a distributed MVCC
timestamp or prove one wall-clock snapshot instant.

## Global indexes

A local index is stored with its base table. A global index uses a separate
hidden relation with independent placement.

A global index key has 1 through 4 RFC 6901 paths. A locator has 1 through 8
paths and includes all base placement paths and the primary-key path.

The lifecycle states are `Building`, `CatchingUp`, `Ready`, and `Draining`.
Only a `Ready` index can serve reads. Foreground writes maintain every active
incarnation during build and drain.

A global-index write and its base mutation use the distributed transaction
protocol. A unique global index enforces uniqueness in the hidden relation.

The backfill package plans one bounded task per base shard. The caller must run
all tasks and publish the `Ready` state. The repository has no lifecycle or
backfill CLI.

## Replicated state and digests

Normal replicated command admission and apply use one indexed point read for a
client session header. A retained sequence adds one indexed fixed-ring-slot
read. User planning uses point reads for mutation keys. These paths do not scan
the shard image. Mutation planning is bounded by 64 distinct keys. It performs
indexed point reads plus bytewise validation and digest work on the supplied
changes.

A stable session key binds tenant and client ID. Session creation is an explicit
zero-mutation `CommandSessionOpen` with the canonical caller tuple `(epoch=0,
sequence=1, AckThrough=0)`. It is the only command that may create a header.
Ordered apply assigns the Open entry's Raft apply index as the shard-issued
client epoch and returns it in a `ResultSessionOpened` completion. Sequence 1
therefore retains the Open result and the first user mutation uses the returned
token at sequence 2. Callers do not guess or coordinate the token. An Open with
stale mutable fences is an unstored `ErrStaleCommand`, not a minted token.

The collision-verifiable header stores that token, `RetryHome`, `AckThrough`,
sequence high-water, absolute UTC Unix-nanosecond lease deadline, status,
configured retry window, and physical slot count.
`RetryHome` is the fixed-width routing discriminator retained through
intact-shard ownership transitions. An active epoch accepts strictly
consecutive sequences. An exact retained retry is idempotent and may advance
`AckThrough` without replacing its slot. A different `RetryHome`, fingerprint,
or `LogicalCommandDigest` conflicts. A competing Open cannot replace an active
or retired header. The terminal `uint64` sequence is reserved for retirement or
revocation; an ordinary command at that sequence returns `ErrSessionSequence`.

Open initializes the deadline. Renew compares the exact retained deadline and
strictly extends it as the next session sequence. Revoke compares that deadline,
acknowledges the complete prior high-water, clears the deadline, and records a
terminal revocation result. Its `(sequence=H+1, AckThrough=H)` tuple is the
ordering fence: activity that wins the sequence makes the delayed revoke a
conflict rather than allowing it to seal newer state. Apply never consults a
replica-local clock.

The logical retirement floor is the greater of `AckThrough` and the sequence
high-water minus the retry-window width, with subtraction clamped at zero.
Older retries return `ErrRetryRetired` and never execute. Explicit retirement
with current mutable fences appends the next sequence and seals an epoch. A
stale retirement records a stale result and leaves the epoch active. A stale
terminal retirement is an unstored `ErrStaleCommand` refusal, so the same
sequence can be resubmitted with refreshed fences. The retired image remains
retryable until an exact `CommandSessionRelease` repeats its epoch, retirement
sequence, `AckThrough`, `RetryHome`, and fingerprint. The Open result at
sequence 1 follows the same floor: once acknowledged or displaced by the ring,
an old Open retry is retired rather than reinterpreted as a new operation.

Release validates the complete ring before changing it. Every physical slot
must carry the exact epoch, occupy the canonical modulo position, preserve
strict applied order across wrap, and only the latest slot may contain the
retirement result. One atomic publication then deletes the header and every
slot and decrements both retained counts. An active session is never released,
and an old Release cannot delete a newer image. Once the header is absent, a
released token at or below the durable epoch high-water resolves to
`ErrSessionReleased`.

The hidden collection stores one raw binary session header and at most
`RetryWindow` raw fixed-size slots per retained stable identity. The slot stores
compact result metadata. Completion lookup reconstructs a canonical completion
envelope rather than retaining one envelope per operation. The state,
session-header, and slot codecs use durable opaque values without hexadecimal
JSON wrappers.

The physical slot count is exactly `min(HighSequence, RetryWindow)`. A missing
header triggers one ordered prefix-existence probe for the session digest;
orphan slots poison the machine instead of being mistaken for free capacity.
Reopen validates the hidden image in one ordered pass, including exact epoch,
slot-count, modulo-position, applied-order, and retirement invariants. Scratch
state is proportional to retained session identities. Persistent dedupe rows
and the dedupe portion of reopen are bounded by
`1 + MaxSessions + MaxSessions * RetryWindow`, not by total operations.
Release makes `SessionCount` and `SessionSlotCount` reusable; a new Open is
refused at `MaxSessions` while that many images remain retained.
`LookupSessionLease` recovers the retained deadline and sequence fence with
point reads. This kernel does not run timers, attest elapsed time, authenticate
the proposer, or expose serving authority; RF3 serving still owns those
requirements before unbounded client-identity churn is a supported claim.

`SessionEpochHighWater` is the durable anti-resurrection fence. During ordinary
apply it is the greatest apply-index token issued by SessionOpen, and Release
never lowers it. Mutation, retirement, and release commands cannot create a
missing header. A missing token at or below the high-water is retired, while a
token above it is invalid, so delayed commands cannot resurrect reclaimed
effects. A replayed old Open can create only a new empty session with a later
apply-index token; it cannot replay user mutations.

The replicated SQL hidden collection's persisted `MaxBatchDocuments` is derived
from the configured retry window as `RetryWindow + 2`, with a byte limit that
admits both the full Release delete geometry and the three-record hot
publication. Each publication passes a precise `BatchDocumentsHint`, so the
durable transaction's dedup map normally reserves only the actual one-to-three
system changes. The hint is not an admission limit: a cold Release can grow to
the frozen hard bound and delete all retained slots atomically.

The current range-split child artifact and tail move user rows, not session
headers or ring slots. A `RetryHome` that moves to a non-retained child would
therefore lose its retained retry image, and compact completions also need the
original shard lineage rather than the child's new binding. Range split remains
non-serving when the source has any retained session header or slot: the split
controller refuses the source seal and catalog publication, and the transition
builders reject direct construction. A serving split requires a certified
session-partition transfer carrying both the retained ring and its origin
descriptor before catalog publication.

Staged child initialization nevertheless seeds the empty child's
`SessionEpochHighWater` to the certified source cut's applied index. Every
source-issued token that could predate the child is therefore fenced even
without copied session rows. The first successful child Open is ordered after
that cut and receives its later child apply index. This prevents delayed source
commands from manufacturing child-local sessions while the retained-retry
transfer gate remains closed.

`DataChainDigest` is a deterministic replicated transition fence. Each
effective mutation advances it from the prior chain, the frozen apply contract,
and the sorted exact before-and-after rows. It is history-sensitive. Two shards
with the same current rows can have different `DataChainDigest` values when
their mutation histories differ. A rejected, duplicate, configuration-only, or
data no-op command preserves it.

`ImageDigest` is the canonical identity of one validated, ordered user image.
The machine computes it on the cold path during reopen and import. Snapshot
artifact creation computes it while it already streams the image. An explicit
audit can compute it from a coherent read snapshot. Normal admission and apply
do not compute it.

The current kernel does not maintain a canonical incremental Merkle root.
`DataChainDigest` does not prove same-content equality across different
histories. A canonical `ImageDigest` still requires a complete ordered image
scan or an existing complete image stream.

Replicated-state collections are exclusively owned by the state machine.
Out-of-band row mutation is outside the storage contract. A serving candidate
must pair a canonical `ImageDigest` with the same applied cut; snapshot staging
does so before membership or serving authority can be granted.

## Replay-backed local apply

The replicated apply lane can exclusively attach its fixed system and user
collections to one `CheckpointGroup`. After Raft persistence, each ordinary
state-machine transition appends conditional redo to only the dirty members
and appends one unsynced marker decision. It publishes the member snapshots and
the new applied cut together without a local Sync. This is a local replay
contract; it does not weaken the embedded database's ordinary durability
profiles.

Periodic or pressure-driven checkpoints Sync all `K` fixed-member journals and
then Sync one authenticated `checkpoint.vgc` certificate. The marker remains a
recyclable implementation log and is not part of the normal barrier. The
certificate commits its exact transaction prefix, aborts any later prepared
suffix after a crash, and permits physical collection folds to finish or retry
after the durable cut is already established.

`AppliedIndex` is the newest reader-visible local transition.
`CheckpointAppliedIndex` is the greatest certificate-backed contiguous cut and
the only index currently exposed as safe input to Raft-WAL retention. They may
differ between checkpoints. The WAL can now capture one immutable selected
current-slot cut and build a fully synced, strictly reopened compacted sibling
around a newer snapshot base. Its generation seal binds the exact source file
and cut, placement identity, topology epoch, snapshot term and configuration,
checkpoint-retention commitment, HardState, and retained suffix.

Every WAL has one mandatory authenticated family manifest. Publishing a
candidate records one selecting generation and atomically returns its complete
fixed-width identity, including snapshot-base and retention commitments, so an
outcome-unknown publication still fences SQL. Activation revalidates and
checkpoints that exact SQL base before replacing the logical WAL leaf, Syncs
the parent, proves the selected inode, publishes the active family slot, and
proves the logical name again before releasing the opaque SQL completion.
Failure remains fail-closed and retryable at its ordered boundary. The retired
source then has no namespace link; later compactions repeat the same protocol
and authenticate the preceding generation digest.

The builder has a strict two-image disk budget and a sealed record/chunk heap
budget: the live source and one deterministic preallocated stage may coexist.
Replay authenticates every source record but projects only entries above the
certified checkpoint base into the stage, so a large checkpointed prefix costs
read/authentication bandwidth but no target write amplification. Historical
HardState and the changing presence/term of the future base remain separate
from the projected suffix until the exact final cut is proved. The
per-generation build lease and deterministic stage name bound crash debris to
one reclaimable image instead of an unbounded set of randomized WAL files.

This WAL-generation lane is consumed by the RF3 serving composition, whose
bounded authenticated peer runtime, Multi-Raft Host, replicated shard service,
settlement path, and leader-aware native gateway executor serve real in-process
and multi-process test traffic. `vibedb-shard serve-rf3` opens an already
prepared fixed three-voter member and constructs its peer and native serving
side. It does not create a WAL generation, provision a group, or make the
ordinary public gateway use the native path except for canonical point reads,
so it is not turnkey HA orchestration.
Transition capture is deliberately rejected while a
`CheckpointGroup` owns the apply state; online range split must use the later
publish-before-prune serving integration rather than adding another ordinary
durable participant to this fixed zero-Sync path.

## Autosplit boundary

The `autosplit` package records fixed-memory pressure evidence and recommends a
bucket boundary. `PlanSplit` validates a desired manifest with at most three
children.

`internal/topologyscheduler` admits up to 4,096 recommendations with one
caller-owned 8 KiB workspace and returns at most 64 candidate ordinals in a
fixed-size decision. The hot path is allocation-free. Every recommendation is
fenced to the exact catalog generation and exact source allocation; source
range lookup uses the manifest's immutable O(log shard-count) start index.
Priority is deterministic and fixed-point. Policy bounds minimum benefit,
concurrent splits per distribution, total batch size, and estimated migration
bytes so one hot keyspace cannot consume the entire publication cut.

The scheduling unit is a physical range allocation, not a tenant. Tenant keys
continue to map through virtual buckets and may occupy ranges on many shards.
Admission returns indices only. `BuildSplitPlanBatch` can then bind them to
caller-prepared destinations while rechecking the same catalog and source
fences, the durable allocation high-water, endpoint membership, and resource
uniqueness across the batch. Allocation namespaces remain per distribution.
The handoff still does not choose destination members, reserve identities,
move data, execute a controller action, or grant catalog authority.

A single-owner fixed-memory feedback table can suppress duplicate in-flight
work and apply capped exponential retry delay in source evidence windows. Its
1,024 entries use fixed-width source fingerprints plus a compact open-addressed
index; the table retains no topology strings and the warm admission path still
allocates nothing. Feedback is deliberately advisory and need not survive a
crash. Durable split artifacts, controller reconstruction, and catalog/source
generation checks remain the authority after restart.

Capacity placement accepts at most 4,096 exact-generation node reports and 128
non-retained children. Reports use the same seven fixed SABLE resource units
plus per-cut migration ingress, receive concurrency, a readiness bit, and a
numeric failure domain. The pointer-free caller workspace is bounded below 320
KiB and the warm path allocates nothing. Work is ordered by dominant share of
cluster capacity, then each replica minimizes projected dominant node pressure.
Hard policy can exclude source leaders, require distinct failure domains, cap
new replicas and primaries per node, and cap physical migration after replica
fanout.

The placement cut stores only fixed-width node ordinals. A cold bridge uses one
temporary leader backing, rechecks node and catalog generations plus endpoint
membership, then invokes the existing allocation-high-water-fenced split plan
builder. Capacity evidence never prepares Raft members or grants serving or
catalog authority.

The same capacity reports also drive bounded replica-move admission. One cut
can consider 1,024 exact physical allocations and select at most 64 moves with
a caller-owned fixed workspace and no warm-path allocations. Each candidate
binds measured seven-resource demand and migration bytes to the current range,
allocation generation, routing version, ownership epoch, and catalog
generation. The scheduler derives the source from the current first leader,
excludes every existing replica from destination choice, and can require a
distinct numeric failure domain from every replica that remains after the
move.

Move scoring minimizes the maximum projected dominant pressure across the
relieved source and destination. Source releases, target load, receive slots,
and migration ingress are reserved immediately across the cut; a node may be
both a source and a destination without double-counting incoming load as
removable evidence. Per-source and per-target concentration limits prevent a
single cut from draining or filling one node too aggressively. The handoff
rechecks the full candidate, node, and policy fingerprint before it exposes
endpoint identities. Raft group/member identity, snapshot transfer, execution,
and catalog publication remain the existing external authority boundary.

`PlanSplit` does not move data or publish the catalog. The internal
`rangesplit` package can populate non-retained child images from one source
scan. It uses compiled `vibejson` placement and deterministic hash-chained
artifacts. Verification checks key order and document placement before it
exposes a complete chunk.

Split planning constructs its target manifest copy-on-write. It allocates new
contiguous shard and range-start arrays for fast routing, structurally shares
immutable identity and leader backing for untouched shards, and defensively
copies only the replacement children. The edit revalidates exact source-range
coverage, adjacency, IDs, allocation generations, and endpoints without a map
or per-unchanged-shard allocation.

Replica movement uses a narrower immutable manifest transition. A leader-only
change copies the shard array and changed leader set while sharing the unchanged
range-start index and every untouched leader backing. Forward planning and
post-cutover recovery use symmetric transitions, so neither path reclones every
shard or rebuilds unchanged range metadata.

The range-split tail translator binds the exact source applied position, term,
last-entry digest, `DataChainDigest`, and snapshot base. It parses each before
and after document at most once. It produces one idempotence-addressed batch
for every child. Empty entries advance every child, and shard-key moves produce
a delete and a put.

A non-serving child stage applies verified artifact chunks and tail batches to
one durable collection. It persists a fixed-size cursor after durable row
effects. Recovery revalidates an artifact prefix before it skips that prefix.
The stage reconstructs the deterministic artifact from the completed
destination and requires the exact expected digest before tail catch-up.

An optional capture collection receives each exact before-and-after source
transition atomically with its replicated source publication. Its compact raw
binary records bind the split plan, placement program, publication chain, and
mutable ownership coordinates. Recovery verifies the full retained chain.

The source closes the final write gap with a terminal ownership-fence entry.
All mutable serving coordinates advance together, every child persists the
corresponding empty batch, and certification reconstructs that capture entry
and matches every durable child cursor. Each destination also scans and hashes
its complete ordered final image. Reopen verifies the same proof. A fixed-size
checksum-protected certificate binds those non-retained child images and the
exact cut but deliberately grants no serving authority.

A sealed destination can be converted in place into the standard
replicated-state snapshot base. The conversion reuses the sealed image proof,
binds the exact state envelope in a cut-zero checkpoint-group certificate,
then certifies and folds the one-row hidden-state seed. It does not rewrite,
rescan, or serialize the user image again after the canonical preparation
pass. The independent child Raft runtime must install that small base before
the child is eligible to serve.

The SQL driver owns this conversion. It prevents SQL sessions while the child
image is incomplete and while the activated claim still lacks its exact base.
It stages into the final bound table, publishes the hidden apply participant,
and transfers one exclusive claim to the normal replicated apply path. The
user collection handle and storage identity do not change. An uncertain
catalog publication retains the intended apply identity for exact settlement
and retry. Explicit child-resume open remains fail-closed across missing,
seed-only, and final-certificate crash intervals; ordinary open cannot treat a
missing active certificate as a fresh image.

Replicated catalog and split/move journal traffic uses the distinct
`topology` authorization capability. The fixed capability is carried by every
probe, linearizable read, proposal, and byte-identical retry, and the topology
class is also authenticated inside the canonical replicated command bytes. An
ordinary `data_write` principal therefore cannot relabel or construct a
catalog command even when it can reach the same RF3 endpoint. Authenticated
gateways must grant their internal TLS identity `delegate`, `data_read`,
`data_write`, and `topology`; capability zero remains valid only on the
explicit loopback development transport.

The child WAL is not allocated twice. A validated immutable member identity
provides the planned SQL binding before the WAL exists. After activation, the
WAL builder checks the live apply cut, artifact manifest, snapshot-base state,
planned binding, and newly created WAL. Only then can the existing runtime mint
an incarnation and construct a Raft node.

Retained cleanup never deletes storage behind Raft and never runs before
topology authority moves. The sealed certificate and ready child runtimes first
authorize the exact manifest successor through the existing catalog generation
CAS. Older catalog leases drain before cleanup starts. An unforgeable sealed capability from
the durable CAS receipt and drain authority binds the exact operation, manifest,
generation, and certificate, then authorizes bounded ordered prune batches submitted as normal replicated
deletes at the post-seal fences. Each batch is checkpointed before proposal and
confirmed from the exact capture transition across both apply/cursor crash
windows. Intervening retained-range writes advance one bounded capture entry per
controller turn without changing a pending prune identity. The serving path
currently applies and captures an out-of-range transition; cleanup validates
the capture and stops before another destructive batch or completion. Pending key payloads are persisted so a mutable retained
head cannot change an outcome-unknown retry. A final cursor-checkpointed scan
hashes the retained image in bounded work units.

Only the planned source may be replaced, and unrelated shard identities and
leaders remain unchanged. The certificate route generation, target catalog
generation, and source successor are equal. Crash recovery reconstructs the
same fixed byte-native operation identity from durable artifacts and current
catalog authority; no second consensus path or progress journal can override
those authorities.

The internal split reconciler validates the prepared first-leader SQL and Raft
identity for each new child. It derives one action at a time from the capture,
artifacts, child cursors, cutover certificate, apply profile, WAL binding,
runtime status, prune proof, and catalog. Its warm wait path does not allocate.
The caller must retain the immutable plan and execute each proof-checking action.
Post-publication recovery reconstructs and validates the prior source manifest
from the exact child sequence. Caller mutation of the original split request
cannot relabel an accepted controller plan.
The execution helpers encode the source seal without JSON only from an exact
caught-up unsealed tail, and construct the certified unpublished catalog
successor. They do not bypass replicated apply or the catalog CAS.
The reconciler treats a captured source ahead of its tail cursor as catch-up,
including the crash window after the ownership seal applies but before that
seal reaches every child stage. Child progress is a single monotonic phase,
not independent booleans; skipped phases and premature evidence are rejected.
Certified disjoint splits may prepare concurrently against one source catalog
and publish as one bounded successor batch. Every split retains its own data
proofs; composition only removes repeated catalog cloning and CAS contention.
The batch accepts distinct source allocations within one distribution as well
as independent distributions.
The gateway can run the replicated publish-before-prune split controller when
the replicated catalog authority is configured. The repository still has no
replica-move executor or merge planner.

## Replication kernel

The internal replication kernel contains:

- An encrypted preallocated Raft WAL
- A deterministic replicated SQL state machine
- A bounded single-owner Multi-Raft scheduler with normal-proposal coalescing
- Static authenticated-identity frame validation
- A composable mutual TLS ordinary-message stream foundation
- Offline snapshot artifacts and resumable staging
- A stateless replica-move reconciler

One scheduler turn admits only the currently queued normal-proposal prefix: at
most 64 entries and 1 MiB for a multi-entry batch. A valid proposal up to the
16 MiB command limit occupies a turn alone when it exceeds that target. The
next fair group turn captures `Ready`; there is no timer or wall-clock hold.
Configuration changes and read barriers remain strict boundaries. Committed
normal entries can be applied as a bounded consecutive prefix in one atomic
state-machine publication. Prefix selection also obeys the replicated state's
exact system and user mutation budgets. The 128-entry node workspace is a hard
ceiling, not a guarantee that every persisted profile can publish 128
independent-session commands. Each published prefix creates a synchronous
result settlement gate before the runtime can release read states or advance
the `Ready`. The default non-serving Host supplies an explicit no-local-waiters
sink. An internal bounded waiter registry can instead inject proposal-lifecycle
and result-settlement sinks. It validates canonical commands before Host
enqueue, coalesces only exact attempt identities, and publishes owned results
only after deterministic apply. Before Host ownership transfer, the registry
claims the exact Runtime source identity: `Group`, `AllocationGeneration`,
`MemberID`, `StoreID`, and `NodeIncarnation`. Settlement and proposal-lifecycle
callbacks must also present the claim's nonzero `RegistryID` and `OwnerEpoch`,
which prevents cross-registry and release/reclaim ABA. This is an internal
lifecycle fence, not network or request authentication. Live Hosts sharing one
registry must own disjoint group keys. Replacing a group owner requires fencing
ingress, terminating its pending attempts, closing the old Runtime, and
releasing the exact source capability before replacement admission. Outbound
messages are still retained as individual frames.

Wait cancellation releases only local waiter ownership. While a blocking
claimant is between wake checks, its fixed slot and notification channel remain
occupied until that claimant acknowledges the logical release; this bounded
anti-reuse handshake may briefly delay replacement admission without allocating.
An admitted attempt remains bounded until deterministic apply or a Host-observed
leadership, fault, removal, or close boundary makes it retryable as an
infrastructure outcome. A leader that stays live without quorum has no
time-based abandonment policy in this non-serving safe point. A serving gateway
still needs a leader-and-quorum lease policy around request deadlines.

The composition has a bounded authenticated peer service, replicated shard
service, leader-aware native gateway executor, request identities, and a
separate authenticated snapshot-artifact service. `serve-rf3` constructs the
peer and replicated shard services from one fixed manifest. The public gateway
uses the executor for catalog-bound point reads through a shared authenticated
pool and an exact leader-hint cache. It does not expose public RF3 writes or
multi-relation query execution. `serve-rf3` does not construct the snapshot
service and does not provide certificate enrollment, dynamic address
discovery, member changes, or empty-learner snapshot activation.

Do not describe this kernel as a turnkey replicated deployment.

## Implementation references

- `gateway/catalog.go`, `executor.go`, `merge.go`, `global_index_read.go`,
  `replicated_data_read.go`, `replicated_table.go`, and
  `replicated_leader_cache.go`
- `gateway/read_snapshot.go`, `transaction.go`, `writer.go`, `global_index.go`,
  and `global_index_backfill.go`
- `distribution/manifest.go`, `router.go`, `tuple.go`, and `bucket.go`
- `shardservice/admit.go`, `read_fence.go`, and `server.go`
- `autosplit/recorder.go`, `planner.go`, `tracker.go`, and `action.go`
- `internal/topologyscheduler/admission.go`, `feedback.go`, `planning.go`,
  `capacity_placement.go`, and `replica_move.go`
- `internal/rangesplit/partition.go`, `artifact.go`, `tail.go`, and `stage.go`
- `internal/rangesplit/stage_cursor.go` and `stage_cursor_store.go`
- `internal/rangesplit/source_capture.go` and `internal/replicatedstate/capture.go`
- `internal/raftmodel/node.go`, `internal/raftmember/runtime.go`,
  `internal/multiraft/host.go`, and `internal/replicatedstate/apply_batch.go`
- `internal/raftserve/identity.go`, `internal/raftserve/registry.go`, and
  `internal/raftserve/settlement.go`
- `internal/rangesplit/cutover.go`
- `internal/rangesplit/stage_image.go` and `activate.go`
- `internal/rangesplit/retained_prune.go` and `retained_prune_cursor.go`
- `internal/replicatedstate/staged_snapshot.go`
- `sql/driver/replicated_child_stage.go`
- `internal/raftmember/staged_child.go`
- `internal/splitcontroller/reconcile.go`
- `internal/splitcontroller/execute.go`
- `internal/rangesplit/manifest.go` and `gateway/catalog_transition.go`
- `internal/raftstore`, `internal/raftmember`, and `internal/multiraft`
- `internal/rafttransport`, `internal/replicatedstate`, and `internal/rebalance`
- `docs/design/raft-peer-transport.md`
- `internal/replicatedstate/session_codec.go` and
  `internal/raftmember/apply_capacity.go`
