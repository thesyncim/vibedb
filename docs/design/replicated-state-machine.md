# Replicated state machine

**Status:** current trusted local, non-serving replicated-apply boundary.

This design defines the implementation of
`raftmodel.StateMachine`. It applies one bounded `replication.Command` to one
durable user collection and publishes the matching completion and Raft applied
state atomically through one hidden durable system collection.

This is deliberately not a serving or high-availability milestone. It does not
wire shard RPCs to Raft, permit client-facing replicated SQL writes, install or
publish runtime snapshots, compact the Raft WAL, reserve physical system/user
storage, provide peer authentication or network I/O, or authorize Read Committed
or Serializable transactions across replicas. A static-WAL qualification now
proves finite logical completion-count headroom for one exact healthy,
initialized WAL/apply pair
after checking its binding and applied/committed/log cut. It does not reserve
bytes, grant proposal authority, or produce a lease. Its purpose is to make the
local committed-entry boundary executable and crash-testable before those
features depend on it.

The local non-serving runtime boundary is now executable. One
`raftmember.Runtime` exclusively owns the exact WAL, SQL root, apply claim, and
Raft Node for a range; `internal/multiraft.Host` schedules a bounded set of those
independent range groups. One range uses the same path with one group. Idle
groups have no goroutine, timer, or scheduler scan after their initial probe,
while no Ready, input, or logical tick is queued, but still retain their normal
in-memory and durable state. No wall-clock cadence layer is implemented; a
serving integration must schedule Raft ticks efficiently. This is
operation-count fairness between completed
synchronous steps, not latency,
election-liveness, transport, or HA qualification.

## Scope and construction

One machine owns:

- one exact replicated shard binding;
- one hidden synchronous durable system collection;
- one frozen named synchronous durable user collection;
- the catalog-scoped `durable.TxnLog` shared by those collections; and
- explicit nonzero cross-collection transaction limits.

The admission profile supports exactly one user collection and inline bounded
mutation completions only. Both system and user handles must use the synchronous
durability lane and must have no exact indexes. User values are JSON documents.
The single deterministic profile binds a nonzero validation digest and pure
mutation validator into every logical-image digest, requires the exact
one-shard-key placement contract, scans and routes every existing row at open,
and validates collapsed final mutations before no-op elision and while
computing a coherent snapshot digest.
The collection, profile, and binding cannot change while the machine is open.

Construction also proves that the transaction log and both collections are
live entries in the same physical directory. This proof performs no marker
minting or storage mutation, and the commit path repeats it before staging; a
miswired log therefore cannot first be discovered after a committed entry has
reached apply.

The binding carries the immutable logical lineage needed to reject a command
from another shard or recovery lineage:

```text
ClusterID and ClusterIncarnation
TopologyRecoveryEpoch
Distribution and Shard
AllocationGeneration and ShardIncarnation
GroupID
```

The machine also pins one accepted mutable authority profile at construction:

```text
ActivePolicyGeneration and ProtectionEpoch
OwnershipEpoch
SchemaGeneration
RoutingVersion and RouteGeneration
```

`ReplicaSetVersion` is publication state rather than immutable construction
identity. Configuration application advances it to the configuration entry's
Raft index. The other authority generations are static only because this
unserved slice has no topology-command grammar yet; the state record keeps
them distinct from immutable lineage so a committed authority update can
advance them without rebuilding the machine. A serving contract must represent
election term/leader authority explicitly rather than
silently treating this pinned `OwnershipEpoch` as a forever-static lease.

The SQL replicated binding now persists this full tuple beside the independent
local SQL `LogID`; `LogID` remains neither the shared Raft `GroupID` nor a
distributed serving lease. The `raftmember` adapter derives `MemberID` and
`StoreID` from a live healthy WAL before opening trusted apply. Enrolled hosting
`NodeID` enrollment and external lease/authority are absent serving gates. Those local
coordinates fence one physical replica and do not belong in the portable
logical snapshot, whose position is the shared
`(ShardIncarnation, GroupID, AppliedSequence)` lineage.

## Hidden system collection

The system collection contains one fixed state key and one completion record
per retained client command. Its values use one strict binary record encoding
wrapped in one canonical JSON string because durable collections accept JSON
documents.

The state record binds at least:

- the exact construction binding;
- `Applied`, the last entry term and type, and a digest of the exact normal
  entry or the configuration apply identity;
- the canonical logical data digest;
- the exact deterministic `ConfState`;
- `ReplicaSetVersion`;
- the exact static-bootstrap snapshot identity; and
- the retained completion count.

An unsupported format identifier, duplicate or noncanonical fields, invalid
checksums, an inconsistent binding, an impossible index transition, or a state
record that does not match the user collection fail closed.

A completion key is a domain-separated SHA-256 digest of length-framed tenant,
client ID, client epoch, and client sequence. `RetryHome` is deliberately not
part of the key: changing it for the same client sequence must conflict with
the retained command, not create a second command. The stored record repeats
the full client tuple and retry home, retains a digest of the exact
`Command` bytes, and contains the exact `Completion`; lookup never trusts a
hash match without comparing those fields.

## Ordered normal application

Application accepts only the next index. Repeating the current index is
idempotent only when term, entry type, and exact entry digest match the state
record. Entry terms may not decrease. Gaps, regressions, term regressions, or
different bytes at the same index are corruption.

An empty normal entry advances only the state record. A nonempty entry is
opened through the frozen command codec and checked against the complete
binding and current mutable generations before any durable mutation. A
committed entry carrying another cluster, shard, recovery lineage, or group is
log corruption and fails the machine; it is not converted into a client
rejection. Only mismatched mutable authority generations have deterministic
no-op completion semantics.

For a new valid client identity:

1. Preserve mutation ordinal order and collapse repeated keys to their final
   Put/Delete effect. This is semantic last-write-wins, not key sorting or
   command normalization.
2. Check every final key/value against the frozen collection and JSON bounds,
   read its current row, and invoke the deterministic mutation profile.
3. Only after validation, omit deletes of absent keys and puts whose exact
   value is already present.
4. Compute the prospective logical digest from the canonical ordered
   key/value image plus the final overlay.
5. Stage changed user keys, the exact completion, and the next state record in
   one `durable.UpdateCollections` call.
6. Publish the in-memory reader state only after the durable operation has
   published both collections.

If no user key changes, the system collection is the only dirty participant.
If user data changes, the transaction decision sync is the sole atomic commit
point. Recovery therefore exposes either the old user data and old system
state, or the new data, completion, and publication together—never a mixture.

An exact later duplicate leaves user data and the original completion bytes
unchanged, advances only `Applied`, and returns the original completion. The
original `AppliedSequence` is not rewritten. Reusing a client sequence with a
different fingerprint, retry home, collection, or exact command digest is a
persisted no-op conflict: user data and the old completion remain unchanged,
while the state record advances for the committed Raft entry.

Stale mutable generations and other deterministic application refusals also
advance as persisted no-ops with a bounded rejection completion. A corrupt
command envelope, corrupt retained record, impossible state transition, or
storage failure is terminal to the machine because replicas cannot safely
choose different interpretations.

For the SQL deterministic profile, a put must derive exactly its mutation key
from the configured JSON primary pointer. Missing, null, non-scalar, malformed,
or mismatched primary values become deterministic `ResultInvalidDocument`
completions; an over-bound derived key becomes `ResultTargetBound`. A delete
must contain exactly one complete ascending non-null canonical ordered-key
component. When its row exists, deriving the primary key from that row must
reproduce the same bytes. These checks occur inside planning after dedupe,
conflict, stale-fence, and unknown-collection routing, so committed semantic
refusals still advance and remain deduplicable.

The one current completion result grammar uses codes 1 through 6, including
code 6 for `ResultWrongShard`. Strict record decoding recognizes only this
grammar; open, lookup, and duplicate planning reject any other result format.

The SQL placement validator is intentionally narrow: the sole primary pointer
must also be the sole shard-key pointer and must yield a String or exact Number
accepted by the current tuple codec and native mapper. Puts route the
validated document scalar; present deletes route the current document; absent
deletes route the decoded ordered-key scalar. Points outside the exact
half-open target range produce `ResultWrongShard`. Open and snapshot digesting
route every extant row, so wrong-shard data cannot be legitimized by omission.
Composite keys, placement changes, and proof tying a serving router decision to
this exact range are not represented by the current command grammar.

The fixed current completions represent only this low-level unconditional
mutation batch. `Command` carries no arbitrary SQL result, expected row
revisions, read set, predicate, or multi-collection intent. A richer SQL command
format is required before replicated SQL DML can preserve its existing
transaction semantics.

## Configuration and static snapshot

A configuration entry mutates only the system state. It persists the exact
core-produced `ConfState`, sets `ReplicaSetVersion` to the entry index, advances
`Applied`, and leaves the logical data digest unchanged.

The current state-machine port supplies configuration metadata plus the
core-produced `ConfState`, not the original `ConfChange` bytes. Its retained
configuration digest therefore binds index, term, entry type, and deterministic
`ConfState`; it is exact at the apply-port boundary but is not a claim to retain
the original configuration proposal envelope.

The Raft state-machine port still accepts only the exact static bootstrap
snapshot fixed at construction. Installation is an idempotent verification at
the same cut, including exact snapshot bytes/identity, logical digest,
`ConfState`, binding, and replica-set version. A portable runtime artifact can
now be exported and verified, but installing it as replacement durable state,
publishing it to Raft storage, and swapping WAL generations remain unsupported
until the destination staging repository and crash protocol exist.

## Reader publication and reopen

`Applied()` and `Published()` read one immutable publication pointer. A
coherent snapshot operation holds the machine publication lock while one
`durable.SnapshotCollections` cut pins both the hidden system collection and
the user collection, then captures the matching publication. The pin remains
live through export and is closed on every outcome. Consumers must not pair an
independently acquired raw collection snapshot with an applied index, and an
exporter must not omit the completion bytes merely because the publication
pointer itself is coherent.

Open decodes the system state, compares the full binding, verifies the static
snapshot contract, recomputes the canonical logical digest from user data, and
publishes that exact state before returning. If an apply returned an unknown
outcome after the transaction decision may have become durable, the machine
fails stop. Reopen resolves the decision and observes either the old cut or the
fully new cut; `raftmodel.NewNode` then resumes from the recovered `Applied`
watermark without reapplying an already published entry.

`WriteSnapshotArtifact` now streams precisely this coherent cut. Its one current
binary grammar embeds the canonical state envelope, raw user-collection name,
every hidden system row, and every user row in strict collection/key order.
Rows are never fragmented. Ordinary chunks target 4 MiB, one exceptional row
is bounded by the frozen 4 MiB document profile, and callers can reuse a fixed
payload buffer. Every chunk carries its sequence and prior digest; the footer
binds exact row, payload, chunk, and encoded-byte totals. Verification refuses
declared oversize chunks before allocation, verifies a complete chunk before
exposing borrowed row bytes, matches the hidden state row to the header, and
emits exact end-offset/digest checkpoints for resumable destination staging.
No `encoding/json`, synthesized SQL, or per-row collection string is involved.

This is a certified transfer primitive, not a serving snapshot. Transfer
authentication and topology authorization must supply the expected final
manifest. `SnapshotArtifactStage` writes chunks only into caller-owned
non-serving synchronous collections, splits them at the destination's real
batch bounds, and orders every acknowledged collection update before cursor
publication. The fixed cursor sidecar is checksum-protected, atomically
replaced under an advisory writer lease, directory-synced, and binds the source
state/header, exact range offset, hash-chain predecessor, cumulative bounds,
collection order, and last key. Resume therefore requests only the next range;
it retains no second artifact copy and replays at most a cursor-outcome-unknown
chunk through exact idempotent puts. Cursor replacement is grouped at a 64 MiB
default (and forced on every receive return), avoiding a directory fsync per
4 MiB transfer chunk while keeping crash replay explicitly bounded.

After the footer matches the authenticated expected manifest, `OpenCandidate`
still performs the expensive full proof: hidden state and completions, user
placement validation, logical-digest recomputation, binding, membership, and
applied publication. It returns only a non-serving Machine. The transport
orchestrator, learner snapshot publication, ordered log-tail catch-up, suffix
reservation reconstruction, and WAL generation-swap protocol remain absent.
Snapshot publication and compaction therefore continue to depend on those
gates.

## Isolation boundary

VibeDB's local SQL engine already implements Read Committed and Serializable.
That does not make those modes replicated automatically.

This command is a blind, unconditional, single-collection write set. It can
be ordered and deduplicated safely, but it cannot encode a state-dependent SQL
predicate, read dependencies, per-key/tombstone revisions, a relation-level
phantom fence, or a multi-table atomic intent. Replicated SQL writes remain
disabled because the current command/result grammar does not carry those
preconditions or update durable conflict metadata.

Read-only local transactions may continue to use the existing engine. A
distributed strong read additionally needs leader authority plus `ReadIndex`
and a coherent machine snapshot at or beyond that index. Cross-shard
Serializable is unsupported because no distributed
concurrency-control protocol exists.

## Qualification and serving exclusions

The apply slice must retain the following qualification before any serving
integration:

- frozen state/completion-key golden vectors, strict decoding, fuzzing, 32-bit
  builds, and allocation bounds;
- Put/Delete/repeated-key/no-op differential histories against a reference map;
- exact duplicate, fingerprint conflict, retry-home conflict, terminal
  immutable-binding mismatch, and every mutable-generation no-op mismatch;
- empty normal entries and configuration publication/reopen;
- crash injection before each participant prepare, decision append/sync,
  collection flip, reader publication, return, and Raft advance;
- reopen checks proving data, completion, state, and logical digest are wholly
  old or wholly new;
- same-index exact replay versus different term/type/data corruption;
- coherent reader snapshots racing apply; and
- bounded capacity failures that occur before a committed entry can enter a
  serving system.

The existing [SQL replicated binding](sql-replicated-binding.md) and
[SQL replicated apply](sql-replicated-apply.md) provide exact SQL/WAL identity,
a persistent direct-write fence, a hidden atomic participant, and an opaque
local apply claim. They deliberately grant no serving authority.

Serving is prohibited because the current tree lacks:

- physical system/user byte reservation and safe completion GC beyond the
  instantaneous static-base logical headroom proof;
- learner snapshot publication into Raft storage, a reconstructed
  suffix-reservation ledger, and WAL generation compaction; coherent export,
  resumable non-serving destination staging, and full candidate validation
  alone grant no authority;
- peer enrollment, mutually authenticated network I/O, shared per-peer flow
  control, snapshot transport, dynamic membership reconciliation, and
  deadline/slow-disk isolation around the in-process host and frame validator;
- leader-aware routing with a fenced range proof, applied-position tokens,
  completion lookup, and a coherent SQL snapshot bound to the now-exposed
  non-serving `ReadIndex` outcome; and
- a replicated SQL command grammar capable of the advertised isolation mode.
