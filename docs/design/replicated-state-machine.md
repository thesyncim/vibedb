# Replicated state machine

`internal/replicatedstate` applies deterministic replicated commands to one
hidden system collection and one user collection. It is a non-serving internal
component.

## Command contract

A command carries two kinds of identity:

- Immutable binding contains cluster ID, cluster incarnation, topology
  recovery epoch, distribution, shard, allocation generation, shard
  incarnation, and group ID.
- Mutable fences contain replica-set version, active-policy generation,
  protection epoch, ownership epoch, schema generation, routing version, and
  route generation.

Member ID, WAL store ID, SQL log ID, and SQL-root layout belong to the
surrounding WAL and SQL reopen identity. They are not fields in a replicated
command.

An immutable mismatch is terminal. A stale mutable fence on an ordinary
sequenced command produces a durable deterministic stale result in the client
session ring. SessionOpen and a terminal-sequence retirement fail with an
unstored `ErrStaleCommand`; neither may mint state that cannot pass lookup and
reopen validation.

Each stable session record is keyed by tenant and client ID. The record verifies
that identity and stores the current shard-issued client epoch, `RetryHome`,
cumulative `AckThrough` watermark, accepted sequence high-water, status, fixed
retry window, and physical slot count. `RetryHome` is the fixed-width routing
discriminator retained through intact-shard ownership transitions. This
component does not route or migrate session rows during a physical range split.
The client epoch and the stable key together identify the current session.

Session creation is a separate zero-mutation operation. `CommandSessionOpen`
uses the canonical caller tuple `(epoch=0, sequence=1, AckThrough=0)` and is the
only path that can create a header. Ordered apply assigns the entry's Raft apply
index as its shard-local epoch and returns that token in a
`ResultSessionOpened` completion. The Open result occupies logical sequence 1;
the first user mutation uses the issued token at sequence 2. Callers never
guess an epoch, and successful Opens for distinct missing identities receive
distinct tokens.

An active epoch accepts only its next sequence. A new sequence cannot regress
`AckThrough`. An exact retained retry is idempotent and can advance
`AckThrough` without replacing its ring slot. Reuse of a retained sequence with
a different `RetryHome`, fingerprint, or `LogicalCommandDigest` is a conflict.
An exact Open retry returns the retained issued token while sequence 1 remains
above the retry floor. A competing Open cannot replace an active or retired
header.

The terminal `uint64` client sequence is reserved for retirement. An ordinary
command at that sequence returns `ErrSessionSequence`.

The logical retirement floor is the greater of `AckThrough` and the sequence
high-water minus the retry-window width, with subtraction clamped at zero. A
retry at or below this floor, or from an older epoch, returns `ErrRetryRetired`
and never executes. A session-retire command has no mutations and acknowledges
the previous high-water. With current mutable fences it appends the next
sequence and seals the epoch. A stale retirement records `ResultStaleFence` and
leaves the epoch active. A stale retirement at the terminal sequence is an
unstored `ErrStaleCommand` refusal. It leaves the session unchanged so the
client can resubmit the same sequence with refreshed fences.

`CommandSessionRelease` reclaims a retired image. It is not the next session
sequence: it repeats the exact retirement epoch, sequence, `AckThrough`,
`RetryHome`, and fingerprint. Apply validates every retained slot before
deletion. All slots must carry the exact epoch, match the canonical sequence for
their modulo position, and preserve strict applied order; only the latest slot
may contain `ResultSessionRetired`. A successful Release atomically deletes the
header and all physical slots while updating the durable counts. A release of
an active session is refused, and an old release cannot delete a newer session.
Once the header is absent, a token at or below the durable epoch high-water
proves the idempotent `ErrSessionReleased` postcondition.

The state row's `SessionEpochHighWater` is the durable anti-resurrection fence.
During ordinary apply it is the greatest Raft apply index ever assigned by
SessionOpen. It remains after Release. Mutation, retirement, and release
commands never create a missing header: a missing token at or below the
high-water is retired, while a token above it is invalid. This prevents a
delayed pre-release command from resurrecting reclaimed effects. An old Open
can be ordered again only as a new zero-mutation Open, which receives a new
apply-index token and cannot replay user mutations.

Each stable identity owns one session header and at most `RetryWindow` fixed
ring slots. `MaxSessions` is at most 1,048,576. `RetryWindow` is at most 256.
The physical slot count is exactly `min(HighSequence, RetryWindow)`. Retirement
keeps the image retryable; Release removes it and decreases `SessionCount` and
`SessionSlotCount`. Successive accepted operations for a retained identity do
not increase dedupe rows after its ring is populated. The machine refuses a new
Open at `MaxSessions` only until another retained image is released, so the
capacity is not consumed by historical operation count.

User data, replicated state, a changed session header, and a changed ring slot
publish atomically. Release deletes its complete bounded session image in one
publication. A committed session refusal or conflict advances the Raft apply
position without executing user mutations or inserting a result slot. An
invariant failure poisons the machine until reopen.

## Hidden record storage

The hidden system collection uses the `ValidationOpaqueBinary` profile and
durable opaque-value mode. State, session headers, and ring slots are raw
checksum-protected binary records. They do not pass through user JSON
validation and do not use hexadecimal JSON wrappers.

A ring slot stores fixed-size result metadata, not a complete completion
envelope. `LookupCompletion` reconstructs the canonical completion from the
machine binding, session header, and retained slot metadata. It compares the
supplied fingerprint and `LogicalCommandDigest` with the slot before it accepts
an exact retry.

The hidden image contains one state row, one row per retained stable identity,
and at most `RetryWindow` slot rows per identity. If a point lookup finds no
header, one ordered prefix-existence probe checks that the same session digest
has no orphan slot. Reopen validates the image in one ordered pass, including
the exact physical slot count, canonical modulo sequence for every slot, strict
applied order across a wrapped ring, and a single latest retirement result. Its
session scratch state is proportional to the retained session count, and its
slot work is bounded by `MaxSessions * RetryWindow`.

`ValidateImmutableBaseApplyCapacity` validates this bounded session profile
against a live immutable-base WAL. `AdoptRuntime` uses that proof directly.
Runtime adoption therefore binds the exact WAL/apply cut and fixed session
limits without requiring the retry-ring ceiling to cover the complete sealed
WAL suffix.

## Publication identities

Normal command admission and apply use one indexed point read for the session
header. A retained sequence adds one indexed ring-slot read. User planning uses
point reads for mutation keys. These paths do not scan the hidden or user
image. Mutation planning is bounded by 64 distinct keys. It performs indexed
point reads plus bytewise validation and digest work on the supplied changes.

Release is the cold exception: it validates and deletes at most `RetryWindow`
slots. The required hidden-collection `MaxBatchDocuments` is
`RetryWindow + 2`, and the required byte limit admits both that delete geometry
and the three-record hot publication. The SQL replicated store persists this
exact derived profile, so smaller retry profiles do not inherit the maximum
256-slot geometry. Each publication also supplies a precise
`BatchDocumentsHint`. This sizes only the durable transaction's initial
dedup-map reservation—normally one to three system records—while retaining the
hard ability to grow to the full bounded Release batch.

`DataChainDigest` is the deterministic, history-sensitive transition fence for
the replicated publication. An effective row mutation advances it from the
previous chain, the frozen apply contract, and the sorted exact before-and-after
rows. A command that does not change user rows preserves it.

`ImageDigest` is a separate canonical identity for one validated, ordered user
image. Reopen and import compute it on their cold full-image paths. Snapshot
artifact creation computes it during the required image stream. A caller can
also request an explicit full-image audit from a coherent read snapshot.

The machine does not maintain a canonical incremental Merkle root.
`DataChainDigest` can differ for equal current images that have different
mutation histories. Canonical same-content comparison uses `ImageDigest`, which
still requires a complete ordered scan or an existing complete image stream.

The user and system collections are exclusive state-machine storage. Direct
out-of-band mutation violates the apply contract. Ordinary reopen validates
every user row and the frozen apply contract, but after transition history
exists it cannot derive the expected canonical image from `DataChainDigest`.
Serving certification must compare `ImageDigest` at the same applied cut;
`OpenCandidate` performs that comparison for transferred images.

## SQL apply boundary

User validation must be deterministic. It can return a deterministic
wrong-shard result. Replicated apply cannot depend on local time, a random
value, or a network result.

Direct SQL mutation is not permitted after replicated binding. The apply claim
owns the prepared SQL root.

## Snapshot artifacts

A snapshot artifact streams one coherent system and user image. It has a
hash-chained header, chunks, and footer. Its footer includes the canonical
`ImageDigest` computed during the user-image stream.

The default chunk target is 4 MiB. The minimum chunk size is 4 KiB. A row is
never split between chunks.

Verified cursors permit exact byte-offset resume. The receiver must persist a
cursor checkpoint after the related row effects. The default cursor checkpoint
budget is 64 MiB.

Staging uses caller-owned non-serving collections. `OpenCandidate` validates
state, session headers, ring slots, placement, all user rows, and the artifact
`ImageDigest`. Opening a candidate does not grant membership or serving
authority.

The separate range-split child artifact currently transfers user rows and an
empty child session image. Initialization seeds the child
`SessionEpochHighWater` to the certified source cut's applied index. This is the
deliberate exception to the normal Open-issued high-water. Every source-issued
token that could predate that cut is therefore fenced on the child even though
no session rows are copied; the first child Open receives a later child apply
index. The artifact does not partition retained headers or slots by
`RetryHome`, so split-safe retained retries remain unimplemented. A source with
any retained session header or slot cannot pass the split controller's
source-seal or catalog-publication gates, and direct transition construction
also rejects it. A serving split must either release those images or migrate
them with their original completion lineage before that gate can be relaxed.

The runtime does not transport snapshots through Raft messages. A learner must
receive a certified base through an external offline process. It can then
catch up with append entries.

## Implementation references

- `internal/replicatedstate/types.go`, `machine.go`, and `apply.go`
- `internal/replicatedstate/session_codec.go` and `state_codec.go`
- `internal/replicatedstate/digest.go` and `read.go`
- `internal/replication/command.go` and `completion.go`
- `internal/raftmember/apply_capacity.go`
- `internal/replicatedstate/snapshot_artifact.go`
- `internal/replicatedstate/snapshot_stage.go`
- `sql/driver/replicated_store.go`
