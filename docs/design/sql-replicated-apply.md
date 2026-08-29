# Replicated SQL apply

Replicated apply is an internal deterministic mutation path for a bound SQL
root. It is not a public SQL session. `vibedb-shard serve-rf3` opens an exact
externally prepared apply identity and transfers its claim to the RF3 runtime;
the static `vibedb-shard serve` command does not use this path.

## Ownership

A runtime adopts one healthy WAL, one bound SQL database, and one apply claim.
Direct SQL mutation remains fenced while the claim is active.

Publishing or recovering a selected WAL generation latches its complete family,
generation, binding, snapshot-base, and retention identity on the apply claim.
That claim remains the only apply/snapshot capability until raftstore has made
the family active and returned its opaque completion. Closing a claim while
this selection is pending atomically retires its connector before releasing the
claim reference; the same `Database` cannot manufacture an unfenced successor.
Only a complete root close and authenticated WAL/SQL reopen may reconstruct and
resume an interrupted activation.

## Staged child activation

`OpenReplicatedChildStage` claims one bound user collection before any child
row is visible through SQL. It requires an exact child artifact, placement
program, shard range, allocation generation, ownership epoch, routing version,
and apply profile. The claim accepts verified artifact chunks and consecutive
tail batches. It does not expose the collection or the state machine.

`Activate` requires a sealed cutover certificate. It publishes or reuses the
hidden apply collection, attaches both collections to one seeded checkpoint
group, certifies the single replicated-state row, and transfers the exclusive
connector reference to a base-pending `ReplicatedApply`. It does not copy the
user rows or replace their storage incarnation. The base-pending owner accepts
only the exact authenticated snapshot base; proposal, apply, lookup, export,
and SQL serving remain refused until transaction 2 certifies that binding and a
runtime claims the ordinary apply owner.

An invalid artifact-output profile fails before hidden storage publication. An
uncertain catalog publication returns the intended apply identity and keeps the
stage exclusive. An exact retry settles publication and resumes activation.

## Determinism

A replicated command contains immutable binding identity and mutable state
fences. User validation cannot depend on local time, random input, or a network
result.

A wrong immutable binding is terminal. A stale mutable fence on an ordinary
sequenced command records a durable deterministic stale result in the client
session ring. A stale SessionOpen or terminal-sequence retirement is an
unstored `ErrStaleCommand`; neither may mint state that cannot pass lookup and
reopen validation.

## Atomic publication

Session creation is explicit. `CommandSessionOpen` carries no user mutations
and has the canonical caller tuple `(epoch=0, sequence=1, AckThrough=0)`. It is
the only command that can create a session header. Ordered apply assigns the
Open entry's Raft apply index as the shard-issued client epoch and returns that
token in a `ResultSessionOpened` completion. The first user mutation therefore
uses the returned token at sequence 2. Clients do not predict the next epoch,
so concurrent Opens for different stable identities cannot race on a shared
`high-water + 1` value.

The apply machine publishes user data, replicated state, a changed session
header, and a changed retry slot atomically. A stable session header is keyed by
tenant and client ID. It stores the current shard-issued epoch, `RetryHome`,
cumulative `AckThrough` watermark, sequence high-water, absolute UTC
Unix-nanosecond lease deadline, status, configured retry window, and physical
slot count. `RetryHome` is the fixed-width routing
discriminator retained through intact-shard ownership transitions. Physical
split activation does not yet migrate session rows.

An active epoch accepts strictly consecutive sequences. An exact retained retry
is idempotent and can advance `AckThrough`. Reuse with a different `RetryHome`,
fingerprint, or `LogicalCommandDigest` is a conflict. The logical retirement
floor is the greater of `AckThrough` and `HighSequence-RetryWindow`, with
subtraction clamped at zero. A retry at or below that floor returns
`ErrRetryRetired` and never executes. Sequence 1 is the retained Open result, so
an Open retry also becomes retired when the ring or acknowledgement watermark
moves past it.

A session-retire command has no mutations and acknowledges the prior sequence
high-water. With current mutable fences it appends the next sequence and seals
the current epoch. A stale retirement records a stale-fence result and leaves
the epoch active. A stable identity cannot Open again while its active or
retired header remains.

Open initializes a positive deadline. Renew is an ordinary next-sequence
operation with an exact retained-deadline compare and a strict extension;
success stores `ResultSessionRenewed`. Revoke compares the retained deadline,
acknowledges the complete prior high-water, stores `ResultSessionRevoked`,
clears the deadline, and leaves the image retired for exact retry and Release.
The `(ClientSequence=H+1, AckThrough=H)` fence prevents a delayed revoke from
sealing activity that consumed the sequence first. Apply reads no wall clock.

`CommandSessionRelease` is an exact, non-sequenced acknowledgement of the
retirement tuple. It verifies the epoch, retirement sequence, `AckThrough`,
`RetryHome`, fingerprint, and every physical ring slot before it changes state.
Every slot must belong to the exact epoch, occupy its canonical modulo position,
and retain strictly increasing apply order; the retirement result must be the
latest retained sequence. Release then deletes the header and all slots in the
same publication that decrements the session and slot counts. It cannot delete
an active or newer session. Retrying an already completed Release derives the
durable postcondition from the absent header and the epoch high-water and
returns `ErrSessionReleased`.

The state row retains `SessionEpochHighWater`, the durable anti-resurrection
fence. During ordinary apply it is the greatest apply index ever issued by
SessionOpen. Ordinary commands never create a missing header. A missing command
token at or below this high-water is retired; a greater token is invalid.
Release preserves the high-water, so delayed commands cannot resurrect
reclaimed effects. Replaying an old zero-mutation Open after release may
allocate a new token and empty session, but it can never replay user data.

The terminal `uint64` client sequence is reserved for retirement. An ordinary
command at that sequence returns `ErrSessionSequence`. A stale terminal
retirement is an unstored `ErrStaleCommand` refusal. It leaves the session
unchanged so the client can resubmit the same sequence with refreshed fences.

One command permits at most 64 distinct mutations. The frozen apply identity
sets `MaxSessions` and `RetryWindow`. The system image contains one state row,
one header per retained stable identity, and exactly
`min(HighSequence, RetryWindow)` fixed slots per identity. Retirement keeps this
bounded state available for retry. Exact Release reclaims it and reduces
`SessionCount`, so cooperative clients can reuse capacity indefinitely. A new
Open is refused at `MaxSessions` while that many session images are retained.
`LookupSessionLease` provides point-read recovery of the retained lease and
sequence fence. The kernel supplies no timer, elapsed-time attestation,
authentication, or serving authority; RF3 serving must provide those pieces
before it may claim unbounded client-ID churn.

The hidden apply collection stores state, session headers, and retry slots as
raw checksum-protected binary values in durable opaque-value mode. It does not
wrap these records in hexadecimal JSON. A slot retains compact fixed-size
result metadata. Completion lookup reconstructs the canonical completion
envelope from the machine binding, session header, and slot. It checks the
supplied fingerprint and `LogicalCommandDigest` before it accepts an exact
retry.

Admission and apply use one indexed session-header point read and at most one
indexed slot point read on the ordinary path. Release is deliberately a cold
bounded operation: it validates and deletes at most `RetryWindow` slots. A
missing header triggers one bounded ordered prefix-existence probe so an orphan
slot can never be treated as an unused identity. Reopen validates the hidden
image in one ordered pass, including canonical slot positions and apply order.
Its hidden row count and session scan work are bounded by currently retained
sessions and their fixed retry windows, with a hard ceiling of
`1 + MaxSessions + MaxSessions * RetryWindow`, not by the number of applied
operations.

The hidden collection's persisted mutation limits are derived from the exact
`RetryWindow`. The base session/control profile uses
`max(7, 3*RetryWindow+3)` for `MaxBatchDocuments`; request-ledger and
distributed-transaction profiles can freeze a wider limit for their command
geometry. The byte bound admits hot publication, Release, execution-pin, and
route-gate rows. Normal publications pass their exact system-row count as
`BatchDocumentsHint`, so the durable transaction initially reserves for the
actual authority, session, slot, route-gate, and transaction rows. That count
can exceed three. The hint controls initial reservation only; a durable batch
can still grow to its frozen profile limit for Release or another admitted
wide command.

An invariant or corruption error poisons the machine until reopen.

Range-split child activation authenticates the sealed user image once and
seeds its empty hidden session image through the checkpoint group. The seed
transaction writes one state row, certifies and folds it, and returns a compact
snapshot base that references the already durable image rather than copying it.
The activation claim remains non-serving and rejects proposal, apply,
completion lookup, and snapshot export until the exact returned base is
installed and durably bound.

The child `SessionEpochHighWater` is the certified source cut's applied index.
This is the deliberate exception to the normal Open-issued high-water: it
fences every source-issued token that could predate the child, so a delayed
mutation cannot create a child-local session. The first successful child Open
is ordered after that cut and receives its later child apply index. The current
artifact still does not transfer retained headers or slots. It therefore cannot
preserve retries whose `RetryHome` moves to a non-retained child or reconstruct
an old completion from the child's new immutable binding. The split controller
continues to require the source session image to be empty before sealing and
publication; serving split retries need a certified transfer contract before
that gate can be relaxed.

## Runtime order

The member runtime persists a Raft `Ready` before it sends related messages. It
then applies committed entries, records read states, and advances Raft.

A deterministic WAL or apply failure latches terminal runtime failure.

WAL generation selection is also an apply fence. The live capture binds one
snapshot-base certificate and checkpoint-retention witness to the exact WAL,
topology epoch, shard-store binding, schema state, and applied cut. Selection
returns its authenticated fixed-width generation identity in the same API result
as any persistence error, so an outcome-unknown family write cannot race WAL
close and leave SQL serving.

A selecting reopen latches that same family/generation/binding identity before
returning the apply claim. Settlement re-authenticates the snapshot and SQL
binding, seals the matching retention floor, installs the base idempotently,
and checkpoints the complete group. The apply fence is released only by the
opaque capability minted after the WAL logical-leaf barrier and active-family
publication. Keeping the SQL apply claim open while closing and reopening only
the WAL is supported; it receives the same exact completion.

`ValidateImmutableBaseApplyCapacity` checks the live WAL cut, retained session
and slot counts, retry-window ceiling, and epoch high-water. `AdoptRuntime`
uses this bounded-state proof directly. Applying a sealed WAL suffix no longer
consumes an append-only completion budget because Release makes session
capacity reusable.

## Implementation references

- `internal/replicatedstate/apply.go` and `machine.go`
- `internal/replicatedstate/session_codec.go` and `read.go`
- `internal/raftmember/runtime.go`, `apply.go`, and `apply_capacity.go`
- `sql/driver/replicated_store.go` and `replicated_apply.go`
- `sql/driver/replicated_child_stage.go`
- `internal/rangesplit/activate.go`
