# Raft WAL

**Status:** current non-serving static-base Raft WAL.

This design defines the first disk-backed implementation of
`raftmodel.StableStore`. It provides bounded recovery, exact Ready retry,
single-writer fencing, encryption at rest, and crash-ordered publication for
one Raft member. It is not a serving or high-availability milestone.

The current WAL deliberately does not provide Raft transport, Multi-Raft
scheduling, shard allocation, serving fences, adaptive splitting, distributed
query routing, log compaction, key rotation, or a snapshot newer than the
static bootstrap snapshot. A file eventually reaches `ErrFull` and must not
accept more durable Raft work. This document defines the only WAL contract
accepted by the unreleased repository.

## Qualified platforms

Runtime durability is qualified on Linux and macOS only. Windows and other
targets retain compile gates, but `Create` and `Open` return
`ErrPlatformUnsupported` before opening, creating, linking, truncating, or
otherwise mutating a WAL path. In particular, this contract does not claim
that flushing an ordinary read-only Windows directory handle durably publishes
a hard link on every supported filesystem.

Linux creates and repairs the physical reservation with `fallocate(2)` mode 0.
macOS creates it with `F_PREALLOCATE/F_ALLOCATEALL` from physical EOF and then
sets the logical length. On reopen, macOS does not repeat that EOF-relative
allocation request: it verifies that `st_blocks * 512` covers the sealed
logical capacity and fails closed if the file is sparse or hole-punched. Open
then syncs the file and re-proves its namespace identity before returning.

Copy and restore tooling must preserve physical allocation. A sparse or
hole-punched Linux copy is repaired before Open returns; failure to restore the
reservation, including ENOSPC, fails Open. A sparse macOS copy is rejected and
must be restored by qualified operator tooling.

## Construction and identity

`Create` takes:

- an immutable `Identity`;
- one explicit AES key ID, exactly 32 bytes of master key material, and opaque
  wrapped-key provider metadata;
- the static bootstrap snapshot and its topology recovery epoch; and
- hard physical and recovery bounds.

The immutable identity contains cluster ID and incarnation, distribution and
shard names, allocation generation, shard incarnation, Raft group ID, member
ID, and store ID. Every component is nonempty or nonzero as appropriate and is
authenticated in the static header. `Open` requires exact equality. Strings
and byte slices retained or returned by the Store are cloned.

The topology recovery epoch is mutable committed topology state, not immutable
member identity. `Open` therefore requires an independently trusted, nonzero
expected epoch and compares it exactly with the authenticated bootstrap record
before returning a handle. A lower or higher epoch fails closed. A post-open
getter is diagnostic only and is not the topology fence.

The bootstrap snapshot is fixed at index 1, term 1. Its ConfState is
static, bounded to 64 voters and learners, and preserves protobuf presence for
`AutoLeave`. Unknown protobuf fields on Snapshot, SnapshotMetadata, ConfState,
HardState, and Entry are rejected rather than silently discarded by the strict
current codec.

## Physical layout

All integers are little-endian. Fixed headers and records use a 4 KiB damage
granule.

| Offset | Length | Contents |
| ---: | ---: | --- |
| 0 | 4096 | Static authenticated header |
| 4096 | 4096 | Alternating current slot 0 |
| 8192 | 4096 | Alternating current slot 1 |
| 12288 | variable, 4096-aligned | Bootstrap record followed by Ready records |
| selected WAL end | remaining fixed capacity | Unselected/preallocated tail |

The file has one sealed, fixed logical size and is physically reserved at
Create. There is no truncate, rename, generation compaction, or reuse of
selected record space.

### Static header

The plaintext prefix contains the magic, codec and cipher-suite versions,
geometry, explicit key ID and wrapped-metadata lengths, a deterministic header
nonce, and a randomly minted nonzero 128-bit file ID. Key ID and wrapped
metadata are plaintext locators but are authenticated as GCM associated data.
The encrypted body contains the immutable identity, bootstrap snapshot
reference, and exact sealed bounds. Zero padding and a Castagnoli CRC plus its
complement cover the full 4 KiB image.

The bootstrap reference contains a random nonzero snapshot ID, exact encoded
size, SHA-256 digest, index, and term. Snapshot bytes are not embedded in the
static header; the reference selects the authenticated bootstrap record.

### Current slots

Exactly one current slot selects the durable logical image. The encrypted body
contains:

- monotonically increasing slot generation;
- selected WAL end, record sequence, and record-chain digest;
- durable boot incarnation;
- HardState and live-log first/last bounds;
- the final selected Ready key and encrypted semantic digest, when present;
- bootstrap snapshot ID, index, term, size, chunk count, and digest; and
- topology recovery epoch.

Generation parity fixes the slot position. Each publication overwrites the
other 4 KiB slot. The plaintext prefix contains fixed geometry, generation,
file ID, key ID, an object tag, and a derived nonce. The full slot has zero
padding and a Castagnoli CRC/complement.

### WAL records

Record 1 is the bootstrap record and starts the chain from the static-header
SHA-256 digest. Every later record is one nonempty durable Ready. Each record
prefix contains kind, flags, padded/plain/cipher lengths, sequence,
NodeIncarnation, ReadyID, previous-chain digest, file ID, object tag, nonce, and
key ID. Ciphertext is followed by authenticated zero padding and a full-record
CRC/complement. The next chain value is SHA-256 over the complete aligned
record, including padding and checksum.

Every record starts and ends on a 4 KiB boundary. This prevents a torn later
append from sharing a damage granule with an acknowledged prior record.

## Encryption and object identity

The WAL uses AES-256-GCM. The caller's master material is never used directly
as a GCM key. Domain-separated HMAC-SHA-256 derives per-file data and nonce keys
from the authenticated file ID.

Static-header nonce derivation is unique within a freshly minted file. Current
slots and records need a stronger rollback-safe construction because a torn,
unselected attempt can be discarded and its logical generation or sequence can
later be reused. Their public object tag is a keyed, domain-separated HMAC over
the object number, every variable envelope field, file context, and plaintext.
It is not a raw plaintext hash and therefore is not an offline dictionary oracle
for low-entropy HardState. A per-object AES key and nonce are derived from the
tag and logical number. Exact retry is byte-identical; changing payload,
generation/sequence, slot, kind, flags, lengths, incarnation, Ready ID, or
previous chain changes the tag, key, nonce, and ciphertext.

Raw semantic Ready and bootstrap SHA-256 digests remain inside authenticated
ciphertext. Cryptographic outputs may legitimately be all zero; no digest,
tag, nonce, or chain field uses zero as a format sentinel. Randomly minted
structural IDs do reserve zero and a random source that returns all zero bytes
is rejected after one bounded read.

There is no online key rotation. A wrong key ID, wrapped-metadata expectation,
or master key fails closed.

## Bounds and admission

The static header seals exact values for maximum file bytes, aligned record
bytes, selected record count, live entries, and live entry footprint. Open must
supply the same values.

Public options cannot be smaller than the fixed `raftmodel` Ready envelope:

- at least `raftmodel.MaxMessageEntries` (4096) entries;
- live bytes for `raftmodel.MaxUncommittedEntriesSize` (64 MiB) plus 32 bytes
  of accounting per entry;
- an aligned record large enough for that data, all 4096 entry headers, the
  Ready header, maximum key ID, GCM tag, record prefix, and checksum; and
- at least one bootstrap record plus one Ready record.

Normalization also proves:

`header + maximum bootstrap envelope + maximum live bytes + maximum Ready record <= file capacity`

This leaves one admitted worst-case Ready beyond the maximum live-log image.
`ReserveReady` is a capacity-admission check before accepting proposals or
inbound append work that may create a durable Ready. It conservatively requires
one maximum aligned physical record, one remaining record slot, 4096 remaining
live-entry slots, and the maximum Ready live-byte footprint. The driver must
still capture and drain work already exposed by RawNode. A tiny durable batch
may fit after worst-case reservation closes, and message-only, heartbeat, read,
or otherwise durability-empty Ready batches remain accepted at physical,
record-count, and logical-live limits.

The WAL also exposes a detached `CapacityProfile`: exact
`CapacityFormatStatic`, authenticated log base index, and sealed `MaxEntries`.
Runtime snapshot and compaction are unsupported, and qualification rejects any
non-static profile. `raftmember.ValidateStaticNoGCCompletionCapacity`
uses the profile for one finite, count-only state-machine proof. It first reads
the locked apply claim's exact SQL/WAL binding and authority, derives the live
WAL binding from that authority, and rejects any coordinate mismatch. The
claim must be healthy and initialized. With retained completion count `C`,
applied index `A`, committed index `H`, and last log index `L`, qualification
checks `C <= A-1`, `1 <= A <= H <= L`, and `L-1 <= MaxEntries`.

The claim also reports its exact apply format. Only the current apply grammar,
which creates at most one completion per entry, qualifies; any other identifier
is rejected. Consequently:

`C + (L-A) <= L-1 <= MaxEntries <= MaxCompletions`

Every normal entry creates at most one retained completion, so the inequality
covers the entire existing and future admitted suffix without a dynamic
per-entry ledger. The implementation also checks `C+(L-A) <= MaxCompletions`
with overflow-safe subtraction. Qualification is an instantaneous predicate
under caller-exclusive startup ownership, not a lease or reservation token. It
must be repeated for every reopened exact WAL/apply pair and is invalid for any
capacity format other than `CapacityFormatStatic`.

This proof does not replace proposal ordering. `raftmember.Runtime` now
constructs and exclusively retains the exact WAL, apply claim, SQL database,
and `raftmodel.Node`. It performs application `AdmitCommand`, then
`ReserveReady`, then `Node.Propose` under the Node's single-scheduler contract.
The runtime remains non-serving: a nil proposal error means local core
admission only, not leadership, commit, apply, or a client result.

`internal/multiraft.Host` multiplexes a bounded set of those runtimes. Each
range/shard is one independent Raft group; a one-range deployment is simply one
runtime in the same Host path. Host retains no goroutine or wall-clock timer per
group. After its initial adoption probe, a quiescent group leaves the runnable
queue and consumes no scheduler-scan CPU while it has no message, proposal,
explicit logical tick, campaign, or unfinished Ready work. The group still
retains its ordinary runtime, memory, and WAL state. A dormant-group removal
path closes and releases a retired range before reusing its bounded Host slot;
topology authorization and generation fencing remain outside this kernel. A
future production cadence must still deliver logical ticks efficiently (for
example with a timer wheel); this kernel does not make election/heartbeat work
free.

Nor is the inequality a physical storage reservation. It closes the logical
retained-completion-count bound only. The SQL system/user files still have
per-mutation and transaction bounds but no aggregate persistent-byte reserve;
that remains a separate serving prerequisite.

Empty Ready batches still perform read-only namespace fencing, but issue zero
WAL writes and zero syncs. `MustSync` does not change that property when there
is no Snapshot, Entry, or HardState to persist.

The default maximum record is 80 MiB; the public absolute cap is 96 MiB.
Recovery reads and decrypts one record at a time, so 96 MiB is the true
worst-case single-record read/decrypt amplification. Large-batch chunking under
one atomic current-slot commit is unsupported.

## Incarnations and Ready order

`BeginIncarnation` is mandatory once per newly created or opened handle and
must run before construction of `raftmodel.Node`. It durably increments a
never-reused boot counter in a current-slot publication. Callers must not invent
or reuse an incarnation reported by `CurrentIncarnation`.

Within one begun handle, the first Ready ID is 1 and each new Ready is exactly
the prior ID plus one, including durability-empty Ready batches. Reusing the
latest accepted key is allowed only for an exact canonical retry. Regressions,
gaps, or changed bytes fail closed. Retry-key binding begins after an in-order
batch passes the bounded canonical validation; rejected malformed or
unsupported input is not retained as attacker-controlled retry state.

No Open resumes an old process's in-memory Ready sequence. Open resolves the
selected image, then `BeginIncarnation` publishes a fresh incarnation and the
new Node starts at Ready ID 1.

## Nonempty Persist protocol

For one canonical nonempty Ready, Persist performs:

1. Validate identity, strict Ready order, protobuf canonicality, entry/log term
   monotonicity, committed-prefix safety, HardState, live bounds, and actual
   encoded record fit without mutating the live image.
2. Bind the attempted canonical Ready key and digest for exact retry.
3. Re-prove the canonical live parent directory and exact locked regular WAL
   inode.
4. Positional-write the complete aligned record at the selected WAL end.
5. Sync the WAL, making the record an available but not yet selected object.
6. Build the exact alternate current-slot bytes that select that record and
   bind the resulting HardState/log/snapshot/retry image.
7. Positional-write the complete alternate slot and sync the WAL again.
8. Re-prove parent path, leaf, descriptor, size, and writer namespace.
9. Only then apply the prevalidated delta to the single-owner in-memory image.

The record barrier precedes the selecting-slot barrier. Consequently, recovery
can see an ignored orphan record or a current slot whose complete dependencies
are durable, never an authenticated selector for an unsynced record under the
qualified filesystem contract.

Suffix replacement is amortized over the changed suffix, not the entire live
log. Overlap at the same index and term must have identical semantic Entry
fields. Committed entries cannot change. Entry terms cannot decrease within a
batch or across the retained-prefix/snapshot boundary. Terms equal to
`MaxUint64` and local pseudo-target votes are rejected. Truncated backing-array
pointers are cleared so removed entry data is not retained.

## Error classification and retry settlement

Validation, capacity, namespace, and record-stage failures are definite with
respect to the selected image: the previous current slot still selects the
durable image. Once canonical validation has bound a Ready key, only the exact
batch may retry that key, including after a partial record write or record-sync
error.

On the first settlement attempt, a namespace-proof or slot-read error before
any current publication is definite. An error during current-slot write,
current-slot sync, or the post-publication namespace proof has an unknown
outcome. The handle retains one bounded pending transition containing the exact
current-slot bytes, next current state, and prevalidated image delta. While
pending, reads and conflicting mutation fail with `ErrPersistenceUnknown`.

An exact same-handle retry reads the target slot. If it already equals the
pending bytes, retry only syncs and re-proves it. If it is partial or different,
retry rewrites the same exact slot bytes, syncs, and re-proves it. It does not
rewrite the already-synced record. Successful settlement commits the in-memory
delta once. `BeginIncarnation` uses the same pending-settlement rule. A
namespace-identity mismatch permanently poisons that handle even when its
publication outcome is unknown; it must be closed and reopened rather than
trusted after the path appears to be restored.

Closing and reopening is the alternative settlement path. Recovery chooses the
authenticated current image, ignores unselected record bytes, and then requires
a fresh `BeginIncarnation`. It cannot resume the old Ready key.

## Recovery and corruption policy

Open holds the writer lease while it:

1. authenticates the static header, immutable identity, key, and exact bounds;
2. decodes both current slots and selects the newest adjacent authenticated
   generation;
3. scans exactly the selected WAL range from the bootstrap record;
4. verifies record geometry, CRC, GCM, padding, sequence, and digest chain;
5. reconstructs the HardState and log under the same live bounds;
6. compares the reconstructed image, bootstrap reference, final Ready digest,
   incarnation, and topology epoch with the selected current slot;
7. verifies or repairs physical allocation, syncs, and re-proves the live
   namespace.

Interior corruption, valid-CRC envelope or GCM failure, wrong key or identity,
record gaps, non-adjacent valid slot generations, impossible log transitions,
and selected-record corruption fail closed. Bytes after the selected WAL end
are ignored because the fixed tail may contain an orphan record or older
preallocated contents. The WAL never truncates that tail.

An all-zero current slot is absent. A checksum-invalid slot is the only explicit
torn classification. If the other slot is authenticated, recovery may fall
back to it; an all-zero inactive slot after generation 1 is treated the same way
because a torn sector or page may read back as zero. Once a slot's CRC is valid,
every envelope, geometry, key, AEAD, payload, and semantic error is fatal and
cannot trigger rollback.

This rule has a deliberate but important limit: without an external monotonic
witness, this contract cannot distinguish an in-flight torn write from post-ack bitrot or
tampering that makes the newest slot checksum-invalid or all zero. Such damage
can roll recovery back to the older authenticated term, vote, log, and retry
state. Whole-file rollback is likewise undetectable. Therefore this WAL does not
claim general authenticated anti-rollback and its safety contract excludes
post-ack media corruption, malicious storage, and whole-file rollback.

`RecoveredTornCurrentSlot` must be emitted as high-severity telemetry. The
member should remain quarantined from serving and topology rejoin until an
operator or later repair protocol validates the authoritative replica state.
External generation witnesses, authenticated generation manifests, or a later
generation-compaction protocol are required to close this limitation.

## Writer and namespace fencing

The writer lease is held on the actual WAL file descriptor using an OS file
lock plus same-process duplicate-descriptor tracking. Hard-link aliases contend
on the same inode.

OS inode locks are advisory, and namespace proofs observe boundaries rather
than continuously mediating the filesystem. Every process that can write a WAL
inode must cooperate through `storeio.LockWriter`. The WAL directory, host
administrator, and local storage stack are trusted not to mutate the inode or
namespace between proofs. The WAL does not defend against a malicious or
non-cooperating local writer or administrator.

The Store pins the parent directory and retains its canonical live path and
FileInfo. Before every mutation, before every durability-empty Ready, and after
each current-slot sync, it reopens the live parent and proves:

- the live parent is the pinned directory inode;
- the named leaf is a non-symlink regular file;
- the leaf is the exact locked descriptor inode; and
- logical size equals the sealed capacity.

Parent rename/rebind, leaf unlink/replacement, symlink substitution, or a
foreign same-size hard link poisons the handle. A pre-publication namespace
failure is definite; a post-current-publication mismatch has an unknown
durability outcome but permanently poisons the handle, so Close/Open is the
only settlement path.

Create constructs, preallocates, writes, and syncs a uniquely named temporary
WAL while holding its descriptor lock. It publishes with an atomic hard link to
the absent final name, which cannot replace another creator's file, then syncs
the parent directory. Competing creators yield exactly one official locked
inode. A crash leaves either no final name, a fully recoverable final name, or a
safe unselected temporary construction name. Unexpected publication-settlement
errors are unknown unless absence or a foreign existing leaf is proved.

## Validation gates

The qualification seal includes deterministic byte goldens for static header, bootstrap
record, and current slot; every current-slot short-write prefix; CRC-valid
authentication corruption; full Open-level orphan/torn/full-selector crash
cuts; record and slot write/sync faults; same-handle and close/reopen retry
settlement; corruption and chain tests; topology and namespace fences;
concurrent and cross-process writer contention; sparse-allocation behavior;
differential checks against `raft.MemoryStorage`; an actual `raftmodel.Node`
restart; protobuf presence and unknown-field cases; finite-bound and allocation
tests; fuzz decoders; race, vet, checkptr, cross-compilation, and focused
benchmarks.

Passing those gates establishes only this durable-storage contract. It does not
authorize serving, HA, failover, Serializable isolation, Read Committed
isolation, lease reads, or wall-clock-based correctness claims.
