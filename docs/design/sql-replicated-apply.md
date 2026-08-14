# SQL replicated apply

This is the current trusted, unserved local apply boundary.

This boundary connects an exact SQL replicated binding to the durable replicated
state machine without reopening any public SQL write path. One opaque
`ReplicatedApply` claim implements `raftmodel.StateMachine`; its collections,
transaction log, and underlying machine remain private to the driver.

It grants no proposal authority, leadership,
lease, peer authentication, `ReadIndex`, replicated position token, runtime
snapshot/compaction, or physical completion/system/user byte reservation. A
separate static-WAL qualification can prove finite logical completion-count
headroom only after exact binding, claim health, completion count, applied cut,
WAL commit, and last-index checks. The result is an instantaneous predicate,
not a lease, and cannot advertise replicated Read Committed or Serializable
writes.

## Catalog and storage ownership

The strict optional `replicated_apply` catalog member is present only beside an
exact `replicated_shard_store` binding. Its sole current SQL catalog grammar
uses numeric `0` in every format sentinel; every other grammar is rejected. It
freezes:

- its random hidden storage identity;
- the deterministic validation profile and SHA-256 profile digest;
- the required exact one-column placement profile, current native tuple and
  mapper identifiers, and half-open target key range;
- the exact hidden collection key, document, batch-document, and batch-byte
  limits;
- the retained completion maximum;
- every `durable.TxnLimits` dimension; and
- an exact sealed system recovery-journal record region of `655,872` bytes.

The hidden synchronous schema-free/index-free collection lives in the same
private storage directory and uses the same transaction log and recovery
decisions as the sole user table. It is not a SQL relation and never enters the
layout epoch. Open protects its data and journal from orphan cleanup, opens it
with the catalog's exact sealed-journal option before transaction recovery,
proves both participants belong to the same physical transaction directory,
and closes it before the transaction log. The base binding already owns the
exact `1,048,576`-byte transaction-marker record region; activation does not
replace or resize it. That region holds 2,048 current 512-byte,
two-participant decisions.

The system journal also has two 512-byte headers, so its complete file is
`656,896` bytes. Like the `16,794,624`-byte user journal and
`1,048,576`-byte marker record regions, this is a sidecar-only allocation
identity. It makes no main-file capacity, Raft reservation, or serving claim.

First activation requires an already-bound empty user table and no live SQL
sessions or existing apply claim. It publishes and fences the hidden file and
journal before publishing the catalog descriptor. A definite catalog failure
removes or retains the unpublished candidate for bounded cleanup. A published
or outcome-unknown failure retains the proposed apply identity for exact retry.
An incomplete collection close transfers both handle and path to the database's
retirement owner; no failure path drops a live writer or unlinks its inode.

Creating or mutably opening any of these sealed sidecars requires Linux strict
allocation of the complete record region plus its `1,024` header bytes. There
is no truncate fallback and no in-place conversion from an ordinary sidecar.

## Exact open and settlement

The base SQL identity remains a separate fence. An activated root refuses
base-only replicated open. Ordinary restart supplies both the retained base
identity and the complete retained apply identity; both are compared before
namespace or transaction recovery.

A process can die after activation's catalog rename and before receiving the
random hidden storage identity. The dedicated settlement open takes the exact
base identity and intended completion/transaction/placement profile. It
compares those and the catalog-owned system-sidecar profile before recovery,
passes the exact user, system, and marker options to durable open, then returns
the catalog's complete apply identity for durable retention. A profile-only
normal open, a storage-ID-only settlement, and an ordinary-option fallback are
not available.

Retained identities reject independently prepared roots, but cannot distinguish
a byte-identical copy of either the SQL root or WAL root. Revocation and serving
therefore still require an external witness or lease.

## Deterministic SQL mutation profile

The single deterministic profile digest binds its numeric-zero format sentinel,
logical user-table name, canonical primary JSON pointer, ordered-key grammar,
all four user mutation limits, and a required strict placement profile. The
placement profile admits exactly one shard-key pointer, requires it to equal
the table's sole primary pointer, requires the current tuple and native-mapper
identifiers, and stores one exact half-open `KeyRange`. Its
domain-separated validation digest also binds `Distribution`, `Shard`,
`AllocationGeneration`, `RoutingVersion`, `RouteGeneration`, and every
placement field. It excludes
local filenames, SQL `LogID`, WAL `StoreID`, member identity, and other
member-local coordinates so the logical digest remains portable.
`SchemaGeneration` is currently the external assertion that every replica was
provisioned with the same profile; `Command` does not carry the digest, so
this slice makes no cryptographic peer-attestation claim.

Planning first performs completion dedupe/conflict and
stale/unknown-collection classification. It then collapses repeated keys to
their final ordinal effect, checks ordinary collection/JSON bounds, reads the
current row, invokes the SQL validator, and only then elides no-ops. Open runs
the same JSON and put validation over every existing user row before accepting
its logical digest.

A put is valid only when the configured primary pointer resolves to a non-null
Number or String whose canonical ascending ordered-key encoding exactly
equals the mutation key. A delete key must decode as exactly one complete
ascending non-null scalar component. An absent canonical delete is a valid
no-op. For a present delete, deriving the primary key from the current document
must reproduce the same bytes. Primary mismatch or invalid shape produces a
durable `ResultInvalidDocument`; an over-bound derived key produces
`ResultTargetBound`. Neither semantic refusal wedges ordered apply.

After primary/document agreement, a put maps that scalar and must fall inside
the exact target range. A present delete routes the current document; an absent
delete decodes and routes its sole ordered-key scalar. The range start is
inclusive and its concrete end is exclusive. An otherwise valid mutation
outside the range produces durable `ResultWrongShard`. The one current result
grammar uses codes 1 through 6, including wrong-shard code 6, and retained
completions are checked against the machine's exact profile on open, lookup,
and dedupe.

Open and coherent snapshot digesting re-run placement validation over every
extant row. A copied, corrupt, or out-of-band row that belongs to another shard
therefore fails closed even when no new mutation mentions it.

## Publication and SQL isolation fence

The wrapper holds `database.mu` across every machine mutation. A user-changing
entry publishes the user collection, exact completion, and state record through
one `durable.UpdateCollections` decision. The synchronous mutation-attempt hook
runs before apply returns while the SQL lock is still held. The driver advances
the table conflict clock when the user generation changed or the collection has
a sticky persistence error, conservatively covering unknown outcomes before a
future transaction can validate.

The claim holds one connector lifetime reference. Acquisition requires
`refs == 0`; later read-only sessions may coexist, but all public DML, DDL,
`RETURNING`, materialization, index, and dirty-commit surfaces remain fenced.
Closing the claim releases only its singleton capability and connector
reference. It never unbinds the root or removes the hidden participant.

This profile proves document/primary agreement and deterministic placement for
the current `Command` shape only: one primary key that is also the one shard
key, one native mapper, and one exact target range. Composite primary or shard
keys, placement migration, and an end-to-end proof that a serving router chose
the same fenced range are outside the current command/routing contract.

Local reads are still local reads. This boundary provides no leader authority,
`ReadIndex`, or exact coherent state-machine snapshot for a strong distributed
read; pairing `Published()` with an independently captured SQL snapshot is
forbidden.

## Serving exclusions

The static no-compaction WAL check is only an instantaneous logical-count
predicate. The current boundary does not reserve collection main-file bytes,
committed in-flight user/state/completion bytes, Raft log space, snapshots, or
ranges. It also provides no completion garbage collection, compacted-WAL suffix
ledger, runtime snapshot export/install, authenticated peer transport, dynamic
membership, leadership-aware routing, shared replicated position, retry result
grammar, composite-key routing proof, `ReadIndex`-gated coherent read, SQL
predicate/read/range dependency grammar, or cross-shard coordination. None of
the current apply or sidecar identities grants serving authority or an SQL
isolation guarantee.
