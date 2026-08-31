# Distributed transaction protocol

`gateway.Executor.ExecBatch` runs a byte-bounded atomic write across shards
and tables. The gateway proves every statement's owner before it sends
transaction traffic. A participant is one exact shard target, not one row or
table. Mutations routed to the same fenced target share a participant.

The executor has two mutation-transaction authorities. A batch whose every
table has replicated metadata and whose supported mutations resolve to one or
more RF3 groups uses the fused Raft path. A batch over static tables uses the
static shard journal described below. Mixed static/RF3 authority is refused,
and an unsupported RF3 mutation shape does not fall back after classification.
Complete validation happens before either authority receives I/O. This choice
is independent of the general `SELECT` transport, which is also RF3-backed in
replicated-catalog mode.

## Bounds

- Participant count: no semantic count limit
- Inline coordinator fast path: at most 64 participants and 32 KiB
- Segmented coordinator manifest: 64 KiB pages and at most 64 MiB encoded
- Active segmented pages per shard journal: 512 MiB
- Retained transaction journal per shard: 8 GiB, including control reserve
- Maximum participant mutation payload: 16 MiB
- Maximum participant intent scopes: 256
- Transaction ID: random nonzero 128-bit value

The gateway sorts participants by distribution and shard. The first
participant is the coordinator. Small transactions retain the compact `VTC1`
record. Wider transactions use prefix-compressed `VTM1` pages bound by a fixed
`VTCM` descriptor. The begin request carries `VTCM` and page zero together, so
the shard proves the coordinator's routing, allocation, and ownership fences
before writing either record. Later pages are ordered, independently
checksummed, and authenticated by the descriptor root before commit.

Admission remains finite without turning an encoding convenience into a
product limit. Operation profiles cap total mutation count, canonical mutation
bytes, deadline, and in-flight shard requests. Staging and recovery retain one
manifest page plus a worker-sized result window instead of the aggregate page
set or one response per participant. A shard journal reserves the exact bytes
needed to decide and retire every admitted local record. Retiring a segmented
coordinator immediately drops its resident page set, including during replay.
The live journal appends within its current generation and keeps the finite
8 GiB admission ceiling. Terminal responses coalesce bounded background
compaction checks. When a check recommends compaction, the shard rewrites the
current authoritative state into one canonical generation and atomically
installs it. Active coordinator manifests and participant mutation stages
remain intact; retired manifest pages and superseded transitions are reclaimed,
while terminal records retain their byte-exact stage for delayed retries and
lookups.

## RF3 SQL lowering

The public RF3 lane deliberately accepts only statement shapes with one exact,
byte-native meaning:

- one or more insert rows, either complete documents or unique top-level
  named-column scalar rows encoded as canonical runtime documents, each lowered
  to insert-if-absent
- one exact-primary-key whole-document update, lowered to replace-if-present
- one exact-primary-key delete or one finite primary-key `IN` set

The update document must preserve the placement key. Returning clauses,
`INSERT ... SELECT`, conflict clauses, residual predicates, ordering, limits,
repeated relation keys, invalid/duplicate flat columns, and mixed static/RF3
tables are refused before execution. Nested/duplicate named columns or a row
missing its placement-key column are also refused. A multi-row document or
named-column INSERT may route rows to different RF3 groups. Multiple statements
and tables in one group become sorted numeric relation batches on the same
participant and commit atomically. Ordered keys and document bytes enter Raft.
SQL and table-name strings do not.

Mutation count, encoded bytes, relation-batch size, and intent-scope count are
independently bounded before admission. These resource limits are not a
participant-count contract: a participant can contain several relation
batches, and the segmented coordinator streams the admitted group set.

Insert and update use replicated conditional mutation kinds rather than a
gateway pre-read. This keeps the affected-row result and conflict decision in
the same deterministic apply. The state machine returns one fixed-width
affected-row result, stores it with the session result, and reproduces it on an
exact retry.

The authenticated client durably owns one nonzero installation ID. It opens
epoch 1 and a fixed lane ordinal through `issuer_open`. Replicated catalog and
request-ledger authority return a grant digest. Each write carries the exact
grant reference, a strictly monotonic lane sequence, and a nonzero 128-bit
request ID distinct from the generated transaction ID. The transport supplies
the principal and tenant.

One fused home-group transition advances the contiguous issuer high-water and
creates the request head. This transition rejects sequence gaps, rewinds,
foreign principals, forged grants, and a request ID reused with different
bytes. The request digest is independent of catalog generation, routes,
lowering, and mutation placement. It hashes the exact ordered statements,
operation class, parameter kinds, boolean values, and parameter bytes.

The ledger streams the sealed program, pending waves, and terminal result under
bounded capacities. A committed response carries an opaque authenticated ACK
capability. `ack_exec_batch` verifies the exact request, terminal revision,
result digest, and token before it advances bounded collection. Exact ACK retry
resumes after a lost response. A completed ACK retry performs no new write.
The command has no unsequenced or process-local RF3 fallback.

## State machines

The coordinator states are:

```text
Staging -> Committed -> Retired
       \-> Aborted  -> Retired
```

The participant states are:

```text
Staged -> Prepared -> Applied -> Released
   |          |
   +----------+-> Aborted -> Released
```

An exact retry of a state transition is idempotent. A transition also checks
the expected revision. Recovery can create an `Aborted` tombstone for a
missing participant at the initial revision.

## Commit sequence

1. Stage the mutation at each participant.
2. Prepare each participant.
3. Commit the coordinator record.
4. Apply the mutation at each participant.
5. Release participant state.
6. Retire the coordinator record.

The durable coordinator commit is the commit point. Apply and cleanup can
finish later.

### SQL-atomic apply marker

A SQL participant publishes its user-table mutation and one hidden applied
marker through the same database transaction log. The marker is keyed by the
raw 16-byte transaction ID and retains only the applied revision and affected
row count. The larger staged mutation remains in the participant journal.

Before publication, an existing marker is read under the catalog write lock.
An exact revision match returns the retained affected-row count and discards
the retry's SQL mutation. A different revision or malformed marker reports a
distributed transaction conflict, so neither retry path republishes user
data.

The hidden collection uses raw opaque values. Its compact binary codec is not
JSON. The sole codec sentinel admits only the current grammar. Stale development
text fails closed without an alternate compatibility decoder or migration
ladder. `docs/format.md` records the exact envelope and storage bounds.

Participant staging acquires scoped durable barriers. An overlapping
participant fails fast. This behavior prevents a cross-shard deadlock.
Disjoint bucket scopes can proceed concurrently.

## Failure results

`TransactionOutcomeUnknownError` means that the gateway cannot resolve the
coordinator commit. Keep the transaction ID. Do not run the SQL again as a new
transaction.

`CommittedTransactionError` means that the commit is durable, but later apply
or cleanup work did not finish. Recover the same transaction ID.

A journal sync failure poisons the journal handle. A torn final append can be
truncated during recovery. Corruption before the tail fails closed.

The public durable RF3 response carries `transaction_id`, `committed:true`, and
the complete ACK handle after a durable decision. If the connection closes or
the operation reports an error after admission, the client must retry the exact
batch with the same request ID, grant reference, and issuer sequence. It must
not invent a new identity or skip the sequence.

## Recovery

`RecoverTransaction` scans current catalog shards and requires exactly one
coordinator. It replays committed work idempotently. Segmented recovery reads
and root-verifies one page at a time against one pinned catalog route index. It
can abort an incomplete coordinator or participant set after the recovery
pulse sequence reaches its fixed bound without allocating in proportion to the
aggregate manifest. Wall time can schedule a recovery pass. It cannot satisfy
the recovery authority.

`RecoverAll` scans non-retired coordinators on all current shards. The shipped
gateway calls it every five seconds.

The RF3 request service stores the exact recovery handle in the request-ledger
RF3 group. It stores request identity, streamed logical plan, pending waves,
terminal result, ACK state, issuer lane, and contiguous issuer high-water. Each
transaction wave is fenced by one logical execution-pin epoch.

Request replay occurs before new SQL planning. A terminal or pending entry
therefore survives gateway replacement and does not replan against a newer
catalog generation, split, or move. The replacement gateway uses leader-only
transaction recovery reads to resume the sealed program or return the retained
terminal result. A plain pre-admission failure that created no request head can
be retried at the same sequence. A gap or a changed exact retry fails closed.

`runServe` constructs the durable service and refuses to start without its
catalog topology, RF3 ledger client, execution-pin journals, replicated issuer
authority, terminal authority, and shared ACK key. It installs no process-local
request registry. After explicit authenticated ACK, the collector advances
only contiguous GC-complete issuer sequences.

Recovery matches the routing version, allocation generation, and ownership
epoch that the transaction recorded. The implementation does not prove
recovery across arbitrary later resharding or retired topology history.

## Global-index use

The static transaction path expands base-table and global-index mutations into
the same protocol. For update and delete, the base shard captures the exact key
set and canonical before/after images without publishing. A precondition guards
the selected keys and before-image digests; after local SQL runs, a second digest
check proves the actual postimages before any index participant applies. Each
final static index participant releases all old claims before replacement puts,
including across an atomic statement batch.

The replicated state machine can also atomically apply base and global-index
relation batches. The public RF3 SQL lowering routes ready unique and non-unique
indexes as independent relation participants. Same-key replacement and index
removal use exact prior-value checks. RF3 direct and computed declared-column
updates read one linearizable old row, retain the canonical evaluated postimage
and old-value CAS in the logical program, and never re-evaluate expressions
during transaction recovery. RF3 `RETURNING` remains separately fenced.

The RF3 read side has two contracts. `read_batch` supports multi-table and
multi-group exact-primary-key `SELECT *` and returns one route/applied-index
observation per group. General `query` in replicated-catalog mode retains the
ordinary SQL planner and bounded merge path over leader `ReadIndex` reads;
targeted/scatter reads, projections, global order/limit, and mergeable
aggregates are supported, while global-index read and repartition-exchange plans
are refused. The general-query response exposes no observation vector. Neither
contract provides one global MVCC timestamp or a historical-read API.

## Implementation references

- `gateway/transaction.go`
- `gateway/recovery.go`
- `gateway/transaction_manifest.go`
- `gateway/replicated_sql_transaction.go`
- `gateway/replicated_query.go`
- `gateway/durable_sql_request_executor.go`
- `gateway/replicated_request_service.go`
- `gateway/replicated_request_ledger_catalog.go`
- `gateway/replicated_request_issuer_collector.go`
- `gateway/replicated_transaction_protocol.go`
- `gateway/replicated_transaction_recovery.go`
- `gateway/writer.go`
- `cmd/vibedb-gateway/durable_request_runtime.go`
- `cmd/vibedb-gateway/durable_exec_batch_wire.go`
- `cmd/vibedb-gateway/issuer_open_wire.go`
- `cmd/vibedb-gateway/exec_batch_ack_wire.go`
- `internal/distributedtxn/codec.go`
- `internal/distributedtxn/manifest.go`
- `internal/distributedtxn/journal.go` and `journal_compact.go`
- `shardservice/mutation_batch.go` and `journal_compactor.go`
