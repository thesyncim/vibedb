# PostgreSQL access to distributed RF3 SQL

Status: opt-in, loopback-only RF3 development endpoint with distributed reads
and durable single-statement auto-commit writes. The development supervisor also
supports CREATE TABLE, including IF NOT EXISTS, by preparing independent RF3
groups in the existing three data processes and publishing their catalog entry
only after a quorum read succeeds. Other DDL, write transaction blocks,
savepoints, repeatable-read/serializable transactions, global-index lookup plans,
and repartition exchange plans are refused.

## Local use

Build `cmd/vibedb`, `cmd/vibedb-shard`, and `cmd/vibedb-gateway`, then run:

```sh
vibedb cluster dev --replicas 3 --root /absolute/path/to/dev-state --pg-listen 127.0.0.1:7432
psql 'postgresql://local@127.0.0.1:7432/vibedb?sslmode=disable'
```

The development database is `vibedb`, user `local`, with no password. GoLand
uses its PostgreSQL driver, **No auth**, and URL
`jdbc:postgresql://127.0.0.1:7432/vibedb?sslmode=disable&user=local`.
This explicitly trusted endpoint must remain on loopback; it is not production
authentication. Native gateway traffic remains mutually authenticated.

The seeded table is `documents`, with primary key `/id`. Try:

```sql
SELECT id, value FROM documents ORDER BY id;
SELECT id, documents."$doc" FROM documents;
SELECT COUNT(*) FROM documents;
INSERT INTO documents (id, value) VALUES ('example', 'hello');
UPDATE documents SET "$doc" = '{"id":"example","value":"updated"}' WHERE id = 'example';
DELETE FROM documents WHERE id = 'example';
```

For a real six-column table with declared types and constraints, see
[`multi-column-table.sql`](../examples/multi-column-table.sql), or
[`employees-1000.sql`](../examples/employees-1000.sql) for 1,000 rows in sixteen
bounded INSERT statements. Online CREATE and restart persistence are covered
by the three-node PostgreSQL process test. Extra fields in `documents` are not
declared columns, and discovery does not scan rows to infer them. Declared types
and NOT NULL constraints are enforced, but projected cells still use the
existing JSON wire type rather than claiming native PostgreSQL scalar typing.

JSON object field access accepts constant non-numeric keys:

```sql
SELECT * FROM documents WHERE "$doc"->>'city' = 'Lisbon';
SELECT id, documents."$doc"->>'city' AS city FROM documents;
SELECT * FROM documents WHERE "$doc"->>'city' IS NOT NULL;
SELECT "$doc"->'address'->>'city' AS city FROM documents;
```

`->>` returns text; missing paths and JSON null return SQL NULL. A bare text
expression is not a WHERE condition: supply a comparison or IS [NOT] NULL.
Accessors use compiled native paths, not whole-document JSON reparsing. Equality
against text that cannot represent a non-string JSON value (such as `'Lisbon'`)
lowers to the ordinary field predicate, preserving indexing and distribution.
Ambiguous values such as `'92'` retain text conversion so both JSON `92` and
JSON `"92"` match. Prepared execution is allocation-free once warmed in the
query-engine regression; network and storage still have costs. Dynamic or numeric
keys, array indexes, arbitrary JSON expressions, and further access after `->>`
are not supported.
An intermediate array is rejected at execution rather than silently using JSON
Pointer's array-index coercion. This is bounded object access, not full PostgreSQL
JSON-operator parity.

Keep GoLand's console in **Auto** transaction mode. INSERT supports whole JSON
documents or flat column/value tuples, including multiple rows. UPDATE replaces
the whole document and requires primary-key equality; DELETE requires primary-key
equality or a finite IN list. RETURNING, ON CONFLICT, arbitrary UPDATE field
assignments, and multi-statement write batches are not supported. Execute each
write independently, with its own Query message or Execute/Sync batch. This is
not a complete PostgreSQL transactional SQL implementation.

The launcher starts three replicas each for catalog, ledger, and data, plus a
gateway. This is three-way replication, not three physical data shards. The
same gateway planner supports targeted and bounded scatter reads when the
catalog contains multiple data shards. Reads go to each group's current leader;
replicas are not interchangeable load-balanced read targets.

GoLand 2026.2 synchronization supports the current database's `public` schema:
tables, declared columns, whole-document projection, keys, and exact indexes.
Use the normal PostgreSQL driver and select only `public`. Full, fragment, and
incremental-form requests are recognized; without PostgreSQL XIDs, refreshes
return bounded full snapshots. PostgreSQL-only objects remain absent.
`public.documents` and `documents` address the same table. This does not add
general PostgreSQL SQL support or lift the write restrictions above. The
console supports the documented DML; arbitrary data-grid-generated field
UPDATEs are not supported. See `integration/jdbc/README.md` for the live-driver
discovery and isolated CRUD gate.

## Ownership boundaries

The PostgreSQL listener is a protocol adapter, not a second SQL authority.
Authenticated sessions use the same catalog, distributed planner, RF3
read barriers, and authorization as native gateway clients. PostgreSQL writes
call the same native durable request-ledger service. They never mutate a replica
directly. Flat INSERT documents use the existing SQL runtime's canonical encoder.
The existing embedded PostgreSQL backend remains supported independently.

The execution path is PostgreSQL session -> gateway planner -> catalog-selected
Raft groups -> shard-local SQL execution -> bounded distributed merge. Queries
must not be routed to an arbitrary single replica or evaluated by copying an
unbounded distributed database into a gateway-local store.

## Efficiency and implementation checkpoint

Reuse the existing planner and executor; do not introduce a second SQL engine
or a second routing authority. Keep buffers and cursor storage caller-owned,
avoid per-row interface dispatch, and prove allocation claims with regressions.
Quorum communication, storage reads, and distributed merge still have real
costs: zero additional allocation is not a claim of zero total execution cost.

The backend boundary and data-only RF3 cut are implemented. Foundation tests show
that warmed embedded execution allocates four times both directly and through
the backend, while the reusable data-cut capture and materialized-row lifecycle
allocate zero times. The full pgwire and replicated-state race suites pass;
the authenticated three-voter test exercises the new quorum cut, capability
separation, stale-fence rejection, cancellation, and release/reuse.

The cut uses the live durable intent count for constant-time, conservative
group-wide refusal. The reopen-only intent map is not a live authority. SQL
executes directly on borrowed, generation-pinned snapshots. Ownership filters
run before indexed predicates, aggregates, ordering, limits, and join input
materialization. No backup capability or gateway-local database is involved.

RF3 scatter uses independent leader ReadIndex cuts, never legacy transaction
read fences. The successful gateway Result retains its deterministic per-group
observation vector. A failure returns no partial rows. These independent cuts
are **not a global MVCC snapshot**, including inside a READ COMMITTED transaction.

The listener admits at most 16 sessions. Each distributed PostgreSQL query has
at most four concurrent shard calls, 100,000 result rows, and 4 MiB aggregate
result bytes (or smaller session limits). Native SQL frames have 1 MiB request
and 4 MiB result ceilings; each shard also bounds working/intermediate/aggregate
memory and charges the shared native frame admission budget. PostgreSQL row
materialization has an additional retained-memory preflight. These finite
bounds are not a claim of allocation-free or zero-cost distributed execution.

Writes use a serialized issuer lane per table (at most 64) and checksummed,
fsynced outboxes beside the catalog-session journal. The original `.pg-writes`
outbox retains its identity and pending request; additional tables use the
`.pg-writes.tables` directory. A pending write blocks subsequent writes to its
own table, not unrelated tables. Every retained outbox is reopened and recovered
at startup, including when no client reconnects to that table.

New internal route and execution coordination sessions use narrowly authorized,
class-scoped identities. Exact release reclaims their binding and retry ring;
route-session result sidecars are reclaimed in the same atomic transition.
Ordinary client identities retain their permanent authority fences. The active
session limit is unchanged, while retained legacy bindings and active scoped
bindings have separate bounded namespaces. Regression coverage includes a full
legacy binding budget, concurrent scoped sessions, and reopen after release.
Retained legacy command bytes and execution-session journals remain legacy;
pending requests must be resolved under their original identities, never by
deleting an outbox or inventing a new request identity. This introduces new
replicated command classes: upgrade all replica binaries before enabling the
new gateway; mixed-version rolling upgrades and downgrade after new-class writes
are not supported. Released physical route-pin records still require the
existing authorized compaction protocol; session cleanup is not pin compaction.

Each outbox retains at most one request and its terminal ACK, with a 4 MiB record limit.
The issuer identity and exact SQL/parameter bytes are durable before admission.
Recovery replays the retained ledger result before replanning; terminal ACK
cleanup never executes the SQL again. Unknown outcomes return SQLSTATE 40003;
do not blindly resubmit them. The server resolves its retained request, but a
new PostgreSQL connection supplies no application retry identity. Committed
success cannot be changed into cancellation merely because a late cancel arrives.
No write outbox or recovery worker is created unless this endpoint is enabled.

## Required contracts

- Preserve Parse/Bind/Describe/Execute/Sync, cancellation, prepared-parameter
  types, portal ownership, SQLSTATE errors, and bounded result preflight.
- Isolate the embedded SQL session behind a backend boundary without adding
  per-row interface allocation or regressing warmed embedded execution.
- Derive shard placement, pruning, pushdown, ordering, aggregation, and exchange
  from the existing distributed planner. Do not accept caller-supplied plans.
- Acquire a leader ReadIndex and an immutable, generation-fenced SQL read cut
  for each participating RF3 group. Refuse intersecting unresolved transaction
  intents and enforce physical range ownership during split transitions.
- Use public data-read authority for SQL reads. Backup snapshot capabilities
  must not be reused or granted to database clients.
- Retain the actual per-group observation vector. Independent group barriers
  are not a global MVCC snapshot or a serializable distributed transaction.
- Apply aggregate result, intermediate, exchange, concurrency, and deadline
  budgets across the whole distributed query, not independently per replica.
- Publish writes only through durable sequenced request execution. Persist
  retry identity before admission, settle unknown outcomes with identical
  request bytes, and separate database commit from client delivery and ACK.
- PostgreSQL disconnect/reconnect does not itself identify an application
  retry. Never promise transparent exactly-once replay of arbitrary JDBC SQL.
- Do not report multi-statement transaction atomicity, read-your-writes, or
  isolation levels until the distributed backend actually implements them.
  Unsupported transaction modes must fail before their first mutation.
- Derive introspection from the authoritative catalog and verify JDBC/GoLand's
  actual metadata requests. Do not synthesize nonexistent columns or indexes.
- DDL must use certified schema rollout; never modify a replica's embedded
  catalog directly or silently create a separate local database.

## Qualification and delivery

Add regressions at the backend, RF3 read-cut, distributed planner/executor,
and PostgreSQL boundaries. Exercise real PostgreSQL JDBC prepared statements,
metadata discovery, supported reads/writes, cancellation, limits, and errors.
Use multiple physical data shards to verify targeted and scatter plans, global
ordering/limits, supported joins/aggregates, and refusal of unsupported shapes.
Repeat against Docker RF3 with leader loss, stale routes, split transitions,
pending intents, and lost write replies. Verify that failed distributed reads
do not leak partial rows and shutdown releases every snapshot and session.

Qualification performed in Docker includes targeted/two-shard scatter routing,
global ordering/limit and count merge, independent observation vectors, failed
shard refusal, ownership filtering, read-only transaction state, result bounds,
and cancellation. Real psql and the installed GoLand PostgreSQL JDBC 42.7.3
driver exercise prepared reads, scans, counts, co-located joins, table discovery,
and single-statement DML. Journal regressions cover exact-byte recovery after
unknown execution, retained ACK recovery without re-execution, sequence reuse
after pre-admission refusal, exclusive ownership, and corrupt-state rejection.
Local three-voter reads also succeed while each data
replica is paused in turn and resumed. Broader SQL writes, exchange, full IDE
introspection, and global transaction isolation remain explicitly unsupported.
