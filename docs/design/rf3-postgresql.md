# PostgreSQL access to distributed RF3 SQL

Status: available as an opt-in, loopback-only RF3 **read-only** development
endpoint. SQL writes, DDL, savepoints, repeatable-read/serializable transactions,
global-index lookup plans, and repartition exchange plans are refused.

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
SELECT COUNT(*) FROM documents;
```

The launcher starts three replicas each for catalog, ledger, and data, plus a
gateway. This is three-way replication, not three physical data shards. The
same gateway planner supports targeted and bounded scatter reads when the
catalog contains multiple data shards. Reads go to each group's current leader;
replicas are not interchangeable load-balanced read targets.

Automatic GoLand synchronization is disabled: table discovery is supported,
but complete PostgreSQL system-catalog/column/index introspection is not.
The exact PostgreSQL JDBC 42.7.3 public-table discovery request is covered by
a regression, alongside the existing psql catalog shims.

## Ownership boundaries

The PostgreSQL listener is a protocol adapter, not a second SQL authority.
Authenticated sessions use the same catalog, distributed planner, RF3
read barriers, and authorization as native gateway clients. Writes continue
through the native durable request-ledger API, not this PostgreSQL adapter.
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
and SQL write rejection. Local three-voter reads also succeed while each data
replica is paused in turn and resumed. Broader SQL writes, exchange, full IDE
introspection, and global transaction isolation remain explicitly unsupported.
