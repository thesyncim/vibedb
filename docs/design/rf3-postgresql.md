# PostgreSQL access to distributed RF3 SQL

Status: implementation in progress; not yet an available endpoint.

## Ownership boundaries

The PostgreSQL listener is a protocol adapter, not a second SQL authority.
Authenticated sessions must use the same catalog, distributed planner, RF3
read barriers, request ledger, and authorization as native gateway clients.
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

The backend boundary and data-only RF3 cut are implemented. Docker tests show
that warmed embedded execution allocates four times both directly and through
the backend, while the reusable data-cut capture and materialized-row lifecycle
allocate zero times. The full pgwire and replicated-state race suites pass;
the authenticated three-voter test exercises the new quorum cut, capability
separation, stale-fence rejection, cancellation, and release/reuse.

The cut uses the live durable intent count for constant-time, conservative
group-wide refusal. The reopen-only intent map is not a live authority. Its
physical relation snapshots still require ownership filtering before SQL
evaluation after a split. SQL execution on these cuts, the native SQL wire
operation, distributed gateway integration, JDBC validation, and GoLand setup
remain unfinished. No RF3 PostgreSQL endpoint is exposed yet.

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

Only after those paths pass should the development launcher expose a
localhost PostgreSQL listener and GoLand receive a tested datasource.
