# vibedb

vibedb is an embedded JSON document database written in Go. It provides a
mutable in-memory store, a bounded-residency durable store, immutable O(1)
snapshots, exact JSON indexes, ordered scans, and a typed query engine. Durable
mutations publish complete copy-on-write generations: readers do not reconcile
memtables, tombstones, or version chains.

The project is pre-v1. Public APIs and the on-disk format may change without a
migration path.

## Published benchmark snapshot

The latest checked-in competitive run is a dated 2026-07-28 baseline, not a
claim about current `main`. Several storage and index paths changed after its
measured commits.

| Lane or measurement | Published result | Context |
| --- | ---: | --- |
| Buffered-visible mixed workloads | 180k–1.01M ops/s | 1.9–3.6× SQLite |
| Ordinary-sync mixed workloads | 17.9k–176k ops/s | trailed SQLite and Badger |
| Power-safe mixed workloads | 370–3,663 ops/s | roughly 6–13% behind SQLite |
| Sustained churn, 100k live documents | 36.0 MiB allocated | flat; no maintenance |
| Verbatim primary bulk, 100k documents | 28.1 MiB allocated | published corpus |

See the [short performance guide](docs/performance.md) for interpretation and
the [competitive results](bench/competitive/RESULTS.md) for complete tables,
commits, corpus definitions, caveats, and reproduction commands.

## Quickstart

```go
db, err := sql.Open("vibedb", "example.vdb")
if err != nil { log.Fatal(err) }
defer db.Close()

if _, err = db.Exec(`
	CREATE TABLE users (
		id STRING PRIMARY KEY,
		name STRING NOT NULL,
		active BOOL NOT NULL
	)`); err != nil {
	log.Fatal(err)
}
if _, err = db.Exec(`INSERT INTO users VALUES (?)`,
	`{"id":"user:1","name":"Ada","active":true}`); err != nil {
	log.Fatal(err)
}

var name []byte
if err = db.QueryRow(
	`SELECT name FROM users WHERE id = ?`, "user:1",
).Scan(&name); err != nil {
	log.Fatal(err)
}
fmt.Println(string(name)) // Ada
```

Imports are `database/sql`, `log`, `fmt`, and a blank import of
`github.com/thesyncim/vibedb/sql/driver`. SQL is the public textual query
language; JSON is the stored row representation. See the
[supported SQL surface](docs/design/sql-surface.md) for schemas, indexes,
joins, transactions, exact-number behavior, and explicit subset limits.

## PostgreSQL clients

The `pgwire` package exposes the same typed SQL catalog and query engine over
PostgreSQL protocol v3:

```go
catalog, err := vibedriver.Open("example.vdb")
if err != nil { log.Fatal(err) }
defer catalog.Close()

srv, err := pgwire.NewServer(
	pgwire.FromSQLDatabase(catalog),
	pgwire.Options{Auth: pgwire.Trust()},
)
if err != nil { log.Fatal(err) }
ln, err := net.Listen("tcp", "127.0.0.1:5433")
if err != nil { log.Fatal(err) }
go func() { log.Print(srv.Serve(ln)) }()
```

pgx and lib/pq clients can issue the document SQL subset with PostgreSQL `$1`
parameters: `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`,
`SELECT`, uncorrelated predicate subqueries, inner and left joins, prepared
statements, and explicit transactions. Stock
`psql` can connect and issue the same supported direct SQL. Whole-document
parameters are described as PostgreSQL `json`; projected JSON values preserve
their exact wire spelling. `INSERT ... RETURNING` supports projected JSON paths
and `*`, including multi-row inserts in VALUES order.

For example, the server above accepts a direct psql session with the documented
cleartext fallback:

```sh
psql -X "host=127.0.0.1 port=5433 user=demo dbname=demo sslmode=prefer"
```

A read-only heap `*store.Database` or one `*durable.Collection` can still be
served with `FromDatabase` or `FromCollection`. A
[nested integration gate](integration/pgclient/pgclient_test.go) exercises
pinned pgx v5 and lib/pq releases over loopback TCP in CI. A separate
[psql gate](integration/pgclient/psql_test.go) drives the official PostgreSQL
18.4 client through encryption fallback, SCRAM, SQL execution, error recovery,
and clean termination. Together they cover schema validation, indexes, writes,
a join, rollback/read-your-writes, stable SQLSTATEs, and close/reopen
persistence.

This is a PostgreSQL client protocol and SQL-subset implementation, not a
PostgreSQL catalog emulator. TLS, `pg_catalog`, ORM/BI schema discovery, psql
backslash introspection, savepoints, and transactional DDL are explicitly
unsupported. See the [pgwire contract](pgwire/doc.go) for the exact surface,
authentication, result types, transaction boundaries, cancellation, and
resource bounds.

## Durability at a glance

| Option | Success means | Reader visibility | Crash window |
| --- | --- | --- | --- |
| `DurabilityBufferedVisible` | accepted into bounded canonical COW staging | immediate | acknowledged changes after the last successful `Flush` may be lost |
| `DurabilityAsyncVisible` | accepted by the bounded background committer | immediate | acknowledged generations not yet reported by `DurableGeneration` may be lost |
| `DurabilitySync` (zero value) | one recovery-journal record appended and synced on the primary graph, then the mutation applies and publishes | after the journal sync — visibility strictly follows durability | recovery selects the last checkpointed root and replays the journal's acknowledged records |

For buffered mode, `CheckpointPowerSafe` is the zero-value `Flush` strength;
`CheckpointFilesystem` explicitly selects an ordinary filesystem boundary.
See [the durability contract](docs/durability.md) before choosing a weaker
mode.

## Start here

- [Architecture](docs/architecture.md): representation invariants and the
  read, write, checkpoint, and snapshot paths.
- [Store API](docs/store.md): current heap and durable surfaces.
- [Durability](docs/durability.md): acknowledgement, crash, recovery, and
  platform-sync contracts.
- [On-disk format](docs/format.md): current byte-level format authority.
- [Performance](docs/performance.md): measured tables and benchmark honesty
  rules.
- [Design documents](docs/design/): implementation records, rejected options,
  promotion evidence, and future work.
- [Contributing](CONTRIBUTING.md): tests, benchmarks, and documentation rules.

The repository was extracted from
[vibejson](https://github.com/thesyncim/vibejson) on 2026-07-27; that
repository carries the earlier design and measurement history.
