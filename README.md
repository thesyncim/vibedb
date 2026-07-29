# vibedb

vibedb is an embedded JSON document database written in Go. It provides a
mutable in-memory store, a bounded-residency durable store, immutable O(1)
snapshots, exact JSON indexes, ordered scans, and a typed query engine. Durable
mutations publish complete copy-on-write generations: readers do not reconcile
memtables, tombstones, or version chains.

The project is pre-v1. Public APIs and the on-disk format may change without a
migration path.

## Measured baseline

These are medians from the checked-in Apple M4 Max baseline: Go 1.26.0,
darwin/arm64, 100,000 documents, three isolated process runs. They describe
the current default durable store unless a row says otherwise.

| Measurement | Result |
| --- | ---: |
| Random point read | 1,162 ns, 0 B / 0 alloc |
| Ordered iteration | 7.546 ns/document, 0 B / 0 alloc |
| Ordered all-bytes scan | 79.64 ns/document, 0 B / 0 alloc |
| Exact indexed filter, 945 matches | 36.108 µs |
| Verbatim bulk file, 23.73 MiB raw JSON | 32.2 MiB allocated |
| Explicit compact bulk, low / high cardinality | 13.9 / 26.1 MiB allocated |
| Power-safe mixed workloads vs comparable SQLite | 6.3–14.7% lower throughput |

The full tables, commits, corpus definitions, caveats, and reproduction
commands are in [competitive results](bench/competitive/RESULTS.md). Compact
bulk is a separate representation, not the mutable default.

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
`SELECT`, inner joins, prepared statements, and explicit transactions. Stock
`psql` can connect and issue the same supported direct SQL. Whole-document
parameters are described as PostgreSQL `json`; projected JSON values preserve
their exact wire spelling.

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
| `DurabilitySync` (zero value) | data and alternate root crossed the platform's power-safe barriers | after the barriers | recovery selects the complete old or new generation |

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
- [Design documents](docs/design/): promotion specifications and future work.
- [Contributing](CONTRIBUTING.md): tests, benchmarks, and documentation rules.

The repository was extracted from
[vibejson](https://github.com/thesyncim/vibejson) on 2026-07-27; that
repository carries the earlier design and measurement history.
