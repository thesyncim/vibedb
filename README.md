# vibedb

vibedb is an embedded JSON document database written in Go. It provides a
mutable in-memory store, a bounded-residency durable store, immutable generation
snapshots, exact JSON indexes, ordered scans, and a typed query engine. Durable
mutations publish semantically complete generations. Eligible foreground lanes
represent a generation as an immutable canonical base plus a bounded,
generation-stamped row overlay; point reads and scans merge that overlay
exactly, including delete records. The overlay is folded by bounded foreground
checkpoint work—there is no background compaction or offline maintenance.
Creating a durable snapshot may wait for the current writer and seal bounded
dirty overlay/parent state; pinning the sealed immutable generation itself is
constant time.

The project is unreleased. Public APIs and the primary-file version-0 on-disk
format may change in place; development files are recreated after a format
change. Superseded APIs are deleted rather than deprecated or retained behind
compatibility shims; development callers are expected to track the current
surface.

## Quickstart

The root package is the normal application surface. It owns the database
directory, collection files, synchronization, and descriptor shutdown:

```go
db, err := vibedb.Open("application.vdb",
	vibedb.WithDurability(vibedb.Durable),
)
if err != nil { log.Fatal(err) }
defer db.Close()

users := db.Collection("users")
if _, err = users.Put("user:1", []byte(
	`{"name":"Ada","active":true}`,
)); err != nil {
	log.Fatal(err)
}

document, found, err := users.Get("user:1")
if err != nil { log.Fatal(err) }
fmt.Println(found, string(document))
```

Imports are `fmt`, `log`, and `github.com/thesyncim/vibedb`. A collection
handle is lazy: obtaining it performs no I/O, while its first mutation creates
the collection. `Get` returns caller-owned bytes. Logical collection names are
valid UTF-8 up to 120 bytes and are encoded into reversible lowercase-hex disk
filenames, so separators, reserved device words, case, and Unicode forms cannot
collide through filesystem rules.

After `CreateIndex`, execute the same compiled typed query across every profile
with one-off `Collection.Run`, or retain bounded hot-loop storage behind
`session := collection.NewSession(); defer session.Release()` and call
`session.Run(compiled)` against a fresh immutable generation each time.

Choose one explicit lifecycle contract:

| Profile | Mutation success means | Persistence boundary |
| --- | --- | --- |
| `vibedb.Memory` | published in process memory | none; `Open` never accesses its path |
| `vibedb.Buffered` | published from bounded memory | `Flush` or `Close` |
| `vibedb.Durable` | recovery record power-safely fenced before visibility | each successful mutation |

The low-level `store`, `store/durable`, and query packages remain available for
custom storage geometry and execution. Descriptor-owning embedders use
`vibedb.OpenFile(file, vibedb.AdvancedOptions{...})`; the returned collection
must be closed before the caller closes its file. `file.Name()` must be an
absolute stable path to that descriptor: durable and buffered collections own
the adjacent `.rjournal` sibling, require parent-directory authority, and must
be renamed or backed up as a primary+journal pair. The lexical primary path
must be a regular non-symlink file and must not be renamed, replaced, unlinked,
or retargeted until `Collection.CloseCompleted()` reports that the exclusive
descriptor borrow has ended.

## Capabilities and limits

The [executable capability matrix](docs/capabilities.md) is the authority for
which combinations of indexing, point or transactional mutation, entry point,
and durability succeed atomically or return a documented error. It is generated
from the same case manifest exercised by native, `database/sql`, and pgwire
tests; prose does not define a stronger database than those tests do.

The important current boundaries are explicit: separate native point calls are
separate publications; bounded `Collection.Update` batches are atomic only in
the publication modes listed by the matrix; a SQL transaction may read several
tables but write exactly one; DDL is not transactional; and the SQL and pgwire
surfaces implement the documented subset rather than a general PostgreSQL
catalog. Pgwire recognizes the exact PostgreSQL 18.4 psql basic introspection templates for
`\l`, `\dn`, `\dt`, `\di`, `\d`, and `\d name`; `\df`, `\du`, and `\dv` are
recognized and honestly empty. The `\d name` capture is currently limited to
bare ASCII identifiers of at most 128 bytes. Queryable `pg_catalog`, ORM/BI discovery, and
ad-hoc catalog SQL remain unsupported. See the
[SQL surface](docs/design/sql-surface.md) and
[pgwire contract](pgwire/doc.go) for syntax, resource bounds, and unsupported
features before adopting a client or ORM.

## Performance snapshot

The write, concurrency, and space rows come from clean commit `7fe6769` on an
Apple M4 Max. The scan-mix row and CPU/scan gates were refreshed from clean
commit `b5702bc` on the same host. Engine ratios use matched buffered-visible
semantics; space rows report both apparent file size and allocated filesystem
blocks.

| Lane or measurement | Result | Context |
| --- | ---: | --- |
| Single-client YCSB-A/B/F and churn | 2.52–2.79× Badger | matched buffered-visible CP64 runs |
| Single-client scan mix | 1.58× Badger (57.7% ahead) | 390,929.5 versus 247,961.5 ops/s; ordered full-scan p50 remains 7.0% slower |
| Concurrent existing-key writes | 2.35–2.50× Badger | matched 1, 8, and 32-client runs |
| Concurrent churn | 2.73–2.93× Badger | matched 1, 8, and 32-client runs |
| Online sustained churn, low/high cardinality | 22.075 / 16.020 · 54.841 / 36.070 MiB | apparent / allocated after 200k mutations; zero forced checkpoints; no background/offline maintenance |
| Offline Repack floor, low/high cardinality | 9.001 / 9.520 · 18.767 / 19.520 MiB | apparent / allocated; separate out-of-place maintenance result |
| Unified paired bulk, low/high cardinality | 9.001 / 9.520 · 18.767 / 19.520 MiB | apparent / allocated for the current durable pair |
| Native checkpoint leaf patch / generic replan | 1.914 / 256.121 µs | median; ~134× faster; 0 allocations |
| Unified full scans, 100k documents | 23.49 ordered · 91.57 / 94.21 competitive ns/document | low / high cardinality where applicable; 0 allocations |
| Masked scan | 178.4 ns/selected document | one occupied row per live posting tile; 0 allocations |

See the [short performance guide](docs/performance.md) for interpretation and
the [competitive results](bench/competitive/RESULTS.md) for complete tables,
commits, corpus definitions, caveats, and reproduction commands.

## SQL quickstart

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

srv, err := pgwire.NewServer(catalog, pgwire.Options{
	Auth:                 pgwire.Trust(),
	MaxIntermediateBytes: 64 << 20,
})
if err != nil { log.Fatal(err) }
ln, err := net.Listen("tcp", "127.0.0.1:5433")
if err != nil { log.Fatal(err) }
go func() { log.Print(srv.Serve(ln)) }()
```

pgx and lib/pq clients can issue the document SQL subset with PostgreSQL `$1`
parameters: `CREATE TABLE`, `CREATE INDEX`, `INSERT`, `UPDATE`, `DELETE`,
`SELECT`, uncorrelated predicate subqueries, sole-source derived tables, inner,
left, and right joins, prepared statements, and explicit transactions. Stock
`psql` can connect and issue the same supported direct SQL. Whole-document
parameters are described as PostgreSQL `json`; projected JSON values preserve
their exact wire spelling. `INSERT ... RETURNING`, `UPDATE ... RETURNING`, and
`DELETE ... RETURNING` support projected JSON paths and `*`; `ON CONFLICT DO
NOTHING` skips duplicate document identities atomically.
`SELECT DISTINCT` is supported for non-aggregate projections.
`TRUNCATE [TABLE]` atomically replaces a table with an empty durable
incarnation while preserving its schema and indexes. `DROP INDEX [IF EXISTS]`
physically rebuilds and atomically publishes the table without the named exact
index; `ON table` disambiguates table-local index names.

For example, the server above accepts a direct psql session with the documented
cleartext fallback:

```sh
psql -X "host=127.0.0.1 port=5433 user=demo dbname=demo sslmode=prefer"
```

A [nested integration gate](integration/pgclient/pgclient_test.go) exercises
pinned pgx v5 and lib/pq releases over loopback TCP in CI. A separate
[psql gate](integration/pgclient/psql_test.go) drives the official PostgreSQL
18.4 client through encryption fallback, SCRAM, SQL execution, error recovery,
and clean termination. Together they cover schema validation, indexes, writes,
a join, rollback/read-your-writes, stable SQLSTATEs, and close/reopen
persistence.

This is a PostgreSQL client protocol and SQL-subset implementation, not a
PostgreSQL catalog emulator. The pinned PostgreSQL 18.4 psql client's bounded basic meta-commands (`\l`,
`\dn`, `\dt`, `\di`, `\d`, and `\d name`) are answered from the SQL catalog;
`\d name` currently accepts only bare ASCII names up to 128 bytes; `\df`,
`\du`, and `\dv` return honest empty results. TLS, queryable
`pg_catalog`, arbitrary catalog SQL, ORM/BI schema discovery, savepoints, and
transactional DDL are explicitly unsupported. See the
[pgwire contract](pgwire/doc.go) for the exact surface,
authentication, result types, transaction boundaries, cancellation, and
resource bounds.

## Durability at a glance

| Option | Success means | Reader visibility | Crash window |
| --- | --- | --- | --- |
| `DurabilityBufferedVisible` | accepted into bounded foreground staging | immediate | acknowledged changes after the last successful `Flush` may be lost |
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
- [Capability matrix](docs/capabilities.md): executable native, database/sql,
  pgwire, indexing, transaction, operation, and durability contracts.
- [Durability](docs/durability.md): acknowledgement, crash, recovery, and
  platform-sync contracts.
- [On-disk format](docs/format.md): current byte-level format authority.
- [Performance](docs/performance.md): measured tables and benchmark honesty
  rules; the [executable benchmark coverage matrix](bench/competitive/COVERAGE.md)
  records which measurement shapes exist and which remain gaps.
- [Design documents](docs/design/): current design constraints and future work.
- [Contributing](CONTRIBUTING.md): tests, benchmarks, and documentation rules.
