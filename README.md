# VibeDB

VibeDB is an embedded JSON database for Go applications. It gives you a small
native API, durable local storage, immutable snapshots, exact JSON indexes, and
an optional SQL layer—all inside your process.

> [!WARNING]
> VibeDB is under active development and has not been released. APIs and the
> on-disk format may change without a compatibility migration. Do not use it as
> the only copy of important data yet.

## Why VibeDB?

- Store JSON documents by application-defined keys.
- Choose in-memory, buffered, or synchronous durability per database.
- Query with either the native typed Go API or a documented SQL subset.
- Connect through `database/sql`, pgx, lib/pq, or `psql` when SQL is useful.
- Use exact single-column or compound JSON-path indexes.
- Read immutable snapshots while writes publish new generations atomically.
- Put explicit limits on query, join, transaction, and intermediate memory.
- Run without background compaction or an external database service.

## Install

```sh
go get github.com/thesyncim/vibedb
```

VibeDB currently follows the Go version declared in [`go.mod`](go.mod).

## Native API quick start

```go
package main

import (
	"fmt"
	"log"

	"github.com/thesyncim/vibedb"
)

func main() {
	db, err := vibedb.Open(
		"data/app.vdb",
		vibedb.WithDurability(vibedb.Durable),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	users := db.Collection("users")
	_, err = users.Put("user:1", []byte(
		`{"name":"Ada","active":true}`,
	))
	if err != nil {
		log.Fatal(err)
	}

	document, found, err := users.Get("user:1")
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Println(string(document))
	}
}
```

`Collection` handles are lazy: asking for one does not touch the filesystem;
the first mutation creates it. `Get` returns caller-owned bytes.

For repeated typed queries, import `github.com/thesyncim/vibedb/query`, compile
a `query.Query` once, and reuse a collection session:

```go
compiled := query.Select(query.Path("name")).Where(
	query.Cmp("active", query.Eq, true),
)

session := users.NewSession()
defer session.Release()

result, err := session.Run(compiled)
if err != nil {
	log.Fatal(err)
}
for row := 0; row < result.RowCount; row++ {
	name, _ := result.Columns[0].Cells[row].Text()
	fmt.Println(name)
}
```

See the [store guide](docs/store.md) for updates, deletes, batches, indexes,
snapshots, and lower-level storage configuration.

## Pick the interface that fits

| Interface | Best for | Package |
| --- | --- | --- |
| Native document API | Embedded key/document access with the smallest surface | `github.com/thesyncim/vibedb` |
| Typed query builder | Compiled, reusable Go queries and tight execution control | `github.com/thesyncim/vibedb/query` |
| `database/sql` | Schemas, SQL, prepared statements, and transactions in Go | `github.com/thesyncim/vibedb/sql/driver` |
| PostgreSQL protocol | Existing pgx/lib/pq clients, direct `psql`, or non-Go clients | `github.com/thesyncim/vibedb/pgwire` |

All four interfaces use the same JSON storage and query engine. SQL is a query
language over JSON documents; it is not a separate relational storage engine.

## SQL quick start

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	db, err := sql.Open("vibedb", "data/app-sql.vdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE users (
			id STRING PRIMARY KEY,
			name STRING NOT NULL,
			active BOOL NOT NULL
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(
		`INSERT INTO users VALUES (?)`,
		`{"id":"user:1","name":"Ada","active":true}`,
	)
	if err != nil {
		log.Fatal(err)
	}

	var name []byte
	err = db.QueryRow(
		`SELECT name FROM users WHERE id = ?`,
		"user:1",
	).Scan(&name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(name))
}
```

The supported surface includes schema-checked tables, exact indexes, DML with
`RETURNING`, joins, derived tables, CTEs, set operations, a bounded recursive
CTE subset, a documented window-function subset, views, predicate subqueries,
and snapshot transactions. Unsupported shapes return explicit positioned
errors instead of silently changing semantics.

VibeDB SQL is intentionally a subset, not PostgreSQL compatibility. Read the
[SQL surface](docs/design/sql-surface.md) before choosing an ORM or generating
queries dynamically.

## PostgreSQL clients

The `pgwire` package exposes the same SQL runtime over PostgreSQL protocol v3:

```go
package main

import (
	"log"
	"net"

	"github.com/thesyncim/vibedb/pgwire"
	vibedriver "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	catalog, err := vibedriver.Open("data/app-sql.vdb")
	if err != nil {
		log.Fatal(err)
	}
	defer catalog.Close()

	server, err := pgwire.NewServer(catalog, pgwire.Options{
		Auth: pgwire.Trust(), // local development only
	})
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:5433")
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(server.Serve(listener))
}
```

Then connect with a PostgreSQL client:

```sh
psql -X "host=127.0.0.1 port=5433 user=demo dbname=demo sslmode=disable"
```

The server supports the simple and extended protocols, prepared statements,
SCRAM-SHA-256, transaction state, cancellation, text and binary results, and a
small compatibility layer for basic `psql` introspection commands. The example
is intentionally local and plaintext. For a network listener, set
`Options.TLSConfig`, require encrypted startup and cancellation with
`Options.RequireTLS`, and use SCRAM authentication. It does not provide a
queryable PostgreSQL catalog or general ORM/BI discovery.

See the [pgwire contract](pgwire/doc.go) for the exact protocol and client
compatibility boundary.

## Durability profiles

The zero-value/default profile is `Durable`.

| Profile | A successful mutation means | Persistence boundary |
| --- | --- | --- |
| `vibedb.Memory` | Visible in process memory | None; the path is ignored |
| `vibedb.Buffered` | Visible from bounded memory | `Flush` or `Close` |
| `vibedb.Durable` | Persisted to the recovery journal before it becomes visible | Every successful mutation |

Durable and buffered databases use the path passed to `Open` as a database
directory. Each successful write publishes a new database state while existing
snapshots keep seeing the state they opened. Maintenance runs as bounded
foreground work, so there is no background compactor to tune or wait for.

Read the [durability contract](docs/durability.md) before selecting buffered or
advanced durability modes. In particular, a storage error can report an
unknown commit outcome; callers must close, reopen, and reconcile instead of
blindly retrying.

## Important limitations

- The project is unreleased; APIs and on-disk grammars may change without a
  compatibility migration.
- DDL is atomic per statement but is not transactional.
- Native read-write transactions are serializable. SQL and pgwire transactions
  default to Read Committed, with explicit Repeatable Read/Snapshot and
  Serializable modes. Conflict detection covers handle-mediated writes on the
  same `vibedb.Database`, `*sql.DB`, or pgwire endpoint — not writers through a
  different handle to the same files. Logical revisions, not wall-clock time,
  order conflict checks.
- An aborted transaction that wrote to a table absent at BEGIN can leave that
  empty table in the catalog; it holds no documents but is user-visible
  residue.
- Arbitrary `pg_catalog` queries and general ORM schema discovery are not
  supported.
- Pgwire does not implement replication, `COPY`, `LISTEN`/`NOTIFY`,
  SCRAM-SHA-256-PLUS channel binding, or the full PostgreSQL type and function
  systems. TLS is available through PostgreSQL's traditional `SSLRequest`
  negotiation, not the newer direct-negotiation mode.
- Materialized views are not implemented; ordinary durable views are read-only.
- Memory and work are explicitly bounded. Exceeding a limit returns an error
  without publishing a partial query result or mutation.

The [capability matrix](docs/capabilities.md) is the executable authority for
which combinations of operations, indexes, transactions, and durability are
supported.

## Distributed tier (experimental, server-only)

VibeDB is embedded-first: none of the four embedded interfaces above require a
cluster, and the default embedded behavior is unchanged whether or not a
placement configuration is supplied. The repository also carries an early,
server-only distributed tier plus the routing types it shares with one opt-in
embedded facade:

- a leader-only shard service (`shardservice`) that executes admitted SQL
  locally through a borrowed `sql/driver` session;
- a stateless routing gateway (`gateway`) that pins one immutable catalog
  generation, reloads a strictly newer valid catalog after stale refusals, and
  dispatches bounded, leader-only, explicitly read-only queries to the shards;
- a bounded Cascades-style optimizer core (`planner`) with memoized rules,
  required physical properties, distributed exchanges, multidimensional costs,
  compact generation-pinned statistics, and deterministic planning metrics;
- the frozen placement scalar and tuple codec (`distribution`) used as
  cross-shard routing identity; and
- the `cmd/vibedb-shard` and `cmd/vibedb-gateway` binaries that run the server
  tier.

Both commands are loopback-only. The gateway accepts newline-delimited JSON,
not pgwire, and neither server protocol includes built-in transport
authentication.

Every shard catalog must be created explicitly with `vibedb-shard init`. Its
write-once SQL catalog identity binds the distribution, shard ID, and
topology-issued allocation generation to a random local LogID; `serve` opens
existing bound stores only, and generic SQL opens reject them. This prevents
accidental local rebinding. Before a server starts, it also durably advances
nonzero ownership-epoch and routing-version high-waters and holds the only live
serving claim for that exact open store until all connections drain. Regressed
coordinates and a second local server fail closed. This is same-store local
fencing, not a lease, election, or replication authority: it cannot revoke a
running process over another store handle, distinguish or revoke a copied
store, police a trusted caller opening direct SQL sessions on the same
`Database`, or prove that a replica is caught up. The shard server itself does
not release its claim until its owned sessions drain.

The shard service, gateway, and their binaries are server-only and not part of
the embedded API. The one embedded touch point is opt-in and carries no
network: the `sql/driver` local-cluster facade (`OpenCluster` /
`OpenClusterConnector`) runs the same placement and write preflight over the
shared `distribution` types against a single embedded store as a degenerate
single-shard local cluster.

The server tier is leader-only today. The repository also contains a bounded,
non-serving Raft kernel, append-only WAL, local replicated-apply machine,
in-process Multi-Raft scheduler, and a frame/roster validator that accepts a
caller-supplied authenticated NodeID. Those
internal packages are not wired into `vibedb-shard`, `vibedb-gateway`, a public
API, or operator configuration, and they do not make the server tier highly
available.

| Server capability | Current state |
| --- | --- |
| Leader-only shard process | Available; one locally fenced store |
| Stateless gateway | Available; generation-pinned read fan-out, single-shard fast writes, and synchronous fixed-participant atomic write batches; automatic recovery remains pending |
| Embedded single-shard placement checks | Available through `OpenCluster` |
| Peer enrollment, authentication, and network transport | Not available |
| Replicated client writes and automatic failover | Not available |
| Runtime Raft snapshots, WAL compaction, and dynamic membership | Not available |
| Follower/session reads, online movement, and backup/PITR orchestration | Not available |
| Adaptive splitting | Shadow-only recommender; not wired to topology publication or data movement |

The tier is unreleased and unstable like the rest of VibeDB. The
[capability matrix](docs/capabilities.md) covers the embedded surface only.

The implementation target keeps the gateway's explicitly routed fast path but
adds replicated distributed transactions, snapshots, global indexes, query
exchange, and online movement as opt-in costs when an operation crosses shards.
Its analytical lane uses vectorized multi-stage execution, projection/data
skipping, pushdown, and parallel replicas without imposing a MergeTree-style
write path on transactional storage.
Tenants route through many virtual buckets rather than being assigned to one
physical shard. The target and its correctness/performance gates are specified
in [Distributed system target](docs/design/distributed-system.md); the table
above remains the honest statement of what serves today.

Read the design before relying on any of it:

- [Distributed sharding](docs/design/distributed-sharding.md)
- [Distributed system target](docs/design/distributed-system.md)
- [Distributed transactions](docs/design/distributed-transactions.md)
- [Query planner](docs/design/query-planner.md)
- [Placement tuple format](docs/design/distribution-tuple-format.md)

## Performance

VibeDB is designed for compiled queries, reusable execution storage, bounded
foreground work, and allocation-free warm paths. Published tables, artifact
hashes, corpus definitions, methodology, and reproduction commands are kept in
the repository rather than summarized as context-free headline numbers here:

- [Performance guide](docs/performance.md)
- [Competitive benchmark results](bench/competitive/RESULTS.md)
- [Benchmark coverage matrix](bench/competitive/COVERAGE.md)

## Documentation

Start with the [documentation index](docs/README.md) for user, operator, and
engineering references.

| Topic | Document |
| --- | --- |
| Start using the storage API | [Store guide](docs/store.md) |
| Exact supported combinations | [Capability matrix](docs/capabilities.md) |
| SQL syntax and limitations | [SQL surface](docs/design/sql-surface.md) |
| Crash, recovery, and acknowledgement guarantees | [Durability](docs/durability.md) |
| Storage and snapshot design | [Architecture](docs/architecture.md) |
| On-disk format | [Format](docs/format.md) |
| Benchmarks and methodology | [Performance](docs/performance.md) |
| Development workflow | [Contributing](CONTRIBUTING.md) |

## Contributing

VibeDB favors measured behavior and explicit failure over undocumented
fallbacks. Changes should include correctness tests, resource-bound tests where
applicable, and benchmarks for hot-path work. See [CONTRIBUTING.md](CONTRIBUTING.md).
