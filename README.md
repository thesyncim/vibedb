# VibeDB

An embedded JSON database for Go, with exact indexes, typed queries, and
serializable transactions. VibeDB also provides a SQL dialect through
`database/sql`, a PostgreSQL wire adapter, and an RF3 distributed runtime.

[Get started](docs/getting-started.md) · [API guides](docs/api/README.md) ·
[Design](docs/design/README.md) · [Operations](docs/operations/README.md) ·
[Reference](docs/reference/README.md)

**Development status:** APIs and data formats can change between commits.
Use an exact revision for evaluation; see [stability and compatibility](docs/status.md).

## Store JSON in Go

Use Go 1.27 or later, as declared in [go.mod](go.mod). In a Go module, install
the revision you intend to evaluate:

```sh
go get github.com/thesyncim/vibedb@a0de0919
```

```go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/thesyncim/vibedb"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	db, err := vibedb.Open("./data")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	users := db.Collection("users")
	if _, err := users.Put("user:1", []byte(`{"name":"Ada","team":"compiler"}`)); err != nil {
		return err
	}

	doc, found, err := users.Get("user:1")
	if err != nil {
		return err
	}
	fmt.Printf("found=%t doc=%s\n", found, doc)
	return nil
}
```

The default profile persists a mutation's recovery record before acknowledging
it. Collections are created on the first valid write; stored JSON is
canonicalized. The [tutorial](docs/getting-started.md) walks through reopening
the database, and the [native API guide](docs/api/native.md) covers indexes,
queries, and resource lifetimes.

## Choose an interface

| Interface | Use it for | Read next |
| --- | --- | --- |
| Native Go | Embedded document storage and transactions | [Native API](docs/api/native.md) |
| Typed queries | Reusable plans over JSON collections | [Query API](docs/api/query.md) |
| `database/sql` | Embedded SQL with Go connection pooling | [SQL API](docs/api/sql.md) |
| PostgreSQL wire | SQL access from pgx, lib/pq, psql, and JDBC | [Client support and limits](docs/api/pgwire.md) |
| Distributed RF3 | Evaluate replicated SQL across local physical nodes | [Start a cluster](docs/operations/local-cluster.md) |

The SQL and network interfaces are experimental. PostgreSQL wire support
covers selected client behavior and the VibeDB dialect; check the
[SQL reference](docs/reference/sql.md) before porting an application.

## Understand the engine

The embedded and distributed paths share the durable collection engine.
Readers pin immutable generations; mutations maintain documents and exact
indexes together. Distributed nodes combine SQL coordination, Raft groups,
and node-owned storage in one process.

- [Architecture](docs/architecture.md): components, reads, writes, and ownership.
- [Query execution](docs/design/query-execution.md): access paths, operators, and budgets.
- [Transactions](docs/transactions.md) and [durability](docs/durability.md): visibility, commit, and recovery.
- [Benchmark reports](docs/benchmarks/README.md): measurements with their methods and raw evidence.

## Develop

From a checkout:

```sh
make build
make test
make bench
```

These targets enable Go's SIMD experiment. Use `GOEXPERIMENT=nosimd` with
`make` for portable checks, or set `GOEXPERIMENT=simd` explicitly for raw Go
commands. See [SIMD](docs/simd.md) for supported kernels and CPU fallbacks.

Read [Contributing](CONTRIBUTING.md) for focused tests, generated docs, and
qualification workflows, and [Security](SECURITY.md) for reporting and trust
boundaries. The repository has no project license; third-party notice files
apply to their respective incorporated work.
