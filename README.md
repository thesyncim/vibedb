# VibeDB

VibeDB is a JSON document database for Go. Embed it in an application for
exact indexes, full-text search, typed queries, and serializable transactions,
or reach it through `database/sql` and the PostgreSQL wire protocol. The same
engine also runs as an RF3 replicated cluster. The cluster has embedded
gateways, exactly-once writes, online hot-shard split, and online scale-out and
scale-in.

[Get started](docs/getting-started.md) · [Documentation](docs/README.md) ·
[API guides](docs/api/README.md) · [Design](docs/design/README.md) ·
[Operations](docs/operations/README.md) · [Reference](docs/reference/README.md)

**Development status:** APIs, wire protocols, and data formats can change
between commits. Evaluate a pinned revision; see [status](#status-strengths-and-limitations).

## Store JSON in Go

Use Go 1.27 or later, as declared in [go.mod](go.mod). In a Go module, install
the revision you intend to evaluate:

```sh
go get github.com/thesyncim/vibedb@47ac5ab23
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

Running it prints `found=true doc={"name":"Ada","team":"compiler"}`. The
default `Durable` profile persists a mutation's recovery record before
acknowledging it, so a second run reopens `./data` and reads the same row.
Collections are created on the first valid write, and stored JSON is
canonicalized. The [tutorial](docs/getting-started.md) explains the files it
creates and how to close correctly; the [native API guide](docs/api/native.md)
covers indexes, queries, and transactions.

## Choose an interface

| Interface | Use it for | Read next |
| --- | --- | --- |
| Native Go (`vibedb`) | Embedded document storage, exact indexes, and transactions | [Native API](docs/api/native.md) |
| Full-text search | BM25-ranked TINQL search over one text path, from Go or SQL (`==>`, `SCORE()`) | [Search](docs/api/search.md) |
| Typed queries (`query`) | Reusable plans and explicit snapshot control | [Query API](docs/api/query.md) |
| `database/sql` | Embedded SQL tables and DDL with Go connection pooling | [SQL API](docs/api/sql.md) |
| PostgreSQL wire | Access from psql, pgx, lib/pq, and JDBC | [Client support and limits](docs/api/pgwire.md) |
| RF3 cluster | Replicated SQL across physical nodes, each with an embedded gateway | [Start a local cluster](docs/operations/local-cluster.md) |

The SQL surface is the VibeDB dialect, not PostgreSQL. Check the
[SQL reference](docs/reference/sql.md) before porting an application.

## How it works

Readers pin immutable generations, and each mutation maintains documents and
their exact indexes together. In a cluster, every shard, the catalog, and the
request ledger is an independent Raft group with three replicas. Each physical
node hosts replicas from many groups behind one shared node log. Any node's
gateway can route a request. A durable request identity lets a client retry an
unknown outcome without applying the write twice. Controllers move replicas,
split hot shards, and add or retire nodes while traffic continues.

- [Architecture](docs/architecture.md): components, reads, writes, and ownership.
- [Distributed design](docs/design/README.md#distributed-design): routing,
  replication, exactly-once writes, transactions, reads, and topology changes.
- [Transactions](docs/transactions.md) and [durability](docs/durability.md):
  visibility, commit, and recovery.

## Status, strengths, and limitations

VibeDB is development-grade software with no release, support window, or
compatibility promise. Its strengths are explicit, tested contracts:

- **Embedded:** serializable transactions across collections, crash-atomic
  durable commits, and exact indexes that publish with their rows. A generated
  [capability matrix](docs/capabilities.md) runs the same mutation cases
  through native Go, `database/sql`, and pgwire.
- **Distributed:** Linux CI workflows run named fault and scaling scenarios
  against shipped binaries on pull requests:

| Scenario | Workflow |
| --- | --- |
| Process kills, partitions, and exact replay of acknowledged writes | `durable-rf3-external.yml`, `durable-rf3-multirelation.yml` |
| Physical nodes hosting many RF3 groups, at 3 and 6 nodes | `fused-node-rf3.yml` |
| Online 3 → 4 → 3 node scaling under open-loop traffic | `seamless-scale-in-out.yml` |
| Automatic hot-shard split | `dev-hot-split.yml` |
| Restore into fresh identities, then failover | `restore-rf3-external.yml` |
| Clock faults and log retention | `clock-fault-matrix.yml`, `wal-retention.yml` |
| RF3 create, restart, and replica replacement on Linux and macOS | `ci.yml` (`portable-rf3`) |

Key gaps:

- No rolling upgrade, downgrade, or cross-build restore; every process must run
  one build.
- Online split covers base relations only; a split that must move a global
  index fails closed.
- Scaling commands ship, but their inputs (operator credential, node
  preparation manifest, node descriptor) have no shipped generator.
- No global MVCC snapshot across Raft groups; no Kubernetes operator.
- Tin search rebuilds postings in memory per searched generation; postings
  are not persisted or maintained incrementally.
- PostgreSQL wire support covers selected client flows, not PostgreSQL
  compatibility. Several [benchmark reports](docs/benchmarks/README.md) record
  VibeDB slower than the comparison system.

[Stability and compatibility](docs/status.md) lists every current limitation,
and [deployment readiness](docs/operations/production-readiness.md) gives the
evidence level of each operator workflow.

## Develop

From a checkout:

```sh
make build
make test
```

These targets enable Go's SIMD experiment. Use `GOEXPERIMENT=nosimd` with
`make` for portable checks, or set `GOEXPERIMENT=simd` explicitly for raw Go
commands. See [SIMD](docs/simd.md) for supported kernels and CPU fallbacks.

Read [Contributing](CONTRIBUTING.md) and the [developer guide](docs/development/README.md)
for tests, generated docs, and qualification workflows, and [Security](SECURITY.md)
for reporting and trust boundaries. The repository has no project license;
third-party notice files apply to their respective incorporated work.
