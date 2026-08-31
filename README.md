# VibeDB

> [!CAUTION]
> **Unreleased development software.** VibeDB has no tagged release, support
> window, compatibility promise, or published project license. Any commit may
> break Go APIs, SQL behavior, commands, manifests, wire protocols, or on-disk
> data. Pin an exact commit, keep recoverable backups, and do not use this
> repository for irreplaceable data.

VibeDB is an embedded JSON database for Go. Its primary interface is a small
native API for named collections, exact indexes, typed queries, and serializable
transactions. The repository also contains a bounded SQL runtime, an
experimental PostgreSQL protocol adapter, and an experimental RF3 distributed
system.

## Choose a path

| You want to… | Start here | Status |
| --- | --- | --- |
| Embed a JSON database in Go | [Getting started](docs/getting-started.md) | Development; best-tested path |
| Build typed document queries | [Typed query API](docs/api/query.md) | Development |
| Use `database/sql` | [SQL API](docs/api/sql.md) | Experimental SQL subset |
| Connect a PostgreSQL client | [PostgreSQL wire adapter](docs/api/pgwire.md) | Experimental protocol adapter; **not PostgreSQL compatibility** |
| Explore a local RF3 cluster | [Local cluster](docs/operations/local-cluster.md) | Development and qualification only |
| Work on storage internals | [Storage layers](docs/store.md) | Expert, unstable APIs |

The [current-status page](docs/status.md) lists compatibility rules, known
defects, and the evidence boundary for this exact source snapshot.

## Try the embedded API

VibeDB requires the Go version declared in [`go.mod`](go.mod). Until a release
exists, depend on an exact commit rather than `main`:

```bash
go get github.com/thesyncim/vibedb@<commit>
```

```go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/thesyncim/vibedb"
)

func run() (err error) {
	db, err := vibedb.Open("./vibedb-data")
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

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

`Open` owns a database directory. The default profile acknowledges a mutation
only after its recovery record is power-safe. A collection may use a primary
file and a recovery-journal sidecar; copy or back up the complete closed
database directory, never an arbitrary live file.

## What is implemented

- Canonical JSON documents in named, lazily materialized collections.
- Exact scalar and compound indexes.
- Immutable snapshots, typed queries, and bounded execution workspaces.
- Serializable native transactions and multi-table SQL transactions.
- A deliberately bounded SQL dialect through `database/sql`.
- PostgreSQL protocol v3 simple/extended flows, SCRAM, TLS negotiation,
  prepared statements, portals, cancellation, and selected client discovery
  shims.
- RF3 replication, routing, authenticated services, durable request recovery,
  backup/restore, schema-rollout, split, and replica-move primitives used by
  checked-in development commands.

## What is not promised

- Compatibility between different commits or old development data.
- PostgreSQL SQL, catalog, extension, ORM, or server compatibility.
- A production-ready distributed database or Kubernetes operator.
- Rolling mixed-build upgrades, downgrades, or format migration.
- Published performance, cost, or horizontal-scaling results.
- A fixed total-memory ceiling: some indexes and off-heap structures are
  data-dependent.

Browse the [documentation map](docs/README.md) for architecture, operations,
reference material, and contribution paths.

## Build and test this checkout

```bash
go build ./...
go vet ./...
go test -p=1 -timeout=25m ./...
```

This rewrite does not claim a complete green root-module run. See
[current status](docs/status.md) for the exact validation record and opt-in
qualification boundaries; investigate any failure from your checkout.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) before changing a persistence,
protocol, or evidence contract. See [SECURITY.md](SECURITY.md) for the current
reporting limitations and trust boundaries.

The repository currently has no project license. Files named `LICENSE-*` and
`PATENTS-*` are notices for incorporated third-party work; they do not grant a
license to VibeDB as a whole.
