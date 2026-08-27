# VibeDB

VibeDB is an embedded JSON database for Go. The root package provides a small
owned-lifecycle API. The repository also includes a typed query engine, a SQL
runtime, a `database/sql` driver, and a PostgreSQL wire server.

The durable engine uses bounded memory and a fixed 4 KiB base-page format. The
zero-value product profile makes a mutation durable before it becomes visible.
You can select buffered or in-memory operation when your application needs a
different acknowledgement contract.

> **Development status:** APIs, command contracts, and storage grammars can
> change between tested commits. The distributed runtime is experimental and
> authenticated by default. An explicit development mode permits plaintext
> only on loopback listeners.

The gateway exposes three separate distributed paths: general SQL through
static shard services, replicated exact-key reads, and strict exact-key
`exec_batch` mutations over one or more prepared RF3 groups. It also composes
replicated catalog, request-ledger, replica-move, automatic hot-range split,
live-backup, and fresh-identity restore control paths. The
read lane supports all-or-nothing multi-table batches with one `ReadIndex` cut
per group. The mutation lane supports single- and multi-row whole-document
insert, exact-primary-key whole-document update, and exact-primary-key delete
including finite `IN` key sets, with atomic same-group multi-relation batches
and independently placed global-index maintenance.

These distributed commands are unreleased. The generated feature-state matrix
distinguishes a present primitive from command integration and from mandatory
external qualification. It does not turn unexecuted CI gates into evidence.

## Requirements

- Go 1.26 or a compatible newer toolchain
- A supported operating system for the selected storage backend

The portable storage backend is the fallback. Linux can select `io_uring` and
direct I/O through the advanced storage API.

## Install

```bash
go get github.com/thesyncim/vibedb@2ebcdff1047d
```

Replace the commit with the revision that your application has tested.

## Native API

```go
package main

import (
	"fmt"
	"log"

	"github.com/thesyncim/vibedb"
)

func main() {
	db, err := vibedb.Open("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("close VibeDB: %v", err)
		}
	}()

	users := db.Collection("users")
	if _, err := users.Put("user:1", []byte(`{"name":"Ada"}`)); err != nil {
		log.Fatal(err)
	}

	document, found, err := users.Get("user:1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(found, string(document))
}
```

`Open` treats `./data` as a database directory. It creates one durable file for
each collection when the first mutation needs that collection.

## Select an interface

| Interface | Package | Use case |
| --- | --- | --- |
| Native facade | `github.com/thesyncim/vibedb` | Embedded JSON CRUD and multi-collection transactions |
| Typed queries | `github.com/thesyncim/vibedb/query` | Programmatic query construction and reusable execution buffers |
| SQL | `github.com/thesyncim/vibedb/sql/driver` | Go applications that use `database/sql` |
| PostgreSQL wire | `github.com/thesyncim/vibedb/pgwire` | PostgreSQL clients over a configured TCP listener |
| Storage control | `github.com/thesyncim/vibedb/store/durable` | Explicit geometry, I/O mode, durability lane, and verification control |

## Documentation

- [Install and run VibeDB](docs/getting-started.md)
- [API guide](docs/api/README.md)
- [Architecture](docs/architecture.md)
- [Durability and recovery](docs/durability.md)
- [Current capability matrix](docs/capabilities.md)
- [Distributed feature state](docs/distributed-feature-state.md)
- [RF3 quickstart](docs/operations/distributed-quickstart.md)
- [Distributed runtime](docs/operations/distributed.md)
- [Replica lifecycle operations](docs/operations/replica-lifecycle.md)
- [Security policy](SECURITY.md)

The documentation uses an STE-informed technical style. It keeps standard
software and database terminology. See the [documentation language
guide](docs/STYLE.md).

## Verify a checkout

```bash
go build ./...
go vet ./...
go test -p=1 -timeout=25m ./...
```

The full CI workflow also cross-compiles 32-bit targets, runs selected race
tests, and tests the PostgreSQL client module.

## Contribute

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you change a storage contract,
on-disk format, unsafe-code boundary, or benchmark claim.
