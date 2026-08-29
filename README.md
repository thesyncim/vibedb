# VibeDB

VibeDB is a durable embedded JSON database for Go with explicit operation,
cache, and concurrency limits. The root package provides named collections,
exact indexes, typed queries, and serializable transactions through a small
owned-lifecycle API. The default profile makes a mutation power-safe before it
becomes visible. Buffered and in-memory profiles are explicit alternatives with
different acknowledgement contracts.

The repository also contains a SQL runtime, a `database/sql` driver, and an
experimental PostgreSQL wire-protocol endpoint. They use VibeDB's documented
SQL subset. Wire compatibility does not imply PostgreSQL SQL compatibility.

> **Development status:** This repository has no tagged release or published
> support window. It also has no published project license. Files named
> `LICENSE-*` contain third-party notices only. APIs, command contracts, and
> storage grammars can change between tested commits. The PostgreSQL wire
> endpoint and all distributed commands are experimental and unreleased.

## Current scope

The credible product boundary today is the embedded engine. Its durable store
uses a fixed 4 KiB base-page format and a configurable page-cache and mutable
row-overlay budget. Indexed open also constructs a data-dependent resident
exact-index epoch outside that budget, so the current engine does not claim a
fixed total-memory ceiling. The `Durable` and `Memory` profiles support
multi-collection transactions. The `Buffered` facade deliberately refuses a
commit that dirties more than one collection.

The experimental distributed runtime is mode-dependent. Explicit
`-dev-static-catalog` mode routes supported SQL to static shard services.
Replicated-catalog mode routes the documented bounded `SELECT` plan subset to
RF3 leaders with `ReadIndex`. It also provides canonical point reads,
exact-primary-key read batches, and durable sequenced write batches for the
supported canonical mutation shapes, including complete-document and unique
top-level named-column INSERT rows. RF3 has catalog, request-ledger,
transaction, split, replica-move, backup, and restore primitives. It is not a
general distributed SQL or arbitrary cross-shard PostgreSQL transaction layer.
Network-serving commands require mutual TLS and an authorization policy by
default. `vibedb-gateway serve` and the static `vibedb-shard serve` command
permit explicit unauthenticated loopback development serving. The gateway flag
also applies with a replicated catalog and selects raw replicated-shard dialing;
`vibedb-shard serve-rf3` itself has no plaintext mode.

This source tree does not currently provide an AI control plane, an
object-storage durability layer, or evidence of near-linear horizontal
scaling. The competitive harness currently publishes no results, so this
documentation makes no "fastest" or infrastructure-cost claim. See the
[competitive benchmark policy](bench/competitive/README.md) and the current
[no-results status](bench/competitive/RESULTS.md).

The generated [distributed feature-state matrix](docs/distributed-feature-state.md)
distinguishes a present primitive from command integration and mandatory
external qualification. It does not turn an unexecuted or failing gate into
evidence.

## Requirements

- Go 1.26 or a compatible newer toolchain
- A supported operating system for the selected storage backend

The portable storage backend is the fallback. Linux can select `io_uring` and
direct I/O through the advanced storage API.

## Install

```bash
go get github.com/thesyncim/vibedb@main
```

There is no released version to select. Record the pseudo-version resolved into
`go.mod`, then test and pin that exact revision before deployment.

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
| Native facade | `github.com/thesyncim/vibedb` | Embedded JSON CRUD, exact indexes, typed queries, and transactions |
| Typed queries | `github.com/thesyncim/vibedb/query` | Programmatic query construction and reusable execution buffers |
| SQL | `github.com/thesyncim/vibedb/sql/driver` | Go applications that use `database/sql` |
| PostgreSQL wire | `github.com/thesyncim/vibedb/pgwire` | PostgreSQL protocol clients using the documented VibeDB SQL subset |
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
