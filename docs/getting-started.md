# Get started with the embedded database

[Documentation](README.md) · [Development status](status.md)

In this tutorial you will create a durable database, write two JSON documents,
read one back, and reopen the database without rewriting it.

## Prerequisites

- The Go toolchain declared by [`go.mod`](../go.mod)
- A new Go module
- A local directory that the process may create

Add VibeDB at an exact commit:

```bash
go mod init example.com/vibedb-start
go get github.com/thesyncim/vibedb@a0de0919
```

## Create the program

Save this as `main.go`:

```go
package main

import (
	"errors"
	"flag"
	"fmt"

	"github.com/thesyncim/vibedb"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() (err error) {
	seed := flag.Bool("seed", false, "write the tutorial documents")
	flag.Parse()

	db, err := vibedb.Open("./data")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	users := db.Collection("users")
	if *seed {
		for key, doc := range map[string]string{
			"user:1": `{"name":"Ada","active":true}`,
			"user:2": `{"name":"Linus","active":false}`,
		} {
			if _, err := users.Put(key, []byte(doc)); err != nil {
				return err
			}
		}
	}

	doc, found, err := users.Get("user:1")
	if err != nil {
		return err
	}
	fmt.Printf("found=%t document=%s\n", found, doc)
	return nil
}
```

Seed the database, then reopen it without writing:

```sh
go run . -seed
go run .
```

Both runs should report `found=true` and the same stored document. The second
run only reads, so it checks that the first run's write survived close/reopen.
`Open` creates a database directory;
the collection is materialized only when the first valid write commits.

## Understand the defaults

The zero-value profile is `vibedb.Durable`:

- A successful mutation is recovery-safe before readers can observe it.
- Keys are nonempty and at most 256 bytes by default.
- Documents are nonempty complete JSON and at most 4 MiB by default.
- Stored JSON is canonicalized; byte spelling may differ from the input.
- Collection names are portable UTF-8 identities up to 120 bytes. Avoid NUL.

A durable collection can have a primary file plus a recovery-journal sidecar.
A database can also own a transaction decision log. Treat the complete closed
directory as the backup unit.

## Choose a different acknowledgement profile

```go
db, err := vibedb.Open("./data", vibedb.WithDurability(vibedb.Buffered))
```

| Profile | Successful mutation means | Persistence action |
| --- | --- | --- |
| `Durable` | Recovery record is power-safe before visibility | Default; still close the database |
| `Buffered` | Row is visible but can be lost on a crash | `Flush` or successful `Close` advances recoverable state |
| `Memory` | Row exists only in this process; path is ignored | None |

Buffered acknowledgement does not include a durability fence for that
mutation. It does **not** promise zero device I/O: first use can create and sync
metadata or a journal.

## Close correctly

`Close` stops new work on its first call. Teardown can return a retryable error
while a snapshot is still held, or a sticky persistence error after resources
were released. Application code should:

1. release query results, sessions, transactions, and snapshots;
2. call `Close` and inspect the error;
3. use `CloseCompleted` when deciding whether another close attempt is useful;
4. reopen before retrying a mutation whose persistence result was unknown.

The full lifecycle contract is in the [native API guide](api/native.md).

## Next steps

- [Data model and indexes](data-model.md)
- [Serializable transactions](transactions.md)
- [Typed queries](api/query.md)
- [SQL through `database/sql`](api/sql.md)
- [Durability and recovery](durability.md)

To explore RF3 replication, finish the embedded tutorial first and then use the
[generated local cluster](operations/local-cluster.md).

## Source map

- [vibedb.go](../vibedb.go): `Open`, `WithDurability`, `Collection`, `Put`, `Get`, `Close`
- [vibedb_test.go](../vibedb_test.go): durable profile CRUD, flush, close, and reopen cases
- [vibedb_lifecycle_internal_test.go](../vibedb_lifecycle_internal_test.go): retryable and completed close behavior
