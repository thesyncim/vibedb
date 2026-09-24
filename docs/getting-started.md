# Get started with the embedded database

[Documentation](README.md) · [Development status](status.md)

In this tutorial you create a durable database, write two JSON documents, read
one back, and reopen the database without rewriting it. It takes a few minutes
and needs no server.

## Prerequisites

- The Go toolchain declared by [`go.mod`](../go.mod) (currently Go 1.27)
- An empty working directory that the process may write
- macOS or Linux; the default `Durable` profile does not need Linux-only
  allocation support

## Create the module

Run these commands in the empty directory. Pin an exact revision: VibeDB has no
stable API or on-disk format between revisions, so reopen data only with the
build that wrote it.

```sh
go mod init example.com/vibedb-start
go get github.com/thesyncim/vibedb@47ac5ab23
```

## Write the program

Save this as `main.go`:

```go
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/thesyncim/vibedb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
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

## Run it

Seed the database, then run again without writing:

```sh
go run . -seed
go run .
```

Both runs print:

```text
found=true document={"active":true,"name":"Ada"}
```

The second run only reads, so it proves that the first run's write survived
close and reopen. The stored document has sorted members and no whitespace:
VibeDB stores canonical JSON, so byte spelling can differ from the input.

## Verify what was created

```sh
ls data
```

```text
c-7573657273.vjc
c-7573657273.vjc.rjournal
```

`Open` created the `data` directory. The `users` collection was created by the
first successful write: its primary file name is the hex-encoded collection
name, and the `.rjournal` sibling is its recovery journal. Treat the closed
`data` directory as one backup unit; see [embedded backup](operations/embedded-backup.md).
Delete `data` to start over.

## Understand the defaults

The default profile is `vibedb.Durable`:

- A successful mutation's recovery record is synchronized with the platform's
  power-safe barrier before readers can see it or the call returns.
- Keys are nonempty and at most 256 bytes.
- Documents are nonempty, complete JSON values of at most 4 MiB.
- Collection names are nonempty UTF-8 without NUL, at most 120 bytes.

To trade durability for write latency, choose another profile:

```go
db, err := vibedb.Open("./data", vibedb.WithDurability(vibedb.Buffered))
```

| Profile | Successful mutation means | Persistence action |
| --- | --- | --- |
| `Durable` | Recovery record is power-safe before visibility | Default; still close the database |
| `Buffered` | Visible in this process; can be lost on a crash | `Flush` or a successful `Close` makes it recoverable |
| `Memory` | Exists only in this process; the path is ignored | None |

## Close correctly

`Close` stops new work on its first call. It can return a retryable error while
a transaction or snapshot is still open, or a sticky persistence error after
resources were released. In application code:

1. release query results, sessions, and transactions;
2. call `Close` and inspect the error;
3. use `CloseCompleted` to decide whether another `Close` is useful;
4. after a persistence error, reopen before deciding whether a write happened.

## Current gaps

- The tutorial's API is a development interface; pin a revision.
- A database directory written by one revision is not guaranteed to open with
  another.
- One process owns a database directory at a time; there is no multi-process
  access or network server in this mode.

## Next steps

- [Native API](api/native.md): indexes, queries, transactions, and lifecycle
- [Data model](data-model.md): keys, canonical JSON, and schemas
- [Full-text search](api/search.md)
- [SQL through `database/sql`](api/sql.md)
- [Durability and recovery](durability.md)

To explore replication, finish this tutorial first and then use the
[local RF3 cluster](operations/local-cluster.md).

## Source map

- [vibedb.go](../vibedb.go): `Open`, `WithDurability`, `Collection`, `Put`, `Get`, `Close`
- [internal/collectionname/collectionname.go](../internal/collectionname/collectionname.go): collection file names
- [vibedb_test.go](../vibedb_test.go): profile CRUD, flush, close, and reopen cases
- [vibedb_lifecycle_internal_test.go](../vibedb_lifecycle_internal_test.go): retryable and completed close
