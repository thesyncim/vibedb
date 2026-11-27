# Get started with the embedded database

[Documentation](README.md) · [Stability](status.md)

Create a durable embedded database in Go, write JSON documents, read them back,
and verify persistence across close and reopen. No external server is required.

## Prerequisites

- Go 1.27+ (as declared in [`go.mod`](../go.mod))
- macOS or Linux with a writable working directory

## 1. Initialize a module

Pin the exact revision you are evaluating (data formats can change across
commits):

```sh
mkdir vibedb-start && cd vibedb-start
go mod init example.com/vibedb-start
go get github.com/thesyncim/vibedb@main
```

## 2. Write `main.go`

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
	seed := flag.Bool("seed", false, "write sample documents")
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

## 3. Run and verify reopen

Seed once, then run a read-only pass to confirm durability across restart:

```sh
go run . -seed
go run .
```

Both commands print:

```text
found=true document={"active":true,"name":"Ada"}
```

VibeDB canonicalizes JSON on write (sorted object keys, no extra whitespace).

Inspect `./data`:

```sh
ls data
# c-7573657273.vjc
# c-7573657273.vjc.rjournal
```

Collections are created on first write: `c-<hex-name>.vjc` holds pages and
`.rjournal` is the recovery journal. Back up the closed directory as a single
unit ([embedded backup](operations/embedded-backup.md)).

## Durability profiles and defaults

Default limits: keys ≤ 256 B (nonempty), JSON documents ≤ 4 MiB, collection
names ≤ 120 B UTF-8 without NUL. One process owns a data directory at a time.

Choose a durability profile via `vibedb.WithDurability(...)`:

| Profile | Visibility / crash contract | Persistence action |
| --- | --- | --- |
| `Durable` (default) | Synced with platform barrier before returning | Close cleanly when done |
| `Buffered` | Visible in-process; may lose un-flushed writes on crash | Call `Flush()` or `Close()` to persist |
| `Memory` | In-process only; path ignored | None |

## Closing safely

`Close` blocks new work on its first call. Release all query results, sessions,
and transactions before closing:

1. Call `Close()` and inspect any returned error.
2. Use `CloseCompleted(err)` to distinguish a retryable close (open handles
   remain) from a terminal close with a sticky persistence error.
3. After a persistence error, reopen and inspect state before assuming whether
   a write committed.

## Next steps

- [Native API](api/native.md) — exact indexes, queries, transactions, lifecycle
- [Full-text search](api/search.md) — `tin` indexes and BM25 ranking
- [SQL API](api/sql.md) and [PostgreSQL wire](api/pgwire.md)
- [Local RF3 cluster](operations/local-cluster.md) — run a 3-node replicated cluster

## Source map

- [vibedb.go](../vibedb.go) · [internal/collectionname/collectionname.go](../internal/collectionname/collectionname.go)
- [vibedb_test.go](../vibedb_test.go) · [vibedb_lifecycle_internal_test.go](../vibedb_lifecycle_internal_test.go)
