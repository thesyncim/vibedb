# Install and run VibeDB

This tutorial creates a durable database, writes a JSON document, reads it, and
uses a transaction. It uses the native facade.

## 1. Create a module

```bash
mkdir vibedb-example
cd vibedb-example
go mod init example.com/vibedb-example
go get github.com/thesyncim/vibedb@2ebcdff1047d
```

VibeDB requires Go 1.26. Use the tested revision shown above.

## 2. Add the program

Create `main.go`:

```go
package main

import (
	"errors"
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
	created, err := users.Put("user:1", []byte(`{"name":"Ada","visits":1}`))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("created: %v\n", created)

	err = db.Update(func(tx *vibedb.Tx) error {
		accounts := tx.Collection("accounts")
		audit := tx.Collection("audit")

		if _, err := accounts.Put("account:1", []byte(`{"balance":90}`)); err != nil {
			return err
		}
		_, err := audit.Put("entry:1", []byte(`{"account":"account:1","delta":-10}`))
		return err
	})
	if errors.Is(err, vibedb.ErrTxConflict) {
		log.Fatal("transaction conflict: retry the complete transaction")
	}
	if err != nil {
		log.Fatal(err)
	}

	document, found, err := users.Get("user:1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("found: %v document: %s\n", found, document)
}
```

## 3. Run the program

```bash
go run .
```

The default `Durable` profile creates `./data` as a database directory. A
collection is lazy. VibeDB creates its file when the first mutation uses it.

`Put` returns `created=true` when the key did not exist. `Get` returns a copy
that the caller owns. Always check both `found` and `err`.

## 4. Select a durability profile

The zero value and the default are `vibedb.Durable`.

```go
db, err := vibedb.Open("./data", vibedb.WithDurability(vibedb.Buffered))
```

The profiles have different acknowledgement contracts:

| Profile | Visibility and persistence |
| --- | --- |
| `Durable` | A successful mutation is power-safe before reader visibility. |
| `Buffered` | A successful mutation is visible from bounded memory. A successful `Flush`, or a successfully completed `Close`, makes the included visible generation crash-safe. |
| `Memory` | All state stays in process memory. `Open` ignores its path. |

Call `Close` for every profile. `Close` is idempotent. A buffered `Close`
attempts the final checkpoint.

## 5. Choose the next guide

- Use [the native API](api/native.md) for CRUD, indexes, and transactions.
- Use [typed queries](api/query.md) for programmatic filtering and aggregation.
- Use [SQL](api/sql.md) for `database/sql` applications.
- Use [the PostgreSQL wire server](api/pgwire.md) for PostgreSQL clients.
- Read [durability and recovery](durability.md) before you select advanced
  storage options.

## Implementation references

- `vibedb.go`: `Open`, `Collection`, `Put`, `Get`, `Flush`, and `Close`
- `vibedb_txn.go`: `Update`, `Begin`, `Commit`, and transaction errors
- `vibedb_test.go`: `Example` and facade lifecycle tests
