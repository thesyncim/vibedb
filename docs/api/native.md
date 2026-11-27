# Native Go API

[Documentation](../README.md) / [API guides](README.md) · [Stability](../status.md)

`github.com/thesyncim/vibedb` is the recommended embedded Go interface. It owns
the database directory, collection files, recovery journals, transaction
decision log (`txn.vtm`), and writer locks—providing named JSON collections,
exact indexes, `tin` full-text indexes, typed queries, and serializable
transactions.

For low-level page/snapshot handles, read [storage engines](../store.md) first.
For SQL tables, use [SQL API](sql.md) (SQL catalogs use a separate directory
format).

## Open, write, read, and close

```go
package main

import (
	"errors"
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
	dir, err := os.MkdirTemp("", "vibedb-native-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	db, err := vibedb.Open(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	users := db.Collection("users")
	created, err := users.Put("user:42", []byte(`{"name":"Ada","active":true}`))
	if err != nil {
		return err
	}
	doc, found, err := users.Get("user:42")
	if err != nil {
		return err
	}
	fmt.Printf("created=%t found=%t doc=%s\n", created, found, doc)
	return nil
}
```

Output: `created=true found=true doc={"active":true,"name":"Ada"}` (object keys
are sorted because stored JSON is [canonicalized](../data-model.md#canonical-json)).

Never modify files inside an open directory (`c-<hex>.vjc`, `.rjournal`,
`txn.vtm`). Close the database before backing up the directory
([embedded backup](../operations/embedded-backup.md)).

## Durability profiles and options

| Profile | Successful mutation guarantees | Multi-collection transaction |
| --- | --- | --- |
| `Durable` (default) | Recovery record synced with platform barrier before visibility | Crash-atomic |
| `Buffered` | Visible in-process; recoverable after `Flush` or `Close` | Refused (`ErrTxUnsupportedLane`) |
| `Memory` | In-process only; path ignored, no filesystem I/O | Visibility-atomic |

Select a profile with `vibedb.WithDurability(p)` or configure
`vibedb.WithAdvancedOptions(vibedb.AdvancedOptions{...})`:

- `Durability` — `Durable`, `Buffered`, or `Memory`.
- `Engine` — `durable.Options` (schema, indexes, geometry, bounds) for newly
  created collections. Conflicting `Engine.Durability` or
  `Engine.RecoveryJournal` is rejected; `Memory` accepts only `Engine.Collection`.
  Do not set `Engine.OpaqueValues` through the facade.
- `FileMode` / `DirMode` — permissions for new files (`0600`) and dirs (`0700`).
- `TxnLimits` — whole-transaction dirty collection/key/byte bounds.

Invalid options return `ErrInvalidOptions` before touching disk. Existing
durable collections keep their persisted limits on reopen.

## Collections, CRUD, and scans

`db.Collection(name)` is lazy (no I/O until first valid `Put`, committed write,
`CreateIndex`, or `CreateTinIndex`). Names must be nonempty UTF-8 without NUL,
≤ 120 bytes (`ErrInvalidCollectionName` is deferred to data operations).

- `Put(key, doc)` — validates and canonicalizes JSON; returns `created == true`
  when inserting a new key. Invalid JSON/schema writes publish nothing.
- `Delete(key)` — returns `deleted == false` on miss; does not materialize a
  lazy collection.
- `Get(key)` / `Append(dst, key)` — return caller-owned canonical JSON; miss is
  `(nil, false, nil)`.
- `Range(fn)` — scans one immutable generation. Callback key/doc bytes are
  borrowed read-only views valid only during the callback; copy to retain.
  Order is bytewise lexical on disk and physical slot order on `Memory`—use a
  typed query with `OrderBy` when order matters.

## Exact and full-text indexes

```go
_ = users.CreateIndex("by_team", "/team")
_ = users.CreateIndex("by_team_active", "/team", "/active")
_ = users.CreateTinIndex("bio_tin", "/bio")
hits, _ := users.TinSearch("/bio", `compiler AND "systems"`, 10)
```

- **Exact indexes (`CreateIndex`):** 1–4 distinct RFC 6901 JSON Pointers;
  indexes null, bool, number, and string values (omits missing, arrays,
  objects). Candidates are always rechecked against the document. No
  `DropIndex` or unique indexes in the facade (use SQL or `store/durable`).
- **Full-text indexes (`CreateTinIndex` / `TinSearch`):** Single string path,
  BM25-ranked TINQL search, maintained incrementally across generations with a
  checksummed `.tin` sidecar. See [full-text search](search.md).

<a id="search-text-with-a-tin-index"></a>

## Typed queries and sessions

```go
compiled := query.Select(query.Path("name")).
	Where(query.Cmp("active", query.Eq, true)).
	OrderBy("name", query.Asc)

// One-off execution
res, err := users.Run(compiled)
defer res.Release()

// Reusable hot-loop session (one consumer per goroutine)
sess := users.NewSession()
defer sess.Release()
res, err = sess.Run(compiled) // valid until next sess.Run or sess.Release
```

Compiled `*query.Query` values are immutable and goroutine-safe; `*Session` is
single-consumer. The facade accepts builder queries only; use [SQL API](sql.md)
or `query.PrepareStatement` ([Query API](query.md)) for SQL text.

## Serializable transactions

```go
err := db.Update(func(tx *vibedb.Tx) error {
	if _, err := tx.Collection("users").Put("user:42", updatedUser); err != nil {
		return err
	}
	_, err := tx.Collection("audit").Put("event:9001", auditEvent)
	return err
})
```

- `Update` commits on nil return and rolls back on error or panic (does not
  auto-retry `ErrTxConflict`). `Begin` / `BeginReadOnly` give manual control.
- `View` captures one coherent database cut at begin; mutations return
  `ErrTxReadOnly`.
- Committed `Update` transactions are serializable (validating reads, misses,
  scans, queries, and lazy collection creation). Note: reads inside `Update`
  are provisional until commit succeeds (each collection snapshots on first
  touch), so avoid external side effects before commit.
- Nested transactions on the same goroutine return `ErrTxNested`; finished
  handles return `ErrTxDone`. See [transactions](../transactions.md).

## Default limits

| Scope | Limit | Default |
| --- | --- | ---: |
| Collection name / key / document | Bytes | 120 B / 256 B / 4 MiB |
| Exact index / Tin index | Paths | 1–4 / 1 |
| Per dirty collection in Tx | Staged keys / bytes | 64 / 16,793,600 B |
| Whole transaction | Dirty collections / keys / bytes | 16 / 256 / 67,174,400 B |
| Read-write Tx read tracking | Exact keys / bytes / collections | 4,096 / 1 MiB / 128 |

Query execution budgets are documented in [Query API](query.md#resource-controls).

## Flush, metrics, close, and `OpenFile`

- `Collection.Flush()` / `Database.Flush()` make visible generations durable
  (no-op on `Memory` or unmaterialized collections; `Database.Flush` is a
  per-collection walk, not a cross-collection cut).
- `Collection.Metrics()` returns `Durability`, `Documents`,
  `PublishedGeneration`, and `DurableGeneration` (observability only, not a
  transaction ID).
- `Database.Close()` stops admission, flushes as needed, and releases locks.
  Check `CloseCompleted(err)` to distinguish retryable teardown (release open
  transactions/snapshots and retry) from a completed close with a sticky
  persistence error.
- `OpenFile(file, opts)` lends a caller-owned `*os.File` exclusively for one
  standalone durable collection (creates `<path>.rjournal`; rejects `Memory`,
  no catalog or transactions; check `CloseCompleted` before closing `file`).

## Ownership and errors

| Handle / bytes | Rule |
| --- | --- |
| `*Database`, managed `*Collection`, compiled `*query.Query` | Goroutine-safe; close managed collections via `Database.Close` |
| `Get` / `Append` bytes | Caller-owned |
| `Range` bytes, `Session` result | Borrowed; valid only during callback or until next `Run`/`Release` |
| `*Session`, `*Tx`, `*TxCollection` | Single-consumer |

Match errors with `errors.Is` (lower-level `store.Err*` errors pass through):

| Error | Action |
| --- | --- |
| `ErrInvalidOptions`, `ErrInvalidCollectionName`, `ErrInvalidQuery` | Fix caller input; nothing published |
| `ErrKeyTooLarge`, `ErrDocumentTooLarge`, `ErrTxTooLarge` | Shrink payload or raise configured limits |
| `ErrTxConflict` | Retry the full transaction |
| `ErrTxReadOnly`, `ErrTxUnsupportedLane`, `ErrTxNested`, `ErrTxDone`, `ErrManagedCollection` | Fix transaction/lifecycle usage |
| `ErrClosed` | Stop using handle; finish teardown |
| `ErrCommitOutcomeUnknown` | Stop writes, close and reopen DB before reconciling |

## Limitations

- No SQL text, joins, unique indexes, `DropIndex`, collection drop/rename,
  savepoints, or context deadlines in the root facade.
- `Range` order differs between disk (lexical) and `Memory` (physical slot).
- `Buffered` rejects multi-collection write transactions.

## Source map

- [vibedb.go](../../vibedb.go) · [vibedb_query.go](../../vibedb_query.go) · [vibedb_txn.go](../../vibedb_txn.go)
- [internal/collectionname/collectionname.go](../../internal/collectionname/collectionname.go)
- Tests: [vibedb_test.go](../../vibedb_test.go) · [vibedb_txn_serializable_test.go](../../vibedb_txn_serializable_test.go) · [vibedb_lifecycle_internal_test.go](../../vibedb_lifecycle_internal_test.go) · [capability_matrix_facade_test.go](../../capability_matrix_facade_test.go)
