# Native embedded API

The root `vibedb` package provides an owned-lifecycle database of named JSON
collections. The default profile is durable.

## Open and close a database

```go
db, err := vibedb.Open("./data")
if err != nil {
	return err
}
defer db.Close()
```

`Open` validates all options before it accesses the filesystem. Durable and
buffered databases use the path as a directory. The memory profile ignores the
path.

Call `Close` when admission is stopped. `Close` is idempotent after teardown
finishes. If `CloseCompleted` is false after an error, release active resources
and call `Close` again.

Do not copy a database, collection, transaction, session, or snapshot value
after first use.

## Select a collection

```go
users := db.Collection("users")
```

`Collection` returns the same lazy handle for the same name. It performs no I/O
and creates no file. The first valid mutation creates a missing collection.

A facade collection name must be nonempty valid UTF-8 and at most 120 bytes.
The durable catalog encodes the UTF-8 bytes into a portable filename.

## Put, get, delete, and range

```go
created, err := users.Put("user:1", []byte(`{"name":"Ada"}`))
document, found, err := users.Get("user:1")
deleted, err := users.Delete("user:1")
```

The default key limit is 256 bytes. A key cannot be empty. The default document
limit is 4 MiB. A document cannot be empty and must be valid JSON.

`Put` stores canonical JSON. `Get` returns bytes that the caller owns.
`Append` adds an owned result to a caller buffer. A miss leaves that buffer
unchanged.

`Range` observes one immutable collection cut:

```go
err := users.Range(func(key string, document []byte) error {
	// document is valid only during this callback.
	return nil
})
```

Callback keys and documents are borrowed. Do not retain them. Durable range
order is lexical by key. Memory range order is stable chunk and slot order.
Do not depend on one common order across profiles.

Deleting a missing key is a successful no-op. Deleting from a missing lazy
collection does not create a file.

## Create an exact index

```go
err := users.CreateIndex("by-team-and-role", "/team", "/role")
```

Index paths use RFC 6901 JSON Pointer syntax. The index contains exact scalar
or compound values. The durable engine creates it online. The memory engine
rolls back the definition if backfill fails.

An index supplies candidates. Query execution still checks the full predicate.

## Run a typed query

```go
compiled := query.Select(query.Path("team"), query.Count()).
	GroupBy("team")

result, err := users.Run(compiled)
```

Use a session in a repeated loop:

```go
session := users.NewSession()
defer session.Release()

result, err := session.Run(compiled)
```

Each run takes a fresh snapshot. A session is single-consumer. `Session.Run`
returns a session-owned result for every profile. Do not retain the result or
its cells after the next run or release. Durable source cells are copied out of
storage pages, but this does not extend the session result lifetime. `Release`
is idempotent and makes the session unusable.

## Run a multi-collection transaction

```go
err := db.Update(func(tx *vibedb.Tx) error {
	accounts := tx.Collection("accounts")
	audit := tx.Collection("audit")

	if _, err := accounts.Put("account:1", []byte(`{"balance":90}`)); err != nil {
		return err
	}
	_, err := audit.Put("entry:1", []byte(`{"delta":-10}`))
	return err
})
```

`Update` commits on a nil callback result. It rolls back on an error. It also
rolls back before it propagates a panic.

`Begin` starts a serializable read-write transaction. `BeginReadOnly` and
`View` capture a coherent read-only cut. Transactions provide read-your-writes.

A conflict publishes nothing and returns `ErrTxConflict`. Retry the complete
transaction with application backoff.

The default cross-collection limits are:

| Resource | Maximum |
| --- | ---: |
| Dirty collections | 16 |
| Staged documents | 256 |
| Staged key and value bytes | 67,174,400 |

Each collection also permits at most 64 distinct staged keys and 16,793,600
staged key and value bytes.

Exact read dependency tracking uses at most 4096 keys or 1 MiB for each
collection. It then changes to a coarse collection dependency. At most 128
collections can have tracked read dependencies.

`Range` and query reads add coarse dependencies and detect phantoms. Lost
history in a participating collection causes a conservative conflict.

Native transactions do not have savepoints. Reentering `Update` or `View` on
the same goroutine returns `ErrTxNested`.

The buffered facade rejects a transaction with two or more dirty collections.
It returns `ErrTxUnsupportedLane`. The durable and memory profiles support the
facade multi-collection path with their documented crash boundaries.

## Observe metrics

```go
metrics, err := users.Metrics()
```

Metrics contain the profile, document count, visible generation, and durable
generation. Memory reports durable generation zero. Buffered visibility can be
ahead of durable generation.

## Flush buffered data

```go
if err := db.Flush(); err != nil {
	return err
}
```

`Flush` is a no-op for memory. For disk profiles, it attempts all open
collections and returns the first error.

`Database.Flush` is a per-collection walk, not a coherent cross-collection
snapshot. A concurrent write can publish after its collection has been
flushed. Quiesce writers when one database-wide persistence cut matters.

## Borrow a standalone file

`OpenFile` opens one disk collection from a caller-owned descriptor. The
descriptor must have an absolute stable name and identify a regular non-symlink
file. Its parent directory must be accessible. The primary basename plus
`.rjournal` must fit the portable 255-byte component limit.

The caller keeps descriptor ownership but lends it exclusively to the
collection. Do not read, write, seek, truncate, lock, rename, replace, unlink,
or close it until collection close finishes.

The collection owns a sibling recovery journal at
`file.Name()+".rjournal"`. Treat the primary and journal as one storage pair.

## Error handling

Use `errors.Is` with the stable facade errors:

- `ErrClosed`
- `ErrInvalidOptions`
- `ErrInvalidCollectionName`
- `ErrManagedCollection`
- `ErrKeyTooLarge`
- `ErrDocumentTooLarge`
- `ErrTxConflict`
- `ErrTxTooLarge`
- `ErrTxDone`
- `ErrTxReadOnly`
- `ErrTxUnsupportedLane`
- `ErrCommitOutcomeUnknown`
- `ErrTxNested`

An unknown commit outcome requires close and reopen. Do not retry the mutation
until recovery determines the committed state.

## Implementation references

- `vibedb.go`, `vibedb_query.go`, and `vibedb_txn.go`
- `vibedb_test.go` and `vibedb_txn_serializable_test.go`
- `internal/collectionname/collectionname.go`
