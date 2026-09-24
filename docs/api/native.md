# Native Go API

[Documentation](../README.md) / [API guides](README.md) · [Development status](../status.md)

Use `github.com/thesyncim/vibedb` when an application wants VibeDB to own an
embedded database lifecycle. It provides named JSON collections, exact
secondary indexes, full-text (tin) indexes, typed queries, and serializable
transactions without exposing storage pages or snapshot leases.

This guide covers the root `vibedb` package. The `store` and `store/durable`
packages are lower-level engines with different JSON, ownership, snapshot,
indexing, and lifecycle contracts; read [storage engines](../store.md) before
using either directly. The SQL driver keeps its own catalog format; a
database opened with `vibedb.Open` is not a SQL catalog.

## Open and close a database

`Open` uses the `Durable` profile by default. It owns the directory, the
collection files, their recovery journals, the transaction decision log, the
descriptors, and the writer locks behind the returned database.

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

Expected output:

```text
created=true found=true doc={"active":true,"name":"Ada"}
```

Object members come back sorted because the facade stores canonical JSON; see
[canonical JSON](../data-model.md#canonical-json).

A durable collection named `users` is stored as `c-7573657273.vjc` (the
hex-encoded name) plus `c-7573657273.vjc.rjournal`. A database that has run a
multi-collection transaction also owns `txn.vtm`. Do not read, rename,
replace, copy, truncate, or delete files inside an open database directory.
Close the complete database before treating the directory as a backup unit;
see [embedded backup](../operations/embedded-backup.md). `Close` is required
for resource release in every profile.

## Choose a durability profile

Select a profile with `WithDurability` or the `Durability` field of
`AdvancedOptions`.

| Profile | Successful mutation means | Multi-collection transaction |
| --- | --- | --- |
| `Durable` | Its recovery record passed the power-safe fence before visibility and acknowledgement | Supported, crash-atomic |
| `Buffered` | Its new generation is visible in this process; it can be lost until a successful `Flush` or `Close` | Refused with `ErrTxUnsupportedLane` |
| `Memory` | Its new generation is visible in process memory; `Open` ignores the path and performs no filesystem operation | Supported, visibility-atomic, no persistence |

`Buffered` acknowledgement includes no durability fence for that mutation, but
it is not a promise of zero I/O: lazy creation and journal preparation can
create, allocate, or synchronize metadata.

The durability wording describes the implemented fence. It is not a
certification of the filesystem, controller, device cache, hypervisor, or
power-loss behavior. See [durability and recovery](../durability.md).

## Configure `Open`

Most callers need only `WithDurability`. `WithAdvancedOptions` replaces the
complete advanced configuration; combined options apply from left to right.

| `AdvancedOptions` field | Purpose |
| --- | --- |
| `Durability` | Selects `Durable`, `Buffered`, or `Memory` |
| `Engine` | Low-level `durable.Options` (schema, exact indexes, geometry, resource bounds) for newly created collections |
| `FileMode` | Permissions for newly created files; zero selects `0600` |
| `DirMode` | Permissions for newly created directories; zero selects `0700` |
| `TxnLimits` | Whole-transaction bounds on dirty collections, staged documents, and staged bytes |

Validation rules:

- `Durable` and `Buffered` require a nonempty path. `Memory` ignores it.
- The selected profile owns `Engine.Durability`; a conflicting engine mode is
  rejected. `Engine.RecoveryJournal` is rejected because it would change the
  selected acknowledgement contract.
- `Memory` accepts only `Engine.Collection`; disk-specific engine settings and
  file modes are rejected.
- Invalid options return `ErrInvalidOptions` before `Open` creates, truncates,
  or locks filesystem state. A nil `Option` is also invalid.
- `Open` copies and freezes the schema and index definitions it is given.
- Existing durable collections keep their persisted contract. A zero-option
  reopen adopts each collection's persisted key and document limits, and
  transactions enforce the same persisted limits as direct writes.

`Engine` is an expert escape hatch. Do not enable `Engine.OpaqueValues`
through the facade: direct, lazy, and transactional facade writes do not apply
one consistent opaque-value rule. Use `store/durable` directly for
uninterpreted byte values.

## Use lazy collections

`Database.Collection(name)` returns the same pointer for a valid name while
the database is open. It performs no I/O and creates no storage. Reads from an
absent collection behave like reads from an empty one. The first valid `Put`,
a committed transaction that writes it, `CreateIndex`, or `CreateTinIndex`
materializes it.

`Collection` cannot return an error, so name validation is deferred: every
data operation on an invalid handle returns `ErrInvalidCollectionName`. A
valid name is nonempty, valid UTF-8, contains no NUL, and is at most
`MaxCollectionNameBytes` (120) bytes. Names are logical strings: separators,
trailing dots or spaces, and distinct Unicode normalization forms are legal
and remain distinct.

## Read and write JSON

The facade stores one nonempty JSON value under each nonempty key.

```go
created, err := users.Put("user:42", []byte(`{"active": true, "name": "Ada"}`))
deleted, err := users.Delete("user:42")
```

- `Put` validates and canonicalizes the complete value, then inserts or
  replaces it atomically. `created` is true only when the key was absent.
- `Delete` returns `deleted == false` for an absent key and does not create a
  lazy collection.
- Invalid JSON, a schema violation, or an admission refusal publishes nothing.
  An invalid first write does not create collection files.
- `Get` returns caller-owned canonical JSON. A miss is `(nil, false, nil)`.
- `Append` appends a caller-owned value to `dst`. A miss leaves `dst`
  unchanged.
- `Range` visits one immutable generation. Its key and document are borrowed
  read-only views, valid only during the callback; copy either before
  retaining it. A callback error stops `Range` and is returned unchanged.

`Range` order is not portable. Disk profiles visit keys in bytewise lexical
order; `Memory` visits physical chunk and slot order. Use a typed query with
`OrderBy` when order is part of the result.

Keys are limited to 256 bytes and documents to 4 MiB by default. An empty key
returns `ErrKeyTooLarge` and an empty document returns `ErrDocumentTooLarge`.
See the [data model](../data-model.md) for canonical JSON, schemas, and exact
value semantics.

## Create an exact index

`CreateIndex` builds one non-unique exact scalar or compound index and returns
after its build completes.

```go
if err := users.CreateIndex("by_team", "/team"); err != nil {
	return err
}
if err := users.CreateIndex("by_team_and_active", "/team", "/active"); err != nil {
	return err
}
```

An index has one to four distinct RFC 6901 JSON Pointer paths; order matters
for a compound index. Null, boolean, number, and string values are indexed.
Missing paths, arrays, and objects are omitted. Candidates are rechecked
against the documents, so an index changes the access path, never the result.

A failed build on `Memory` is rolled back so the name can be reused. The
facade has no `DropIndex` and no unique index; use `store/durable` or SQL for
those.

<a id="search-text-with-a-tin-index"></a>

## Search text

`CreateTinIndex(name, path)` declares a full-text index over one string path,
and `TinSearch(path, tinql, topK)` returns ranked hits:

```go
if err := docs.CreateTinIndex("body_tin", "/body"); err != nil {
	return err
}
hits, err := docs.TinSearch("/body", `luxury AND "vintage watches"`, 10)
if err != nil {
	return err
}
for _, hit := range hits {
	fmt.Println(hit.Key, hit.Score)
}
```

Tin indexes are rebuilt in memory for each new generation that is searched.
See [full-text search](search.md) for the query language, ranking, cost
model, and limitations.

## Run typed queries

Compile a reusable `*query.Query`, then choose one-off execution or a reusable
session.

```go
compiled := query.Select(query.Path("name")).
	Where(query.Cmp("active", query.Eq, true)).
	OrderBy("name", query.Asc)

result, err := users.Run(compiled)
if err != nil {
	return err
}
defer result.Release()
```

`Collection.Run` takes one fresh immutable generation and returns a one-off
`query.Result`. Call `Release` when finished. A nil query returns
`ErrInvalidQuery`; querying an absent lazy collection returns an empty
result.

For a hot loop, keep one session per consumer:

```go
session := users.NewSession()
defer session.Release()

result, err := session.Run(compiled)
if err != nil {
	return err
}
// Read or copy cells before this session's next Run.
_ = result
```

Each `Session.Run` takes a fresh generation. Its result pointer, cells, and
workspace remain valid only until the next `Run` or `Release`. A session is
single-consumer and must not be copied or shared. A compiled query is
immutable and may be shared; give each goroutine its own session.

The facade accepts builder queries only. To run SQL text, use the
[SQL driver](sql.md) or `query.PrepareStatement` over a lower-level source;
see the [query API](query.md).

## Run serializable transactions

Use `Update` for a read-write transaction and `View` for a coherent read-only
cut.

```go
err := db.Update(func(tx *vibedb.Tx) error {
	if _, err := tx.Collection("users").Put("user:42", updatedUser); err != nil {
		return err
	}
	_, err := tx.Collection("audit").Put("event:9001", auditEvent)
	return err
})
```

`Update` commits when the callback returns nil, rolls back on an error, and
rolls back before re-panicking. It does not retry conflicts. `View` offers the
same `Get`, `Append`, `Range`, and `Run` vocabulary; `Put` and `Delete` return
`ErrTxReadOnly`. `Begin` and `BeginReadOnly` give the caller control of
`Commit` and `Rollback`.

Committed read-write transactions are serializable: commit validates point
reads, absent-key reads, scans, queries, and lazy-collection creation, and
returns `ErrTxConflict` without publishing if any of them changed. The reads a
callback observes are provisional until commit succeeds: each collection is
snapshotted when the transaction first touches it, so reads from two
collections can come from different moments. Do not perform external side
effects based on reads inside `Update`. `View` reads one coherent database cut
captured at `BeginReadOnly`.

After commit or rollback, `Tx` and every `TxCollection` return `ErrTxDone`.
Reentering `Update` or `View` on the same goroutine returns `ErrTxNested`;
there are no native savepoints. See [transactions](../transactions.md) for
retries, admission limits, and the crash-atomic commit protocol.

## Know the default limits

| Scope | Limit | Default |
| --- | --- | ---: |
| Collection name | UTF-8 bytes | 120 |
| Point operation | Key bytes | 256 |
| Point operation | JSON document bytes | 4 MiB |
| Exact index | Ordered paths | 1–4 |
| Tin index | String paths | 1 |
| One dirty collection in a transaction | Distinct staged keys | 64 |
| One dirty collection in a transaction | Staged key and document bytes | 16,793,600 |
| Whole transaction | Dirty collections | 16 |
| Whole transaction | Distinct staged keys | 256 |
| Whole transaction | Staged key and document bytes | 67,174,400 |
| Whole read-write transaction | Exact read keys before coarse escalation | 4,096 |
| Whole read-write transaction | Retained read-key bytes before coarse escalation | 1 MiB |
| Whole read-write transaction | Collections with read dependencies | 128 |

`AdvancedOptions.TxnLimits` changes the three whole-transaction write limits.
Per-collection bounds come from each collection's `MaxBatchDocuments` and
`MaxBatchBytes`. Query execution has separate budgets described in the
[query guide](query.md#resource-controls).

## Flush, observe, and close

`Collection.Flush` makes that collection's visible generation recoverable.
`Database.Flush` flushes every materialized collection and returns the first
error. It is not a database-wide persistence cut: concurrent writers can
publish around its per-collection walk. Flush is a no-op for `Memory` and for
an unmaterialized collection.

`Collection.Metrics` returns a detached snapshot:

| Field | Meaning |
| --- | --- |
| `Durability` | Selected profile |
| `Documents` | Documents in the sampled generation |
| `PublishedGeneration` | Per-collection reader-visible publication counter |
| `DurableGeneration` | Recoverable generation; zero for `Memory`, and behind publication for `Buffered` until `Flush` or `Close` |

A generation is observability data, not a database revision, transaction ID,
or application version.

`Database.Close` closes admission, synchronizes as required by the profile,
and releases database-owned resources. It is idempotent once teardown
completes. Handles from `Database.Collection` are managed: their `Close`
returns `ErrManagedCollection`.

A close attempt can return an error before every lease or writer lock is
released. `CloseCompleted` separates incomplete teardown from a completed
close that carries a sticky persistence error. Release the blocker (for
example an open transaction) and call `Close` again only while completion is
false. After close begins, data operations return `ErrClosed`.

### Handle persistence errors and unknown outcomes

- Validation, schema, index-definition, admission, and conflict errors
  publish nothing.
- `ErrCommitOutcomeUnknown` is the ambiguous decision window of a
  multi-collection durable commit. Stop writing, close the complete database,
  and reopen it; recovery applies all participants or none.
- Other I/O or fence errors can poison a writer even though the API cannot
  prove what reached stable storage. Close and reopen, inspect state by
  application identity, and retry only through an idempotent policy.

## Borrow one file instead of a directory

`OpenFile(file, options)` opens one durable collection on a caller-owned
`*os.File`. Use it only when the application must own the primary descriptor.

The descriptor must name a regular, non-symlink file through a stable
absolute path that still resolves to the same inode. The caller lends it
exclusively: do not read, write, seek, truncate, lock, rename, replace, or
unlink it until `Collection.Close` completes. The engine creates
`<path>.rjournal` beside it, so the parent directory needs create, write, and
sync permission.

`Collection.Close` flushes and releases engine resources but does not close
the caller's descriptor. Check `CloseCompleted` before closing the file.
`OpenFile` rejects `Memory` and file-mode options. It has no catalog and no
transactions.

## Ownership and concurrency reference

| Value or bytes | Owner and concurrency rule |
| --- | --- |
| `*Database` | Owns catalog resources; safe for concurrent use; do not copy |
| `*Collection` from `Database.Collection` | Stable managed handle; safe for concurrent use; close through the database |
| Standalone `*Collection` from `OpenFile` | Owns the engine borrow, not the descriptor; close it explicitly |
| `Get` / `Append` bytes | Caller-owned; valid after later writes and close |
| `Range` callback bytes | Borrowed, read-only, callback lifetime |
| `query.Result` from `Run` | Caller releases it |
| `*Session` and its result | Single consumer; result invalidated by the next run or release |
| `*Tx` / `*TxCollection` | Single consumer; inert after finish |
| Compiled `*query.Query` | Immutable after first use; share across goroutines |

Readers use immutable generations and do not block writers. Writes to one
collection serialize at publication. A transaction that reads and writes only
one collection commits in parallel with transactions on other collections;
a transaction that spans collections takes a database-wide commit lock.
Operations admitted concurrently with `Close` either complete or return
`ErrClosed`.

## Match errors by identity

Use `errors.Is`; do not compare error text. Lower-level typed errors, such as
`store.ErrSchemaViolation`, `store.ErrIndexDefinition`, `store.ErrIndexExists`,
and `store.ErrIndexNotFound`, pass through the facade intact.

| Error | Action |
| --- | --- |
| `ErrInvalidOptions` | Correct the configuration; storage was not touched |
| `ErrInvalidCollectionName` | Correct the logical name |
| `ErrKeyTooLarge` / `ErrDocumentTooLarge` | Supply a nonempty value within the collection's limits |
| `ErrInvalidQuery` | Supply a non-nil compiled query |
| `ErrManagedCollection` | Close the owning database, not its child handle |
| `ErrClosed` | Stop using the handle; finish or retry teardown |
| `ErrTxConflict` | Retry the whole transaction |
| `ErrTxTooLarge` | Reduce the transaction or raise its configured limits |
| `ErrTxReadOnly` | Remove the mutation from `View` / `BeginReadOnly` |
| `ErrTxUnsupportedLane` | Write one collection per transaction, or use another profile |
| `ErrTxNested` | Compose work in one transaction |
| `ErrTxDone` | Discard the finished handle |
| `ErrCommitOutcomeUnknown` | Close and reopen the database before inspecting or retrying |

## Limitations

- No SQL text, joins across collections, or unique indexes through the
  facade; use the [SQL driver](sql.md) or [query API](query.md).
- No `DropIndex`, collection drop, or rename in the facade.
- No savepoints, automatic retries, or deadline/cancellation parameters on
  facade calls.
- `Range` order differs between profiles.
- Read-write transactions do not give a callback a coherent cross-collection
  snapshot; only a successful commit is serializable.
- `Buffered` cannot commit a transaction that writes two or more collections.
- Full-text indexes are rebuilt per searched generation; see
  [full-text search](search.md#limitations).
- There is no stable API or on-disk format across development revisions; see
  [stability](../status.md).

## Source map

- Open, profiles, options, CRUD, indexes, metrics, and lifecycle: [vibedb.go](../../vibedb.go)
- Query execution and session ownership: [vibedb_query.go](../../vibedb_query.go)
- Transactions, limits, conflicts, and commit outcomes: [vibedb_txn.go](../../vibedb_txn.go)
- Collection-name codec: [internal/collectionname/collectionname.go](../../internal/collectionname/collectionname.go)
- Facade contract tests: [vibedb_test.go](../../vibedb_test.go), [vibedb_txn_test.go](../../vibedb_txn_test.go), [vibedb_tin_test.go](../../vibedb_tin_test.go)
- Serializable conflict and cut tests: [vibedb_txn_serializable_test.go](../../vibedb_txn_serializable_test.go), [vibedb_txn_snapshot_internal_test.go](../../vibedb_txn_snapshot_internal_test.go)
- Close retry and close-race tests: [vibedb_lifecycle_internal_test.go](../../vibedb_lifecycle_internal_test.go)
- Executable profile matrix: [capability_matrix_facade_test.go](../../capability_matrix_facade_test.go)
