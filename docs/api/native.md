# Native Go API

[Documentation](../README.md) / [API guides](README.md) · [Development status](../status.md)

Use `github.com/thesyncim/vibedb` when an application wants VibeDB to own an
embedded database lifecycle. It provides named JSON collections, exact indexes,
typed queries, and serializable transactions without exposing storage pages or
snapshot leases.

This guide describes the root `vibedb` package only. The `store` and
`store/durable` packages are lower-level engines with different JSON,
ownership, snapshot, indexing, and lifecycle contracts. Read [Low-level storage
engines](../store.md) before using either directly.

## Open and close a database

`Open` uses the `Durable` profile by default and owns the directory, collection
files, recovery journals, transaction marker, descriptors, and writer locks
behind the returned database.

```go
package main

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() (err error) {
	db, err := vibedb.Open("./data")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	users := db.Collection("users")
	created, err := users.Put(
		"user:42",
		[]byte(`{"name":"Ada","active":true}`),
	)
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

Do not read, rename, replace, copy, truncate, or delete files inside an open
database directory. Close the complete database before treating the directory
as a backup unit. `Close` is required for resource release even in the
`Durable` profile.

## Choose a durability profile

Select a profile with `WithDurability` or as the `Durability` field of
`AdvancedOptions`.

| Profile | Successful mutation means | Multi-collection transaction |
| --- | --- | --- |
| `Durable` | Its recovery record passed the power-safe fence before visibility and acknowledgement | Supported |
| `Buffered` | Its new generation is visible in this process; it may be lost before a successful `Flush` or `Close` | One dirty collection only |
| `Memory` | Its new generation is visible in memory; `Open` ignores the path and performs no filesystem operation | Supported; no crash persistence |

Buffered acknowledgement does not include a durability fence for that
mutation. It also does not mean “no I/O”: lazy creation and journal preparation
can create, allocate, or synchronize metadata.

The durability wording describes the implemented fence, not a certification of
the filesystem, controller, device cache, hypervisor, or power-loss behavior.
See [Durability and recovery](../durability.md) for the failure model.

## Configure `Open`

Most callers need only `WithDurability`. `WithAdvancedOptions` replaces the
complete advanced configuration; when options are combined, `Open` applies
them from left to right.

| `AdvancedOptions` field | Purpose |
| --- | --- |
| `Durability` | Selects `Durable`, `Buffered`, or `Memory` |
| `Engine` | Supplies low-level collection schema, index, geometry, and resource settings for newly created collections |
| `FileMode` | Permissions for newly created files; zero selects `0600` |
| `DirMode` | Permissions for newly created directories; zero selects `0700` |
| `TxnLimits` | Bounds dirty collections, staged documents, and staged bytes across one transaction |

Important validation rules:

- `Durable` and `Buffered` require a nonempty path. `Memory` ignores its path.
- The selected profile owns `Engine.Durability`; a conflicting engine mode is
  rejected. The facade also rejects `Engine.RecoveryJournal` because it would
  change the selected acknowledgement contract.
- `Memory` accepts only `Engine.Collection`; disk-specific engine settings and
  file modes are rejected.
- Invalid options are rejected before `Open` creates, truncates, or locks
  filesystem state.
- Existing durable collections retain their persisted immutable contract. A
  zero-option reopen adopts persisted key and document limits.
- Treat configuration values as immutable after `Open`. The facade freezes
  schema and exact-index definitions, but this development snapshot has a
  [known shallow-copy defect](../status.md#known-limitations) for
  `Engine.SkipIndexes` used by later lazy collections.

`Engine` is intentionally an expert escape hatch. In particular, do not enable
`OpaqueValues` through the root facade: current direct and transactional facade
writes do not apply one consistent opaque-value rule. Use `store/durable`
directly if uninterpreted byte values are required.

## Use lazy collections

`Database.Collection(name)` returns the same pointer for each valid name while
the database is open. It performs no I/O and does not create storage. Reads from
an absent collection behave like reads from an empty collection; the first
valid `Put`, a successful transaction that writes it, or `CreateIndex`
materializes it.

Name validation is deferred because `Collection` cannot return an error. A data
operation on an invalid handle returns `ErrInvalidCollectionName`. A portable
name is nonempty valid UTF-8 and at most `MaxCollectionNameBytes` (currently 120
bytes). Avoid NUL: its handling is not yet consistent between memory and disk
layers.

## Read and write JSON

The facade stores one nonempty JSON value under each nonempty key. `Put`
validates and canonicalizes the complete value, then inserts or replaces it
atomically.

```go
created, err := users.Put("user:42", []byte(`{
  "active": true,
  "name": "Ada"
}`))

deleted, err := users.Delete("user:42")
```

- `Put` returns `created == true` only when the key was absent in the operation's
  view.
- `Delete` returns `deleted == false` for an absent key and does not create a
  lazy collection.
- Invalid JSON, a schema violation, or an admission refusal publishes nothing.
  An invalid first write does not create collection files.
- `Get` returns caller-owned canonical JSON. A miss is `(nil, false, nil)`.
- `Append` appends an owned value to caller-provided storage. A miss leaves the
  destination unchanged.
- `Range` visits one immutable collection generation. Its key and document are
  borrowed, read-only views valid only during the callback; copy either before
  retaining it.
- A callback error stops `Range` and is returned unchanged.

Do not depend on `Range` order. The facade makes no portable ordering promise:
the memory and durable profiles currently traverse different physical orders.
Use a typed query with `OrderBy` when order is part of the result contract.

The default key limit is 256 bytes and the default document limit is 4 MiB.
Empty keys return `ErrKeyTooLarge`; empty documents return
`ErrDocumentTooLarge`. See [Data model](../data-model.md) for canonical JSON,
schemas, names, and exact value semantics.

## Create an exact index

`CreateIndex` builds one non-unique exact scalar or compound index and returns
only after its facade-visible build completes.

```go
if err := users.CreateIndex("by_team", "/team"); err != nil {
	return err
}

if err := users.CreateIndex("by_team_and_active", "/team", "/active"); err != nil {
	return err
}
```

An index has one to four distinct RFC 6901 JSON Pointer paths. Path order is
significant for a compound index. Missing, unresolvable, array, and object
values are omitted; scalar candidates are rechecked against source documents,
so an index changes the access path rather than query results.

The facade has no `DropIndex` or unique-index method. Applications that need
lower-level index DDL must accept the direct engine's separate ownership and
stability contract.

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
`query.Result`. Call `Release` when finished so retained result and execution
storage can be dropped. A nil compiled query returns `ErrInvalidQuery`; querying
an absent lazy collection returns an ordinary empty result.

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
session-owned workspace remain valid only until the next `Run` or `Release`.
A session is single-consumer and must not be copied or used concurrently. A
compiled query is immutable after compilation and may be shared; concurrent
execution needs an independent session per goroutine.

See [Typed query API](query.md) for builders, result cells, joins, execution
budgets, and direct-source ownership.

## Run serializable transactions

Use `Update` for a read-write transaction and `View` for a coherent read-only
database cut.

```go
err := db.Update(func(tx *vibedb.Tx) error {
	users := tx.Collection("users")
	audit := tx.Collection("audit")

	if _, err := users.Put("user:42", updatedUser); err != nil {
		return err
	}
	_, err := audit.Put("event:9001", auditEvent)
	return err
})
```

`Update` commits only when the callback returns nil. It rolls back on a returned
error and rolls back before re-panicking. It does not retry conflicts. `View`
uses the same `Get`, `Append`, `Range`, and `Run` vocabulary, but `Put` and
`Delete` return `ErrTxReadOnly`.

Use `Begin` or `BeginReadOnly` when the caller must control `Commit` and
`Rollback`. Do not let `Tx` or `TxCollection` escape their lifetime: after
commit or rollback, operations return `ErrTxDone`. Nested `Update` or `View` on
the same goroutine is refused with `ErrTxNested`; there are no native
savepoints.

Transactions read one coherent begin cut plus their staged overlay. Commit
checks point reads, absent-key reads, scans, queries, phantoms, ABA writes, and
lazy-collection races. `ErrTxConflict` publishes nothing; retry the complete
operation with a new transaction.

Profile support differs at publication time: `Buffered` refuses a transaction
that dirties two or more collections with `ErrTxUnsupportedLane`. `Durable` and
`Memory` support bounded multi-collection publication. Read-only and empty
transactions do not materialize lazy collections.

See [Transactions](../transactions.md) for serializability, retries, admission
limits, and crash-atomic multi-collection commit.

## Know the default limits

| Scope | Limit | Default |
| --- | --- | ---: |
| Collection name | UTF-8 bytes | 120 |
| Point operation | Key bytes | 256 |
| Point operation | JSON document bytes | 4 MiB |
| Exact index | Ordered paths | 1–4 |
| One dirty collection in a transaction | Distinct staged keys | 64 |
| One dirty collection in a transaction | Staged key and document bytes | 16,793,600 |
| Whole transaction | Dirty collections | 16 |
| Whole transaction | Distinct staged keys | 256 |
| Whole transaction | Staged key and document bytes | 67,174,400 |
| Whole read-write transaction | Exact read keys before coarse escalation | 4,096 |
| Whole read-write transaction | Retained exact-key bytes before coarse escalation | 1 MiB |
| Whole read-write transaction | Collections with read dependencies | 128 |

`AdvancedOptions.TxnLimits` changes the three whole-transaction write limits;
it does not change per-collection batch bounds or read-dependency bounds. Query
execution and result materialization have separate limits described in the
[query guide](query.md).

> [!WARNING]
> This snapshot has a known bounds mismatch: after reopening a collection with
> custom persisted key or document limits, direct operations use the persisted
> limits but transactional operations use the database-open limits. Avoid that
> configuration until the defect in [current status](../status.md) is fixed.

## Flush, observe, and close

`Collection.Flush` makes that collection's currently visible generation
recoverable. `Database.Flush` attempts every materialized collection and
returns the first mapped error, but it is not a coherent database-wide
persistence cut: concurrent writers can publish around its per-collection
walk. Flush is a no-op for `Memory` and for an unmaterialized lazy collection.

`Collection.Metrics` returns a detached snapshot:

| Field | Meaning |
| --- | --- |
| `Durability` | Selected facade profile |
| `Documents` | Documents in the sampled generation |
| `PublishedGeneration` | Per-collection reader-visible publication counter |
| `DurableGeneration` | Recoverable generation; zero for `Memory`, and possibly behind publication for `Buffered` |

Generation is observability data, not a database revision, transaction ID,
wall clock, or application version.

`Database.Close` closes admission, synchronizes as required by the profile, and
releases all database-owned resources. It is idempotent after teardown
completes. Collections returned by `Database.Collection` are managed handles;
calling their `Close` returns `ErrManagedCollection`.

A close attempt can return an error before every lease or writer lock is
released. `CloseCompleted` distinguishes incomplete teardown from a completed
close carrying a sticky persistence error. Release the blocker and call
`Close` again only when completion is false. Once close begins, data operations
remain closed and return `ErrClosed`.

### Handle persistence errors and unknown outcomes

Keep publication failure separate from ordinary validation:

- Validation, schema, index-definition, admission, and transaction-conflict
  errors publish no requested logical mutation.
- `ErrCommitOutcomeUnknown` is the ambiguous durable decision-fence window for
  a multi-collection commit. Stop writes, close the complete database, and
  reopen it to recover either all participants or none before inspecting state
  or retrying.
- Other I/O or durability-fence errors can poison a writer even when the API
  cannot prove what reached stable storage. Do not blindly submit different
  data. Close and reopen the owned database, inspect application identity, and
  retry only through an idempotent policy.

## Borrow one file instead of a directory

`OpenFile` is the standalone facade for one durable collection. Use it only
when the application must own the primary `*os.File` descriptor.

The descriptor must name a regular, non-symlink file through a stable nonempty
absolute path. The caller retains ownership but lends it exclusively to the
collection: keep it open and do not read, write, seek, truncate, lock, rename,
replace, or unlink it until `Collection.Close` completes. The parent directory
must grant the engine create, read, write, and sync authority for the
`.rjournal` sibling.

`Collection.Close` flushes and releases engine resources but does not close the
caller-owned primary descriptor. Check `CloseCompleted`, retry incomplete
teardown if necessary, and only then close the file. `OpenFile` rejects the
`Memory` profile and file/directory permission options. It does not provide a
database catalog or multi-collection transactions.

## Ownership and concurrency reference

| Value or bytes | Owner and concurrency rule |
| --- | --- |
| `*Database` | Owns catalog resources; concurrent operations are supported; do not copy after first use |
| `*Collection` from `Database.Collection` | Stable managed handle; use the pointer, do not copy it; close through the database |
| Standalone `*Collection` from `OpenFile` | Owns the exclusive engine borrow, not the primary descriptor; close it explicitly |
| `Get` / `Append` bytes | Caller-owned and valid across later writes and close |
| `Range` callback bytes | Borrowed, read-only, callback-lifetime only |
| `query.Result` from one-off `Run` | Caller releases it |
| `*Session` and its result | Single-consumer; result invalidated by the next run or release |
| `*Tx` / `*TxCollection` | Transaction-lifetime, single-consumer handles; inert after finish |
| Compiled `*query.Query` | Reusable and concurrency-safe; each concurrent session remains independent |

Readers use immutable generations. Writes to one collection are serialized at
publication. Transaction commits are serialized per database, but a commit's
collection fences cover only its participants, so unrelated direct writes need
not wait. Operations admitted concurrently with `Close` either complete safely
or return `ErrClosed`.

## Match errors by identity

Use `errors.Is`; do not compare error text. Validation and engine errors not
listed here can propagate through the facade with their typed identity intact.

| Error | Action |
| --- | --- |
| `ErrInvalidOptions` | Correct the configuration; invalid `Open` does not touch storage |
| `ErrInvalidCollectionName` | Correct the logical name |
| `ErrKeyTooLarge` / `ErrDocumentTooLarge` | Supply a nonempty value within the selected collection contract |
| `ErrInvalidQuery` | Supply a non-nil compiled query |
| `ErrManagedCollection` | Close the owning database, not its child handle |
| `ErrClosed` | Stop using the handle; finish or retry teardown as appropriate |
| `ErrTxConflict` | Retry the whole transaction against a fresh cut |
| `ErrTxTooLarge` | Reduce the transaction or deliberately change its configured limits |
| `ErrTxReadOnly` | Remove the mutation from `View` / `BeginReadOnly` |
| `ErrTxUnsupportedLane` | Use one dirty collection or a supported profile |
| `ErrTxNested` | Compose in one transaction; native savepoints do not exist |
| `ErrTxDone` | Discard the finished transaction handle |
| `ErrCommitOutcomeUnknown` | Close and reopen the complete database before inspecting or retrying |

Malformed JSON and schema/index failures use lower-level typed errors, such as
`store.ErrSchemaViolation`, `store.ErrIndexDefinition`, and `store.ErrIndexExists`.
They remain safe to match through `errors.Is`.

## Source map

- Open, profiles, options, CRUD, indexes, metrics, and lifecycle: [vibedb.go](../../vibedb.go)
- Query execution and session ownership: [vibedb_query.go](../../vibedb_query.go)
- Transactions, limits, conflicts, and commit outcomes: [vibedb_txn.go](../../vibedb_txn.go)
- Facade contract tests: [vibedb_test.go](../../vibedb_test.go), [vibedb_txn_test.go](../../vibedb_txn_test.go)
- Serializable conflict tests: [vibedb_txn_serializable_test.go](../../vibedb_txn_serializable_test.go)
- Close retry and close-race tests: [vibedb_lifecycle_internal_test.go](../../vibedb_lifecycle_internal_test.go)
- Executable profile matrix: [capability_matrix_facade_test.go](../../capability_matrix_facade_test.go)
