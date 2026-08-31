# Data model

VibeDB stores keyed JSON values in named collections. This page defines the
application-visible model of the root `vibedb` package and calls out the places
where the two low-level storage packages deliberately differ.

> [!CAUTION]
> VibeDB is unreleased development software. Pin one exact Git commit: APIs,
> disk formats, file layouts, recovery behavior, commands, and wire behavior may
> break between any two commits. Do not store irreplaceable data in VibeDB.

## Keep the API layers separate

| Layer | Model | Intended use |
| --- | --- | --- |
| `vibedb` | Owned database lifecycle, canonical JSON, three durability profiles | Applications; this page uses this layer by default |
| `store` | Heap-resident engine, explicit immutable snapshots and engine geometry | Engine integration and specialized in-process workloads |
| `store/durable` | File-backed engine, explicit descriptor and snapshot ownership | Storage integrations that need low-level control |

A guarantee made by one layer does not automatically belong to another. In
particular, `store.Collection` does not promise the facade's canonical output,
and `store/durable.Options.OpaqueValues` is not part of the facade's JSON model.

## Logical shape

```text
Database
└── Collection name
    ├── Key → JSON value
    ├── Key → JSON value
    └── Exact indexes over JSON Pointer paths
```

- A database is a catalog of collections.
- A collection is a map from one key to one JSON value.
- A key is unique only within its collection.
- A `Put` for an existing key replaces the complete value atomically.
- A `Delete` of a missing key is a successful no-op.
- The facade has no foreign keys, cascades, or cross-collection uniqueness.

`Database.Collection(name)` returns a stable, lazy handle. Getting the handle
does no I/O and creates no collection. Reads from an absent collection behave as
empty reads; its first valid mutation creates it. `CreateIndex` also materializes
an absent collection.

## Names and keys

| Item | Facade rule | Default bound |
| --- | --- | ---: |
| Collection name | Non-empty, valid UTF-8, at most `MaxCollectionNameBytes` | 120 bytes |
| Key | Non-empty Go string; treated as bytes, not JSON | 256 bytes |
| JSON value | Non-empty, valid JSON, within the collection limit | 4 MiB |

The key and value bounds can be changed for newly created disk collections with
`AdvancedOptions.Engine`. A zero-option reopen adopts the bounds persisted in
an existing collection rather than overwriting them with facade defaults.

Collection names are logical strings, not path fragments. Durable catalogs
encode them into portable filenames; separators, trailing spaces, and distinct
Unicode normalization forms remain distinct names. Do not infer a collection
name from, or construct one of, those filenames.

For behavior that is portable across all current profiles, exclude NUL from
collection names. The facade and durable codec accept it as UTF-8, while the
low-level heap catalog currently rejects NUL-bearing names.

## JSON values

Any JSON root value is legal unless a schema narrows it: object, array, string,
number, boolean, or null. The facade validates every write and returns canonical
JSON bytes from `Get`, `Append`, and `Range`.

```go
created, err := users.Put("user:42", []byte(`{
  "name": "Ada",
  "active": true
}`))

doc, found, err := users.Get("user:42")
// doc is caller-owned canonical JSON.
```

Canonicalization is an encoding contract, not an application schema. Code that
needs a field to exist or have a particular type must define a schema or check
the value. Do not depend on the input's whitespace or other source spelling
surviving a facade write.

`store/durable` can instead persist non-empty opaque bytes. Opaque mode disables
JSON parsing, schemas, exact indexes, skip indexes, and JSON representation
options. Use that low-level API directly; do not enable opaque mode through
`vibedb.AdvancedOptions`, whose facade operations retain JSON semantics.

## Schemas

A schema is compiled once with `store.CompileSchema` and supplied in collection
options. It can constrain the root and selected RFC 6901 JSON Pointer paths.

| Rule | Meaning |
| --- | --- |
| `Root == 0` | Accept any JSON root type |
| `Required == true` | The path must be present |
| `SchemaNull` in `Types` | A present JSON null is allowed |
| Unspecified path | Allowed; schemas are open to additional fields |
| `SchemaInteger` | Lexical JSON integer: no fraction or exponent |
| `SchemaNumber` | Any JSON number, including integers |

Compilation rejects invalid or duplicate paths. A failed write returns a typed
`*store.SchemaViolationError`, matches `store.ErrSchemaViolation` with
`errors.Is`, and publishes nothing. Compiled schemas are immutable and safe to
share. Collection creation freezes the schema contract.

## Exact indexes

The facade creates an index with `Collection.CreateIndex(name, paths...)`.

- Each path is an RFC 6901 JSON Pointer.
- One path creates a scalar index; two to four create an order-sensitive
  compound index.
- Null, booleans, numbers, and strings are indexable.
- Missing paths, unresolvable paths, arrays, and objects are omitted.
- Index hashes only select candidates; execution rechecks values for correctness.
- The facade call completes the online build before returning success.

The facade exposes non-unique exact indexes only. Unique exact indexes are a
`store/durable` feature; heap `store.Collection.CreateIndex` rejects `Unique`.

Indexes change access paths, not query results. A building low-level heap index
uses exact scan fallback for uncovered chunks. Later writes maintain every
published index before their new generation becomes visible.

## Reads and immutable generations

Every successful state-changing mutation publishes an immutable collection state.
Readers that already hold the old state continue to see it.

| Operation | View and ownership |
| --- | --- |
| `Get` | Current value; returned bytes are owned by the caller |
| `Append` | Appends an owned value to caller storage; a miss leaves it unchanged |
| `Range` | One immutable generation; key and value are borrowed for the callback |
| `Run` | One immutable generation; release the one-off query result |
| `Session.Run` | Fresh generation per call; result lasts until the next run or release |
| `Database.View` | One coherent cut across all collections |

Copy callback bytes before retaining them. Do not mutate borrowed bytes. The
facade does not promise a portable `Range` order: heap traversal is stable
chunk/slot order, while durable traversal is bytewise lexical key order.

Several independent `Get` calls are not a snapshot and may cross publications.
Use `Database.View` when reads from different keys or collections must belong to
one coherent cut.

## Transactions and generations

`Database.Update` and `Begin` provide serializable, read-your-writes
transactions. A conflict publishes nothing and returns `ErrTxConflict`; the
caller owns the retry policy. `Database.View` and `BeginReadOnly` never publish.

The `Buffered` facade profile accepts a transaction that dirties one collection
but refuses a transaction that dirties two or more with
`ErrTxUnsupportedLane`. `Durable` and `Memory` support bounded
multi-collection commits.

A generation is a per-collection publication counter. It is not a database
revision, wall clock, transaction ID, or cross-collection ordering token. In the
buffered profile, `Metrics.DurableGeneration` may trail
`PublishedGeneration` until `Flush` or `Close` succeeds. The Memory profile
reports a durable generation of zero.

## Modeling guidance

- Use stable, compact keys that encode application identity, not JSON structure.
- Keep one consistency boundary in one document when whole-value replacement is
  the natural update.
- Split collections by lifecycle, schema, or access pattern—not by physical
  filename concerns.
- Use a transaction for invariants spanning documents or collections.
- Treat generation numbers as observability data, not durable business versions.
- Keep exportable source data while testing upgrades; there is no compatibility
  or migration promise between development commits.

## Source map

- Facade model and bounds: `vibedb.go` (`Open`, `Collection`, `Put`, `Get`, `Range`, `Metrics`)
- Transaction cuts and overlays: `vibedb_txn.go`
- Query result lifetimes: `vibedb_query.go`
- Collection-name codec: `internal/collectionname/collectionname.go`
- Schemas and exact indexes: `store/store_schema.go`, `store/store_index_exact.go`
- Heap snapshot behavior: `store/engine.go`
- Durable opaque mode and limits: `store/durable/store_file_options.go`
