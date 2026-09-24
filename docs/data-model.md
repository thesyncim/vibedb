# Data model

[Documentation](README.md) / [Design](design/README.md) · [Development status](status.md)

VibeDB stores keyed JSON values in named collections. This page defines the
application-visible model of the root `vibedb` package and notes where the
low-level storage packages and the SQL driver differ.

## Keep the API layers separate

| Layer | Model | Intended use |
| --- | --- | --- |
| `vibedb` | Owned database lifecycle, canonical JSON, three durability profiles | Applications; this page describes this layer unless noted |
| `sql/driver` | SQL tables over the same document engine, with declared columns and a primary-key path | SQL applications and pgwire |
| `store` | Heap-resident engine with explicit immutable snapshots | Engine integration and in-process workloads |
| `store/durable` | File-backed engine with explicit descriptor and snapshot ownership | Storage integrations that need low-level control |

A guarantee of one layer does not automatically hold in another. For example,
`store.Collection` does not canonicalize values, and
`store/durable.Options.OpaqueValues` is not part of the facade's JSON model.

## Logical shape

```text
Database
└── Collection (by name)
    ├── key → JSON value
    ├── key → JSON value
    ├── exact indexes over JSON Pointer paths
    └── tin (full-text) indexes over one string path each
```

- A database is a catalog of collections.
- A collection maps each key to exactly one JSON value.
- A key is unique within its collection.
- `Put` for an existing key replaces the complete value atomically; there is
  no partial update or patch operation in the facade.
- `Delete` of a missing key is a successful no-op.
- There are no foreign keys, cascades, or cross-collection uniqueness.

`Database.Collection(name)` returns a stable, lazy handle. Getting it does no
I/O. Reads from an absent collection behave as empty; the first valid
mutation or index creation creates it.

## Names and keys

| Item | Rule | Default bound |
| --- | --- | ---: |
| Collection name | Nonempty, valid UTF-8, no NUL | 120 bytes |
| Key | Nonempty Go string, compared as bytes | 256 bytes |
| JSON value | Nonempty, one complete JSON value | 4 MiB |

Key and value bounds can be changed for newly created disk collections with
`AdvancedOptions.Engine` (`MaxKeyBytes`, `MaxDocumentBytes`). A zero-option
reopen adopts the bounds persisted in each existing collection.

Collection names are logical strings, not path fragments. Disk profiles
hex-encode them into file names (`users` becomes `c-7573657273.vjc`), so
separators, trailing spaces, and different Unicode normalization forms remain
distinct names. Do not construct or parse those file names.

## JSON values

Any JSON root value is legal unless a schema narrows it: object, array,
string, number, boolean, or null. The facade validates every write and returns
canonical JSON from `Get`, `Append`, and `Range`.

```go
created, err := users.Put("user:42", []byte(`{
  "name": "Ada",
  "active": true
}`))

doc, found, err := users.Get("user:42")
// doc is `{"active":true,"name":"Ada"}` and is owned by the caller.
```

### Canonical JSON

Canonicalization is a deterministic storage encoding, not RFC 8785:

- insignificant whitespace is removed;
- object members are sorted by key bytes (UTF-8 order), recursively;
- duplicate member names are kept, in their original relative order;
- array order is preserved;
- string escapes are normalized;
- **number spellings are preserved**: `1.0e0` stays `1.0e0`.

Queries compare numbers by exact decimal value, so `1`, `1.0`, and `1e0` are
equal in predicates, grouping, and indexes even though their stored bytes
differ. When an object has duplicate member names, path reads use the last
occurrence. Code that needs a field to exist or have a type must use a schema
or check the value; do not depend on input byte spelling surviving a write.

`store/durable` can instead store nonempty opaque bytes. Opaque mode disables
JSON parsing, schemas, and indexes. Use that low-level API directly; do not
enable it through `vibedb.AdvancedOptions`.

## Schemas

A schema is compiled once with `store.CompileSchema` and supplied in
`AdvancedOptions.Engine.Collection.Schema`. It constrains the root type and
selected RFC 6901 paths:

```go
schema, err := store.CompileSchema(store.SchemaDefinition{
	Root: store.SchemaObject,
	Fields: []store.SchemaField{
		{Path: "/name", Types: store.SchemaString, Required: true},
		{Path: "/age", Types: store.SchemaInteger | store.SchemaNull},
	},
})
```

| Rule | Meaning |
| --- | --- |
| `Root == 0` | Accept any root type |
| `Required: true` | The path must be present |
| `SchemaNull` in `Types` | A present JSON null is allowed |
| Unspecified path | Allowed; schemas are open to extra fields |
| `SchemaInteger` | A number written without a fraction or exponent |
| `SchemaNumber` | Any JSON number, including integers |

Compilation rejects invalid or duplicate paths. A failed write returns a
`*store.SchemaViolationError` that matches `store.ErrSchemaViolation` and
publishes nothing. A schema is frozen when the collection is created;
existing durable collections keep their persisted schema. SQL tables derive
a schema from their declared columns.

## Exact indexes

`Collection.CreateIndex(name, paths...)` creates a non-unique exact index.

- Each path is an RFC 6901 JSON Pointer; one path makes a scalar index, two
  to four make an order-sensitive compound index.
- Null, booleans, numbers (by exact value), and strings are indexed.
- Missing paths, arrays, and objects are omitted; an array's elements are not
  indexed individually.
- Index terms select candidates only; execution rechecks every candidate.
- The facade call completes the build before returning.

Unique exact indexes exist in `store/durable` and SQL (`CREATE UNIQUE
INDEX`), not in the facade. Later writes maintain every published index
before their generation becomes visible.

## Full-text (tin) indexes

`Collection.CreateTinIndex(name, path)` and SQL `CREATE INDEX ... USING tin`
declare a full-text index over one string path. Other values never match.
Text is tokenized into words and folded for case and Latin-1 accents. The
index is derived from the same generation a query reads, and it is rebuilt in
memory for each new generation that is searched. See
[full-text search](api/search.md).

## Reads and immutable generations

Every successful mutation publishes a new immutable collection generation.
Readers that hold an older generation keep seeing it.

| Operation | View and ownership |
| --- | --- |
| `Get` | Current value; bytes owned by the caller |
| `Append` | Appends an owned value to caller storage; a miss leaves it unchanged |
| `Range` | One immutable generation; key and value borrowed for the callback |
| `Run` | One immutable generation; release the result |
| `Session.Run` | Fresh generation per call; result valid until the next run |
| `Database.View` | One coherent cut across all collections |

`Range` order is bytewise lexical key order on disk profiles and physical
slot order on `Memory`. Separate `Get` calls are not a snapshot; use
`Database.View` when reads from different keys or collections must come from
one cut.

## Transactions and generations

`Database.Update` and `Begin` provide serializable, read-your-writes
transactions; a conflict publishes nothing and returns `ErrTxConflict`.
`Buffered` refuses a transaction that writes two or more collections. See
[transactions](transactions.md).

A generation is a per-collection publication counter, not a database
revision, clock, transaction ID, or cross-collection ordering token. A failed
low-level batch can advance it without changing logical content. On
`Buffered`, `Metrics.DurableGeneration` trails `PublishedGeneration` until
`Flush` or `Close`; on `Memory` it is zero.

## Modeling guidance

- Use stable, compact keys that encode application identity.
- Keep data that changes together in one document when whole-value
  replacement is the natural update.
- Split collections by lifecycle, schema, or access pattern.
- Use a transaction for invariants that span documents or collections.
- Treat generation numbers as observability data, not business versions.
- Keep exportable source data: there is no compatibility or migration promise
  between development revisions.

## Limitations

- No partial updates, JSON patch, or server-side document merge in the
  facade; SQL `UPDATE` assignments are the only field-level write path.
- No secondary indexes over array elements, and no expression or partial
  indexes.
- Keys are opaque bytes with no range-scan API in the facade; ordered access
  goes through queries.
- Canonical output preserves number spelling and duplicate members, so two
  semantically equal documents can have different stored bytes.

## Source map

- Facade model and bounds: [vibedb.go](../vibedb.go)
- Transaction cuts and overlays: [vibedb_txn.go](../vibedb_txn.go)
- Collection-name codec: [internal/collectionname/collectionname.go](../internal/collectionname/collectionname.go)
- Schemas and exact indexes: [store/store_schema.go](../store/store_schema.go), [store/store_index_exact.go](../store/store_index_exact.go)
- Heap snapshots: [store/engine.go](../store/engine.go)
- Durable options and opaque mode: [store/durable/store_file_options.go](../store/durable/store_file_options.go)
