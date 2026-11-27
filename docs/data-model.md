# Data model

[Documentation](README.md) / [Design](design/README.md) · [Stability](status.md)

VibeDB stores keyed JSON values in named collections. Each API layer applies
its own contract over this model: `vibedb` owns canonical JSON and lifecycle,
`sql/driver` adds declared table schemas, `store` is heap-resident, and
`store/durable` exposes low-level files (including optional `OpaqueValues`
byte-for-byte storage outside the facade).

## Logical shape and bounds

```text
Database
└── Collection (logical UTF-8 name, ≤ 120 B, no NUL)
    ├── key (nonempty bytes, ≤ 256 B) → canonical JSON value (≤ 4 MiB)
    ├── exact indexes (1–4 RFC 6901 JSON Pointer paths)
    └── tin full-text indexes (1 string path each)
```

- Keys are unique per collection; `Put` replaces the whole value atomically;
  `Delete` on a missing key is a no-op. Collections materialize lazily on first
  valid write or index creation.
- Collection names are hex-encoded into disk filenames (`c-<hex>.vjc`), so
  slashes, spaces, and Unicode normalization forms remain distinct.
- Disk key/document limits are configurable on creation (`MaxKeyBytes`,
  `MaxDocumentBytes`) and persisted per collection.

## Canonical JSON

Every JSON root type (object, array, string, number, boolean, null) is valid
unless narrowed by a schema. Facade writes canonicalize JSON deterministically
(not RFC 8785):

- Insignificant whitespace is stripped; string escapes are normalized.
- Object members are sorted recursively by UTF-8 key bytes; duplicate keys keep
  their relative order (path reads resolve to the **last** occurrence).
- Array order and **number spellings** are preserved (`1.0e0` stays `1.0e0`),
  while queries and indexes compare numbers by exact decimal value (`1 == 1.0 == 1e0`).

## Schemas and indexes

Compile an open schema with `store.CompileSchema` (`Root` type + RFC 6901
`Fields` with `Types` bitmask and `Required`); schemas freeze at collection
creation (`store.ErrSchemaViolation` publishes nothing). SQL tables derive
their schema from declared columns.

- **Exact indexes (`CreateIndex`):** 1–4 ordered RFC 6901 pointers; indexes
  null, bool, exact numbers, and strings (omits missing, arrays, objects).
  Candidates are always rechecked. Unique exact indexes are available in SQL
  and `store/durable`.
- **Full-text (`tin`) indexes:** Single string path, Latin-1 accent folding,
  incremental cross-generation updates with a checksummed `.tin` sidecar
  ([full-text search](api/search.md)).

## Generations and reads

Every mutation publishes a new immutable generation; existing readers keep
their pinned view. `Get`/`Append` return caller-owned bytes; `Range` borrows
bytes for the callback (lexical key order on disk, physical slot order on
`Memory`). Use `Database.View` for a coherent multi-collection cut. Generation
numbers are per-collection observability counters, not transaction IDs or
content versions (topology preparation can advance them on a failed batch).

## Limitations

- No partial JSON patch in the native facade (use SQL `UPDATE` for field-level
  mutations).
- No array-element, expression, or partial secondary indexes.
- No key-range scan in the facade (use typed queries with `OrderBy`).

## Source map

- [vibedb.go](../vibedb.go) · [vibedb_txn.go](../vibedb_txn.go) · [internal/collectionname/collectionname.go](../internal/collectionname/collectionname.go)
- [store/store_schema.go](../store/store_schema.go) · [store/store_index_exact.go](../store/store_index_exact.go) · [store/durable/store_file_options.go](../store/durable/store_file_options.go)
