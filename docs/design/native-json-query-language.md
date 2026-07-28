# Native JSON query subset

**Status:** proposed plan. The documents below are not a current public
contract.

## Decision

VibeDB will support a small, versioned JSON query language, but it will not
build a second general-purpose alternative to SQL.

SQL remains the primary, broader relational language for ad-hoc work,
aggregates, grouping, SQL data definition, `database/sql`, pgwire, and existing
tooling. Native JSON serves a narrower application use case:

- construct queries without generating text;
- bind exact JSON values without converting them through SQL types;
- distinguish missing paths from explicit JSON `null` in predicates;
- express bounded cross-collection existence and one inner join;
- serialize a stable subset that applications can store or send to an API.

The supported v1 subset is:

```text
from
+ where
+ zero or more conjunctive existence joins
+ at most one inner equi-join
+ projection
+ ordering
+ limit
```

Grouping, reductions, containment, arbitrary functions, mutation, schema
alteration, and dynamic index administration remain outside the native query
language. “Subset” means a subset of the engine's logical capabilities, not
identical surface truth tables or a claim that today's SQL parser is a strict
superset. SQL and native missing/null semantics intentionally differ. When SQL
already expresses a larger operation, applications use it rather than
extending JSON until it becomes SQL encoded as objects. A new relational
operator otherwise lands in the shared plan and broader SQL surface before the
native subset considers it.

Schemas and initial indexes are still supported. They belong to a separate,
small catalog command surface because they define a collection rather than
select rows from one.

## Architectural boundary

The Go builder, native JSON, and SQL lower shared operations into one logical
query representation. Storage backends implement that representation through
shared execution code and explicit backend adapters:

```text
native JSON query ─┐
Go builder         ├─> normalized logical query ─> query execution
SQL lowering       ┘                              ├─ heap storage
                                                  └─ durable storage

native catalog command ─> canonical collection definition
                           ├─ schema compiler
                           ├─ exact-index compiler
                           └─ database catalog mutation
```

The catalog command does not pass through the query compiler. Query parsers do
not create collections or indexes, and catalog code does not evaluate
predicates.

“Shared execution” does not require pretending every SQL feature is already a
logical query node. SQL currently owns cursor processing for SQL-only `HAVING`
and `OFFSET`. That may move into shared nodes later, but it does not block the
native subset and must not leak into a storage backend.

The current loose JSON accepted by `query.Parse` remains a legacy Go API during
migration. The strict language is a new versioned entry point that lowers to
the same `query.Query`; it does not silently change existing callers.

## Why JSON is appropriate for the subset

JSON is a good serialized request language here because its literals are the
stored value domain and applications can construct it safely. It is not
automatically a good general database language. Joins, aggregation, DDL,
migrations, and result shaping each require semantics regardless of syntax.
SQL already supplies those semantics and an ecosystem around them.

The native subset earns its place where SQL is least natural:

- nested document paths;
- exact decimal JSON numbers;
- missing versus present-null predicates;
- machine construction without quoting or text concatenation;
- a deliberately bounded operator set.

The JSON document is a serialized language, not a complete network wire
protocol. Authentication, authorization, framing, content type, cancellation
transport, streaming, and response envelopes require a separate transport
contract. No protocol guesses a dialect from the first input byte.

## Versioned query document

A reusable query owns its dialect and version:

```json
{
  "dialect": "vibedb-query",
  "version": 1,
  "from": "orders",
  "where": {
    "/status": "open",
    "/total": {"$gte": {"$param": "minimum"}}
  },
  "exists": [
    {
      "from": "accounts",
      "on": {"outer": "/account_id", "inner": "$key"},
      "where": {"/enabled": true}
    }
  ],
  "join": {
    "from": "customers",
    "as": "customer",
    "on": {"outer": "/customer_id", "inner": "$key"},
    "where": {"/tier": "pro"}
  },
  "select": [
    {"name": "order_id", "path": "/id"},
    {"name": "customer_name", "path": "@customer/name"},
    {"name": "total", "path": "/total"}
  ],
  "orderBy": [
    {"path": "/created_at", "direction": "desc"}
  ],
  "limit": {"$param": "rows"}
}
```

`dialect`, `version`, and `from` are required. Every other clause is optional,
subject to the join and projection rules below. Unknown and duplicate members
fail closed.

Parameter values are supplied separately from the reusable document:

```json
{
  "query": {
    "dialect": "vibedb-query",
    "version": 1,
    "from": "orders",
    "where": {"/total": {"$gte": {"$param": "minimum"}}},
    "select": [{"name": "total", "path": "/total"}],
    "limit": {"$param": "rows"}
  },
  "params": {
    "minimum": 100,
    "rows": 50
  }
}
```

The outer object is an execution binding, not a second query syntax. A Go API
may accept the versioned query document at prepare time and the parameter map
at execution time.

Versioned native documents always execute through a database and resolve
`from` from the selected database snapshot. They never ignore `from` in favor
of a separately supplied collection handle, so a dropped-and-recreated name
cannot be confused with an old handle. Collection-handle execution remains a
Go-builder and legacy API, not an alternate native-v1 binding.

## Strict input and normalization

V1 deliberately has a small accepted grammar:

- clause names and operator names are case-sensitive;
- arrays are required for `select`, `orderBy`, and `exists`;
- `join` is one object because v1 permits only one inner join;
- ordered concepts derive order only from arrays;
- duplicate object members are rejected at every depth;
- unknown members and operators are rejected;
- request numbers are parsed from their exact token, never through `float64`;
- paths use only the canonical wire grammar described below;
- collection names, aliases, paths, directions, and operators cannot be
  parameters.

Empty `select`, `orderBy`, and `exists` arrays are rejected; omit a no-op
clause. Empty `$and`, `$or`, and membership lists remain meaningful by their
defined identities.

Duplicate rejection applies to query and admin documents, not stored user
documents. Stored JSON may still contain duplicate object names; current path,
schema, and index lookup consistently observes the last occurrence. Changing
stored-document admission is a separate storage-format and write-contract
decision.

Accepted documents normalize immediately into a deterministic logical form.
Normalization:

- compiles paths before checking path equality;
- decodes strings before canonical comparison;
- sorts and deduplicates commutative `$and`, `$or`, and conjunctive `exists`
  children by their normalized encoding;
- sorts unordered field-predicate members;
- assigns parameter slots by deterministic normalized traversal;
- canonicalizes exact numbers by numeric value for comparison and cache
  identity while retaining their original spelling for diagnostics;
- sorts and deduplicates inline `$in` values in
  `null < boolean < number < string` order because membership order is not
  semantic;
- preserves only semantic array order, including projection and sort keys.

This is deterministic normalization, not a claim to prove arbitrary logical
equivalence. For example, `$ne` and `$not` over `$eq` may normalize to the same
primitive node, while De Morgan rewrites are not required.

The plan-cache key contains the dialect, version, normalized query, every
inline literal value, and inferred parameter constraints. It excludes only
separately bound parameter values. Numerically or string-equivalent source
spellings may share one executable; a per-prepare diagnostic wrapper retains
the caller's spellings and parameter names. Cache entry count and retained
bytes are bounded by the owning API or server.

## Paths and source qualification

The strict wire language uses RFC 6901 JSON Pointers:

- `""` is the driving document root;
- `/profile/name` addresses a path in the driving collection;
- `@customer` is the root of join alias `customer`;
- `@customer/name` addresses pointer `/name` in join alias `customer`;
- `$key` addresses collection-key metadata only where explicitly allowed.

Source qualification is lexical. Adding an alias can never reinterpret an
existing driving path. An alias matches `[A-Za-z_][A-Za-z0-9_]*`, is
case-sensitive, and cannot collide with another alias.

Dotted paths may remain SDK or legacy shorthand, but the versioned wire
language does not accept them. Both forms may normalize to the same internal
source slot plus compiled pointer before execution.

A top-level `where` accepts driving pointers only. A join's own `where`
accepts inner pointers only. `select` and `orderBy` may use driving or qualified
joined pointers. General `$key` projection, filtering, and ordering require a
logical metadata column and are deferred; `$key` is initially admitted only in
join conditions.

There is no wildcard, recursive descent, implicit array traversal, or
flattening. A pointer may address a particular array element. A future
array-traversal operator must state its cardinality and receive its own logical
node.

## Predicate grammar

The normative shape grammar is:

```text
Predicate      := FieldPredicate
                | {"$and": [Predicate, ...]}
                | {"$or":  [Predicate, ...]}
                | {"$not": Predicate}
FieldPredicate := {Pointer: Scalar | FieldOps, ...}  // at least one member
FieldOps       := {FieldOp: FieldOperand, ...}       // at least one member
FieldOp        := "$eq" | "$ne" | "$lt" | "$lte" | "$gt" | "$gte"
                | "$in" | "$nin"
                | "$exists" | "$null" | "$missing" | "$nullish"
Operand        := Scalar | ParamRef
ListOperand    := [Scalar, ...] | ParamRef
ParamRef       := {"$param": Name}                   // exactly one member
Scalar         := null | boolean | exact-number | string
```

For equality and ordered field operators, `FieldOperand` is `Operand`; for
membership it is `ListOperand`; for unary tests it is the literal `true`.
A predicate object cannot mix pointer members with a boolean operator. A field
operator object cannot be empty. Unknown, missing, mixed, or extra members are
errors.

One field may carry a bare scalar equality:

```json
{"/active": true}
```

or an operator object whose members are ANDed:

```json
{
  "/total": {
    "$gte": {"$param": "minimum"},
    "$lt": 1000
  }
}
```

The admitted field operators are:

```text
$eq $ne $lt $lte $gt $gte
$in $nin
$exists $null $missing $nullish
```

`$and`, `$or`, and `$not` are whole-predicate operators, not field operators.

`$eq`, `$ne`, and ordered comparisons accept a scalar literal or parameter
reference. `$in` and `$nin` accept an array of scalar literals or a parameter
reference bound to such an array. Scalar literals are `null`, boolean, number,
or string. Array and object literals are not comparison operands in v1, so
`{"$param":"name"}` is unambiguously an operand expression rather than a
literal object.

Unary tests take the literal `true`. `$and` and `$or` take predicate arrays;
`$not` takes one predicate. Empty `$and` is true and empty `$or` is false,
which makes generated predicates composable without special cases.

## Predicate semantics

Native predicates are two-valued. Every predicate returns true or false; they
do not inherit SQL `UNKNOWN`.

Equality is defined over present scalar JSON values:

- null equals null;
- booleans compare by value;
- numbers compare by exact decimal value, so `1`, `1.0`, and `1e0` are equal;
- strings compare by decoded content;
- different JSON kinds are not equal;
- arrays and objects are not equality operands in v1;
- a missing path is unequal to every value, including null.

`$ne` is the boolean complement of `$eq`. `$in` is the OR of equality against
its members, and `$nin` is its complement. An empty membership list therefore
matches nothing for `$in` and everything for `$nin`.

The core missing/null cases are:

| Predicate over `/a` | `1` | `2` | explicit `null` | missing |
| --- | ---: | ---: | ---: | ---: |
| `{"/a":{"$eq":1}}` | true | false | false | false |
| `{"/a":{"$ne":1}}` | false | true | true | true |
| `{"/a":{"$eq":null}}` | false | false | true | false |
| `{"/a":{"$ne":null}}` | true | true | false | true |
| `{"/a":{"$in":[1,null]}}` | true | false | true | false |
| `{"/a":{"$nin":[1,null]}}` | false | true | false | true |
| `{"/a":{"$exists":true}}` | true | true | true | false |
| `{"/a":{"$null":true}}` | false | false | true | false |
| `{"/a":{"$missing":true}}` | false | false | false | true |
| `{"/a":{"$nullish":true}}` | false | false | true | true |

`$lt`, `$lte`, `$gt`, and `$gte` accept number or string operands. A stored
value must be present and have the same orderable kind; missing, null, boolean,
container, and cross-kind cases return false. Numbers use exact decimal order
and strings use decoded byte order. There is no coercion.

`$not` is ordinary boolean complement. Applications that mean “present and not
equal” must combine `$exists` and `$ne`.

These are intentional native semantics. SQL lowering retains SQL null and
three-valued behavior through explicit logical primitives.

## Parameters

Parameter names are non-empty strings with a published length bound. A
parameter may replace only:

- one scalar equality or ordered-comparison operand;
- one complete `$in` or `$nin` scalar list;
- `limit`.

Preparation intersects use-site constraints for each parameter:

```text
scalar
├─ ordered-scalar (number or string)
└─ count (non-negative JSON integer token)
scalar-list
```

`scalar-list` is incompatible with every scalar constraint.
`scalar ∩ ordered-scalar` is `ordered-scalar`; `scalar ∩ count` is `count`; and
`ordered-scalar ∩ count` is `count`. Any empty intersection is a preparation
error. Binding requires every referenced value, rejects unreferenced values,
checks the inferred constraint, and preserves exact number tokens.

Inline and bound limits accept only integer-token spellings in
`[0, 1_000_000]` under portable defaults; `1.0` and `1e0` are not limit
spellings even though they compare numerically equal to `1`. An explicitly
wider result-row budget raises the matching limit ceiling.

Heterogeneous membership lists are valid because equality is kind-strict.
Duplicate values have no effect. Parameters cannot change paths, operators,
aliases, collection names, projection shape, or sort direction.

## Cross-collection operators

All named collections resolve from one database snapshot. The database API
must gain a deterministic `SnapshotCollections(names...)` operation so a
two-collection query does not lease and snapshot every unrelated collection.
Supplying independently captured collection snapshots is not an alternate
joined-query API.

### Existence join

`exists` is a conjunctive semi-join. It retains a driving document once when
at least one inner document matches. The following is an `exists` clause
fragment inside a versioned query:

```json
{
  "exists": [
    {
      "from": "customers",
      "on": {"outer": "/customer_id", "inner": "$key"},
      "where": {"/tier": "pro"}
    }
  ]
}
```

An existence clause has no alias and contributes no result column. Multiple
clauses conjoin. V1 does not place existence joins inside predicate boolean
trees, so `OR EXISTS` and `NOT EXISTS` are explicit omissions rather than
accidental spellings.

### Inner join

`join` is one fan-out inner equi-join. The following is a `join` plus `select`
fragment inside a versioned query:

```json
{
  "join": {
    "from": "line_items",
    "as": "item",
    "on": {"outer": "$key", "inner": "/order_id"},
    "where": {"/active": true}
  },
  "select": [
    {"name": "order_total", "path": "/total"},
    {"name": "sku", "path": "@item/sku"}
  ]
}
```

Every `exists` or `join` clause requires `from` and exactly one `on` object
with exactly `outer` and `inner`. `where` is optional; only fan-out `join`
requires `as`. `on.outer` is a driving pointer or `$key`; `on.inner` is an
inner pointer or `$key`. Unknown or missing members fail. This permits
foreign-key-to-key, key-to-foreign-key, and key-to-key joins without contextual
path tricks.

Join values are non-null scalars: boolean, number, or string. Missing, null,
arrays, and objects match nothing. Numbers use exact decimal equality. An
existence clause retains its driving row once; a fan-out join emits one result
row per matching pair. The inner `where` is evaluated before matching.

Collection keys are opaque Go string bytes. Key-to-key joins compare those
bytes exactly. A key compared with a document JSON string uses the string's
decoded UTF-8 bytes; a non-UTF-8 key therefore cannot match a JSON string but
can match the identical key from another collection.

The join cannot read an existence result or another alias, so joins do not
chain in v1. A fan-out query requires `select`, unique output names, and at
least one selected joined path; otherwise the caller should use `exists`.

### Planning and indexes

Join syntax never contains an index name or hint. Indexes affect cost, not
results.

The current join implementation builds the filtered inner values. A ready
single-column exact index on the driving join path may accelerate outer
membership, and an inner filter may use indexes while selecting build rows.
The design does not promise a repeated inner-index probe strategy that the
engine does not implement.

### Backend and resource contract

Existence joins already execute over heap and durable database snapshots, but
an arbitrary inner pointer can still collect the complete filtered inner key
set. Phase 2 therefore applies the inner-build row/byte, workspace, and
cancellation budgets to existence as well as fan-out before exposing either.

Heap fan-out currently stores heap-specific row locations; durable fan-out is
rejected. Native `join` cannot become public until execution has a
backend-neutral joined-row ownership model, or an equally bounded shared
operator, and heap/durable differential tests pass.

Fan-out also needs pair and result budgets independent of `limit`. Ordering may
need to inspect every pair before applying the limit. The execution API
therefore enforces caller-configured maxima for:

- inner build rows and bytes;
- examined and materialized join pairs;
- result rows and bytes;
- workspace memory;
- elapsed work through context cancellation.

Exceeding a budget returns a typed error before an unbounded allocation. A
partial result is never reported as complete. Streaming or spill may be added
behind the logical operator, but neither changes row semantics or bypasses a
budget.

## Projection, ordering, limit, and results

`select` is an ordered array of explicit output definitions:

```json
{"name": "customer_name", "path": "@customer/name"}
```

Names must be non-empty and unique. V1 has no wildcard, expression, aggregate,
or nested result constructor. When the fan-out `join` clause is absent,
omitting `select` produces the current one-column result: column `*` contains
the complete driving document for each row. It does not turn the result itself
into a document-shaped row. An empty `select` is invalid.

Projected cells retain raw JSON and exact source number spelling. Scalar result
types distinguish null, boolean, number, and string; arrays and objects share
the current `json` result type. For compatibility with the current flat result
representation, a projected missing path is returned as null. Predicate
evaluation still distinguishes missing from explicit null. A future
presence-bearing cell type would require a new result contract and version.

`orderBy` is an ordered array of `{path, direction}` objects. Direction is
`asc` or `desc`. Sortable row values are null/missing, boolean, number, and
string, with ascending order:

```text
null-or-missing < false < true < number < string
```

Numbers and strings use the comparison rules above. A container at a sort path
is a typed execution error detected before results are returned. This is an
intentional v1 change from the legacy executor's source-byte container order
and requires an explicit order-domain validation node plus characterization,
differential, and cleanup tests. Descending reverses the order. Equal sort keys
have no cross-backend relative-order promise; callers needing deterministic
pagination must include a future stable key ordering once key projection/order
is supported.

`limit` is a non-negative integer or matching parameter. Zero returns no rows.
It limits emitted rows, not the join work required by filtering or ordering.
No result order is promised without `orderBy`.

Native v1 deliberately has no grouping, aggregate reductions, `having`,
`offset`, nested result construction, or continuation token.

## Native catalog commands

Schema and index definitions are supported through a separate dialect:

```json
{
  "dialect": "vibedb-admin",
  "version": 1,
  "createCollection": {
    "name": "orders",
    "schema": {
      "root": ["object"],
      "fields": [
        {
          "path": "/id",
          "types": ["string"],
          "required": true
        },
        {
          "path": "/total",
          "types": ["number"],
          "required": true
        },
        {
          "path": "/note",
          "types": ["string", "null"],
          "required": false
        }
      ]
    },
    "indexes": [
      {
        "name": "by_customer_status",
        "paths": ["/customer_id", "/status"]
      },
      {
        "name": "by_created_at",
        "paths": ["/created_at"]
      }
    ]
  }
}
```

An admin request has `dialect`, `version`, and exactly one tagged command.
Unknown or duplicate members fail. Admin authorization is outside the query
language and must be enforced by any transport.

The minimal native admin v1 contains:

- `createCollection`;
- `describeCollection`;
- `listCollections`.

`createCollection` requires `name`; `schema` and `indexes` are optional. A
schema object has exactly `root` and `fields`. Each field has exactly `path`,
`types`, and boolean `required`. Each index has exactly `name` and `paths`.
An omitted `indexes` member and an empty index array normalize identically.

`describeCollection` takes exactly `{"name":"orders"}`.
`listCollections` accepts optional `limit` and `after`:

```json
{
  "dialect": "vibedb-admin",
  "version": 1,
  "listCollections": {
    "limit": 100,
    "after": "orders"
  }
}
```

`limit` defaults to 100 and is in `[1, 256]`. `after` is an exclusive
byte-lexical collection name. Each page observes one catalog instant; because
v1 only creates names, concurrent creates cannot invalidate returned entries,
but a complete concurrent enumeration may need to restart to discover a new
name before its cursor.

The semantic results are:

- create: `status` is `created` or `unchanged`, plus a collection description;
- describe: one collection description or `collection_not_found`;
- list: at most 256 name-ordered summaries plus `nextAfter`, absent when done.

A description contains name, opaque versioned `definitionHash`, canonical
schema/index definitions, index state, document count, and generation. A list
summary contains name, definition hash, count, and generation. Serialized
admin results obey the 1 MiB result ceiling. The definition hash is an equality
and precondition token; its algorithm is not API unless a later format
specification explicitly freezes it.

Portable collection names are byte-exact valid UTF-8 between 1 and 240 bytes.
They cannot be `.`, `..`, contain NUL, `/`, or `\`, or end in the reserved
durable collection-file suffix `.vjc`. Names are not Unicode-normalized or
case-folded. This adopts the durable catalog's safe path-element subset for
both backends and leaves room for the final `.vjc` suffix. Temporary install
names use a fixed-length hash rather than extending the user stem.

Destructive drop, rename, online index mutation, schema alteration, and
multi-command catalog transactions are deferred. Existing trusted Go APIs may
remain broader, but they are not silently exposed by the serialized dialect.

### Schema contract

One collection has either no schema or one immutable open-document schema.
There are no independently named schema objects: creating a schema means
including it in the collection definition.
The supported type names are:

```text
null boolean integer number string array object
```

`integer` means a JSON number written without a fraction or exponent and is a
subset of `number`. A `types` array is a non-empty union. Duplicate types and
the redundant `number` plus `integer` combination are rejected.

When `schema` is present:

- `root` is required and constrains the root JSON type;
- `fields` is required and may be an empty array for a root-only schema;
- non-empty `fields` contains unique, non-root RFC 6901 paths;
- `required` controls presence independently from allowed types;
- a required nullable field must be present but may contain null;
- an optional field may be absent, but a present value must match;
- a missing or unresolvable ancestor makes the field absent;
- unspecified paths remain allowed.

The schema is intentionally not JSON Schema. V1 has no closed objects,
properties tree, defaults, coercion, enumeration, numeric range, string
pattern, array cardinality, cross-field constraint, foreign key, or uniqueness
constraint. Those names are rejected rather than accepted and ignored.

The collection key remains separate string metadata supplied to collection
operations. A schema path is not implicitly a primary key, and an exact index
is not unique.

An omitted schema selects the schemaless fast path. It is distinct from an
explicit schema whose root union admits every JSON type: both accept the same
documents initially, but only the latter has a compiled, persisted schema
identity against which future catalog operations can compare.

The canonical schema compiles before any catalog or file mutation. Every
insert, replacement, bulk build, and durable write validates the document
against the same compiled definition. A validation failure publishes no
document and no partial batch.

Schemas are immutable in native admin v1. A later evolution design must
distinguish metadata-only widening from validation-required tightening,
provide bounded copy/rebuild migration, use an expected-definition-hash
precondition, and atomically publish either the new schema and all conforming
data or nothing. Old snapshots retain their original definition and data.

Schema validation controls writes, not query meaning. Unspecified paths remain
legal query paths, and no index or optimizer decision may be required for
correctness. A prepared plan may use declared type facts only when it is bound
to the collection-definition hash and rechecks that identity at execution.

### Exact-index contract

Each initial index has a required unique name and one to four distinct, ordered
RFC 6901 paths. It is an exact, non-unique scalar posting index:

- one path is a single-column index;
- multiple paths form an order-sensitive compound key;
- a document is omitted if any component is missing, unresolved, an array, or
  an object;
- explicit null, boolean, exact number, and decoded string values are indexed;
- hashes only prune candidates and every candidate is rechecked exactly.

There is one index family in v1, so the request does not carry a meaningless
`kind`, method, collation, direction, uniqueness flag, or partial predicate.
Unknown additions fail.

A single-column index is eligible for equality and membership predicates. A
compound index is eligible only when every component has an equality
constraint. Range comparisons and `orderBy` do not claim an ordered access
path. The planner may decline any eligible index, and query correctness never
depends on one.

All syntactically valid paths are allowed consistently. A schema may make an
index provably empty—for example, because an ancestor is scalar-only—but the
catalog compiler does not implement partial reachability analysis. Such an
index remains correct and empty; unindexable rows are simply absent.

Different logical names may declare the same ordered path vector. They remain
independently discoverable while sharing one physical index definition and
posting data. Reversing compound-path order creates a different physical
definition. Portable v1 stays within the current durable catalog ceilings:
4,096 schema fields, 4,096 logical index names, 64 distinct physical index
definitions, and four paths per index.

Initial indexes are part of atomic collection creation. Because the collection
is empty, they publish ready on the first generation and every later write
maintains them. The complete schema and index catalog is persisted and
rehydrated by durable storage.

Native v1 does not expose online `createIndex` or `dropIndex`. Heap storage has
`Building`, bounded backfill, `Ready`, logical drop, and reclamation today;
durable definitions are frozen at creation. A future common lifecycle must add
durable catalog mutation and specify building, ready, failed/cancelled,
crash/reopen, resume/retry, concurrent-write maintenance, held snapshots, and
physical reclamation before either command becomes portable.

That future lifecycle has minimum state semantics:

- `building` is never trusted as complete; the planner either ignores it or
  combines covered postings with an exact scan of uncovered data;
- `ready` means complete coverage and write maintenance before publication;
- `failed` and `cancelled` are never planner-visible and define explicit
  retry-or-drop behavior;
- recovery resumes from durable coverage without losing writes committed while
  the build was in progress.

### Atomicity and idempotency

`createCollection` validates and canonicalizes the complete name, schema, and
index list before it creates or publishes anything.

Canonical definition identity is independent of harmless input order:

- schema type names use one fixed enum order;
- schema fields sort by compiled pointer;
- logical indexes sort by name;
- compound paths retain their declared order;
- identical ordered path vectors share one physical definition;
- omitted and empty index lists are identical;
- absent schema remains distinct from an explicit all-types schema.

- a new canonical definition creates the collection and returns `created`;
- retrying the same name and identical canonical definition returns
  `unchanged`;
- the same name with a different definition returns `definition_conflict`;
- validation and pre-publication failures publish no new collection;
- a backend capability mismatch, including inability to maintain one declared
  index, fails before publication.

Concurrent same-name creates serialize at the catalog boundary. Identical
definitions yield exactly one `created` and the rest `unchanged`; different
definitions yield one winner and `definition_conflict` for every loser.

Returned errors and process crashes have different durability contracts. For
an initially absent name, a returned pre-commit ordinary error leaves it
absent. `definition_conflict` guarantees the already-present definition is
unchanged. No failure mutates an existing collection. If publication of a new
durable collection may have completed but acknowledgement or directory
synchronization fails, the command returns `commit_unknown`; the caller
resolves it with `describeCollection` or safely retries the exact definition.
After a crash and reopen, a formerly absent name is either absent or resolves
to the complete canonical definition—never a partially initialized schema or
index catalog. Recovery removes or ignores uncommitted temporary artifacts.
The durable sequence uses a confined temporary file, syncs the complete
collection, atomically installs the final name, and syncs the parent directory.
Fault injection covers every create, file-sync, install, directory-sync,
reopen, and retry cut.

`describeCollection` returns the canonical semantic definition, its stable
hash, document generation/count, and index readiness. Create-only v1 does not
invent a database-global catalog revision over the current one-file-per-
collection durable design. A future mutation command uses the expected
definition hash as its precondition unless storage first gains a real
revisioned catalog. `listCollections` returns name-ordered summaries. Physical
page, chunk, cache, queue, and file-layout tuning remains host configuration
rather than part of the portable semantic definition.

## SQL coexistence

SQL is the broad language; native JSON is the supported application subset.
Neither is serialized as the other's AST.

- Shared predicates, projection, joins, ordering, and limit lower to common
  logical nodes.
- SQL retains placeholders, aliases, three-valued logic, grouping, reductions,
  `HAVING`, `OFFSET`, and other SQL-specific rules.
- Native JSON retains the two-valued truth tables in this document.
- A semantic difference is represented during lowering and tested; it does not
  become a hidden branch in heap or durable storage.
- A feature missing from native JSON is not a reason to add syntax when SQL
  already expresses it.

Catalog convergence has a narrower boundary than query convergence. Portable
v1 deliberately has one catalog-mutation authority per database:

- a native-owned database accepts `vibedb-admin` and rejects SQL DDL;
- a SQL-owned database accepts SQL DDL and rejects `vibedb-admin`;
- native and SQL query front ends may read either through its snapshot adapter.

The ownership mode is fixed and durably marked when the database is created.
Opening the same directory through the other mutation mode fails before any
name lookup or file creation. This avoids two successful declarations of one
name without inventing an incomplete cross-catalog reservation protocol. A
future combined catalog must first define one shared name reservation
authority.

Both owners compile the same engine `CollectionDefinition`, which contains
only storage semantics: compiled schema and initial exact indexes. Native
`createCollection` materializes it immediately. Within a SQL-owned database,
SQL retains adapter-owned table metadata for its document-derived primary key,
lazy table state, and SQL names:

```text
SQL CREATE TABLE
  -> pending SQL table metadata (primary-key path, schema)
SQL CREATE INDEX before first INSERT
  -> adds an initial index to that pending definition
first INSERT
  -> materializes one engine CollectionDefinition
```

After materialization, SQL online index creation remains rejected until the
portable online lifecycle exists. Primary-key derivation remains SQL adapter
policy because collection keys are independent metadata; it is not smuggled
into the engine schema. SQL need not abandon SQL-only metadata, but it must not
maintain a second authoritative copy of a materialized collection's schema or
index catalog: the pending definition is replaced by the persisted definition
hash after materialization.

## Errors, bounds, and cancellation

Errors have stable codes plus a JSON Pointer into the request. At minimum they
distinguish:

- unsupported dialect or version;
- duplicate or unknown member;
- invalid path, alias, or collection;
- invalid operator or operand;
- missing, extra, or incompatible parameter;
- schema or index definition failure;
- collection definition conflict;
- durable commit outcome unknown;
- missing backend capability;
- runtime type mismatch;
- cancellation;
- compile, workspace, join, result-row, or result-byte budget exhaustion.

Unknown versions fail as unsupported rather than malformed. Binding and
execution errors never mutate a prepared query.

The proposed portable v1 defaults are:

| Resource | Default ceiling |
| --- | ---: |
| Serialized query or admin document | 1 MiB |
| Serialized admin result | 1 MiB |
| JSON nesting depth | 64 |
| Predicate nodes / one boolean fan-in | 1,024 / 256 |
| Total inline literal bytes / one membership list | 512 KiB / 4,096 items |
| Parameters / total bound parameter bytes | 256 / 1 MiB |
| Projection columns / sort keys | 256 / 16 |
| Existence joins / fan-out joins | 8 / 1 |
| Collection name / other name / one compiled pointer bytes | 240 / 255 / 16 KiB |
| Prepared-plan cache entries / retained bytes | 1,024 / 64 MiB |
| One canonical collection definition | 512 KiB |
| Schema fields | 4,096 |
| Logical indexes / distinct physical definitions / paths per index | 4,096 / 64 / 4 |
| Examined rows / logical document bytes / durable page reads per source | 10,000,000 / 1 GiB / 1,000,000 |
| Any join inner build rows / retained bytes | 1,000,000 / 64 MiB |
| Examined or materialized join pairs | 10,000,000 |
| Result rows / retained bytes | 1,000,000 / 64 MiB |
| Total execution workspace | 128 MiB |

Execution checks cancellation at least once per 1,024 scanned rows, join pairs,
or other repeated work units. An embedded caller may configure lower limits or
explicitly opt into wider runtime row/byte/workspace budgets. The serialized
language's structural maxima remain versioned and portable.

Retained-byte accounting includes result cell arrays and headers, owned or
borrowed-payload bookkeeping, decoded strings, join tables, and backing
capacity—not merely serialized JSON payload. Every count and byte limit applies
independently, so a wide result normally reaches its retained-byte ceiling long
before its row ceiling.

Scan ceilings apply independently to the driving source and every existence or
fan-out inner source, including scans that match zero rows. A caller deadline
may stop work earlier but is not a substitute for these finite work limits.

Parsing and normalization enforce structural bounds before reserving execution
workspace. Execution checks cancellation at bounded work intervals. No
convenience API silently disables limits; a trusted embedded caller must opt
into wider explicit limits.

## Implementation sequence

### Phase 0: characterize before refactoring

1. Build golden Go-builder, legacy-JSON, and SQL corpora for every shared
   predicate, projection, ordering, limit, and join behavior.
2. Record current missing/null, duplicate-member, path, container,
   cross-kind, join-cardinality, and result behaviors.
3. Enumerate intentional native-v1 differences without changing legacy APIs.
4. Capture heap/durable differential results and allocation/latency baselines.
5. Verify and freeze the numeric structural and runtime defaults above before
   any implementation phase begins.

### Phase 1: normalized subset

1. Define internal nodes for source-qualified pointers, source-key metadata
   extraction, parameter slots, primitive two-valued predicates, projection,
   ordering, limit, existence, and one fan-out join.
2. Preserve documented SQL-only cursor processing until it has shared nodes.
3. Add explicit scalar-order-domain validation so native container rejection
   does not become an ad hoc executor branch.
4. Make path identity canonical before extraction-slot, grouping, ordering,
   join, and index matching validation.
5. Add selected-collection database snapshots with bounded lease cleanup.
6. Remove evaluator behavior from parsers and storage backends for operations
   represented by the common nodes.

### Phase 2: strict versioned query

1. Add a duplicate-aware, exact-number parser and version dispatch.
2. Implement the normative path, predicate, projection, ordering, and join
   shapes above.
3. Add parameter discovery, constraint inference, stable slots, and binding.
4. Add deterministic normalization and bounded plan caching.
5. Preserve the reusable compiler and warmed execution allocation boundaries.
6. Enforce scan, inner-membership rows/bytes, workspace, result, and
   cancellation budgets before non-fan-out or existence-query support ships.
7. Add a broader-SQL semi-join spelling/lowering before calling native
   `exists` part of the relational capability subset; native and SQL predicate
   truth tables remain intentionally distinct.
8. Ship non-fan-out and existence-query support only after heap/durable parity.

### Phase 3: canonical catalog definitions

1. Introduce one engine-owned semantic `CollectionDefinition` containing the
   optional schema and initial exact indexes.
2. Add the durable native-owned/SQL-owned catalog marker and reject cross-mode
   mutation before resolving collection names.
3. Compile the full definition before mutation and implement atomic
   create-or-match idempotency for heap and durable databases.
4. Persist and rehydrate the canonical definition and hash.
5. Add `describeCollection` and `listCollections`.
6. Let SQL materialize its pending table through the same engine definition
   while retaining only primary-key derivation and lazy-state metadata in the
   adapter.
7. Add golden canonical-definition bytes/hash fixtures, malformed/truncated/
   corrupt-catalog rejection, create-close-reopen-describe equality, and
   ownership-conflict and create/install/directory-sync crash cuts before
   declaring the format stable.
8. Update `docs/format.md`, `docs/store.md`, and SQL/admin API documentation in
   the implementation PR that freezes persisted catalog bytes.

### Phase 4: bounded fan-out

1. Replace heap-specific joined-row locations with backend-neutral ownership
   or a shared bounded operator.
2. Extend the Phase 2 join-build budgets with fan-out pair accounting and
   backend-neutral result ownership.
3. Complete durable fan-out without a second semantic implementation.
4. Prove heap/durable parity for key/path orientations, inner filters, zero
   matches, duplicate inner join values, exact numbers, ordering, limits,
   held snapshots, cancellation, and adversarial fan-out.
5. Accept native `join` only after every gate passes.

### Phase 5: public API readiness

1. Expose prepare, bind, execute, create, describe, and bounded-list APIs
   without leaking parser or backend internals.
2. Publish result ownership, parameter lifetime, and cancellation rules.
3. Fuzz every serialized document, path, parameter, schema, and index parser.
4. Add examples for database-bound query and catalog use, plus separate legacy
   collection-handle examples.
5. Document intentional SQL/native/legacy differences beside each surface.

## Validation matrix

Each implementation phase records its baseline commit, corpus, command, and
threshold in the PR that changes behavior.

| Gate | Required evidence |
| --- | --- |
| Correctness | Golden and randomized differential tests produce zero unexplained SQL/Go/native or heap/durable mismatches |
| Read path | Equivalent prepared native and Go plans select the same candidate/index path and perform the same bounded durable I/O |
| Space | Parser, cache, snapshot, join, workspace, and result limits stop at their documented byte/count ceilings |
| Allocations | A warmed prepared native plan adds no execution allocations beyond the equivalent warmed Go plan with equally reserved result/workspace capacity |
| Latency | Prepared native execution remains within 5% of the equivalent Go plan on the checked-in corpus; parse/bind are benchmarked separately |
| Prepare/bind | Warm prepare performs no more allocations and is within 10% of legacy `Compiler.Parse`; reusable bind is allocation-free and within 10% of a typed reference binder performing the same validation and slot writes on the checked-in 16-scalar/4-list corpus |
| Catalog command | Excluding storage sync, command lowering is within 10% of direct API creation and retains at most the canonical definition plus the 1 MiB parse arena |
| Catalog atomicity | Returned errors, `commit_unknown`, fault injection, and crash/reopen cuts satisfy their distinct absent-or-complete outcomes |
| Catalog format | Canonical order permutations reproduce byte-identical definition/hash golden fixtures across reopen |
| Index exactness | Hash-collision, exact-number, null, missing, compound-order, alias, and candidate-recheck tests return no false result |
| Join bounds | Adversarial many-to-many tests stop at each configured budget without publishing a partial complete result |
| Snapshot safety | Selected-collection, concurrent catalog change, held snapshot, close, and durable lease tests show no skew or leaked lease |

Performance thresholds may be deliberately changed only with before/after
measurements and an architectural explanation, following `CONTRIBUTING.md`.

## V1 release gates

The native query subset is ready to call version 1 only when:

- every accepted document fully normalizes before execution;
- legacy unversioned JSON behavior remains characterized and separate;
- duplicate members, unknown fields, and every published bound fail closed;
- exact literals and parameters never round through `float64`;
- the complete missing/null/membership truth tables pass on every backend;
- arrays never acquire implicit traversal;
- projected missing behavior is documented and tested;
- binding cannot change query shape or storage object names;
- joined collections come from one selected-collection database snapshot;
- existence and fan-out cardinality are explicit;
- scan, result, workspace, all-join build, fan-out pair, and cancellation
  budgets are enforced in the phase that first uses them;
- fan-out passes heap/durable differential and held-snapshot tests;
- schema and initial index definitions compile before atomic publication;
- each database has one durable catalog-mutation owner and cross-mode mutation
  fails before name resolution;
- invalid creation publishes no new collection and conflict leaves the
  existing definition unchanged;
- every write path enforces the same immutable schema and ready indexes;
- SQL/native tests cover shared semantics and enumerate intentional
  differences;
- the validation matrix has recorded baselines and passes;
- docs, examples, fuzz tests, and error locations describe the same syntax.

## Deliberately deferred

- grouping, reductions, `having`, and `offset`;
- structural containment and exact container equality;
- arbitrary scalar functions;
- nested result construction and JSON `lookup`;
- explicit array traversal;
- stable continuation tokens and general `$key` projection/order;
- query-driven writes and JSON Patch;
- online index creation/drop and physical reclamation;
- schema alteration, migration, closed objects, and richer constraints;
- destructive or transactional catalog commands;
- chained, outer, cross, natural, and recursive joins.

Each deferred query feature must first become a shared logical operator with
bounded backend execution. Each deferred catalog feature must first become one
atomic engine-owned capability on heap and durable storage. SQL remains
available when its existing semantics already fit the task.

## Rejected alternatives

### A complete JSON alternative to SQL

Rejected. Once JSON grows grouping, arbitrary join graphs, functions, DDL,
migrations, and mutations, it has recreated a less familiar SQL-sized
language. The supported subset keeps a clear application purpose.

### SQL as the internal representation

Rejected. Native missing/null semantics and exact JSON operands should lower
directly to logical primitives rather than be disguised as SQL text or AST.

### A JSON serialization of Go structs

Rejected. It would expose implementation layout and make compiler refactors
wire compatibility changes.

### JSON Schema compatibility

Rejected for v1. The engine has a small open-document type/presence schema.
Calling that partial behavior JSON Schema would promise keywords and recursive
semantics it does not implement.

### Dynamic native DDL before backend parity

Rejected. Heap online index lifecycle and frozen durable definitions are a real
capability difference. Syntax cannot make the durable mutation, crash
recovery, and reclamation contract exist.

### The legacy JSON shorthand as the versioned contract

Rejected. Scalar-or-array clause aliases, contextual join behavior, dotted
path ambiguity, duplicate-member handling, embedded parameters, and
missing/null collapse are not suitable compatibility boundaries.

## See also

- [Architecture](../architecture.md)
- [Store API and invariants](../store.md)
- [SQL surface](sql-surface.md)
- [Engine unification](unification.md)
