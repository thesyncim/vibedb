# Opt-in distributed routing with Vitess-compatible mappings

**Status:** proposed, pre-release design.
**Scope:** placement, routing, shard services, retained change export, online
movement, replica selection, and the contracts required for logical tables and
databases larger than 100 TB.

This document is the execution companion to
[`distributed-sharding.md`](distributed-sharding.md). The two documents must be
kept coherent. Where this proposal changes an older assumption in the parent
document, the required amendments (listed at the end of this document and
folded into `distributed-sharding.md` directly) must land in the same
documentation change.

Vibedb is unreleased. The implementation may break internal APIs and introduce a
new placement format. It must not preserve an earlier prototype at the expense
of placement correctness, consistency, or a reviewable implementation sequence.

## Decision

Implement an optional distributed router above Vibedb's existing typed SQL
runtime and local execution engine.

The router will:

- bind each logical table to a named distribution group;
- map one or more typed shard-key columns to one or more keyspace ranges;
- resolve those ranges against an immutable, versioned shard manifest;
- classify the physical route as empty, single-shard, targeted, or scatter;
- select the shard leader or an eligible replica under an explicit consistency
  policy;
- return an immutable route pinned to one routing version;
- remain independent from query execution, storage, transport, replication, and
  topology consensus.

The router will not embed VTGate, implement VTTablet, parse SQL a second time,
or adopt Vitess's MySQL execution/control plane.

Vitess compatibility is limited to deterministic distribution semantics that
are explicitly implemented and proven by differential tests.

## Capacity and availability are separate

Sharding does not require Raft.

A capacity-only deployment requires:

```text
deterministic placement
+ one fenced writer authority per shard
+ independent physical shards
+ a router
```

Raft, or another qualified replication protocol, is an optional availability
layer. It may provide replicated durability, leader election, stale-leader
fencing, membership changes, failover, and replica reads.

The existing recovery journal is **not** a replication or migration stream.
`docs/design/recovery-journal.md` defines a fixed-capacity, recyclable,
recovery-only redo ring. It cannot be tailed from an arbitrary historical
position and cannot satisfy online split, backup-position, or replication
catch-up requirements.

Therefore this design introduces a separate retained logical change stream
before online movement or replication:

```text
committed logical batch
        |
        +--> local state publication
        |
        +--> retained ordered change segment
                 |
                 +--> split/move consumer
                 +--> backup/export consumer
                 +--> optional replication consumer
```

The retained stream has stable per-shard commit positions independent of
checkpoint, repack, recovery-journal recycling, and local file layout. PR 5a
defines that component explicitly.

The router and local placement core remain independent from both Raft and the
change-stream implementation.

## Architecture

```text
SQL parser and typed runtime
          |
          v
constraint program
(plan-time constants, parameters, deferred memberships)
          |
          v
bind at execution time
          |
          v
finite typed domains per shard-key column
          |
          v
distribution mapper
(points or keyspace ranges)
          |
          v
immutable shard manifest
          |
          v
empty / single / targeted / scatter
          |
          v
leader or eligible replica selection
          |
          v
distributed executor
          |
          v
independent local Vibedb shards
```

The local `store` package remains unaware of the global cluster. A physical
shard is a normal independently owned Vibedb collection partition with its own
root, journal, cache, snapshots, file lifetime, and maintenance lifecycle.

## Terminology

### Distribution group

A **distribution group** is a named logical keyspace shared by colocated tables.

```go
type DistributionName string
type RoutingVersion uint64
type MapperVersion uint32
type TupleVersion uint32

type MapperSpec struct {
    Kind    string
    Version MapperVersion
    Params  map[string]string // immutable validated configuration
}

type ManifestRef struct {
    Distribution DistributionName
    Version      RoutingVersion
}

type DistributionSpec struct {
    Name          DistributionName
    KeyspaceBytes uint8
    Mapper        MapperSpec
    Manifest      ManifestRef
}
```

Configuration maps are permitted in immutable catalog metadata, not on the
steady-state route hot path.

Related tables reference the same group:

```text
distribution: tenant_data

messages        -> tenant_data(tenant_id)
channels        -> tenant_data(tenant_id)
channel_members -> tenant_data(tenant_id)
```

Colocation is proven by the same immutable distribution identity, compatible
ordered shard-key types, and join equality. Matching column names alone prove
nothing.

### Table placement

```go
type TablePlacement struct {
    Table        string
    Distribution DistributionName
    Columns      []string
    TupleVersion TupleVersion
}
```

`Columns` contains canonical top-level schema column names in significant order.

Version 1 placement scalars are deliberately small:

- exact `Number`;
- `String`.

Version 1 rejects:

- `Bool`;
- timestamps, because Vibedb has no separate stable timestamp scalar contract;
- `Any`;
- object, array, and general JSON values;
- nested paths;
- nullable or optional columns.

A shard-key column must be declared, required, non-null, and concretely typed.
Column order, spelling, type, mapper version, mapper parameters, and tuple
version are placement identity.

Changing placement identity is an online repartitioning operation.

### Unique-key locality invariant

When a table is bound to a distribution, **every unique key, including the
primary key, must contain every shard-key column**.

For shard key `(tenant_id, channel_id)`, these are valid local uniqueness
contracts:

```text
PRIMARY KEY (tenant_id, channel_id, id)
UNIQUE      (tenant_id, channel_id, external_id)
```

This is rejected:

```text
PRIMARY KEY (id)
```

Per-shard uniqueness would otherwise silently cease to be global uniqueness.

The invariant is checked when placement is bound in PR 2. The current dialect
does not support `ALTER TABLE` or `ON CONFLICT`, so version 1 does not need
in-place placement mutation or distributed upsert semantics.

### Placement metadata lifecycle

PR 2 uses immutable static configuration supplied when opening the local cluster
facade:

```go
type ClusterConfig struct {
    Distributions []DistributionSpec
    Placements    []TablePlacement
    Manifests     []Manifest
}
```

No catalog persistence is implied in PR 2.

PR 4b introduces the minimal authoritative catalog needed by a distributed
gateway: distribution specs, placements, active manifests, ownership epochs,
and publication generations. Schema/catalog migration beyond that remains a
later operational concern.

### Physical shard

A shard owns one non-overlapping half-open interval of a distribution group's
keyspace and contains the physical partitions for tables bound to that group.

The active manifest never contains overlapping ranges.

## Fixed-width keyspace

The Vitess-compatible subset is explicitly constrained to **8-byte keyspace
positions**.

Do not expose keyspace IDs as arbitrary byte strings and then store them in a
`uint64` accidentally. Define the fixed-width contract directly:

```go
const KeyspaceWidth = 8

type KeyspacePoint [KeyspaceWidth]byte

type KeyspaceEnd struct {
    Point KeyspacePoint
    Max   bool // exclusive 2^64 sentinel
}

type KeyRange struct {
    Start KeyspacePoint // inclusive
    End   KeyspaceEnd   // exclusive
}
```

Ordering is unsigned lexicographic big-endian ordering.

`End.Max` represents exactly `2^64`, is legal only for the final keyspace range,
and removes the ambiguous "`End == 0` may mean maximum" convention.

Manifest validation requires:

- sorted ranges;
- no gaps;
- no overlaps;
- first start at zero;
- only the final range ending at `Max`;
- complete coverage of the 8-byte keyspace;
- stable unique shard identifiers;
- at least one leader endpoint per active shard.

Vindexes that emit non-8-byte destinations are rejected by this compatibility
profile. This intentionally excludes mappings such as raw `binary` unless they
are wrapped by an explicitly supported fixed-width mapping.

## Mappers return explicit points and ranges

A mapper consumes an ordered leading prefix of typed shard-key columns and
returns explicit point and/or range destinations.

```go
type DestinationSet struct {
    Points []KeyspacePoint
    Ranges []KeyRange
}

type Mapper interface {
    Arity() int
    SupportedPrefixes() PrefixSet
    Version() MapperVersion
    Admits(prefixLen int, values []Scalar) error
    MapPrefix(values []Scalar) (DestinationSet, error)
}
```

Points are first-class. They are not encoded as
`[point, successor(point))`, avoiding overflow and sentinel ambiguity at the
maximum keyspace point.

Destination normalization must:

- sort and exact-deduplicate points;
- sort and merge overlapping or adjacent ranges where semantics permit;
- remove points already covered by a range;
- reject malformed ranges;
- preserve the fixed 8-byte compatibility profile.

A full mapping commonly returns one point.

A supported leading prefix of a multi-column mapper may return a wider range.
This is required for Vitess `multicol`: constraining leading components can
target a subset of shards without knowing every component.

A missing non-leading component never permits skipping ahead. For
`(tenant_id, channel_id, bucket)`:

- `tenant_id = ?` may map a prefix range;
- `tenant_id = ? AND channel_id IN (...)` may map several prefix destinations;
- `channel_id = ?` without `tenant_id` is unknown and becomes scatter;
- `tenant_id = ? AND bucket = ?` without `channel_id` may use only the known
  leading prefix.

The manifest resolver intersects explicit points and ranges with shard ranges.

## Vitess compatibility boundary

Reuse or exactly reproduce:

- supported Vindex mapping behavior;
- 8-byte keyspace positions;
- half-open key ranges;
- shard names such as `-40`, `40-80`, and `80-`;
- a strict supported subset of VSchema;
- primary/replica target terminology.

Do not reuse:

- VTGate execution or planning;
- Vitess SQL parsing;
- VTTablet or QueryService;
- Vitess topology;
- MySQL replication;
- VReplication;
- Vitess sessions or transactions;
- failover automation.

The root module must remain free of Vitess dependencies.

Suggested layout:

```text
distribution/
    bound_constraints.go
    destination.go
    manifest.go
    mapper.go
    policy.go
    route.go
    router.go
    token.go

x/vitessroute/
    go.mod
    adapter.go
    config.go
    multicol.go
    xxhash.go
    numeric_cast.go
    testdata/
    LICENSE-VITESS
```

The optional nested module follows existing repository precedent and prevents
the Vitess protobuf/gRPC dependency graph from affecting embedded users.

The public routing API must never expose upstream Vitess Go types.

## Valid VSchema subset

### Recommended single-column string key

```json
{
  "sharded": true,
  "vindexes": {
    "tenant_xxhash": {
      "type": "xxhash"
    }
  },
  "tables": {
    "messages": {
      "column_vindexes": [
        {
          "column": "tenant_id",
          "name": "tenant_xxhash"
        }
      ]
    }
  }
}
```

`xxhash`, `binary_md5`, or an explicitly supported Unicode mapping may accept
strings according to their upstream contract.

The legacy Vitess `hash` Vindex is not a general string hash. It uses numeric
conversion and a DES-based mapping. The adapter must reject string values for
that mapping and should prefer `xxhash` for new schemas.

### Multi-column key

A compatible multi-column configuration uses the Vitess `multicol` mapping and
its per-column mappings. The loader accepts only the exact upstream fields for
the pinned Vitess version, conceptually:

```json
{
  "sharded": true,
  "vindexes": {
    "tenant_channel": {
      "type": "multicol",
      "params": {
        "column_count": "2",
        "column_bytes": "4,4",
        "column_vindex": "xxhash,xxhash"
      }
    }
  },
  "tables": {
    "messages": {
      "column_vindexes": [
        {
          "columns": ["tenant_id", "channel_id"],
          "name": "tenant_channel"
        }
      ]
    }
  }
}
```

The actual accepted JSON keys and values are pinned by differential tests
against one Vitess release. Unknown or unsupported fields are errors, never
silently ignored.

The `multicol` result is constructed from independently mapped/truncated column
components. It is not a hash of Vibedb's tuple encoding.

Initially supported:

- one primary Vindex per table;
- `xxhash`;
- a carefully defined numeric-cast `hash`, if retained;
- `multicol` over supported component mappings;
- only configurations whose total destination width is 8 bytes.

Initially rejected:

- lookup and owned Vindexes;
- sequences;
- reference tables;
- routing rules;
- auto-increment behavior;
- cross-keyspace plans;
- arbitrary binary-width destinations;
- Vitess tablet/topology records.

## Frozen placement scalar and tuple encoding

The tuple codec is **not** a universal input to Vitess-compatible Vindexes.

It has two roles:

1. canonical placement identity and shard-key immutability comparison;
2. input to native Vibedb mappers that explicitly declare that contract.

Each Vitess-compatible mapper consumes columns according to its pinned upstream
input contract.

### Version 1 scalar set

Version 1 admits only:

```text
String
exact Number
```

`Bool`, timestamps, `Any`, nested values, objects, and arrays are rejected.
Keeping the first scalar set closed prevents an agent from inventing an
unreviewed placement format.

### Exact-number canonicalization

Vibedb equality treats numerically equal spellings as the same value. Placement
must use the same exact canonical decomposition already used by grouping and
join equality:

```text
canonical sign
+ adjusted decimal weight
+ trimmed significant digits
```

The following values must have identical scalar and tuple bytes:

```text
5
5.0
5e0
50e-1
```

Negative zero canonicalizes to zero:

```text
-0 == 0
```

`internal/orderedkey` is the implementation substrate to study, but its current
format is not automatically frozen as placement identity. PR 1a must either:

- extract a minimal placement-specific `tuple/v1` snapshot; or
- fork the relevant exact-number/string encoding into a frozen package.

Local ordered-index or store-format evolution must never silently change
placement bytes.

```go
type TupleCodec interface {
    AppendScalar(dst []byte, value Scalar) ([]byte, error)
    AppendTuple(dst []byte, values []Scalar) ([]byte, error)
    Version() TupleVersion
}
```

Strings use binary UTF-8 bytes with an unambiguous framing/escaping contract.
There is no collation behavior in placement version 1.

### Deduplication identity

Finite-domain values are deduplicated by their frozen canonical scalar bytes.
Complete shard-key tuples are compared by frozen tuple bytes.

No Go interface equality, source spelling, float conversion, or textual
concatenation participates in placement equality.

### Per-mapper input serialization

Every compatible mapper pins its own input serialization. Tuple bytes are not
used implicitly.

For byte-hashing mappings such as the supported `xxhash` profile:

- `String` maps from its raw UTF-8 bytes;
- `Number` is admitted only when it is an exact integer that losslessly fits the
  supported upstream signed or unsigned 64-bit domain;
- admitted integers serialize as canonical minimal decimal ASCII;
- zero serializes as `0`, including negative zero;
- no leading plus sign or leading zero is emitted;
- fractional, overflowing, and otherwise unsupported numbers are rejected.

For the legacy numeric-cast `hash` mapping:

- only the same lossless admitted integer set is accepted;
- conversion follows the pinned upstream numeric rule;
- strings and fractional numbers are rejected;
- no rounding, truncation, or stringify fallback is allowed.

For `multicol`, each component is serialized and mapped independently according
to its declared component mapping. The result is not a hash of a Vibedb tuple.

All mapper input behavior is placement identity and requires differential golden
vectors.

## Constraint binding

The router consumes bound typed domains indexed by shard-key ordinal, not SQL
text and not a hot-path `map[string]...`.

```go
type DomainKind uint8

const (
    DomainUnknown DomainKind = iota
    DomainEmpty
    DomainFinite
)

type ValueDomain struct {
    Kind   DomainKind
    Values []Scalar // canonical-byte deduplicated
}

type BoundConstraints []ValueDomain
```

`BoundConstraints[i]` corresponds to placement column `Columns[i]`.

The SQL planner owns a `ConstraintProgram` that may reference:

- constants;
- typed bound parameters;
- runtime values;
- join-fed membership sets.

The program is bound as late as practical before routing. Version 1 may scatter
when a join-fed set is not yet available, but the API permits route-after-bind
later without redesign.

### Predicate semantics

For each shard-key ordinal:

1. intersect every equality and membership predicate;
2. compare and deduplicate by frozen canonical scalar bytes;
3. produce `DomainEmpty` on contradiction;
4. preserve `DomainUnknown` when a value cannot be bound safely.

Examples:

```text
x = 5 AND x IN (4, 5, 5.0) -> finite {5}
x = 5 AND x IN (6, 7)      -> empty
x IN (5, 5.0, 5e0)         -> finite {5}
```

An empty domain yields `RouteEmpty` without mapper or network work.

The implementation should use placement arity and caller-owned/reused slices to
avoid map allocation and string hashing on the route hot path.

## Candidate expansion and admission

Routing expands only finite values needed by the longest mapper-supported
leading prefix.

```go
type RouteLimits struct {
    MaxCandidateMappings int
    MaxTargetShards      int
}

type RouteAdmission uint8

const (
    AdmissionTargetedOnly RouteAdmission = iota
    AdmissionAllowScatter
    AdmissionAllowScatterOnOverflow
)

type RoutePolicy struct {
    Limits    RouteLimits
    Admission RouteAdmission
}
```

The zero value is fail-closed: targeted routes only.

Admission truth table:

| Admission | Unknown/scatter route | Candidate/target overflow |
| --- | --- | --- |
| `AdmissionTargetedOnly` | `ErrScatterRejected` | exact limit error |
| `AdmissionAllowScatter` | scatter allowed | exact limit error |
| `AdmissionAllowScatterOnOverflow` | scatter allowed | degrade to scatter |

Zero limits are replaced by conservative constructor defaults, never
interpreted as unlimited.

Before Cartesian expansion:

- intersect predicates;
- canonical-byte deduplicate each finite domain;
- stop at the longest valid leading prefix;
- estimate the product with overflow-safe arithmetic.

A route physically covering every active shard is classified as `RouteScatter`,
even when finite exact values produced it.

## Route contract

```go
type ShardID string
type EndpointID string
type OwnershipEpoch uint64

type RouteKind uint8

const (
    RouteEmpty RouteKind = iota
    RouteSingle
    RouteTargeted
    RouteScatter
)

type Role uint8

const (
    RoleLeader Role = iota
    RoleReplica
)

type Target struct {
    Shard          ShardID
    Endpoint       EndpointID
    OwnershipEpoch OwnershipEpoch
    Role           Role
}

type Route struct {
    Kind           RouteKind
    Distribution   DistributionName
    RoutingVersion RoutingVersion
    Targets        []Target
}
```

Endpoint IDs are opaque to the dependency-free router and are resolved by
transport code.

Ownership epochs fence stale writers independently from routing versions. PR 4a
introduces a static configured epoch checked by the shard service. PR 5 changes
epochs during movement.

The manifest resolver performs:

```text
point destination -> O(log shard_count)
range destination -> O(log shard_count + overlapping_shards)
```

Validated manifests retain ordered range starts for binary search. Steady-state
routing never linearly scans every shard.

## Read routing

```go
type ReadConsistency uint8

const (
    ReadStrong ReadConsistency = iota
    ReadSession
    ReadStale
)

type ReplicaFallback uint8

const (
    FallbackLeader ReplicaFallback = iota
    FallbackError
)

type ReadPolicy struct {
    Consistency ReadConsistency
    Session     SessionToken
    PreferZone  string
    Fallback    ReplicaFallback
}
```

The zero values are intentionally safe:

```text
ReadStrong + FallbackLeader
```

The enum ordering is part of the public safety contract and must remain stable.

Initial behavior:

| Policy | Eligible endpoint | Guarantee |
| --- | --- | --- |
| `ReadStrong` | leader | current shard-local read under the selected ownership/replication protocol |
| `ReadSession` | replica proven to include the token, otherwise configured leader validation/fallback | read-your-writes visibility represented by the token |
| `ReadStale` | healthy serving replica | internally consistent pinned local root that may lag |

No policy silently weakens consistency.

### Opaque session tokens

```go
type SessionToken []byte
```

The public boundary is opaque from the first release.

Internally, a token may carry:

- distribution and routing generation;
- per-source-shard commit positions;
- transition proofs or certificates;
- expiry/version data.

On a routing-version mismatch with `FallbackLeader`, route to the relevant
leader for token validation or translation. If visibility cannot be proven, the
leader returns a typed `token expired` or `token indeterminate` result. Never
silently downgrade to stale.

Without replication, only `ReadStrong` is available and resolves to the
single-writer endpoint.

## Write routing

Reads and writes use separate safety contracts.

```go
type ShardKey []Scalar

type WriteRouter interface {
    RouteInsert(table string, keys []ShardKey) (Route, error)
    RouteDelete(table string, constraints BoundConstraints) (Route, error)
    RouteUpdate(
        table string,
        constraints BoundConstraints,
        assignedColumns []string,
    ) (Route, error)
}
```

The SQL layer extracts post-coercion shard-key values from inserted rows.

Before any write routing is enabled, placement validation enforces that every
primary or unique key contains every shard-key column.

### Inserts

- every row supplies every shard-key column;
- all rows are mapped before dispatch;
- version 1 accepts only a single physical target shard;
- cross-shard multi-row inserts return `ErrCrossShardWrite`;
- no participant receives work before preflight completes.

### Deletes

A delete must resolve to exactly one physical shard in version 1. Unknown,
targeted multi-shard, and scatter deletes fail before dispatch.

Because every primary key contains the shard key, a complete primary-key
predicate remains directly routable.

### Updates

Every assignment to a shard-key column is rejected statically, including:

```sql
SET tenant_id = tenant_id
```

A non-shard-key update must resolve to exactly one shard.

A future shard-key move is an explicit distributed delete-plus-insert workflow,
not relaxed update behavior.

## Distributed execution wire contract

The shard-service wire contract carries:

- SQL text;
- typed bound parameters;
- target distribution and shard;
- routing version;
- ownership epoch;
- read policy;
- request deadlines and resource limits.

It does **not** carry a serialized Vibedb execution plan or serialized
`ConstraintProgram`.

Each shard service parses and plans the SQL locally using the same Vibedb parser
and planner. This does not violate the rule against parsing SQL a second time
inside the router: the router consumes bound routing constraints, while the
authoritative shard independently executes the original statement.

This choice avoids freezing a second distributed plan format and aligns with the
existing pgwire prepared-statement/typed-parameter model.

The shard service pins a local snapshot for the request duration. Multi-shard
reads pin one independent local snapshot per shard; no global real-time snapshot
is implied.

## Logical tables larger than 100 TB

The scaling unit is not one giant Vibedb file.

```text
100+ TB logical table
        |
        v
hundreds or thousands of independently owned table partitions
        |
        v
one partition per physical keyspace shard
```

A logical table may therefore be larger than 100 TB while each physical shard
stays inside a qualified operating envelope.

There is no data-size ceiling in the 8-byte routing space. The practical limits
are:

- per-shard storage and recovery behavior;
- number of independently owned shards;
- movement and split throughput;
- scatter admission;
- gateway fan-out;
- schema and backup orchestration.

### Shared distributions across tables

Large databases must not maintain unrelated manifests per table when tables are
intended to colocate.

A named distribution group owns one manifest and mapper. Every bound table uses
that same routing generation.

This permits local joins such as:

```text
messages(tenant_id)
channels(tenant_id)
members(tenant_id)
```

when the join equates corresponding shard-key components.

The planner proves colocation from the shared distribution identity, compatible
ordered types, and join equality—not from column names.

### Non-uniform ranges

Manifests permit arbitrary validated boundaries:

```text
-80
80-c0
c0-e0
e0-f0
f0-
```

A hot or oversized range can be split repeatedly without repartitioning the
entire table.

Equal-width ranges are an initialization convenience, never an invariant.

### Shard operating envelope

A shard must not be allowed to grow indefinitely.

```go
type ShardEnvelope struct {
    SoftLiveBytes       uint64
    HardLiveBytes       uint64
    SoftWriteBytesPerS  uint64
    HardWriteBytesPerS  uint64
    MaxRecoveryDuration time.Duration
    MaxSnapshotDuration time.Duration
    MaxRestoreDuration  time.Duration
    MaxMaintenanceDebtBytes uint64
}
```

The actual thresholds come from Vibedb qualification, not copied from MySQL or
another engine.

The split planner considers:

- live bytes;
- write and read rate;
- recovery and replay duration;
- checkpoint/repack debt;
- snapshot copy duration;
- backup/restore duration;
- hot-key concentration;
- node capacity and failure domain.

The required shard count is driven by the strictest storage, throughput,
recovery, and movement limit—not only total bytes.

### Huge tenants and hot keys

Sharding only by `tenant_id` guarantees tenant locality but caps one tenant at
one shard's operating envelope.

A table expected to contain extremely large tenants must use a second locality
dimension, for example:

```text
(tenant_id, channel_id)
(tenant_id, bucket)
(tenant_id, time_bucket)
```

A `multicol`-style prefix mapping permits:

- full key -> one point/shard;
- leading tenant prefix -> a keyspace range/subset of shards;
- tenant-wide work -> bounded fan-out inside that prefix when the mapper and
  manifest permit it.

The trade-off is explicit:

```text
tenant_id only
  -> tenant operations local
  -> one tenant cannot exceed one shard

tenant_id + subkey
  -> one tenant can span many shards
  -> tenant-wide operations fan out
```

No mapper can fix a single un-splittable hot key. Schemas must expose a stable
subdivision dimension for such workloads.

## Online shard splitting and movement

A static manifest can address 100 TB but cannot operate safely at that scale.
Online split/move is a core requirement.

It cannot be built on Vibedb's recovery-only recyclable journal.

PR 5 depends on PR 5a's retained logical change stream and stable source commit
positions.

```text
1. allocate target shard owners and epochs
2. pin a source snapshot at ChangePosition P
3. bulk-copy that snapshot
4. consume retained logical changes strictly after P
5. apply changes idempotently on targets
6. verify rows, indexes, counts, roots/checksums, and progress
7. fence source writes by changing/closing the ownership epoch
8. drain and apply the final retained tail
9. atomically publish the new routing version and ownership epochs
10. preserve rollback/catch-up state for the declared window
11. retire source ownership only after safety gates pass
```

The active routing manifest remains non-overlapping. Migration state lives in a
separate transition record.

Required primitives are:

- one fenced writer authority per source shard;
- stable snapshot export;
- retained ordered logical change positions;
- idempotent target apply keys;
- exact verification;
- atomic catalog publication;
- typed stale routing-generation and ownership-epoch handling.

The term **transition token** is not used here; session tokens belong to replica
read semantics in PR 6.

## Routing and control plane at scale

Gateways are stateless with respect to authoritative metadata.

```text
clients
   |
   v
many stateless gateways
   |
   v
atomic pointers to immutable catalog snapshots
   |
   v
hundreds or thousands of shard services
```

PR 2 uses static in-process configuration.

PR 4b introduces a minimal authoritative catalog containing:

- distribution specs;
- table placements;
- active manifests;
- ownership epochs;
- publication generations;
- endpoint membership and serving state.

Steady-state queries never synchronously consult the catalog. Gateways receive,
validate, and atomically publish complete immutable snapshots.

A request pins one catalog/routing generation for routing and execution. It never
mixes generations.

The control plane is not required to be Raft-backed by this design. Whatever
implementation is selected must provide strongly ordered publication and fenced
ownership updates under its claimed failure model.

## Scatter admission

Scatter cost grows with shard count and is always explicit.

The core routing contract uses `RouteAdmission`; it does not carry a workload
class.

PR 4b may layer operational classes over that primitive:

```text
interactive     -> targeted only by default
batch           -> explicitly bounded scatter
administrative  -> explicitly bounded scatter with separate quotas
```

The gateway caps:

- concurrent shard requests;
- total buffered rows and bytes;
- merge-sort memory;
- per-shard and global deadlines;
- retries;
- result limits;
- partial-result behavior.

A route covering every active shard is scatter for admission and metrics,
regardless of how it was derived.

## Alternative-key lookups and global indexes

A local secondary index only finds rows inside a known shard.

At 100 TB this query must not scatter:

```sql
WHERE message_id = ?
```

when placement is by `tenant_id`.

Long-term options are:

- require the owning tenant/distribution key;
- encode routing information into globally unique IDs;
- maintain a separately distributed lookup index;
- add a lookup-routing service with explicit consistency semantics.

Reserve the abstraction without implementing it in version 1:

```go
type LookupRouter interface {
    Resolve(
        table string,
        index string,
        key Scalar,
    ) (DestinationSet, error)
}
```

Lookup maintenance, transactional guarantees, ownership, and resharding require
a separate design. Do not import Vitess lookup Vindexes casually.

## Schema changes

A logical table schema change is orchestrated across every active shard in its
distribution group.

The control plane records:

- target schema version;
- compatible routing/tuple versions;
- per-shard application state;
- validation state;
- cutover or rollback state.

A schema change may not mutate shard-key type, order, mapper, or tuple version
in place. Those changes create a new distribution placement and use the online
resharding protocol.

## Backup and restore

A 100+ TB logical database is backed up per shard plus authoritative metadata.

A backup set records:

- distribution and catalog generation;
- table schema versions;
- per-shard snapshot identity;
- per-shard retained `ChangePosition`;
- ownership epoch;
- transition state required for restore;
- checksums and encryption metadata.

A `ChangePosition` is supplied by PR 5a's retained logical stream, not by the
recovery journal.

Without a separately qualified global barrier, the backup is a set of
shard-local consistent snapshots, not one global real-time snapshot.

Restore to a different shard count uses a verified repartition/import workflow.

## Failure behavior

Required typed errors include:

```go
var (
    ErrNoDistribution          = errors.New("table has no distribution")
    ErrInvalidManifest         = errors.New("invalid shard manifest")
    ErrUnsupportedMapper       = errors.New("unsupported mapper")
    ErrInvalidShardValue       = errors.New("invalid shard-key value")
    ErrIncompleteShardKey      = errors.New("incomplete shard key")
    ErrRouteExpansionLimit     = errors.New("route expansion limit exceeded")
    ErrTargetShardLimit        = errors.New("target shard limit exceeded")
    ErrScatterRejected         = errors.New("scatter route rejected")
    ErrShardKeyImmutable       = errors.New("shard key is immutable")
    ErrCrossShardWrite         = errors.New("cross-shard write unsupported")
    ErrNoEligibleReplica       = errors.New("no eligible replica")
    ErrRoutingVersion          = errors.New("routing version mismatch")
    ErrSessionTokenExpired     = errors.New("session token expired")
    ErrSessionTokenIndeterminate = errors.New("session token cannot be proven")
    ErrChangePositionExpired    = errors.New("change position expired")
    ErrChangeConsumerLag        = errors.New("change consumer exceeded retention")
    ErrOwnershipEpoch           = errors.New("ownership epoch mismatch")
)
```

Malformed or ambiguous placement metadata fails closed.

A stale routing version may be retried only under a bounded gateway policy and
never by mixing manifests in one operation.

## Observability

Every routed operation exposes:

- distribution and table;
- route kind;
- routing version;
- mapper and tuple versions;
- bound leading-prefix length;
- candidate mapping count;
- destination range count;
- selected shard count;
- selected role and zone;
- replica fallback count;
- scatter reason;
- overflow reason;
- route, selection, execution, and merge latency;
- bytes/rows returned per shard;
- stale-route and token-validation outcomes.

Do not log raw shard-key values by default.

Cluster-level signals include:

- shard live bytes and rates;
- recovery/snapshot/restore estimates;
- maintenance debt in bytes;
- split pressure;
- hot ranges;
- movement backlog;
- scatter rate;
- gateway fan-out;
- replica progress;
- manifest propagation age.

## Required amendments to `distributed-sharding.md`

The documentation change must amend the parent design so it no longer
contradicts this execution plan.

Required changes:

1. Describe Raft as one optional replication/failover implementation, not a
   prerequisite for sharding or leader-only operation.
2. Distinguish the recyclable recovery journal from the retained logical change
   stream required for migration, backup positions, and replication catch-up.
3. Introduce typed ownership epochs checked by shard services.
4. State that active manifests remain non-overlapping while transition records
   carry migration state.
5. Make public session tokens opaque and require leader validation/translation
   across routing changes.
6. Permit a leader-only deployment with static ownership before replication.
7. Define SQL text plus typed parameters as the initial shard-service wire
   contract; no serialized plan format is introduced.
8. Define the uniqueness-locality invariant: every unique key contains all
   shard-key columns.
9. Separate continuous scale-model tests from full 100+ TB release
   qualification.

## Dependency strategy

### Core

`distribution` is dependency-free and testable with native mappings.

### Optional compatibility adapter

`x/vitessroute` is a nested module.

Two acceptable internal strategies:

1. narrowly import stable upstream mapping packages; or
2. maintain a small Apache-2.0-attributed compatible implementation.

The choice is based on dependency graph, API stability, auditability, and
differential-test coverage.

The adapter pins one Vitess release. Updating that release is an explicit
compatibility change with regenerated vectors and review.

## Qualification

No scale, compatibility, allocation, or consistency claim is admitted without
evidence.

### Tuple and equality vectors

Mandatory golden vectors include:

```text
5 == 5.0 == 5e0 == 50e-1
-0 == 0 where SQL equality says so
large exact exponents
integer boundaries
escaped and zero-containing strings
binary UTF-8 distinctions
```

Equivalent values must produce identical tuple bytes and native placement.

### Vitess differential tests

For every supported mapping:

1. pin one upstream Vitess release;
2. generate deterministic scalar and prefix corpora;
3. compute upstream destinations;
4. compute adapter destinations;
5. require exact range equality;
6. persist representative vectors;
7. fuzz invalid numeric conversions and boundary widths.

### Manifest properties

- every keyspace point belongs to exactly one active shard;
- range intersection returns exactly the overlapping shards;
- `End.Max` appears only on the final shard;
- active manifests never overlap or contain gaps;
- binary search equals a slow reference resolver;
- manifests are immutable after publication;
- atomic replacement never returns a mixed generation.

### Constraint properties

- predicate order does not change domains;
- equality and membership intersect correctly;
- exact numeric spellings deduplicate;
- contradiction yields `RouteEmpty`;
- bounded expansion never exceeds limits;
- unknown dynamic values never narrow routing unsafely;
- leading prefixes produce only mapper-authorized ranges.

### Write properties

- multi-row inserts never partially dispatch across shards;
- shard-key assignments are rejected statically;
- delete/update never fan out in version 1;
- all cross-shard write errors happen before participant publication.

### Replica/session properties

- zero policy is strong;
- no policy silently weakens;
- stale replicas never satisfy session reads without proof;
- version mismatch with leader fallback performs validation;
- unprovable tokens return typed expired/indeterminate errors.

### Scale qualification

Qualification is tiered.

#### Continuous scale-model gates

These run in CI or scheduled engineering validation using many small shards:

- thousands of small shard manifests;
- repeated split storms;
- manifest and catalog churn;
- ownership-epoch changes;
- stale-route storms;
- gateway fan-out limits;
- control-plane outage after cache publication;
- change-stream lag and retention pressure;
- node drain orchestration;
- schema rollout state machines;
- backup-catalog reconstruction.

These tests prove orchestration invariants, not 100 TB storage behavior.

#### Per-shard qualification

Run against the actual local engine at increasing scale:

- qualified maximum live bytes and document/index counts;
- sustained read/write churn;
- crash/recovery and replay bounds;
- checkpoint and repack behavior;
- index rebuild;
- retained-change export overhead;
- snapshot transfer;
- backup and restore;
- failure injection during maintenance.

#### Release qualification

The full 100+ TB generated/imported logical-table run is a release-qualification
event, not a normal CI gate.

It must cover:

- a 100+ TB logical table across qualified shards;
- huge-tenant subsharding;
- repeated non-uniform online splits;
- colocated local joins;
- bounded tenant-prefix fan-out;
- rejected accidental global scatter;
- schema rollout;
- backup catalog and full restore;
- verified shard-count change;
- documented cost, duration, and recovery envelope.

Only completed release qualification permits a public 100+ TB claim.

### Performance targets

Measure independently:

- constraint binding;
- numeric canonicalization;
- tuple append;
- single-column mapping;
- multi-column full and prefix mapping;
- point and range manifest lookup;
- finite-domain expansion;
- replica selection;
- full route construction;
- gateway fan-out/merge.

The single-key, single-shard route should be allocation-free after catalog and
manifest publication. This remains a target until benchmarks prove it.

## Implementation plan

This is the executable ladder from the frozen placement format through
100+ TB operations. Each item below is a separate PR.

### Global rules

For every PR, the implementing engineer or agent must:

1. read the current repository and directly affected tests before editing;
2. produce a concise implementation map;
3. stay inside the PR boundary;
4. add tests with the implementation;
5. run repository-standard vet/test/race commands;
6. run relevant fuzz smoke tests and benchmarks;
7. keep commits small and bisectable;
8. stop rather than invent placement, consistency, durability, or wire-format
   semantics;
9. record deviations in the PR description.

Never:

- regenerate, rewrite, or edit committed placement golden vectors merely to make
  a failing test pass;
- update expected Vitess differential vectors without proving an intentional
  pinned-version change;
- alter frozen placement bytes as incidental refactoring;
- introduce Raft, networking, Vitess, or topology before the permitting PR;
- serialize execution plans onto the wire;
- weaken consistency or scatter admission silently;
- split one SQL write across shards in version 1;
- claim 100+ TB readiness from scale-model tests.

A golden-vector mismatch is a stop condition. The implementation or the design
must be reviewed; expected bytes are not self-healing test data.

### Required reconnaissance

Before PR 1a, inspect at least:

```text
query/
sql/
store/
store/durable/
internal/orderedkey/
internal/conformance/
pgwire/
cmd/
docs/design/distributed-sharding.md
docs/design/recovery-journal.md
docs/design/hybrid-mutations.md
docs/design/parallel-tablet-writers.md
docs/design/sql-surface.md
docs/durability.md
CONTRIBUTING.md
go.mod
integration/pgclient/go.mod
bench/competitive/go.mod
```

Report:

- canonical typed value representation;
- exact numeric equality and negative-zero behavior;
- number/string encoding reusable for frozen placement;
- constants, parameters, `IN`, and join-fed membership representation;
- existing primary-key immutability behavior;
- package dependency direction;
- durable commit and snapshot boundaries;
- why the recovery journal cannot be a retained stream;
- pgwire request/parameter capabilities;
- command/server precedent;
- conformance capability-matrix gates;
- fuzz and benchmark conventions;
- contradictions with this design.

### Repository-standard validation commands

Unless a narrower package-only command is explicitly added, every code PR runs:

```bash
go vet ./...
go test -count=1 -timeout=25m ./...
go test -count=1 -race -timeout=25m ./...
```

Targeted fuzz smoke commands and benchmarks are additional, not replacements.

### PR 1a — Frozen placement scalars and tuple codec v1

#### Goal

Freeze the only byte format in the routing design before building manifests or
routing around it.

#### Scope

```text
distribution/scalar and tuple packages
tests/testdata for golden vectors
docs/design/
```

`internal/orderedkey` is reference material. Extract or fork only the minimal
placement encoding; do not freeze unrelated local index/store formats.

#### Pre-resolved semantics

Version 1 admits only:

```text
String
exact Number
```

Rules:

- binary UTF-8 strings;
- exact canonical number decomposition matching current group/join equality;
- `5`, `5.0`, `5e0`, and `50e-1` encode identically;
- `-0` encodes identically to `0`;
- `Bool`, timestamp, `Any`, object, array, and nested values are rejected;
- tuple version is explicit;
- committed golden vectors are immutable compatibility artifacts.

#### Deliverables

- closed placement `Scalar`;
- canonical scalar encoding;
- canonical ordered tuple encoding;
- tuple version 1;
- zero-allocation append APIs where correctness permits;
- golden vectors and fuzz tests;
- written format specification sufficient for an independent implementation.

#### Mandatory tests

- spelling-equivalent exact numbers;
- negative zero;
- huge valid exponents;
- integer boundaries;
- empty/zero-containing/escaped UTF-8 strings;
- type separation and tuple boundaries;
- deterministic repeated encoding;
- malformed/unsupported value rejection.

#### Stop conditions

Stop if:

- exact equality cannot be reproduced without semantic change;
- a frozen copy would accidentally expose/store-freeze unrelated internals;
- expected vectors disagree with existing equality;
- an allocation target conflicts with correctness.

#### Acceptance gate

Repository-standard commands pass, targeted fuzz smoke passes, benchmark and
allocation results are recorded, and golden vectors were not rewritten to hide
a mismatch.

### PR 1b — Fixed keyspace, immutable manifests, and resolver

#### Goal

Implement placement-independent keyspace geometry and fast manifest resolution.

#### Deliverables

- typed `ShardID`, `EndpointID`, `OwnershipEpoch`, and `RoutingVersion`;
- `[8]byte` keyspace points;
- explicit maximum end sentinel;
- explicit `DestinationSet.Points` and `.Ranges`;
- immutable manifest validation;
- binary-search point resolution;
- binary-search-plus-walk range resolution;
- deterministic destination normalization;
- slow reference resolver used only in tests;
- concurrent immutable publication tests.

#### Mandatory tests

- gaps/overlaps/order rejection;
- complete keyspace coverage;
- `End.Max` only on final shard;
- maximum point handling without successor overflow;
- point/range normalization;
- fast resolver equals reference resolver;
- race-safe atomic publication;
- O(log n) behavior demonstrated by benchmark shape.

#### Non-goals

No scalar changes, domains, mappers, SQL, endpoints selection policy, networking,
or Vitess.

### PR 1c — Domains, native mapper, route classification, and admission

#### Goal

Build dependency-free routing over PR 1a/1b.

#### Deliverables

- ordinal `BoundConstraints []ValueDomain`;
- canonical-byte intersection and deduplication;
- native test mapper with full and prefix destinations;
- mapper arity/prefix/type contracts;
- candidate expansion with overflow-safe estimation;
- `RouteEmpty`, `RouteSingle`, `RouteTargeted`, and `RouteScatter`;
- `RouteAdmission` truth-table behavior;
- conservative default limits;
- leader-only target selection using immutable manifest metadata.

#### Mandatory tests

- equality/IN intersection;
- contradiction to empty;
- unknown never narrows;
- dedup before product;
- prefix mapping;
- all-shard result classified as scatter;
- exact candidate and target limit errors;
- all admission modes;
- single-shard hot-path allocation benchmark.

#### Non-goals

No SQL integration, static cluster facade, networking, Vitess, catalog, replicas,
or online movement.

### PR 2 — SQL binding, placement validation, and local cluster facade

#### Goal

Exercise routing and write preflight end-to-end without networking.

#### Metadata model

Distribution specs, placements, and manifests are supplied as immutable static
configuration at open time. They are not persisted in a catalog yet.

#### Deliverables

- planner `ConstraintProgram`;
- bind after typed constants/parameters are available;
- shape permitting later execution-time join-fed memberships;
- route-after-bind;
- placement validation that every unique/primary key contains every shard-key
  column;
- insert routing from post-coercion rows;
- same-shard multi-row insert acceptance;
- cross-shard insert rejection before execution;
- shard-key `SET` rejection, including no-ops;
- single-shard-only update/delete;
- degenerate local cluster mode:
  - one distribution;
  - one manifest shard;
  - one embedded store;
  - full router/write-preflight path;
  - no network.

#### Regression gates

- constants and parameters route identically;
- exact-number spelling equivalence survives binding;
- deferred membership stays unknown until available;
- local non-distributed behavior is unchanged;
- `internal/conformance` capability matrix remains unchanged;
- pgwire/local SQL tests remain unchanged except intentional new routing tests.

#### Non-goals

No catalog persistence, shard server, RPC, gateway, Vitess, replication, or
cross-shard execution.

### PR 3 — Optional Vitess-compatible mapper module

#### Goal

Add a nested module with a strict pinned compatibility profile.

#### Deliverables

- nested `go.mod`;
- pinned Vitess version;
- Apache attribution/license handling;
- strict VSchema subset;
- `xxhash`;
- fixed-width `multicol` with prefix ranges;
- exact per-mapper input serialization:
  - raw UTF-8 strings;
  - minimal decimal ASCII for lossless admitted integers;
  - all other numbers rejected;
- optional legacy numeric `hash` only with complete differential proof;
- differential vectors for points and prefixes;
- no upstream types in Vibedb public APIs;
- no root dependency changes.

#### Stop conditions

If upstream package reuse drags topology, tablet, execution, or server machinery,
implement the small compatible algorithms locally with attribution and retain
the differential harness.

### PR 4a — Shard service and ownership admission

#### Goal

Create the first deployable shard service without gateway fan-out.

#### Pre-decided wire contract

Requests carry SQL text plus typed parameters, not serialized plans.

The shard uses the same Vibedb parser/planner locally.

#### Deliverables

- shard-service command/package;
- request/response protocol;
- target distribution, shard, routing version, and ownership epoch fields;
- statically configured shard ownership and epoch;
- rejection of wrong shard or stale epoch;
- typed not-owner/stale-generation responses;
- request deadlines and resource limits;
- local snapshot pinning for the request;
- leader-only read/write service;
- prepared/typed parameter behavior compatible with existing pgwire semantics
  where reused.

#### Acceptance gate

Prove epoch admission, snapshot lifetime, restart behavior, malformed request
handling, resource limits, and no serialized-plan format.

### PR 4b — Gateway, minimal catalog, and bounded distributed reads

#### Goal

Add stateless gateways and authoritative metadata publication.

#### Deliverables

- minimal persistent catalog for distributions, placements, manifests, epochs,
  and publication generations;
- complete immutable catalog snapshots;
- atomic gateway publication;
- one pinned generation per operation;
- leader-only endpoint dispatch;
- bounded targeted/scatter reads;
- concurrency, deadline, retry, memory, and result limits;
- shard result merge;
- stale route/epoch retry only after refreshed compatible catalog state;
- interactive/batch/admin operational profiles layered over core admission;
- route/fan-out metrics.

#### Non-goals

No online movement, retained stream, replicas, or cross-shard writes.

### PR 5a — Retained logical change stream

#### Goal

Create the load-bearing catch-up primitive missing from the repository.

This is separate from the recovery journal.

#### Core contract

```go
type CommitSequence uint64

type ChangePosition struct {
    Shard ShardID
    Epoch OwnershipEpoch
    Seq   CommitSequence
}

type ChangeID struct {
    Shard    ShardID
    Epoch    OwnershipEpoch
    Seq      CommitSequence
    Ordinal  uint32
}
```

A committed logical batch receives a stable sequence. Positions remain valid
across checkpoint, repack, process restart, and recovery-journal recycling.

#### Deliverables

- segmented retained logical change storage;
- stable monotonic per-shard positions;
- transaction/batch commit boundaries;
- idempotent mutation IDs;
- export from an arbitrary retained position;
- consumer leases/watermarks;
- minimum time/byte retention;
- hard retention limits;
- explicit backpressure or migration abort before required entries are dropped;
- crash recovery and corruption detection;
- schema/version information sufficient for deterministic apply;
- local root/snapshot metadata recording the corresponding change position;
- backup/export API using `ChangePosition`.

When change export is enabled for a shard, an acknowledged commit must not become
unexportable. The implementation must document the exact durability ordering
between local state and retained change records.

#### Mandatory failure tests

- crash before/after local publication;
- crash before/after stream durability;
- segment rotation;
- checkpoint/repack;
- slow consumer;
- hard-retention pressure;
- expired position;
- duplicate apply;
- process restart and resume;
- corruption/truncation.

#### Non-goals

No shard split, gateway cutover, follower election, replica reads, or session
tokens.

### PR 5 — Online split and movement

#### Goal

Consume PR 5a to perform non-uniform online split/move safely.

#### Deliverables

- target allocation and ownership epochs;
- source snapshot at `ChangePosition`;
- retained-tail catch-up;
- idempotent target apply;
- row/index/root/count/checksum verification;
- source write fencing;
- final drain;
- atomic catalog/manifests/epoch publication;
- rollback window;
- stale routing-generation and ownership-epoch handling;
- safe source retirement.

#### Mandatory failure matrix

Inject failure during every step: snapshot, catch-up, verification, fencing,
final drain, publication, rollback, and retirement.

No failure may permit two unfenced writers, acknowledged data loss, or an active
overlapping manifest.

### PR 6 — Optional replication and replica reads

#### Goal

Add one qualified replication protocol. Raft is a candidate, not a prerequisite.

The protocol may consume or subsume PR 5a's logical stream, but it must preserve
the same stable commit-position/export guarantees. It must never depend on the
recovery journal as a retained history.

#### Deliverables

- replicated durable commit/apply contract;
- election/failover and stale-writer fencing if claimed;
- replica applied positions;
- opaque session tokens;
- leader token validation/translation across routing changes;
- strong/session/stale reads;
- explicit fallback;
- no silent consistency downgrade.

### PR 7 — 100+ TB operations and qualification

#### Goal

Automate shard envelopes, split pressure, schema, backup/restore, and scale
qualification.

#### Deliverables

- `MaxMaintenanceDebtBytes` and other unit-explicit telemetry;
- hot-range and oversized-shard detection;
- non-uniform split planner;
- node drain/movement orchestration;
- schema rollout/rollback;
- backup catalog and restore;
- alternative-key lookup strategy;
- continuous scale-model suite;
- full 100+ TB release-qualification workflow.

#### Qualification tiers

Continuous tests use thousands of small shards to prove state-machine and
orchestration invariants.

The full 100+ TB generated/imported table is a release qualification, with
documented cost, duration, recovery bounds, and hardware.

### Commit discipline

Within each PR:

1. tests/contracts first where practical;
2. core implementation separately;
3. integration separately;
4. benchmarks/docs last;
5. no unrelated cleanup;
6. preserve bisectability;
7. record benchmark before/after data;
8. list every design deviation.

### Final report for every PR

Every PR report includes:

- changed files;
- invariants implemented;
- exact validation commands;
- fuzz/race results;
- benchmarks and allocations;
- unresolved risks;
- design deviations;
- next PR boundary;
- confirmation that forbidden scope and golden-vector rewriting did not occur.

## Rejected alternatives

### Embed VTGate

Rejected. It imports a MySQL-oriented planner, executor, topology, tablet,
session, and transaction model rather than a small mapping contract.

### Implement VTTablet first

Rejected. It is an interoperability project, not a shortcut to native sharding
or replication.

### Require Raft for sharding

Rejected. It unnecessarily couples capacity scaling to one availability
protocol.

### Add Vitess to the root module

Rejected. Embedded users must not pay dependency, build, binary, or maintenance
costs for unused distributed behavior.

### Use tuple bytes as every Vindex input

Rejected. Vitess mappings have independent scalar/multi-column contracts;
`multicol` maps columns separately.

### Represent only point destinations

Rejected. Multi-column prefixes and future range mappings require ranges in the
first stable interface.

### Route only at plan time

Rejected. Parameters and join-fed memberships may become available later.

### Hash textual concatenation

Rejected. It violates typed equality and creates wrong-shard risks.

### Permit implicit stale replica reads

Rejected. Consistency changes require explicit policy.

### Permit cross-shard writes in version 1

Rejected. Partial publication and recovery semantics require a separate design.

### Treat a targeted all-shard set as non-scatter

Rejected. Physical fan-out governs capacity, admission, and metrics.

## Review resolution

This design incorporates the following review findings:

- Invalid `hash` over string/multi-column VSchema example:
  replaced with single-column `xxhash` and a pinned `multicol` example.
- Tuple encoding incorrectly centered in Vitess mapping:
  limited to placement identity, immutability, and native Vibedb mappers.
- Point-only destinations:
  replaced with `DestinationSet` of keyspace ranges and prefix routing.
- Numeric spelling wrong-shard risk:
  requires the same exact canonical number decomposition as Vibedb equality.
- `internal/orderedkey` evolution risk:
  requires a frozen placement-specific v1 snapshot/fork.
- Numeric-cast Vindex ambiguity:
  specifies lossless admitted integer conversion only.
- String collation ambiguity:
  fixes v1 equality/placement to binary UTF-8.
- `uint64`/Vitess byte-string mismatch:
  defines fixed `[8]byte` points and an explicit maximum end sentinel.
- Nonexistent table/column IDs:
  uses canonical string table/column paths and rejects nested/Any/JSON keys.
- Plan-time-only extraction:
  introduces bindable constraint programs and bound domains.
- Limit behavior:
  defines typed errors or explicitly permitted scatter fallback.
- All-shard targeted routes:
  classifies them as scatter.
- `RequireTargeted` had no owner:
  moves it into `RoutePolicy`.
- Equal/In intersection and duplicate behavior:
  defines exact typed intersection and dedup before expansion.
- Insert routing gap:
  adds direct shard-key routing for post-coercion rows.
- Multi-row cross-shard inserts:
  rejects them before dispatch.
- Shard-key update ambiguity:
  statically rejects every assignment to a shard-key column.
- Naked colocation ID:
  replaces it with named immutable distribution groups.
- Undefined endpoint:
  defines opaque `EndpointRef`.
- Session token/version behavior:
  makes tokens opaque and sends mismatches to leader validation when configured.
- Safe policy zero values:
  explicitly freezes `ReadStrong + FallbackLeader`.
- Raft coupling:
  removes Raft from routing and capacity requirements.
- 100+ TB logical tables/databases:
  adds per-shard physical partitioning, non-uniform online splits, shard
  envelopes, stateless gateways, bounded scatter, lookup strategy, schema
  orchestration, backup/restore, and scale qualification.

A second, agent-readiness-focused review pass produced:

- PR 5a for a retained logical change stream, with the recovery journal
  explicitly excluded;
- the split of PR 1 into 1a frozen bytes, 1b keyspace/manifests, and 1c routing;
- the pre-resolved v1 scalar set of String + exact Number and fixed `-0 == 0`;
- explicit points alongside ranges;
- ordinal hot-path domains instead of string maps;
- one admission enum and truth table instead of separate scatter/overflow
  policy;
- byte-hashing mapper serialization rules;
- the unique-key-must-cover-shard-key invariant;
- static PR 2 metadata and a one-shard local cluster facade;
- the split of distributed execution into PR 4a shard service and PR 4b
  gateway/catalog;
- the SQL-text-plus-typed-parameters wire format;
- ownership epochs before online movement;
- routing-generation/ownership-epoch handling in place of a migration token;
- repository-standard vet/test/race commands and expanded reconnaissance;
- immutable golden-vector guardrails;
- tiered continuous scale-model and full 100+ TB release qualification;
- typed shard/endpoint IDs and unit-explicit maintenance debt;
- the required amendments to the parent distributed-sharding design.

## Acceptance criteria

This design is accepted when reviewers agree that:

1. placement scalar/tuple bytes are frozen and reviewed separately in PR 1a;
2. version 1 admits only binary UTF-8 strings and exact numbers;
3. negative zero and spelling-equivalent numbers follow Vibedb equality;
4. compatible mappers pin their own input serialization;
5. destinations contain explicit points and ranges;
6. fixed 8-byte keyspace geometry and maximum bounds are unambiguous;
7. manifests and ownership epochs are typed and immutable per generation;
8. hot-path constraints are ordinal domains, not string-keyed maps;
9. admission has one documented enum and truth table;
10. every unique key contains every shard-key column;
11. PR 2 has an end-to-end one-shard local cluster vehicle;
12. SQL text plus typed parameters is the initial shard wire format;
13. a shard service exists before a gateway assumes it;
14. catalog persistence appears explicitly in PR 4b;
15. the recovery journal is never used as a retained catch-up stream;
16. PR 5a lands a durable retained logical change stream before movement;
17. split/move uses ownership fencing and non-overlapping active manifests;
18. session tokens remain opaque and distinct from migration state;
19. Raft is optional and parent-document amendments land together;
20. continuous scale-model and full 100+ TB release gates are distinct;
21. golden vectors cannot be edited to hide implementation failures;
22. every PR runs repository-standard vet/test/race commands;
23. no Vitess dependency enters the root module.

## Repository placement

This document lives at `docs/design/vitess-compatible-routing.md`. The
required amendments above are folded into `docs/design/distributed-sharding.md`
directly. A paste-ready execution prompt for PR 1a lives at
`docs/design/pr1a-execution-prompt.md`.

Implementation begins with **PR 1a — Frozen placement scalars and tuple codec
v1** and proceeds through the PR ladder above.
