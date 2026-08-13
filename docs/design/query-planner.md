# Query planner

**Status:** the reusable optimizer core, compact catalog statistics, distributed
route planning, gather/merge-gather, and algebraic global finalization are
implemented. Repartition exchanges, broadcast joins, grouped finalization, and
adaptive re-optimization remain execution work; the planner does not advertise
them as executable.

## Objective

VibeDB needs one planner architecture that scales from an embedded scan to a
distributed query without coupling SQL syntax, catalog representation, storage
indexes, and network execution into one switch statement. It must remain:

- correct without statistics and conservative with stale statistics;
- bounded in compile time and retained memory under hostile SQL;
- deterministic for the same SQL, parameters, catalog generation, statistics,
  limits, and objective;
- explicit about ordering, distribution, memory, and network movement;
- extensible by adding an operator, rule, metadata record, or cost function
  without modifying the memo search engine; and
- observable enough to benchmark planning work and space independently from
  execution.

The design uses a Cascades-style memo and top-down optimization by required
physical properties. That is an architectural choice, not a performance claim.
Claims that it beats another system require workload-matched benchmarks with
planning time, execution time, peak memory, network bytes, and plan quality.

## Pipeline

```text
SQL parser and binder
        │
        ▼
logical expressions + immutable catalog generation
        │
        ▼
memo equivalence groups
  ├─ exploration rules (equivalent relational forms)
  └─ implementation rules (physical alternatives)
        │
        ▼
top-down search by (memo group, required physical properties)
  ├─ statistics and uncertainty
  ├─ multidimensional cost
  ├─ hard memory/search limits
  └─ property enforcers (sort/exchange)
        │
        ▼
immutable physical plan + fingerprint + planning statistics
        │
        ▼
gateway fragments / local executor
```

`planner` owns the generic search machinery. `gateway` owns shard semantics,
routing, executable distributed operators, and their cost model. The local
`query` package retains its specialized vectorized compiler and adaptive index
execution; moving local alternatives into the memo later does not require a
second optimizer framework.

## Memo and rules

A memo group denotes one relational result. Its expressions are alternatives
with the same logical properties; child edges point to groups, not fixed child
plans. Duplicate expressions are suppressed. A group can therefore cache one
winner for singleton ordered output and another for hash-partitioned output.

Rules are ordinary objects with a stable name, phase, priority, match, and
apply function. Exploration and implementation are separate phases. Rules may
add an equivalent expression or create a child group, but all mutation passes
through memo limits. Registration order cannot change the winner: rules are
scheduled by phase, descending priority, and name, and equal-cost plans use a
stable 128-bit structural digest with exact structural comparison as the
collision fallback. Rule exploration is atomic: failure or cancellation rolls
back every yielded group/expression and counter. Success seals the memo against
mutation. The same optimizer can then search that immutable memo for multiple
root property requirements without replaying rules.

The initial gateway rules implement a remote query and an algebraic final
aggregate. The operator vocabulary already reserves table/index scans, filters,
projects, hash/merge/nested-loop joins, partial/final aggregates, sort/top-k,
remote execution, gather, ordered merge-gather, hash repartition, and broadcast.
An operator being representable does not make it executable.

## Physical properties

Every physical plan provides, and every consumer may require:

- distribution: any, singleton, random, hash(keys), range(keys), or replicated;
- partition count, with zero as a consumer wildcard; and
- an ordered column prefix with direction and explicit null placement.

Ordering is local to each partition unless the distribution is singleton.
This distinction prevents the classic distributed-planner bug of treating
individually sorted shard streams as a globally sorted result. A locally
ordered fan-out is converted to global order by `MergeGather`; an unordered
fan-out uses `Gather`; a missing required order after gather needs `Sort`.

Hash and range keys are ordered identities, not display strings. A hash on
`(tenant, time)` does not satisfy a hash requirement on `(time, tenant)`.

## Cost and bounded search

Cost is a vector:

```text
startup, CPU, IO bytes/work, network bytes/work, peak memory
```

CPU, IO, network, and startup compose additively. Peak memory composes with
`max`, avoiding the common error of summing memory that is not simultaneously
live. A workload objective supplies weights and a hard memory limit. The
gateway defaults make network movement more expensive than IO and IO more
expensive than CPU; a future workload class can replace those weights without
changing rules.

Search has independent hard limits for groups, expressions, rule applications,
physical alternatives, property states, enforcer-chain steps, recursion depth,
deterministic memo payload bytes, and deterministic top-down search payload
bytes. The byte limits cover memo records, child edges, column identities,
unique-key runs, cached property records, property slices, physical plan nodes,
and their owned slices, so one wide expression or enforcer chain cannot bypass
the cardinality limits. Go map buckets, model-owned returned alternatives, and
allocator slack remain indirectly bounded by the count limits. Cancellation is
checked during exploration and top-down search. Exceeding a limit returns a
typed error; the optimizer never silently publishes the best plan observed
before truncation. Positive and negative property results are memoized, so an
impossible child requirement is not recomputed for every parent alternative.

`OptimizerStatistics` reports memo groups, expressions, rule applications,
accounted payload bytes, owned slice-capacity bytes, physical alternatives,
property states/cache entries/cache hits, enforcer alternatives/steps, plan
nodes, deterministic search payload bytes, memory rejections, and peak search
depth. It deliberately excludes a clock; benchmarks time `Optimize` at the
call boundary.

## Statistics

Statistics are immutable and pinned to the same catalog generation as routing.
They may influence cost only. Routing constraints, ownership fences,
colocation proofs, SQL semantics, and resource admission never trust them.

The compact representation is sparse and flat:

| Record | Retained shape |
| --- | --- |
| table | 64 bytes: interned name, uncertain row count/row width, column run |
| observed column | 64 bytes: path, NDV uncertainty, null rate, average width, skew/histogram runs |
| most-common value | 16 bytes in a shared flat run |
| histogram bucket | 24 bytes in a shared flat run |
| optional physical partition | 40 bytes: table/shard identity and uncertain row count |
| strings/scalars | one interned byte arena |

Tables and columns are sorted and use allocation-free binary search. Heavy
hitters avoid the uniformity error for skewed equality predicates. Sparse
equi-depth cumulative histograms support later range selectivity without a
slice header or allocation per column. Estimates carry central value, lower and
upper bounds, and confidence in the hot compact form; lower bound and confidence
use float32 slots so both 64-byte directory shapes remain unchanged. The
distributed cost model uses the upper row and width bounds. Missing table
statistics use conservative defaults. JSON scalar statistics are canonicalized
at publication, including exact numeric spelling variants, so `5`, `5.0`, and
`50e-1` share one skew key.

Optional per-shard row estimates prevent a targeted route from assuming rows
are uniformly distributed. When every selected shard has an estimate, their
upper bounds are summed; otherwise costing falls back to the whole-table upper
bound. Shard-key equality and `IN` predicates consume heavy hitters and NDV
intervals. Multiple key predicates use exponential backoff: the strongest is
applied fully and each additional selectivity receives a successively smaller
exponent, avoiding an unjustified full-independence assumption.

Catalog save/load persists the cold statistics descriptors. Runtime feedback
must be collected separately, validated, and published as a newer immutable
generation. Mutating statistics under a cached plan would make planning
non-reproducible and is not allowed.

### Measurement baseline

The benchmarks live beside the implementation and report allocations. A local
Go 1.26/Apple M4 Max baseline (not a cross-system performance claim) is:

| Benchmark | Time | Query-path allocation | Retained/search space |
| --- | ---: | ---: | ---: |
| table + column + heavy-hitter lookup, one table | about 14 ns | 0 | — |
| same lookup in a 1,024-table catalog | about 36 ns | 0 | 154 bytes/table for one observed column and one heavy hitter |
| per-shard lookup in a 1,024-partition catalog | about 66 ns | 0 | 50 bytes/partition |
| fresh memo/rules/property search, two physical alternatives | about 1.2 µs | 3,816 bytes / 31 allocations including construction | 352 owned memo bytes |
| build and validate that 1,024-table catalog | about 1.2 ms | 5.77 MB / 25,644 allocations on the cold publication path | compact result measured separately |

Run `go test ./planner -run '^$' -bench . -benchmem` on the target hardware.
The scaled catalog benchmark separates the generation-publication build cost
from the allocation-free query hot path.

## Distributed execution currently proved

One attempt pins one routing/statistics generation. The shared constraint
compiler derives shard-key domains, and the distribution router returns empty,
single, targeted-subset, or scatter routes under workload admission limits.

The following multi-shard shapes are executable now:

- projection/filter reads with bounded fan-out and fail-closed partial-result
  policy;
- colocated inner and left joins whose equality proves every shard-key ordinal;
- unordered `Gather` and ordered k-way `MergeGather`, including global limit;
- exact global `COUNT`, `SUM`, `MIN`, and `MAX` over shard-local aggregate
  states; and
- empty-route aggregate identities: zero for `COUNT`, SQL null for the others.

`SUM` finalization uses exact rational arithmetic and emits a finite exact JSON
decimal under the aggregate byte cap. `MIN`/`MAX` compare exact numbers and
preserve a contributing spelling. Every shard must return exactly one row with
the same schema; malformed states fail closed.

The following still refuse a multi-shard route:

- `AVG`, until shard fragments project both `SUM` and contributing count;
- grouped aggregates, `HAVING`, distinct, windows, and offset;
- derived/CTE/predicate-subquery plans that read physical data elsewhere;
- non-colocated, cross-distribution, right, and full joins; and
- distributed writes.

The next execution milestone is fragment SQL/IR plus streaming exchange. Once
that exists, the existing memo can cost colocated, broadcast, and repartition
join alternatives and two-phase grouped aggregation without changing memo,
property, statistics, or cost APIs.

## Sources

The memo, rules, enforcers, and required-property design follows Goetz Graefe's
[Cascades framework](https://15721.courses.cs.cmu.edu/spring2019/papers/22-optimizer1/graefe-ieee1995.pdf).
CockroachDB's architecture is a useful production reference for separating
[logical optimization from topology-aware physical planning](https://www.cockroachlabs.com/docs/stable/architecture/sql-layer.html)
and for mapping processors from data locality outward in its
[physical planning design](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#physical-planning).
Apache Calcite documents the extensible metadata categories—row count,
selectivity, distinct counts, distribution, collation, and size—that informed
the statistics/property boundary in its
[adapter and metadata model](https://calcite.apache.org/docs/adapter).
The DataFusion SIGMOD paper is a current reference for preserving multiple sort
orders and using two-phase partitioned aggregation in a vectorized engine:
[Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion/files/14586286/DataFusion_Query_Engine___SIGMOD_2024.8.pdf).
