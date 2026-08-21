# Query planner

VibeDB has two complementary planning paths:

- `query` compiles the embedded SQL/native execution surface into bounded local
  operators; and
- `planner` provides a reusable Cascades-style memo, rules, physical
  properties, statistics, and multidimensional cost model used by the
  distributed gateway.

This document describes what those planner primitives and the gateway execute
now. Representable operators are not automatically executable operators.

## Memo and rules

A memo group represents one relational result and stores equivalent logical or
physical expressions. Child edges name groups. Exact comparison, not hashes,
decides equality. Exploration and implementation rules have stable phase,
priority, and name ordering; equal-cost winners use deterministic structural
digests with exact collision fallback.

Rule application is atomic: cancellation or a limit error rolls back the
entire yielded change. Successful exploration seals the memo, after which the
same immutable memo can be searched for multiple root property requirements.

## Physical properties

Plans provide and consume:

- distribution: any, singleton, random, hash, range, or replicated;
- a partition count, with zero as a consumer wildcard; and
- an ordered column prefix with direction and explicit null placement.

Ordering is local to a partition unless the distribution is singleton. Sorted
shard streams therefore require `MergeGather` for global order; unordered
streams use `Gather`; an unmet singleton order requires a sort.

## Cost and limits

Cost is a vector of startup, CPU, IO, network, and peak memory. Sequential peak
memory composes with `max`; a physical operator can provide a custom composer
for pipelined or concurrent children. Overflow and non-finite costs fail
closed.

Independent limits bound groups, expressions, rule applications, physical
alternatives, property states, enforcer steps, recursion depth, memo payload,
search payload, plan nodes, and memory. Cancellation is checked during
exploration and search. Hitting a bound returns a typed error; the optimizer
never publishes a partial best-so-far plan.

`OptimizerStatistics` reports memo/search counts and accounted bytes but no
clock. Benchmarks time the call boundary.

## Statistics

Statistics are immutable and pinned to the same catalog generation as routing.
They affect cost only; routing, ownership, SQL semantics, colocation, and
resource admission never trust them.

The compact catalog stores uncertain table row/width estimates, sparse column
NDV/null/width data, heavy hitters, histograms, and optional per-shard row
estimates in flat sorted runs and an interned scalar arena. Query lookup is
allocation-free. Canonical JSON scalar comparison makes numeric spelling
variants share one statistical identity. Missing or stale data produces
conservative estimates, never a semantic shortcut.

Statistics publication and request-bound construction use byte-native append
APIs. `vibejson` validates and streams decoded JSON strings directly into the
canonical arena, while exact numbers normalize from borrowed spellings without
an intermediate mantissa or Go string. String-returning helpers remain cold
compatibility boundaries rather than distributed request-path dependencies.

## Current distributed execution

One gateway attempt pins one routing/statistics generation. The constraint
compiler derives placement-key domains and produces empty, single, targeted,
or scatter routes under admission limits.

Executable multi-shard shapes are:

- projection/filter reads with bounded fan-out and fail-closed partial-result
  behavior;
- colocated `INNER` and `LEFT` joins whose equality covers every placement-key
  ordinal;
- unordered `Gather` and ordered k-way `MergeGather`, including global
  `LIMIT`; and
- exact global and grouped `COUNT`, `SUM`, `MIN`, and `MAX` finalization, plus
  path-projection `DISTINCT` through the same exact grouped state.

Grouped execution sends the authored `GROUP BY` to every shard as the partial
stage. On a multi-shard route an additive request marker asks the shard runtime
to parse the original SQL once, then remove final-only `ORDER BY`, `OFFSET`, and `LIMIT`
nodes from its owned AST before lowering. The wire still carries authored SQL
and typed parameters, never a serialized plan or a second rewritten SQL string.
This guarantees that a local LIMIT cannot discard a partial group needed by
another shard. The gateway interns returned key tuples using the query engine's
exact group-key encoding and finalizes dense columnar accumulator lanes. Small
integer counts and sums stay in native registers, promote to `big.Int` only on
overflow, and enter rational mode only for a real decimal. Retained state and
completed output share the operation's finite aggregate byte admission. Group
order is stable first appearance when unordered. Grouped `ORDER BY` adds a
bounded exact final sort. `ORDER BY ... OFFSET M LIMIT K` uses an exact O(M+K) max-heap
after final aggregation instead of sorting every group, including when one
group identity spans shards. The physical plan exposes these as `sort` and
`top-k` above `final-aggregate`. Every distributed group key must still be
present in the SELECT output so the final stage receives the complete identity;
a single-shard route retains the local engine's broader projection surface.

Inside each shard, primary-key inequalities and `BETWEEN` bind to canonical
ordered-key bounds and seek the durable primary graph before document decoding.
The query engine retains its compiled predicate as the correctness authority,
then applies filter-first extraction and late projection over only the visited
span. Single-column secondary inequalities binary-search the same persisted,
prefix-compressed ordered term leaves used by equality indexes, union their
stable-slot postings under the pinned overlay generation, and intersect those
masks with other exact candidates before row decoding. Broad term spans decline
under a fixed latency bound. Conjunctive lower and upper bounds over one path
become one physical term span, so `BETWEEN` does not materialize two broad
posting unions merely to intersect them. Covering aggregates remain a separate
exact path; no secondary values or range metadata are duplicated into another
format.

For declared `durable.Options.SkipIndexes`, conjunctive immutable scalar
`=`, `<`, `<=`, `>`, and `>=` comparisons compile into canonical ordered byte
bounds over catalog ordinals. A warmed scan compares those bounds with compact
per-primary-stripe min/max terms without path strings or JSON parsing and
advances rejected leaves before key/value decoding. Exact candidate masks and
native primary ranges take precedence; overlays decline the optimization. The
complete compiled predicate still rechecks every row in retained stripes, so
missing/invalid summaries affect work only, never results. Query statistics
report both skipped stripes and their logical row count.

`SUM` uses exact rational arithmetic and emits a finite canonical JSON decimal.
`MIN` and `MAX` compare exact values and preserve a contributing spelling.
Every shard aggregate must return exactly one row with the expected schema.
Non-integral `MergeGather` keys and numeric `MIN`/`MAX` states share the query
engine's exact arbitrary-exponent byte comparator; the merge heap performs no
number canonicalization, float conversion, or allocation.

The gateway refuses:

- `AVG`, grouped `HAVING`, computed/window DISTINCT, windows, and OFFSET on a
  non-grouped multi-shard route;
- derived, CTE, or predicate-subquery plans that read another physical source;
- non-colocated, cross-distribution, `RIGHT`, and `FULL` joins; and
- unsupported single-statement scatter writes. Explicit bounded write batches
  may partition into a fixed participant set and commit through the durable
  coordinator protocol; the one-shard `Exec` lane remains the lower-latency
  path.

The generic memo includes additional operator identities for testing and
composition, but the gateway must reject any plan whose execution
kernel is absent. See [Distributed server boundary](distributed-sharding.md)
for the operator-facing network and HA limits.
