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
- exact global `COUNT`, `SUM`, `MIN`, and `MAX` finalization.

`SUM` uses exact rational arithmetic and emits a finite canonical JSON decimal.
`MIN` and `MAX` compare exact values and preserve a contributing spelling.
Every shard aggregate must return exactly one row with the expected schema.

The gateway refuses:

- `AVG`, grouped aggregates, `HAVING`, `DISTINCT`, windows, and `OFFSET` on a
  multi-shard route;
- derived, CTE, or predicate-subquery plans that read another physical source;
- non-colocated, cross-distribution, `RIGHT`, and `FULL` joins; and
- every distributed write.

The generic memo includes additional operator identities for testing and
composition, but the gateway must reject any plan whose execution
kernel is absent. See [Distributed server boundary](distributed-sharding.md)
for the operator-facing network and HA limits.
