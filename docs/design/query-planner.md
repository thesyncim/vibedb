# Query planner

VibeDB has a local query planner, a generic memo optimizer, and a gateway plan
layer. Their capability sets are not identical.

## Local access planning

Local execution can use:

- Primary-key point access
- Primary-key range access
- Exact-index or posting candidates
- Skip-index stripe pruning
- Adaptive join access
- Full scan

The full predicate remains authoritative. An index or summary only removes
impossible candidates.

Skip-index pruning applies only to durable collections configured with
`durable.Options.SkipIndexes`. SQL `CREATE INDEX` creates exact posting
indexes, not skip indexes.

`EXPLAIN` emits JSON without execution. `EXPLAIN ANALYZE` runs the query and
adds actual rows, scans, batches, spill, index, and join counters.

Access names use `or-scan` when runtime conditions can select a fallback.

## Generic memo optimizer

The `planner` package implements a Cascades-style memo search. Its logical and
physical vocabulary includes scans, filters, projects, joins, aggregates,
sorts, Top-K, limits, remote execution, gather, repartition, broadcast, and
partial or final aggregation.

Physical distributions include singleton, random, hash, range, and replicated.
Ordering records direction and null order.

The cost model separates startup, CPU, I/O, network, and memory costs.

Default optimizer bounds are:

| Resource | Maximum |
| --- | ---: |
| Memo groups | 4096 |
| Expressions | 32,768 |
| Rule applications | 131,072 |
| Plans | 65,536 |
| Property states | 65,536 |
| Enforcer steps | 65,536 |
| Depth | 1024 |
| Memo payload | 64 MiB |
| Search payload | 256 MiB |

This vocabulary is generic capability. It does not prove that the SQL gateway
can lower every SQL construct to every operator.

## Gateway plan cache

`gateway.Snapshot.Prepare` compiles and caches plans for one immutable catalog
generation. The cache has 1024 direct-mapped slots. SQL text longer than 4 KiB
still compiles but is not cached.

Gateway SELECT planning requires a physical driving table as its first `FROM`
source. Every physical table must exist in the pinned placement catalog.
FROM-less local queries do not have a gateway plan.

Routing uses SQL shard-key constraints and bound parameter values. A READY
global index can provide routing when every indexed key ordinal binds to a
finite, nonempty exact domain and the base shard key is not already a single
point. The gateway selects at most one eligible global index.

## Distributed joins

The gateway distributes a join only when every input has compatible placement
and affinity. The join must prove equality for every shard-key ordinal against
an earlier colocated input.

The gateway permits colocated inner and left joins. Right, full, and cross
joins need an all-shard relation plan and are refused. The current path also
refuses derived-table and CTE joins.

These are gateway restrictions. The local query runtime supports more join
forms.

## Route restrictions

The gateway refuses these shapes on every route, including one target:

- Set expressions
- CTEs
- Physical predicate subqueries
- Derived-table and CTE joins
- Unsupported join types
- Joins without a complete colocation proof

The gateway refuses these shapes only when a route has more than one target:

- Global `HAVING`
- Global windows
- Hidden ordering columns
- Unmergeable `DISTINCT`
- Unmergeable grouping
- Unsafe global offset or Top-K
- Unsupported aggregate shapes

Cross-shard ordering requires a projected ordering key. Hidden merge columns
are not supported.

Cross-shard combination supports direct projected `COUNT`, `SUM`, `MIN`, and
`MAX`. Aggregates inside scalar expressions are refused. Grouped combination
requires every grouping key in the projection. `AVG` is unsupported, and an
ungrouped combined aggregate cannot carry `LIMIT`.

`BoundPlan.ValidateRoute` checks these conditions after bind-time routing and
before dispatch.

## Gateway physical paths

The current gateway memo centers on:

- Remote base-table access
- Global-index access
- Optional final aggregation
- Gather or merge-gather
- Repartition for grouped aggregation

Catalog table, partition, and column statistics can inform cost. Missing join
correlation statistics use conservative expansion assumptions.

Fanout execution supports targeted and scatter routes, ordered merge, global
limit, aggregate combine, grouped partial aggregation, cancellation, and stale
generation retry.

## Write planning

An ordinary distributed mutation must have one provable owner. Cross-shard
insert, scatter update or delete, and shard-key-changing update fail before
dispatch.

The separate transaction-batch API can span shards and tables. An ordinary
multi-row SQL mutation does not automatically select that API.

## Implementation references

- `query/explain.go`, `exec.go`, and `file_execute.go`
- `planner/types.go`, `memo.go`, and `optimizer.go`
- `gateway/planner.go` and `optimizer.go`
- `gateway/global_index_read.go` and `merge.go`
