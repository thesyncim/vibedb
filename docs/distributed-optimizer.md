# Distributed optimizer and statistics

[Documentation](README.md) / [Design](design/README.md) · [Stability](status.md)

The gateway planner uses shard locality, global-index cost ranking, and
published partition statistics to minimize cross-shard traffic and coordinator
work. Cost estimates never weaken correctness: runtime budgets and exact
predicates remain authoritative. See [query execution](design/query-execution.md)
for the execution pipeline.

## Planning and execution optimizations

- **Locality-preserving `GROUP BY`:** When a single-table `GROUP BY` covers
  every distribution-key path, shard groups are disjoint. The planner skips
  final re-aggregation and gathers finalized cells directly into contiguous
  arenas before global `ORDER BY`/`OFFSET`/`LIMIT`.
- **Zero-alloc distributed merge & top-K:** Non-local grouped aggregates merge
  partial shard aggregates with zero-allocation grouped merge, bounded top-K
  heaps, and exact JSON decimal comparison (`vibejson/x/jsoncmp` and
  `x/keyhash`).
- **Costed global-index selection:** Eligible global indexes are ranked by
  probe count, skew-aware cardinality, locator traffic, and base-row width
  (complete base shard-key points always keep direct routing).
- **Local update pruning:** `UPDATE` checks `SET`-target dependencies to skip
  global indexes whose key and locator paths cannot change; affected indexes
  share one structural parse of the before/after document.
- **Lock-free plan cache:** Physical plans are cached in a lock-free
  copy-on-write snapshot in the gateway.
- **Bounded admission:** RF3 SQL queues up to 16 requests behind the 112 MiB
  shared execution budget before acquiring a `ReadIndex` cut.
  `Profile.MaxWorkerAggregateBytes` bounds per-reducer memory under the
  coordinator ceiling.

## Statistics collection and publication

- `store.Snapshot.Analyze` and `durable.Snapshot.Analyze` collect per-partition
  bottom-k row samples and KMV distinct sketches (`planner.AnalyzePartition`,
  `planner.AnalyzeDocuments`).
- `planner.MergePartitionStatistics` unions KMV sketches and merges priority
  samples across shards without double-counting NDVs; `TableStatistics.Groups`
  records joint NDV and tuple MCVs for correlated columns.
- `PartitionAnalysis.MarshalBinary` / `UnmarshalPartitionAnalysis` encode
  versioned, checksummed, size-bounded statistics fenced by shard, generation,
  and schema (`MatchesDefinition`).
- The gateway compiles statistical equality/`IN` constraints with exponential
  backoff across disjoint column groups and conservative fallbacks on missing
  stats.

## Limitations

- Statistics collection is an explicit Go maintenance API (no automatic
  background scheduler or SQL `ANALYZE` command yet).
- Global-index ranking does not yet cost-compare every global index against a
  full base-table scan; general broadcast/shuffle join reordering and range
  selectivity costing remain future work.

## Verification

```sh
go test ./planner ./gateway ./query ./internal/distributedagg
go test -race ./planner ./gateway -run 'TestJoint|TestAnalyze|TestE2ELocalGroups|TestGlobalIndexSelection'
```

## Source map

- Planner & stats: [planner/](../planner/), [store/store_analyze.go](../store/store_analyze.go)
- Distributed merge & gateway: [internal/distributedagg/](../internal/distributedagg/), [gateway/](../gateway/)
- Blocked Bloom SIMD: [query/join_bloom.go](../query/join_bloom.go)
