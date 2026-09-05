# Distributed optimizer and statistics

[Documentation](README.md) / [Design](design/README.md) · [Development status](status.md)

The gateway uses distribution locality, index costs, and published statistics
to reduce data movement and coordination. Runtime bounds and exact predicates
remain authoritative: a cost estimate cannot prove that a row may be omitted.

The implementation notes and measurements below record work that began from
`09e60689`. For the overall execution model, see
[query execution](design/query-execution.md).

## Planning and execution

- **Preserve grouping locality.** A single-table GROUP BY containing every
  distribution-key path produces disjoint groups on the source shards. The
  planner omits final aggregation and repartition; the executor gathers owned
  finalized cells in contiguous batch arenas, then applies the global ordering, offset, and limit. This
  avoids rebuilding a coordinator hash table. Nonlocal groups still combine.
- **Choose global indexes by estimated work.** Eligible indexes compete using
  probe count, skew-aware candidate cardinality, locator traffic, and base-row
  width. Uniqueness bounds result cardinality but does not automatically win
  against a shorter, selective lookup. A complete base shard-key point keeps
  the existing direct route. This change ranks global indexes; it does not
  compare every global-index alternative with a full base-table scan.
- **Keep unrelated updates local.** SET-target dependency checks exclude global
  indexes whose key and locator paths cannot change. Parent paths and escaped
  JSON-pointer tokens are handled explicitly; whole-document replacement and
  DELETE retain maintenance. Computed expressions are classified by their write
  targets, not their read dependencies. Affected indexes reuse one structural
  parse of each captured before/after document.
- **Cost partial and final groups separately.** Local shard NDVs estimate partial
  traffic; global union NDV estimates final output. Reducer memory includes
  accumulator overhead and a probabilistic busiest-partition allowance instead
  of assuming perfect balance. ORDER BY is not advertised by unordered partial
  fragments. Global Top-K is costed after gathering local groups.
- **Honor execution capabilities and budgets.** RF3 catalog targets do not receive
  legacy repartition plans. `Profile.MaxWorkerAggregateBytes` bounds each reducer
  separately and defaults to the coordinator aggregate byte budget. Producer and
  mailbox caps remain bounded by the existing exchange profile. Runtime caps
  remain authoritative if estimates or exact-arithmetic state sizes grow. The
  memo still uses the coordinator budget as its overall memory ceiling, so
  raising the worker cap above that ceiling does not widen plan admission.

RF3 SQL now waits in a bounded admission queue before acquiring a ReadIndex cut
when execution reservations are full. The existing 112 MiB shared budget is
unchanged; at most 16 SQL requests wait with only their charged request frames.
Cancellation releases waiting slots. Eligible transaction snapshots also use
compiled primary-key range bounds; staged-write overlays and filtered split
ownership retain their required source paths.

## Go 1.27 and SIMD

Go 1.27 is the minimum version already declared by this repository. Build with
`GOEXPERIMENT=simd` to enable the new blocked Bloom insertion kernels: NEON on
arm64 and AVX2 on amd64, with an AVX2 feature guard and scalar fallback. Plain
builds and other architectures use the fused scalar implementation. The existing
Linux amd64/arm64 CI matrix now runs the planner, query, and gateway suites with
SIMD enabled as well.

Insertion computes all eight signature words in registers. Probes instead fuse
bit generation with scalar early exits. An all-vector probe and a first-word
hybrid were measured, but both lost to the fused scalar loop on absent keys;
those implementations are not dispatched. SIMD is retained where it improves
the measured operation. Overflow, every word, empty/full blocks, random hashes,
and missing signature bits are checked against the original scalar signature.

## Statistics collection and publication

`store.Snapshot.Analyze` and `durable.Snapshot.Analyze` collect from pinned source
snapshots. `planner.AnalyzePartition` accepts scalar row iterators;
`planner.AnalyzeDocuments` accepts JSON document iterators. Source errors,
invalid scalar data, exhausted sample byte budgets, and cancellation refuse a
partial publication.

Each partition retains a bounded bottom-k row sample and KMV distinct sketches.
`planner.MergePartitionStatistics` unions distinct samples and merges row
samples by global priority, avoiding double-counted NDVs and equal weighting of
unequal shards. Sketch unions use a sequential merge with recycled scratch;
full sketches reject hashes above their threshold before binary search. Duplicate partition IDs and incompatible generations, schemas,
or bounds are refused. Cardinality intervals are approximate statistical
intervals; their confidence labels are not deterministic bounds on execution.
Hash collisions are probabilistic. Collection is an explicit maintenance API;
it is not an automatic scheduler or a new SQL ANALYZE command.

`PartitionAnalysis.MarshalBinary` and `UnmarshalPartitionAnalysis` now preserve
unionable sketches and priority samples in a versioned, checksummed format.
Decoding bounds both wire size and expanded retained state, including repeated
group-path references. `MatchesDefinition` fences shard, generation, schema,
and collection bounds. RPC dispatch, collection scheduling, and atomic catalog
publication of a complete remote scan still need integration.

The resulting `TableStatistics` descriptors can be supplied to the existing
catalog publication APIs. `Groups` adds joint NDV and tuple most-common-value
frequencies with uncertainty. Partition groups preserve local NDVs. Both survive
catalog save/load. Catalogs without groups retain their existing behavior.
Group lookups binary-search the table/partition directory and allocate nothing.

The gateway compiles statistical equality/IN constraints independently of shard
routing, so published non-routing columns also affect costs. It uses disjoint
joint-statistics groups and applies exponential backoff between groups and
remaining scalar predicates. Unknown information retains conservative fallbacks.
Range predicate costing, learned estimators, join-crossing correlations, and
stats-driven physical placement are not implemented here.

## Research basis

- [Trino cost-based optimization](https://trino.io/docs/current/optimizer/cost-based-optimizations.html)
  treats join ordering, build-side selection, distribution, and network work as
  connected decisions. This is the broader distributed optimizer benchmark;
  broadcast/shuffle join enumeration remains a gap in VibeDB's executable plans.
- [CockroachDB's optimizer](https://www.cockroachlabs.com/docs/stable/cost-based-optimizer)
  connects table statistics, plan caching, and locality-aware access selection.
- [PostgreSQL multivariate statistics](https://www.postgresql.org/docs/18/multivariate-statistics-examples.html)
  motivates joint NDV and tuple MCVs for correlated columns.
- [Apache DataSketches' Theta framework](https://datasketches.apache.org/docs/Theta/ThetaSketches.html)
  motivates unionable distinct-value sketches instead of summing shard counts.
- [DuckDB's optimizer overview](https://duckdb.org/docs/lts/internals/overview)
  provides the baseline for predicate pushdown and dynamic-programming join
  ordering. This change uses VibeDB's existing memo infrastructure; it does not
  claim to add executable general distributed join reordering.

## Reproducible checks

```sh
go test -timeout=30m ./planner ./gateway ./query ./store ./store/durable
go test -race ./planner ./gateway -run 'TestJoint|TestAnalyze|TestE2ELocalGroups|TestE2EPlannerSelected|TestUpdateIndexDependencies|TestGlobalIndexSelection'
go test ./gateway -run '^$' -bench 'BenchmarkGroupedLocalityFinalization|BenchmarkMultiIndexDocumentRouting|BenchmarkCostedGlobalIndexBind' -benchmem -count=5
go test ./planner -run '^$' -bench 'BenchmarkAnalyzePartition|BenchmarkDistinctSketchUnion' -benchmem -count=5
GOEXPERIMENT=simd go test ./query -run TestJoinBloom -bench BenchmarkJoinBloomKernel -benchmem -count=5
```

The paired kernel benchmarks compare old and new work on identical inputs. They
are not end-to-end distributed throughput measurements and do not establish
superiority over Trino, CockroachDB, TiDB, or ClickHouse. An external competitor
comparison requires equivalent datasets, routing/locality, consistency,
durability, hardware, client concurrency, and independently checked results.
The local Docker daemon was unavailable during this run.

## Recorded validation

During the recorded implementation, the planner, gateway, query, in-memory
store, and durable store suites passed on Go 1.27 / darwin-arm64. The durable suite needed a 30-minute timeout
and completed in 798 seconds. The planner, gateway, query, and in-memory store
suites also passed with SIMD enabled; focused race tests cover statistics,
local grouping, index selection/update dependencies, and Bloom correctness.
`go vet` passed for planner/gateway/query, and actionlint passed for the CI edit.

The amd64 SIMD differential test passed under Rosetta with AVX2 enabled and
with `GODEBUG=cpu.avx2=off`; these are correctness checks, not native amd64
performance claims. Linux amd64 and arm64 SIMD query test binaries cross-compile.
No remote cluster or competitor throughput result is claimed by these checks.

## Measured kernel results

Five repetitions at 500 ms per case on Apple M4 Max / Go 1.27.0; the table
uses medians. [Raw output](benchmarks/distributed-planner-go127.txt) includes
allocation counts and both plain/SIMD builds.

| Operation | Reference | New | Speedup |
| --- | ---: | ---: | ---: |
| Finalize 10,000 disjoint groups | 2.386 ms | 0.354 ms | 6.7× |
| 16 index routes on one 8 KiB document | 57.564 µs | 8.935 µs | 6.4× |
| Union two 2048-entry distinct sketches | 153.717 µs | 5.610 µs | 27.4× |
| Bloom miss probe | 8.789 ns | 1.751 ns | 5.0× |
| Bloom hit probe | 9.997 ns | 5.049 ns | 2.0× |
| Bloom insertion (NEON) | 8.503 ns | 2.760 ns | 3.1× |

Group finalization allocates 1.67 MB in 23 allocations versus 7.30 MB in 119
allocations. Sketch union and Bloom operations allocate zero bytes in these
kernels. The group reference is the existing coordinator combiner; the index
reference reparses per route; the sketch reference uses repeated sorted
insertion; the Bloom reference retains the original signature materialization.
These are improvements over those internal baselines on these inputs, not a
claim that every query improves by the same factor.
