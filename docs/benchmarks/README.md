# Benchmark reports

[Documentation](../README.md) / Benchmark reports

Measurements are grouped by experiment. Each report records its own revision,
workload, method, and limitations; read those before comparing numbers. Profiles,
failed precursors, and short qualification runs remain part of the record.

Start with [performance methodology](../performance.md). The
[competitive harness publication registry](../../bench/competitive/RESULTS.md)
is separate from this engineering archive.

## Methods

- [RF3 SQL comparison](crdb-sql-method.md).
- [Physical-node SQL comparison](fused-node-sql-method.md).

## Reports

| Date | Report |
| --- | --- |
| 2026-09-05 | [Distributed projected-range comparison](distributed-projected-ranges-2026-09-05/README.md) |
| 2026-09-05 | [Distributed integer GROUP BY comparison](distributed-integer-groups-2026-09-05/README.md) |
| 2026-09-05 | [RF3 Ready-series comparison](rf3-ready-series-2026-09-05/README.md) |
| 2026-09-05 | [Prepared indexed point and ordered-page reads](indexed-page-projection-2026-09-05/README.md) |
| 2026-09-05 | [Packed integer extrema final qualification](packed-extrema-simd-final-2026-09-05/README.md) |
| 2026-09-05 | [Packed integer extrema SIMD evidence](packed-extrema-simd-2026-09-05/README.md) |
| 2026-09-05 | [Packed SIMD layout regression follow-up](packed-extrema-simd-layout-fix-2026-09-05/README.md) |
| 2026-09-04 | [Wide packed equality count SIMD measurements](packed-count-simd-wide-2026-09-04/README.md) |
| 2026-09-04 | [Shared-node log: matched SQL comparison](crdb-sql-2026-09-04-node/README.md) |
| 2026-09-04 | [Retained pgwire semantic prepare](prepared-pgwire-reads-2026-09-04/README.md) |
| 2026-09-04 | [Retained JSON placement index comparison](vibejson-placement-2026-09-04/README.md) |
| 2026-09-04 | [RF3 SQL comparison with concurrent autocommit](crdb-sql-2026-09-04-concurrent/README.md) |
| 2026-09-04 | [RF3 SQL comparison after durable update and checkpoint fixes](crdb-sql-2026-09-04-writes/README.md) |
| 2026-09-04 | [RF3 SQL comparison after adaptive read admission](crdb-sql-2026-09-04-reads/README.md) |
| 2026-09-04 | [Physical-node short comparison audit](fused-node-short-2026-09-04/audit-summary.md) |
| 2026-09-04 | [Pgwire exact unnamed-Parse reuse](pgwire-frontend-reuse-2026-09-04/README.md) |
| 2026-09-04 | [Packed ordered FOR COUNT SIMD evidence](packed-order-simd-2026-09-04/README.md) |
| 2026-09-04 | [Packed integer interval count, Go 1.27 SIMD](packed-interval-simd-2026-09-04/README.md) |
| 2026-09-04 | [Packed equality count SIMD measurements](packed-count-simd-2026-09-04/README.md) |
| 2026-09-04 | [Indexed point reads and ordered pages](indexed-pages-2026-09-04/README.md) |
| 2026-09-04 | [Health-observation rerun](fused-health-2026-09-04/README.md) |
| 2026-09-04 | [Guarded point-update comparison](fused-guarded-update-2026-09-04/README.md) |
| 2026-09-04 | [Gateway pgwire distributed-read prepare cache](pgwire-read-prepare-cache-2026-09-04/README.md) |
| 2026-09-04 | [Distributed SQL result encoding](distributed-read-results-2026-09-04/README.md) |
| 2026-09-04 | [Compact scalar prefix comparison](compact-prefix-2026-09-04/README.md) |
| 2026-09-04 | [Compact prefix CPU change: matched SQL comparison](crdb-sql-2026-09-04-prefix/README.md) |
| 2026-09-04 | [Compact batch-column experiment and revert](fused-storage-2026-09-04/README.md) |
| 2026-09-04 | [CockroachDB comparison after ordered primary LIMIT: 2026-09-04](crdb-sql-2026-09-04/README.md) |
| 2026-09-04 | [Asynchronous Raft serving comparison](crdb-sql-2026-09-04-pipelined/README.md) |
| 2026-09-04 | [AMD64 packed equality count SIMD measurements](packed-count-simd-amd64-2026-09-04/README.md) |
| 2026-09-03 | [CockroachDB comparison: 2026-09-03](crdb-sql-2026-09-03/README.md) |

## Read archived evidence

Some reports retain raw files in a sibling `.tar.gz` archive. Follow the
report's extraction instructions; a path inside that archive becomes a local
link after extraction. Check the recorded hashes before using raw files.
Keep checksum-bound reports unchanged when adding navigation or a new result.

[Research records](../design/research.md) index proposals and bottleneck notes
that have not become a benchmark report.
