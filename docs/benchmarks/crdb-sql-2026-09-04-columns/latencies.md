Median of each repetition’s percentile; individual samples are retained.

| Workload | Clients | Engine | p50 ms | p95 ms | p99 ms |
|---|---:|---|---:|---:|---:|
| point_hit | 1 | vibedb | 0.384 | 0.579 | 0.839 |
| point_hit | 1 | cockroachdb | 0.170 | 0.237 | 0.351 |
| point_hit | 8 | vibedb | 0.691 | 1.115 | 1.466 |
| point_hit | 8 | cockroachdb | 0.236 | 0.317 | 0.379 |
| point_miss | 1 | vibedb | 0.368 | 0.552 | 0.838 |
| point_miss | 1 | cockroachdb | 0.177 | 0.210 | 0.369 |
| point_miss | 8 | vibedb | 0.661 | 1.079 | 1.406 |
| point_miss | 8 | cockroachdb | 0.229 | 0.317 | 0.394 |
| range_64 | 1 | vibedb | 0.609 | 0.863 | 1.208 |
| range_64 | 1 | cockroachdb | 0.274 | 0.341 | 0.486 |
| range_64 | 8 | vibedb | 0.885 | 1.600 | 2.214 |
| range_64 | 8 | cockroachdb | 0.331 | 0.506 | 0.684 |
| group_16 | 1 | vibedb | 5.374 | 5.766 | 6.180 |
| group_16 | 1 | cockroachdb | 2.313 | 2.698 | 4.148 |
| group_16 | 8 | vibedb | 5.601 | 9.364 | 11.148 |
| group_16 | 8 | cockroachdb | 2.641 | 6.157 | 8.935 |
| update_existing | 1 | vibedb | 2.129 | 3.250 | 11.347 |
| update_existing | 1 | cockroachdb | 1.073 | 1.365 | 1.634 |
| update_existing | 8 | vibedb | 3.783 | 16.789 | 22.622 |
| update_existing | 8 | cockroachdb | 1.878 | 2.367 | 2.579 |
