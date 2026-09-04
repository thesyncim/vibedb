Median of each repetition’s latency percentile; individual samples are retained.

| Workload | Clients | Engine | p50 ms | p95 ms | p99 ms |
|---|---:|---|---:|---:|---:|
| point_hit | 1 | vibedb | 0.268 | 0.397 | 0.646 |
| point_hit | 1 | cockroachdb | 0.123 | 0.196 | 0.297 |
| point_hit | 8 | vibedb | 0.507 | 0.830 | 1.066 |
| point_hit | 8 | cockroachdb | 0.175 | 0.242 | 0.287 |
| point_miss | 1 | vibedb | 0.253 | 0.365 | 0.553 |
| point_miss | 1 | cockroachdb | 0.130 | 0.158 | 0.204 |
| point_miss | 8 | vibedb | 0.505 | 0.785 | 0.978 |
| point_miss | 8 | cockroachdb | 0.173 | 0.243 | 0.286 |
| range_64 | 1 | vibedb | 0.419 | 0.559 | 0.748 |
| range_64 | 1 | cockroachdb | 0.195 | 0.247 | 0.311 |
| range_64 | 8 | vibedb | 0.656 | 1.090 | 1.391 |
| range_64 | 8 | cockroachdb | 0.243 | 0.394 | 0.577 |
| group_16 | 1 | vibedb | 3.837 | 4.200 | 4.553 |
| group_16 | 1 | cockroachdb | 1.628 | 1.991 | 2.895 |
| group_16 | 8 | vibedb | 3.845 | 5.460 | 6.073 |
| group_16 | 8 | cockroachdb | 1.926 | 4.442 | 5.214 |
| update_existing | 1 | vibedb | 1.438 | 2.154 | 7.698 |
| update_existing | 1 | cockroachdb | 0.786 | 1.038 | 1.188 |
| update_existing | 8 | vibedb | 2.474 | 5.567 | 14.511 |
| update_existing | 8 | cockroachdb | 1.321 | 1.670 | 1.911 |
