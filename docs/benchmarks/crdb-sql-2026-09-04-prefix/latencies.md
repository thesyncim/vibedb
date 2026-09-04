Median of each repetition’s percentile; individual samples are retained.

| Workload | Clients | Engine | p50 ms | p95 ms | p99 ms |
|---|---:|---|---:|---:|---:|
| point_hit | 1 | vibedb | 0.263 | 0.390 | 0.572 |
| point_hit | 1 | cockroachdb | 0.112 | 0.194 | 0.244 |
| point_hit | 8 | vibedb | 0.508 | 0.868 | 1.186 |
| point_hit | 8 | cockroachdb | 0.174 | 0.242 | 0.287 |
| point_miss | 1 | vibedb | 0.251 | 0.368 | 0.552 |
| point_miss | 1 | cockroachdb | 0.127 | 0.157 | 0.212 |
| point_miss | 8 | vibedb | 0.487 | 0.788 | 1.035 |
| point_miss | 8 | cockroachdb | 0.172 | 0.241 | 0.280 |
| range_64 | 1 | vibedb | 0.414 | 0.553 | 0.742 |
| range_64 | 1 | cockroachdb | 0.193 | 0.240 | 0.296 |
| range_64 | 8 | vibedb | 0.646 | 1.064 | 1.514 |
| range_64 | 8 | cockroachdb | 0.249 | 0.393 | 0.554 |
| group_16 | 1 | vibedb | 3.764 | 4.159 | 4.429 |
| group_16 | 1 | cockroachdb | 1.714 | 2.053 | 2.912 |
| group_16 | 8 | vibedb | 3.844 | 5.539 | 5.865 |
| group_16 | 8 | cockroachdb | 2.036 | 4.547 | 5.489 |
| update_existing | 1 | vibedb | 1.328 | 1.940 | 7.760 |
| update_existing | 1 | cockroachdb | 0.821 | 1.062 | 1.210 |
| update_existing | 8 | vibedb | 2.127 | 5.438 | 19.366 |
| update_existing | 8 | cockroachdb | 1.299 | 1.623 | 1.845 |
