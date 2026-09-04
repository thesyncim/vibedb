Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,418.5 | 5,567.2 | 0.434× |
| point_hit | 8 | 10,842.2 | 32,977.1 | 0.329× |
| point_miss | 1 | 2,533.4 | 5,471.6 | 0.463× |
| point_miss | 8 | 11,338.5 | 33,734.6 | 0.336× |
| range_64 | 1 | 1,539.2 | 3,525.9 | 0.437× |
| range_64 | 8 | 8,137.8 | 22,605.3 | 0.360× |
| group_16 | 1 | 185.0 | 418.6 | 0.442× |
| group_16 | 8 | 1,240.0 | 2,461.6 | 0.504× |
| update_existing | 1 | 398.2 | 922.4 | 0.432× |
| update_existing | 8 | 1,474.3 | 4,175.6 | 0.353× |
