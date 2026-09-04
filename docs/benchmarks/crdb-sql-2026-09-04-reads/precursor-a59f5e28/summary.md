Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,274.4 | 5,219.7 | 0.436× |
| point_hit | 8 | 10,533.4 | 32,050.6 | 0.329× |
| point_miss | 1 | 2,374.1 | 5,383.3 | 0.441× |
| point_miss | 8 | 10,615.0 | 33,662.4 | 0.315× |
| range_64 | 1 | 1,506.0 | 3,447.0 | 0.437× |
| range_64 | 8 | 7,670.5 | 22,857.3 | 0.336× |
| group_16 | 1 | 114.7 | 410.7 | 0.279× |
| group_16 | 8 | 620.2 | 1,885.9 | 0.329× |
| update_existing | 1 | 368.2 | 931.8 | 0.395× |
| update_existing | 8 | 2,097.2 | 3,894.3 | 0.539× |
