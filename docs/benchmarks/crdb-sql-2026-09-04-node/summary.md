Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 3,450.5 | 7,317.8 | 0.472× |
| point_hit | 8 | 14,748.2 | 44,555.4 | 0.331× |
| point_miss | 1 | 3,708.5 | 7,418.1 | 0.500× |
| point_miss | 8 | 14,928.2 | 44,681.8 | 0.334× |
| range_64 | 1 | 2,280.4 | 4,965.3 | 0.459× |
| range_64 | 8 | 11,346.6 | 30,317.6 | 0.374× |
| group_16 | 1 | 257.9 | 587.7 | 0.439× |
| group_16 | 8 | 1,925.5 | 3,330.5 | 0.578× |
| update_existing | 1 | 621.0 | 1,240.3 | 0.501× |
| update_existing | 8 | 2,581.9 | 5,898.9 | 0.438× |
