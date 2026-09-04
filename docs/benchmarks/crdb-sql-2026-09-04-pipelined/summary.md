Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,470.7 | 5,578.8 | 0.443× |
| point_hit | 8 | 11,019.4 | 32,954.9 | 0.334× |
| point_miss | 1 | 2,497.6 | 5,195.5 | 0.481× |
| point_miss | 8 | 10,764.7 | 33,536.6 | 0.321× |
| range_64 | 1 | 1,496.5 | 3,474.6 | 0.431× |
| range_64 | 8 | 8,213.1 | 23,104.3 | 0.355× |
| group_16 | 1 | 182.6 | 406.3 | 0.450× |
| group_16 | 8 | 1,016.7 | 2,485.9 | 0.409× |
| update_existing | 1 | 429.3 | 936.2 | 0.459× |
| update_existing | 8 | 2,416.1 | 4,149.6 | 0.582× |
