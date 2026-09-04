Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 3,554.6 | 7,830.9 | 0.454× |
| point_hit | 8 | 14,479.3 | 43,848.5 | 0.330× |
| point_miss | 1 | 3,745.0 | 7,623.1 | 0.491× |
| point_miss | 8 | 15,210.5 | 44,584.5 | 0.341× |
| range_64 | 1 | 2,304.6 | 5,010.3 | 0.460× |
| range_64 | 8 | 11,342.8 | 29,855.5 | 0.380× |
| group_16 | 1 | 262.5 | 565.0 | 0.465× |
| group_16 | 8 | 1,922.4 | 3,231.2 | 0.595× |
| update_existing | 1 | 658.2 | 1,188.8 | 0.554× |
| update_existing | 8 | 2,807.0 | 6,067.3 | 0.463× |
