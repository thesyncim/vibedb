Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,078.2 | 4,801.9 | 0.433× |
| point_hit | 8 | 6,563.9 | 32,015.1 | 0.205× |
| point_miss | 1 | 2,358.7 | 4,943.5 | 0.477× |
| point_miss | 8 | 7,018.5 | 33,237.1 | 0.211× |
| range_64 | 1 | 1,470.9 | 3,547.7 | 0.415× |
| range_64 | 8 | 4,270.1 | 23,425.8 | 0.182× |
| group_16 | 1 | 170.8 | 402.9 | 0.424× |
| group_16 | 8 | 354.8 | 2,424.0 | 0.146× |
| update_existing | 1 | 390.5 | 909.4 | 0.429× |
| update_existing | 8 | 2,029.3 | 3,966.8 | 0.512× |
