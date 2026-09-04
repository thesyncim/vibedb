Validated 120,000 raw latency samples; medians require all repetitions to pass.

| Workload | Clients | VibeDB ops/s | CRDB ops/s | VibeDB / CRDB |
|---|---:|---:|---:|---:|
| point_hit | 1 | 2,149.8 | 5,565.4 | 0.386× |
| point_hit | 8 | 10,200.5 | 32,465.8 | 0.314× |
| point_miss | 1 | 2,351.1 | 5,387.5 | 0.436× |
| point_miss | 8 | 10,566.2 | 31,991.8 | 0.330× |
| range_64 | 1 | 1,501.6 | 3,448.2 | 0.435× |
| range_64 | 8 | 7,074.6 | 23,247.3 | 0.304× |
| group_16 | 1 | 181.5 | 398.2 | 0.456× |
| group_16 | 8 | 1,242.6 | 2,369.8 | 0.524× |
| update_existing | 1 | 373.1 | 877.2 | 0.425× |
| update_existing | 8 | 1,958.6 | 4,454.1 | 0.440× |
