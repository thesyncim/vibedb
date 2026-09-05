# Ready-series matched RF3 result

Manifest SHA-256: `d9fce4f35949e9882a2fd7b21402527a0dfb6f5edd566110c3510c6c6dd2f140`

| Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |
|---:|---:|---:|---:|---:|---:|
| 1 | 1277.5 | 1251.6 | 2140.6 | 0.980x | 0.585x |
| 8 | 1731.4 | 2091.8 | 10439.6 | 1.208x | 0.200x |

All 96,000 measured operations completed with zero errors and every trial was verified.

The captured CRDB store artifacts were 225.2x larger by logical file bytes than the candidate artifacts across the two orders.
CRDB was captured live while VibeDB was captured after clean stop because those are the reliable harness artifacts, so treat this as raw footprint evidence rather than a promotion-grade space benchmark.
