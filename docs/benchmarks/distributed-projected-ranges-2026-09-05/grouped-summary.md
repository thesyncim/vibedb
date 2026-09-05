| Order | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |
|---|---:|---:|---:|---:|---:|---:|
| before-first | 1 | 1,185.8 | 1,211.0 | 566.4 | 1.021x | 2.138x |
| before-first | 8 | 8,832.6 | 8,154.5 | 3,252.3 | 0.923x | 2.507x |
| after-first | 1 | 1,170.6 | 1,183.2 | 479.9 | 1.011x | 2.465x |
| after-first | 8 | 8,688.2 | 8,793.6 | 2,645.8 | 1.012x | 3.324x |
