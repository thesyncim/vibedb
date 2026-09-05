| Order | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |
|---|---:|---:|---:|---:|---:|---:|
| before-first | 1 | 258.8 | 1,202.3 | 336.9 | 4.646x | 3.569x |
| before-first | 8 | 1,983.1 | 8,806.9 | 2,378.7 | 4.441x | 3.702x |
| after-first | 1 | 167.6 | 819.9 | 341.1 | 4.890x | 2.404x |
| after-first | 8 | 1,148.5 | 6,466.3 | 2,351.3 | 5.630x | 2.750x |
