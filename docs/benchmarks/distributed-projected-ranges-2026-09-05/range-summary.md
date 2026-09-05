| Order | Workload | Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |
|---|---|---:|---:|---:|---:|---:|---:|
| before-first | range_32 | 1 | 3,333.1 | 3,569.3 | 4,841.9 | 1.071x | 0.737x |
| before-first | range_32 | 8 | 18,996.5 | 19,597.2 | 32,089.3 | 1.032x | 0.611x |
| before-first | range_64 | 1 | 3,066.8 | 3,081.3 | 4,789.8 | 1.005x | 0.643x |
| before-first | range_64 | 8 | 17,605.0 | 18,720.1 | 31,125.1 | 1.063x | 0.601x |
| before-first | range_256 | 1 | 1,899.6 | 2,162.8 | 3,403.3 | 1.139x | 0.636x |
| before-first | range_256 | 8 | 11,606.8 | 13,422.3 | 19,173.8 | 1.156x | 0.700x |
| after-first | range_32 | 1 | 3,145.3 | 3,491.3 | 5,022.4 | 1.110x | 0.695x |
| after-first | range_32 | 8 | 17,512.9 | 18,677.2 | 33,550.9 | 1.066x | 0.557x |
| after-first | range_64 | 1 | 2,804.5 | 3,213.3 | 4,577.9 | 1.146x | 0.702x |
| after-first | range_64 | 8 | 16,526.4 | 19,074.5 | 31,581.8 | 1.154x | 0.604x |
| after-first | range_256 | 1 | 1,842.4 | 2,198.7 | 2,169.1 | 1.193x | 1.014x |
| after-first | range_256 | 8 | 11,995.1 | 13,066.4 | 14,267.2 | 1.089x | 0.916x |
