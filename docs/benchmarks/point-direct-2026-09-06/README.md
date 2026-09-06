# Direct point execution checkpoint

The local warm driver loop reaches zero allocations with caller-owned lease/cursor storage and preboxed parameters. On Apple M4 Max / Go 1.27 it measured about 3.1 us per hit and 2.1 us per miss, including cell verification. The unchanged allocating API loop reduced hit/miss allocations from 103/88 to 3/3. This does not establish zero allocation through the RF3 or wire stack.

The distributed comparison does **not** establish a throughput improvement. Reversing arm order changed both engines substantially, including CRDB. Keep both orders; do not publish the favorable first order alone. The single-host fixture cannot establish multi-machine performance or exclude host/VM contention. All measured requests returned zero errors and passed verification.

Three fused physical processes, RF3, four groups, 8192 rows/table, 8000 operations/cell, 2000 warmups, three repetitions, clients 1 and 8, 12 CPU / 24 GiB container limits. VibeDB read authority remained disabled. Before: `16bc587f`; after: `b2121c52`; CRDB image/version and immutable binaries are recorded in `provenance.json`.

Median successful operations/second within each order:

| Order | Engine | Hit C1 | Hit C8 | Miss C1 | Miss C8 |
|---|---|---:|---:|---:|---:|
| before-first | before | 3151.1 | 14926.6 | 3275.5 | 16260.1 |
| before-first | after | 3363.6 | 15800.5 | 3479.4 | 17288.9 |
| before-first | crdb | 6527.8 | 40551.0 | 6261.7 | 34962.9 |
| after-first | before | 4167.2 | 23516.9 | 4317.1 | 22124.8 |
| after-first | after | 3194.3 | 14377.2 | 3101.5 | 16980.5 |
| after-first | crdb | 3720.3 | 12049.5 | 2084.7 | 13108.9 |

`results.csv` preserves every measured cell. `provenance.json` records source revisions, binary and report hashes, topology and limits; full diagnostic artifacts remain at its recorded local path.

Validation: full query, SQL-driver and shard-service suites passed; focused race checks passed; materialization admission, stale handles, live identity mutation, and retained-buffer scrubbing have regression coverage.
