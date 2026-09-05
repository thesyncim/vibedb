# Source-identical range-source microbenchmark

The four runs are ABBA: baseline, candidate, candidate, baseline. They use the same immutable snapshot fixture and the same benchmark source. `oracle_generic` is not timed here; only `range_source` is a before/after arm.

| Lifecycle | Baseline median | Candidate median | Candidate speed ratio | Allocation change |
|---|---:|---:|---:|---|
| Fresh `Exec` | 39,907.5 ns/op | 21,705.5 ns/op | 1.8386x | 125,056 → 48,680 B/op; 51 → 55 allocs/op |
| Warm reused `Exec` | 31,257.0 ns/op | 15,348.5 ns/op | 2.0365x | 0 → 0 B/op; 0 → 0 allocs/op |

All cases reported 64 rows and 64 scanned rows per operation. Candidate range-source cases reported 64 projected rows per operation. The microbenchmark ran on macOS ARM64 Apple M4 Max with Go 1.27 and `GOMAXPROCS=4`; it is a focused local differential, separate from the Linux/ARM64 RF3 campaign.

[Raw trial logs and provenance are retained in validation-logs.tar.gz](validation-logs.tar.gz).
