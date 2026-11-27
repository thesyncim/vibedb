# Performance evidence

[Documentation](README.md) · [Stability](status.md)

Use this guide to choose a benchmark harness and follow VibeDB's measurement
rules. Reusable comparison methods live in [benchmark methods](benchmarks/README.md);
the separate formal registry [`bench/competitive/RESULTS.md`](../bench/competitive/RESULTS.md)
currently has no endorsed entries.

## Harnesses

| Question | Tool | Establishes |
| --- | --- | --- |
| Did a hot path allocate more? | `go run ./bench/gate` | Same-machine `allocs/op` (0 increase) and `B/op` (≤5% increase) vs Git base; never gates time |
| Single embedded workload | `bench/competitive/cmd/mixed` | Isolated latency, throughput, memory, and disk |
| Comparable repeated runs | `cmd/mixedsuite` | Latin-square execution order + raw per-run TSV |
| File & RSS footprint | `cmd/footprint` | Apparent bytes, allocated blocks, Go memory, RSS |
| Churn disk growth / saturation | `cmd/churndisk`, `cmd/saturation` | Bounded disk growth and host-specific plateau |
| Snapshot pressure / SQL overhead | `cmd/snapshotpressure`, `cmd/sqlsurface` | Pinned-snapshot cost; embedded SQL / pgwire overhead |
| Cold open / out-of-RAM streaming | `cmd/lifecycle`, `cmd/outofram` | Isolated lifecycle timers; streaming bounds |
| RF3 fault survival | `go run ./bench/rf3chaos` | External-process fault evidence for one exact build |

`bench/competitive/COVERAGE.md` is generated from a 38-cell harness manifest;
"Implemented" means an executable harness exists, not a published result.

## Allocation gate

```sh
go run ./bench/gate
go run ./bench/gate -base <commit>
```

Local default compares against the merge base of `main`; CI (`bench-gate.yml`)
passes the exact PR base.

## Comparison and publication rules

- Record exact commit, dirty state, Go toolchain, lockfiles, host hardware/OS/FS,
  durability profile, cache budget, corpus digest/seed, warmup, and all raw
  per-run rows (never drop slow/failed repetitions without a predeclared rule).
- Match acknowledgement durability before comparing write latency.
- Account for engine differences (e.g., bbolt mmaps the whole file; SQLite
  adapter uses one connection; plain KV engines do not run exact-index lanes).
- Do not extrapolate single-group in-process RF3 latency into horizontal
  scaling claims, or `BenchmarkScan` (1 byte/doc) into byte-throughput claims.
- `publishcheck` validates inventory, revision, and receipt digests on a
  trusted directory; it does not certify workload fairness or hardware parity.

## Source map

- [bench/gate/main.go](../bench/gate/main.go) · [bench/rf3chaos/main.go](../bench/rf3chaos/main.go)
- [bench/competitive/internal/coverage/manifest.go](../bench/competitive/internal/coverage/manifest.go) · [bench/competitive/cmd/publishcheck](../bench/competitive/cmd/publishcheck)
- [scripts/bench/run-ci-competitive-evidence.sh](../scripts/bench/run-ci-competitive-evidence.sh)
