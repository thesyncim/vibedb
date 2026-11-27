# Competitive benchmark harness

[Performance guide](../../docs/performance.md) · [Results registry](RESULTS.md) · [Coverage matrix](COVERAGE.md)

Separate Go 1.27 module isolating competitor dependencies (`Badger v4.9.5`,
`bbolt v1.5.0`, `Pebble v1.1.5`, `modernc.org/sqlite v1.54.0`; `go.mod` is
authoritative). Defaults to `GOEXPERIMENT=simd`; pass `GOEXPERIMENT=nosimd` for
portable runs.

## Verify harness

```sh
cd bench/competitive && GOEXPERIMENT=simd go test -count=1 ./...
```

## Commands

Run any command with `-h` for current flags:

| Command | Purpose |
| --- | --- |
| `cmd/mixed`, `cmd/mixedsuite` | Single-process workload; Latin-square repeated runs with raw rows |
| `cmd/footprint`, `cmd/churndisk` | Apparent bytes, allocated blocks, Go heap, RSS; fixed-live-set churn |
| `cmd/saturation`, `cmd/snapshotpressure` | Client-count plateau sweep; pinned-snapshot overhead |
| `cmd/sqlsurface`, `cmd/lifecycle` | Embedded SQL / pgwire overhead; cold/hot open, recovery, verify, repack |
| `cmd/outofram`, `cmd/speedprobe` | Above-RAM streaming qualification; VibeDB-only diagnostics |
| `cmd/coveragegen`, `cmd/publishcheck` | Regenerate [COVERAGE.md](COVERAGE.md); validate evidence inventory & receipt |

## Adapter and metric boundaries

| Adapter | Exact JSON indexes | Durability contracts |
| --- | --- | --- |
| VibeDB | 0–3 (`country`, `tier`, `region`) | `buffered-visible`, `async-stable-in-flight`, `ordinary-sync`, `power-safe` |
| SQLite (1 conn) | 0–3 | `buffered-visible`, `ordinary-sync`, `power-safe` |
| Badger, bbolt (mmap), Pebble | none | `buffered-visible`, `ordinary-sync` |

- Run one engine per process; match corpus shape, durability, index set, and
  warmup before comparing.
- `BenchmarkScan` touches 1 byte/doc; use `BenchmarkScanAllBytes` for
  full-byte throughput. `/proc/self/io` `write_bytes` is storage-layer traffic,
  not physical media writes.

## Evidence levels

| Level | Establishes |
| --- | --- |
| Generated coverage | Source harness & oracle exist for the 38-cell matrix (`go generate .`) |
| Command smoke / PR qualification | Bounded structural check (`competitive-evidence.yml`; not for `RESULTS.md`) |
| Publication candidate | Dedicated Linux run via `scripts/bench/run-publishable-evidence.sh <abs-dir> <OUT_OF_RAM_DOCS>` validated by `publishcheck` (`VALIDATED.tsv`) |
| Published result | Reviewed summary in [RESULTS.md](RESULTS.md) linked to immutable raw evidence |
