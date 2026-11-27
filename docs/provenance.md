# Source and algorithm provenance

[Documentation](README.md) · [Stability](status.md)

## License status

No root `LICENSE` file is published for VibeDB as a whole. Third-party notices
apply to their incorporated dependencies and derived algorithms:

| Notice | Scope |
| --- | --- |
| `LICENSE-ETCD-RAFT` | `go.etcd.io/raft/v3` |
| `LICENSE-PROTOBUF`, `PATENTS-PROTOBUF` | `google.golang.org/protobuf` |
| `LICENSE-ROARING` | Roaring-derived bitmap containers (`ALGO-ROARING-001`) |
| `LICENSE-XXHASH` | `github.com/cespare/xxhash/v2` |
| `x/vitessroute/LICENSE-VITESS` | Vitess-derived routing (`ALGO-VITESS-XXHASH-001`, `ALGO-VITESS-MULTICOL-001`) |

## Module dependencies

Authoritative versions live in [`go.mod`](../go.mod). Direct root dependencies:

- `github.com/cespare/xxhash/v2` — placement hashing and planner statistics.
- `github.com/pierrec/lz4/v4` — exact primary pack compression (BSD; no root `LICENSE-LZ4` yet).
- `github.com/thesyncim/vibejson` — JSON parsing, canonical cells, `x/jsoncmp`, `x/keyhash` (ships its own `LICENSE-GO` / `LICENSE-SIMDJSON`).
- `go.etcd.io/raft/v3` — Raft consensus core via `internal/raftmodel`.
- `golang.org/x/sys` — OS syscalls.
- `google.golang.org/protobuf` — deterministic Raft & bootstrap encoding.

Nested modules isolate optional dependency graphs: `bench/competitive` (bbolt,
Badger, Pebble, SQLite), `integration/pgclient` (`pgx/v5`, `lib/pq`),
`integration/pgcompat` (fetches pinned PostgreSQL 18.6 corpus), and
`x/vitessroute` (`vitess.io/vitess` v0.24.2 imported **only in tests** as a
differential oracle).

## Derived algorithms

| ID | Upstream | Location | Notice |
| --- | --- | --- | --- |
| `ALGO-ROARING-001` | Roaring chunked bitmaps & `advanceUntil` | [query/candidates_mask.go](../query/candidates_mask.go), [store/store_index_exact.go](../store/store_index_exact.go) | `LICENSE-ROARING` |
| `ALGO-BLOOM-BLOCKED-001` | Cache-blocked 32-byte Bloom filter | [query/join_bloom.go](../query/join_bloom.go) | — |
| `ALGO-VITESS-XXHASH-001` | Vitess `xxhash` keyspace ID | [x/vitessroute/xxhash.go](../x/vitessroute/xxhash.go) | `x/vitessroute/LICENSE-VITESS` |
| `ALGO-VITESS-MULTICOL-001` | Vitess `multicol`/`cfc` prefix ranges | [x/vitessroute/multicol.go](../x/vitessroute/multicol.go) | `x/vitessroute/LICENSE-VITESS` |

`internal/tin` implements PlanetScale TIN query syntax and normalization using
only the Go standard library.

## Maintenance rules

When deriving code: record upstream revision/license, preserve required notices,
isolate optional graphs in nested modules, add differential/golden tests, cite
an `ALGO-*` ID in code, and update this page. Run `x/vitessroute` differential
tests manually after editing it:

```sh
(cd x/vitessroute && GOEXPERIMENT=simd go test ./...)
```
