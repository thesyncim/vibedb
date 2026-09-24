# Source and algorithm provenance

[Documentation](README.md) · [Development status](status.md)

## Project license status

No file named `LICENSE` grants rights for VibeDB as a whole. Files named
`LICENSE-*` and `PATENTS-*` preserve notices for incorporated dependencies or
derived algorithms:

| Notice | Scope |
| --- | --- |
| `LICENSE-ETCD-RAFT` | etcd Raft dependency |
| `LICENSE-PROTOBUF`, `PATENTS-PROTOBUF` | Protocol Buffers dependency |
| `LICENSE-ROARING` | Roaring-derived work/notices |
| `LICENSE-XXHASH` | xxHash dependency/algorithm |
| `x/vitessroute/LICENSE-VITESS` | Vitess-derived routing algorithms in the optional nested module |

These notices are not a substitute for a VibeDB project license. Resolve that
status before external use or redistribution.

## Root module dependencies

The authoritative dependency set and exact versions are in `go.mod` and
`go.sum`. The root module currently depends directly on:

| Module | Used for | Notice in this repository |
| --- | --- | --- |
| `github.com/cespare/xxhash/v2` | Placement hashing and planner statistics | `LICENSE-XXHASH` |
| `github.com/pierrec/lz4/v4` | Block compression of exact primary packs (`internal/storeio/primary_exact_pack.go`) | **None.** The module is BSD-licensed; no root `LICENSE-LZ4` notice exists yet. |
| `github.com/thesyncim/vibejson` | JSON parsing and canonical cells | The module ships its own `LICENSE-GO` and `LICENSE-SIMDJSON` notices |
| `go.etcd.io/raft/v3` | Raft consensus core, driven through `internal/raftmodel` | `LICENSE-ETCD-RAFT` |
| `golang.org/x/sys` | Platform system calls | None in this repository |
| `google.golang.org/protobuf` | Deterministic encoding of Raft protocol messages and bootstrap records | `LICENSE-PROTOBUF`, `PATENTS-PROTOBUF` |

Nested modules keep their dependencies out of the root module's graph:

| Module | Direct third-party dependencies |
| --- | --- |
| `bench/competitive` | bbolt, Badger, Pebble, `modernc.org/sqlite` (benchmark comparison engines) |
| `integration/pgclient` | `jackc/pgx/v5`, `lib/pq` (client tests) |
| `integration/pgcompat` | None beyond the root module; the runner fetches a pinned PostgreSQL 18.6 source corpus |
| `x/vitessroute` | `vitess.io/vitess` v0.24.2, imported **only by test files** as the differential oracle |

## Derived algorithms

Code comments cite an algorithm provenance ID where an implementation follows
an external design. The IDs used in the source are:

| ID | Upstream | Where | Notice |
| --- | --- | --- | --- |
| `ALGO-ROARING-001` | Roaring bitmaps: chunked containers and the `advanceUntil` galloping search | `query/candidates_mask.go`, `store/store_index_exact.go` | `LICENSE-ROARING` (Apache 2.0) |
| `ALGO-BLOOM-BLOCKED-001` | Cache-blocked Bloom filter, with each key's bits confined to one 32-byte block | `query/join_bloom.go` | None. The comments do not name an upstream implementation. Record one if code was adapted. |
| `ALGO-VITESS-XXHASH-001` | Vitess `go/vt/vtgate/vindexes/xxhash.go` keyspace ID | `x/vitessroute/xxhash.go` | `x/vitessroute/LICENSE-VITESS` (Apache 2.0) |
| `ALGO-VITESS-MULTICOL-001` | Vitess `multicol.go` and `cfc.go` keyspace ID and prefix ranges | `x/vitessroute/multicol.go` | `x/vitessroute/LICENSE-VITESS` |

`internal/tin` claims behavioral compatibility with PlanetScale TIN
normalization and query syntax. Its package comment states that it depends
only on the standard library.

## Vitess routing profile

`x/vitessroute` is an optional nested module. It reimplements the bounded
`xxhash` and `multicol` keyspace-ID behavior of one pinned Vitess source profile
behind VibeDB's dependency-free `distribution.Mapper` interface. No Vitess type
crosses the public API, and non-test code does not import Vitess. The harness
pins upstream v0.24.2. According to the source comments, the reproduced
algorithms are unchanged from v0.22.0 through v0.24.2.

Differential golden vectors are the compatibility oracle. A mismatch is a stop
condition; do not edit the expected vector to make a divergent algorithm pass.
The supported profile is intentionally much narrower than Vitess: no lookup or
owned vindexes, sequences, routing rules, or arbitrary destination widths.

## Maintenance rules

When adding or deriving code:

1. record the upstream project, exact revision, files/symbols, and license;
2. preserve required notices next to the affected module;
3. isolate optional dependency graphs in a nested module when practical;
4. add differential or byte-exact tests for reproduced algorithms;
5. cite an `ALGO-…` ID in the code comment and add it to the table above;
6. update this page and review the unresolved project-license boundary.

No workflow runs the `x/vitessroute` differential tests. Run them after
changing that module:

```sh
(cd x/vitessroute && GOEXPERIMENT=simd go test ./...)
```

## Source map

- Root [go.mod](../go.mod) and [go.sum](../go.sum); optional [Vitess module](../x/vitessroute/go.mod) and [benchmark module](../bench/competitive/go.mod)
- [x/vitessroute/doc.go](../x/vitessroute/doc.go) and golden tests
- root `LICENSE-*`, `PATENTS-*`, and [x/vitessroute/LICENSE-VITESS](../x/vitessroute/LICENSE-VITESS)
- [query/candidates_mask.go](../query/candidates_mask.go), [query/join_bloom.go](../query/join_bloom.go), [internal/storeio/primary_exact_pack.go](../internal/storeio/primary_exact_pack.go)
