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

- `github.com/cespare/xxhash/v2`
- `github.com/thesyncim/vibejson`
- `go.etcd.io/raft/v3`
- `golang.org/x/sys`
- `google.golang.org/protobuf`

Benchmark and integration tools are separate Go modules so their competitor or
client dependencies do not enter the root module's graph.

## Vitess routing profile

`x/vitessroute` is an optional nested module. It reimplements the bounded
`xxhash` and `multicol` keyspace-ID behavior of one pinned Vitess source profile
behind VibeDB's dependency-free `distribution.Mapper` interface. No Vitess type
crosses the public API.

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
5. update this page and review the unresolved project-license boundary.

## Source map

- Root [go.mod](../go.mod) and [go.sum](../go.sum); optional [Vitess module](../x/vitessroute/go.mod) and [benchmark module](../bench/competitive/go.mod)
- [x/vitessroute/doc.go](../x/vitessroute/doc.go) and golden tests
- root `LICENSE-*`, `PATENTS-*`, and [x/vitessroute/LICENSE-VITESS](../x/vitessroute/LICENSE-VITESS)
