# Source and algorithm provenance

This page maps external dependencies and derived algorithms to repository
evidence. License files remain authoritative for license text.

## Root module dependencies

| Dependency | Version in `go.mod` | Use in source | Repository notice |
| --- | --- | --- | --- |
| `github.com/cespare/xxhash/v2` | `v2.3.0` | Hashing and compatible placement adapters | `LICENSE-XXHASH` |
| `github.com/thesyncim/vibejson` | Pseudo-version pinned in `go.mod` | JSON parsing, canonicalization, paths, and output | Dependency module metadata |
| `go.etcd.io/raft/v3` | `v3.7.0` | Internal Raft state machine | `LICENSE-ETCD-RAFT` |
| `golang.org/x/sys` | `v0.47.0` | Platform I/O and system calls | Dependency module metadata |
| `google.golang.org/protobuf` | `v1.36.11` | Raft transport message encoding | `LICENSE-PROTOBUF`, `PATENTS-PROTOBUF` |

The repository also keeps `LICENSE-ROARING` as a third-party notice. Do not
infer a current dependency path from the notice alone. Use source imports and
module files to determine active dependency use.

## Vitess compatibility module

`x/vitessroute` is a nested Go module. It pins Vitess `v0.24.2` for differential
tests and provides a bounded compatibility adapter.

The derived algorithms are:

| ID | Source implementation | Local implementation | Validation |
| --- | --- | --- | --- |
| `ALGO-VITESS-XXHASH-001` | Vitess `vindexes/xxhash.go` | `x/vitessroute/xxhash.go` | Golden and differential tests |
| `ALGO-VITESS-MULTICOL-001` | Vitess `vindexes/multicol.go` and `cfc.go` | `x/vitessroute/multicol.go` | Golden and differential tests |

The local files identify the upstream paths, compatible version range, and
behavioral boundary. `x/vitessroute/LICENSE-VITESS` contains the related
license notice.

The adapter does not expose all Vitess Vindexes. It supports its documented
closed scalar and keyspace profile only.

## Maintenance rules

When you add or update an external dependency:

1. Update the applicable `go.mod` and `go.sum`.
2. Add or update the required license and patent notice.
3. Identify copied or derived source in the local file header.
4. Record the upstream repository, file, and version.
5. Add a stable algorithm ID when behavior must stay compatible.
6. Add golden and differential tests for compatibility code.
7. Update this ledger.

Do not use a benchmark match as the only parity proof. Compare exact outputs
over normal, boundary, and invalid inputs.

## Implementation references

- `go.mod` and `go.sum`
- `x/vitessroute/go.mod` and `go.sum`
- `x/vitessroute/xxhash.go` and `multicol.go`
- `x/vitessroute/differential_test.go` and `golden_test.go`
