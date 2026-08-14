# Provenance

This is the current inventory of externally derived or adapted algorithms in
vibedb. Each implementation site carries a stable `Provenance: ID` comment that
maps to one row below.

The repository has no root project license. `LICENSE-ROARING` contains the
Apache-2.0 text required for the Roaring-derived algorithm named here, and
`LICENSE-ETCD-RAFT` carries the exact license distributed with the pinned Raft
module. `LICENSE-PROTOBUF` and `PATENTS-PROTOBUF` carry the notices from the
protobuf runtime used directly by the integration and transitively by the core.
None of these files licenses the repository as a whole.

## Dependency ledger

| Dependency | Pin, license, and treatment |
| --- | --- |
| `go.etcd.io/raft/v3` | `v3.7.0`, tag commit `b867cf13f6bc0dae21204302df97bc2355c3af55`, module sum `h1:BGzlwx07bLv8PW6OU5HObuz1y4hlPZUXA07pM1mPUh4=`, Apache-2.0 (`LICENSE-ETCD-RAFT`). It is used unmodified as consensus protocol machinery whose transitions are deterministic for fixed internal timeout state and exact input order; upstream privately samples election jitter from `crypto/rand`. VibeDB is responsible for the surrounding scheduling, storage, transport, identity/admission, apply/publication, snapshots, encryption, and configuration safety. The repository currently implements a bounded non-serving scheduler, append-only static-base WAL, local apply boundary, and post-authentication frame validator; it does not implement peer authentication, network transport, runtime snapshots/compaction, or replicated serving. The exact settings, exclusions, and threat model are recorded in [`design/raft-core-selection.md`](design/raft-core-selection.md). |
| `google.golang.org/protobuf` | Direct runtime dependency of the local integration and the selected Raft core's sole runtime module dependency at `v1.36.11`, module sum `h1:fV6ZwhNocDyBLK0dj+fg8ektcVegBBuEolpbTQyBNVE=`, BSD-3-Clause (`LICENSE-PROTOBUF`) plus the distributed additional IP rights grant (`PATENTS-PROTOBUF`). It supplies the generated Raft wire types and protobuf runtime; no protobuf source is copied or modified locally. |

## Algorithm ledger

| ID | Local material | Source and treatment |
| --- | --- | --- |
| `ALGO-BLOOM-BLOCKED-001` | `query/join_bloom.go`: `joinBloomBlock`, `joinBloomSalt`, and `joinBloom.signature` | Blocked Bloom-filter layout described by Felix Putze, Peter Sanders, and Johannes Singler, “Cache-, Hash- and Space-Efficient Bloom Filters,” WEA 2007, DOI `10.1007/978-3-540-72845-0_9`, and specified as `BlockSplitBloomFilter` by Apache Parquet. The published eight odd salts are specification constants. Sizing, the bounded memory policy, the seeded `hash/maphash` hash, adaptive semi-join admission, and all integration are local. False-positive, no-false-negative, differential, and bounded-sizing tests enforce the contract. |
| `ALGO-ROARING-001` | `store/store_index_exact.go`: `storeIndexMergeBulkMasks`; `query/candidates_mask.go`: `advanceStoreMasksUntil` | Forward merge and galloping `advanceUntil` strategy adapted from RoaringBitmap Java PR [#840](https://github.com/RoaringBitmap/RoaringBitmap/pull/840), merge `ef131a71e0aa6cd67b4ea649c957b1cd4c52b141`, and Roaring Go commit `438e356606d4e651d47a1b8a95b5f2fe08f8c7fd`; Apache-2.0, `LICENSE-ROARING`. The local form operates on immutable stable-slot chunk words and a persistent radix posting. It does not copy Roaring's container representation. Randomized differentials cover order, overlap, intersection, and difference. |
| `ALGO-VITESS-XXHASH-001` | `x/vitessroute/xxhash.go`: `vindexBytes` and `xxhashPointFromBytes` | Vitess `xxhash` Vindex keyspace-id computation, `vitess.io/vitess` `go/vt/vtgate/vindexes/xxhash.go`; Apache-2.0, `x/vitessroute/LICENSE-VITESS`. The keyspace id is `binary.LittleEndian` of `cespare/xxhash/v2` `Sum64` over `sqltypes.Value.ToBytes()`: raw bytes for a `String`, minimal decimal ASCII for an admitted lossless integer (the load-bearing subtlety — an integer never hashes as its binary form). Only the small serialization/framing is reproduced; the hash primitive is the same upstream library. The design names `v0.22.0`; the differential harness pins `v0.24.2`, whose `xxhash.go` is byte-identical to `v0.22.0` (`v0.22.0`'s `go/hack` package does not compile under this repo's Go 1.26 toolchain — the `swissmap` GOEXPERIMENT was removed). Vitess is a test-only dependency confined to `x/vitessroute/go.mod`; the root module and shipped adapter files carry no Vitess dependency. Authoritative differential tests drive real upstream Vitess (`x/vitessroute/differential_test.go`, golden vectors in `x/vitessroute/testdata/`); a mismatch is a stop condition. |
| `ALGO-VITESS-MULTICOL-001` | `x/vitessroute/multicol.go`: `resolveColumnBytes`, `mapMultiCol`, and `prefixRange` | Vitess `multicol` Vindex, `vitess.io/vitess` `go/vt/vtgate/vindexes/multicol.go`, plus the `NewKeyRangeFromPrefix`/`addOne` prefix-range logic in `go/vt/vtgate/vindexes/cfc.go`; Apache-2.0, `x/vitessroute/LICENSE-VITESS`. Reproduces `getColumnBytes` (the `ceil(remaining/pending)` width distribution), `mapKsid` (each column hashed by its `xxhash` sub-vindex, truncated to its byte width, and concatenated in column order), and `NewKeyRangeFromPrefix`/`addOne` (a leading prefix maps to `[prefix, addOne(prefix))`; an all-`0xff` prefix opens to the maximum end), narrowed to the fixed 8-byte profile. Pinned for differential testing at `v0.24.2`; the reproduced behavior is unchanged from the design's named `v0.22.0` (`cfc.go` is byte-identical and `multicol.go` differs only cosmetically). Authoritative differential tests drive real upstream Vitess; a mismatch is a stop condition. |

## Maintenance

Before adding externally derived material:

1. add a stable ledger ID with the project or paper, exact revision and source
   location, upstream license, local changes, and integrity tests;
2. add `Provenance: ID` at every adapted implementation site;
3. include required upstream license and notice text;
4. do not guess an origin or retain provenance for code no longer present; and
5. update the eventual root `LICENSE` and `NOTICE` before release.
