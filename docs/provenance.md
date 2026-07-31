# Provenance

This is the current inventory of externally derived or adapted algorithms in
vibedb. Each implementation site carries a stable `Provenance: ID` comment that
maps to one row below.

The repository has no root project license. `LICENSE-ROARING` contains the
Apache-2.0 text required for the Roaring-derived algorithm named here; it does
not license the repository as a whole.

## Algorithm ledger

| ID | Local material | Source and treatment |
| --- | --- | --- |
| `ALGO-BLOOM-BLOCKED-001` | `query/join_bloom.go`: `joinBloomBlock`, `joinBloomSalt`, and `joinBloom.signature` | Blocked Bloom-filter layout described by Felix Putze, Peter Sanders, and Johannes Singler, “Cache-, Hash- and Space-Efficient Bloom Filters,” WEA 2007, DOI `10.1007/978-3-540-72845-0_9`, and specified as `BlockSplitBloomFilter` by Apache Parquet. The published eight odd salts are specification constants. Sizing, the bounded memory policy, the seeded `hash/maphash` hash, adaptive semi-join admission, and all integration are local. False-positive, no-false-negative, differential, and bounded-sizing tests enforce the contract. |
| `ALGO-ROARING-001` | `store/store_index_exact.go`: `storeIndexMergeBulkMasks`; `query/candidates_mask.go`: `advanceStoreMasksUntil` | Forward merge and galloping `advanceUntil` strategy adapted from RoaringBitmap Java PR [#840](https://github.com/RoaringBitmap/RoaringBitmap/pull/840), merge `ef131a71e0aa6cd67b4ea649c957b1cd4c52b141`, and Roaring Go commit `438e356606d4e651d47a1b8a95b5f2fe08f8c7fd`; Apache-2.0, `LICENSE-ROARING`. The local form operates on immutable stable-slot chunk words and a persistent radix posting. It does not copy Roaring's container representation. Randomized differentials cover order, overlap, intersection, and difference. |

## Maintenance

Before adding externally derived material:

1. add a stable ledger ID with the project or paper, exact revision and source
   location, upstream license, local changes, and integrity tests;
2. add `Provenance: ID` at every adapted implementation site;
3. include required upstream license and notice text;
4. do not guess an origin or retain provenance for code no longer present; and
5. update the eventual root `LICENSE` and `NOTICE` before release.
