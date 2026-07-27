# Provenance

This is the canonical inventory for externally derived source, algorithms,
generated output, tests, and corpora. A compact `Provenance: ID` comment at an
implementation site resolves to one row here. Conceptual similarity is not
listed as copied code, and uncertain origins stay explicitly unresolved.

This repository is an independent Go implementation and is not affiliated with
the C++ [`simdjson`](https://github.com/simdjson/simdjson) project.

## Release status

The repository has no root project license. `LICENSE-GO` contains the Go
Authors' BSD-3-Clause text for identified Go-derived material.
`LICENSE-SIMDJSON` and `LICENSE-ROARING` contain Apache-2.0 text for the
identified upstream-derived material named below. None licenses the repository
as a whole. A root `LICENSE` and final `NOTICE` remain release requirements.

## Source and algorithm ledger

| ID | Local material | Authoritative source and license | Local changes and integrity |
| --- | --- | --- | --- |
| `ALGO-BLOOM-BLOCKED-001` | `query/join_bloom.go:{joinBloomBlock,joinBloomSalt,joinBloom.signature}` | Algorithm: Felix Putze, Peter Sanders, and Johannes Singler, “Cache-, Hash- and Space-Efficient Bloom Filters,” WEA 2007, DOI `10.1007/978-3-540-72845-0_9`. The eight-word block layout, the one-bit-per-word rule, and the eight odd multiplicative salts are the widely republished form of that design, specified in the Apache Parquet format as `BlockSplitBloomFilter` (`parquet-format` `bloom_filter.md`) and implemented in Apache Impala and Apache Arrow under Apache-2.0 | No upstream source is copied; the salt constants are the published ones, which are the specification rather than an implementation detail. Sizing, the power-of-two block count, the memory cap, the seeded `hash/maphash` key hash, and the semi-join integration are local. `TestJoinBloomFalsePositiveRate` and `TestJoinBloomSizingIsBoundedAndSound` measure the result; `TestJoinBloomPrefilterHasNoFalseNegatives` and the exhaustive join differential cover the one-sided-error contract the query engine depends on. |
| `ALGO-ROARING-001` | `store_index_exact.go:storeIndexMergeBulkMasks`; `query/store_candidates.go:advanceStoreMasksUntil` | RoaringBitmap Java PR [#840](https://github.com/RoaringBitmap/RoaringBitmap/pull/840), merge `ef131a71e0aa6cd67b4ea649c957b1cd4c52b141`, `RoaringArray.mergeBulk`; Roaring Go commit `438e356606d4e651d47a1b8a95b5f2fe08f8c7fd`, `roaringarray.go:{mergeBulk,advanceUntil}`; Apache-2.0, `LICENSE-ROARING` | Reworked for immutable stable-slot chunk words: one forward union builds a persistent radix posting; Boolean mask intersection uses an overflow-safe dense-linear/skew-galloping hybrid. Randomized differential tests cover ordering, overlap, intersection, and difference. No upstream container representation or source file is copied. |

## Generated material and corpora

| ID | Local material | Source, license, and local treatment |
| --- | --- | --- |

Corpus-only dependencies are not copied into the root package. Their nested
`go.mod` and `go.sum` files are the authoritative version inventory.

## Unresolved origins

These items must not receive a guessed attribution:

- `encoder_int.go` uses `((bits.Len64(v)*1233)>>12)` for decimal digit count.
  History says the trick was borrowed but does not name its source.
- `number_exactness_test.go:TestFloatHardCases` combines boundary families that
  partly overlap C++ simdjson and classic strtod stress suites. The original
  change did not record an exact source for every string.
- `testdata/FUZZ_CORPUS.json` records ownership and hashes but not complete
  discovery, derivation, license, or introduction history for every seed.

Resolve an item only from documentary evidence. Until then, preserve the
warning at its implementation or inventory site.

## Work currently classified as local

The audit found no source-copy evidence for the Stage 2 pair-table DFA and
generated/goto machines; compiled typed plans and executors; hooks and stream
state machines; SIMD thresholds; most string scanning; Unicode-escape phase
tables; fused line-separator logic; ARM64 digit formatting; synthetic benchmark
models; or benchmark adapters. On-Demand-style and “analogue” comments are
conceptual acknowledgements, not source lineage.

## Maintenance rule

Before adding or changing externally related material:

1. record a stable ID, project or paper, exact revision, path or section,
   upstream license, local changes, confidence, and integrity proof here;
2. add `Provenance: ID` at each adapted implementation site;
3. keep required upstream license text with the repository or vendored corpus;
4. never replace missing history with a plausible guess; and
5. update the final `NOTICE` in the same change once that file exists.
