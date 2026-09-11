# Decision: compress durable images, preserve canonical resident indexes

Planning decision, 2026-09-08. Source candidate `11b2cf141`; scan/census follow-up `0dc8ac187` in draft PR 230. This plan was produced by one Astra Max planning pass, with production implementation assigned to Sol. The plan itself does not establish a production performance result.

## Recommendation

**Implement densely packed, LZ4-compressed exact-index leaf images first. Preserve the current logical term leaves and their decoded resident representation.** This simultaneously attacks the two demonstrated costs: repeated string bytes and one rounded physical extent per small logical leaf.

The completed Stage 0 control refined the implementation order after the planning pass: begin with **per-index packs**. Packing plus LZ4 captures most of the opportunity without mixing indexes. Mixing adds roughly 0–18 B/document on the sampled two-index cases, while the existing external build streams one index at a time. Cross-index packing remains an explicit follow-up experiment; it must justify extra streaming and ownership complexity. The format still identifies each member's physical index so admission can reject grafted references.

Do **not** begin with a global field-ID service or a new typed-component reference grammar. First measure their incremental opportunity against packed LZ4, not against today's uncompressed, separately allocated leaves. Low-cardinality long fields should be a particularly strong case for ordinary block compression. That is an inference to verify, not a measured result.

For primary data, the coherent eventual design is **physical page-image compression with a decoded page cache**, retaining the existing scalar codecs inside the decoded page. It is a separate, larger second stage. The exact-index stage can ship independently because exact leaves are already decoded into owned resident bytes at Open; it does not require the general PageCache size refactor.

**Do not ship the current per-string LZ4 candidate unchanged.** Its narrow eligible screen saves 0–14.7% of total file space but adds 3–6% to resident reads and 7–16% to scans. The ongoing dictionary-fragment scan fix deserves one paired check. Keep that candidate only if the final actual-database results meet the agreed latency budget and eliminate no-space-benefit selections. Do not keep extending it to justify its existence. It is not the main compression architecture, and it has no demonstrated benefit for the original 10M varied-v1 payload. Once page-image compression exists, remove the redundant persisted read-time LZ4 string representation unless a measured workload establishes a separate compelling benefit.

Latest parent-reported scan screen: the 8/32-value cases improve to 185–187 ns/document with prepared fragments; 64/128-value cases remain approximately 213–220 ns because the decoded dictionary exceeds the existing 8 KiB pool. Point reads remain around 414–422 ns pending a fresh baseline pair. This repairs an eligible subset; it does not resolve the architectural read-cost or physical-admission concerns.

## What the measurements actually diagnose

The fresh 20,000-row overlap census is far more actionable than codec microbenchmarks. Values below are B/document; these are one-run structural measurements, not qualified performance comparisons.

| Two compound indexes | Total file | Exact key bytes | Exact metadata + postings | Exact leaf extents |
|---|---:|---:|---:|---:|
| 256-byte field, cardinality 8, shared-leading | 257.0 | 87.88 | 39.83 | 231.4 |
| Same field, shared-nonleading | 789.7 | 490.4 | 39.71 | 764.1 |
| 256-byte field, cardinality 1024, shared-leading | 1014 | 491.8 | 42.18 | 794.4 |
| Same field, shared-nonleading | 1063 | 529.0 | 42.29 | 843.6 |
| 16-byte field, cardinality 64, shared-leading | 206.6 | 26.57 | 41.69 | 181.0 |

Sources: `overlap-census.txt` beside this document and `store/durable/store_file_exact_overlap_space_bench_test.go` in the source checkout. Use one-index versus two-index differences for additive index costs: enabling any index also changes primary leaf geometry, so subtracting the no-index file confounds those costs.

The index codec already prefix-compresses adjacent tuple bytes and uses compact bucket/slot postings. It cannot eliminate a repeated long component after an independently varying earlier component. The last row has only about **68.3 B/document of encoded leaves occupying 181 B/document of leaf extents**. Merely changing string compression misses this allocation problem.

The cause is explicit in `internal/storeio/index_term_cutter.go:43`: the hash cut targets 48 terms/run. `store/durable/store_file_primary_exact.go:1509` gives each encoded leaf its own rounded physical extent, with a 4 KiB minimum and power-of-two geometry for exact leaves. Preserve the 48-term run as the mutation locality unit. Increasing the run size to make allocation look better also increases dirty-run rewriting.

Useful arithmetic bounds, **not predictions**:

* On short/shared-leading, the gap between exact extents and existing encoded leaves is about 112.7 B/document, or 54.5% of the whole file. Pack headers, catalog entries, final-pack slack, and churn consume part of this opportunity.
* On long/cardinality-8/nonleading, exact key bytes alone are 62.1% of the whole file; extent overhead above all encoded exact bytes is another roughly 29.6%. These are different byte categories, but neither can be fully removed in a real implementation.
* High-cardinality, differently ordered indexes expose a much harder sharing problem. A 64 KiB window cannot make a collection-wide unique-value pool. Report this limitation explicitly.
* The old 18.094 GiB versus 12.422 GiB CRDB comparison predates log reclamation. This plan does not establish a new whole-database result, count reclamation twice, or justify a 35–40% advantage over CRDB.

## Serious alternatives and why this order wins

| Architecture | Strength | Cost/limitation | Decision |
|---|---|---|---|
| Individual primary dictionary-string LZ4 | Tiny bounded decode; existing prepared-buffer implementation | Repeats decode work on resident reads; codec wins may disappear in extent rounding | Qualify once; not the main design |
| Packed canonical exact images + LZ4 | Removes per-leaf allocation floor; captures within/across-leaf and cross-index byte repetition; current read comparator unchanged | Whole-pack Open decode; partial-pack garbage needs correct reclamation | **First implementation** |
| Shared typed-component bank within a pack | Explicit reuse of arbitrary component positions; no string training; bounded ownership | New parser/layout and reference directories; much may duplicate what LZ4 already achieves | Only after beating packed LZ4 on physical bytes |
| Global stable component pool | Can share long high-cardinality values across dissimilar index orders and checkpoints | Intern lookup, unique-value memory, persistent ownership, compaction, and extra write work | Not first; evaluate only remaining high-cardinality opportunity |
| One posting index per field + intersections | Shares postings and values structurally | More query work; compound range semantics and selective tuple probes become materially different | Reject under the minimal-read-regression goal |
| Generic primary page-image compression + decoded cache | Compression absent from warm query reconstruction; standard block codec over all existing scalar streams | Physical/logical length separation, two buffer roles, native I/O and journal/materialization changes | **Second architecture stage**, after index wins |
| FSST/static trained string symbols | Independent string decoding and a shared symbol table | Training/table amortization is poor on tiny dictionaries; does not solve physical leaf slack | Not the next experiment |

The prior objection that arbitrary shared IDs destroy lexical ordering is only a blocker if IDs enter the comparison representation. Here they need not. `store_file_primary_exact.go:21–46,154–244` already loads durable leaves into owned, canonical, admitted bytes. Both comparisons and fold maintenance use that representation. A future bank can resolve arbitrary stable handles once during admission and keep the same comparator. Do not design order-maintenance IDs or renumber all clean leaves at checkpoints.

The design follows established separation of disk blocks from memory pages, rather than introducing a new query representation. WiredTiger documents separate disk/memory formats and block allocation geometry; its block compression changes disk representation. [WiredTiger page sizes and compression](https://source.wiredtiger.com/develop/tune_page_size_and_comp.html). RocksDB independently compresses blocks and can share dictionary material across blocks, which supports testing a larger compression scope before inventing a field-ID system. [RocksDB dictionary compression](https://github.com/facebook/rocksdb/wiki/Dictionary-Compression). FSST's symbol table enables individual string decompression; that is a different tradeoff from decoding an index pack once at Open. [FSST paper](https://www.vldb.org/pvldb/vol13/p2649-boncz.pdf).

## First-stage data layout and ownership

Add a real physical `PagePrimaryExactPack` schema. This is one format with an explicit raw/LZ4 encoding tag, not a legacy fallback decoder.

```
common page envelope: StoreID, generation, pack logical ID, physical length, checksum
pack header: codec, member count, decoded byte count, stored byte count
member directory: physical index ID, decoded offset, decoded length
body: concatenated canonical IndexTermLeaf images, raw or one LZ4 block
```

Catalog leaves become `{pack PageRef, member ordinal, firstTile, flags, first-key prefix}`. The directory binds each member to its physical index ID; catalog admission validates this identity in addition to current first-key/tile/run checks. The decoded leaf retains its existing checksum and full semantic admission. Validate all sizes, offsets, non-overlap, exact decoded length, and maximum output before exposing a view. Never accept an LZ4 output-length mismatch or guessed codec.

Use a bounded initial **64 KiB maximum decoded pack image**, further constrained by configured `MaxPageSize`, and a bounded member directory. Reserve fixed pack/singleton-directory overhead in the exact leaf hard-cap budget so every admitted logical leaf fits an uncompressed singleton pack. The 48-term hash-cut rule and giant-term stripe locality stay the same; only the envelope allowance changes. Large/giant terms require explicit tests at that boundary.

Allocate pack extents in **exact configured allocation quanta (4 KiB by default)**, not the next power of two. Add this page kind coherently to the exact-quantum allocator/validation paths already used by primary and overflow extents. Preserve nondefault configured PageSize alignment. This is smaller than changing the global allocation quantum or adding sub-page free-space allocation. A pack's raw fallback must always fit its format bounds.

Compress only after a pack is assembled from **unpadded canonical leaf images**. Compare rounded physical size of raw versus LZ4 including all headers and directories. Choose raw on a tie. No per-stream 12.5% heuristic is necessary. Keep a prepared compressor and bounded reusable output/decode scratch. This is ordinary byte compression; do not bake fixture-specific string patterns into it.

### Cross-index grouping is a follow-up

The mixed-index prototype feeds leaves in deterministic round-robin order, retaining canonical order within each index. A production follow-up would identify connected groups of physical indexes with common paths at schema preparation. Flush when the bounded byte/member budget is reached. Physical packing order need not equal any index's lexical order because catalogs already route to leaf identities. The initial per-index implementation preserves the existing streaming builder's memory bound.

An index-by-index pack fill is **not evidence of cross-index reuse**. Stage 0 measured raw packs, per-index LZ4 packs, and mixed-index LZ4 packs separately and validated every decoded member. Common field positions may differ; the compressor operates on their canonical bytes. Generic LZ4 can match them across varying intervening components. Its match distance is bounded, so unrelated orderings/high cardinality can still miss. [LZ4 block format](https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md).

Only new/dirty logical leaves are repacked during ordinary folds. Clean leaf references remain valid. Two indexes changed by the same document update naturally provide contemporaneous packing candidates; an update that changes no indexed term retains today's zero-index-record behavior. Do not synchronously rewrite cold neighboring leaves merely to fill a pack.

### Read path

`openPrimaryExactIndexes` groups catalog members by pack reference, acquires each physical pack once, decompresses it once into prepared bounded scratch, and copies/adopts each canonical member into the existing per-leaf owned storage. Prefer the existing independent leaf ownership initially: retaining a large shared decoded allocation for one surviving cold leaf creates unnecessary memory retention. Release compressed pack leases and temporary decode buffers after admission.

Queries continue using `primaryExactResident`, `IndexTermLeafView`, the current hash/equality route, restart-8 ordering, and current posting traversal. There is **no pack lookup, decompression, field-ID comparison, or dictionary lookup on resident point/range queries**. Report Open/recovery cost and peak startup memory separately; they are real costs, not free work. PageCache can continue caching physical encoded pack pages in this stage.

## Atomic publication, snapshots, and partial-pack reclamation

The indivisible allocation/retirement unit is a **pack**, while the logical replacement unit remains a leaf. Replace the current per-replaced-leaf retirement loop in `stagePrimaryExactPagesLocked` with unique physical pack ownership accounting.

1. Build new packs privately. Stage packs, replacement catalogs, and the exact root in the same existing transaction/sink publication. Never publish a catalog referencing an unsealed/unwritten pack.
2. Carried members preserve their pack references. Derive each new epoch's live pack/member set from its catalogs. A transient writer-owned table is sufficient; durable per-entry refcounts are unnecessary because the catalog is authoritative.
3. Retire an old physical pack exactly once only when the new catalog set has no live member in it. The existing generation/snapshot/recovery retention rules then determine when its extent becomes reusable. Deleting one index cannot retire a pack with members in another index.
4. Before publication failure, leave the old epoch authoritative and abort staged allocation normally. After publication, recovery reads the new exact root and validates every referenced member. Update `VisitPrimaryExactIndexRefs`, generation-migration retirement, free-space/reachability walks, capacity estimates, and crash cleanup to visit/deduplicate physical packs correctly.
5. Do not patch a shared pack in place. A snapshot may require a sibling or earlier member image even when one member was replaced. Pinning/retirement must be checked at physical-pack granularity.

Partially dead packs are an unavoidable cost of preserving cheap leaf replacement. Track their live member bytes; rebuild this information from catalogs at Open. The integration audit found that existing online compaction rebuilds the full primary graph and exact indexes; it is not bounded sparse-pack maintenance. Add an exact-only metadata transaction that collects a bounded set of sparse packs and repacks their live resident members. Start selecting candidates at at least 50% dead decoded member bytes, but **publish compaction only when the replacement set demonstrates a net physical allocation reduction**. Batch small survivors so several minimum-size sources can become fewer physical packs. Bound each maintenance slice by bytes; do not force a large repack into foreground writes. When replacing member references, preserve canonical leaf bytes and term/run identities.

The selection ratio is a maintenance policy, not a performance excuse: a churn benchmark must show bounded retained garbage, write amplification, and recovery-safe reuse before this becomes the default. Include unreclaimed snapshot-pinned packs and peak old+new staging space in those measurements. If sparse-pack maintenance cannot meet the write budget, reduce pack size; do not hide the cost by measuring only bulk creation.

## Primary page-image compression: the next source boundary

Current `PageRef.Length` is overloaded: disk read length, arena reservation, returned lease length, dirty accounting, checksum/header validation, and write length. Source boundaries include `page_cache.go:38,1016–1168,1775,2025–2099`, `page_cache_canonical.go`, `page_cache_inplace.go`, `write_transaction.go`, `primary_graph_sink.go`, `page.go`, `state_root.go`, and native/portable write paths.

Introduce distinct physical and decoded lengths in the reference/envelope contract; never silently reinterpret `Length` in some paths. Reserve cache bytes by decoded size, issue disk reads by physical size, and expose a fully validated decoded page to current leaf readers. Fixed per-I/O scratch/slots hold compressed input; a frame becomes ready only after exact-size decoding and semantic admission. Native io_uring reads must target registered compressed-input slots, not overwrite a decoded frame before validation. Charge both decoded residency and bounded I/O scratch honestly.

Writers build the existing decoded page first, try LZ4 on the whole useful image, choose physical extent after compression, and publish a final reference only after sealing. The current `AllocatePage(...).Ref()`-before-encoding contract therefore needs an explicit prepare/seal boundary; this is not a one-function codec patch. Keep dirty decoded frames and staged physical write buffers separate until durability/abort releases them.

Canonical materialization deserves a specific contract: a compressed page cannot receive the existing byte-offset patch stream or be recompressed silently into its old extent. The eventual physical-image journal must carry an exact before/after stored image, and permit stable-reference replacement only when the encoded image fits the owned physical extent and existing snapshot/recovery rules allow it; otherwise COW publishes a new physical reference. An initial COW-only compressed-page implementation is a useful correctness stage, but its write regression must be measured before default enablement. Update size validation, retirement/free geometry, cache rollback, direct I/O, checkpoints, and crash replay together.

Do not assume this will compress already-efficient alphabet/FOR/packed primary streams further. Its immediate merit over per-string LZ4 is moving decode cost off warm reads; actual primary physical savings remain an experimental question.

## Small implementation sequence and acceptance evidence

Scope estimate: exact packs are a medium storage change requiring separate codec/layout, catalog/sink, and retirement/recovery reviews; their standalone projection is small. General primary compression is a large cache/I/O/journal change and should not be folded into that first PR. The parent has already started the standalone exact-pack size probe; use its output as step 2 rather than starting a second probe.

1. **Finish the current narrow qualification, then freeze scope.** One paired screen after the scan preparation fix; check zero allocations, actual allocated bytes, point/scan latency. Preserve current successful durability fixtures. No new codec sweep or monolithic durable rerun.
2. **Build a standalone pack encoder/decoder plus sizing experiment.** Feed actual canonical exact leaves from the frozen census into raw, per-index LZ4, and mixed-index LZ4 packs. No persistent store changes yet. Include directory/4 KiB rounding, varied field positions/cardinalities and incompressible control. Prove byte-for-byte reconstruction of every member. This produces a concrete layout decision in hours, not another planning cycle.
3. **Integrate raw packs end-to-end.** Add catalog member refs, all creation/transaction/migration sinks, Open hydration, unique-pack retirement, corruption checks and snapshot/crash tests. This isolates ownership from compression. Keep the current canonical resident leaf codec and logical cutter invariants.
4. **Enable pack LZ4 using the already-tested raw/LZ4 tag.** Reuse prepared buffers. Finish physical metrics and sparse-pack maintenance. Run paired point/range/read/full-scan/insert/update/Flush/churn comparisons against both original leaves and raw packs.
5. **Evaluate cross-index mixing and explicit field banks only on residual opportunity.** Mixing must beat per-index packs after streaming, allocation and churn costs. A bounded pack-local bank of typed canonical component bytes is a further candidate; resolve references once into unchanged resident leaves. It must beat packed LZ4 after bank/reference/extent overhead by a meaningful amount (proposed minimum: another 10% of whole-file bytes on its declared target), with the same latency/write budget. Test high-cardinality, misaligned index orders. A global pool is a separate later design, not implied by this step.
6. **Implement the primary physical/decoded split in reviewable pieces.** Raw dual-length identity first; portable compressed I/O next; native I/O and physical-image materialization/journal support before default admission. Remove redundant per-string LZ4 when the new architecture replaces its purpose.

Proposed measurable release budgets (engineering targets, not forecasts):

* At least **25% whole-file reduction** on the chosen long/cardinality-8/nonleading overlap workload, and a clear raw-packing win on the short-field control. If the structural prototype misses this despite the census opportunity, inspect its layout/grouping before building more codecs.
* Prepared resident point and range probes stay at **zero additional allocations** and within **2% median / 5% p99** of baseline over paired repeated runs. Primary full scans must not regress merely because secondary storage changed. Open, recovery and cold reads get separate results.
* Buffered insert/update including final Flush stays within **5% throughput and 10% p99 latency** of baseline on unchanged fields, changed shared fields, changed unshared fields, and mixed churn. Include bytes written and time spent in maintenance. A tradeoff outside that budget needs an explicit decision, not a hidden threshold.
* Meaningful correctness coverage: randomized index equivalence; range boundaries and giant terms; duplicate/member/index-identity corruption; short/truncated/invalid LZ4; mixed-index drop; retained snapshots through repeated repacks; one member replaced while siblings survive; last-member retirement; allocator reuse; failure before/after each publication boundary; reopen after each crash point. Exercise portable and Linux io_uring/direct I/O in bounded CI shards.
* Use the project environment wrapper for Go commands and a stable `CODEX_AGENT_ID` when concurrent. The local experiment used `/Users/thesyncim/.codex/bin/project-env` because this revision has no `scripts/project-env`. Do not run expensive tests in the conflicted original checkout. Compare the same acknowledgement contract, cache size, schema, logical data and final Flush/reopen state; report live physical bytes, total file bytes, filesystem allocation, retained snapshots and peak staging separately.

This plan intentionally makes the first production step smaller than a general compressed-page cache and stronger than another string microcodec. It has a measured large opportunity, preserves the current read algorithms, and makes the principal new risk—shared physical ownership under mutation—explicit and testable.
