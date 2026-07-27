# Unsafe-code inventory

The default build contains bounded uses of `unsafe`; there is no alternate
safety mode. The uses fall into a small number of invariant families described
below. The generated scope list that follows is exhaustive for production Go
files and is checked in CI.

All unsafe changes require review by `@thesyncim`. A reviewer checks the
ordinary Go reference path first, then the bounds and layout proof, then
ownership and GC visibility, and finally the named correctness and performance
gates. Passing a benchmark is not a substitute for any earlier check.

## Invariant families

| Area and files | Why unsafe exists | Bounds and layout invariant | Ownership and lifetime invariant | Required tests | Performance contract |
| --- | --- | --- | --- | --- | --- |
| Borrowed and lazy views: `internal/byteview/byteview.go`, `node.go`, `raw.go`, `string_views.go` | Centralize allocation-free read-only byte/string views and navigate compact index entries without reflection or allocation. | Every view preserves its source length, and every source range comes from a validated token or checked index entry. `IndexEntry` has compile-time size and offset assertions. Empty inputs never dereference a nil base. | Byte views of strings are read-only. Callers keep borrowed storage alive and immutable for the returned view's lifetime. Value-derived `Node` pointers keep owned source and entry arrays visible to the collector. Index-derived nodes, `RawValue`, zero-copy results, and stream values borrow documented caller or reader storage. Go pointers remain typed pointers; none are stored as `uintptr`. | `ownership_lifetime_test.go`, `gc_lifetime_test.go`, `lazy_navigation_contract_test.go`, `parser_test.go`, `reader_lifecycle_test.go`, `route_differential_test.go` | `BenchmarkParse`, `BenchmarkBuildIndex`, `BenchmarkGetRaw`, `BenchmarkStreamReadNDJSON`, `BenchmarkStreamDynamicWalk` |
| Compiled decoding and hooks: `decoder_cursor.go`, `decoder_structural.go`, `typed.go`, `typed_compiled*.go`, `typed_hook*.go`, `typed_reset.go`, decode paths in `marshaler.go` | Execute a reflect-compiled type plan directly against typed destinations and call user methods with ordinary receiver semantics. | Field offsets and element strides come from `reflect.Type`; slice growth occurs before element addressing. Structural positions are validated before typed loads, and scalar fallbacks preserve the same grammar. | Destination pointers stay visible to the runtime. Native hook cursors cross user code by value; receivers are heap-backed or caller-owned according to normal Go method rules. Temporary decoded strings follow the documented owned or zero-copy mode. | `typed_test.go`, `typed_hook_safety_test.go`, `typed_hook_retention_test.go`, `gc_corruption_test.go`, `route_differential_test.go`, `decoder_structural_test.go` | `BenchmarkDecodeLargeReused`, `BenchmarkDecodeLargeIndentedReused`, `BenchmarkHookDecodeSmall`, `BenchmarkFieldSetLookup` |
| Compiled encoding: `encoder_execute*.go`, `encoder_int.go`, `encoder_string.go`, encode paths in `marshaler.go` | Walk a compiled type plan with reflection confined to dynamic storage and type boundaries. SWAR stores format short integers and strings. | Addresses use reflect-derived sizes and live slice or array bounds. Fixed-width loads and stores are guarded by remaining-length checks. Scratch slots retain their concrete pointer-bearing type and oversized backing is discarded. | Source pointers remain GC-visible for the complete call. User methods receive stable, legally retainable receivers. Pooled scratch does not retain caller values or reinterpret heterogeneous pointer layouts. | `encoder_lifetime_test.go`, `encoder_scratch_poison_test.go`, `encoder_heterogeneous_scratch_test.go`, `concurrency_corruption_test.go`, `marshaler_test.go` | `BenchmarkEncodeLarge`, `BenchmarkEncodeMap`, `BenchmarkHookEncodeSmall`, `BenchmarkEncodeTinyAfterHuge` |
| Validation, numbers, dynamic values, and index construction: `any.go`, `index.go`, `index_bitmap.go`, `index_positions.go`, `number_digits.go`, `number_float*.go`, `valid_bitmap*.go`, `valid_fast.go`, `valid_positions.go`, `walk_number_swar.go` | Use checked fixed-width loads, SWAR digit classification, and compact structural buffers in the parser's hottest loops. | Each fixed-width load is dominated by an explicit remaining-byte check. Bitmap, structural, container, and scalar output capacities are proved before stores. Numeric text views stay within the validated token. | Temporary strings and slices do not outlive the source call. Dynamic interface values are constructed through typed Go storage, and index results preserve their documented source lifetime. | `valid_differential_test.go`, `valid_bitmap_test.go`, `number_float_differential_test.go`, `number_rejection_contract_test.go`, `any_box_corruption_test.go`, `index_bitmap_test.go` | `BenchmarkValid`, `BenchmarkValidLarge`, `BenchmarkNumberCorpusParse`, `BenchmarkUnmarshalAnyLarge`, `BenchmarkBuildIndexBitmapIndent4` |
| Internal structural kernels: production files under `internal/kernels/` listed below | Load vector-width blocks and exchange compact Stage 1 and Stage 2 buffers through direct typed calls. | Full vector loads require a complete block; tail handling selects only complete in-range blocks. Output writes are capacity-checked by the caller or function precondition. Stage 2 constants and root-package entry layouts have compile-time agreement checks. | Kernels retain no source or output pointers after return, and all buffers remain ordinary Go allocations. | `internal/kernels/stage1_test.go`, `internal/kernels/stage1_index_test.go`, `internal/kernels/stage1_stream_test.go`, `valid_bitmap_test.go`, `index_bitmap_test.go` | `BenchmarkStage1Block`, `BenchmarkStage1Chunk32`, `BenchmarkStage2PositionsGo`, `BenchmarkValidLarge`, `BenchmarkBuildIndexBitmapIndent4` |
| Internal SIMD scanners: production files under `internal/scanner/` listed below | Load and store vector-width string spans behind direct root calls. | Full vector loads and stores are dominated by remaining-length checks. Copy entry points reject short or overlapping destinations before vector stores. | Scanners retain no source or output pointers after return. Buffers remain ordinary Go allocations and overlapping copies are rejected. | `internal/scanner/scan_test.go`, `internal/scanner/scan_simd_test.go` | `BenchmarkStringScannerASCII`, `BenchmarkCopyHTMLStringPrefixASCII` |
| Dense posting Boolean kernels: `internal/bitset/ops_simd.go`, `ops_dispatch*_amd64.go` | Apply two independent 256-bit `AND`, fused three-input `AND`, `OR`, or `AND-NOT` operations per loop without assembly. | Public wrappers first size every input/output window; the vector body runs only with eight complete words remaining, uses offsets within those slices, and hands the tail to checked scalar indexing. Exact input/output aliasing loads both vectors before either store. GOAMD64 v1/v2 checks runtime AVX2 support before calling a vector body; v3 requires AVX2. | Kernels retain no pointers. Sources and destination remain ordinary caller-owned slices visible to the collector for the call. Dispatch is a static-call branch rather than an indirect function value, preserving caller escape analysis. | `internal/bitset/ops_test.go`, `ops_dispatch_amd64_test.go`, portable/SIMD differential, AVX2-disabled subprocess, ISA disassembly, race, `-d=checkptr=2` | SIMD dispatch must preserve scalar results, aliases, bounds, and caller-buffered zero-allocation behavior. |
| Pointer-free collection metadata: `store_mapped_keys.go`, `store_mapped_docs.go`, `store_file.go`, `store_file_free_scratch.go`, `internal/storemem/block_mmap.go` | Keep immutable base keys, fixed row descriptors, the bounded durable reusable-extent arena, and free-fold planner scratch pointer-free and outside Go `HeapAlloc`, while reconstructing typed views without per-entry slice headers. | Allocation sizes, alignment padding, and every typed partition are overflow-checked before one block is allocated. Key controls use an eight-byte probe with seven repeated wrap bytes, one-based initialized slots, and an exact spelling recheck. Row offsets/counts are validated by `store.Open`; `FreeExtent`, `[2]int`, and `freeFoldSlot` contain no Go pointers, their slices stay inside their exact partitions, and no pointer is stored in external bytes. | Collection states, mapped chunks, and durable collections retain typed block owners. The free scratch block outlives every derived slice and closes only after publishers and I/O resources drain. Native loads finish before `runtime.KeepAlive`; returned values borrow the separately caller-owned image. Mapped owners use finalization as a resource backstop. Heap fallback platforms retain identical semantics. | `store_mapped_keys_test.go`, `store_persist_test.go`, `store_persist_mmap_unix_test.go`, `store_file_test.go`, `store_file_free_scratch_test.go`, `internal/storemem/block_test.go`, forced GC, concurrent reader/writer race, corruption tests, `-d=checkptr=2` | Resource accounting must separate Go heap, external arenas, mappings, cache, staging, free scratch, and caller output; caller-buffered reads and writes retain zero-allocation tests. |
| Store page I/O: `internal/storeio/ring_linux.go`, `device*.go`, `committer*.go`, `index_pool.go`, `page_checksum_simd_{amd64,arm64}.go` | Drive registered fixed-buffer page reads, writes, root-last data-integrity barriers, bounded automatic group commit, and pure-Go SIMD page checksums without cgo or Go-heap payload buffers. | Compile-time and Linux tests fix every UAPI structure size and critical offset. Kernel-returned ring offsets, dimensions, alignment, drop counters, overflow counters, completion tokens, file indexes, buffer indexes, lengths, and offsets are checked before use. Page writes are sorted, non-overlapping, and buffer-unique; the root cannot overlap data. Checksum loops prove a complete 64-byte AVX-512 or 16-byte 128-bit/arm64 window before each unaligned vector load; amd64 entry also requires exact AVX and PCLMUL feature bits. | One locked writer thread owns the ring. Kernel-retained data buffers are anonymous mappings outside the Go heap; Go-backed setup, probe, file, and iovec arrays survive their copying syscalls through `runtime.KeepAlive`. A registered buffer cannot be submitted twice or touched before completion. The single collection producer transfers batches through an atomic SPSC ring; 47-bit-versioned free lists prevent ABA while recycling fixed descriptors and buffers. Checksum kernels retain no input pointer. | `internal/storeio/ring_linux_test.go`, `internal/storeio/device_test.go`, `internal/storeio/committer_test.go`, `internal/storeio/superblock_test.go`, Linux architecture cross-builds, race, `-d=checkptr=2` | Setup is outside steady state. Uncontended batch acquisition/publication, fixed writes, completion, explicit durability waits, and caller-buffered checksum paths use bounded preallocated state with zero heap allocation; backpressure parks only at configured exhaustion. |

The race build, `-d=checkptr=2`, aggressive-GC lifetime tests, scalar/SIMD
differential tests, and corpus tests jointly enforce these invariants. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for the required commands.

## Complete production scope list

<!-- BEGIN GENERATED UNSAFE SCOPES -->
<!-- Generated by internal/cmd/unsafeinventory; do not edit this block. -->
- `internal/bitset/ops_simd.go` — `and3WordsAVX2`
- `internal/bitset/ops_simd.go` — `andNotWordsAVX2`
- `internal/bitset/ops_simd.go` — `andWordsAVX2`
- `internal/bitset/ops_simd.go` — `orWordsAVX2`
- `internal/storeio/adaptive_ordered_leaf_lab.go` — `adaptiveOrderedLeafLabOverlaps`
- `internal/storeio/block_selector.go` — `(*BlockSelector).Space`
- `internal/storeio/common_primary_leaf.go` — `commonPrimaryLeafOverlaps`
- `internal/storeio/global_tablet_catalog.go` — `globalTabletCatalogSlicesOverlap`
- `internal/storeio/index_term_leaf.go` — `(IndexTermLeafDirectBlockView).OneMaskWords`
- `internal/storeio/index_term_leaf.go` — `package scope`
- `internal/storeio/ordered_hash_leaf_lab.go` — `bytesOverlapOrderedHashLeafLab`
- `internal/storeio/page_checksum_simd_amd64.go` — `loadCRC32CBlock`
- `internal/storeio/page_checksum_simd_amd64.go` — `loadCRC32CBlock128`
- `internal/storeio/page_checksum_simd_amd64.go` — `pageChecksumAVX512`
- `internal/storeio/page_checksum_simd_amd64.go` — `pageChecksumPCLMUL8`
- `internal/storeio/page_checksum_simd_arm64.go` — `loadCRC32CBlock128`
- `internal/storeio/page_checksum_simd_arm64.go` — `pageChecksumPMULL4`
- `internal/storeio/page_checksum_simd_arm64.go` — `pageChecksumPMULL9`
- `internal/storeio/retired_interval_index.go` — `RetiredExtentStorageBytes`
- `internal/storeio/retired_interval_index.go` — `RetiredIntervalIndexStorageBytes`
- `internal/storeio/retired_interval_index.go` — `fixedStorageRangesOverlap`
- `internal/storeio/retired_interval_index.go` — `newRetiredExtentArenaIn`
- `internal/storeio/retired_interval_index.go` — `newRetiredIntervalIndexIn`
- `internal/storeio/ring_linux.go` — `(*Ring).RegisterBuffers`
- `internal/storeio/ring_linux.go` — `(*Ring).RegisterFiles`
- `internal/storeio/ring_linux.go` — `(*Ring).mapQueues`
- `internal/storeio/ring_linux.go` — `(*Ring).prepareFixed`
- `internal/storeio/ring_linux.go` — `(*Ring).prepareReadArena`
- `internal/storeio/ring_linux.go` — `(*Ring).requireOperations`
- `internal/storeio/ring_linux.go` — `(*Ring).useReadArena`
- `internal/storeio/ring_linux.go` — `ioUringRegister`
- `internal/storeio/ring_linux.go` — `ioUringSetup`
- `internal/storeio/ring_linux.go` — `package scope`
- `internal/storeio/ring_linux.go` — `u32At`
- `internal/storeio/route_table.go` — `(*RouteBucketTable).Accounting`
- `internal/storeio/route_table.go` — `package scope`
- `internal/storeio/sparse_document_page.go` — `slicesOverlap`
- `query/compiler.go` — `(*chunkArena[T]).firstChunk`
- `query/exec.go` — `FromFileOverlay`
- `query/exec.go` — `FromSegment`
- `query/exec.go` — `package scope`
- `store/durable/store_file.go` — `(*Collection).Stats`
- `store/durable/store_file.go` — `newCollectionResources`
- `store/durable/store_file_free_scratch.go` — `fileFreeScratchSlice`
- `store/durable/store_file_free_scratch.go` — `planFileFreeScratch`
- `store/segment.go` — `(*Segment).buildDoc`
- `store/segment.go` — `(*Segment).buildDocSchema`
- `store/segment_persist.go` — `(*Segment).openDocRecord`
- `store/segment_persist.go` — `(*persistWriter).writeEntries`
- `store/segment_persist.go` — `appendNarrow`
- `store/segment_persist.go` — `openEntries`
- `store/segment_persist.go` — `openSegmentIntoMode`
- `store/segment_persist.go` — `package scope`
- `store/segment_stream.go` — `(*Segment).buildDocPrefix`
- `store/store_document_template.go` — `storeOwnedDocumentEnd`
- `store/store_document_template_read.go` — `(*storeTemplateFieldHint).lookup`
- `store/store_document_template_read.go` — `(*storeTemplatePointerHint).resolve`
- `store/store_float64_reduce_simd_amd64.go` — `reducePackedFloat64LE`
- `store/store_float64_reduce_simd_arm64.go` — `reducePackedFloat64LE`
- `store/store_index_packed.go` — `newStorePackedIndex`
- `store/store_index_packed.go` — `package scope`
- `store/store_mapped_docs.go` — `(*Segment).NarrowAt`
- `store/store_mapped_docs.go` — `(*Segment).RawAt`
- `store/store_mapped_docs.go` — `newStoreMappedDocs`
- `store/store_mapped_docs.go` — `package scope`
- `store/store_mapped_keys.go` — `newStoreMappedKeysLayout`
- `store/store_mapped_keys.go` — `package scope`
- `store/store_owned_documents.go` — `(*Builder).compactDocuments`
- `store/store_owned_documents.go` — `copyStoreOwnedEntries`
- `store/store_owned_documents.go` — `newStoreOwnedDocuments`
- `store/store_zone.go` — `ZoneChunkBytes`
<!-- END GENERATED UNSAFE SCOPES -->
