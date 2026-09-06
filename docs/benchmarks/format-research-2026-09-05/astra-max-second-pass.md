# Astra max second pass: remove representation work as well as bytes

Research baseline: main `f05df25e8bebc13d9bfe11a2038bab43805f6c3d`, read through `/Users/thesyncim/GolandProjects/vibedb-space-savings-rf3`. The parent is separately implementing and qualifying rank-affine primary streams. This report proposes no production changes. I ran only source reads and a small offline byte-grammar model; no Go build, database workload, latency benchmark, or repository edit.

The first-pass reports under `docs/benchmarks/format-research-2026-09-05/` already cover complete group-root checkpoints with Raft as sole redo, implicit wave geometry, numeric streams, inline admission, medium-value slabs, inline overflow tails, and catalog geometry. I am not counting those again as new findings.

## Decision

The strongest additional small implementation is **parametric sealed-route blocks**, initially expanding into the existing route cache. It reduces authenticated index bytes and varint work without changing the ordinary primary read path, foreground wave encoding, redo ownership, or warm route lookup. On an explicitly regular 256-entry example, its route payload is **110 bytes instead of 1,751**, and cold metadata reads are **150 instead of 1,791 bytes**, with the same two `ReadAt` calls. This is an exact grammar budget, not a measured database result. It is a modest whole-storage saving and requires favorable entry packing.

The larger additional primary idea is **same-key immutable overflow-suffix reuse**. For a 1 MiB equal-length value whose change lies in the first overflow piece, each materialized replacement could mint **65,536 instead of 1,052,672 bytes**. The current live value still occupies the same number of extents; savings are in newly written bytes and simultaneously retained versions. Ownership changes, cache residency, and migration make this a separate experiment, not a safe local validator tweak.

Neither should be added to the parent's current rank-affine qualification branch. Neither proves zero latency regression from design alone. The existing 64 KiB full-replacement RF3 fixture is particularly poor coverage for both proposals: it has no reusable overflow suffix, and large Raft entries receive dedicated extents.

## Archived prototype status

The companion `sealed-route-prototype.patch.txt` is an uncompiled, untested
research artifact from this pass. It is preserved to make the design
reviewable, but no files under `internal/raftstore/seglog` from that prototype
are part of the production candidate. It must not be applied, benchmarked as
qualified, or described as merged without a separate format-marker,
recovery, corruption, cold-read, warm-read, and sealing-cost qualification.

## 1. Parametric sealed-route blocks

### What exists

`internal/raftstore/seglog/sealed_index.go:20–26` defines a 64-byte index header, 40-byte route descriptor including its 16-byte MAC, and blocks of at most 256 entries. The group directory already supplies implicit contiguous Raft indexes. The descriptor ordinal is `(index - RouteFirst) / BlockEntries`.

`appendRoutePayload`, at `sealed_index.go:536–580`, nevertheless emits for EVERY entry:

- one flags/type byte;
- one signed term delta varint, normally zero;
- absolute within-extent data offset and data length varints;
- on a new physical extent, its offset delta, extent length, extent ID, and 16-byte WaveID.

The exact current body-size equation is:

`B_old = sum_entries[1 + U(term_delta) + U(data_offset) + U(data_length)] + sum_extents[U(extent_offset_delta) + U(extent_length) + U(extent_id) + 16]`.

`U(x)` is unsigned-varint length. Terms outside signed-delta range use the existing absolute-term escape, which the bounded examples below do not need.

`LazyRouteReader.Point`, `lazy_reader.go:133–219`, already identifies the correct block directly. On a cache miss it reads the 40-byte descriptor and the entire route payload, verifies its MAC, decodes **every** entry into scratch, validates every extent range, and copies the block into the existing bounded cache. Warm hits return an already decoded entry. A Go route entry occupies 72 bytes on the current 64-bit layout (`sealed_index.go:255–261`): a full block is 18,432 bytes in the decoded arena, plus the separate decode scratch.

`NodeStore.packWaveExtents`, `internal/raftstore/node_store.go:1358–1423`, packs whole entry payloads into 32 KiB target extents, paying one AEAD tag per extent. Large entries get dedicated extents. The opportunity is regular small commands packed together, not arbitrary bytes that happen to compress well.

### Proposed grammar and admissibility

Keep the descriptor width and authentication context. Use an explicitly versioned index grammar with a descriptor mode in currently reserved bytes `20:24`. Those bytes are covered by the descriptor MAC today (`sealed_index.go:675–726`). Mode 0 retains the existing payload. Mode 1 is eligible only when:

1. Every entry has the same nonzero term and the same valid type.
2. Entries partition into at most 16 contiguous physical-extent runs.
3. Within a run, extent offset/length, extent ID, and WaveID are exactly equal; data length is constant and positive; and data offsets are exactly `base + ordinal * data_length`.
4. Extents obey today's ascending, non-overlapping physical ordering. Every derived entry remains inside the authenticated descriptor and group-run envelope.
5. The mode is strictly smaller than the old encoding. Any failed proof uses mode 0.

Mode-1 payload:

`U(term), type:u8, U(run_count)`

For every run:

`U(entry_count), U(extent_offset_delta), U(extent_length), U(extent_id), WaveID[16], U(first_data_offset), U(data_length)`.

Do not infer a WaveID from the directory's latest retry identity: a route block can contain older waves, and AEAD AAD uses the original WaveID plus extent ID. Each run retains that exact identity. Zero-byte entries, mixed terms/types, irregular slices, and blocks with too many extents use the old grammar.

The sum of run counts must equal the descriptor's exact entry count. Use checked multiplication/addition before deriving each run's last data end. Require canonical varints, nonzero counts, maximal same-extent runs, no trailing bytes, and zero unknown mode bits. This preserves corruption semantics instead of accepting a merely plausible compressed block.

### Exact source-derived byte budget

The offline model is `/private/tmp/vibedb-astra-max-second-pass-probe.py`, with JSON at `/private/tmp/vibedb-astra-max-second-pass-probe.json`. It spells the existing append grammar directly and sizes the proposed grammar. It does not call VibeDB. Examples use term 7, type 0, first physical extent at offset 4,096, extent IDs starting at 1, a 16-byte WaveID, a 16-byte AEAD tag, and the current 32 KiB packing target. Partial first/last extents are permitted by the proposal but are not needed in this table.

| Entries × entry payload | Extent runs | Current route payload | Proposed payload | Bytes removed | Current → proposed cold metadata bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 × 512 B | 1 | 437 | 29 | 408 | 477 → 69 |
| 256 × 300 B | 3 | 1,708 | 82 | 1,626 | 1,748 → 122 |
| 256 × 512 B | 4 | 1,751 | 110 | 1,641 | 1,791 → 150 |
| 256 × 1,024 B | 8 | 1,839 | 218 | 1,621 | 1,879 → 258 |
| 256 × 65,536 B | 256 | 7,552 | ineligible | 0 | unchanged |

The 256 × 512 case eliminates **754 varint decodes** on each uncached block: `3N + 3R = 780` becomes `2 + 6R = 26`. It also removes 255 repeated type/flags reads. It still constructs the same 256 route entries in the first implementation; do not call that first patch O(1) cold decoding.

At 100,000 entries in separately constructed regular 256-entry blocks of this example, the model totals 683,979 old versus 42,983 proposed route bytes: **640,996 bytes/node**, or **1,922,988 bytes across RF3 (1.834 MiB)**. This synthetic figure is not measured production coverage; actual physical offsets change varint widths and actual submission batches can fragment runs.

### Data bytes, physical allocation, and actual work

This changes only the sealed index. It does not reduce the document bytes in Raft wave frames, the wave headers/MACs, the primary, the collection redo journal, or active/spare reserves. It does not change replicated command/network bytes. RF3 multiplies absolute index savings by three, not the whole-database percentage.

The benefit is not merely unused room inside a fixed 32 MiB file. The sealer truncates the frozen file to its actual data length, writes the index and footer, and syncs (`seglog/engine.go:2034–2053`). The sealed file's required length is exactly `data + index + footer` (`engine_sealed_index.go:55–85`). Smaller indexes therefore reduce logical EOF and can reduce allocated blocks. Under ideal 4 KiB rounding, the per-file block difference differs from the index-byte saving by less than one block. Actual `st_blocks` remains the metric on the real filesystem. Active and spare allocation certificates/reservations are unchanged.

The writer runs only during rotation/control (`engine_sealed_index.go:266–398`). No eligibility work needs to be inserted into `persistWave` or the ordinary primary mutation path. Collect the bounded run proof while the sealer already converts active locations into route entries; do not construct both full encodings and then discard one. The admitted mode emits and copies fewer bytes and hashes fewer bytes. Irregular blocks add some proof comparisons before fallback, so seal service time and rotation stalls still need qualification.

The smallest decoder change expands runs arithmetically into today's `r.decode`, then leaves the existing validation/cache copy and warm cache-hit branch unchanged. It removes varint and HMAC input work and lowers requested read bytes, while preserving exactly two metadata read calls and the existing read/cache ownership contract. It introduces no additional sync, retry authority, GC fence, or COW lifetime.

A later parametric cache could calculate an entry directly from the run. I would **not** include it in the first patch: multiple runs require an ordinal-to-run mapping or search, and warm hits would exchange the current direct array access for extra arithmetic/lookups. A smaller in-memory footprint may win, but that is a separate measured tradeoff.

### Recovery, versioning, first patch, and falsifiers

Use a new sealed-index format marker specific to this grammar; do not blindly change the shared `canonicalFormatMarker`, which also versions unrelated records. `unmarshalSealedIndexHeader` currently demands the exact marker (`sealed_index.go:190–203`). New readers can support old and new sealed indexes. Old readers must reject a new index during sealed metadata open, not discover an unknown mode only on a later point lookup. Keep mode bits inside the existing MAC and the header version inside authenticated sealed top metadata.

No existing sealed file is rewritten in place. New indexes are produced through the existing frozen/pending-seal publication. After an interrupted seal, existing authenticated metadata and exact-inode rules still decide what can be regenerated; mixed grammar retries must not accept a different published index identity. Full `DeepVerify` must continue comparing every decoded route to the original replayed frames (`engine_sealed_index.go:212–263`). Group summary and retry semantics remain unchanged.

The first patch surface is limited to `sealed_index.go`, `engine_sealed_index.go`, and the cold decoder dispatch in `lazy_reader.go`, plus grammar/roundtrip/recovery tests. Keep the current cache representation. Add exact tests for clipped `RouteFirst/RouteLast`, partial blocks, overflow arithmetic, tampered mode/count/extent/WaveID, zero-byte entries, mixed terms, and mode-0 fallback. Existing `lazy_reader_test.go:39–62` already counts metadata calls/bytes and warm-cache behavior.

Falsification workload: real current command lengths and actual wave grouping, plus the exact regular cases above; mixed sizes/terms/types and one-entry waves as negative controls. Measure eligible blocks, saved bytes, sealed `st_blocks`, cold `MetadataBytes`, allocations, cold and warm point latency, sealer CPU/service time, and rotation admission stalls. Preserve a read-heavy term/entry workload and an RF3 follower catch-up workload. Reject a version that improves the ideal codec case but makes common fallback blocks or rotation stalls worse. No such qualification was run in this research pass.

## 2. Same-key immutable overflow suffix reuse

### The format restriction is artificial, but ownership is real

Overflow pieces contain a 64-byte common header, 60-byte overflow header, and 8-byte trailer. At default 64 KiB maximum extents, each full piece carries 65,404 value bytes. A 32-byte head reference lives in the leaf (`store_file_overflow.go:24–69`; `internal/storeio/overflow_page.go:9–60`).

The existing link rule requires both `Next.Generation <= current.Generation` and `Next.LogicalID > current.LogicalID` (`overflow_page.go:100–139`). A new prefix naturally has higher logical IDs than an old suffix, so it cannot currently adopt that suffix even when every suffix byte is identical.

A versioned rule can instead require:

`Next.Generation < current.Generation || (Next.Generation == current.Generation && Next.LogicalID > current.LogicalID)`.

Keep all existing kind, store, physical-bounds, generation, and logical-ID checks. This gives a strict ordering: generation decreases, or logical ID increases within one generation. A cycle is impossible because a cycle could not contain any decreasing-generation edge, and its remaining edges would require strictly increasing IDs. An old-format suffix already satisfies this rule. Complete-chain validation must also prove identical Total and contiguous Offset at every piece.

Share only the unchanged suffix of successive equal-length versions of the **same key**. Never deduplicate across distinct keys, never share a volatile-only suffix in the initial experiment, and never infer equality from CRCs. Exact `bytes.Equal` over already verified old bytes is the comparison. For an initial conservative implementation, only adopt a suffix whose complete refs lie inside the flushed durable state's bounds and generation; skip unflushed materialization candidates without forcing a flush. This avoids introducing new persistence dependencies just to raise coverage.

### Existing replacement work gives a comparison opportunity

Today `collectPrimaryOverflowExtents` acquires and opens every complete old extent before a replacement/delete retires it (`store_file_overflow.go:485–519`). On a cache miss, `PageCache` reads **ref.Length**, not merely the header (`page_cache.go:1050`); `validateLoadedPage:1104–1113` checks the complete page and exact StoreID, length, logical ID, generation, and kind. The collector also calls `OpenOverflowPage`, which validates the whole checksum again. The old payload is therefore already available during this walk.

In that same walk, compare the new value slice at the old piece's exact offset. Remember the last differing piece; everything following it is the reusable suffix. No extra logical page acquisition or second disk pass is necessary for this proof. Strengthen this collector to the resolver's existing full-chain Total/Offset checks (`store_file_overflow.go:423–477`): the current collector only accumulates data lengths against the first Total, which is insufficient evidence for sharing arbitrary pieces.

The extra comparison does cost CPU. Fully changed pieces may fail `bytes.Equal` immediately, but an adversarial change in the last byte of every piece adds almost a full-value comparison and then yields no reuse. Bound initial eligibility to savings of at least one full 64 KiB extent; do not scan unrelated candidates on inserts or deletes. This threshold would deliberately exclude the small 64 KiB first-piece-only opportunity.

### Exact primary-byte budget

For document length D, `A = 65,404`, `m = ceil(D/A)`:

`E(D) = (m-1)*65,536 + 4,096*ceil((132 + D-(m-1)*A)/4,096)`.

If piece k is the last changed piece, mint pieces 0..k and adopt the exact old tail. The physical bytes avoided per materialized replacement are the rounded extents after k, not merely their payload bytes.

| Canonical document | Pieces | Current whole-chain bytes | New bytes if only first piece changes | Avoided bytes | RF3 avoided writes per materialized version |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 KiB | 2 | 69,632 | 65,536 | 4,096 | 12,288 |
| 128 KiB | 3 | 135,168 | 65,536 | 69,632 | 208,896 |
| 1 MiB | 17 | 1,052,672 | 65,536 | 987,136 | 2,961,408 |
| 4 MiB | 65 | 4,206,592 | 65,536 | 4,141,056 | 12,423,168 |

Any change in the final piece yields zero suffix reuse. The current RF3 fixture constructs 65,536-byte documents with a repeated payload character determined by the cycle (`cmd/vibedb-shard/wal_retention_process_qualification_test.go:234–243`). Replacements of the same key advance the cycle by 32 (`node_space_process_qualification_test.go:190–194`), changing that character in the final piece. Its expected suffix benefit is **zero**. An early-field-only counterfactual with constant final bytes saves at most 4 KiB per materialized version before any eligibility threshold; a late-field change saves none.

This does not shrink the one current live chain: it still has exactly E(D) bytes. If Q complete versions remain reachable/fenced, full reminting uses Q*E(D) overflow bytes, while a permanently shared tail with P newly minted prefix bytes per version uses `E(D) + (Q-1)*P`; the graph union saves `(Q-1)*(E(D)-P)`. Real allocator floors can conservatively retain other pages, so this is a reachable-graph budget, not a promised file plateau. Existing free holes are not automatically punched/truncated. Measure live, retired/fenced, reusable, apparent, and allocated bytes separately.

Also distinguish materialized versions from logical updates: buffered mutations currently create memory-only frames, and several updates can fold into one physical checkpoint. Do not multiply the table by every logical update to claim disk-write savings. Raft command bodies, full-value redo records, transaction markers, and fixed sidecar/reserve geometry are untouched.

### Ownership proof and required implementation changes

At publication, the new current version becomes the tail's current owner. The old prefix retires; the tail does not. Old snapshots may still reach it through old prefixes. When a later full replacement/delete finally stops the current key from reaching that tail, retire it once at that later checkpoint base generation. The existing floor `min(reader generation, fallback-root generation)` then protects all older snapshots and alternate roots (`store_file_free.go:87–117`; `appendPrimaryRetirement`, `store_file_primary_mutation.go:3219–3235`). Refcounts are unnecessary only under this same-key, one-current-owner invariant. Cross-key sharing would need a different ownership design.

The current code is **not** ready for mixed chains:

- Batch retirement classifies the entire chain from only its head offset (`store_file_primary_batch.go:1480–1524`). A volatile new prefix followed by a durable old tail breaks that assumption. Classify every retired ref, exclude the retained suffix, and bound both retirement queues before publication.
- The single-mutation paths use the same head-only classification (`store_file_primary_mutation.go:1380–1419`, and the nonbuffered retirement path around 1627). Even if they do not create shared chains initially, they must safely replace/delete them.
- Batch overflow planning assigns a full new chain before old-leaf retirement inspection (`store_file_primary_batch.go:1032–1092`; staging order `620–738`). A small experiment can keep those virtual address/logical-ID reservations, attach a reuse certificate during the existing leaf/old-chain walk, admit only the new prefix, and reduce exact dirty-frame/retirement counts before `ensurePrimaryBatchCapacity:1115–1240`. Unused virtual reservations have no physical backing and are no worse than the baseline address high-water. This does not require rendering the leaf a second time because its planned head identity stays fixed.
- Admission (`store_file_primary_batch.go:1545–1570`) must record only newly created prefix frames in rollback bookkeeping. An aborted batch must not unadmit the old tail or publish its retirement list. The existing stage-before-journal-before-router-publication order stays intact.
- Checkpoint materialization currently resolves an entire volatile chain, walks it again to collect all refs, and remints every piece (`store_file_primary_mutation.go:2261–2290`, `2353–2400`). It must instead copy/remint only the volatile prefix and graft its last link to the existing durable tail. The exact old tail is already an unchanged graph edge; it needs no new WAL reference or redo protocol. Collect only volatile prefix refs for post-publication cache retirement.

A single prefix visitor should replace the current resolve-plus-collect double walk. Even on a full-remint fallback it removes one complete set of cache acquisitions and `OpenOverflowPage` validations per materialization. This is a concrete operation-count reduction; it is not sufficient by itself to offset comparison cost on every workload.

`primaryCheckpointBaseState` may be newer than the flushed durable state because snapshots can materialize without a flush (`store_file_primary_mutation.go:1895–1908`; `store_file.go:475–491`). Do not equate “below checkpoint FileEnd” with “already synced” when defining initial sharing eligibility. Continue using the checkpoint base for existing retirement classification; use the stricter flushed state to authorize the first sharing experiment, without introducing a forced flush.

### The residency caveat prevents a zero-regression claim

No extra logical acquisition is not the same as no extra physical I/O. Baseline replacements encode the entire new value as buffered-dirty frames, which remain resident until materialization. With suffix reuse, a clean durable tail can be evicted. A later replacement may therefore reread that tail from disk even though both versions execute the same number of logical `Acquire` calls. A smaller working set may improve matters overall, but the source does not prove it.

Explicit tail leases until materialization could preserve baseline residency, but they add pin accounting/rollback/lifetime work and give up much of the potential RAM saving. Do not silently add them to justify a performance guarantee. I rank this design behind the sealed-route change for implementation risk, and behind the parent's active primary codec experiment for immediate qualification.

Full document reads still follow the same number and sizes of forward-linked extents and copy the same canonical bytes. They introduce no patch replay or extra level of indirection. The new prefix and old tail may have worse physical locality than a freshly allocated whole chain, so cold sequential/full reads must still be measured. Inserts retain the old full-chain path. Narrow updates can avoid clearing, copying, checksumming, dirty-cache admission, checkpoint allocation, and writing of the retained suffix; fallback updates pay comparison work without these savings.

### Format adoption and falsifiers

The new overflow rule needs an explicit page grammar and a store-wide incompatible-format decision. Old overflow pages remain readable by the new decoder; old readers reject new pages. A page-level version alone only fails lazily on access.

Merely bumping the NEWEST root's version is also insufficient for old-writer rejection: `orderedInlineSuperblocks` at `inline_superblock.go:636–673` discards undecodable candidates and accepts the other slot. An old build could select an older root. A supported migration requires a gate that old builds already enforce, or restart-settled conversion of both root slots before activation. The development format currently promises only exact-writer-build reads (`docs/format.md:5–6`). For the first experiment, use a fresh isolated format/store and preserve the baseline; do not imply a rolling upgrade or silently mutate an existing store.

Falsification matrix: D around 65,404, 65,536, 128 KiB, 1 MiB, and 4 MiB; first/middle/last-piece changes; last-byte change in EVERY piece; variable lengths; repeated updates before a flush; updates alternating with snapshots; long-lived readers; cache capacity below two full values; eviction pressure from other collections; single and batch replacements/deletes; crash before the journal decision, after decision before publication, during physical remint, and around both root syncs. Verify every acknowledged canonical value and every pinned old snapshot; verify no retired extent intersects any reachable current/old tail; count overflow physical writes, cache reads, dirty bytes, retirement pressure, checkpoint count, and allocated blocks.

Keep current RF3 full replacement as an explicit zero-saving negative control, then add same-size early-field updates as a separate workload. Reject the design if fallback tails, extra physical reads, or checkpoint pressure violate the no-slower requirement. No suffix implementation or benchmark was performed in this pass.

## 3. Rejected shortcuts

- Directly point primary values into encrypted Raft extents: this adds decryption/extent lookup to foreground document reads and pins log history to live-value lifetime. The first-pass complete-root/Raft-sole-redo design targets duplicate redo without that read dependency.
- Reuse overflow pages across unrelated keys: current reclamation has exclusive chain ownership and no per-page current-owner counts. Equal bytes do not establish lifetime safety.
- Keep newly minted volatile addresses as durable locations to avoid reminting: intermediate updates reserve virtual append ranges that are intentionally memory-only. Persisting that address history can expand file bounds and defeats allocator reuse. Zero-copy is not automatically space saving.
- Add a compressed route cache in the same first patch: it trades warm direct indexing for run selection and arithmetic. Keep this distinct from the safe-to-compare on-disk grammar experiment.
- Claim suffix reuse makes the 64 KiB RF3 replacement fixture smaller: its final payload piece changes. It is a negative control.
- Claim a 94% route-payload reduction is a 94% Raft or database reduction: data, other metadata, redo, primary, and reserves remain.

The result of this second pass is two concrete additional formats with bounded patch surfaces and explicit counterexamples. It supports pursuing the sealed-route experiment after the current rank-affine qualification; it does not justify widening that qualification or merging either proposal without its own evidence.
