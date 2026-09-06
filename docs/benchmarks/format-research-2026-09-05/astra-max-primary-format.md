# Primary-format research: structural savings without assuming latency equivalence

Repository: /Users/thesyncim/GolandProjects/vibedb-space-savings-rf3.
Final source anchor: task HEAD 809f96dc4581ded1852de755dc963de3effd76ef, rebased onto origin/main f05df25e8bebc13d9bfe11a2038bab43805f6c3d.
Read-only researcher; I did not edit repository source, run builds/tests/benchmarks, or spawn agents. The parent ran the boundary census and subsequently reproduced/fixed the numeric-parser bug cited below. Source line numbers refer to the final inspected checkout, except explicitly retained historical measurement output; the subsequent parent fix can shift line numbers.

## Recommendation

Prototype a **certified affine stream indexed by the physical row rank**, initially as a codec-only research experiment. This attacks a real representation redundancy and replaces bounded delta replay with direct arithmetic. It is the strongest small, tractable primary-format change with a plausible improvement in both space and point-read CPU. Do not turn on its production emitter until projection, native count/group operations, mutation patching, and malformed-page validation understand its row domain.

The **512-byte overflow boundary** is the much larger space opportunity for ordinary medium-sized documents. It is now demonstrated by the parent's census, not conjecture. A 513-byte document can consume an entire 4-KiB overflow extent even when its inline representation would take a few bytes. However, simply raising InlineValueBytes changes mutation, scratch, cache, and checkpoint work and does not satisfy the latency requirement by itself. It needs either a bounded adaptive inline policy with an inexpensive raw representation or a separate immutable packed-value design.

Adaptive root/path geometry is the next low-complexity structural target: the current 64-KiB catalog root and mandatory intermediate routing pages impose 96 KiB of graph metadata even on one small tablet. Its absolute saving is useful across many collections but is not the dominant replicated-SQL budget while 16-MiB sidecars remain.

No latency equivalence is established for any proposal. The retained 48.6% RF3 node-log saving failed its paired latency qualification, and its accounting excluded primary files. Multiplying a primary saving by three is valid for RF3 bytes saved, but does not multiply its percentage of the whole database.

## 1. What the production primary actually stores

Older comments and class-5 benchmark names are misleading if treated as the current grammar. Production uses class 6, VCS1; the old stable cuckoo envelope is not an additional 5 B/row layer under VCS1.

- Production class: internal/storeio/common_primary_unified_leaf.go:54.
- Max primary extent 65,536 bytes; max key 256 bytes: internal/storeio/common_primary_leaf.go:24.
- Max unindexed stripe 4,096 rows, payload header 40 bytes, shape header 16 bytes: internal/storeio/compact_primary_stripe.go:11.
- An exact secondary index selects leaves capped at 256 rows because posting slots are uint8: internal/storeio/primary_graph.go:71 and :108.
- Common page is 64-byte identity header plus 8-byte CRC32C/complement trailer: internal/storeio/page.go:9. InitPage clears the full physical page (:124); sealing hashes the complete prefix including padding (:167).
- Each leaf is rounded to a 4-KiB quantum. The bulk planner chooses the largest fitting row prefix and sizes the actual encoded payload; internal/storeio/primary_graph.go:491 and :529. Primary geometry is already variable in 4-KiB steps, not uniformly 64 KiB.

Let N be leaf rows, S inline shapes, O overflow rows, h_s holes in shape s, T_s static template bytes, K encoded key stream bytes, U optional posting-slot bytes, Q optional summary bytes, and B=ceil(N/64).

The exact payload equation, excluding overflow-owned extents, is:

P = 40 + 4*S + K + U
    + (O>0 ? ceil(N/8)+32*O : 0)
    + ceil(N*w/8) + 2*S*B
    + sum_s [16 + 8 + 4*(h_s+1) + T_s + sum_h C(s,h)]
    + Q

w = bits.Len(S-1) without overflow, bits.Len(S) with overflow.
U is zero for ordinary unindexed stripes and identity posting ordinals; otherwise it is N bytes, for <=256-row leaves.
Physical extent E = 4096*ceil((72+P)/4096).

These terms come directly from compact_primary_stripe.go:122-350 and :1157-1253. The shape directory and template segment ends are u32; scalar dictionary ends are already u16 despite an obsolete u32 comment in compact_stream_codec.go:107.

Each shape groups rows having exactly the same canonical static skeleton. Its holes are encoded as separate scalar streams in shape-local row order. Shared JSON keys/punctuation are consequently stored once per shape per leaf, but corresponding fields in different shapes are separate streams. Scalar static prefixes/suffixes are already factored by alphabet and prefix-integer codecs.

Stream costs, with n values and b=ceil(n/64), are:

| Codec | Exact framing/data structure |
| --- | --- |
| Dictionary | 12 + 2*D + sum(dictionary spelling lengths) + ceil(n*ceil(log2 D)/8) |
| FOR integer | 12 + 8-byte minimum + ceil(n*bitwidth(max-min)/8) |
| Date | 12 + 4-byte base day + packed day offsets |
| Delta | 12 + 4*b restart offsets + sum(block 8-byte first value + zigzag-varint deltas) |
| Delta-pack | 12 + 4*b + sum(block 8-byte first value + 1-byte width + packed deltas) |
| Linear prefix-integer | 12 + 4 dictionary-end bytes + prefix/suffix bytes + 18-byte flags/width/base/step body |
| Alphabet | 12 + dictionary/affix bytes + 4*b restart offsets + per-block minimum length, length width, packed length deltas, packed characters |
| Front | 12 + 4*b restart offsets + full restart spellings + per-value prefix/suffix lengths and suffix bytes |

Sources: internal/storeio/compact_stream_codec.go:99-139, :345-391, :415-568, :570-683, :833-978.

The planner is not a zero-cost compression switch. It measures dictionary, front and alphabet; parses canonical integers/dates/prefix integers; builds competing numeric encodings; then selects. Small dictionaries may be retained up to 25% above the minimum because packed-ID scans are faster. Sources: compact_stream_codec.go:147-223. Removing this preference to report fewer bytes would knowingly discard a read-performance policy.

## 2. Measured byte budgets and the limits they imply

Retained evidence:
docs/benchmarks/space-rf3-2026-09-05/compact-breakdown.txt:3-17 and primary-baseline.txt.
These are 100,000-row, approximately 248.8-byte logical documents, bulk built without secondary indexes.

| Bytes/row | Repetitive | High cardinality |
| --- | ---: | ---: |
| Entire primary file | 10.199 | 69.304 |
| Leaf extents | 9.052 | 68.157 |
| Leaf payload | 8.956 | 68.017 |
| Page framing | 0.018 | 0.075 |
| Extent slack | 0.078 | 0.066 |
| Key stream | 0.009 | 0.040 |
| Shape codes/rank checkpoints | 0.344 | 0.349 |
| Templates | 0.124 | 0.515 |
| Scalar stream headers/directories | 0.314 | 0.605 |
| Scalar dictionary spellings | 0.681 | 0.617 |
| Scalar stream data | 7.459 | 65.787 |

Whole-file minus leaf extents is 114,688 bytes in both cases: 16 KiB mutable prefix + 96 KiB routing graph. It is a mostly fixed small-store tax in these two examples, not 1.147 bytes of irreducible per-document data.

High-cardinality strings are pseudorandom lowercase letters, not arbitrary incompressible bytes. Generator: internal/benchcorpus/corpus.go:47-49, :68-83, :127-132. Alphabet streams spend 62.109 B/row; a 26-letter alphabet needs log2(26)=4.70044 bits/character but the codec uses 5. Even an ideal entropy coder can recover only 0.29956 bits per random letter before length/framing costs. One cannot recover another 40-50 B/row here by shrinking headers. An entropy codec would also introduce new decode/patch CPU and invalidate the requirement until measured.

Conversely, the corpus carries deliberate cross-field structure: Key(i)="doc:%08d", id=i, name="user-"+i, active from i%3, and deterministic tag spellings. The tiny key stream already recognizes the arithmetic key series. It would be wrong to attribute remaining ID/name bytes to irreducible information.

The 10.2/69.3 B gates are not generic production-document budgets. They exclude overflow, exact indexes, journals, and maintenance history. The indexed primary has a hard floor of 4096/256=16 B/row even for an arbitrarily compressible full leaf, before exact-index bytes.

## 3. First prototype: affine values in physical row order

### The redundancy

The benchmark's arrays contain two, three or four tags, creating three different shapes. Grouping by shape scatters id/name into gappy subsequences even though both are arithmetic in the original physical row rank. Current per-shape encoders then emit delta-pack/prefix-integer streams:

- Repetitive: delta-pack 0.854 + prefix-int 0.864 = 1.718 B/row.
- High-cardinality: 0.895 + 0.936 = 1.831 B/row.

These are the codec totals, not a directly instrumented per-path attribution; inspection of the generator/typed candidates identifies id/name as the intended targets. A prototype must print per-path winners to confirm the entire attribution before claiming the saving.

Current point decode can replay up to 63 integer deltas for each such scalar: compact_stream_codec.go:1721-1759. The enclosing value renderer already knows both physical row and shape ordinal: compact_primary_stripe.go:1711. There is no need to discover physical rank by decoding an inverse shape map.

### Proposed grammar

Use an explicit new stream kind, not a silently inferred schema relationship or an overloaded current flag an old decoder might accept.

A particularly clean byte-neutral descriptor uses:
- ordinary 12-byte stream header;
- exactly two dictionary entries for exact prefix/suffix spellings;
- same 18-byte body as linear prefix-integer: flags, optional decimal padding width, base int64, step int64;
- for this kind only, header count is the enclosing leaf row count N, and input coordinate is physical row rank r;
- decoded numeric component is base + step*r.

Regular streams keep header count n_s and consume shape ordinal. Leaf admission requires rank-affine count==leaf.rows, ordinary count==shape.rows. Document this rank-domain distinction explicitly. A 34-byte descriptor plus affixes per shape/hole can replace a row-proportional stream; it need not store a rank array, inverse permutation, or another key copy.

Keeping count=n_s instead is possible, but requires extra contextual endpoint validation and a distinct appendRankValue API. It is easier to accidentally treat a physical rank >=n_s as out of bounds. Choose one model consistently; never use positional inference from an ordinary PrefixInt descriptor.

For a native signed-integer subtype, a 16-byte base/step body and no affix directory could be smaller and preserve native integer proof. That is a useful second step; the first prototype can limit eligibility to existing nonnegative prefix-integer semantics to reduce proof surface.

### Builder proof and cost

At compact_primary_stripe.go:334, both values[] and shapeRows[] are available. shapeRows is already the array of original physical ranks constructed at :187-203.

1. Ignore empty/single-row shapes initially; constant values already have excellent dictionary representation. Ignore cases with equally spaced shape ranks that already get an equally small local linear PrefixInt, unless measurements justify a tie policy.
2. Parse the first two values using checked decimal arithmetic and identical prefix/suffix semantics. Let their numeric components be v0,v1 and physical ranks r0<r1.
3. Require (v1-v0) mod (r1-r0)==0; set step=(v1-v0)/(r1-r0).
4. Derive base=v0-step*r0 with checked multiplication/subtraction. Require base>=0 and checked base+step*(N-1) in [0,MaxInt64]. This conservative whole-leaf-domain check also bounds all intermediate row values because the function is monotone.
5. Verify EVERY selected shape row satisfies numeric value==base+step*r_i. Verify every original spelling is reproduced exactly by the chosen prefix/suffix/padding rule. Never infer identity because a field is named id or a table declares a primary key.
6. Admit only when this descriptor wins the exact byte/representation policy. Preserve existing small-dictionary scan preference. Failed admission returns to existing codecs.

Fold the check into the existing encodePrefixInt parsing work (:833-978) or reuse already parsed canonical integer values. Do not add an unconditional second full scan of every string column. A successful candidate may permit skipping discarded dictionary/alphabet/delta builds, but an eager preflight that fails late can regress inserts. Measure both eligible and adversarially almost-affine fields. An initial conservative shortcut only for sufficiently large nonconstant numeric streams is easier to reason about than bypassing every existing candidate.

Arithmetic proof must precede multiplication. Do not rely on wraparound followed by comparison. The inspected parseCompactPrefixInt check at compact_stream_codec.go:815 was insufficient: decimal 46000000000000000000 wraps uint64 to 9106511852580896768, which is still >= the previous 4600000000000000000 and <=MaxInt64. The checked pattern in compact_primary_integer_groups.go:64, value > (limit-digit)/10 before multiply, is appropriate. After I flagged this issue, the parent independently reproduced actual corruption in TestCompactPrefixIntOverflowDeclinesWithoutChangingValues: ticket:46000000000000000000 became ticket:09106511852580896768. The parent reports fixing the parser with a pre-multiply (MaxInt64-digit)/10 guard and passing focused TestCompact(Stream|Prefix|Projection) tests (1.320 s), with before/after evidence under docs/benchmarks/format-research-2026-09-05/prefix-overflow-{before,after}. This is a reproduced and separately fixed correctness bug; it does not establish end-to-end latency qualification for any proposed format.

Spelling cases needing explicit treatment:
- Fixed-width leading zeros belong to prefix/string semantics; preserve every width exactly.
- Bare canonical JSON integers do not admit leading zeros, plus signs, -0, fractions, exponents or a changed canonical spelling.
- A constant "-" prefix over magnitudes needs a stricter native-integer proof: magnitude zero would reconstruct "-0"; MinInt64 magnitude exceeds existing positive PrefixInt range. Initially decline these edge cases or support a separately checked signed subtype.
- Keep arbitrary non-digit affixes byte-exact, including JSON quotes/escapes; do not assume all prefix integers represent JSON numbers.
- Descending affine streams are valid values; unsigned range assumptions and equality inversion must not mistake them for ascending ones.
- Constant-step zero and one-row cases should keep an explicit canonical representation policy rather than letting an arbitrary fitted slope leak into the grammar.

### Every consumer that must understand the coordinate

| Path | Integration |
| --- | --- |
| Point/full row | compact_primary_stripe.go:1641 and :1711: pass physical row to rank kind, local ordinal to existing kinds. |
| Full scan | compact_primary_scan.go:254-305 already receives row,shape,ordinal. Direct arithmetic avoids resetting a local sequential decoder on gaps. |
| Scalar projection | compact_primary_projection.go:911-973 already knows physical row. Update compactProjectionFieldAt/:134 length proof/:480 prefix rendering so native integers and bounded scratch remain available. |
| Integer grouping/SUM | compact_primary_integer_groups.go:389-406 has both coordinates; update stream exactness admission and read helpers. Do not make the entire query fall back. |
| Single-hole reads | compact_primary_stripe.go:2199 must select the correct coordinate. |
| Counts/filter/extrema | compact_primary_stripe.go:1874-2194 processes one shape at a time. A plain stream count over N rows would count rows belonging to other shapes and is wrong. Use shape-aware arithmetic described below. |
| Replacement certificates | compact_primary_stripe.go:516-628 and :639-1028 compare/rebuild affected streams. Every old-value read must receive physical rank. |
| Column replan | :897-926 currently loops oldStream.count and reconstructs local rows. For rank kind it must enumerate ONLY rows belonging to the target shape, retaining their physical ranks for any subsequent affine proof. |
| Batch capacity proof | compact_primary_batch_bound.go:209-235 deliberately rejects PrefixInt as an unconditional integer proof. Add only a strictly proven native numeric subtype; otherwise use the safe conservative bound. |
| Validation/open | compact_stream_codec.go:1174-1218/:1262-1411 plus compact_primary_stripe.go:1255-1350. Validate kind, sizes, flags, affix directory, count domain, arithmetic bounds, canonical padding and exact leaf association. |
| Slot/index mapping | Physical rank is neither stable posting slot nor shape ordinal. Keep PostingSlot/RankAtSlot semantics unchanged. Splits/checkpoints rebuild the affine descriptor against final row order. |

For shape-aware counts, there is an efficient exact alternative to generic fallback:
- Equality with nonzero step: solve r=(needle-base)/step, check exact divisibility and 0<=r<N, then check rowShape(r)==s. At most one match in that shape.
- Constant step: either zero matches or shape row count.
- Ordered predicates and intervals: derive the satisfying physical rank interval using overflow-safe arithmetic/binary search of the monotone function. Count shape s in [lo,hi) via the existing rank checkpoints plus shape-code prefix popcount. This is bounded by the two boundary restart blocks, not a whole value stream.
- Min/max: find first/last row with that shape; evaluate the appropriate endpoint according to the sign of step.
- Spelling equality: parse/prove exact prefix/suffix and zero-padding shape first, then solve numeric rank. Numeric equality still follows current exact JSON-decimal semantics.
Existing two-bit shape popcount: compact_primary_stripe.go:1418-1472. Higher-width shape codes have bounded restart scans; retaining that bound is acceptable algorithmically, but still benchmark it.

For arbitrary replacement breaking the affine relation, decode the affected shape column into its ordinary values and use the existing planner; that is already the current single-hole COW model. Inserts/deletes change physical ranks, so a full rebuilt leaf must re-prove the relation. Do not carry the old descriptor across reordered rows. A key-sourced affine extension can preserve savings through sequence gaps, but it adds key-decoder dependencies; treat it as a later design, not a free property of the rank scheme.

### Savings and workloads

With unchanged leaf cuts and six id/name descriptors per three-shape leaf, descriptor replacement is approximately:
- Low: replace approximately 171,800 bytes of measured stream bytes with about 5.6 KB of descriptors across 25 leaves, saving roughly 0.166 MB per 100k rows before page rounding; about 18% of live leaf bytes, around 16% of complete primary bytes.
- High: roughly 0.160 MB per 100k rows after about 23 KB of descriptors across 104 leaves; about 2.3% of complete primary bytes. Repacking might change cuts and metadata, so exact payload and extent census must replace this estimate.

This is potentially useful beyond the current corpus for sequence/timestamp fields interleaved across event variants, optional-field shapes, versioned record schemas, and fixed-cadence time-series samples. It does not depend on a field being named id. But monotone values alone are insufficient: arbitrary gaps or shuffled primary order can eliminate eligibility. No additional real workload has been measured by this researcher; report coverage fraction on representative user data before extrapolating a ratio.

## 4. Larger opportunity: remove the medium-value allocation cliff

Default InlineValueBytes=512, MaxPageSize=64 KiB, MaxDocumentBytes=4 MiB:
store/durable/store_file_options.go:1003-1025.
The routing decision is solely logical value length:
store/durable/store_file_overflow.go:30-69.
Values beyond the boundary receive raw overflow storage, regardless of shape/entropy.

Overflow framing is 132 bytes per piece = 64 common header + 8 trailer + 60 overflow header, plus a 32-byte chain-head reference in the leaf. Metadata repeats total length, offset, next PageRef, and data length.
Sources: internal/storeio/overflow_page.go:9-60; store/durable/store_file_overflow.go:24-69.

For V bytes, A=65536-132=65404, m=ceil(V/A):
E_overflow = (m-1)*65536 + 4096*ceil((132 + V-(m-1)*A)/4096).
Add 32 bytes/overflow leaf reference and bitmap/leaf/routing overhead.

Consequences:
- V=513..3964: 4096 overflow bytes.
- V=4096: 8192 overflow bytes.
- V=65536: 65536+4096=69632 overflow bytes (6.25% beyond body, plus leaf reference).
- Multi-MiB values are already close to raw-body size; cutting headers is not a major proportional win there.

Parent's current-main census, 256 rows, exact readback, no latency inference:
docs/benchmarks/format-research-2026-09-05/primary-boundary-census.txt:3-33.
Fixture retained in primary-boundary-census_test.go.txt:13-121.

| Document/configuration | Live primary leaf + overflow bytes |
| --- | ---: |
| 512 B, repeated, inline=512 | 4,096 |
| 513 B, repeated or wide alphabet, inline=512 | 1,060,864 |
| 513 B, repeated, inline=4096 | 4,096 |
| 513 B, wide alphabet, inline=4096 | 135,168 |
| 4,096 B, default inline, either fixture | 2,109,440 |
| 4,096 B, repeated, inline=4096 | 8,192 |
| 4,096 B, wide alphabet, inline=4096 | 1,105,920 |

The configuration comparison proves avoidable allocation/representation overhead. It is not an approved option change and the wide-alphabet deterministic fixture is not an incompressibility proof. Full-file and journal totals are separately retained in that census.

### Design A: adaptive bounded inline with a raw direct-access stream

Introduce a raw spelling stream with fixed-width or u16-end offsets plus concatenated bytes; full-value access can copy a single exact slice. Current front coding can require replay/copies of up to 63 predecessors for a random read, so a raw codec is not redundant for high-entropy strings. A raw candidate could also stop wasting time measuring/encoding alphabet or dictionaries that cannot win.

Select inline based on exact bounded physical cost and mutation admission, not only raw length. Keep very large values outside the main stripe. Canonicalize once; let highly repetitive 513-4096 B documents retain existing shape/dictionary benefits when the bounded planner proves they fit.

Why it might help CPU/I/O: one already acquired leaf instead of an overflow cache lookup/checksum/copy, no separate overflow retirement walk, and fewer allocated blocks. Why it may regress: fewer rows per leaf, more routing metadata, larger COW images for small field updates, more canonicalization and planner work, scratch growth, earlier overlay pressure and maximum-extent rejection. Cold points must count bytes read, not just number of cache calls.

Do not change this policy without revisiting:
- concurrent admission deliberately skips tape work above InlineValueBytes: store_file_primary_concurrent.go:640-668;
- max-extent leaves reject changed overlay puts and use exclusive proof: :903-911;
- raw conservative mutation capacity may require full-leaf encoding;
- bulk CreateFromPrimary explicitly excludes overflow: store_file_primary_bulk.go:139-165;
- persisted options/limits and replay/canonical-size expansion must remain consistent.

### Design B: packed immutable medium-value slabs

Pack multiple raw values into 4-KiB pages with exact slot offsets/lengths and use an immutable page identity plus slot reference from primary leaves. Preserve the 4-KiB cold-read quantum for values that fit a page, direct bounds checking and raw memcpy, and avoid a codec on the read path.

A 513-B row then needs about its own bytes plus a small slot descriptor instead of 4096 B. Seven such rows can fit one page with conservative metadata, giving a large geometric saving without entropy decoding. Values around 4 KiB need a raw block plus bounded tail/metadata scheme if the goal is to remove the 132-byte framing spill.

But ownership is the hard part: current overflow chains are exclusive per value and retirement walks retire the entire chain. Shared pages need exact liveness/reference ownership; updating one row must not retire storage still referenced by another row or old snapshot. Avoid rewriting all packed neighbours on every update. Packing new values only during an already required checkpoint, with updated rows later written separately, is a research direction, not a proven zero-work solution; it creates dead slots requiring bounded cleanup.

This is more promising for broad primary space reduction than shaving leaf headers, but more invasive and much harder to qualify under strict write-tail constraints.

### Design C: inline the final overflow tail

For a 64-KiB value, put the final 132 raw bytes in the primary leaf and keep the first 65,404 bytes out of line; this can remove a 4-KiB continuation page and one cache acquisition. The leaf grows by the tail plus offset metadata, so net saving is at most about 4 KiB/row before leaf growth. It cannot deliver order-of-magnitude savings on 64-KiB high-entropy values. A conservative only-if-existing-leaf-slack-fits policy avoids larger reads but sharply limits coverage. Cold leaf growth, leaf split behavior, and altered total/offset validation are mandatory checks.

## 5. Other ranked designs

### Adaptive catalog roots and path collapse

Current constants: global_tablet_catalog.go:28-35. The builder always allocates a 64-KiB root and at least one 8-KiB catalog leaf (:918-976), plus 8-KiB tablet route, locator and anchor pages (:820-894). There is no small-store collapsed form.

- Merely permit a 4-KiB root when its actual contents fit: save 60 KiB per collection, retaining the same child graph.
- Permit a root to point directly at one tablet, saving another 8-KiB level.
- For one-leaf collections, use a compact root descriptor pointing directly at the leaf; theoretical graph metadata reduces from 96 to 4 KiB. With the 16-KiB mutable prefix and 4-KiB leaf, a 116-KiB live minimum could become 24 KiB.

Warm current reads already use the same resident router (resident_primary_router.go:12-49), so they need no extra on-disk traversal. Cold/snapshot readers may touch fewer pages; COW checkpoints can copy/hash/write less root data. Promotion/demotion has to occur atomically with the existing root publication protocol; a worst-case insertion at a promotion boundary must be included in tests. Root identity, PageKind dispatch, fixed ChildLength decoding, validators, transaction sizing and recovery cannot continue assuming a 64-KiB root.

Relative RF3-SQL impact is small while 16-MiB collection journals dominate: 60 KiB is only about 0.37% of one 16-MiB sidecar, before other bytes. Useful complementary work, not the principal whole-architecture result.

### Share shape-invariant streams/templates

Generalize a shape from owning every stream to referring to stripe-wide field streams and local streams. All-shape fields use global row rank; optional fields use a presence bitmap/local ordinal. This removes repeated dictionaries/headers, can preserve arithmetic ID fields, and reduces per-shape preparation. It is a broader version of the affine insight, especially for many optional-field combinations.

Tradeoffs: static skeleton identity is currently a cheap exact model of arbitrary JSON, including order/duplicate-key semantics. A path-based union cannot casually erase these. Repeated fields/arrays need occurrence identity and exact reconstruction order. Stream sharing alters patch independence and exact scalar capacity proofs. For the measured three-shape corpus the header/template-only opportunity is small; most gain would still come from better shared value streams, not schema text alone.

### Separate physical page capacity from 256-slot indexed identity

The 256-row indexed cap is a semantic posting-location decision, not a property of the compressed document payload. One can imagine several virtual 256-slot buckets in one physical stripe/container, retaining current posting tile identities while sharing templates/streams and 4-KiB allocation.

This can break the 16-B/row primary minimum for very compressible indexed data. However, directly changing slot uint8 to uint16 disrupts tile IDs, liveness, overlays, indexes and snapshot addressing. Co-packed buckets need a shared physical handle/lifetime so one COW update does not rewrite every neighbour's route. Avoid claiming this is a local constant change. Source model: primary_graph.go:29-35/:71; index_term_leaf.go:86-95; compact_primary_stripe.go:1525-1551.

### Narrow redundant metadata

Potential local savings: omit a one-shape rank checkpoint table (currently emitted even though shapeOrdinal directly returns row), use u16 template ends/shape lengths under the 64-KiB leaf ceiling, infer scalar count from its declared domain, omit constant repeated fields from a stream into the static template, shorten fully bounded overflow descriptors by storing shared generation/identity bases.

These are legitimate exact-format changes, but their measured ceiling is small. The one-shape rank table is 2 bytes per 64 rows (0.03125 B/row). Framing plus extent slack is <0.15 B/row on the two bulk corpora. Compression of PageRef metadata cannot justify dropping StoreID/identity/generation validation; factor redundant fields only if the selecting page preserves the same proof.

## 6. Versions, copies and reclamation are part of the cost

The primary is an immutable COW graph, not an LSM. Retired extents store offset/length/retired generation (24 bytes), and reuse waits for both reader and alternate-recovery-root floors:
internal/storeio/free_extent.go:8-27;
store/durable/store_file_free.go:86-122.

Retirement metadata is durable in the publication retiring it; it is not safe to delete old extents because current readers happen to be absent. Free metadata is segmented and folded for touched segments rather than globally rewritten: store_file_free.go:33-69. A held snapshot can legitimately retain an older full leaf and overflow versions; these bytes are not leaf padding.

The existing allocator/hole-punch design already recovers certified free extents. Hole-punch scheduling has bounded discovery, call and byte limits (store_file_hole_punch.go:10-35), and consults fallback generation (:284). More aggressive physical reclamation adds device work and can regress latency even outside foreground mutexes, as the earlier RF3 experiment demonstrated.

Current overflow reads acquire and checksum each piece then append it to output:
store_file_overflow.go:407-470. Buffered overflow writes encode volatile chains, then checkpoint materialization reconstructs and re-mints those chains:
store_file_primary_mutation.go:2263-2286.
That existing second pass provides a concrete place to investigate better durable packing or borrowed immutable body ownership, but reducing it safely crosses into Raft/redo/snapshot ownership and the other researcher's scope.

Current single-hole replacement decodes/replans the affected entire scalar column, then copies unchanged payload ranges into a new page and hashes it:
compact_primary_stripe.go:897-1026.
A proposed format should reduce this work or keep it bounded. A smaller persisted stream that forces whole-row JSON reconstruction, full-leaf generic rebuild, or loss of the native query paths can easily lose the latency comparison.

## 7. Rejected shortcuts and falsification plan

Reject as the immediate answer:
- Whole-page LZ/Zstd/DEFLATE merely because a compressed byte count is small: adds decompression, scratch and patch work; old class-5 DEFLATE numbers are not a VCS1/RF3 result.
- Raw-byte InlineValueBytes increase alone: solves a real space cliff but changes hot-path geometry and has no latency evidence.
- Increasing maximum stripe rows or extent size uniformly: little slack remains in the measured unindexed corpus; worsens cold-read/patch bounds and does not fix indexed slot semantics.
- Eliminating dictionaries' 25% scan preference: knowingly trades read cost for bytes.
- Dropping old snapshot/recovery versions or sharing RF3 bodies without an authenticated immutable ownership protocol.
- Treating apparent file length, live reachable bytes, allocated blocks, and reserved capacity as the same metric.
- Treating new unsupported native predicates as harmless generic fallbacks under a no-regression requirement.

For affine, the smallest useful next experiment is a codec-only before/after payload census with a row-byte oracle:
1. Original two 100k corpora; emit per-path old/new kind and bytes, eligibility fraction, exact total payload and rounded extents. Check every reconstructed key/value byte.
2. Multiple interleaved shapes over sequence IDs and fixed-cadence timestamps; randomized shape assignment, optional fields, nested arrays, fixed-width names. Include natural sequence gaps and shuffled key order to quantify loss of eligibility.
3. Almost-affine columns with mismatch at first/middle/last row and scalar replacements that break the relation, including updates before/after a restart boundary. Confirm fallback produces baseline semantics and measure detection cost later.
4. Limits: 0/1/2/63/64/65/255/256/4095/4096 rows; shape counts 1/2/3/5/64; slots permuted away from lexical rank; row domains with interspersed overflow; empty shapes rejected.
5. Numeric adversaries: MaxInt64, MinInt64 signed fallback, positive/negative step, zero step, overflow in derivation and endpoint evaluation, 20+ decimal digits including 46000000000000000000, -0, leading zeros, varying padding, exponents, decimals, escaped affixes. Recomputed valid CRC with invalid grammar must fail closed.
6. All APIs: full read, cursor seek, reverse/repeated traversal where supported, native projection, count equality/order/interval/extrema, grouped SUM, exact-index selection, scalar patch, insert/delete/split/reopen/snapshot. Row-domain bugs must not hide behind the full-value oracle.
7. Only after correctness/byte census: paired hot/cold point reads, sparse/indexed projection, full scans, native counts/groups, insert/replacement/delete p50/p99, allocations and bytes copied; scalar and SIMD on relevant architectures. Include sustained checkpoints and long-held snapshots, not just warmed codec loops.

For the overflow designs, sweep both sides of 512, 3964/3965, 4096, 65404/65405 and 65536; random/repetitive bodies, narrow changes/full replacements, batch/single inserts, eviction/cold misses, snapshots and delayed checkpoints. Report live graph, retired/fenced extents, allocated blocks, journals and RF3 total separately.

For adaptive roots, include empty/one-leaf/one-tablet transitions and adversarial 256-byte fences, every promotion boundary, crash before/after data barrier/root write/final sync, old snapshots, recovery roots and exact-index placement.

Format 0 currently provides no cross-build migration guarantee: docs/format.md:5-6 and :142-149; page.go:26-29. Any adopted grammar must change build grammar identities and golden fixtures, reject obsolete/noncanonical layouts, and prove crash/reopen boundaries. An affine codec research experiment should not silently mutate files written by the baseline. Production migration should rewrite to an isolated new generation/backup and activate atomically, respecting current snapshot and fallback-root reclamation fences.
