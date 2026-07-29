# Unified document leaf: canonical template rows (one representation, one read path, one write path)

**Status:** executable design, validated against the tree at head (post
`c6df176`). Every claim below is either **(verified)** against named code or a
named harness measurement, or **(projection)** with the measurement that will
decide it. Product ruling this design executes, verbatim intent: "we dont want
2 store systems — since we dont need byte exactness make it faster and make it
also smaller. unify." Byte-exact JSON round-trip — including object key order
— is no longer a requirement. Compact-at-rest plus a decoded resident cache
was explicitly rejected as still being two systems. Deterministic encoding
(same input state → same encoded bytes) remains mandatory: the `-count=2`
byte-identical checkpoint gates, journal-replay identity, and the
bulk-vs-mutation identity anchor all depend on it. Number tokens must not lose
precision; number-token rewriting is admitted only where losslessness is
provable from the JSON grammar (§3.4).

**Idea:** one primary-leaf class stores every document as **canonical
template rows**: the leaf keeps the proven succinct ordered envelope (stable
hash slots, lexical key heap, succinct boundaries) and replaces the raw JSON
value bytes with a per-leaf **shape-template table**, a per-leaf **value
dictionary**, and per-row **typed token streams** in template-hole order over
the *canonical* rendering of the document (keys sorted, whitespace dropped,
escapes normalized, number spellings preserved). A document whose shape no
other row shares degrades gracefully to a **trivial row** — one token holding
its whole canonical spelling — inside the same grammar, the same codec, and
the same dispatch. Mutations never de-template anything: they stage O(row)
overlay records against the immutable fold base (the indexed-write-path §3
pattern, whose P0 arena/fold hooks this design assumes and reuses), or patch
equal-length holes in place; the checkpoint fold is the only re-encoder.
Verbatim (classes 1/2), template-columnar (class 3), compact document-group
(class 4), the `DocumentFormat` option, and the de-compact state machine are
all deleted.

---

## 1. The measured problem (harness, 2026-07-29, M4 Max, 100k × ~249 B docs)

| metric | verbatim (classes 1/2) | compact (class 4) |
|---|---|---|
| GetRaw point read p50 (in-workload) | **0.25–0.38 µs** | 1.08 µs |
| space per 100k docs, low / high cardinality | 28.1 / 28.1 MiB | **7.8 / 17.6 MiB** |
| update p50 | **4.6 µs** | first update to a leaf: **2,301 µs** |
| full-scan filter | **40.6 ns/doc** (fastest of five engines; SQLite 830) | n/a (bulk-only evidence) |

Neither representation dominates, so the store carries both: two encoders,
two read dispatches, a per-collection `DocumentFormat` switch
(`store/durable/store_file.go:200-235`), and a de-compaction state machine
whose cost is the table's worst number. The 2,301 µs is an **O(leaf)
per-mutation disease by construction** **(verified mechanism)**: the first
write to a compact or template leaf reconstructs every row's exact JSON
(`DetemplateRecords`, `internal/storeio/common_primary_compact_leaf.go:482-514`),
re-places and re-encodes the whole group into a raw envelope of up to 64 KiB
(`AdmittedPrimaryLeafForMutation`,
`internal/storeio/common_primary_template_leaf.go:256-338`, fresh
`make([]byte, pageSize)` per attempt), and then the structural path re-fits
and splits it (`store/durable/store_file_primary.go:528-534` documents the
64 KiB mutation scratch this forces). The unified format must make that class
of cost **impossible by construction**, not merely rare.

The freedom that unlocks unification: byte-exact round-trip is dropped.
Canonical key ordering collapses shapes that today mint distinct templates;
canonical whitespace/escape normalization makes the stored spelling a pure
function of document *content*; and the render side no longer owes the
caller the original bytes, only *a* valid, deterministic spelling.

### 1.1 Gates the unified format must meet (these become the promotion gates)

- space ≤ **7.8 / 17.6 MiB** per 100k (claim: below both, §9).
- GetRaw p50 ≤ **0.50 µs** (≤ 2× the 0.25 µs verbatim floor; §10.1 argues
  why parity with the borrowless copy is not physically available and why
  whole-document reads are the rarer operation in an SQL-first product),
  with field probes and filters **faster** than verbatim (§10.2, §10.3).
- update p50 ≤ **4.6 µs** unchanged; no mutation path may cost O(leaf).
- filter ≤ **40 ns/doc** (target ≤ 20).
- zero steady-state allocations on every hot path (standing directive).
- `-count=2` byte-identical checkpoint files; crash/replay identity;
  bulk-vs-mutation byte identity.

## 2. What is already true (verified inventory)

1. **The durable snapshot's document surface is whole-document, copy-out,
   already scratch-rendered.** `Snapshot.AppendRaw(dst, key)`
   (`store/durable/store_file.go:2392`) documents "It never returns a
   borrowed page slice" — the result is a caller-owned copy. `RangeRaw` /
   `RangeMasksRaw*` (`store_file_scan.go:11,118-150`) borrow key/value **for
   one callback only**, and overflow values already reassemble into one
   reused buffer. There is **no field/pointer accessor on the durable
   snapshot**; every field read happens above the store over copies
   (`query/file_execute_pool.go:425-438` copies each scanned value into a
   batch buffer; `query/join_file.go:926-987` copies a whole document then
   navigates it with `PointerCompiled`; `query/result.go:146-164`
   (`ownFileCell`) copies projected cells out before any page frame is
   reused). Consequence: a leaf codec that renders into caller/cursor
   scratch changes **no public borrow contract at all**.
2. **The scan cursor already renders per-class rows into reused scratch.**
   `PrimaryGraphCursor.openLeaf` / `nextRawBorrowed`
   (`internal/storeio/primary_graph_cursor.go:260-320+`) dispatch on the
   class byte and splice template/compact rows into a reused buffer;
   `VisitPrimaryLeafPostingRows`
   (`internal/storeio/primary_exact_index.go:227-284`) does the same for
   index derivation. One read path already tolerates rendered rows; today it
   just tolerates *four classes* of them.
3. **The succinct ordered envelope is proven.** Stable cuckoo hash slots
   (192 normal + stash), control bytes, lexical rank permutation, 7-bit key
   lengths, succinct cumulative record boundaries with select checkpoints,
   extents 4–64 KiB power-of-two (`internal/storeio/common_primary_leaf.go`,
   layout at `:190-262`, placement at `:328-386`). Power-of-two extents are
   load-bearing for the page-cache arena (the 2026-07-27 investigation
   measured 36 % arena slack from non-buddy-friendly extents).
4. **The compact codec's template/dictionary derivation is deterministic and
   gated.** `planDocumentGroup` / `writeDocumentGroupPayload`
   (`internal/storeio/document_group.go:116-366`) — bounded 128-entry value
   dictionary, content-addressed static skeletons, token grammar
   (dict-ref / short literal / long literal), pinned byte-for-byte by
   `TestCompactPrimaryDeterminism`
   (`store/durable/store_file_primary_compact_test.go:253`). The unified
   codec reuses these derivation rules, not the container.
5. **The template-columnar lab measured the splice.** 102.7 ns compiled
   `AppendRaw` splice for the ~250 B/12-field competitive shape, 20.4 ns
   zero-alloc field access, region reseal 2.5× cheaper than whole-leaf
   (docs/design/template-columnar-leaves.md). Those are the load-bearing
   constants in §10's arithmetic. The class-3 *class* failed adoption for
   reasons this design removes: byte-exact templates fragmented shapes, the
   row cap was tied to de-templatability, and mutation cost scaled with
   leaf size because mutation meant de-templating. None of those survive
   canonicalization plus overlay mutations.
6. **The in-place buffered fast path exists and defers resealing.**
   `tryBufferedPrimaryInplace` / `replaceBufferedPrimaryInplace`
   (`store/durable/store_file_primary_mutation.go:932-1029`): buffered lane,
   no indexes/schema, equal-length value swap, value aliases the resident
   frame, second touch onward, reader fence; the patch is a bytes copy into
   the frame with reseal deferred to the checkpoint worker
   (`internal/storeio/page_cache_inplace.go:39-103`). §7.3 generalizes
   exactly this to equal-length *hole* patches.
7. **The general verbatim update is already leaf-copy-shaped.** `UpdateTo`
   copies the page and patches in place only for same-length values; any
   length change is a full `EncodeCommonPrimaryLeaf` re-encode
   (`internal/storeio/common_primary_leaf.go:~1938-2001`) — today's 4.6 µs
   p50 includes that. The unified mutation path (§7) removes the per-
   mutation leaf copy/re-encode entirely, which is what pays for the
   canonicalization work it adds.
8. **The overlay/fold pattern is ratified and landing.**
   docs/design/indexed-write-path.md §3: immutable fold base +
   generation-stamped overlay records, read rule "newest record ≤ G else
   base", fold at checkpoint/structural/canonical-lane points, arena +
   freelist + reclamation-floor discipline. Its P0 is assumed landed; this
   design adds **row-record kinds to the same overlay machinery** rather
   than inventing a second one.
9. **Canonical rendering exists in the library.** `AppendCanonicalize`
   (`vibejson/encode.go:164-229`): decoded keys sorted by UTF-8 byte order,
   duplicates retained in relative order, arrays ordered, escapes
   normalized as `AppendJSON`, **number spellings preserved**. The hot path
   needs tape-level, zero-alloc equivalents (§8.3), not the `Value`-tree
   spelling.
10. **Every stored value is proven JSON at admission.** `vibejson.Validate`
    on the Put path (`store_file_primary_mutation.go:533`), plus a
    per-document `BuildIndex` when a schema is configured (`:482`). The
    write path already touches the bytes; tokenization rides that touch.
11. **The ShapeTapes cliff is the cautionary tale.** The heap segment's
    shape-deduplicated tapes require a *flat root object*
    (`store/segment_shape.go:33-45`): one nested member disqualifies, and
    **zero** of the 100k competitive documents conform (every one carries
    `profile{...}` and `tags[...]`). A conformance model that cliffs on
    nesting is a non-starter; §6 places holes at scalar leaves of arbitrary
    depth (the document-group model, which carries this corpus today).
12. **Parallel-tablet-writers needs leaf-local state.** Staging must remain
    tablet-local; publish is a serialized pointer section; scratch moves to
    shard frames (docs/design/parallel-tablet-writers.md §3-4). Per-leaf
    templates/dictionaries and per-shard overlay arenas satisfy it; nothing
    here is collection-global.

## 3. The representation

One durable class: `CommonPrimaryLeafUnified`, class byte `5` in
`payload[2]`. Everything outside the payload is untouched: `PagePrimaryLeaf`
kind, BucketID/LogicalID identity, tablet routing, COW, snapshots, CRC
framing, overflow chains, posting coordinates.

### 3.1 Leaf layout

```text
payload[0:2]  reserved (0)
payload[2]    class = 5
— succinct ordered envelope, unchanged geometry (verified machinery §2.3) —
  control bytes, normal/stash rank arrays, stash confirm directory,
  7-bit key lengths, overflow bitmap, succinct cumulative record
  boundaries + select checkpoints, extent 4..64 KiB power of two
— unified sections (replace the raw record heap's value semantics) —
  template directory: cumulative u32 ends; per template: hole count u16,
      skeleton bytes (canonical static segments, verbatim), hole-end table
      (the document_group template entry layout, reused)
  dictionary directory: cumulative u32 ends + raw value spellings
      (≤ 128 entries, chosen by the document_group candidate rule)
  record heap (physically lexical, succinct boundaries as today):
      per record: [key bytes][row body]
      row body = templateID u8 | token stream        (templated row)
                 | 0xFF        | canonical JSON bytes (trivial row)
                 | overflow    → 32-byte PageRef     (overflow bitmap set,
                                                      as today)
```

The envelope's slot, lookup, and boundary machinery is reused byte-for-byte;
only the record heap's *value* content changes meaning. A row's stable slot
is its cuckoo hash slot — the same slot discipline as classes 1/2, and now
the **only** posting-slot discipline (classes 3/4's lexical-rank slots die
with them).

### 3.2 Canonical form (what "the document" means at rest)

The stored logical value of every document is its **vibejson canonical
form** (§2.9): object members sorted by decoded key (UTF-8 byte order,
duplicates in original relative order), arrays in order, no interstitial
whitespace, string escapes normalized, number spellings preserved verbatim.
`AppendRaw`, scans, index derivation, and the SQL layer all observe this one
spelling. Canonicalization is idempotent, so journal replay of canonical
bytes reproduces identical state (§7.5).

### 3.3 Templates and holes

A document's **skeleton** is its canonical spelling with every *scalar leaf
value* (string, number, bool, null, and empty containers — exactly the
`Next == 1` tape-leaf criterion the compact path uses,
`common_primary_compact_leaf.go:66-101`) replaced by a hole. Nested objects
and arrays are part of the skeleton: nesting never disqualifies (§6). The
template table stores each distinct skeleton once per leaf,
content-addressed by skeleton bytes (hash-routed, byte-compared — proof,
not fingerprint trust, the `shapeTapeConforms` discipline). Template order
is first-use in lexical rank order; the dictionary uses the
document_group's deterministic candidate selection — both pure functions of
the leaf's final row set (**determinism**).

### 3.4 Typed token grammar (the "we only store JSON" dividend)

Per hole, one token. Tag ranges follow the document-group scheme (dict ids
and short-literal ranges disjoint), extended with typed tags:

| token | payload | losslessness argument |
|---|---|---|
| dict ref | none (id in tag) | dictionary stores the exact canonical spelling |
| short/long literal | spelling bytes | verbatim canonical spelling (all numbers with `.`/`e`, all strings incl. quotes) |
| `true` / `false` / `null` | none | the JSON grammar admits exactly one spelling each; regeneration is identity **(verified: grammar)** |
| canonical int | zigzag varint | admitted **only** when the spelling matches `-?(0|[1-9][0-9]{0,17})` and is not `-0`: such a token is the unique minimal decimal spelling of its int64 value, so `strconv.AppendInt` regenerates it byte-identically **(verified: decimal grammar; pinned by a differential test over the full admission predicate)** |

Everything else — floats, exponents, big/odd integers — stays a literal
spelling. This is the entire number-rewriting policy: rewrite only where
identity is provable; precision loss is impossible by construction.
Measured effect on the competitive doc: `id` 6 B → 3 B, `score` 4 B → 3 B,
`active` 5.7 B (avg) → 1 B; render of these tokens is an integer append or
a constant, cheaper than the memcpy it replaces **(projection, §10.1)**.

### 3.5 Trivial rows: the escape hatch *inside* the one grammar

A row whose skeleton is shared by no other row in the leaf, or whose
skeleton fails the amortization test
`skeletonBytes + directory ≤ Σ savings over its rows`, is stored as tag
`0xFF` + its whole canonical spelling. That is a *token*, not a class:
same encoder, same read dispatch, same slot machinery, one `if` on a tag
byte. Render is one memcpy; mutation is the same O(row) path. This is the
representation decision the one-system bar permits — a variable-length
encoding inside one codec — and it is what makes the worst case (a leaf of
256 mutually alien 4 KB documents) cost canonical-verbatim + ~2 B/row
instead of a cliff (§6).

### 3.6 Leaf packing (bulk build and fold share it)

The planner replaces both `planCompactPrimaryLeaves` and the adaptive
narrow/wide/template planner (`internal/storeio/primary_graph.go:392-615`)
with one rule, reusing the compact planner's memoized packing search: for
each extent in {4, 8, 16, 32, 64 KiB}, binary-search the largest lexical
row prefix that (a) encodes within the extent, (b) places within the
256-slot class, (c) holds ≤ 256 rows; choose the extent minimizing bytes
per row. The de-templatability cap (`rows ≤ raw wide capacity`,
`primary_graph.go:555`) is **deleted** — it existed only so a mutation
could de-template, and mutations no longer de-template anything. Expected
geometry on the competitive corpus: ~140–190 rows in 16 KiB extents
**(projection: recorded by the U1 census)**.

## 4. Design question 1 — this representation against the alternatives

Scored on: GetRaw render, field probe, filter, space, mutation, split.
Arithmetic for the chosen design is in §9–§10; here the deltas.

**(a) Canonical-rendered spans + page-level dictionary compression**
(store canonical JSON verbatim per row; compress the leaf with a shared
dictionary — FSST/LZ-class). Space: plausibly competitive on low-card
(gzip -9 reaches ~8 % on the shipped corpus; a leaf-local scheme lands far
above that, realistically 40–60 % → ~100–150 B/doc, **worse than
compact's 78**). Reads: a point read must either decompress O(leaf)
(1–10 µs for 16–64 KiB — the compact read disease relocated) or keep a
decoded resident cache — the explicitly rejected second system. Field
probes and filters still pay a full parse after decompression. Mutations
recompress O(leaf). Determinism couples to a compressor's exact behavior —
a pinned, vendored constraint the template path does not need. Rejected on
the read and mutation gates; it wins nothing the template form does not.

**(b) Two classes + automatic promoter** (keep verbatim and compact,
promote/demote per leaf by measured heat). Rejected by the owner as still
two systems, and the measurements agree it is the *disease generalized*:
promotion is an O(leaf) rewrite on the write path (the 2,301 µs event,
renamed), demotion is an O(leaf) rewrite on the read path, and every
component that touches leaves keeps two code paths plus hysteresis tuning.
The compact leaf's own history is the existence proof: "bulk-only,
de-compact on first write" *is* an automatic promoter, and it produced the
worst number in the baseline table. Documented only to record why it fails
the one-system bar.

**(c) Fully columnar per leaf** (column-major: all values of hole h
contiguous). Wins pure single-column scans (better locality per column,
vectorizable) and would shave the filter lane further — but the filter
gate is already beaten row-major by 2.5–4× (§10.3), so the win is
headroom we do not need, bought with: GetRaw render becomes a gather
across ~12 column regions (~12 cache lines + 12 offset lookups vs 1–2
lines row-local — the render is the gate under pressure); O(row) mutation
dies (one row replacement touches every column region; variable-length
columns each need slack and per-column compaction — mutation cost
O(columns) scattered writes and the fold becomes a 12-way merge);
and posting/scan enumeration pays the same gather per row. Rejected:
row-major with per-leaf dictionaries captures the space, meets the filter
gate, and keeps mutations row-local. Columnar remains available *above*
the store where it already lives (`query`'s Segment extraction, float64
sidecars in the heap engine).

**Sub-template alternative for nesting** (template references template):
adds an indirection per nested container on every render and a second
identity space to canonicalize, to optimize the case where an outer shape
varies while an inner repeats — which the per-leaf dictionary already
captures at the value level. Rejected for complexity with no measured
corpus that rewards it; revisit only with evidence (§14).

## 5. Design question 2 — the render story with no second system

**No public contract changes.** (§2.1) `AppendRaw` already appends into the
caller's buffer and never leaks page spans; `Range*` values are borrowed
for one callback and template/compact rows *already* arrive from the
cursor's reused splice scratch (§2.2). The unified read path therefore:

- **Point read:** route → leaf → cuckoo slot lookup (`LookupHashed`,
  unchanged) → rank → succinct boundary select → row body → splice
  directly into the caller's `dst` (templateID → skeleton segments
  interleaved with token renders; trivial row → one memcpy; overflow →
  existing chain reassembly of canonical bytes). Zero allocations: the
  splice appends to `dst` exactly as the copy does today; per-cursor and
  per-workspace scratch grow once and are reused (the `IndexWorkspace`
  discipline, `store/durable/store_file.go:2250-2264`).
- **Scans:** `nextRawBorrowed` splices into the cursor scratch it already
  owns; the callback borrow window (one invocation) is unchanged.
- **Posting derivation:** `VisitPrimaryLeafPostingRows` becomes single-
  class: splice into the reused scratch it already threads, slot = stable
  hash slot (one slot discipline; the lexical-rank branch dies).

The one deliberate addition: a **token-level row view** on the cursor
(templateID + token iterator + hole resolver) so the scan/filter lanes and
a future SQL column probe can consume rows *without* rendering (§10.2,
§10.3). That is the same one read path exposing structure it already has —
not a cache, not a second representation; it borrows the admitted page
under the same lease/epoch rules as every other borrowed span.

**Why GetRaw parity with verbatim is not physically available:** verbatim's
value step is one 249 B memcpy (~15–20 ns); the templated row must
interleave ~13 skeleton segments with ~12 token renders — 102.7 ns
measured for exactly this shape in the class-3 lab (§2.5). The floor for
"assemble from fragments" is real. §10.1 quantifies the resulting p50 and
§1.1's second clause covers it: in an SQL-first product the whole-document
read is the *pass-through* operation (`SELECT doc`, KV-style Get), while
predicates, projections, joins, and index maintenance — the operations
this engine is being pointed at (`sql/driver/stmt.go:311` parses the whole
fetched document per point row today; `join_file.go:987` per probed row) —
read fields, and fields get **faster** than verbatim (§10.2).

## 6. Design question 3 — nested and irregular documents

Holes live at scalar leaves of **arbitrary depth**; skeletons carry all
structure between them (§3.3). There is no flatness gate: the ShapeTapes
conformance rule (flat root only, §2.11) engages for 0 % of the
competitive corpus, while the document-group hole model — the one this
design reuses — carries 100 % of it today as class 4.

- **Competitive corpus conformance:** every document is template-eligible;
  canonical skeletons differ only by `tags` arity (2/3/4) → ~3 templates
  per leaf, each shared by ~⅓ of the leaf's rows **(verified shape: the
  corpus generator, `bench/competitive/corpus.go:139-201`; count is a
  projection recorded by the U1 census)**.
- **Realistic corpora:** template eligibility requires only that a shape
  recur among the ~140–190 lexically adjacent rows of one leaf.
  Key-order canonicalization widens eligibility over today's class 4:
  producers that emit the same fields in different orders collapse to one
  skeleton (today they mint distinct templates).
- **The remainder:** a deeply nested 4 KB document with unique structure
  is a **trivial row** (§3.5): stored at canonical-spelling size + tag +
  row addressing ≈ **canonical verbatim + ~2 B**, rendered with one
  memcpy (faster than its templated render would be), mutated through the
  same O(row) path. A whole store of such documents converges to
  canonical verbatim + ~1 % — a floor, not a cliff. Space/speed for the
  non-conforming remainder is therefore bounded by the verbatim baseline
  itself.
- Values larger than `InlineValueBytes` keep today's overflow chains
  (canonical bytes in the chain, trivial semantics at the row). Unchanged
  machinery, one code path.

## 7. Design question 4 — mutations: overlay + fold, and the in-place class

### 7.1 Why not mutate the leaf image per operation

Unified leaves are bigger than 4/8 KiB (template amortization wants
16–32 KiB extents, §3.6). Today's non-in-place update copies and/or
re-encodes the leaf per mutation (§2.7) — acceptable at 4–8 KiB inside
4.6 µs, regressive at 16–32 KiB (a 32 KiB COW copy alone is ~1.5–3 µs,
plus staging and reseal). The overlay makes update cost independent of
leaf geometry, which is exactly what lets the leaf grow to where templates
pay. One mechanism serves both goals; this is the same trade the exact
index made in indexed-write-path §3, and it composes with P0's landed
arena, generation stamping, publish-section linking, and reclamation
floor — this design adds **record kinds**, not machinery.

### 7.2 Records and rules

Per-leaf (bucket-keyed) overlay chains in the P0 arena, generation-stamped,
newest-first, linked in the same gate + fence section as the state
publish:

- **row record** `(bucket, key, slot, gen, rowImage)` — the complete new
  row body (canonical bytes in trivial form, or tokens against a *base*
  templateID when the shape matches an existing base template; new shapes
  stay trivial until fold — no mid-window template appends, no dictionary
  lookups on the ack path).
- **tombstone** `(bucket, slot, gen)` — delete.
- **insert** is a row record whose slot is drawn from the leaf's free-slot
  set (lowest free slot — deterministic mid-window identity; §7.4 owns the
  durable identity).

**Read rule.** A probe at generation G resolves a key: slot lookup on the
fold base as today; then one overlay check — newest record for the key
≤ G wins (row image or tombstone), else the base row. A clean leaf's check
is one counter test (the P0 empty-overlay fast path). Scans merge the
bucket's few overlay records (sorted at link time) with base ranks.
Every branch is a pure function of (base, records ≤ G).

**Write rule (all O(row)):**
- *update, same shape, equal hole lengths:* in-place hole patch, §7.3 —
  zero records.
- *update, otherwise:* tokenize (§8.2), one row record.
- *insert:* one row record (+ the P0 posting records for indexed
  collections); *delete:* one tombstone.
- *nothing on the mutation path ever decodes, re-encodes, splits, or
  de-templates a leaf.* The words "de-template" and "de-compact" leave the
  tree (§12).

### 7.3 The in-place equal-length class (bufferedInplace, generalized)

The value-row layout admits a *stronger* in-place class than today's,
because equality can be checked per hole: eligibility = buffered lane,
same key/slot, same templateID, every changed hole's token byte-length
unchanged, plus today's verified guards (no indexes/schema, pending-parent
second touch, reader fence; §2.6). The patch writes only the changed
holes' bytes into the resident frame — a strict subset of today's
whole-value copy — and defers reseal through the existing
`NeedsReseal` checkpoint mechanism. Succinct boundaries are untouched by
construction (lengths equal), so no directory maintenance. The harness's
same-size update (`SameSizeUpdatedJSON`, one digit of `score`) qualifies.
Indexed collections can later relax "no indexes" via P0's zero-record
rule (indexed value unchanged ⇒ index untouched); not gated here.

### 7.4 Fold: the only re-encoder

At every fold point (checkpoint materialize, structural transaction,
canonical per-mutation lane), each **dirty** leaf re-encodes as a pure
function of its final row set: canonical rows in lexical order →
placement (`PlaceCommonPrimaryLeafRecords`, deterministic) → template
census (§3.3) → dictionary selection → token emission → seal. Untouched
leaves persist by reference. Consequences:

- **Bulk-vs-mutation byte identity by construction**, at leaf strength:
  the fold and the bulk builder are the *same* encoder over the same rows
  (the §6.4 discipline of indexed-write-path, applied to document
  leaves). The identity gate extends from exact pages to primary pages.
- **Slot reassignment happens only inside a fold**, where postings for
  the affected bucket are re-derived from the same final rows in the same
  transaction — so the P0 *mid-window* rebase-group machinery is no
  longer needed for class transitions (there are none). Between folds,
  slots are stable: in-place patches keep slots, inserts take free slots,
  deletes free them.
- Fold cost is O(dirty leaf), paid once per window, exactly where the
  checkpoint already pays COW staging and reseal for dirtied leaves
  (§10.4 bounds it).
- **Splits/merges** stay structural and fold-first, through the existing
  record-array path (`store_file_primary_structural.go:778-875`); each
  side's templates and dictionary **re-derive** from its own rows — never
  copied, never shared across leaves — so split output is independent of
  split history (determinism), and templates remain strictly leaf-local
  (tablet-local ⊂ parallel-writers requirement, §2.12).

### 7.5 Durability, replay, determinism

Canonicalization happens once, at admission, **before** the journal
record is written: journal Put records carry canonical bytes. Replay
drives the ordinary mutation path; canonicalization is idempotent, so
replay reproduces the same records, the same fold inputs, and — by fold
purity — byte-identical pages. `-count=2` holds because encode is a pure
function of final state and page allocation stays transaction-ordered.
The three existing identity/crash suites extend with: crash between a
row-record link and its fold; fold-vs-bulk differential on mutated
stores; trivial/templated boundary flips under churn.

## 8. Where canonical key ordering lives: leaf encoder, not tape builder

**Recommendation: fold/stage-time, in the storage layer.** The vibejson
tape builder keeps reflecting the document *as given*.

- **Boundary argument.** The tape is the library's public contract and
  the query/SQL layers consume tapes of *arbitrary* JSON — needle
  operands (`query/predicate.go:675`), bound parameters
  (`pgwire/extended.go:1031`), schema validation — where reordering
  would change observable semantics (duplicate-key resolution, pointer
  iteration order) for every consumer, including external library users.
  Canonical order is a property of the *stored representation*; the
  base-store layering puts representation decisions in the leaf encoder.
- **Cost argument.** Parse-time canonicalization taxes every read-side
  `BuildIndex` (scan batches build tapes over already-canonical rendered
  bytes; sorting there is pure waste). Stage-time canonicalization runs
  once per stored version — and is skippable: stored-and-rendered bytes
  are canonical by construction, and the admission pass detects
  already-canonical input (sorted keys, no gaps, canonical escapes) in
  the same walk that tokenizes, making the steady state (engine-rendered
  or SQL-generated input) a no-op.
- **What the query layers see** does change *data-wise*: reads return
  canonical spellings, so downstream tapes see sorted members. That is a
  value change permitted by the product ruling, not a semantics change in
  the library.

**Library work (U0):** tape-level `AppendCanonicalIndexed(dst, Index)`
(zero-alloc render from an existing tape + member-order scratch in a
workspace), `IndexIsCanonical(Index)` (one-pass check), and the typed-int
admission predicate (§3.4). `AppendCanonicalize`'s `Value`-tree spelling
stays for general use; differential tests pin the two against each other.

## 9. Space: the arithmetic

Per-document at rest, competitive corpus (~249 B raw, 12–13 scalar
leaves, 12 B key), leaf of ~160 rows in a 16 KiB extent
**(projection; the U1 census pins every line)**:

| component | low-card B/doc | high-card B/doc |
|---|---|---|
| key bytes | 12 | 12 |
| envelope fixed (control/rank/stash/confirm ≈ 980 B/leaf) | 6.1 | 6.1 |
| succinct boundaries + key bits + templateID | ~2.5 | ~2.5 |
| skeleton amortized (3 × ~110 B + directory) / 160 | 2.2 | 2.2 |
| dictionary amortized (~21 hot entries ≈ 340 B) / 160 | 2.1 | ~0 |
| tokens (typed ints/bools + dict refs vs literals) | ~44 | ~141 |
| graph overhead (locator/route/anchor/catalog) | ~1 | ~1 |
| **total** | **≈ 70** | **≈ 165** |

→ **≈ 7.0 MiB low / ≈ 16.5 MiB high per 100k**, against compact's
81.8 / 184.5 B/doc (7.8 / 17.6 MiB). The wins over compact and where they
come from: the 16 B/row group directory + chunk descriptors are replaced
by the envelope's ~8.6 B/row all-in addressing (−7 B); typed tokens save
~7–8.5 B/row (§3.4); canonicalization collapses shapes on real corpora
(0 on this order-fixed corpus — the projection takes no credit for it).
Cost taken: the hash-slot machinery (+~2 B/row vs binary-search-only) buys
the O(1) point read. **Gate: ≤ 7.8 / ≤ 17.6 MiB (never exceed compact);
projection 7.0–7.5 / 16.2–17.0.** Verbatim's 28.1 MiB is retired outright.

## 10. Speed: the arithmetic

### 10.1 GetRaw (whole document)

Verbatim p50 0.25–0.38 µs = route + leaf pin + cuckoo lookup + succinct
select + 249 B memcpy into `dst` + lease/epoch overhead **(verified
pipeline; measured band)**. Unified replaces the memcpy with the splice:
102.7 ns measured for this shape (§2.5), minus the ~15–20 ns memcpy it
subsumes, minus a few ns from tag-only/varint tokens, plus ~3–5 ns
empty-overlay check → **+80–95 ns ⇒ p50 ≈ 0.34–0.47 µs**. Trivial rows
render at verbatim parity. Uncounted upside: the working set shrinks 4×
(28.1 → ~7 MiB), so the in-workload band tightens at 100k and beyond.
**Gate: p50 ≤ 0.50 µs, 0 allocs; report the splice ns separately.**

### 10.2 Field probe (new capability, SQL-first argument)

Today the durable store has no field read: a point column read is
AppendRaw (copy) + `PointerCompiled` walk over the copy
(≥ ~250–400 ns for this shape, plus the copy) **(verified pattern:
`sql/driver/stmt.go:311`, `query/join_file.go:957-987`)**. The token view
resolves (template, path) → hole once per leaf-template (skeleton walk,
~150 ns, amortized over the leaf's rows) and reads a row's hole by
walking ≤ 12 one-byte tags + lengths: 20.4 ns measured for the class-3
equivalent (§2.5). **Gate: point field probe ≤ 100 ns end-to-end
(vs ≥ 500 ns whole-doc-parse today); scan-side per-row hole read ≤ 25 ns,
0 allocs.** This is the "field reads faster than verbatim" clause of the
gate, and the payoff surface is the SQL driver's row decode and join
probe loops.

### 10.3 Filter (the 40 ns/doc lane)

The harness lane runs `Cmp(country, Eq, "PT")` through the query executor
over the file scan **(verified: `engine_vibejson_durable.go:381-399`)**.
Unified adds a **token filter lane** to the file scan: when every
predicate path resolves to a hole in every template of a leaf, evaluate
tokens directly and render only survivors. Per doc: amortized hole
resolution ~3 ns + tag walk to hole ~4–6 ns + compare (1-byte dict id,
varint int, or short memcmp) ~2–4 ns + iteration ~2 ns ≈ **10–16 ns/doc**,
with 1 % survivors paying the splice (+~1 ns amortized). Non-resolvable
predicates fall back to render-then-filter ≈ 40.6 + ~85 ≈ ~125 ns/doc —
still 6.6× faster than SQLite's 830, and reported, not hidden. **Gate:
harness filter ≤ 40 ns/doc; target ≤ 20. Fallback lane recorded as its
own number.**

### 10.4 Update, insert, delete, checkpoint

Same-shape equal-length update: tokenize-and-compare (validate already
paid; BuildIndex ~150–250 ns + span walk ~50 ns + skeleton hash ~30 ns)
+ hole patch ~30 ns; journal/ack unchanged; the 4–8 KiB COW copy +
re-encode leaves the common path — net **≤ 4.6 µs held with headroom
(projection: −0.5–1 µs)**. Shape-changing update / insert / delete: one
O(row) record + link. **First update to a cold (bulk-built) leaf: same
O(row) path — the 2,301 µs event is structurally impossible; gate: cold
first-update p99 ≤ 10 µs.** Checkpoint: fold re-encodes dirty leaves
(~2–4 µs per 16 KiB leaf: placement + census over ≤ 190 rows + token
emission + seal) — ≤ 64 dirty leaves per window ⇒ +≤ 250 µs against
today's 330–355 µs buffered p50, partially offset by the COW staging the
overlay removed. **Gate: buffered checkpoint p50 ≤ 600 µs at the mixed
workload (target ≤ 450); update p50 ≤ 4.6 µs; churn ≥ today's unindexed
lane; 0 allocs steady-state.**

## 11. Phases and promotion gates

Every phase independently landable; every earlier gate stays green.
No migration anywhere: stores are recreated (standing rule).

| phase | lands | promotion gates (all must hold) |
|---|---|---|
| **U0** — library primitives | tape-canonical render/check (`AppendCanonicalIndexed`, `IndexIsCanonical`), typed-int admission predicate, skeleton/hole extractor reusing the compact span walk | differential vs `AppendCanonicalize` on the full corpus + fuzz; int round-trip differential (spelling → int64 → spelling identity over the admission predicate); render ≤ 250 ns / 0 allocs on the 250 B shape |
| **U1** — codec + bulk + reads | class 5 codec, one planner (§3.6), point/scan/posting read paths, token filter lane, `Create` option `UnifiedLeaves` (bulk-built stores; mutations to class-5 leaves route through the existing structural rewrite — correct, uncontested, not gated) | space ≤ 7.8 / 17.6 MiB (projection 7.0–7.5 / 16.2–17.0); GetRaw p50 ≤ 0.50 µs; filter ≤ 40 ns/doc; scan all-docs within today's scan gate; 0 allocs on point/scan/filter; `-count=2` byte-identical files; corruption: every section independently fail-closed, splice never reads outside slot bounds; template census recorded (shapes/leaf, trivial fraction) |
| **U2** — mutations | row/tombstone overlay records on the P0 arena, read-rule merge, in-place hole patch, dirty-leaf fold, journal-of-canonical-bytes, structural split/merge over unified leaves | update p50 ≤ 4.6 µs; cold first-update p99 ≤ 10 µs (baseline 2,301 µs); insert/delete churn ≥ today's unindexed lane; checkpoint p50 ≤ 600 µs; GetRaw mid-window (warm overlay of 64) ≤ 0.55 µs; 0 allocs mutation + read; bulk-vs-mutation **leaf** byte identity; crash matrix incl. record-link/fold boundary; `-count=2` |
| **U3** — the cutover | `UnifiedLeaves` becomes the only behavior; delete classes 1–4 as classes, `DocumentFormat`, de-compact/de-template machinery, both old planners; page envelope version bump; golden fixtures regenerated | full suite green with every U1/U2 gate re-run; indexed lanes (P0–P3 gates of indexed-write-path) unregressed; `docs/format.md` rewritten to one leaf class; zero references to deleted symbols |
| **U4** — accelerators (each optional, evidence-gated) | per-hole zone vectors (leaf skip), region checksums/reseal, SQL column probe over the token view, indexed-in-place relaxation via P0 zero-record rule | filter with 1 %-selectivity leaf skip ≥ 2× the U1 lane; region reseal ≥ 2× vs whole-leaf (lab already: 2.5×); SQL point column probe ≤ 100 ns; space regression ≤ 1 % |

## 12. What dies (deletion inventory, executed at U3)

- `CommonPrimaryLeafNarrow/Wide` as value codecs (the envelope machinery
  survives inside class 5); `UpdateTo/InsertTo/DeleteTo/Promote*To`
  (replaced by overlay + fold).
- Class 3 entirely: `common_primary_template_leaf.go`,
  `template_columnar_leaf.go`, the TC census/adoption rule — its two
  surviving mechanisms (offset field access, region reseal) are absorbed
  as §10.2 and U4.
- Class 4 entirely: `common_primary_compact_leaf.go`, the embedded
  document-group leaf payload, `CommonPrimaryCompactLeafMaxRows`, the
  packing search's de-templatability cap. (`document_group.go` itself
  retires with the legacy chunk engine on the unification track.)
- `DocumentFormat` / `DocumentFormatCompact` / `PrimaryLeafCompact` /
  `PrimaryLeafAdaptive` policy plumbing (`store_file.go:200-235`,
  `primary_graph.go:392-615` planners → one planner).
- The de-compact state machine: `AdmittedPrimaryLeafForMutation`'s
  detemplate branches, both `DetemplateRecords`, `detemplateEvents`
  diagnostics, and the 64 KiB de-compact rationale for
  `primaryLeafScratch` (the scratch itself shrinks to fold/structural
  use).
- The class dispatch branches in `appendPrimaryLeafValue`,
  `PrimaryGraphCursor.openLeaf`, `VisitPrimaryLeafPostingRows` — each
  becomes single-class.

## 13. Rejected alternatives (cross-cutting)

- Canonical spans + page dictionary compression — §4(a).
- Two classes + promoter — §4(b), owner-rejected.
- Fully columnar leaves — §4(c).
- Sub-templates for nesting — §4 tail.
- Parse-time canonicalization in the tape builder — §8.
- Compact-at-rest + decoded resident cache — owner-rejected as two
  systems; also loses determinism-of-residency and doubles memory
  accounting.
- Mutating the leaf image per operation (no overlay) — §7.1: couples
  update cost to leaf geometry and forces small leaves, which is the
  measured reason class 3 adopted 0 %.
- Lexical-rank posting slots (compact's discipline) for the unified
  class: rank shifts on insert/delete ⇒ every insert becomes a bucket
  rebase; the stable hash-slot discipline keeps slot stability the norm
  (indexed-write-path §2.2) — the envelope already provides it.

## 14. Honest limits

- **The splice tax is real.** Whole-document reads of templated rows pay
  ~+85–95 ns over a memcpy, and no cleverness removes assembling from
  fragments. Bounded by the trivial-row floor and repaid by the 4× space
  cut and field/filter wins; the U1 gate pins the accepted number.
- **Fold pays for churn.** A window that dirties many large leaves grows
  checkpoint cost (O(dirty leaf bytes)); bounded by the ≤ 600 µs gate and
  the existing pressure-escalation discipline.
- **Mid-window inserts hold provisional slots**; a fold may reassign
  them (rebase inside the fold's own transaction). Probes between folds
  resolve through the overlay, so no reader observes the reassignment —
  but the invariant load-bearing here is fold-internal consistency
  (INVARIANT 6).
- **Small leaves amortize poorly.** A 4 KiB unified leaf carries the
  ~980 B envelope (~25 %); the planner avoids the geometry except for
  sparse edges, and the census reports the tail.
- **Canonicalization changes returned bytes.** Consumers that stored
  non-canonical spellings get canonical ones back. Permitted by the
  product ruling; differential tests move to semantic equality only where
  the engine canonicalizes, never against independent oracles silently.
- **Number spellings outside the provable-int class are preserved, not
  packed.** Floats/exponents stay literals; any future numeric packing
  needs its own losslessness proof (out of scope, by directive).

## INVARIANTS

1. **One representation.** Every primary leaf is class 5. There is
   exactly one leaf encoder (shared by bulk build and fold), one point
   read, one scan row source, one posting enumerator. No resident decoded
   alternative representation of leaf content exists.
2. **Canonical determinism.** The stored spelling of a document is a pure
   function of its JSON value (canonical form §3.2); leaf bytes are a
   pure function of (final row set, leaf seed, extent rule); store files
   are `-count=2` byte-identical; bulk build, incremental mutation +
   fold, and journal replay of the same final state produce byte-
   identical leaves.
3. **Losslessness.** Every token class round-trips its exact canonical
   spelling: dictionary and literal by storage, `true`/`false`/`null` by
   grammar uniqueness, canonical ints by the §3.4 admission predicate.
   No number token is rewritten outside that predicate.
4. **O(row) mutations.** No operation on any mutation path reads,
   decodes, or re-encodes more than its own row (plus O(1) records and
   links). Leaf re-encoding happens only at fold points (checkpoint,
   structural, canonical lane). De-templating does not exist.
5. **Read-rule purity.** A snapshot at generation G resolves every row as
   (fold base, overlay records ≤ G) — exact per-generation views,
   indefinitely, under the P0 reclamation floor.
6. **Posting-slot agreement.** A row's posting bit is derived from the
   same (bucket, slot) the read path selects it by, at every generation;
   slot reassignment occurs only inside a fold transaction that
   re-derives the affected bucket's postings from the same final rows.
7. **Graceful degradation.** Every JSON value the store admits is
   representable; the worst-case at-rest cost of any document is its
   canonical spelling + O(1) bytes (trivial row), and its render is one
   memcpy. No conformance cliff exists.
8. **Contract stability.** `AppendRaw` never returns borrowed page
   spans; `Range*` spans are valid for one callback; the token view
   borrows under the same lease/epoch rules as every admitted-page span.
9. **Zero-GC steady state.** Point reads, scans, filters, mutations, and
   probes allocate nothing per operation; arenas, dictionaries, and
   scratch grow only at fold/pressure boundaries under the exclusive
   writer.
10. **Locality for parallel writers.** Templates, dictionaries, and
    overlay chains are leaf-local; overlay arenas shard per write token;
    publish remains O(pointer links). Nothing in this format is
    collection-global mutable state.

## MIGRATION

No backward compatibility is owed (pre-release; standing no-migration
rule). By phase: **U0/U2** none (library + resident/journal semantics;
the journal record format is unchanged — only its value bytes become
canonical). **U1** adds class byte 5 behind a Create option; existing
stores open unchanged. **U3** is the breaking step: the common page
envelope version bumps (4 → 5) so every page from the superseded layout
fails closed; classes 1–4, `DocumentFormat`, and the de-compact machinery
are deleted; golden fixtures under `internal/storeio/testdata/format0/`
are regenerated; stores created before U3 are recreated, not migrated.
`docs/format.md`'s class 1–4 sections are replaced by the class 5 layout
(§3.1).
