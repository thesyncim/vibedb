# Template-columnar leaves

This is the design and qualification plan for a third primary-leaf class.
Every number here is a projection or a gate until the lab says otherwise;
nothing in this document licenses a performance claim.

## Observation

The ordered primary graph now stores document bytes verbatim in its
leaves. For realistic small JSON documents, a large fraction of those
bytes — field names, punctuation, repeated structure — is identical
across documents that share a shape, and the remaining costs of the
store trace back to that redundancy: the space floor is raw bytes, every
typed access re-parses text, every filter scans text, every same-shape
update rewrites bytes that did not change, and every checkpoint reseal
hashes them.

## Design

A template-columnar leaf keeps the existing envelope — stable slots,
control bytes, lexical rank permutation, key heap, adaptive 4-64 KiB
classes, checksums — and changes only how document bytes are laid out:

- A per-leaf, content-addressed **template dictionary**. A template is
  the exact byte skeleton of one document shape: every invariant byte,
  with typed holes where values vary. Reconstruction is a deterministic
  splice, so the exact original JSON spelling round-trips byte for byte.
- Each document row stores its template reference plus **packed field
  slots**: the variant bytes for each hole, offset-indexed so one field
  is addressable without touching the rest.
- A per-field **zone vector** in the leaf header: min/max (bytewise for
  strings, numeric for canonical numbers) plus a null/absent mask per
  template hole. Range and equality predicates prune whole leaves before
  touching rows, for every field, with no secondary index and no
  write-path index maintenance.
- **Region checksums**: the leaf checksum tree covers the template
  dictionary and each column region separately, so an in-place field
  patch reseals only its region and the root, not the whole leaf.
- Class selection stays measured and per leaf: a leaf whose documents do
  not share templates profitably stays raw. The bulk builder chooses by
  measured encoded size; runtime reclassification follows the existing
  bounded slot-class rewrite rules.

Read paths:

- `AppendRaw` splices template segments with field bytes — a handful of
  bounded memcpys instead of one.
- Field projection (`GetRaw` pointer, SQL column access, query
  predicates) reads one slot through the offset index — no JSON scan.
- Ordered scans with predicates evaluate column-wise over the leaf and
  materialize only surviving rows.

Write paths:

- Ingest already parses for validation; the same pass yields the
  template and field extraction at marginal cost.
- A same-template update diffs field slots; unchanged fields are
  untouched bytes, the exclusive in-place window shrinks to the changed
  slots, and the reseal covers only dirtied regions.
- Cross-template updates fall back to whole-row replacement inside the
  leaf, and COW/split rules are unchanged.

Everything else — tablet routing, BucketID stability, snapshots, COW,
frame-native staging, optimistic reads, durability contracts — is
untouched. This is a leaf codec and access-path change, not an engine.

## Why this is the highest-leverage remaining change

| Axis | Mechanism | Status |
| --- | --- | --- |
| Disk and cache space | structure stored once per leaf | projected 35-55% below verbatim leaves on representative corpora; lab gate |
| Typed/field access | column offsets, no parse | projected order-of-magnitude on field reads; lab gate |
| Predicate scans | leaf zone vectors + column evaluation | projected leaf pruning comparable to indexed access for selective ranges; lab gate |
| Update bytes | field-slot diff + region reseal | projected reseal cost O(changed region); lab gate |
| Raw point read | template splice overhead | projected small regression vs one memcpy; hard gate: within the existing read budgets |
| Secondary-index pressure | zone vectors absorb range predicates | postings remain for high-selectivity exact terms |

## First lab verdict and the v2 mechanisms

The v1 lab (template + offset-indexed slots, naive extraction, memcpy
splice) measured: field access 14.1ns and region reseal 26x cheaper than
whole-leaf — both promoted mechanisms — but only 15.5% space on the
competitive shape, 96.6ns splice, and extraction at 2.3x a bare
validation pass. Three lessons, three v2 mechanisms:

- **Fused extraction.** The structural validation pass already visits
  every boundary byte; the shape fingerprint and value spans must be
  emitted by that pass, never by a second walk.
- **Compiled splice programs.** A template compiles once into a gather
  program executed with the encoder-execute overlapping-store
  discipline; per-segment cost falls from memcpy-call overhead to
  vector-store cost. Predicated scans never splice rejected rows.
- **Value tier.** Structure is a minority of representative corpora; the
  space gate needs the per-leaf content-addressed value dictionary for
  repeated value spans, and per-doc metadata becomes varint length
  vectors reconstructed by prefix sum instead of offset directories.

## v2 verdict and adoption policy

The v2 lab measured: 64.9% space saving on the low-cardinality
competitive shape (258.4 to 90.6 B/doc), 35.2% on high-cardinality, 0%
adversarial overhead with raw fallback, 20.4ns field access, region
reseal 2.5x cheaper than whole-leaf — and two honest misses: 102.7ns
splice against the 30ns aspiration and fused extraction at +55.7% of a
bare validation pass (+84ns/doc, which is single-digit percent of the
complete bulk pipeline; the validation-relative gate used the wrong
denominator and is superseded).

Adoption is therefore per-leaf class policy, not a blanket default: the
builder selects the template-columnar class when the measured leaf-level
saving is at least 25% and the collection policy accepts splice-priced
full-document reads; raw remains the class for full-scan-dominated
tablets. Field projection, predicated scans, and zone pruning are
strictly faster under the class; whole-document materialization pays the
splice and is said so plainly. One further splice iteration (run
coalescing, batched dictionary resolution) may narrow the gap but does
not gate integration.

## Qualification gates (isolated lab first)

1. Encoded bytes per document vs the raw leaf, per corpus: ≥40% on the
   low-cardinality competitive corpus (templates plus value dictionary),
   ≥25% on the high-cardinality variant (structure only), ≤2% overhead
   on the adversarial unique-shape corpus (raw fallback evidence).
2. Compiled `AppendRaw` splice ≤30ns at the representative shape;
   ordered all-bytes scan within the 60ns/doc gate; predicated scans
   splice only surviving rows.
3. Field access: ≤25ns per field local, zero allocations.
4. Fused template extraction: ≤15% over the bare validation pass,
   measured as one pass, not validation plus a second walk.
5. Same-template field patch + region reseal: measured against the
   whole-leaf reseal it replaces.
6. Corruption: every region independently fail-closed; grafted template
   dictionaries rejected; splice never reads outside its slot bounds.

Integration only after the lab passes: bulk-build class selection first,
read paths second, mutations third, exactly like the primary graph's own
sequence. The lab lives in internal/storeio as an isolated codec with
benchmarks, following the promotion discipline every other candidate has
used.
