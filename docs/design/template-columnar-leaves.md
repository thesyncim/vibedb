# Template-columnar leaves

**Status:** the codec is built and wired as primary leaf class 3
(`CommonPrimaryLeafTemplate`, internal/storeio/template_columnar_leaf.go
wrapped in internal/storeio/common_primary_template_leaf.go), and the bulk
builder is able to select it per leaf. But the honest leaf-geometry
investigation found it **adopts 0% under the production gates at every leaf
size** on the corpora that matter, so it is not on any real write path. The
space win that actually landed on the primary graph is a different codec — the
compact document-group leaf (class 4) — recorded in
[The space that landed: the compact leaf](#the-space-that-landed-the-compact-leaf).
One template hypothesis survives, in
[The surviving hypothesis](#the-surviving-hypothesis).

**Idea:** keep the ordered leaf envelope, store repeated JSON structure once,
and address varying fields as packed slots. Selection is per leaf, with raw
fallback.

## Observation

The ordered primary graph stores document bytes verbatim in its leaves. For
realistic small JSON documents, a large fraction of those bytes — field names,
punctuation, repeated structure — is identical across documents that share a
shape, and the remaining costs of the store trace back to that redundancy: the
space floor is raw bytes, every typed access re-parses text, every filter scans
text, every same-shape update rewrites bytes that did not change, and every
checkpoint reseal hashes them.

## Design

A template-columnar leaf keeps the existing envelope — stable slots, control
bytes, lexical rank permutation, key heap, adaptive 4-64 KiB classes,
checksums — and changes only how document bytes are laid out:

- A per-leaf, content-addressed **template dictionary**. A template is the
  exact byte skeleton of one document shape: every invariant byte, with typed
  holes where values vary. Reconstruction is a deterministic splice, so the
  exact original JSON spelling round-trips byte for byte.
- Each document row stores its template reference plus **packed field slots**:
  the variant bytes for each hole, offset-indexed so one field is addressable
  without touching the rest.
- A per-field **zone vector** in the leaf header: min/max plus a null/absent
  mask per template hole, so range and equality predicates prune whole leaves
  before touching rows, with no secondary index and no write-path index
  maintenance.
- **Region checksums**: the leaf checksum tree covers the template dictionary
  and each column region separately, so an in-place field patch reseals only
  its region and the root, not the whole leaf.
- Class selection is measured and per leaf: a leaf whose documents do not share
  templates profitably stays raw. The bulk builder chooses by measured encoded
  size (`planTemplateLeafCount`); the row count is capped at what a raw wide
  leaf holds so a later mutation can always de-template it back into the raw
  envelope.

This is a leaf codec and access-path change, not an engine: tablet routing,
BucketID stability, snapshots, COW, frame-native staging, epoch-protected
reads, and durability contracts are untouched.

## The lab verdict was strong in isolation

The v2 lab measured, on the codec alone: 64.9% space saving on the
low-cardinality competitive shape (258.4 to 90.6 B/doc), 35.2% on
high-cardinality, 0% adversarial overhead with raw fallback, 20.4 ns field
access, and region reseal 2.5x cheaper than a whole-leaf reseal — with two
honest misses: 102.7 ns splice against the 30 ns aspiration, and fused
extraction at +55.7% of a bare validation pass. Two of those mechanisms —
region reseal and offset-indexed field access — are unambiguous wins that
survive independently of the class decision.

## Why the class adopts 0% on the real graph

Integrating the class and measuring the leaf geometry the graph actually
builds retired the "blanket default" hypothesis. Under the adaptive selection
rule — adopt the template class only when it stores the same rows at least 25%
below the raw leaf's page cost per document — the census on the exact
competitive corpus adopts it for **zero** leaves
(internal/storeio/store_file_primary_tc_census_test.go,
`TestPhase0TCCensus`). The measured reasons compound:

- The graph's leaves are small (a template row count capped at raw wide leaf
  capacity so it stays de-templatable), the per-document values are near-unique,
  and the fixed per-document row plus column/zone directory overhead dominates
  the per-doc byte cost — exactly the regime where structure-once saves little.
- Making the leaf larger would amortize that fixed directory overhead and let
  the space case close, but a larger template leaf loses on mutation cost:
  de-templating and region reseal both scale with leaf size, so the geometry
  that would pay for space costs more on every write. No single leaf size wins
  both axes, so the honest gate adopts the class nowhere.

The class byte, the codec, and the selection rule remain in the tree because
they are correct and the two surviving mechanisms depend on the codec; the
class is simply never chosen by a production build.

## The space that landed: the compact leaf

The space win on the primary graph came from a different, byte-exact codec: the
**compact document-group leaf**, primary leaf class 4
(`CommonPrimaryLeafCompact`, internal/storeio/common_primary_compact_leaf.go),
selected by `PrimaryLeafCompact` / `DocumentFormat: DocumentFormatCompact`. It
embeds the container-independent grouped payload — one shared shape-template
table and value dictionary per leaf, each row reduced to a dictionary/literal
token stream — so a document stored compact on the ordered primary graph
reconstructs byte for byte identically to the same document stored compact in
the legacy chunk layout. It reuses the `PagePrimaryLeaf` kind and the leaf's
stable BucketID, so the router, catalog, COW, snapshots, and the exact-index
maintainer route to it exactly as to any other leaf; only the payload codec
differs.

The 2026-07-30 published competitive footprint confirms that a 100k-document
compact primary graph occupies **7.8 MiB low-cardinality / 17.6 MiB
high-cardinality** apparent space — below the legacy chunk-layout compact
footprint at both cardinalities. It is bulk-only, explicitly selected
evidence, and the reader never has to know which format wrote the file.

## The surviving hypothesis

One template idea is not refuted, only unbuilt: a **TC-dedicated
split-on-write class** whose split policy is chosen for template density rather
than shared with the raw adaptive selection — packing same-shape rows into a
leaf sized to amortize the directory while keeping the mutation granule small,
instead of asking one adaptive size to win both axes. A back-of-envelope
projection puts it near **128.5 B/doc**. It is a projection with no code; it
gates on a lab that measures the split policy's space and mutation cost
together, exactly as the codec lab measured the codec.

## Mechanisms worth keeping regardless of class

| Mechanism | Status |
| --- | --- |
| Region reseal (checksum only the dirtied region) | measured 2.5x cheaper than whole-leaf; codec-level win |
| Offset-indexed field access, zero-alloc | measured 20.4 ns/field; codec-level win |
| Blanket template default | retired: 0% adoption under the honest gates |
| Compact document-group leaf (class 4) | landed; the space win on the primary graph |

## Qualification gates (the codec lab contract)

The isolated codec passed these; the class did not clear the graph-level
adoption gate above. They remain the contract any future template-density class
(the surviving hypothesis) must re-run against its own split policy:

1. Encoded bytes per document vs the raw leaf, per corpus.
2. Compiled `AppendRaw` splice cost; ordered all-bytes scan within the scan
   gate; predicated scans splice only surviving rows.
3. Field access: local, zero allocations.
4. Fused template extraction measured as one pass, not validation plus a second
   walk.
5. Same-template field patch plus region reseal, measured against the
   whole-leaf reseal it replaces.
6. Corruption: every region independently fail-closed; grafted template
   dictionaries rejected; splice never reads outside its slot bounds.
