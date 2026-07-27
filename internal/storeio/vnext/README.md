# Production layout qualification

This package is the comparative qualification harness for production Store
layout primitives. Implementations graduate into the parent `storeio` package
once their format, corruption boundary, allocation behavior, space accounting,
and read-latency gates are proven here.

The design is not an LSM and has no reader-visible journal, tombstone layer,
forwarding page, or delta overlay. One immutable root continues to name one
canonical state.

## Unified layout

- A keyed fingerprint directory stores no key bytes. Every candidate is
  verified against the complete key in its record block.
- Fingerprint leaves store 316 entries in one 4 KiB frame. An entry is an
  implied eight-bit hash prefix, a 56-bit hash suffix, and a packed 26-bit
  stable block ID plus six-bit slot.
- A stable block map replaces the current chunk directory; it must never become
  a third lookup layer. Ordinary raw/group block references use a 12-byte
  physical locator (43-bit quantum offset, 48-bit generation, four-bit extent
  span, one-bit kind), or 0.1875 bytes per full 64-row block. The selecting
  shard supplies logical identity and the admitted document header plus exact
  key comparison verifies it. Rare groups with derived sidecars use an
  explicitly accounted extended reference.
- The 50-bit immutable route shard is the collision-correct control, not the
  final space target. It packs a 16-bit independent tag, a 32-bit stable
  location, and two state bits; the complete document key remains authoritative.
  Production scale instead requires shard-local row IDs and a separately
  accounted immutable block map so the complete resident route stays at or
  below 5.00 bytes per current key.
- Raw blocks retain at most 64 stable slots. Each slot uses a direct four-byte
  `(row start, key end)` record, and JSON ends at the next live slot. A fixed
  48-byte, six-bit lexical permutation orders those stable slots without
  duplicating keys. Block-local lower-bound and iteration compare or return the
  authoritative key bytes in-place; a cross-block ordered tree therefore needs
  only shortest fences and block references, not one copied key per document.
- Selective packed blocks store one shared JSON prefix and suffix plus one
  contiguous independently decodable middle per row. They retain the same
  four-byte stable-slot directory and six-bit lexical permutation as raw
  blocks. A block stays raw unless the packed candidate saves at least one
  4 KiB quantum and 12.5% of the raw physical span. Packed promotion is
  disabled unless the integrated reader has separately cleared the
  read-latency gate for the production build and CPU.
- Data spans may be any 4 KiB multiple from 4 through 64 KiB. Metadata remains
  fixed at one quantum until the cache and allocator support exact spans.
- A canonical term leaf owns either an inline posting payload or a direct
  manifest entry for one immutable component. There is no
  directory-to-posting lookup layer. One posting tile covers 64 existing
  stable chunks, or 4,096 rows, and deterministically chooses the smallest of
  empty, all-live, dense, maximal runs, sparse chunk masks, and sparse row
  deltas. Dense tiles are bounded at 512 bytes; payloads of at most 24 bytes
  stay in the term leaf.
- Manifest posting components have a typed 128-bit content identity. Hash
  equality is only a lookup accelerator: integration must compare type,
  length, and complete bytes before sharing. Components are immutable and
  snapshot reclamation remains governed by root reachability and generation
  leases, never by an in-place refcount.

## Invariants

- Fingerprints route only; exact key bytes decide identity.
- A resident route root and its block-map root publish as one generation.
  Readers never consult a route delta, tombstone overlay, or mismatched map.
- The route, block map, and ordered root publish atomically. Historical
  snapshots retain the matching immutable roots.
- Stable block IDs never encode physical offsets.
- An existing-key replacement does not rewrite the fingerprint directory unless
  its stable location changes.
- Pages and blocks are immutable after publication.
- No allocator may reuse an extent reachable from an active snapshot or either
  validated recovery root.
- A block map replaces the current chunk radix and must match or improve its
  depth, cache footprint, and zone-summary behavior.

## Gates

- Warm raw-block exact lookup: no slower than the current document page by more
  than 2%.
- Warm fingerprint leaf probe: no slower than the existing exact-rechecked
  fingerprint leaf.
- Packed `AppendRaw`: within 2% of raw lookup plus copy, with no shared decode
  buffer and no fragmented reconstruction program.
- Zero allocations for encode, open, hit, miss, and exact collision handling.
- Raw physical bytes at most 1.20 times key plus JSON bytes on the representative
  small-document corpus.
- Packed physical bytes at most 0.75 times key plus JSON bytes on repetitive
  blocks.
- Incompressible or weakly compressible blocks have exactly one canonical raw
  representation; readers never probe a packed alternative.
- Fingerprint leaf at most 24 physical bytes per key at 70% occupancy.
- Resident route plus block map, top-level directory, padding, load slack, and
  current-root allocation at most 5.00 bytes per current key at 100 million,
  one billion, and modeled 100 billion-key scale. Retained snapshot history is
  reported separately against an explicit byte cap and never hidden in this
  ratio.
- At least 99.9% of random absent resident-route probes acquire no document
  extent. A tag match still performs a complete-key comparison and never
  becomes identity.
- No extra point-read page or cache miss after integration.
- Rare postings stay in the term leaf. A non-inline tile is referenced
  directly by that term's manifest entry; promotion is forbidden if
  integration introduces a second directory descent.
- Warm build, admission, and iteration allocate zero times for every posting
  codec.
- Against the current 32-byte `(index, hash, chunk, mask)` leaf record, the
  representative per-tile posting-space kill gates are:
  - all-live at most 1%;
  - dense at most 30%;
  - maximal runs at most 5%;
  - one wide sparse mask at most 60%;
  - one row per chunk at most 10%;
  - one inline singleton at most 30%.
- The all-live codec may be promoted only when the tile live mask is already
  co-resident with the term-leaf/manifest lookup. It must not add a live-mask
  I/O or permit a deleted slot to survive reuse.
- The integrated warm iterator must not regress equality-query p99 by more
  than 3%; cold rare equality must read no more pages than the current tree.
- Malformed canonical varints, duplicate rows/chunks, non-maximal runs,
  singleton masks in the wide spelling, non-smallest codecs, dirty inline
  tails, component-ID mismatches, and posting bits outside the live universe
  must all fail admission.
- A small isolated replacement normally rewrites one 4–8 KiB data extent.
- A 90% delete trace returns to within 10% of a fresh rebuild after explicitly
  bounded local canonical-maintenance steps.

The trace uses a bounded recent-block placement window and reports its probes.
Its write-byte counters cover data extents only; fingerprint-directory,
block-map, allocator, retirement, and publication-root traffic must be measured
by the integrated Store before promotion. Resident codec benchmarks likewise
qualify codecs, not the complete point-read path.

Production primitives live in the parent `storeio` package and are promoted
into collection open, recovery, and the page cache one atomic path at a time.
The integrated format still must pass span-aware cache allocation, crash
injection across every 4 KiB-multiple extent, and a point-read comparison
against the current key-tree and chunk-radix path before it replaces that path.

The encoder deterministically emits maximal common JSON edges. The decoder
accepts any structurally valid edge decomposition; canonicality is therefore a
writer/publication invariant, while the checksum and structural validation are
the corruption boundary.
