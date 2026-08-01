# On-disk format

This document maps the one mutable format written by `store/durable` and
decoded by `internal/storeio`. The codecs are the byte-level authority. All
multi-byte integers are little-endian, all reserved bytes are zero, and all
format-version fields inside the primary store file contain
`DevelopmentFormatVersion == 0`. The separate recovery-journal sibling has its
own explicitly gated format field; its supported versions 0 and 1 are described
below.

The project is unreleased. A primary-file schema change edits format 0 in place
and regenerates the format-0 golden images. There are no migration decoders,
alternate primary-file development versions, or retired page layouts. Recovery
journal v0 compatibility is an explicit record-grammar contract, not a primary
store migration decoder.

## File layout

`MutableStoreLayout(pageSize)` defines the fixed prefix:

| Region | Offset | Length |
| --- | ---: | ---: |
| inline root slot 0 | `0` | `PageSize` |
| inline root slot 1 | `PageSize` | `PageSize` |
| materialization journal 0 | `2*PageSize` | `4096` |
| materialization journal 1 | `2*PageSize + 4096` | `4096` |
| allocator padding | after journal 1 | to the next `PageSize` boundary |
| allocated pages | `DataStart` | remainder of file |

The base `PageSize` is 4096 bytes. Variable extents are bounded by the persisted
`MaxPageSize`. No allocated reference or materialization target may begin below
`DataStart`.

The two inline root slots alternate on publication. Recovery validates both,
selects the newest complete generation, performs any required materialization
journal rollback, and validates every top-level reference before exposing the
store.

## Inline root

`inline_superblock.go` defines the checksummed `SJINL000` record. Its first
4096 bytes contain:

| Offset | Field |
| --- | --- |
| 0:8 | magic `SJINL000` |
| 8:12 | version `0` |
| 12:16 | record size `4096` |
| 16:20 | reserved | zero |
| 20:24 | base page size | `4096` |
| 24:40 | generation and its complement |
| 40:48 | allocated file end |
| 72:88 | store id |
| 96:608 | embedded 512-byte `StateRoot` payload |
| 608:4088 | version-0 inline free-set delta and reserved zero space |
| 4088:4096 | CRC32C and its complement |

Bytes after the 4096-byte record up to `PageSize` are zero. The inline free run
holds at most one latest operation per extent offset. When it fills, it spills
to the external free-image/delta graph and starts a new cumulative run.

## Common page envelope

Every allocated page uses `page.go`'s 64-byte header and 8-byte trailer:

| Offset | Field | Rule |
| --- | --- | --- |
| 0:8 | magic | `SJPAGE00` |
| 8:10 | version | `0` |
| 10:12 | header length | `64` |
| 12 | kind | one current `PageKind` |
| 13:16 | reserved | zero |
| 16:20 | physical extent size | validated against the allocation geometry |
| 20:24 | payload length | excludes header, padding, and trailer |
| 24:32 | generation | non-zero |
| 32:40 | logical id | non-zero |
| 40:56 | store id | must match the owning file |
| 56:64 | reserved | zero |
| 64:... | payload then zero padding | kind-specific |
| final 8 bytes | CRC32C and complement | covers everything before the trailer |

`PageRef` is 32 bytes: physical offset, logical id, generation, length, kind,
and three reserved zero bytes. A reference is admitted only when its range,
identity, generation, kind, and allocation geometry agree with the selected
root. Logical id `0` is invalid; id `1` is the first real page identity and the
base of the primary-leaf namespace. The inline root is not a common page and
does not consume a logical id.

The current page kinds are:

| Value | Kind |
| ---: | --- |
| 1 | `PageOverflow` |
| 2 | `PageIndexPosting` |
| 3 | `PageFreeImage` |
| 4 | `PageFreeDelta` |
| 5 | `PageFreeIndex` |
| 6 | `PageCatalogSegment` |
| 7 | `PagePrimaryCatalog` |
| 8 | `PagePrimaryLocator` |
| 9 | `PageTabletRoute` |
| 10 | `PagePrimaryAnchor` |
| 11 | `PagePrimaryLeaf` |
| 12 | `PagePrimaryExactRoot` |
| 13 | `PagePrimaryExactLeaf` |
| 14 | `PagePrimaryExactCatalog` |

No decoder probes payload bytes to guess another page kind.

## StateRoot

The inline root embeds the sole 512-byte payload defined by `state_root.go`.
There is no separately allocated state-root page. The active prefix is:

| Offset | Field |
| --- | --- |
| 0:4 | version `0` |
| 4:8 | option bits |
| 8:16 | document count |
| 16:24 | next logical id |
| 24:28 | physical exact-index count |
| 28:32 | maximum index depth |
| 32:40 | exact catalog rejection hash |
| 40:44 | qualified materialization damage granule |
| 44:48 | maximum physical extent size |
| 48:80 | canonical catalog head |
| 80:96 | canonical catalog digest |
| 96:100 | canonical catalog byte length |
| 100:112 | maximum key, inline value, and document lengths |
| 112:144 | ordered-primary root |
| 144:160 | paired recovery-journal id |
| 160:192 | exact-index root, or zero when no index exists |
| 192:512 | reserved zero suffix |

The ordered-primary root is the sole document root. A mutable store cannot open
without it. The only current option bits are schema binding and canonical
materialization qualification; unknown bits fail closed.

## Ordered primary graph

The document graph is:

```text
StateRoot.PrimaryRoot
  -> PagePrimaryCatalog
     -> PageTabletRoute / PagePrimaryLocator
        -> PagePrimaryAnchor
           -> PagePrimaryLeaf
              -> inline canonical JSON or PageOverflow chain
```

The catalog and tablet codecs live in `global_tablet_catalog.go`,
`segmented_tablet_router.go`, `primary_bucket_identity.go`, and
`tablet_anchor_map.go`. They route lexical fences to stable tablet, leaf, and
slot identities. `PagePrimaryLeaf` uses the unified canonical grammar in
`common_primary_unified_leaf.go`: a succinct ordered envelope, optional shape
templates, scalar dictionary entries, typed tokens, and complete trivial JSON
spellings when templating does not save space. This is one encoding grammar,
not a selectable document-format mode.

Values up to `InlineValueBytes` remain in the leaf. Larger values use a
forward-linked `PageOverflow` chain whose fixed header and every reference are
validated before bytes are returned. Publication creates the overflow chain
and its referencing leaf in one generation.

## Exact indexes

`StateRoot.ExactIndexRoot` names one version-0 `PagePrimaryExactRoot`. Its
records are ordered by physical index id and contain a leaf count plus a
`PagePrimaryExactCatalog` reference. Logical aliases share one physical record.

An exact catalog is a bounded one- or two-level ordered tree over
`PagePrimaryExactLeaf` pages. Each leaf wraps one canonical `IndexTermLeaf`
stream. Content-defined term runs and fixed giant-term tile stripes make leaf
boundaries deterministic for the final posting content. Open walks the entire
catalog, validates order and routing prefixes, and admits every posting against
the selected primary graph's live slot masks.

## Catalog, free space, and journals

`PageCatalogSegment` stores the canonical schema and exact-index definition.
StateRoot records its byte length and digest, so reopen reconstructs and hashes
the exact bytes rather than trusting a compact rejection key.

The allocator publishes a free image plus bounded deltas through
`PageFreeImage`, `PageFreeIndex`, `PageFreeDelta`, and the inline free run.
Every extent is aligned, non-overlapping, above `DataStart`, and absent from the
selected live graph before reuse.

The paired recovery journal (`recovery_journal.go`) is the separate
`<store>.rjournal` file used by synchronous mutation acknowledgement and by
eligible buffered-visible checkpoint deltas. Its identity is bound by both the
store id and `StateRoot.JournalID`. Readers never consult it.

The journal starts with two alternating 512-byte header sectors followed by a
sector-aligned, fixed-capacity record region. A header records its independent
format version, store and journal ids, page and sector geometry, base generation
and sequence, capacity, and monotonic recycle count under a CRC32C and its
complement. Create sizes the complete file once before publishing its identity;
later positional appends never extend it. Linux normally reserves the blocks
with `fallocate` and falls back to a one-time truncate only where allocation is
unsupported; other platforms set the complete size with truncate.

The current ordinary buffered-delta policy caps, and with the shipped overlay
geometry selects, a 2.5 MiB record region. Its foreground admission guard keeps
up to 512 KiB for one estimated future carried suffix, leaving the qualified
2 MiB current append window. This reserve is a bounded fallback policy rather
than another on-disk region: an exact current batch that does not fit, or a
future suffix wider than the reserve, takes the physical checkpoint path. The
two 512-byte headers are additional to the record-region capacity. Per-mutation
v0 journals retain their option-derived bounded capacity rather than inheriting
the 2.5 MiB delta policy.

Every record begins with the 32-byte `RRJ0` prefix and ends with a CRC32C and
its complement, then zero padding to the 512-byte damage granule. The prefix
carries kind, reserved zero bytes, sequence, generation, and either key/value
lengths or batch entry-count/body-length. One checksum covers the complete
ordered batch, so a torn batch is not partially admitted.

The header gates two record grammars:

- format v0 is the legacy put/delete grammar. Batch entries are full put or
  delete operations that all belong to the batch's single generation;
- format v1 extends batches with compact scalar-patch entries for the ordinary
  buffered delta lane. A scalar patch stores the key, new canonical integer,
  boolean, or null spelling, a 16-bit canonical byte offset, an 8-bit old
  spelling length, one reserved-zero byte, and the expected CRC32C of the
  complete resulting canonical document. It is emitted only when smaller than
  carrying that complete document. Scalar patch is never valid as a standalone
  record kind.

Opening a v0 journal preserves the v0 grammar for later appends; it is not
silently upgraded. A scalar-patch kind authenticated inside v0, an unknown
format, malformed metadata, or an attempt to use a v1 journal with another
runtime durability lane fails closed. The v1 word occupies a field that older
binaries required to be zero, so they reject v1 before interpreting its entry
kinds.

The fixed materialization journal slots inside the primary file carry complete
before-image sectors for qualified in-place canonical page updates. Recovery
rolls back an incomplete materialization before selecting a root. Readers never
consult either journal.

### Online block reclamation

After a physical root is durable and the paired redo journal has been recycled
through that root, the foreground completion path may deallocate filesystem
blocks beneath extents already absent from every admissible root. This changes
only physical allocation: allocator records and the apparent file length are
unchanged.

The scheduler grants one pass to each newly authoritative physical generation;
journal-only `Flush` boundaries do not run it. Under the same snapshot gate
used to publish durable state it samples the exact current/fallback roots and
journal base, raises the direct-reader fence, and copies a bounded candidate
window. One pass inspects at most 1,024 exact free identities and 64 coalesced
physical runs across reusable, pending-retirement, and absorbed-retirement
sources. Active sources share the discovery budget, with no more than three
redistribution rounds. Spending is separately capped at six successful
deallocation calls and 20 MiB. Oversized identities advance in bounded chunks
across later physical generations.

Candidate values are copied while readers are diverted, but the scheduler
releases both the reader fence and snapshot gate before validation and before
any filesystem syscall. The caller's writer lock still prevents allocator
reuse. The generation guard records the exact authority returned by a
successful planning pass, so repeated completion of one root is a no-op and a
hard pre-syscall validation error does not consume a different generation.

Linux uses `fallocate(PUNCH_HOLE|KEEP_SIZE)` and Darwin uses `F_PUNCHHOLE`;
other platforms report the optimization unsupported. `EINTR` is retried at
most four times. Unsupported operation or any syscall error increments the
corresponding `durable.Stats` counter, counts the candidate as skipped, and
disables further attempts for that open collection without poisoning or
failing an otherwise successful durability boundary. Online reclamation needs
no background compactor and no offline maintenance pass; filesystems without
hole punching retain logically reusable space but may not return its blocks to
the filesystem until an explicit rewrite such as `Repack`.

## Publication and recovery

Ordinary copy-on-write publication performs these steps:

1. allocate and encode complete replacement pages;
2. write and synchronize all data extents;
3. encode the complete StateRoot and free-set delta in the alternate inline
   root slot;
4. write and synchronize that root slot;
5. expose the generation and retire unreachable extents behind the reader
   epoch/lease fence.

The synchronous journal lane first appends and synchronizes one redo record,
then publishes the complete reader-visible generation. Qualified canonical
materialization instead synchronizes a before-image journal, overwrites and
synchronizes changed sectors, and finally publishes the alternate root.

Recovery validates checksums, complements, reserved bytes, store identities,
generation relationships, all graph references, catalog digests, exact-index
postings, and allocator disjointness. For a v1 scalar patch it first reconstructs
the complete canonical result and requires the recorded result checksum before
calling the ordinary mutation path. V1 delta entries name one consecutive
generation each, ending at the batch generation. If bounded replay pressure
physically checkpoints a prefix and recovery is interrupted again, the next
open derives and skips exactly the prefix already covered by the selected root;
legacy v0 batches retain their one-generation replay-from-entry-zero rule.
Recovery fails closed on any disagreement.

## Verification and golden images

`durable.Verify` performs offline graph and allocator checks without mutating
the input. `durable.Salvage` and `durable.Repack` write fresh version-0 stores;
they do not copy old page layouts.

`internal/storeio/testdata/format0` contains the canonical golden images for
the current schema. `format0_golden_test.go` regenerates and compares them and
asserts that every stored version field is zero. Malformed-input tests cover
checksum, reserved-byte, bound, identity, routing, and graph-consistency
failures.

See also [architecture.md](architecture.md), [durability.md](durability.md),
and [store.md](store.md).
