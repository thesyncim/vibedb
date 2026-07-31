# On-disk format

This document maps the one mutable format written by `store/durable` and
decoded by `internal/storeio`. The codecs are the byte-level authority. All
multi-byte integers are little-endian, all reserved bytes are zero, and all
stored format-version fields contain `DevelopmentFormatVersion == 0`.

The project is unreleased. A schema change edits format 0 in place and
regenerates the format-0 golden images. There are no migration decoders,
alternate development versions, or retired page layouts in the current format.

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

The paired recovery journal (`recovery_journal.go`) carries acknowledged redo
records for the synchronous mutation lane. Its identity is bound by both the
store id and `StateRoot.JournalID`. The fixed materialization journal slots
carry complete before-image sectors for qualified in-place canonical page
updates. Recovery rolls back an incomplete materialization before selecting a
root. Readers never consult either journal.

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
postings, and allocator disjointness. It fails closed on any disagreement.

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
