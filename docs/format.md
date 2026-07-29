# On-disk format

This document is the readable map of the current mutable byte format written
by `store/durable` and decoded by `internal/storeio`. It covers the fixed
prefix, alternating inline roots, common envelope, every current page payload,
checksums, publication, recovery, and reclamation.

It does not cover API surface, `Options` semantics, defaults, or mutation
behavior (snapshots and index construction) — see [store.md](store.md).
It does not cover the two I/O backends' wire protocol with the kernel
(`internal/storeio/ring_linux.go`, `device_portable.go`) beyond noting their
existence in the commit-protocol section; that is an implementation detail of
how `Device.Commit` gets bytes onto disk, not part of the durable format
itself.

All multi-byte integers are little-endian. All sizes are in bytes unless
stated otherwise.

## How to read this specification

Start with [Overview](#overview), [Mutable file prefix](#mutable-file-prefix),
[Inline root](#inline-root-sjinl001), [Common page envelope](#common-page-envelope),
and [StateRoot](#stateroot). Then follow the `PageRef` from `StateRoot` into the
payload section for the required page kind. Publication and recovery are in
[Commit and durability protocol](#commit-and-durability-protocol).

The words **must**, **required**, and **reject** describe decoder validation.
Reserved bytes are zero unless a table states otherwise. Offsets are relative
to the start of the enclosing record or payload; ranges are half-open.

Every byte-layout table below is normative for this development tree, subject
to the codec-authority rule in the next section.

## Authority and development lineage

This is **development format 0**. The repository has not released a persistent
format, deliberately provides no backward-compatibility or migration promise,
and may reject files written by any superseded development layout. A schema
change edits format 0 in place; it does not add a compatibility branch. The
common page envelope is currently version `4` specifically to make pages from
superseded development layouts fail closed; each in-place development
renumbering bumps it so a valid checksum is never the only thing selecting a
decoder.
The current StateRoot and `PageKind` set contain no TTL/expiration counter,
directory, or page kind.
The few payload-local tag `1` forms documented below are branches that the
current codecs explicitly emit or accept; they are current schema
discriminants, not a repository-level compatibility guarantee.

The codecs are authoritative, not this prose. In particular:

- `internal/storeio/mutable_file_layout.go` defines the fixed mutable-file
  prefix and `DataStart`;
- `internal/storeio/inline_superblock.go` defines the `SJINL001` inline root
  and inline free delta;
- `internal/storeio/state_root.go` and `internal/storeio/page.go` define the
  embedded state payload, `PageRef`, common page envelope, and durable kinds;
- each kind's named codec file defines its payload;
- `internal/storeio/materialization_journal.go`,
  `materialization_recovery.go`, and `committer_materialization.go` define
  optional canonical materialization and recovery.

`internal/storeio/superblock.go` still contains a provisional standalone
root codec used by lower-level code and tests. It is not the root layout of a
current mutable `store/durable` file and must not be used to infer that layout.

## Overview

The base page (`PageSize`) is fixed at 4096 bytes: every tree, root, directory,
and metadata page is exactly one base page, while a leaf or overflow extent
grows from one base page up to `MaxPageSize` (default 64 KiB). Variable base
page sizes are not supported — `Create` rejects any `PageSize` other than 4096
with `ErrUnsupportedPageSize`, and `Open` of a store that recorded a different
base page fails the same way. The base page is no longer discovered from the
file.

A mutable durable collection is a graph of checksummed physical pages rooted
at one of two alternating full-`PageSize` inline root slots:

```text
root slot 0 (one full PageSize; 4 KiB SJINL001 prefix)
root slot 1 (one full PageSize; 4 KiB SJINL001 prefix)
materialization journal 0 (fixed 4 KiB)
materialization journal 1 (fixed 4 KiB)
padding to the next PageSize boundary
DataStart
  -> PrimaryRoot (GlobalTabletCatalog: catalog -> optional branch -> leaf
       -> tablet root -> locator/anchor) -> ordered PagePrimaryLeaf pages
       -> each leaf record is an inline value, or, for a value past
          InlineValueBytes, a PageOverflow chain (one PageRef; each extent
          sized to its piece up to MaxPageSize)
  -> ExactIndexRoot (PagePrimaryExactRoot -> PagePrimaryExactLeaf posting tiles)
  -> external FreeImage/FreeIndex/FreeDelta spill pages when inline capacity fills
```

A document value is stored inline in its ordered primary leaf when it fits
`InlineValueBytes`; a value up to `MaxDocumentBytes` is written out of line as a
forward-linked chain of `PageOverflow` extents, and the leaf record holds only
the 32-byte chain-head `PageRef`. `Put` mints the chain in the same write
transaction that publishes the referencing leaf, so the value and its leaf
become durable in one generation; replaying a `Put` re-derives the chain.

Ordinary mutations are copy-on-write: they allocate replacement extents,
write and synchronize data pages, then publish and synchronize the alternate
root slot. A file explicitly qualified with a non-zero power-loss damage
granule may use bounded canonical materialization for eligible updates. That
path records complete before-image sectors in one fixed journal slot,
synchronizes the journal, writes and synchronizes only changed target sectors,
then publishes and synchronizes the alternate root. The journal is recovery
metadata only; readers never consult it and therefore acquire no delta-chain
or journal read amplification.

`LogicalID` gives a page a stable identity across copy-on-write replacement
(so a `PageRef` chain can be diffed/attributed across generations), while its
physical `Offset` changes every time the logical page is rewritten.
`Generation` records which collection generation produced that physical version;
because unchanged subtrees are shared, a live page's `Generation` is often
older than the embedded StateRoot that currently references it. A
canonically materialized page keeps its physical identity while its contents
change under the journal/root protocol.

## Mutable file prefix

`MutableStoreLayout(pageSize)` in
`internal/storeio/mutable_file_layout.go` is the only authority for fixed
offsets:

| Region | Offset | Length |
| --- | --- | --- |
| inline root slot 0 | `0` | `PageSize` |
| inline root slot 1 | `PageSize` | `PageSize` |
| materialization journal 0 | `2*PageSize` | `4096` |
| materialization journal 1 | `2*PageSize + 4096` | `4096` |
| allocator padding | after journal 1 | to next `PageSize` boundary |
| ordinary allocations | `DataStart = alignUp(2*PageSize + 8192, PageSize)` | remainder of file |

No `PageRef`, materialization target, or free extent may begin below
`DataStart`. At `PageSize == 4096`, `DataStart == 16384`; at larger page
sizes the two 4 KiB journals are packed and the end is rounded once.

## Inline root (`SJINL001`)

Each root slot owns a complete `PageSize` extent. Its first 4096 bytes are the
deterministic, checksummed `InlineSuperblockSize` record below; bytes
`[4096:PageSize)` remain zero. The two slots alternate by successful physical
publication, seeded from the slot selected during recovery rather than inferred
from a potentially skipped logical generation.

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0:8 | magic | `"SJINL001"` | fixed |
| 8:12 | version | u32 | `DevelopmentFormatVersion == 0` |
| 12:16 | size | u32 | `InlineSuperblockSize == 4096` |
| 16:20 | flags | u32 | must be `0` |
| 20:24 | pageSize | u32 | power of two, `>= 4096` |
| 24:32 | generation | u64 | monotonic, non-zero |
| 32:40 | ~generation | u64 | bitwise complement |
| 40:48 | fileEnd | u64 | exclusive allocated high-water mark |
| 48:72 | reserved | — | zero |
| 72:88 | storeID | `[16]byte` | file identity |
| 88:96 | reserved | — | zero |
| 96:608 | StateRoot payload | 512 bytes | embedded; no external root page |
| 608:612 | inline-free version | u32 | `DevelopmentFormatVersion == 0` |
| 612:614 | inline-free count | u16 | `0..106` |
| 614:616 | reserved | — | zero |
| 616:648 | external predecessor | `PageRef` | newest external `PageFreeDelta`, or zero |
| 648:680 | free-index head | `PageRef` | `PageFreeIndex`, or zero |
| 680:4072 | records | up to 106 × 32 bytes | canonical, offset-sorted cumulative free deltas |
| used-record end:4088 | reserved | — | zero |
| 4088:4092 | checksum | u32 | CRC32C over `[0:4088)` |
| 4092:4096 | ~checksum | u32 | complement |

The inline free run keeps at most one latest operation per extent offset.
When its bounded capacity cannot represent the next cumulative state, the run
spills to external free-log pages and starts again from that predecessor.
Root, StateRoot, and the current bounded free-set diff therefore publish in
one physical root write.

`DecodeInlineSuperblock` validates the complement, checksum, reserved bytes,
embedded StateRoot, inline free records, identities, extents, and bounds
before returning any offset. `RecoverMutableInlineStateRoot` reads both full
root slots and both fixed journal slots, validates candidates newest-first,
performs any required journal rollback, then validates every top-level
reference before selecting a root. The alternate valid generation becomes
the conservative reclamation fence when it remains independently readable.

## Common page envelope

`internal/storeio/page.go`. Every allocator-managed page carrying a
`PageKind` shares one fixed 64-byte header and one fixed 8-byte trailer.
`PageHeaderSize = 64`, `PageTrailerSize = 8`.

```text
 0        8    10   12 13  14      16       20                24
+--------+----+----+--+--+--------+--------+--------+--------+
| magic  |ver |hlen|K |Fl|reserved|pageSize| payload|generation
+--------+----+----+--+--+--------+--------+--------+--------+
 24                32                40                     56       64
+-----------------+-----------------+----------------------+--------+
|   generation    |    logicalID    |     storeID (16B)    |reserved|
+-----------------+-----------------+----------------------+--------+
 64                                                    pageSize-8  pageSize
+---------------------------------------------------------+--------+
|                  payload ... zero padding ...            | CRC32C |
+---------------------------------------------------------+--------+
```

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0:8 | magic | `"SJPAGE01"` | fixed |
| 8:10 | version | u16 | `4` (`pageVersion`); rejects every superseded development layout |
| 10:12 | headerLength | u16 | `PageHeaderSize` = `64`, self-describing |
| 12 | Kind | u8 | `PageKind`, see table below |
| 13 | Flags | u8 | kind-specific; `0` for every kind except `PageDocumentGroup` |
| 14:16 | reserved | — | must be zero |
| 16:20 | PageSize | u32 | physical extent size; power of two except `PageDocument`/`PageOverflow`, which may use any 4 KiB multiple |
| 20:24 | PayloadLength | u32 | payload bytes, excludes header/padding/trailer |
| 24:32 | Generation | u64 | `!= 0` |
| 32:40 | LogicalID | u64 | `!= 0`, stable across copy-on-write replacement |
| 40:56 | StoreID | `[16]byte` | must match the owning collection |
| 56:64 | reserved | — | must be zero |
| 64 : 64+PayloadLength | payload | — | kind-specific, see below |
| 64+PayloadLength : PageSize-8 | padding | — | must be zero; CRC-covered but not re-scanned by readers |
| PageSize-8 : PageSize-4 | checksum | u32 | CRC32C over bytes `[0 : PageSize-8)` |
| PageSize-4 : PageSize | ~checksum | u32 | complement |

`InitPage` clears the destination extent, writes the header, and returns the
exact `[64 : 64+PayloadLength)` payload window for the caller to fill.
`SealPage` (or the internal fast path `sealInitializedPage`, which trusts
`InitPage` already zeroed the padding) computes and writes the trailer.
`OpenPage` re-derives the header, verifies the CRC32C and its complement,
verifies reserved bytes are zero, and returns a capacity-clipped payload view
— padding is checksum-covered but never re-scanned by readers, since the
clipped view makes it unreachable.

### PageKind

```go
const (
	PageStateRoot PageKind = iota + 1 // 1
	PageDocument                      // 2
	PageOverflow                      // 3
	PageChunkDirectory                // 4
	PageKeyDirectory                  // 5
	PageIndexDirectory                // 6
	PageIndexPosting                  // 7
	PageDocumentGroup                 // 8
	PageFloat64Group                  // 9
	PageFloat64Catalog                // 10
	PageFloat64Stripe                 // 11
	PageIndexGroupCatalog             // 12
	PageFreeImage                     // 13
	PageFreeDelta                     // 14
	PageFreeIndex                     // 15
	PageFingerprintDirectory          // 16
	PageCatalogSegment                // 17
	PagePrimaryCatalog                // 18
	PageTabletDirectory               // 19
	PagePrimaryLocator                // 20
	PageTabletRoute                   // 21
	PagePrimaryAnchor                 // 22
	PagePrimaryLeaf                   // 23
	PagePrimaryExactRoot              // 24
	PagePrimaryExactLeaf              // 25
)
```

| Value | Kind | Purpose |
| --- | --- | --- |
| 1 | `PageStateRoot` | standalone StateRoot envelope for lower-level/provisional use; mutable durable roots embed the payload |
| 2 | `PageDocument` | stable-slot micro-page of up to 64 documents in one logical chunk |
| 3 | `PageOverflow` | one ordered piece of a JSON value too large to inline |
| 4 | `PageChunkDirectory` | packed-radix routing from chunk id to `PageDocument`/`PageDocumentGroup` |
| 5 | `PageKeyDirectory` | ambiguous development key-directory kind; not accepted as a populated current mutable store's primary directory |
| 6 | `PageIndexDirectory` | B+tree from `(indexID, tupleHash, chunk)` to an inline stable-slot mask and certificate |
| 7 | `PageIndexPosting` | packed exact-match posting codec; inert in the current mutable durable writer |
| 8 | `PageDocumentGroup` | immutable multi-chunk compact/bulk document extent (template+dictionary compression) |
| 9 | `PageFloat64Group` | detached column-major typed float64 sidecar for a `PageDocumentGroup` run |
| 10 | `PageFloat64Catalog` | B-tree directory over `PageFloat64Stripe` leaves (scan accelerator) |
| 11 | `PageFloat64Stripe` | aggregate-only, mask-free dense float64 projection for a chunk range |
| 12 | `PageIndexGroupCatalog` | bounded aggregate-only categorical grouping cover |
| 13 | `PageFreeImage` | one free-set image segment, ordered by offset |
| 14 | `PageFreeDelta` | one free-set diff page and predecessor link |
| 15 | `PageFreeIndex` | directory naming independent free-image segments |
| 16 | `PageFingerprintDirectory` | keyed-hash B+tree to candidate `(chunk, slot)` locations; exact document-key recheck is authoritative |
| 17 | `PageCatalogSegment` | self-describing canonical Store catalog segment; reopen selects this decoder by durable kind, never by probing payload |
| 18 | `PagePrimaryCatalog` | hybrid-primary graph root: exact lexical catalog whose leaves name each macro-tablet's root `PageRef` (`StateRoot.PrimaryRoot`) |
| 19 | `PageTabletDirectory` | tablet-directory node of the primary catalog, routing a key to the macro-tablet that owns it |
| 20 | `PagePrimaryLocator` | tablet local-ID locator mapping a stable `BucketID` to the current leaf page and slot holding it |
| 21 | `PageTabletRoute` | tablet segmented route block over its primary leaves |
| 22 | `PagePrimaryAnchor` | tablet lexical anchor map: immutable interval fences routing to stable `BucketID`s |
| 23 | `PagePrimaryLeaf` | ordered primary leaf holding the key/value records |
| 24 | `PagePrimaryExactRoot` | physical exact-index id to term-leaf reference catalog built beside `PrimaryRoot` (`StateRoot.ExactIndexRoot`); aliases share physical leaf entries |
| 25 | `PagePrimaryExactLeaf` | common-page envelope around one canonical adaptive `IndexTermLeaf` byte stream (exact posting tiles for one physical index) |

### PageRef — 32 bytes

Every inter-page pointer in the format uses this one fixed encoding
(`internal/storeio/state_root.go`):

```text
 0                8                 16                24    25  26  28    30      32
+----------------+-----------------+-----------------+-----+---+---+-----+-------+
|     Offset     |    LogicalID    |    Generation   |Length|K |Fl | Aux         |
+----------------+-----------------+-----------------+-----+---+---+-------------+
```

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0:8 | Offset | u64 | physical byte offset of the referenced page |
| 8:16 | LogicalID | u64 | referenced page's stable logical id |
| 16:24 | Generation | u64 | referenced page's `Generation` (may be older than the referrer's) |
| 24:28 | Length | u32 | referenced page's physical `PageSize` |
| 28 | Kind | u8 | expected `PageKind` |
| 29 | Flags | u8 | `0` except document-group float64-sidecar routing (see below) |
| 30:32 | Aux | u16 | `0` except document-group float64-sidecar routing |

## StateRoot

`internal/storeio/state_root.go`. Mutable roots embed the fixed 512-byte
`StateRootPayloadSize` body at inline-root bytes `[96:608)`; the standalone
`PageStateRoot` encoder uses the same body with fixed `LogicalID = 1`.
Its schema version is development format `0`; no prior StateRoot version is
accepted.

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0:4 | version | u32 | `DevelopmentFormatVersion == 0` only |
| 4:8 | Options | u32 | bit flags, see below |
| 8:16 | DocumentCount | u64 | live document count |
| 16:24 | NextLogicalID | u64 | next `LogicalID` to allocate, `> 1` |
| 24:28 | ChunkHighWater | u32 | exclusive upper bound of live chunk ids |
| 28:32 | LiveChunks | u32 | `<= ChunkHighWater` |
| 32:36 | ChunkDocuments | u32 | documents per chunk, `1..64` |
| 36:40 | IndexCount | u32 | number of declared exact indexes |
| 40:44 | IndexMaxDepth | u32 | — |
| 44:52 | IndexCatalogHash | u64 | binds the durable catalog (exact indexes, float64 covering columns, schema) |
| 52:56 | FreeChunkHint | u32 | conservative lower bound of a chunk that may have a free slot |
| 56:88 | ChunkDirectory | `PageRef` | required iff `LiveChunks != 0` |
| 88:120 | KeyDirectory | `PageRef` | required iff `DocumentCount != 0`; current mutable stores require kind `PageFingerprintDirectory` |
| 120:152 | IndexDirectory | `PageRef` | optional only when `IndexCount != 0` |
| 152:184 | Float64ScanHead | `PageRef` | points at a `PageFloat64Catalog` root |
| 184:216 | IndexGroupHead | `PageRef` | points at a `PageIndexGroupCatalog` chain head |
| 216:220 | MaterializationDamageGranule | u32 | `0` disables canonical materialization; otherwise the persisted qualified power-loss damage granule |
| 220:224 | MaxPageSize | u32 | largest physical extent admitted by this Store |
| 224:256 | PageCatalogHead | `PageRef` | first segment of the exact canonical Store catalog |
| 256:272 | PageCatalogDigest | 16 bytes | digest of the canonical catalog bytes |
| 272:276 | PageCatalogBytes | u32 | exact canonical catalog length; zero means absent |
| 276:280 | MaxKeyBytes | u32 | immutable key-admission bound |
| 280:284 | InlineValueBytes | u32 | immutable inline-value bound |
| 284:288 | MaxDocumentBytes | u32 | immutable complete-document bound |
| 288:320 | PrimaryRoot | `PageRef` | zero selects the current fingerprint/chunk primary; otherwise the 64 KiB `PagePrimaryCatalog` root at `PrimaryCatalogRootLogicalID` |
| 320:336 | JournalID | 16 bytes | UUID of the paired recovery journal file; zero means no journal is referenced |
| 336:368 | ExactIndexRoot | `PageRef` | `PagePrimaryExactRoot` for exact indexes built beside `PrimaryRoot`; aliases share physical leaf entries. Zero unless the ordered primary declares indexes and keeps no legacy `IndexDirectory` |
| 368:512 | reserved | — | zero |

The chunk, key, index, and float64 directory roots have
`Length == PageSize`. `IndexGroupHead` may be a larger valid physical extent
that is a whole `PageSize` multiple. `PrimaryRoot` has the fixed production
catalog identity, kind, and 64 KiB length and requires the reserved primary
logical-ID bands below `NextLogicalID`. `ExactIndexRoot` is one `PageSize`
catalog whose ordered records each contain either zero (an empty physical index)
or a `PagePrimaryExactLeaf` reference; leaf extents are powers of two up to
`MaxPageSize`, and each leaf payload is exactly `AppendIndexTermLeaf` output.
Every populated top-level ref has `Generation <= root.Generation`; all roots are
pairwise distinct in both `LogicalID` and `Offset`.

Ordered-primary leaves are all `PagePrimaryLeaf` pages; their payload byte
`[2]` low seven bits are the leaf-class discriminator, read only after the
common-page checksum has validated the payload. Class `1` is the 4 KiB narrow
succinct envelope, `2` the 8 KiB wide envelope, and `3` the template-columnar
class, whose payload from byte `3` on is a template-columnar image (a
content-addressed template skeleton per shape, packed field slots, a per-leaf
value dictionary, per-field zone vectors, and region checksums). The bulk
builder stages the template class per leaf when it lowers the page cost per
document by at least a quarter versus the raw envelope; readers dispatch on the
class byte with no payload probing, and a mutation de-templates a template leaf
back into a raw envelope on first write.

Class `4` is the compact document-group class, the ordered-primary form of
`DocumentFormatCompact`. Its payload from byte `3` on is a `PageDocumentGroup`
payload (see below) embedded verbatim — one shared shape-template table and
value dictionary per leaf, each row reduced to a dictionary/literal token
stream — so the leaf is self-describing behind its class byte and needs no
separate format flag on the collection. The embedded payload's rows are keyed
by lexical rank rather than a chunk/slot directory; a compact leaf's
posting-stable slot is that lexical rank (as for the template class). The
enclosing common-page CRC covers the embedded bytes, so the group payload
carries no second checksum. `CreateFromPrimary` stages class-4 leaves only when
`DocumentFormat` is `Compact`; each leaf packs as many rows as minimise its
byte-per-row within the 64 KiB maximum extent and the 256-row posting-slot
ceiling, capped so its rows still fit a 64 KiB raw wide leaf. A run that cannot
be grouped (a lone trailing row, or a document too large to co-locate) falls
back to one adaptive raw leaf, which reads dispatch to on the class byte just
like the compact ones.

Reads reconstruct each compact row's exact original JSON by splicing the
shared template's static segments with the row's decoded values; a point read
decodes into a caller-owned scratch (the verbatim path's zero-allocation
borrow does not apply, because a compact value is assembled from fragments
rather than stored contiguously), and an ordered scan reconstructs per row into
one reused scratch. A `Put`/`Update`/`Delete` on a compact collection does
**not** patch the compact image in place: like the template class it
de-compacts the whole leaf into a raw envelope on first write, then the ordinary
structural path re-fits and splits it into wide leaves (so ordinary writes
always leave verbatim leaves behind). The de-compacted leaf can be as large as
the 64 KiB maximum before that re-fit, which is why the mutation leaf scratch
covers the maximum leaf extent rather than only the wide class. The exact-index
maintainer enumerates compact rows through the same `VisitPrimaryLeafPostingRows`
enumerator every class uses, decoding each row's field bytes before extraction,
so a compact leaf's postings match what a lookup selects it by.

### Options bits

```go
const (
	StateOptionShapeTapes uint32 = 1 << iota
	StateOptionPostings
	StateOptionValueDict
	StateOptionHashKeys
	StateOptionFloat64Columns
	StateOptionSchema
	StateOptionCanonicalMaterialization
)
```

`StateOptionFloat64Columns` means every live document page carries the
complete configured float64 covering-column catalog. `StateOptionSchema`
means the catalog hash also binds an application-supplied schema definition;
the schema itself is caller configuration, not part of the durable format.
`StateOptionCanonicalMaterialization` means this file may contain journaled
canonical page replacements; it requires the exact non-zero damage granule
stored in the StateRoot. That granule is a power of two in `[512, 3584]` and
must divide `PageSize`; zero and the option bit must either both be absent or
both be present.
Any bit outside this known set fails closed on decode.

## ChunkDirectory

`internal/storeio/chunk_directory.go`, kind `PageChunkDirectory`. A
packed-radix (Patricia-trie-like) node over 32-bit chunk ids, 6 bits per
level (`chunkDirectoryRadixBits = 6`), so each node has up to 64 children and
`Shift` values are `0, 6, 12, 18, 24, 30` (`chunkDirectoryMaxShift = 30`; at
shift 30 only the low 4 bits of a chunk id remain, so a top node's `Bitmap`
may only use lanes 0–3). `Shift == 0` marks a leaf whose children are
`PageDocument` or `PageDocumentGroup` references; several leaf lanes may
name the same `PageDocumentGroup` extent (its `Aux`/`Flags` may carry the
float64-sidecar routing bits described under Float64Group below), so those
duplicates are exempted from the page's own child-uniqueness check.

The tree's height is variable and is named by the root's own `Shift`: readers
must take it from the page rather than assume `chunkDirectoryMaxShift`. A tree
is only as tall as its live chunk ids require — one level while every chunk
fits a single 64-lane leaf, and one more per factor of 64 — so a root may carry
a non-zero `Prefix`, and a chunk id outside the span it covers is absent rather
than corrupt. Writers raise the height by wrapping the existing root in new
levels when an insert falls outside its span; the height tracks the monotone
chunk high-water mark and so never shrinks. Only development format `0` is
accepted; superseded fixed-height development pages are rejected.

```text
 0        4        8                16       17   18       20                  32
+--------+--------+-----------------+--------+----+--------+-------------------+
|version | Prefix |     Bitmap      | Shift  |cnt |  Flags | reserved (12B)    |
+--------+--------+-----------------+--------+----+--------+-------------------+
 32                    +32*count
+---------------------------------------------------------+
|      PageRef[count], ordered by increasing set bit lane  |
+---------------------------------------------------------+
 32+32*count           +30*count   (leaves with FlagZones only)
+---------------------------------------------------------+
|      ChunkZone[count], same packed lane order            |
+---------------------------------------------------------+
```

| Offset | Field | Type | Notes |
| --- | --- | --- | --- |
| 0:4 | version | u32 | development format `0` |
| 4:8 | Prefix | u32 | chunk-id prefix this node covers (`chunkID &^ ((1<<(Shift+6))-1)`) |
| 8:16 | Bitmap | u64 | one bit per populated lane (0–63) |
| 16 | Shift | u8 | multiple of 6, `0..30`; `0` = leaf |
| 17 | count | u8 | `popcount(Bitmap)`, `<= 64` |
| 18:20 | Flags | u16 | bit 0 = `ChunkDirectoryFlagZones`; every other bit must be `0` |
| 20:32 | reserved | — | must be zero |
| 32 : 32+32×count | refs | `PageRef[]` | packed in increasing set-bit-lane order (sparse — no empty slots stored) |
| 32+32×count : +30×count | zones | `ChunkZone[]` | present only when `Flags & 1`; one 30-byte chunk summary per reference, same order |

### Chunk summaries (zone maps)

A leaf may carry one fixed-width 30-byte summary per lane
(`ChunkZoneSize`), immediately after the reference array and in the same
packed order. The flag is per page: a leaf without it is byte-identical to
what earlier builds wrote, and a leaf with it must be a leaf — `Flags & 1`
on a branch (`Shift != 0`) is rejected on both encode and decode.

The array fits because a full 64-lane leaf is not full. In the smallest legal
page (4096) the payload is 4024 bytes, of which the header takes 32 and
64 references take 2048, leaving 1944 — exactly 30 bytes a lane with 24 to
spare. Summaries therefore cost **no additional page**: a store with summaries
and the same store without them occupy the same number of bytes.

`internal/storeio` treats the 30 bytes as opaque and validates nothing about
their content; they are covered by the page checksum like every other byte.
Their schema is owned by `store` (`store/store_zone_compact.go`,
`ZoneSummary.Encode`/`Decode`):

| Offset | Field | Notes |
| --- | --- | --- |
| 0:2 | status | bits 0–1 state (`0` stale, `1` ok, `2` poisoned), bit 2 overflow, bits 3–4 entry count (0–3), bits 5–13 three 3-bit flag fields (absent / null / value), bits 14–15 reserved zero |
| 2:8 | tags | 3 × u16, the top 16 bits of the FNV-1a path hash of a tracked top-level member name |
| 8:17 | min | 3 × u24, the top 24 bits of the entry's minimum value code |
| 17:26 | max | 3 × u24, the top 24 bits of the entry's maximum value code |
| 26:30 | reserved | must be zero |

An all-zero record decodes as *stale* — "no statistics" — so a lane written
by a builder that computed no summary keeps every chunk rather than pruning
on a record it cannot read. The same is true of any record whose state,
entry count, or reserved bits are out of range.

Because a summary lives in the same page, generation, and checksum as the
chunk reference it describes, and is written by the same copy-on-write commit,
there is no state in which a recovered generation has a chunk extent without
its summary or a summary without its extent.

**Lookup**: check `Prefix` matches the chunk id's prefix at this `Shift`,
extract `lane = (chunkID >> Shift) & 63`, probe `Bitmap` for that bit, then
`rank = popcount(Bitmap & (bit-1))` selects the packed reference —
one bitmap probe, one popcount, no scan. Non-leaf refs must have `Kind ==
PageChunkDirectory` and exact `Length == PageSize`; leaf refs must have `Kind
∈ {PageDocument, PageDocumentGroup}` and `Length >= PageSize`. A
`PageDocument` may use any whole multiple of the metadata quantum; a
`PageDocumentGroup` retains its larger power-of-two geometry.

## FingerprintDirectory

`internal/storeio/page_key_directory.go`, kind
`PageFingerprintDirectory`. This is the current mutable primary-key B+tree.
It stores keyed 64-bit fingerprints and candidate `(chunk, slot)` locations,
not complete keys. A fingerprint match is never authoritative: every
candidate is resolved to its document page and compared against the complete
key. Collisions affect candidate work, not correctness.

The fixed payload header is 64 bytes
(`PageKeyDirectoryPayloadHeaderSize`):

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | version | u32, development format `0` |
| 4 | Level | u8, `0` leaf, `1..15` branch |
| 5 | Flags | u8, zero |
| 6:8 | count | u16 |
| 8:16 | MinHash | u64, inclusive |
| 16:24 | MaxHash | u64, inclusive |
| 24:26 | entrySize | u16, `16` leaf or `40` branch |
| 26:32 | reserved | zero |
| 32:64 | Next | `PageRef`; leaf locality hint, zero for branches |

**Leaf record — 16 bytes** (`PageKeyLeafEntrySize`):

| Offset | Field | Notes |
| --- | --- | --- |
| 0:8 | Hash | u64, ordered; equal collision fingerprints allowed |
| 8:12 | Chunk | u32, `< ChunkHighWater` |
| 12 | Slot | u8, `< ChunkDocuments` |
| 13:16 | reserved | zero |

Equal hashes are ordered by `(Chunk, Slot)`. The leaf `Next` ref is only a
physical-locality hint: copy-on-write may leave it naming an older leaf
generation, so collision traversal derives successors from the selected
root's parent path.

**Branch record — 40 bytes** (`PageKeyBranchEntrySize`):

| Offset | Field | Notes |
| --- | --- | --- |
| 0:8 | MaxHash | u64 inclusive upper bound; equal bounds may span a collision run |
| 8:40 | Child | `PageRef`, kind `PageFingerprintDirectory` |

`PageKeyDirectory` can carry the same packed codec under its distinct durable
kind. The lower-level StateRoot codec recognizes both kinds, but the current
`store/durable` open contract requires `PageFingerprintDirectory` for a
non-empty primary root. The durable kind, rather than payload guessing,
selects the decoder.

## IndexDirectory

`internal/storeio/index_directory.go`, kind `PageIndexDirectory`. A B+tree
keyed by `(IndexID, TupleHash, Chunk)` — `IndexDirectoryKey`. Hashes are
candidate accelerators only; exact scalar/tuple recheck happens above this
layer. Its 32-byte header stores development version `0` (0:4), Level (4),
Flags (5, zero), count (6:8), leaf certificate-heap start (8:12; zero for a
branch), and zero reserved bytes (12:32). Branch fanout is capped at 64.

**Leaf record — 32 bytes** (`IndexDirectoryLeafRecordSize`):

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | IndexID | u32 |
| 4:8 | Chunk | u32 |
| 8:16 | TupleHash | u64 |
| 16:24 | Bits | u64 non-zero stable-slot mask, inline |
| 24:26 | certificate offset | u16 absolute payload offset |
| 26:28 | certificate length | u16 |
| 28:30 | Flags | u16; `IndexEntryCollision` marks a non-unique certificate stream |
| 30 | Kind | u8; currently only `IndexEntryInlineMask == 0` |
| 31 | reserved | zero |

Certificate bytes occupy the exact payload suffix after the record array.
Canonical encoding tiles that heap without gaps; adjacent equal certificates
may share the same span. The certificate narrows exact-value candidates but
does not replace document recheck when collision flags require it.

**Branch record — 48 bytes** (`IndexDirectoryBranchRecordSize`): same
16-byte `IndexDirectoryKey` prefix (`IndexID`, `Chunk`, `TupleHash` — note
the encoded field order is `IndexID(4) Chunk(4) TupleHash(8)`, not
declaration order) followed by a 32-byte `PageRef` to a child
`PageIndexDirectory` node.

## Free log — inline delta, FreeImage, FreeIndex, and FreeDelta

`internal/storeio/inline_superblock.go`, `free_log.go`, and `free_index.go`.
The inline root publishes a bounded cumulative free-set delta with the same
generation as StateRoot. Its `externalPrev` names the newest preceding
`PageFreeDelta`, while `indexHead` names the `PageFreeIndex` chain describing
the external base image. There are no external free-root scalar fields.

The representation is a base image plus a chain of per-commit diffs, and it
replaced a B+tree of `PageFreeDirectory` nodes. The tree could persist exactly
one edit per commit, because mutating it costs pages, allocating those pages
changes the free set, and a changed free set changes the tree's shape;
everything a commit reclaimed past the first extent therefore stayed in memory
and was abandoned at the next restart. A flat record breaks that cycle because
it can describe its own allocation: content is fixed once an extent is chosen,
so `d` records need `p = ceil(d/capacity)` pages, allocating those adds at most
`p` more records, and recomputing converges. The writer allocates the delta
pages **last** and encodes them **after** allocating, which is why a commit can
record its complete diff.

**Chain shape.** Each external delta names both its predecessor (`Prev`) and
the image segment index (`IndexHead`) the chain is relative to. Replay walks
external deltas oldest-to-newest, then applies the inline cumulative records;
for one offset the newest operation wins. A fold rewrites only affected
`PageFreeImage` segments and the smaller `PageFreeIndex` chain rather than
rewriting one linked image in full.

**Image page** — 64-byte payload header (`FreeImagePayloadHeaderSize`): version
(0:4, development format `0`), flags (4:5), reserved (5:6), record count
(6:8, u16), reserved (8:64). Records are 24 bytes
(`FreeImageRecordSize`, one `FreeExtent`): `Offset` (u64, 0:8), `Length` (u64,
8:16), `RetiredGeneration` (u64, 16:24) — the last generation that may still
reach this extent; reuse additionally waits for every active `GenerationLease`
and protected recovery root to advance past it (see "Reclamation" below).
Extents are non-overlapping and ordered by `Offset` within a segment.

**Index page** — 64-byte payload header (`FreeIndexPayloadHeaderSize`), using
the common free-log header and a `Prev` `PageRef` at bytes 16:48. Each 56-byte
record contains a `PageFreeImage` ref (0:32), `FirstOffset` (32:40),
`LargestFree` (40:48), `Count` (48:52), and zero reserved bytes (52:56).
Records and chained index pages form an ordered partition of offset space.

**Delta page** — 96-byte payload header (`FreeDeltaPayloadHeaderSize`): version
(0:4, development format `0`), flags (4:5), reserved (5:6), record count
(6:8, u16), reserved (8:16), `Prev` `PageRef` (16:48), `IndexHead` `PageRef`
(48:80), reserved
(80:96). Records are 32 bytes (`FreeDeltaRecordSize`): operation (0:1),
reserved (1:8), then the same three `FreeExtent` fields at 8:32. `FreeOpSet`
(1) inserts or replaces the extent at that offset — a reclaim, and a coalesced
extent growing, are both this. `FreeOpDelete` (2) removes whatever is at that
offset and carries only the offset, because an extent's offset never moves:
allocation always takes from an extent's tail. Records within one page are
applied in order and are not required to be sorted, since a later record in the
same commit legitimately supersedes an earlier one for the same offset.

## DocumentPage

`internal/storeio/document_page.go`, kind `PageDocument`. One logical chunk
of up to 64 stable-slot documents; `Live` is a 64-bit occupancy bitmap — no
tombstones, no empty-row descriptors, a deleted slot simply clears its bit
and shifts every other row's packed rank. The untyped encoding currently uses
payload version `1`; the typed-column development encoding uses format `0`.
Its physical extent is the smallest whole
multiple of the Store's allocation quantum that holds the encoded page, up to
`MaxPageSize`; it is not rounded to the next power of two.

```text
 0        4        8                16       20   21   22       24    25  26 27  28
+--------+--------+-----------------+--------+----+----+--------+----+---+--+----+
|version |ChunkID |      Live       |dataLen |cnt |Flag|recSize |ovfC|f64Cnt|f64B|f64Len
+--------+--------+-----------------+--------+----+----+--------+----+---+--+----+
```

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | version | u32, `1` untyped or development format `0` with typed columns |
| 4:8 | ChunkID | u32, `< chunkHighWater` |
| 8:16 | Live | u64 occupancy bitmap, `popcount == count`, `<= 64` |
| 16:20 | dataLength | u32, packed key+value byte region length |
| 20 | count | u8, row count |
| 21 | Flags | u8, must be `0` |
| 22:24 | recordSize | u16, `= DocumentPageRecordSize = 8` (self-describing) |
| 24 | overflowCount | u8 |
| 25:27 | float64ColumnCount | u16 (format `0` only; version `1` reserves this suffix) |
| 27 | reserved | |
| 28:32 | float64Length | u32 (format `0` only) |

Followed by `count` 8-byte row descriptors (`DocumentPageRecordSize = 8`),
one per row in increasing stable-slot order, each a pair of cumulative
offsets into the packed data region: `[0:4)` cumulative key-end offset,
`[4:8)` cumulative record-end offset with its high bit
(`documentPageOverflowBit = 1<<31`) set when the value is an overflow
reference rather than inline JSON. Row `i`'s key spans
`[recordEnd(i-1), keyEnd(i))` and its value spans `[keyEnd(i), recordEnd(i))`
of the data region (masking off the overflow bit), with `recordEnd(-1) == 0`.

An inline value is exactly its JSON bytes. An overflow value is a fixed
40-byte descriptor (`DocumentOverflowDescriptorSize = PageRefSize + 8`): a
32-byte `PageRef` (`Kind == PageOverflow`) followed by an 8-byte `JSONLength`
u64 giving the value's *complete* decoded length across the whole overflow
chain.

If `float64ColumnCount != 0`, the float64 section follows the packed
key/value data at offset `dataStart + dataLength`: `float64ColumnCount`
columns, each a `(mask u64, dense values)` pair — one `1`-bit per row that
has a finite value in that column (`mask &^ Live == 0`), followed by that
many densely packed 8-byte IEEE754 values in ascending-slot order (NaN and
±Inf are rejected). This is the *raw inline* float64 encoding shared with
`DocumentGroup`'s own column section; it differs from the separately
checksummed `Float64Group`/`Float64Stripe` typed extents described below.

## OverflowPage

`internal/storeio/overflow_page.go`, kind `PageOverflow`. One ordered piece
of a JSON value too large to inline, chained via `Next`. Every piece uses the
smallest whole allocation-quantum multiple that holds it, so the final piece
does not pay a power-of-two tail tax.

```text
 0        4    6    7  8        12       16                24
+--------+----+----+--+--------+--------+-----------------+
|version |Flag|Slot|rs|  Chunk |dataLen |     Total       |
+--------+----+----+--+--------+--------+-----------------+
 24                32                                     64
+-----------------+---------------------------------------+
|     Offset      |            Next PageRef (32B)          |
+-----------------+---------------------------------------+
```

Fixed 64-byte header (`OverflowPagePayloadHeaderSize`): version (0:4,
development format `0`),
Flags (4:6, must be `0`), Slot (6, owning document's stable slot),
reserved(7), Chunk (8:12), this piece's data length (12:16, redundant with
`PayloadLength - 64`, cross-checked), Total (16:24, complete value length),
Offset (24:32, this piece's byte offset within the complete value), Next
(32:64, `PageRef` to the following piece; zero `PageRef` marks the final
piece, which must satisfy `Offset + len(data) == Total`). Data bytes follow
at offset 64. Every non-final piece's `Next` must have a strictly greater
`LogicalID` than the current piece — the chain's logical ids increase
monotonically.

## DocumentGroup

`internal/storeio/document_group.go`, kind `PageDocumentGroup`. An
immutable, independently checksummed extent packing **2 or more consecutive
logical chunks** (bulk/compact generations) with structural and value
deduplication: a bounded page-local dictionary (up to 128 entries) for
repeated complete leaf spellings, and a page-local template table that
stores each record's non-varying JSON bytes once and encodes each row as a
list of tokens standing in for its variable leaf values (dictionary
reference, short literal, or long literal). This is *not* general
compression — every literal byte is stored byte-for-byte; only structural
repetition and repeated exact values are deduplicated.

Fixed 64-byte header (`DocumentGroupPayloadHeaderSize`):

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | version | u32, development format `0` |
| 4:8 | FirstChunk | u32 |
| 8:10 | ChunkCount | u16, `>= 2` |
| 10:12 | RowCount | u16 |
| 12:14 | TemplateCount | u16 |
| 14:16 | DictionaryCount | u16, `<= 128` |
| 16:20 | chunk-section bytes | u32, `= ChunkCount * 16` |
| 20:24 | row-section bytes | u32, `= RowCount * 16` |
| 24:28 | key-bytes length | u32 |
| 28:32 | body-bytes length | u32 |
| 32:36 | template-section bytes | u32 |
| 36:40 | dictionary-section bytes | u32 |
| 40:44 | column-section bytes | u32 |
| 44:46 | ColumnCount | u16 (0 when the group instead uses a Float64Group sidecar) |
| 46:48 | Flags | u16, only `DocumentGroupFlagFloat64Sidecar` (`1<<0`) plus its order/logical-delta bit fields, see below |
| 48:56 | decodedBytes | u64, sum of every row's exact decoded JSON length (integrity cross-check) |
| 56:58 | recordSize | u16, `= DocumentGroupRecordSize = 16` |
| 58:60 | chunkSize | u16, `= DocumentGroupChunkSize = 16` |
| 60:64 | reserved | must be zero |

Sections follow the header back-to-back in this fixed order: **chunk
directory**, **row directory**, **key bytes**, **body tokens**, **template
table**, **dictionary table**, **column data** — each section's length comes
from the header fields above, so offsets are computed once at open time.

**Chunk descriptor — 16 bytes** (`DocumentGroupChunkSize`), one per logical
chunk, `ChunkID` strictly `FirstChunk + ordinal`: `ChunkID` (u32, 0:4),
`Live` (u64, 4:12), `firstRow` (u16, 12:14, this chunk's first row's index
into the row directory), `count` (u8, 14, `== popcount(Live)`), reserved(15).

**Row descriptor — 16 bytes** (`DocumentGroupRecordSize`), one per row in
chunk then stable-slot order: cumulative key-end offset (u32, 0:4),
cumulative body-end offset (u32, 4:8), exact decoded `JSONLength` (u32,
8:12), `TemplateID` (u16, 12:14), `Slot` (u8, 14), reserved(15). A row's key
spans `[keyEnd(row-1), keyEnd(row))` of the key-bytes section and its body
token stream spans `[bodyEnd(row-1), bodyEnd(row))` of the body section.

**Body tokens.** Each row's variable leaf values are a sequence of one-byte
tags (one per template "hole"): a byte `< DictionaryCount` is a dictionary
reference; a byte in `[0x80, 0xfe]` (`documentGroupShortLiteral` =
`0x80`..`documentGroupLongLiteral-1`) is a short literal, length
`token-0x80+1` (1–127 bytes), literal bytes follow immediately; byte `0xff`
(`documentGroupLongLiteral`) is a long literal, followed by a uvarint length
and then that many literal bytes. Because `DictionaryCount <= 128 == 0x80`,
the dictionary-id and short-literal ranges never overlap.

**Template table.** A `TemplateCount`-entry directory of cumulative u32 end
offsets (4 bytes each) into the following template-data region. Each
template entry: `values` count (u16, 0:2, number of leaf "holes"), reserved
(2:4), `staticBytes` length (u32, 4:8), then `values+1` cumulative u32 "ends"
into the static-bytes tail that follows — reconstructing a row interleaves
static bytes `[ends[i], ends[i+1])`... with the row's per-hole tokens
(`AppendJSON` does exactly this walk).

**Dictionary table.** A `DictionaryCount`-entry directory of cumulative u32
end offsets (4 bytes each) into the following raw dictionary-value bytes.

**Column section.** Only used when `ColumnCount != 0` (i.e. no float64
sidecar). Per chunk, per column, in that nesting order: `mask` u64 then
dense 8-byte IEEE754 values for each set bit — the same encoding as
DocumentPage's inline float64 section.

**Float64 sidecar routing.** When a bulk/compact generation instead sets
`DocumentGroupFlagFloat64Sidecar` in `Flags`/`PageRef.Flags`, `ColumnCount`
is `0` and the typed columns live in a separately checksummed
`PageFloat64Group` extent, addressed by a bounded delta encoded directly in
the referencing `PageRef`'s `Flags` (u8) and `Aux` (u16) — no second tree
walk is needed to find it. See "Float64Group sidecar addressing" below for
the exact bit layout.

## Float64Group

`internal/storeio/float64_group.go`, kind `PageFloat64Group`. A detached,
independently checksummed, column-major typed extent covering the same
consecutive chunk range as one or more adjacent `DocumentGroup` (or, for
non-grouped compact generations, `DocumentPage`) extents. Column-major
layout means a one-column reduction touches only that column's bytes.

Fixed 48-byte header (`Float64GroupPayloadHeaderSize`): version (0:4,
development format `0`),
`FirstChunk` (4:8), `ChunkCount` (8:10, u16), `ColumnCount` (10:12, u16),
`RowCount` (12:14, u16), chunk-record size (14:16, `= 8`), chunk-section
bytes (16:20, `= ChunkCount*8`), directory-section bytes (20:24, `=
ColumnCount*4`), data-section bytes (24:28), reserved (28:48).

Sections: **chunk directory** — one 8-byte `Live` bitmap per chunk; **column
directory** — one 4-byte entry per column, low 30 bits are the column's
cumulative end offset into the data section, high 2 bits are its
`Float64GroupEncoding`; **data** — per column: one 8-byte `mask` per chunk
(all masks for the column first), then that column's dense values across
every chunk in chunk order, encoded at its selected width.

`Float64GroupEncoding` (per-column, chosen by the writer to be the narrowest
that round-trips every finite value in the column exactly):

| Value | Encoding | Width |
| --- | --- | --- |
| 0 | `Float64GroupFloat64LE` | 8 (raw IEEE754, NaN/Inf rejected) |
| 1 | `Float64GroupUint8` | 1 |
| 2 | `Float64GroupUint16` | 2 |
| 3 | `Float64GroupUint32` | 4 |

### Float64Group sidecar addressing

A `PageDocumentGroup`'s `PageRef` (as stored in a `ChunkDirectory` leaf or a
`DocumentGroupFloat64Sidecar`/`AttachDocumentGroupFloat64Sidecar` call site)
encodes the sidecar's location as a small forward delta rather than a second
pointer, so no additional tree is walked:

```text
PageRef.Flags (u8):
  bit 0       DocumentGroupFlagFloat64Sidecar (1 = sidecar present)
  bits 1-4    order: log2(sidecar pages), sidecar length = allocationQuantum << order
  bits 5-7    high 3 bits of (logicalDelta - 1)

PageRef.Aux (u16):
  bits 0-10   offsetPages: sidecar.Offset = group.Offset + offsetPages * allocationQuantum
  bits 11-15  low 5 bits of (logicalDelta - 1)

sidecar.LogicalID = group.LogicalID + logicalDelta   (logicalDelta = 1..256)
sidecar.Generation = group.Generation
sidecar.Kind = PageFloat64Group
```

`offsetPages` is 11 bits (max `2047`), so `DocumentGroupFloat64MaxForwardBytes
= 2047 * allocationQuantum` bounds how far forward of the document group its
sidecar may be allocated; `logicalDelta` is 8 bits split 3-high/5-low and
ranges `1..256`. This lets many adjacent document groups share one typed
extent purely through arithmetic on their own `PageRef`, with no directory
lookup.

## Float64Catalog (stripe directory)

`internal/storeio/float64_catalog.go`, kind `PageFloat64Catalog`. An ordered
B-tree (`Float64ScanHead` root) mapping chunk ranges to `PageFloat64Stripe`
leaves — a **scan accelerator only**; documented mutation paths clear
`Float64ScanHead` and fall back to the authoritative `Float64Group`
sidecars/`DocumentPage` inline columns rather than keeping this tree
incrementally consistent. Fixed fanout 64, max depth 6
(`Float64DirectoryMaxLevel`).

32-byte header (`Float64DirectoryPayloadHeaderSize`): version (0:4,
development format `0`),
Level (4, u8, `0` = leaf), reserved (5, must be 0), count (6, u8), reserved
(7, must be 0), reserved (8:32). Followed by `count` 40-byte records
(`Float64DirectoryRecordSize`): `FirstChunk` lower bound (u32, 0:4), reserved
(4:8), child/leaf `PageRef` (8:40) — leaves (`Level == 0`) point at
`PageFloat64Stripe` pages of any valid size; branches point at
`PageFloat64Catalog` children with `Length == PageSize` exactly. Entries are
strictly ordered by `FirstChunk`.

## Float64Stripe

`internal/storeio/float64_stripe.go`, kind `PageFloat64Stripe`. An
aggregate-only, mask-free dense projection for a contiguous chunk range —
deliberately omits stable-slot validity masks (only the authoritative
Float64Group/inline columns carry those, for mutation overlay correctness);
a stripe assumes every row it names is present.

Fixed 64-byte header (`Float64StripePayloadHeaderSize`): version (0:4,
development format `0`),
`FirstChunk` (4:8), `ChunkCount` (8:12, u32), `RowCount` (12:16, u32),
`ColumnCount` (16:18, u16), column-record size (18:20, `= 12`), directory
bytes (20:24, `= ColumnCount*12`), data bytes (24:28), reserved (28:64).

**Column directory entry — 12 bytes** (`Float64StripeColumnSize`):
cumulative end offset into the data section (u32, 0:4), value `count` (u32,
4:8, `<= RowCount`), `Float64GroupEncoding` (u8, 8), reserved (9:12, must be
zero). Data section is each column's dense values back-to-back, at its
column's chosen width, in stable row order.

## PostingPage

`internal/storeio/posting_page.go`, kind `PageIndexPosting`. Packs several
independent, uniquely-identified exact-match value streams
("segments") for one declared index into one physical page. The current
certificate-bearing encoder uses development format `0`; the decoder also
accepts the un-certified payload version `1`. This codec remains defined, but
the current `store/durable` commit path does not encode this page kind;
secondary-index masks live inline in `PageIndexDirectory`.

32-byte header (`PostingPagePayloadHeaderSize`): version (0:4, development
format `0` or un-certified `1`),
`IndexID` (4:8), segment count (8:10, u16), Flags (10:12, must be `0`),
directory-section bytes (12:16, `= count*48`), data-section bytes (16:20),
reserved (20:32).

**Segment header — 48 bytes** (`PostingSegmentHeaderSize`), one per segment,
strictly increasing `StreamID`:

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | StreamID | u32, `!= 0`, exact-value dictionary id, strictly increasing across segments |
| 4:8 | FirstChunk | u32 |
| 8:12 | LastChunk | u32, `>= FirstChunk` |
| 12:16 | Rows | u32, total set bits across every entry |
| 16:24 | TupleHash | u64, candidate accelerator only |
| 24:32 | Next.LogicalID | u64, continuation segment's page (0 = none) |
| 32:36 | data offset | u32, absolute payload offset of this segment's certificate+entries |
| 36:40 | data length | u32, `len(certificate) + len(encoded entries)` |
| 40:42 | entry count | u16 |
| 42:44 | Next.Segment | u16, packed rank inside the continuation page |
| 44:46 | Flags | u16, only `PostingSegmentCollision` (`1<<0`) defined — marks a certificate whose hash covers more than one exact value/tuple |
| 46:48 | certificateLength | u16 (format `0`; version `1` reserves it and has no certificate) |

**Certificate** (`certificateLength` bytes): an exact scalar or compound-tuple
representative for the stream's hash, or empty (readers must then recheck
documents directly).

**Posting entries** (the remaining `dataLength - certificateLength` bytes):
a variable-length stream of `(chunk delta, 64-bit slot bitmask)` pairs, chunk
ids strictly increasing, entries encoded with a uvarint token:

- singleton mask (exactly one set bit): `token = delta<<7 | slot<<1` (bit 0
  clear) — one uvarint, no trailing word.
- multi-bit mask: `token = delta<<1 | 1` (bit 0 set) — a uvarint followed by
  a raw little-endian `uint64` mask.

The first entry's `delta` is always encoded as `0` regardless of its actual
chunk id (the chunk is instead taken from the segment header's `FirstChunk`);
every subsequent entry's `delta` is `chunk - previousChunk`, required `> 0`.

## IndexGroupCatalog

`internal/storeio/index_group_catalog.go`, kind `PageIndexGroupCatalog`. A
bounded, aggregate-only categorical grouping cover — one entry per
`(IndexID, distinct scalar value)` pair with its row count and one
representative row token. It does not replace the authoritative exact path:
current exact lookup is rooted in `IndexDirectory` and performs the required
certificate/document rechecks. Two page forms are accepted:
self-contained version `1` and segmented/chained development format `0` for
high-cardinality indexes that exceed one page.

**Legacy header — 32 bytes** (`IndexGroupCatalogPayloadHeaderSize`, version
`1`): version (0:4), entry count (4:8, u32), `CoveredIndexes` (8:16, u64
bitmap, one bit per durable index id), `DocumentCount` (16:24, u64), reserved
(24:32).

**Segmented header — 64 bytes** (`SegmentedIndexGroupCatalogPayloadHeaderSize`,
development format `0`): same first 24 bytes, then `Next` `PageRef` (24:56,
chains to
the following `PageIndexGroupCatalog` page; `Kind == PageIndexGroupCatalog`,
same `Generation` as this page, strictly greater `LogicalID`), reserved
(56:64).

**Entry — 32-byte header + value, 8-byte aligned**
(`IndexGroupCatalogEntryHeaderSize`): `IndexID` (u32, 0:4), value length (u32,
4:8), `Count` (u64, 8:16, rows in this group), `First` (u64, 16:24, stable
row token: `chunk = First>>6`, `slot = First&63`), reserved (24:32). Value
bytes (the exact JSON scalar representative) follow immediately and the
whole entry is padded with zero bytes to the next 8-byte boundary
(`alignIndexGroupCatalog`). Entries are ordered by non-decreasing `IndexID`.
For self-contained version `1` pages, every bit set in `CoveredIndexes` must
have entries whose `Count` sums to exactly `DocumentCount`; segmented format
`0` pages instead
only require each entry's running per-index total to stay `<= DocumentCount`
— the complete-coverage sum is checked across the whole chain, not per page.

## Materialization journal

`internal/storeio/materialization_journal.go`. Each of the two fixed 4096-byte
slots stores one complete undo capsule. Capsules alternate by successfully
published sequence; recovery selects the newest checksum-valid monotonic
capsule, not a sequence-derived physical slot. Empty or torn prospective
capsules do not supersede the preceding valid one.

The fixed header is 112 bytes:

| Offset | Field | Notes |
| --- | --- | --- |
| 0:8 | magic | `"SJMTRL01"` |
| 8:12 | version | development format `0` |
| 12:16 | size | `4096` |
| 16:24 | commit marker | fixed `"COMMIT01"` word |
| 24:32 | ~commit marker | complement |
| 32:48 | StoreID | same file as roots and targets |
| 48:56 | Sequence | non-zero monotonic capsule sequence |
| 56:64 | ~Sequence | complement |
| 64:72 | TargetGeneration | root generation that commits the after-images |
| 72:80 | ~TargetGeneration | complement |
| 80:84 | PageSize | Store page size |
| 84:86 | target count | `1..6` |
| 86:88 | patch count | `1..7` |
| 88:92 | target-table offset | canonical value `112` |
| 92:96 | patch-table offset | immediately after target records |
| 96:100 | data offset | immediately after patch records |
| 100:104 | data length | packed before-image bytes, at most `3584` |
| 104:108 | flags | zero |
| 108:112 | SectorSize | persisted qualified damage granule, power of two and at least `512` |

Each 56-byte target record contains a complete 32-byte `PageRef`, before-image
CRC32C, context CRC32C for bytes outside undo spans, a 96-bit
domain-separated SHA-256 truncation over the after-image patch geometry and
bytes, and the first/count range of its patches. The strong digest covers only
the at-most-3584 bytes the commit can tear; the common-page CRC covers the
complete admitted image. This avoids both the weak, correlated second CRC32C
and another full-page cryptographic scan. Each 16-byte patch record contains
target rank (u16), data length (u16), target-relative offset (u32),
packed-data offset (u32), and four reserved zero bytes. Patches are aligned
complete damage granules, sorted by `(target, offset)`, non-overlapping and
non-adjacent. Before-image data is packed after the tables. Bytes through
offset 4088 are CRC32C-covered; 4088:4092 stores the checksum and 4092:4096
its complement.

The capsule is deliberately bounded. An update whose complete undo sectors and
metadata do not fit must use ordinary copy-on-write; the format never creates
a read-visible delta chain.

## Recovery journal (`RJRNL01`)

`internal/storeio/recovery_journal.go`. A bounded redo log kept in a **separate
preallocated file beside the store**, not a region inside it: `fdatasync`
flushes every dirty page of the file it is called on, so an in-store journal
region would drag concurrently pre-written checkpoint pages into every
acknowledgement sync. A dedicated file keeps the sync domain to the journal's
own preallocated pages. Readers never consult it; it is write-only until crash
recovery replays it. The `StateRoot.JournalID` (payload `320:336`) names the
paired journal file, and the journal header repeats `{StoreID, JournalID}` so
recovery cannot pair mismatched files. A referenced-but-missing journal fails
closed; a cleanly-recycled journal with no live records is the ordinary empty
state.

The file begins with two alternating 512-byte header slots (record region
starts at `1024`). Recycle rewrites the slot opposite the live one and syncs,
so a torn recycle leaves the previous header intact and recovery falls back to
it — the same double-publication reason the store keeps two roots. Each header:

| Offset | Field | Notes |
| --- | --- | --- |
| 0:8 | magic | `"RJRNL01\0"` |
| 8:12 | version | development format `0` |
| 12:16 | header size | `512` |
| 16:32 | StoreID | must equal the paired store |
| 32:48 | JournalID | must equal `StateRoot.JournalID` |
| 48:52 | PageSize | store page size |
| 52:56 | SectorSize | append/damage granule, power of two, at least `512` |
| 56:64 | BaseGeneration | records `<=` this are folded into the root; replay skips them |
| 64:72 | BaseSequence | first live record must carry `BaseSequence+1` |
| 72:80 | Capacity | preallocated record-region bytes, a whole multiple of `SectorSize` |
| 80:88 | RecycleCount | strictly monotonic; recovery selects the highest valid slot |
| 504:508 | checksum | CRC32C over `[0:504)` |
| 508:512 | ~checksum | complement |

Records are appended at the write cursor, each padded up to a whole
`SectorSize` so a torn append can only damage its own tail. The 32-byte fixed
prefix is magic (`"RRJ1"`), kind (u16: `1` put, `2` delete-reserved), two
reserved bytes, sequence (u64), generation (u64), key length (u32), and value
length (u32), followed by the key bytes, the value bytes, a CRC32C over the
prefix-through-value body, and its complement. Recovery scans from the region
start validating strict-monotonic sequence (`BaseSequence+1`, then `+1` each)
and framing; the first invalid record is the truncation point of a torn or
reordered tail. Records whose generation exceeds the selected root's generation
are replayed through the ordinary mutation path; the rest are already durable
in the root. A record that will not fit the preallocated capacity forces a
checkpoint, which recycles the head — the file never extends per record, so an
acknowledgement sync never commits filesystem metadata.

## Checksum and integrity

Every inline root, materialization capsule, and common page is protected by
CRC32C (Castagnoli polynomial, `crc32.MakeTable(crc32.Castagnoli)`) plus its
bitwise complement stored immediately after it — the complement is a cheap
second independent detector for a sector-level torn or short write that
happens to leave the checksummed region itself unmodified. `PageChecksum`
(`internal/storeio/superblock.go`) is the single algorithm used everywhere;
`internal/storeio/page_checksum_std.go`'s fallback is `crc32.Checksum(data,
pageChecksumTable)` — Go's hardware-dispatched CRC32C.

Coverage:

- **Inline root**: CRC32C over bytes `[0:4088)` of its 4096-byte prefix.
- **Materialization journal**: CRC32C over bytes `[0:4088)`.
- **Recovery journal header**: CRC32C over bytes `[0:504)` of each 512-byte
  header slot.
- **Recovery journal record**: CRC32C over the record's prefix, key, and value
  (excluding the sector padding), stored with its complement at the body end.
- **Common page**: CRC32C over bytes `[0 : PageSize-8)` — the full 64-byte
  header, the payload, and the zero padding between the payload and the
  trailer. Padding is checksum-covered even though readers never re-scan it
  directly, so a corrupted padding byte still fails the CRC.

`internal/storeio/page_checksum_simd_amd64.go` and
`page_checksum_simd_arm64.go` (`goexperiment.simd`-gated, Go's experimental
SIMD intrinsics) implement carry-less-multiplication CRC32C folding —
`pageChecksumAVX512` folds four independent 512-bit streams with VPCLMULQDQ
plus a final 128-bit PCLMUL reduction, and a PCLMUL-only 8-stream fallback
exists for pre-AVX-512 hardware. **As currently wired, neither SIMD path is
selected by `pageChecksum`**: both files' own `pageChecksum` function still
calls the standard-library CRC32C (their doc comments state the SIMD bodies
are "directly correctness- and ISA-tested candidates" kept available for a
future dispatch, not an active fast path). This is worth flagging explicitly
since the presence of dedicated SIMD source files could otherwise suggest
they are already on the hot path.

## Commit and durability protocol

`internal/storeio/committer.go`, `committer_worker.go`,
`committer_materialization.go`, `device_portable.go`, and `device_ring.go`.

**Ordinary copy-on-write.** The physical `Device.Commit` contract is:

1. write all sorted, non-overlapping staged data pages;
2. take a data-integrity barrier when data pages exist;
3. write the complete alternate `PageSize` inline root slot;
4. take the final root barrier.

A crash before root publication leaves the preceding root authoritative. A
torn new root fails its complement/CRC/schema checks. A fully selected root
can name only data pages that crossed the earlier barrier. Recovery validates
both inline-root candidates newest-first and every top-level reference before
returning a generation.

One serialized producer and one private device-owner goroutine form the
asynchronous `Committer` pipeline. Ordinary adjacent generations may be group
committed: all of their data pages are written, but only the newest root is
published. This suppresses intermediate root writes without changing the
format or weakening the newest published generation's ordering.

**Optional canonical materialization.** A file may enter this path only when
StateRoot persists `StateOptionCanonicalMaterialization` and an exact non-zero
`MaterializationDamageGranule`. Eligible patch-only updates execute two
separately synchronized phases:

1. write and synchronize the alternate complete 4 KiB undo capsule;
2. positional-write its recorded target sectors and the alternate complete
   inline root in either order, then synchronize them together.

No target-before-root ordering is required because recovery accepts a root
exactly at `TargetGeneration` only after strongly validating every target
after-image. A hybrid batch containing any unjournaled immutable COW page
retains three phases—journal, all data, root—because the capsule cannot prove
those full pages complete. On Darwin intermediate ordering boundaries use
`F_BARRIERFSYNC` (falling back to `F_FULLFSYNC` when unsupported), while the
final acknowledgement and recovery-repair boundaries use `F_FULLFSYNC`.
Other portable builds use the data-sync primitive. The io_uring backend uses
the same two- versus three-fence distinction. Materialized batches are
isolated queue boundaries and are never group-coalesced.

Recovery resolves the newest valid journal against each root candidate. If a
candidate is older than `TargetGeneration`, recovery preflights every target,
reconstructs complete before-image sectors, applies the rollback, and
synchronizes repaired pages before exposing that root. A candidate exactly at
`TargetGeneration` is accepted only when every target matches its context,
96-bit after-patch digest, common-page checksum, StoreID, and exact `PageRef`.
A newer candidate does not inspect the stale capsule because later serialized
commits may already have reused its target extents. The equality rule also
applies before advertising an alternate fallback generation. Successful
capsules remain retained. After recovery has durably rolled back a failed
capsule, it invalidates and synchronizes every otherwise-valid inline root at
that failed generation, then clears and separately synchronizes only the
failed journal slot. The ordering is rollback fence → failed-root invalidation
fence → journal-cancellation fence. This permits the skipped generation to be
reused without either selecting a rolled-back exact root or colliding with
stale journal metadata.

### Reclamation

A retired physical extent (`FreeExtent{Offset, Length, RetiredGeneration}`)
cannot be reused until every reader that might still resolve it has moved
on. `GenerationLeases` (`generation_leases.go`) is a fixed-capacity table of
active reader snapshots; `Minimum(current)` returns the oldest generation any
lease still protects (or `current+1` if none are active). `ExtentReclaimer`
accumulates retired extents (`Retire`/`RetireBatch`, overlap-checked) and
`AppendReusable(dst, currentGeneration, oldestRecoveryGeneration)` releases
into the caller's reuse pool only the extents whose `RetiredGeneration` is
strictly below both the reader floor and the oldest generation a crash
recovery might still need to select — i.e. an extent is reusable only once
no live reader and no still-selectable inline-root generation can reach it.

## Golden-format tests

`internal/storeio/format0_golden_test.go` regenerates checked-in, byte-exact
sparse fixtures through public encoders and compares the complete decoded
images, including every implicit zero byte. The initial
`internal/storeio/testdata/format0/` suite covers the 4 KiB mutable prefix,
empty standalone and inline roots, the provisional standalone superblock,
representative document/fingerprint/posting pages, both maximum materialization
journal geometries, fixed constants and kind numbers, checksums/complements,
reserved-zero ranges, resealed semantic corruption, and checksum-valid
rejection of the removed TTL numbering.

The remaining format-0 completion gates are:

1. fixture-backed 8 KiB and 64 KiB mutable prefixes (their exact root, journal,
   and `DataStart` geometry is already asserted as constants);
2. a maximum-inline-free root and corruption coverage for every remaining
   inline field class;
3. byte-exact pages for Overflow, ChunkDirectory, the legacy KeyDirectory,
   IndexDirectory, DocumentGroup, all float64 and index-group kinds, and all
   free-log kinds;
4. the superseded external-root magic and a complete `store/durable` file with
   a populated legacy `PageKeyDirectory` primary root;
5. crash-matrix fixtures captured after each ordinary phase, both patch-only
   materialization phases, and all three hybrid phases, asserting selected
   generation, rollback bytes, idempotent second recovery, and reclamation
   fence;
6. extension of the existing source-constant conformance table to every
   remaining page-specific offset and width.

## See also

- [store.md](store.md) — public API, `Options` semantics and defaults, mutation
  and query behavior, snapshots, and index construction.
- [durability.md](durability.md) — acknowledgement and platform persistence
  contracts layered over this publication format.
- [provenance.md](provenance.md) — externally derived source/algorithm inventory; the
  on-disk format itself is original to this repository and has no
  provenance-ledger entry (the Roaring-inspired posting/candidate execution
  strategy it stores is ledger entry `ALGO-ROARING-001`, but that entry
  covers in-memory execution, not this file format).
- [UNSAFE.md](../UNSAFE.md) — safety invariants for the `unsafe`-using code that
  interprets these bytes.
