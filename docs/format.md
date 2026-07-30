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
common page envelope is currently version `5` specifically to make pages from
superseded development layouts fail closed; each in-place development
renumbering bumps it so a valid checksum is never the only thing selecting a
decoder.
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

The base page (`PageSize`) is fixed at 4096 bytes. Most roots and metadata pages
are one base page. The global primary catalog has its fixed production extent,
and exact-catalog pages, leaves, and overflow extents may grow from one base
page up to `MaxPageSize` (default 64 KiB); exact-catalog and exact-leaf extents
are powers of two, while primary document and overflow extents use their
kind-specific sizing rules. Variable base page sizes are not supported —
`Create` rejects any `PageSize` other than 4096 with `ErrUnsupportedPageSize`,
and `Open` of a store that recorded a different base page fails the same way.
The base page is no longer discovered from the file.

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
  -> ExactIndexRoot
       -> PagePrimaryExactRoot
            -> zero for an empty physical index, otherwise
               PagePrimaryExactCatalog
                 -> level-0 ordered term-leaf entries, or
                 -> level-1 child refs to level-0 catalog pages
                      -> PagePrimaryExactLeaf posting tiles
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
| 8:10 | version | u16 | `5` (`pageVersion`); rejects every superseded development layout |
| 10:12 | headerLength | u16 | `PageHeaderSize` = `64`, self-describing |
| 12 | Kind | u8 | `PageKind`, see table below |
| 13 | Flags | u8 | `0` for every current kind |
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
	_                                 // 2  (was PageDocument; retired)
	PageOverflow                      // 3
	_                                 // 4  (was PageChunkDirectory; retired)
	_                                 // 5  (was PageKeyDirectory; retired)
	_                                 // 6  (was PageIndexDirectory; retired)
	PageIndexPosting                  // 7
	_                                 // 8  (was PageDocumentGroup; retired)
	_                                 // 9  (was PageFloat64Group; retired)
	_                                 // 10 (was PageFloat64Catalog; retired)
	_                                 // 11 (was PageFloat64Stripe; retired)
	_                                 // 12 (was PageIndexGroupCatalog; retired)
	PageFreeImage                     // 13
	PageFreeDelta                     // 14
	PageFreeIndex                     // 15
	_                                 // 16 (was PageFingerprintDirectory; retired)
	PageCatalogSegment                // 17
	PagePrimaryCatalog                // 18
	PageTabletDirectory               // 19
	PagePrimaryLocator                // 20
	PageTabletRoute                   // 21
	PagePrimaryAnchor                 // 22
	PagePrimaryLeaf                   // 23
	PagePrimaryExactRoot              // 24
	PagePrimaryExactLeaf              // 25
	PagePrimaryExactCatalog           // 26
)
```

Retired identifiers are held blank so every surviving durable kind keeps the
on-disk value it already had; a page carrying a retired kind fails closed on
decode. `PageIndexPosting` survives because the in-memory heap store encodes
its query-time packed index in this format.

| Value | Kind | Purpose |
| --- | --- | --- |
| 1 | `PageStateRoot` | standalone StateRoot envelope for lower-level/provisional use; mutable durable roots embed the payload |
| 2 | — | retired (was the chunk-layout document micro-page) |
| 3 | `PageOverflow` | one ordered piece of a JSON value too large to inline |
| 4 | — | retired (was the chunk-radix directory) |
| 5 | — | retired (was the development key directory) |
| 6 | — | retired (was the chunk exact-index directory) |
| 7 | `PageIndexPosting` | packed exact-match posting codec; used by the in-memory heap store's packed index |
| 8 | — | retired (was the chunk compact document-group extent) |
| 9 | — | retired (was the float64 column-major sidecar) |
| 10 | — | retired (was the float64 stripe directory) |
| 11 | — | retired (was the float64 dense stripe) |
| 12 | — | retired (was the categorical index-group cover) |
| 13 | `PageFreeImage` | one free-set image segment, ordered by offset |
| 14 | `PageFreeDelta` | one free-set diff page and predecessor link |
| 15 | `PageFreeIndex` | directory naming independent free-image segments |
| 16 | — | retired (was the fingerprint key directory) |
| 17 | `PageCatalogSegment` | self-describing canonical Store catalog segment; reopen selects this decoder by durable kind, never by probing payload |
| 18 | `PagePrimaryCatalog` | primary graph root: exact lexical catalog whose leaves name each macro-tablet's root `PageRef` (`StateRoot.PrimaryRoot`) |
| 19 | `PageTabletDirectory` | tablet-directory node of the primary catalog, routing a key to the macro-tablet that owns it |
| 20 | `PagePrimaryLocator` | tablet local-ID locator mapping a stable `BucketID` to the current leaf page and slot holding it |
| 21 | `PageTabletRoute` | tablet segmented route block over its primary leaves |
| 22 | `PagePrimaryAnchor` | tablet lexical anchor map: immutable interval fences routing to stable `BucketID`s |
| 23 | `PagePrimaryLeaf` | ordered primary leaf holding the key/value records |
| 24 | `PagePrimaryExactRoot` | physical exact-index id to `(LeafCount, Catalog PageRef)` root built beside `PrimaryRoot` (`StateRoot.ExactIndexRoot`); aliases share one physical-index record |
| 25 | `PagePrimaryExactLeaf` | common-page envelope around one canonical adaptive `IndexTermLeaf` byte stream: one bounded ordered term slice or one giant-term stripe piece |
| 26 | `PagePrimaryExactCatalog` | one level-0 ordered term-leaf catalog page, or one level-1 page naming level-0 children for a physical exact index |

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
| 44:52 | IndexCatalogHash | u64 | binds the durable catalog (exact indexes, schema) |
| 52:56 | FreeChunkHint | u32 | conservative lower bound of a chunk that may have a free slot |
| 56:88 | — | `PageRef` slot | retired (was the chunk directory root); held blank |
| 88:120 | — | `PageRef` slot | retired (was the key/fingerprint directory root); held blank |
| 120:152 | — | `PageRef` slot | retired (was the chunk exact-index directory root); held blank |
| 152:184 | — | `PageRef` slot | retired (was the float64 scan head); held blank |
| 184:216 | — | `PageRef` slot | retired (was the index-group head); held blank |
| 216:220 | MaterializationDamageGranule | u32 | `0` disables canonical materialization; otherwise the persisted qualified power-loss damage granule |
| 220:224 | MaxPageSize | u32 | largest physical extent admitted by this Store |
| 224:256 | PageCatalogHead | `PageRef` | first segment of the exact canonical Store catalog |
| 256:272 | PageCatalogDigest | 16 bytes | digest of the canonical catalog bytes |
| 272:276 | PageCatalogBytes | u32 | exact canonical catalog length; zero means absent |
| 276:280 | MaxKeyBytes | u32 | immutable key-admission bound |
| 280:284 | InlineValueBytes | u32 | immutable inline-value bound |
| 284:288 | MaxDocumentBytes | u32 | immutable complete-document bound |
| 288:320 | PrimaryRoot | `PageRef` | the 64 KiB `PagePrimaryCatalog` root at `PrimaryCatalogRootLogicalID`; the sole document root — a zero `PrimaryRoot` fails `Open` |
| 320:336 | JournalID | 16 bytes | UUID of the paired recovery journal file; zero means no journal is referenced |
| 336:368 | ExactIndexRoot | `PageRef` | `PagePrimaryExactRoot` for exact indexes built beside `PrimaryRoot`; aliases share one physical-index record. Zero when the collection declares no indexes |
| 368:512 | reserved | — | zero |

The retired `PageRef` slots are held blank so the fields after them keep
their byte offsets; nothing writes or reads them any longer. `PrimaryRoot` has
the fixed production catalog identity, kind, and 64 KiB length and requires the
reserved primary logical-ID bands below `NextLogicalID`. `ExactIndexRoot` is
one `PageSize` version-1 root whose ordered records contain one physical
index's leaf count and catalog reference. An empty physical index has a zero
count and zero catalog; a populated one names a `PagePrimaryExactCatalog`.
Exact-catalog and exact-leaf extents are powers of two up to `MaxPageSize`, and
each exact-leaf payload is exactly `AppendIndexTermLeaf` output.
Every populated top-level ref has `Generation <= root.Generation`; all roots are
pairwise distinct in both `LogicalID` and `Offset`.

Ordered-primary leaves are `PagePrimaryLeaf` pages using one canonical unified
grammar. Payload byte `[2]` has low seven bits `5`; its high bit marks the
empty spelling. Payload byte `[0]` is `liveRows-1` for a non-empty leaf and
zero for an empty leaf, and byte `[1]` is the live stash count. Every leaf has
the 256-slot succinct ordered envelope: 192 normal cuckoo slots, 64 stash
slots, lexical-rank tables, 7-bit key lengths, an overflow bitmap, succinct
cumulative record boundaries, and select checkpoints. The physical extent is
the planner-selected power of two from 4–64 KiB.

Immediately after the envelope metadata is the 16-byte unified header:

| Offset | Field | Notes |
| --- | --- | --- |
| 0:2 | TemplateCount | u16, at most 255 |
| 2:4 | DictionaryCount | u16, at most 128 |
| 4:8 | TemplateSectionBytes | u32, directory plus entries |
| 8:12 | DictionarySectionBytes | u32, directory plus spellings |
| 12:16 | TrivialContentBytes | u32, exact bytes required if every row used its canonical-trivial spelling |

The template section starts with cumulative u32 entry ends. Each entry is
`HoleCount` (u16), zero (u16), `StaticBytes` (u32), `HoleCount+1`
cumulative u32 static-segment ends, then the canonical static bytes. The
dictionary section likewise starts with cumulative u32 ends followed by the
raw canonical scalar spellings.

The record heap is in lexical key order. Each record is `[key][row body]`;
the envelope supplies the boundaries and key length. A row body begins with
either a template id and its token stream or `0xff` followed by the complete
canonical JSON spelling. Overflow rows are selected by the envelope bitmap and
carry a 32-byte `PageRef` instead of an inline row body. Token tags `0x00–0x7f`
are dictionary ids, `0x80–0xf7` are 1–120-byte literals, `0xf8` is a
uvarint-length literal, `0xf9/0xfa/0xfb` are true/false/null, and `0xfc`
is a zigzag-varint canonical integer. `0xfd/0xfe` are invalid.

The posting-stable slot is always the envelope's cuckoo slot. Bulk build and
checkpoint fold use the same deterministic planner; point mutations publish
generation-stamped row/tombstone overlays and folds re-census the final live
rows into this same grammar. Templated and trivial rows are encodings within
the grammar, not alternate leaf classes or collection modes.

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

## Ordered-primary exact index

`internal/storeio/primary_exact_index.go`, `index_term_leaf.go`, and
`index_term_cutter.go`; kinds `PagePrimaryExactRoot`,
`PagePrimaryExactCatalog`, and `PagePrimaryExactLeaf`. A physical exact index
is an ordered set of bounded term leaves rather than one whole-index leaf.
`StateRoot.ExactIndexRoot` names the one-base-page root. Its payload version is
`1`; version-0 exact roots from the superseded one-leaf layout are rejected and
stores containing them are recreated rather than migrated.

**Exact root.** The fixed payload header is 16 bytes:

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | version | u32, `1` |
| 4:8 | count | u32, exactly `StateRoot.IndexCount`, at most 64 |
| 8:16 | reserved | zero |

The header is followed by `count` fixed 40-byte records in physical-index-id
order:

| Record offset | Field | Notes |
| --- | --- | --- |
| 0:4 | LeafCount | u32, number of term leaves in this physical index |
| 4:8 | reserved | zero |
| 8:40 | Catalog | `PageRef`, kind `PagePrimaryExactCatalog`, or zero |

`LeafCount == 0` if and only if `Catalog` is zero. Logical aliases resolve to
the same physical index id and do not duplicate root records or term leaves.

**Exact catalog.** A catalog page is a power-of-two extent no larger than
`MaxPageSize`. Its common 16-byte payload header is:

| Offset | Field | Notes |
| --- | --- | --- |
| 0:4 | version | u32, `1` |
| 4 | level | u8, `0` leaf catalog or `1` child-reference page |
| 5:8 | reserved | zero |
| 8:12 | count | u32, non-zero |
| 12:16 | reserved | zero |

A level-0 catalog contains `count` variable-length records:

| Record offset | Field | Notes |
| --- | --- | --- |
| 0:32 | Leaf | `PageRef`, kind `PagePrimaryExactLeaf` |
| 32:36 | FirstTile | u32, first posting tile of the leaf's first term |
| 36 | Flags | u8, only `Piece = 1` and `RunCut = 2` |
| 37 | PrefixLength | u8, `<= 40` |
| 38:... | Prefix | first term's canonical bytes, truncated to 40 bytes |

`Piece` marks a single-term piece emitted for a giant term.
`RunCut` records that the leaf's first term satisfies the content-defined
term-boundary predicate. A level-1 catalog contains at least two consecutive
32-byte `PagePrimaryExactCatalog` child refs after its header; every child must
be level 0, so catalog depth is at most two. A physical index that fits one
catalog extent uses one level-0 page directly.

**Term leaves and deterministic cuts.** `PagePrimaryExactLeaf` wraps exactly
one canonical `AppendIndexTermLeaf` byte stream; the nested `IndexTermLeaf` and
`TermPosting` codecs are unchanged by spanning. Its physical extent is the
smallest power of two that holds the common envelope and payload. The cutter's
payload budget is:

```text
min(IndexTermLeafMaxBytes, MaxPageSize - PageHeaderSize - PageTrailerSize)
```

The cutter is deterministic for the final posting content:

1. A term starts a content-defined run when the low 16 bits of its StoreID-keyed
   route hash are below `1365`, targeting one run per 48 terms.
2. A term too large for an otherwise empty leaf is divided at absolute
   2048-tile stripe boundaries. Empty stripes emit nothing.
3. A run or stripe that still exceeds the byte budget is cut at the last term
   or posting that fits.

Sizing uses a deterministic upper bound on encoded bytes, so it may cut earlier
than an exact trial encode but never emits an oversized leaf. Identical final
postings therefore produce identical leaf boundaries in bulk build,
checkpoint fold, and journal replay. One term may occupy consecutive piece
leaves ordered by increasing first tile; an entire physical index no longer
has to fit one `MaxPageSize` extent.

**Admission.** Root selection validates the exact-root envelope and its
per-index catalog refs. Full `Open` then derives posting liveness from the
selected primary graph, walks each catalog (depth at most two), requires the
flattened entry count to equal `LeafCount`, and admits every `IndexTermLeaf`
against those live masks. It recomputes each entry's prefix, first tile,
`Piece`, and `RunCut` fields from leaf content; requires a piece leaf to contain
one term; and requires strict full `(first canonical term, first tile)` order
across catalog pages. Catalog-local decoding also validates record lengths,
reserved bytes, allowed flags, reference bounds, and non-decreasing stored
prefix order. A stale posting, grafted leaf, reordered catalog, or mismatched
router certificate therefore makes `Open` fail closed.

## ChunkDirectory (retired)

Kind 4 carried the chunk layout's packed-radix routing from chunk id to
document extents, with optional per-lane 30-byte zone summaries. The layout is
gone: no current file contains one, the identifier is held blank, and a page
claiming it fails closed on decode. The ordered primary graph routes through
`PagePrimaryCatalog`/`PageTabletDirectory`/`PagePrimaryAnchor` instead.

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
±Inf are rejected). This is the *raw inline* float64 encoding; it differs
from the separately checksummed `Float64Group`/`Float64Stripe` typed extents
described below.

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

## Float64Group

`internal/storeio/float64_group.go`, kind `PageFloat64Group`. A detached,
independently checksummed, column-major typed extent covering the same
consecutive chunk range as one or more `DocumentPage` extents. Column-major
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
returning a generation. This selection phase admits the top-level
`PagePrimaryCatalog` and `PagePrimaryExactRoot` codecs; it does not recursively
prove every descendant. After selection, `store/durable.Open` validates the
primary routing graph and performs the full exact-index admission described
above. A failure in that deeper admission fails `Open` closed; the current
implementation does not restart inline-root selection with the older candidate
because of a descendant-only error.

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

### Offline verification

`store/durable.Verify` selects the root with the same first-phase rule, then
walks the reachable graph. For ordered-primary exact indexes it traverses
`PagePrimaryExactRoot` to every level-1/level-0 catalog page and referenced
term-leaf envelope, checks their common-page identities and local catalog
shape, records their extents for alias and free-overlap detection, and rejects
a catalog tree deeper than two levels.

That offline walk is intentionally not the same as online semantic admission.
It does not currently compare each root `LeafCount` with the number of
traversed leaves, recompute catalog prefixes/tiles/flags from term content, or
prove posting bits against live masks derived from the primary graph. Full
`Open` performs those exact-index semantic checks.

## Golden-format tests

`internal/storeio/format0_golden_test.go` regenerates checked-in, byte-exact
sparse fixtures through public encoders and compares the complete decoded
images, including every implicit zero byte. The initial
`internal/storeio/testdata/format0/` suite covers the 4 KiB mutable prefix,
empty standalone and inline roots, the provisional standalone superblock,
representative document/fingerprint/posting pages, both maximum materialization
journal geometries, fixed constants and kind numbers, checksums/complements,
reserved-zero ranges, and resealed semantic corruption.

The remaining format-0 completion gates are:

1. fixture-backed 8 KiB and 64 KiB mutable prefixes (their exact root, journal,
   and `DataStart` geometry is already asserted as constants);
2. a maximum-inline-free root and corruption coverage for every remaining
   inline field class;
3. byte-exact pages for Overflow, ChunkDirectory, the legacy KeyDirectory,
   IndexDirectory, the ordered-primary exact root/catalog/leaf kinds, all
   float64 and index-group kinds, and all free-log kinds;
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
