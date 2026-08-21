# On-disk format

This document maps the one mutable format written by `store/durable` and
decoded by `internal/storeio`. The codecs are the byte-level authority. All
multi-byte integers are little-endian, all reserved bytes are zero, and all
format sentinels inside the primary store file contain numeric `0`
(`DevelopmentFormatVersion`). The separate recovery-journal and transaction-
decision sidecars each use one current grammar. In both sidecar headers,
`Format` is a corruption sentinel that must contain numeric `0`; every other
value is rejected. StateRoot and the current primary layout are unchanged by
multi-collection transactions; their new on-disk surface is the `txn.vtm`
decision-log sidecar and kind-4 conditional journal records.

The project is unreleased. A primary-file schema change replaces the current
grammar and regenerates its golden images. There are no migration decoders,
alternate primary-file grammars, or retired page layouts. The two
sidecar codecs likewise decode only the current grammars described here.

The SQL catalog follows the same current-only rule. The `format` members of
`replicated_shard_store`, `replicated_apply`, and its placement profile are
numeric-zero corruption sentinels. Every other SQL catalog grammar is rejected.

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
| 608:4088 | current inline free-set delta and reserved zero space |
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
| 192:200 | immutable main-file physical-capacity ceiling, or zero for elastic allocation |
| 200:512 | reserved zero suffix |

The ordered-primary root is the sole document root. A mutable store cannot open
without it. The current option bits are schema binding, persisted primary-stripe
skip summaries, and canonical materialization qualification; unknown bits fail
closed.

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

The compact stripe payload begins with one 40-byte `VCS1` header:

| Offset | Field |
| --- | --- |
| 0:4 | magic `VCS1` |
| 4:8 | row count |
| 8:10 | shape count |
| 10 | shape-code bit width |
| 11 | flags; bit 0 means overflow rows are present |
| 12:16 | encoded key-stream bytes |
| 16:20 | shape-code bytes |
| 20:24 | rank-checkpoint bytes |
| 24:28 | shape-stream bytes, excluding summaries |
| 28:32 | stable-slot bytes |
| 32:34 | catalog-ordered summary count, at most 8 |
| 34:36 | reserved zero |
| 36:40 | trailing summary-section bytes, at most 4 KiB |

After the existing shape directory, key/slot/overflow sections, shape codes,
rank checkpoints, and shape streams, the summary section carries one
variable-capacity entry per persisted skip path. Each entry starts with
`{flags u8, reserved u8, entryBytes u16, minBytes u16, maxBytes u16}` followed
by canonical ordered scalar term bytes and zero padding. Bit 0 marks valid
extrema; every other flag and reserved byte is zero. Each bound is at most 256
bytes, `min <= max`, and every term must pass the same ordered-key grammar used
by exact indexes. A disabled entry contains no bounds and an all-zero body.
Open validates the complete tiling, padding, term grammar, and bounded count
before admitting the leaf; a filtered scan additionally requires that count to
equal its pinned catalog path count.

Values up to `InlineValueBytes` remain in the leaf. Larger values use a
forward-linked `PageOverflow` chain whose fixed header and every reference are
validated before bytes are returned. Publication creates the overflow chain
and its referencing leaf in one generation.

## Exact indexes

`StateRoot.ExactIndexRoot` names one current `PagePrimaryExactRoot`. Its
records are ordered by physical index id and contain a leaf count plus a
`PagePrimaryExactCatalog` reference. Logical aliases share one physical record.

An exact catalog is a bounded one- or two-level ordered tree over
`PagePrimaryExactLeaf` pages. Each leaf wraps one canonical `IndexTermLeaf`
stream. Content-defined term runs and fixed giant-term tile stripes make leaf
boundaries deterministic for the final posting content. Open walks the entire
catalog, validates order and routing prefixes, and admits every posting against
the selected primary graph's live slot masks.

## Catalog, free space, and journals

`PageCatalogSegment` stores the canonical schema, exact-index definition, and
ordered skip paths. Its 64-byte canonical header carries skip-path count at
`46:50`; bytes `50:64` remain reserved zero. The sorted skip-path string IDs
follow logical exact-index aliases and precede schema fields. StateRoot records
the catalog byte length and digest, so reopen reconstructs and hashes the exact
bytes rather than trusting a compact rejection key. The catalog order is the
summary ordinal order in every primary stripe.

The allocator publishes a free image plus bounded deltas through
`PageFreeImage`, `PageFreeIndex`, `PageFreeDelta`, and the inline free run.
Every extent is aligned, non-overlapping, above `DataStart`, and absent from the
selected live graph before reuse.

The paired recovery journal (`recovery_journal.go`) is the separate
`<store>.rjournal` file used by synchronous mutation acknowledgement and by
eligible buffered-visible checkpoint deltas. It is eager for synchronous and
explicit per-mutation-journal stores, but ordinary buffered-visible bulk images
omit it until their first valid mutation. The first physical checkpoint then
publishes its identity in `StateRoot.JournalID`; a journal-only checkpoint is
forbidden before that root is durable. Readers never consult it.

The journal starts with two alternating 512-byte header sectors followed by a
sector-aligned, bounded record region. A header records `Format == 0`, store and
journal ids, page and sector geometry, base generation and sequence, capacity,
monotonic recycle count, and flags under a CRC32C and its complement. Creation
allocates the initial region before publishing its identity; positional record
appends never extend it. An ordinary unsealed acknowledgement journal may grow
within the hard ceiling before an oversized record's point of no return. Growth
preallocates the extension, publishes its capacity through the alternate
header, and synchronizes that header before the new geometry becomes
authoritative. Linux normally uses `fallocate` and falls back to truncate only
when allocation is unsupported; other platforms set the requested size with
truncate. That fallback establishes an ordinary sidecar's EOF, not a physical-
allocation certificate.

Header selection may ignore an all-zero or checksum-invalid torn alternate
slot. A checksum-authenticated slot whose domain, geometry, flags, reserved
bytes, or other semantics are invalid is hard corruption and blocks fallback
to an older slot. The same rule applies to `txn.vtm`; it prevents an older
capacity, base, or marker epoch from hiding acknowledged records.

The current recovery-journal header assigns bytes `88:92` as its flags word.
Bit 0 (`SealedCapacity`) says that the header's record-region `Capacity` is an
immutable physical-allocation certificate. Bits 1 through 31 are reserved and
bytes `92:504` are reserved; all must remain zero. A reader rejects the header
if any reserved bit or byte is set. The
complete sealed file length is exactly `1024 + Capacity`: both 512-byte headers
are outside the record-region capacity. The flag and capacity are covered by
the header checksum and survive header recycle.

The recovery-journal record-region hard ceiling is 16 MiB plus 17,408 bytes.
That bound covers the current replicated SQL ceiling: a 16 MiB command budget,
64 maximum-size 256-byte keys, conditional and per-entry framing, checksum
trailer, and final 512-byte sector padding. It is an allocation/hostile-header
clamp, not a promise that arbitrary larger durable collection options qualify
for sealing.

The current ordinary buffered-delta policy caps, and with the shipped overlay
geometry selects, a 2.5 MiB record region. Its foreground admission guard keeps
up to 512 KiB for one estimated future carried suffix, leaving the qualified
2 MiB current append window. This reserve is a bounded fallback policy rather
than another on-disk region: an exact current batch that does not fit, or a
future suffix wider than the reserve, takes the physical checkpoint path. The
two 512-byte headers are additional to the record-region capacity. Per-mutation
acknowledgement journals use their option-derived bounded capacity rather than
inheriting the 2.5 MiB delta policy.

Every record begins with a 32-byte prefix and ends with a CRC32C and its
complement, then zero padding to the 512-byte damage granule. The prefix carries
kind, reserved-zero bytes, sequence, generation, and either key/value lengths
or batch entry-count/body-length. One checksum covers the complete ordered
batch, so a torn batch is not partially admitted.

The current top-level record kinds are:

| Value | Kind | Generation grammar |
| ---: | --- | --- |
| 1 | `Put` | one logical generation |
| 2 | `Delete` | one logical generation |
| 3 | `Batch` | one atomic generation of ordered put/delete entries |
| 4 | `ConditionalBatch` | one decision-bound atomic generation of ordered put/delete entries |
| 5 | `DeltaBatch` | one put/delete entry per consecutive generation, ending at the record generation |

Kind 4 prefixes the batch entries with `MarkerID [16]byte`,
`MarkerEpoch uint64`, and `TxnID uint64`; the record CRC covers that header and
every entry. Kind 5 carries only complete logical put/delete entries. An
authenticated live window is either the atomic family (kinds 1 through 4) or
the delta family (kind 5), never both. Atomic records form a one-generation
chain from the header base, with same-generation reuse allowed immediately
after a conditional that may abort. Delta records form one contiguous interval
from the header base. Any other kind, mixed family, invalid generation chain,
or nonzero header `Format` fails closed.

### Transaction decision log

One sidecar per database directory, reserved name `txn.vtm`, lives beside the
collection files in a `durable.Database` directory or the SQL driver's
`<catalog>.tables/` directory. It is not a decodable collection filename. The
primary `StateRoot` layout and `DevelopmentFormatVersion == 0` remain
unchanged; the decision log is a sibling container.

The container follows the recovery journal's discipline: two alternating,
independently checksummed 512-byte header sectors, a bounded preallocated
record region, positional sector-aligned appends, strict sequence validation,
and torn-tail truncation. Header fields include the numeric-zero `Format`
sentinel, `MarkerID [16]byte` minted at creation, `Epoch uint64`,
`BaseSequence uint64`, capacity, recycle count, flags, and checksum. Two record
kinds are each sealed by CRC32C plus complement and padded to the append sector:

- kind 1 `decision`: sequence (database commit sequence), `TxnID`, participant
  count, and per-participant `{StoreID, JournalID, PreparedGeneration}`;
- kind 2 `participant-retired`: sequence and `StoreID`, written after
  `DropCollection` checkpoints the collection past every conditional record.

A decision is the durable fact that transaction `TxnID` committed naming those
participants. Every retained kind-4 journal record is resolved, including one
whose generation the selected root appears to cover. A committed resolution
requires the decision marker identity and epoch to match and an exact
participant tuple `(StoreID, JournalID, PreparedGeneration)`, with the prepared
generation equal to the journal record generation. The log is minted lazily at
the head of the first multi-collection commit and fenced through parent-
directory fsync before any prepare may reference its `MarkerID`. A decision is
retained until every named participant has successfully completed its resolved
fold and journal recycle (or has a durable retirement). Offline pairing checks
live in `cmd/vibedb-verify`.

The current decision-log header assigns bytes `64:68` as its flags word. Bit 0
(`SealedCapacity`) gives its record-region `Capacity` the same immutable
physical-allocation meaning. Bits 1 through 31 are reserved zero; any set
reserved bit, or any nonzero byte in the reserved `68:504` suffix, rejects the
header before record replay. Its complete sealed file
length is likewise exactly `1024 + Capacity`, with the two header sectors
outside that capacity.

The decision log retains an independent 16 MiB record-region hard ceiling. It
does not inherit recovery-journal envelope overhead when that separate clamp
changes.

The fixed materialization journal slots inside the primary file carry complete
before-image sectors for qualified in-place canonical page updates. Recovery
rolls back an incomplete materialization before selecting a root. Readers never
consult either journal or the decision log.

### Sealed sidecar allocation

Sealing uses the current checksummed header flags above; it does not introduce
an alternate container layout. Creation requires an empty regular file and an
exact, sector-aligned capacity. It strictly allocates the complete absolute
prefix `[0, 1024 + Capacity)`, publishes and synchronizes the header, and then
requires the regular-file EOF to remain exactly that total. A sealed recovery
journal cannot use `GrowCapacity`; its capacity is immutable even across
recycle. A sealed decision log is recycled within the same immutable region.

A profile-qualified durable open must supply an exact sealed profile: the
recovery-journal option must equal the persisted record-region capacity, and
the decision-log options must request `SealedCapacity` with the same exact
capacity. A paired recovery-journal open checks the selected header's store
id, journal id, page size, and recovery epoch before allocation proof or record
scan. Generic mutable `OpenRecoveryJournal` can instead accept and reprove the
self-described persisted seal, but that capacity-immutable mutable handle is not
external-profile qualification. After selecting the authoritative header,
open rejects a short or long EOF before any allocation syscall or record scan.
It then reproves the complete prefix, synchronizes that proof, checks exact EOF
again, and only then scans records. It never repairs an EOF mismatch by
truncating or extending the sidecar.

The current replicated SQL catalog fixes three canonical record-region sizes:

| Owner | Sidecar record region | Complete file |
| --- | ---: | ---: |
| base binding | user recovery journal: `16,794,624` | `16,795,648` |
| base binding | transaction marker: `1,048,576` | `1,049,600` |
| apply activation | system recovery journal: `655,872` | `656,896` |

The complete file adds the two 512-byte headers (`1,024` bytes) to each record
region. Bind accepts only a sole schema-free/index-free unmaterialized user
table with no existing transaction marker. It creates a fresh sealed user
storage incarnation; it never converts a materialized collection or ordinary
sidecar in place. One catalog cut publishes the replacement table, the exact
sidecar profile, and the complete replicated identity while `txn.vtm` remains
absent. The sealed marker may be minted only after the catalog rename and its
parent-directory durability fence succeed.

The marker region holds 2,048 current 512-byte, two-participant decisions.

Replicated exact open and settlement validate the numeric-zero SQL catalog
grammar and all three persisted sidecar identities before namespace or
transaction recovery, then pass the exact sealed capacities to durable open.
An ordinary zero-option open is not a fallback for this profile.

Strict allocation is Linux-only. It first performs mode-zero
`fallocate(fd, 0, 0, total)` over the complete prefix, repairing holes and
establishing backing for every byte, then applies
`FALLOC_FL_UNSHARE_RANGE` over that same prefix to privatize copy-on-write
extents. An `EOPNOTSUPP` response to unshare is accepted only when `fstatfs` on
that descriptor proves ext4, which has no writable reflink support. Every
other filesystem or error fails closed; there is no truncate fallback for a
sealed file. Platforms without this proof cannot create or mutably open a
sealed sidecar.

`InspectRecoveryJournal` and `InspectTxnMarker` are explicitly read-only. They
validate the persisted header and exact apparent EOF before scanning, but do
not allocate, unshare, synchronize, repair, or return a mutable handle. An
inspection therefore does not qualify the file for mutable recovery or
serving.

The certificate assumes exclusive allocation ownership. Callers and unrelated
processes must not truncate, extend, hole-punch, reflink-clone, or otherwise
change either sealed sidecar outside its owning API. This is only a storage
foundation for the recovery journal and transaction decision log. It defines
only the catalog-named SQL sidecar allocation identities above. It reserves no
collection main-file, Raft log, snapshot, or range capacity, and does not
certify any node or range to serve traffic.

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
generation relationships, journal record families, all graph references,
catalog digests, exact-index postings, and allocator disjointness. Kind-5 delta
entries name one consecutive generation each, ending at the batch generation.
If bounded replay pressure physically checkpoints a prefix and recovery is
interrupted again, the next open derives and skips exactly the prefix already
covered by the selected root. Kinds 3 and 4 remain one-generation atomic
batches; retained conditionals are resolved against their exact decision
participant tuple before a resolved fold and recycle can consume them. Recovery
fails closed on any disagreement.

## Verification and golden images

`durable.Verify` performs offline graph and allocator checks without mutating
the input. `durable.Salvage` and `durable.Repack` write fresh current stores;
they do not copy old page layouts.

The storeio golden-image fixtures contain the canonical bytes for the current
schema. Their generator tests regenerate and compare them, and assert that
every stored format sentinel is zero. Malformed-input tests cover
checksum, reserved-byte, bound, identity, routing, and graph-consistency
failures.

See also [architecture.md](architecture.md), [durability.md](durability.md),
and [store.md](store.md).
