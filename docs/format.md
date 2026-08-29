# On-disk format

The on-disk format is unreleased development format 0. The repository has no
compatibility decoder or migration framework.

> **CAUTION:** A format change replaces format 0 and its golden images in
> place. Do not expect a file from another commit to open. Pin the producer and
> reader to one tested revision.

For the exact same-build restart boundary and the distinction between binary
compatibility and schema-generation rollout, see
[Unreleased compatibility and rolling restarts](operations/unreleased-compatibility.md).

## File layout

The base page is exactly 4096 bytes. The fixed mutable prefix is:

| Offset | Size | Record |
| ---: | ---: | --- |
| 0 | 4096 | Inline root A |
| 4096 | 4096 | Inline root B |
| 8192 | 4096 | Materialization capsule A |
| 12,288 | 4096 | Materialization capsule B |
| 16,384 | - | Start of ordinary allocation |

The two roots provide alternate physical-checkpoint publication. A rooted
commit or checkpoint writes immutable pages and publishes one of them. Some
buffered and journal-backed synchronous mutations publish a bounded logical
generation before the next physical checkpoint. Recovery combines the
selected root with valid recovery-journal records. The two capsules support
bounded rollback for eligible canonical page replacement.

## Inline root

An inline root is one 4096-byte record with magic `SJINL000`. It contains a
complete 512-byte `StateRoot` and a bounded inline free-set delta.

The record ends with CRC32C and its complement. Decode validates all required
zero padding.

## Common immutable page

An immutable page has:

- A 64-byte header
- Magic `SJPAGE00`
- Format version 0
- Store ID
- Generation
- Stable logical ID
- Page size and payload length
- Page kind
- Payload and validated padding
- An 8-byte CRC32C-plus-complement trailer

A referenced unchanged page can have an older generation than the selected
root. Page identity uses the stable logical ID and the complete reference.

## State root

`StateRoot` records the authoritative top-level state. Its fields include:

- Store ID and generation
- Page and maximum-page sizes
- Document count and next logical ID
- Persisted key, inline-value, and document limits
- Ordered primary root
- Exact-index root and index-catalog identity
- Canonical page-catalog reference, digest, and size
- Materialization damage granule and option flags
- Recovery-journal ID
- Physical-capacity metadata

Open treats nonzero persisted admission fields as format contracts. A nonzero
caller option must match them exactly. Runtime tuning fields remain caller
controlled.

The inline-root envelope also carries the cumulative free-set delta and its
external free-log predecessor.

## Page references

A `PageRef` contains file offset, logical ID, generation, length, and page kind.
Recovery checks that the reference is in bounds and that the page matches all
identity fields.

## Primary data and indexes

### Ordered primary graph

The primary structure is one ordered page graph. Its global tablet catalog has
an exact lexical root of 64 KiB and 8 KiB catalog nodes. The normal catalog is
two levels. A third level admits adversarial long separators. A catalog leaf
names an independently cacheable tablet route.

One tablet has:

- one 8 KiB `PageTabletRoute` containing a 4 KiB segmented-router root
- one 8 KiB `PagePrimaryLocator` mapping 4096 stable local IDs
- one to 16 independently replaceable 8 KiB `PagePrimaryAnchor` pages
- up to 4096 primary leaves

An anchor page holds at most 256 lexical fences, but row count is not its only
packing bound. The encoder front-compresses fences and starts another anchor
when the encoded fence arena is full. Incompressible fences can therefore end
an anchor before row 256. Stable local ID is independent of lexical rank, and
the encoded locator's exact `(page ID, row slot)` is authoritative. It must not
be reconstructed as `rank / 256, rank % 256`.

### Compact primary stripes (`VCS1`)

Every production primary leaf is a common `PagePrimaryLeaf` whose payload
starts with `VCS1`. The internal `CommonPrimaryLeafCompact` discriminator is 6,
and `VCS1` is the sole durable primary-leaf grammar. The older unified class-5
encoder remains only as an internal codec-test helper. Production open,
mutation, and verification do not accept it as an alternate format.

A stripe is rounded to a 4 KiB physical extent and can grow to 64 KiB. An
unindexed stripe admits at most 4096 rows. A collection with exact indexes uses
at most 256 rows per stripe so each posting retains one stable byte-sized slot.
Within a stripe:

- keys form one compact scalar stream in strict lexical row order
- rows carry bit-packed shape IDs
- rows with the same JSON shape share one static template
- each scalar hole in that template is an independent compact stream
- 64-row restart and shape-rank checkpoints bound point decoding
- optional posting slots preserve exact-index identity
- overflow rows carry a bitmap plus the complete 32-byte first-page `PageRef`,
  rather than participating in the inline scalar streams.

Every scalar stream is self-delimiting and selects one reversible encoding:

| Encoding | Current representation |
| --- | --- |
| Dictionary | Sorted distinct spellings plus bit-packed IDs |
| Front | Prefix/suffix tuples with a full restart every 64 values |
| Frame of reference | Minimum signed integer plus bit-packed offsets |
| Delta | Full signed-integer restart plus zigzag-varint deltas |
| Packed delta | Per-64-value base and bit-packed zigzag deltas |
| Date | Exact JSON `"YYYY-MM-DD"` strings as bit-packed day ordinals |
| Prefix integer | Shared prefix/suffix plus linear, varint, or packed decimal integers |
| Alphabet | Up to 64 middle bytes, shared affixes, and bit-packed symbols and lengths |

The planner measures every applicable representation. It normally selects the
byte minimum, but deliberately retains a dictionary with at most 128 entries
when it is within 25 percent of that minimum because packed-ID scans are
cheaper. This is a deterministic representation choice, not an adaptive index.
Every codec reconstructs its exact input bytes. For document holes, those bytes
are the canonical scalar spelling. None of the codecs are lossy.

Skip indexes append at most eight field summaries to each stripe. Each valid
summary stores canonical ordered minimum and maximum terms of at most 256 bytes,
and the complete summary tail is bounded at 4 KiB. A container, an oversized
scalar, or another unprunable value disables only that stripe/path summary. It
does not change query correctness.

### Inline and overflow values

`InlineValueBytes` is a persisted admission contract, 512 bytes by default.
An admitted canonical value above that ceiling and no larger than
`MaxDocumentBytes` (4 MiB by default) uses a linked `PageOverflow` chain. Each
checksummed page records the complete value length, this piece's byte offset
and length, and the next complete `PageRef`. The final page has a zero next
reference. Pieces are raw value bytes. The current overflow format does not
apply the `VCS1` field codecs or cross-value deduplication, and reads reassemble
the chain.

Exact indexes store canonical scalar tuples and posting references. Equal
ordered path sets share one physical index even when they have different
logical names. One canonical compound tuple is bounded at 4096 bytes.

## Recovery journal

The primary root binds store ID and journal ID. The journal has its own
checksummed headers and record region.

Synchronous mutations append and sync redo before visibility. Conditional
records also carry transaction-marker identity, epoch, transaction ID, and
participant data.

Open refuses a missing or mismatched required journal. A standalone open also
refuses unresolved conditional records.

## Transaction decision log

A database creates `txn.vtm` lazily for its first multi-collection transaction.
The log records one durable decision for a bounded participant set.

Recovery combines the decision with participant journal records. When a valid
decision log has no matching decision, recovery presumes abort. A committed
decision rolls all participants forward. Missing `txn.vtm` or a missing
required participant fails closed when conditional records remain.

## Distributed SQL participant apply state

`distributed-transaction-state.vjc` is a private synchronous collection keyed
by the exact 16-byte distributed transaction ID. Its values are raw opaque
bytes, not JSON documents. The collection fixes both `InlineValueBytes` and
`MaxDocumentBytes` at 27, admits one mutation per batch, and fixes
`MaxBatchBytes` at 43: one 16-byte key plus one maximum 27-byte value.

The apply value belongs to the single unreleased format-0 image and has this
canonical grammar:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `VDPA` |
| 4 | 1 | Codec sentinel `0` |
| 5 | 1 | Fixed `ParticipantApplied` state (`3`) |
| 6 | 2 | Required zero reserved bytes |
| 8 | 1-10 | Canonical unsigned-varint revision, greater than zero |
| next | 1-9 | Canonical unsigned-varint affected-row count, at most `MaxInt64` |

The record is exactly 10 to 27 bytes and has no trailing padding. Decode
requires exact exhaustion and rejects overlong, overflowing, or truncated
varints. The codec sentinel selects the sole current grammar; it is not a
released version or a compatibility dispatch point. Earlier development
decimal-JSON values fail closed. Format 0 replaces them in place and has no
alternate decoder or migration ladder.

## Distributed transaction coordinator manifests

The compact `VTC1` coordinator record remains the byte-identical fast path for
at most 64 participants and 32 KiB. Those are inline-layout bounds, not a
distributed transaction participant limit. A wider ordered participant set is
encoded as canonical `VTM1` pages and bound by one fixed `VTCM` coordinator.

One `VTM1` page is at most 64 KiB:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | Magic `VTM1` |
| 4 | 1 | Current unreleased format sentinel |
| 5 | 3 | Required zero bytes |
| 8 | 4 | Little-endian page index |
| 12 | 4 | Little-endian participant count |
| 16 | 8 | Little-endian first aggregate participant ordinal |
| 24 | 4 | Little-endian entry-payload bytes |
| 28 | 4 | Required zero bytes |
| 32 | variable | Canonical participant entries |
| final | 4 | CRC32C of every preceding page byte |

Each entry starts with 80 fixed bytes: distribution-prefix length,
distribution-suffix length, shard-prefix length, shard-suffix length,
participant state, three required zero bytes, routing version, allocation
generation, ownership epoch, the 32-byte mutation digest, and the 16-byte
authority witness. The two identity suffixes follow. Prefixes refer only to the
preceding entry in the same page; the first entry uses zero prefixes. Entries
and pages are strictly ordered and deduplicated.

`VTCM` is exactly 116 bytes. It stores magic and sentinel, staging state,
revision, catalog generation, recovery deadline, 16-byte transaction ID, and a
56-byte manifest descriptor followed by required zero bytes and CRC32C. The
descriptor contains aggregate participant count, aggregate encoded bytes, page
count, and a SHA-256 root over the ordered page-digest chain. Aggregate encoded
manifest bytes are capped at 64 MiB. Coordinator begin carries `VTCM` and page
zero together so the addressed shard validates the canonical coordinator
identity before either journal append. Commit is refused until the durable page
sequence exactly reconstructs the descriptor and root.

## Replicated checkpoint-group certificate

A replay-backed replicated tablet owns one fixed collection set and its
transaction log through `checkpoint.vgc`. The file is exactly 8192 bytes: two
alternate 4096-byte certificate slots. It has one unreleased grammar, format
0, with no compatibility decoder.

Each slot has an eight-byte `VIBECPG\0` identity, a 168-byte header, one to 60
64-byte fixed-member records, required zero reserved bytes and padding,
and a 32-byte SHA-256 checksum. Its canonical fields are:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 8 | Identity `VIBECPG\0` |
| 8 | 2 | Format sentinel `0` |
| 10 | 2 | Header bytes (`168`) |
| 12 | 2 | Fixed-member count (`1..60`) |
| 14 | 1 | Maximum logical apply span, with encoded zero meaning effective span one |
| 15 | 1 | Required zero reserved byte |
| 16 | 8 | Certificate sequence |
| 24 | 8 | Raft applied index |
| 32 | 8 | Transaction high-water |
| 40 | 8 | Transaction base |
| 48 | 8 | Marker epoch |
| 56 | 16 | Marker ID |
| 72 | 8 | Seed applied index, or zero for an ordinary group |
| 80 | 32 | Seed state-envelope commitment, or zero |
| 112 | 32 | Seed-member logical-name digest, or zero |
| 144 | 24 | Truncated membership digest |

The member bank begins at byte 168. At the maximum 60 members it ends at byte
4008. The final authenticated tail before the checksum is:

| Offset | Size | Field |
| ---: | ---: | --- |
| 4008 | 8 | Retention applied floor, or zero |
| 4016 | 24 | Retention commitment, or zero |
| 4040 | 8 | Maximum-span witness transaction |
| 4048 | 8 | Maximum-span witness first applied index |
| 4056 | 8 | Maximum-span witness last applied index |
| 4064 | 32 | Slot checksum |

A zero retention floor requires a zero commitment. A nonzero floor must not
exceed the certificate applied cut and its commitment must equal the first 24
bytes of SHA-256 over
`vibedb/checkpoint-group/retention-seal/format-0\0`, the floor, marker ID, and
the fixed seed and member-lineage digest. The commitment excludes mutable
transaction, epoch, and maximum-span fields so every later certificate can
revalidate and carry the exact seal unchanged.

Encoded maximum span zero is the sole canonical representation of effective
span one and requires all three witness fields to be zero. Encoded one is
noncanonical. Values 2 through 128 require an exact nonzero witness tuple. Its
inclusive first and last range has exactly the encoded length, its transaction
does not exceed the transaction high-water, and its last index does not exceed
the applied cut. Larger values are corruption.

Across adjacent authenticated slots, an unchanged maximum span requires the
witness tuple to remain identical. A widened maximum requires the witness
transaction to be newer than the previous transaction high-water and no newer
than the selected high-water. Its first index must be newer than the previous
applied cut and its last index must not exceed the selected applied cut. These
rules prove that a newly advertised maximum came from one exact consecutive
transaction rather than from an accumulation of singleton transactions.

Every adjacent ordinary transaction certificate and marker rollover must
retain the exact retention tuple. A same-cut, same-marker-epoch certificate may
install or advance a retention floor only to its exact current applied cut with
the canonical commitment above. The next same-cut certificate may mirror that
exact nonzero tuple into the other slot. A zero-seal duplicate, changed or
regressed seal, same-cut maximum-span change, or combined seal and span change
is corruption. This successor grammar is shared by reopen and live two-slot
qualification.

Each member record binds the SHA-256 logical-name digest, store ID, and
recovery-journal ID. The checksum is SHA-256 over the domain
`vibedb/checkpoint-group/format-0\0` followed by every preceding byte in the
slot.

The selected slot must be in `sequence % 2`; when both slots authenticate,
their sequences must be consecutive. Decode re-encodes the complete slot and
requires byte-for-byte canonical equality. A checksum-invalid newest slot may
be a torn write and falls back to the other authenticated slot. A
checksum-valid but noncanonical slot, wrong parity, or nonconsecutive history
is corruption and never grants rollback authority.

Every monotonic domain is independent and never wraps. Certificate sequence
zero is noncanonical. At certificate sequence `2^64-1`, any checkpoint, marker
rollover, seal, or recovery transition that requires a successor fails with
`ErrCheckpointGroupSequence` before journal or marker Sync, certificate-slot
writes, or owner mutation. An aligned mutation-terminal owner is different.
After recovery has folded the authenticated member prefix and discarded every
later suffix, it attaches the selected marker and certificate read-only without
rewriting either one. Repeated opens therefore consume no successor. A new
retention seal reserves both slot generations, plus any required checkpoint
generation, before doing work. Mutation admission likewise retains its future
certificate generation and reserves any worst-case pressure checkpoint and
marker-rollover generations before invoking the update callback.

The transaction marker has separate DCSN, epoch, and header recycle-count
domains. DCSN successor zero is an exhausted append endpoint. Epoch or recycle
count `2^64-1` remains a valid readable header while no rollover is required.
A required rollover refuses before callback, replay, marker write, or Sync.
The selected same-epoch marker has `Header.BaseSequence` equal to the
certificate transaction base. The sole accepted transitional marker is one
empty epoch ahead and has `Header.BaseSequence` equal to the certificate
transaction high-water. Normal recovery reanchors the empty successor marker
at that authenticated high-water before publishing its same-cut certificate,
so scanned aborted decisions cannot launder their DCSN into owner lineage. If
the old and authenticated bases are equal while an aborted suffix remains,
recovery first zeroes and Syncs the aligned first-record sector. Only then does
it publish and Sync the successor header. Either crash side therefore scans an
empty suffix.

Participant recovery journals have their own DCSN and header recycle count. A
legal final DCSN `2^64-1` may still be certified and folded, leaving an empty
`BaseSequence=2^64-1` journal whose next DCSN is the zero sentinel. Only a later
append is refused. Recycling an already-empty journal to its existing base is
an exact no-op and consumes neither a header generation nor a barrier. Any
header-changing recycle or capacity growth reserves a nonzero recycle-count
successor before changing file size, root state, or a header slot.

Collection generations occupy the canonical 48-bit range `1..2^48-1`.
Admission reserves the bounded topology-stage and journal-recycle budget of the
largest permitted participant batch before callback execution. Recovery sums
the atomic-stage and sequential-fallback generation budgets of every committed
conditional record before replaying the first member. Exhaustion in any of
these domains is reported as `ErrCheckpointGroupSequence`. It is never
laundered into sequence zero, a torn-slot ambiguity, or a partially folded
group.

The certificate, rather than `txn.vtm`, is commit authority for this lane. It
commits the consecutive conditional-journal prefix through its transaction
high-water and aborts every later prepared suffix. Generic collection and
database openers refuse a directory carrying `checkpoint.vgc`; recovery must
open the certificate and exact fixed membership together before consulting or
folding transaction state.

A no-copy imported child starts from an authenticated cut-zero seed
certificate. The seed commitment hashes the exact replicated-state envelope
under `vibedb/checkpoint-group/seed-state-envelope/format-0\0`; it therefore
binds the imported applied cut and complete state contract, not merely a
last-entry digest. The seed member must be empty, while the already durable
user-image member may be nonempty. Transaction 1 writes that single state row
and is certified and folded before activation succeeds. A later same-applied
transaction binds the immutable snapshot base; after that transition, the
group transaction cut rather than the original envelope bytes is reopen
authority.

Only the explicit sealed-child resume opener accepts the pre-certificate
profile: exact fixed membership, empty seed member, zero or more already
durable imported rows in the other members, and clean marker and journals.
Allowing zero rows preserves empty split children; the sealed child cursor and
cutover certificate remain the image authority. The SQL root stays non-serving
until that exact stage is reclaimed and its snapshot base is installed.
Ordinary open treats the same missing certificate as corruption, so deleting a
live certificate never becomes rollback authority.

An ordinary checkpoint orders one Sync for each of its `K` participant
journals followed by one certificate Sync. The normal `K+1` barrier does not
Sync the recyclable marker. Once the certificate Sync succeeds, its applied
index is an authenticated input to Raft-WAL retention even if subsequent
physical collection folds must be retried. It is not standalone deletion
authority: term, configuration state, member lineage, certificate witness, and
the required retained suffix must also be bound. No ordinary replicated
transition performs a local Sync.

## Raft-WAL generation family and activation

Every Raft WAL belongs to one mandatory authenticated generation family from
its first creation. There is one format and one recovery contract. The 128-bit
family ID hashes the logical WAL leaf and complete sealed member-placement
identity. Its deterministic 4 KiB
`.vibedb-raft-<32 lowercase family hex>.family` manifest contains two
authenticated 512-byte slots; all remaining bytes are zero. Creation writes the
same source state to both slots. A missing or invalid peer is therefore damage,
not an older-format fallback, and quarantines serving until repaired by an
explicit operator path.

Family slots form one semantic state machine:

```text
source -> selecting generation 1 -> active generation 1
       -> selecting generation 2 -> active generation 2 -> ...
```

The source state has slot generation 1. Selecting states use consecutive even
slot generations and active states use the following odd slot generations.
Each transition authenticates the family and member identity. Source-to-first
selection binds the original file; selecting-to-active may change only phase
and slot generation; every later selection increments the WAL generation by
one and binds its parent generation digest and active source file. Two valid
slots that do not describe one adjacent legal transition are corruption.

WAL creation, family-manifest publication, and compacted-generation construction
use deterministic `.create.stage`, `.family.stage`, and
`.g<16 lowercase generation hex>.wal.stage` names. Publication uses hard-link
witnesses plus parent-directory barriers. A source Open may retain one verified
same-inode `.create.stage` witness without a directory Sync because that inode
is still authoritative; the first source-to-selecting transition must
unconditionally settle and remove it before the source can lose its logical
link. Selecting and active recovery reject any such alias. Family and candidate
stage retries settle outcome-unknown removals before accepting absence. One
persistent zero-length `.create.lock`, derived only from the logical WAL leaf,
serializes every initial creation attempt independently of caller-supplied
identity. A separate persistent per-family `.build.lock` serializes compacted
generation construction. Neither coordination inode is replaced. Under the
family owner lock, an abandoned same-lineage stage or
unselected candidate is proved by inode and authenticated contents, removed,
and followed by a directory barrier. A foreign or corrupt occupant is never
overwritten. These rules bound the namespace to one source, one candidate or
construction image, one family manifest, and fixed coordination files rather
than accumulating randomized preallocated WALs after crashes.

A healthy serving generation can capture an authenticated current-slot cut and
construct its next deterministic sibling. The builder does not retain a source
descriptor: it transiently opens and proves the named source while replaying,
then closes it; the owning Store's lifetime writer and family leases exclude a
competing source owner. Source replay authenticates and canonically decodes
every record through the captured cut but writes only the evolving suffix above
the certified checkpoint base. Historical HardState and full-log last index are
tracked separately from that projection. Sequential entries coalesce in bounded
memory, conflicting uncommitted suffixes truncate scratch in place, and the
checkpointed prefix is never copied to target scratch.

Retained-entry records have a fixed 24-byte header and one to the ordinary
Ready-entry limit of exact 32-byte entry headers plus raw data. They carry no
Ready ID because Ready IDs identify a process lifetime, not a durable entry.
Records are bounded by the sealed record and Ready-entry limits with a 4 MiB
preferred plaintext chunk; one individually legal larger entry remains one
bounded record. Peak heap is one fixed 64 KiB scan buffer, one source-record
ciphertext/plaintext pair, one retained chunk and encoding, one encrypted
target record, and `O(MaxReadyEntries)` scalar descriptors. There is no
source-sized entry arena or second scratch file.

One fixed 512-byte generation seal terminates the candidate. Its authenticated
record chain binds:

- family, generation, parent-generation binding, and complete member identity;
- source file and static-header identity, selected current generation, WAL end,
  record count, record-chain digest, node incarnation, exact source Ready ID,
  and topology epoch;
- snapshot-base index and term, the WAL bootstrap-record digest, the distinct
  state-machine snapshot-certificate identity, and stable ConfState digest;
- the exact checkpoint-retention witness commitment and HardState;
- retained first/last index, count, logical bytes, and semantic suffix digest;
  and
- source first/last index and the resulting generation binding digest.

Decode requires canonical record order: bootstrap, retained-entry records,
seal, then Ready records from a newer node incarnation or the same incarnation
with a Ready ID strictly above the authenticated source cursor. A missing,
repeated, or late seal, regressed incarnation or Ready cursor, broken parent binding, or any
mismatch with the authenticated current cut is corruption. Exact candidate
rebuild is idempotent only while the seal remains terminal; an independently
advanced candidate is not the same build.

A candidate never grants serving or deletion authority. Selection first locks
and reopens the exact candidate, proves the live source namespace, and publishes
the selecting family slot while holding both authorities. That immediately
fences the source WAL and its bound SQL apply claim. Recovery opens the selected
candidate only through the logical family manifest and exposes just its
activation evidence.

The live driver adopts that exact selected candidate into the retained Store
handle before completing activation. It preserves the owner incarnation and
Ready sequence, allowing compaction without restarting the Raft node. The
sealed cursor authenticates this handoff; it is not permission to replay an
older Ready batch under the new generation.

Activation validates the exact SQL preparation, schema/shard binding, applied
cut, and retention witness. It installs the certified snapshot base and
checkpoints the complete SQL group before replacing the logical WAL leaf. The
replacement then receives an unconditional parent-directory barrier and a
namespace proof before the family active slot is written, followed by one final
logical-name proof after active authority is durable. Only then does raftstore
mint an opaque completion capability for the exact family, generation, and
binding digest; zero, stale, foreign, or replayed values cannot release the SQL
fence. A failed final proof keeps the same handle fenced and retries only that
proof and completion; earlier failures remain selecting and retry the ordered
settlement. Once active, the retired full WAL has no namespace link and repeated
compaction continues through the authenticated parent-binding chain.

Building temporarily requires the authoritative source and one complete
preallocated target WAL simultaneously. Admission must reserve twice the sealed
`MaxFileBytes`, plus fixed metadata and operational filesystem headroom. The
activation protocol releases that temporary physical amplification without
weakening immutable-generation recovery.

## Database directory names

The durable database encodes a logical collection name as:

```text
c-<lowercase hexadecimal UTF-8 bytes>.vjc
```

The journal adds `.rjournal`. For example:

```text
orders -> c-6f7264657273.vjc
orders journal -> c-6f7264657273.vjc.rjournal
```

The logical name limit is 120 UTF-8 bytes. Unicode normalization is not part of
the identity. Byte-distinct names remain distinct.

## Recovery validation

Open checks:

- Root checksum and monotonic generation
- Store and journal identity
- Extent bounds and alignment
- Page kind, logical ID, and generation
- Page checksum and zero padding
- Canonical catalog digest
- Persisted option and schema contracts
- Transaction and conditional-record pairing

Conflicting roots, invalid references, corruption, and unknown required
records fail closed.

## Golden images

Byte-exact format 0 fixtures live in `internal/storeio/testdata/format0`. Tests
also construct checksum-valid semantic corruption and truncation cases.

When a format change is intentional:

1. Change the codec and validation together.
2. Replace the format 0 golden images.
3. Add corruption and truncation tests for the new fields.
4. Make obsolete development images fail closed.
5. Update this page in the same change.

Do not add a compatibility reader unless the project adopts a released format
and an explicit migration policy.

The replicated session header and slot grammar follows the same format-0 rule.
Lease-enabled headers replace reserved bytes in place with an absolute deadline
and mandatory lease marker. The single current codec sentinel remains `1`;
pre-lease development images fail closed rather than opening with an invented
deadline.

## Range-split source capture records

`internal/rangesplit/source_capture.go` stores source-capture values only in a
private opaque collection. Header and transition-entry rows share one 16-byte
raw binary envelope: an eight-byte identity, numeric format sentinel `0`,
record kind, required zero reserved bytes, and exact little-endian total
length. The header binds the split plan, placement program, collection, initial
publication, and its semantic digest. Each entry carries fixed publication
metadata and digests followed by strictly ordered transition frames.

Every transition has a 56-byte header: before/after presence bits, required
zero reserved bytes, little-endian key/before-document/after-document lengths,
an eight-byte before keyspace point, and a 32-byte before-document digest. The
payload then stores the exact key and after-document bytes. It does not store
the raw before document; its length, point, and digest are the retained before
witness.

A header is exactly `264 + collection bytes`. An entry is exactly
`248 + 56*transition count + key bytes + after bytes`; raw payload growth
therefore has no base64 expansion, and before-document size does not increase
the physical record.

The decoder accepts only this current grammar, requires exact frame exhaustion,
and borrows capacity-clamped key and document slices from the record. It parses
only present after values as JSON; the before side is the authenticated witness
described above. The binary envelope, collection, and keys are opaque bytes. A
stale development JSON/base64 capture fails closed.

## Implementation references

- `internal/storeio/page.go`
- `internal/storeio/mutable_file_layout.go`
- `internal/storeio/inline_superblock.go`
- `internal/storeio/state_root.go`
- `internal/storeio/compact_primary_stripe.go` and `compact_stream_codec.go`
- `internal/storeio/compact_primary_summary.go` and `overflow_page.go`
- `internal/storeio/segmented_tablet_router.go` and
  `segmented_tablet_router_geometry.go`
- `internal/storeio/global_tablet_catalog.go`
- `internal/storeio/recovery_journal.go` and `txn_marker.go`
- `internal/collectionname/collectionname.go`
