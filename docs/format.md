# On-disk format

The on-disk format is unreleased development format 0. The repository has no
compatibility decoder or migration framework.

> **CAUTION:** A format change replaces format 0 and its golden images in
> place. Do not expect a file from another commit to open. Pin the producer and
> reader to one tested revision.

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

The primary structure is an ordered page graph. Leaf extents can grow from one
base page to the configured maximum page size, which defaults to 64 KiB.

Small values can stay inline. Larger values use overflow storage. Exact indexes
store canonical scalar tuples and posting references. Equal ordered path sets
share one physical index even when they have different logical names.

Skip indexes store compact minimum and maximum summaries for each primary
stripe. A container, an oversized scalar, or another unprunable value disables
pruning only for that stripe and path. It does not change query correctness.

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

## Implementation references

- `internal/storeio/page.go`
- `internal/storeio/mutable_file_layout.go`
- `internal/storeio/inline_superblock.go`
- `internal/storeio/state_root.go`
- `internal/storeio/recovery_journal.go` and `txn_marker.go`
- `internal/collectionname/collectionname.go`
