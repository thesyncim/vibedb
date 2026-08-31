# Development on-disk format

> [!CAUTION]
> **Format 0 is replace-in-place development state, not a compatibility
> promise.** A later commit may reject every file described here. There is no
> cross-version decoder, downgrade path, or migration ladder. Same-format
> physical generation migration exists, but it does not make different builds
> compatible. Use the reader and docs from the exact writer commit, and keep an
> independent restorable copy of any data you care about.

The codecs and validators in `internal/storeio`, `store/durable`,
`internal/raftstore`, and the replicated-state packages are authoritative. This
page is a map for contributors and offline inspection, not a standalone parser
specification.

## Recovery unit

| Component | Purpose | Backup rule |
| --- | --- | --- |
| Collection primary | Immutable pages plus alternating current roots | Keep with its journal when present |
| `.rjournal` | Recovery/delta records and conditional prepares | Never copy or move independently of the primary |
| `txn.vtm` | Cross-collection decision log | Preserve with the complete database directory |
| Database catalog | Collection identities and persisted options | Preserve the closed directory as one unit |
| Raft WAL family | Authenticated consensus state and generation activation | Preserve exact identity, keys, manifest, and family |
| Gateway/control journals | Request, catalog, or operation recovery | Preserve with matching identities and secrets |

A live arbitrary file copy is not a supported backup. Stop and close the
writer, or use a protocol that certifies an immutable cut.

## Common page envelope

The common page uses a 64-byte header, payload/body, and 8-byte checksum
trailer. The physical allocation quantum is fixed at 4 KiB. A durable
collection's maximum page extent defaults to 64 KiB and is configurable up to
the storage codec's 64 MiB hard ceiling. For a 4 KiB base page, the mutable
prefix is:

| Offset | Region |
| ---: | --- |
| `0` | current root slot A |
| `4096` | current root slot B |
| `8192` | undo capsule A |
| `12288` | undo capsule B |
| `16384` | first immutable data extent |

Headers bind type, format, geometry, identity, generation, and payload length as
required by the concrete page codec. CRC32C checksums detect accidental
corruption; they are not authentication against an adversary. Validation also
enforces semantic bounds and graph relationships.
Writers zero padding, but readers do not treat every checksum-covered body
padding byte as a reserved-zero field.

## Collection graph

The durable collection is not an LSM/SST layout. Its current primary graph
contains:

- alternating roots and a state root;
- a lexical key catalog/router;
- the current class-6 `VCS1` compact primary representation;
- inline values and raw linked overflow pages;
- exact-index catalogs, term leaves, posting structures, and build state;
- free/retired extent state and snapshot/reclamation metadata.

Overflow values are not transparently compressed or deduplicated. Exact-indexed
open rebuilds a resident epoch by scanning persisted exact leaves/live rows; an
indexed open is not metadata-only and its total memory is data-dependent.

## Root publication order

The portable device commit is ordered:

```text
data writes → data barrier → alternate root write → final sync
```

The previous valid root remains the recovery point until the new root is
complete. A root-write or final-sync failure has an unknown outcome; close and
reopen before deciding which root is authoritative.

Topology preparation may publish an equivalent representation before a logical
batch is admitted. A higher generation therefore does not prove that user rows
changed.

## Recovery journal

A journal binds the exact store identity and durability lane. Records are
framed, bounded, checksummed, and replayed only when their identity and sequence
match the primary. A torn final tail is ignored logically during scan; recovery
does not promise to physically truncate the underlying file at that moment.

The synchronous lane appends and syncs the recovery record before applying and
publishing the visible row change. Buffered lanes establish different
acknowledgement and checkpoint boundaries; see [durability](durability.md).

## Cross-collection decisions

For two or more durable participants, each collection records a conditional
prepare. The database synchronizes all prepares, then writes and synchronizes
one commit decision in `txn.vtm`; absence of that durable decision means abort.
Reopen requires the complete catalog, decision log, and participants. A missing
or mismatched participant fails closed.

Checkpoint groups use a different certificate protocol for replicated apply.
Their recyclable `txn.vtm` is not a Raft WAL; an uncertified suffix depends on
the external Raft WAL for recovery.

## Raft WAL generation family

The Raft store uses preallocated 4 KiB-aligned records, AES-256-GCM,
checksums/integrity fields, and a digest chain. A family manifest and alternating
current slots select the active authenticated generation. Ready batches are
idempotent by `(node incarnation, Ready ID)` and different bytes for the same
identity are rejected.

Ordinary Raft snapshots are not stored as `MsgSnap` records. Certified snapshot
artifacts are staged non-serving, verified, checkpointed, and activated as a new
immutable WAL base through restart-settled publication steps.

## Replicated hidden state

Replicated-state rows use reserved hidden-key ranges for transaction control,
route gates, backup, request ledgers, and execution pins. Unknown hidden rows are
reopen-fatal so a newer grammar cannot be silently misread by an older build.

Distributed coordinator, request-ledger, range-split, schema, backup, restore,
and control journals each have closed current grammars and independent hard
bounds. Their exact bytes belong in the owning codec, not duplicated prose.

## Golden images

`internal/storeio/testdata/format0` contains byte-exact fixtures for the current
unreleased format. An intentional grammar change must:

1. replace the affected fixture and test oracle;
2. add malformed/truncated/corruption cases for the new shape;
3. retain rejection of obsolete or noncanonical layouts;
4. update build grammar identities and this map;
5. prove the new crash and reopen boundaries.

Golden fixtures are test evidence only. They are not files that future versions
promise to open.

## Source map

- `internal/storeio/page.go`, `state_root.go`, `inline_superblock.go`,
  `mutable_file_layout.go`, `recovery_journal.go`, and primary/index codecs
- `store/durable/store_file_*`, `store_database_*`, and `checkpoint_group.go`
- `internal/raftstore`
- `internal/replicatedstate`, `internal/distributedtxn`, and
  `internal/requestledger`
- `internal/storeio/testdata/format0`
