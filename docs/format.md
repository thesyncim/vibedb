# Development on-disk format

[Documentation](README.md) · [Stability](status.md)

Format 0 is the current development disk grammar; reopen files only with the
build that wrote them. Codecs in `internal/storeio`, `store/durable`, and
`internal/raftstore` are authoritative.

## Recovery unit

| Component | Purpose | Backup rule |
| --- | --- | --- |
| Collection primary (`.vjc`) | Immutable pages + alternating roots | Keep with `.rjournal` |
| `.rjournal` | Redo/delta records & conditional prepares | Never separate from primary |
| `.tin` sidecar | SHA-256-checksummed sealed full-text postings | Optional cache; rebuilt on mismatch |
| `txn.vtm` | Cross-collection commit decision log | Keep with complete DB directory |
| Shared node log | Descriptor catalog, segmented Raft log, checkpoints | Preserve whole node dir + keys |
| Raft WAL family | AES-256-GCM authenticated consensus state | Preserve identity, keys, manifest |

Never copy live files; close writers or use a certified cut protocol. Physical
nodes use [NodeStore](../internal/raftstore/node_store.go) and
[seglog](../internal/raftstore/seglog/) (separate from the `.vjc` page layout).

## Page envelope and collection graph

Pages use a 64-byte header, body, and 8-byte CRC32C trailer (accidental
corruption detection, not adversarial authentication). Base allocation quantum
is 4 KiB; max extent defaults to 64 KiB (hard codec cap 64 MiB). Mutable
prefix:

| Offset | Region |
| ---: | --- |
| `0` / `4096` | Alternating root slots A / B |
| `8192` / `12288` | Undo capsules A / B |
| `16384+` | Immutable data extents |

The durable collection is copy-on-write (not LSM): alternating roots, state
root, lexical key router, class-6 `VCS1` compact primary leaves (supporting
arithmetic scalar sequences for ≥64 values), inline + raw linked overflow pages
(uncompressed), exact-index catalogs/term leaves/postings, and free/retired
extent metadata.

Root commit order: `data writes → data barrier → alternate root write → final sync`.
A root-write or final-sync failure is outcome-unknown; close and reopen.

## Recovery journal

Journals bind store identity and durability lane with framed, checksummed
records (torn tails are logically ignored on scan). Header flags (offset 88 in
`.rjournal`, offset 64 in `txn.vtm`) encode allocation mode:

| Flags | Capacity contract |
| ---: | --- |
| `0` | Ordinary unsealed journal |
| `1` | Sealed capacity with strict private physical reservation |
| `3` | Sealed capacity with portable allocation (RF3 default) |

Bit 1 requires bit 0; build grammar digests include these flags, so mismatched
builds fail closed.

## Raft WAL and replicated hidden state

- **Raft WAL:** Preallocated 4 KiB-aligned AES-256-GCM records with digest
  chaining and alternating current slots; Ready batches are idempotent by
  `(node incarnation, Ready ID)`. Snapshots move as certified artifacts
  activated as a new WAL base, never Raft `MsgSnap`.
- **Hidden state:** Reserved key ranges store transaction control, route gates,
  request ledgers, backup, and execution pins; unknown hidden rows fail reopen.
- **Golden fixtures:** `internal/storeio/testdata/format0` holds byte-exact
  current-grammar fixtures (test oracles only, not backward-compatibility
  guarantees).

## Source map

- Page & journal codecs: [internal/storeio/page.go](../internal/storeio/page.go), [recovery_journal.go](../internal/storeio/recovery_journal.go), [testdata/format0](../internal/storeio/testdata/format0)
- Durable & Raft storage: [store/durable/](../store/durable/), [internal/raftstore/](../internal/raftstore/), [internal/replicatedstate/](../internal/replicatedstate/)
