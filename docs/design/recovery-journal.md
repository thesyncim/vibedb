# Recovery-only redo journal

**Status:** implemented for the ordered primary graph.

## Purpose

The journal provides a bounded, append-and-sync acknowledgement path without
adding a representation readers must merge. Normal reads always follow the
published canonical root and pages. Only open-time recovery reads journal
records.

Each store owns a paired journal file. The store root and journal header carry
the same random identity, so files from different stores cannot be paired. The
journal has fixed capacity derived from options; admission fails rather than
growing without bound.

## File geometry and versions

The sibling file is named `<store>.rjournal`. It contains two alternating,
independently checksummed 512-byte header sectors followed by a fixed-capacity
record region. A header carries an independent journal format version, store
and journal identities, store page size, 512-byte append sector, base
generation, base sequence, capacity, and monotonic recycle count. Recovery
selects the valid header with the greatest recycle count. A torn recycle can
therefore fall back to the previous header and safely replay records it still
describes.

The entire file is sized before a store root is allowed to name its journal;
normal appends are positional writes and never extend it. Linux normally uses
`fallocate` to reserve the complete region, with a one-time truncate fallback
for filesystems that do not implement it. Other platforms set the full size
once with truncate. This avoids making acknowledgement latency depend on file
growth or recurring size-metadata updates.

The ordinary buffered-delta configuration uses journal format v1. Its current
compact policy caps, and with the shipped overlay geometry selects, a 2.5 MiB
record region, plus the two header sectors. The foreground fit guard preserves
up to 512 KiB for one estimated future carried suffix, yielding the qualified
2 MiB current append window. The reserve is a policy calculation, not a fixed
subregion: exact prepared-batch size decides whether the current append fits,
and a wider current or future interval takes the bounded physical checkpoint
fallback. Per-mutation acknowledgement journals use the separate
option-derived capacity policy.

Records start on the damage-granule boundary. A 32-byte prefix carries magic,
kind, reserved-zero bytes, sequence, generation, and key/value or batch-body
geometry. One CRC32C and its complement seal the complete logical record; zero
padding extends it to a whole sector. Strict sequence validation prevents stale
bytes left after recycle from becoming live records.

Two record grammars are accepted deliberately:

- v0 is the legacy grammar. Standalone and batch entries are put/delete redo;
  all entries of a batch share its one generation. A v0 journal remains v0 on
  reopen and current code continues to emit full-value entries into it;
- v1 extends a batch with scalar-patch entries for an ordinary buffered delta.
  Each patch stores its key, new canonical integer, boolean, or null spelling,
  the canonical byte offset, old spelling length, and the checksum expected for
  the complete resulting canonical document. Scalar patch is batch-only and is
  chosen only when its metadata plus scalar are smaller than the full value.

The header version gates decoding and writing. Unknown versions, an
authenticated scalar-patch entry in v0, malformed scalar metadata, generation
underflow, or a v1 journal opened under the per-mutation/synchronous lane fails
closed. The v1 header word reuses a field old binaries required to be zero, so
v0-only code rejects the file before it can mistake scalar-patch kind 4 for
legacy redo.

## Ordering

For `DurabilitySync`, a mutation is fully validated and encoded as redo before
the journal record is synchronized. Only then may the canonical generation
become visible and the call return. A logical supported batch uses one record
and checksum, preserving its all-or-nothing publication.

Buffered-visible mutations may accumulate in memory until `Flush`, `Close`, or
pressure selects a checkpoint cut. For a complete ordinary class-5 interval,
`Flush` writes one v1 batch containing exactly one ordered entry per consecutive
generation and synchronizes the journal once. Deletes remain logical deletes;
eligible existing-key replacements become scalar patches and every other put
carries its complete canonical value. Structural work, an interval gap, or a
capacity/reserve miss uses the physical root checkpoint instead.

A physical checkpoint writes and synchronizes its canonical data before
publishing the alternate durable root. Only after that root covers the records
being discarded does recycle write the opposite journal header and synchronize
it. A failed recycle leaves the old in-memory and on-disk base authoritative;
the records remain replayable rather than being retried from an ambiguous head.

The exact operating-system barrier depends on the configured durability mode
and platform. Ordinary filesystem synchronization and the platform's strongest
power-loss barrier remain distinct contracts.

## Recovery

Open validates the journal header, identity, bounds, sequence, record kind,
length, checksum, and generation relationship before replay. Replay goes
through the ordinary mutation semantics so primary rows, schemas, indexes, and
canonical encoding do not acquire a second implementation. A corrupt,
mispaired, out-of-order, or unsupported record fails closed.

Damage consistent with a not-yet-synchronized tail is narrower: invalid magic,
framing, sequence, or record CRC stops scanning at the last complete prefix.
Once a complete record CRC authenticates its bytes, an unknown kind or
semantically impossible payload is not treated as a tail; it returns a hard
journal-record error and Open fails closed.

Scalar replay reads the current canonical document, verifies the selected old
span and result bounds, performs the length-preserving or length-changing
splice, and compares CRC32C of the complete result with the checksum in the v1
entry before calling `Put`. A stale base, wrong span, damaged patch, or wrong
result is therefore rejected without publishing guessed content.

V1 batches use a different generation grammar from v0: `N` entries cover the
consecutive interval `record.Generation-N+1` through `record.Generation`. If
bounded staging pressure physically checkpoints a prefix while Open is
replaying and recovery is interrupted again, the journal is deliberately not
recycled. On the next Open, the selected root generation determines the exact
covered prefix and replay resumes at the first uncovered entry. This is
required for length-changing patches, which cannot safely be applied twice.
Legacy v0 batches remain one-generation atomic groups and always replay from
entry zero.

Recovery then selects and verifies the durable root that covers the replayed
cut. The final replay fold completes before the journal is recycled. A failed
fold or recycle leaves the records intact for the next Open. Readers are not
exposed until journal handling and root validation finish.

## Post-durability online reclamation

After a physical root is durable and recycle proves the journal base covers it,
the foreground completion path may deallocate blocks for generation-safe free
extents. A physical-generation guard permits one bounded pass per newly
authoritative root; journal-only Flushes do not invoke it. The pass inspects at
most 1,024 exact identities and 64 coalesced runs from reusable,
pending-retirement, and absorbed-retirement sources. Active-source shares may
be redistributed for at most three rounds. It spends at most six successful
platform calls and 20 MiB, retaining bounded progress for an oversized exact
extent.

The planner samples the durable/fallback generations and journal base under the
snapshot gate and copies its fixed candidate window while direct readers are
diverted. It releases that reader fence and the snapshot gate before validation
and before every Linux `fallocate(PUNCH_HOLE|KEEP_SIZE)` or Darwin
`F_PUNCHHOLE` call; the writer lock prevents reuse in the gap. The exact sampled
generation, not a later atomic observation, advances the guard after successful
planning.

Unsupported platforms/filesystems and real syscall errors are optional
outcomes: the first occurrence is counted, the candidate is skipped, and
further attempts are disabled for that open collection without poisoning the
writer or changing the durability result. `EINTR` alone receives up to four
bounded attempts. Logical reuse remains available even where physical hole
punching is unavailable. Consequently online space reclamation requires no
background compactor and no offline maintenance pass.

## Required gates

- crash injection before and after every append, synchronization, publication,
  checkpoint, and recycle boundary;
- byte-identical logical state after replay and uninterrupted execution;
- exact-index equivalence after replay;
- bounded full-journal behavior and retry-safe failure reporting;
- v0 append/replay compatibility and fail-closed v1 grammar gates;
- complete-result checksum rejection for stale or damaged scalar patches;
- interrupted v1 replay that checkpoints a prefix and resumes its exact suffix;
- fixed-window hole-punch bounds, generation authority, fence ordering, and
  optional unsupported/error behavior;
- no journal lookup or allocation on a warmed read path;
- separate benchmark publication for ordinary-sync and power-safe lanes.
