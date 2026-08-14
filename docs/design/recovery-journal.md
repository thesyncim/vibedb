# Recovery-only redo journal

**Status:** implemented for the ordered primary graph.

## Purpose

The journal provides a bounded, append-and-sync acknowledgement path without
itself becoming a reader representation. Only open-time recovery reads journal
records. Normal reads follow the published immutable generation cut, which can
be a canonical page graph plus the separate bounded in-memory primary overlay;
they never inspect or replay journal bytes.

Each store owns a paired journal file. The store root and journal header carry
the same random identity, so files from different stores cannot be paired. The
journal has bounded capacity derived from options and the hard ceiling;
admission, explicit growth, checkpoint, and recycle never make it unbounded.

## File geometry and record grammar

The sibling file is named `<store>.rjournal`. It contains two alternating,
independently checksummed 512-byte header sectors followed by a bounded record
region. A header carries store and journal identities, store page size,
512-byte append sector, base generation, base sequence, capacity, monotonic
recycle count, and sealed-capacity flag. Its `Format` field is a corruption
sentinel and must contain numeric `0`; every other value is rejected. Recovery
selects the semantically valid header with the greatest recycle count. An
uninitialized or checksum-invalid torn alternate slot can therefore fall back
to the previous header and safely replay records it still describes. A
checksum-authenticated slot with an invalid domain, field, flag, or reserved
byte is hard corruption and never permits fallback to an older capacity/base.

The initial file is sized before a store root is allowed to name its journal;
normal appends are positional writes within the authenticated capacity and
never extend it. An ordinary unsealed acknowledgement journal may explicitly
grow, within the hard ceiling and before the oversized record's point of no
return: it preallocates the extension before publishing the larger capacity in
the alternate header. Ordinary Linux creation and growth normally use
`fallocate`, with truncate only when allocation is unsupported; other
platforms set the requested size with truncate. A sealed journal instead has
an exact, immutable `1024 + Capacity` length, cannot grow, and must handle
pressure by checkpoint/recycle or refusal.

The ordinary buffered-delta policy caps, and with the shipped overlay geometry
selects, a 2.5 MiB record region, plus the two header sectors. The foreground
fit guard preserves up to 512 KiB for one estimated future carried suffix,
yielding the qualified 2 MiB current append window. The reserve is a policy
calculation, not a fixed subregion: exact prepared-batch size decides whether
the current append fits, and a wider current or future interval takes the
bounded physical checkpoint fallback. Per-mutation acknowledgement journals
use the separate option-derived capacity policy.

Records start on the damage-granule boundary. A 32-byte prefix carries kind,
reserved-zero bytes, sequence, generation, and key/value or batch-body
geometry. One CRC32C and its complement seal the complete logical record; zero
padding extends it to a whole sector. Strict sequence validation prevents stale
bytes left after recycle from becoming live records.

The one current grammar has five authenticated top-level kinds:

| Kind | Name | Generation meaning |
| ---: | --- | --- |
| 1 | Put | one logical generation |
| 2 | Delete | one logical generation |
| 3 | Batch | one atomic generation containing ordered put/delete entries |
| 4 | ConditionalBatch | one decision-bound atomic generation containing ordered put/delete entries |
| 5 | DeltaBatch | one put/delete entry per consecutive generation, ending at the record generation |

A conditional batch prefixes its entry body with `MarkerID [16]byte`,
`MarkerEpoch uint64`, and `TxnID uint64`. The decision log supplies the exact
participant generation binding during recovery. A delta batch carries complete
logical puts and deletes; it has no alternate entry grammar.

One unrecycled live window belongs to exactly one record family. The atomic
family contains kinds 1 through 4 and forms a one-generation chain from the
header base; a conditional may be followed by another record at the same
generation because an aborted prepare does not advance logical state. The delta
family contains only kind 5 and forms one contiguous generation interval from
the header base. Mixing atomic and delta records before recycle is corruption
and is also refused at append time.

## Ordering

For `DurabilitySync`, a mutation is fully validated and encoded as redo before
the journal record is synchronized. Only then may the canonical generation
become visible and the call return. A logical supported batch uses one kind-3
record and checksum, preserving its all-or-nothing publication. A database
transaction prepare uses the same atomic family through kind 4.

Buffered-visible mutations may accumulate in memory until `Flush`, `Close`, or
pressure selects a checkpoint cut. For a complete ordinary class-5 interval,
`Flush` writes one kind-5 delta batch containing exactly one ordered put/delete
entry per consecutive generation and synchronizes the journal once. Structural
work, an interval gap, an atomic-family live window, or a capacity/reserve miss
uses the physical root checkpoint instead.

A physical checkpoint writes and synchronizes its canonical data before
publishing the alternate durable root. Only after that root covers the records
being discarded does recycle write the opposite journal header and synchronize
it. A failed recycle leaves the live handle poisoned: the opposite header may
already be visible or durable even though memory intentionally retains the old
base. Only a complete reopen may select the authoritative header and replay the
remaining records; the live handle never retries from that ambiguous head.

The exact operating-system barrier depends on the configured durability mode
and platform. Ordinary filesystem synchronization and the platform's strongest
power-loss barrier remain distinct contracts.

## Recovery

Open validates the journal header, identity, bounds, sequence, record kind,
length, checksum, window family, and generation relationship before replay.
Replay goes through the ordinary mutation semantics so primary rows, schemas,
indexes, and canonical encoding do not acquire a second implementation. A
corrupt, mispaired, out-of-order, mixed-family, or unsupported record fails
closed.

Damage consistent with a not-yet-synchronized tail is narrower: invalid magic,
framing, sequence, or record CRC stops scanning at the last complete prefix.
Once a complete record CRC authenticates its bytes, an unknown kind or
semantically impossible payload is not treated as a tail; it returns a hard
journal-record error and Open fails closed.

For a kind-5 record with `N` entries, the entries cover the consecutive interval
`record.Generation-N+1` through `record.Generation`. If bounded staging
pressure physically checkpoints a prefix while Open is replaying and recovery
is interrupted again, the journal is deliberately not recycled. On the next
Open, the selected root generation determines the exact covered prefix and
replay resumes at the first uncovered entry.

Kinds 1 through 4 use the atomic generation grammar. Kinds 3 and 4 replay their
ordered entry set as one logical batch whenever it fits the current recovery
arena. If narrower reopen limits require private sequential replay, the journal
is retained across any intermediate checkpoint; a later Open replays the
complete covered atomic suffix in order rather than treating a root at the
record generation as proof that the whole batch was consumed.

Every retained kind-4 record must be resolved, including a record whose
generation the selected root appears to cover. Resolution first matches
`MarkerID`, `MarkerEpoch`, and `TxnID`, then requires an exact decision
participant tuple `(StoreID, JournalID, PreparedGeneration)` where
`PreparedGeneration` equals the record generation. No decision in the current
marker epoch means presumed abort; a decision with a mismatched participant
tuple, marker identity, or epoch fails closed. Standalone open has no resolver
and therefore returns the typed in-doubt error for any retained conditional.
An aborted conditional does not advance the logical generation, so the next
atomic record may reuse its prepared generation.

Recovery then selects and verifies the durable root that covers the replayed
cut. The final resolved fold completes before `RecycleResolved` can discard the
window. A failed resolution, fold, or recycle leaves the records intact for the
next Open, and the transaction decision log remains retained while any
participant may still need it. Readers are not exposed until journal handling
and root validation finish.

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
- exact kind numbers, atomic/delta family exclusion, and generation-chain
  validation;
- exact conditional participant binding, including root-covered records, and
  decision retention through successful resolved fold/recycle;
- interrupted delta replay that checkpoints a prefix and resumes its exact
  suffix;
- fixed-window hole-punch bounds, generation authority, fence ordering, and
  optional unsupported/error behavior;
- no journal lookup or allocation on a warmed read path;
- separate benchmark publication for ordinary-sync and power-safe lanes.
