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

## Ordering

For `DurabilitySync`, a mutation is fully validated and encoded as redo before
the journal record is synchronized. Only then may the canonical generation
become visible and the call return. A logical supported batch uses one record
and checksum, preserving its all-or-nothing publication.

Buffered-visible mutations may accumulate in memory until `Flush`, `Close`, or
pressure selects a checkpoint cut. The checkpoint writes and synchronizes its
canonical data before publishing the alternate durable root. Journal storage is
recycled only after that root covers the records being discarded.

The exact operating-system barrier depends on the configured durability mode
and platform. Ordinary filesystem synchronization and the platform's strongest
power-loss barrier remain distinct contracts.

## Recovery

Open validates the journal header, identity, bounds, sequence, record kind,
length, checksum, and generation relationship before replay. Replay goes
through the ordinary mutation semantics so primary rows, schemas, indexes, and
canonical encoding do not acquire a second implementation. A corrupt,
mispaired, out-of-order, or unsupported record fails closed.

Recovery then selects and verifies the durable root that covers the replayed
cut. Readers are not exposed until journal handling and root validation finish.

## Required gates

- crash injection before and after every append, synchronization, publication,
  checkpoint, and recycle boundary;
- byte-identical logical state after replay and uninterrupted execution;
- exact-index equivalence after replay;
- bounded full-journal behavior and retry-safe failure reporting;
- no journal lookup or allocation on a warmed read path;
- separate benchmark publication for ordinary-sync and power-safe lanes.
