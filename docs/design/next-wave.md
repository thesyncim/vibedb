# Next wave: speed, space, durability, and the all-cases sweep

Everything here is projected or planned until its gate runs; the labels
are sacred. This document extends the plan of record after the
2026-07-27 wave and is sequenced against the open queue (splits and
merges, the recovery journal, parallel tablet writers, the
template-columnar class, the publishable suite refresh).

## Speed

**Hybrid epoch-protected reads.** Per-frame pin traffic is the measured
residual of the point-read path, and per-frame seqlock validation was
measured at parity and discarded. The amortized alternative: readers
announce an epoch in a padded per-core slot and read clean
immutable-after-seal frames with no per-frame atomics; reclamation waits
a two-epoch grace. Dirty in-place-mutable frames keep the pin protocol —
the split follows the existing frame state. Projected warm point read
320-350ns against the 376ns mmap engine this lane still loses to; race
builds keep the pinned path. Gate: the point-read benchmark, the
differential oracle, and a reclamation stress proving no
use-after-reclaim under forced eviction churn.

**Deferred COW-leaf reseal.** A uniform ref-changing write COWs its leaf
into a frame-native staging frame and seals it at acknowledgement; the
needs-reseal discipline that in-place patches already use applies
unchanged, moving the checksum to checkpoint capture. Projected uniform
acknowledgement from 8.3us toward ~5us. Gate: the mutation benchmarks
and the buffered crash boundary.

## Space

**Content-addressed overflow deduplication.** Repeated oversized values
share one overflow chain, keyed by content identity with exact byte
comparison before sharing, refcounted by root reachability exactly like
every extent. Bulk-build first, runtime admission second. Gate: the
footprint lanes on a corpus with repeated large values, plus retirement
correctness when one sharer is deleted.

## Durability and crash recovery

**Exhaustive crash-point enumeration.** A fault-injecting Device wrapper
enumerates every commit's write sequence and induces a crash after every
write and sync, plus torn tails, dropped writes, bounded reorderings,
and ENOSPC at each distinct path (data page, alternate root, journal
append, file growth). Every induced state must reopen to a verified
prior-or-committed root. This subsumes sampled crash tests with a
systematic sweep; the enumeration is bounded because the portable
device's sequence per commit is short and explicit.

**Verify and salvage, with catalog-loss recovery as a stated property.**
Leaves are self-describing — BucketID plus complete keys — so the entire
routing graph is reconstructable from leaf extents alone. The offline
verify tool walks checksums, graph reachability, free-set consistency,
and posting agreement; the salvage tool rebuilds catalog, tablets,
anchors, and locators from surviving leaves. Read-only opens at the last
durable root provide consistent online backup without pausing writers.

## All-cases workload lanes

Additions to the churn and qualification harnesses, each with its
measured gate: sub-64-byte documents (per-row overhead and occupancy),
overflow-heavy documents (chain read costs under cache pressure),
adversarial shared-prefix keys (fence growth and catalog bloat), crash
during bulk creation (a half-built file must fail closed), and
many-collection database catalogs.

## Sequencing

Splits and merges remain first among engine work — three open weaknesses
gate on them. The journal follows, then parallel writers. The epoch-read
and reseal-deferral items slot after the publishable suite refresh so
their gates measure against published baselines. The fault-device sweep
and verify/salvage tooling are parallel-safe and may start any time; the
overflow dedup and corner lanes ride the harness cadence.
