# Next wave: speed, space, durability, and the all-cases sweep

Everything still labelled projected or planned here holds until its gate runs;
the labels are sacred. Since this document was first written, routed splits and
merges, the recovery journal, epoch-protected reads, and the compact leaf have
landed, and the template-columnar class was measured and rejected as a default.
The remaining open queue is parallel tablet writers, deferred COW-leaf reseal,
overflow dedup, the fault-device sweep, verify/salvage, and the publishable
suite refresh.

## Speed

**Hybrid epoch-protected reads — landed.** Per-frame pin traffic was the
measured residual of the point-read path, and per-frame seqlock validation was
measured at parity and discarded. The shipped alternative: the direct point
read (`Collection.AppendRaw`) announces its generation in a padded per-slot
word and reads clean frames with no lock and no per-call generation lease
(`ReadEpochs`); the serialized writer scans the slots lock-free, and reclamation
is generation-based — a retired extent is not reused until no epoch or lease
`Minimum` and no recovery generation can still reach it. The read falls back to
the leased path only when the epoch table declines the entry (full, writer
fence, Close, or a persistence failure); a long-lived `Snapshot` still holds a
generation lease. This did not hit the projected 320-350 ns: the measured point
read is 437-446 ns, still trailing the 376 ns mmap engine, so point-read work
continues. The differential oracle and the reclamation stress
(`TestFilePrimaryReadEpochStress`, plus the zero-allocation assertion) gate it.

**Deferred COW-leaf reseal — future.** In-place same-size patches already defer
their reseal to the checkpoint worker (`pageCacheFrameNeedsReseal`). The
remaining item extends that discipline to the uniform ref-changing write, which
still seals its COW leaf image at acknowledgement (`preparePrimaryLeafMutation`
encodes the full leaf under the writer hold). Moving that checksum to checkpoint
capture is projected to lower uniform acknowledgement from 8.3 µs toward ~5 µs.
Gate: the mutation benchmarks and the buffered crash boundary.

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

Routed splits and merges, the journal, and epoch reads have landed. Parallel
tablet writers are next among engine work, since the journal's group commit
multiplies with them. The reseal-deferral item slots after the publishable
suite refresh so its gate measures against a published baseline. The
fault-device sweep and verify/salvage tooling are parallel-safe and may start
any time; the overflow dedup and corner lanes ride the harness cadence. The
[distributed-sharding plan](distributed-sharding.md) has its own gated
sequence after the shard-local storage contract is stable; it does not turn
local `TabletID` partitions into network ownership units.
