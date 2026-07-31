# Next wave

This is the current storage qualification backlog. Projected behavior remains
unclaimed until its named test or benchmark passes.

## Parallel writes

Implement tablet-local staging with grouped fencing and one serialized
publication point. The design and gates are in
[parallel-tablet-writers.md](parallel-tablet-writers.md).

## Deferred COW-leaf reseal

Same-size owned-frame patches already defer their checksum to checkpoint.
Extend that discipline to ordinary ref-changing writes only if crash-boundary
tests prove that the captured checkpoint owns every byte needed for reseal.
Mutation and buffered-checkpoint benchmarks decide whether the added state is
worth keeping.

## Indexed batches

Allow a transactional batch to stage document and exact-index changes together
instead of refusing indexed collections. The publication must remain atomic,
journal replay must rebuild identical postings, and SQL must stop surfacing the
current indexed-batch exclusion only after the store supports it.

## Overflow deduplication

Evaluate content-addressed sharing for repeated oversized values. Exact bytes
must be compared before sharing, references must follow snapshot reachability,
and deletion of one sharer must not reclaim a chain still reachable by another.
Footprint measurements need a repeated-overflow corpus rather than extrapolated
savings.

## Crash enumeration

Add a fault-injecting device that enumerates each write and synchronization
boundary, including torn tails, dropped writes, bounded reordering, and ENOSPC.
Every resulting image must either reopen to a verified committed root or fail
closed without inventing state.

## Overflow-aware salvage

Salvage currently recovers inline rows and reports skipped out-of-line values.
Recover overflow chains only after it can validate identity, order, bounds, and
completeness without relying on the damaged catalog.

## Workload coverage

Extend qualification with sub-64-byte rows, overflow-heavy rows, adversarial
shared-prefix keys, interruption during bulk creation, and many-collection SQL
catalogs. Each lane records its memory, disk, latency, and correctness bounds;
none silently joins an existing published comparison.

Distributed ownership follows the separate
[distributed-sharding plan](distributed-sharding.md) after the local storage
contract is stable.
