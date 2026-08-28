# Durable request child-ledger fanout

Status: design boundary; not yet a persisted format or shipped command path.

The parent request ledger is intentionally single-wave. It has one payload build, one pending wave, one route-pin record, and one outstanding route-pin digest. Consequently, parallel proposals to different shard authorities cannot be made crash-safe by scheduling goroutines around the current lifecycle.

The target parallel design gives every logical participant an independently replicated child ledger. Child homes are derived from the authenticated parent request key and the canonical participant ordinal, so a tenant is not pinned to one ledger shard. The parent retains only a compact authenticated child manifest and aggregate decision/terminal state.

Persisted additions requiring format review are:

1. A canonical child identity containing parent key digest, parent request digest, plan root, participant ordinal, and child ledger home.
2. Paged parent child-manifest records with a descriptor, page-chain root, participant count, and sealed phase. Pages must stream without a participant-count cap.
3. Parent counters and chain witnesses for admitted, prepared, terminal, and reclaimed children. Counters are monotone summaries; exact child state remains authoritative in each child ledger.
4. A child head binding the parent manifest root, transaction identity, logical participant digest, route authority, and terminal decision digest.
5. A parent decision record that is legal only after every child has a prepared witness, or after an abort fence covers every non-prepared child.
6. A child terminal witness returned to the parent in canonical ordinal order. Parallel execution may complete out of order, but parent aggregation and client-visible results may not.
7. Bounded child-manifest garbage collection tied to terminal ACK, execution-pin release, and child terminal proofs.

Required recovery ordering is: reopen parent manifest; stream children in ordinal order; settle each child’s exact pending command; reconstruct prepared/terminal counters; recover or install the parent decision; finish children in bounded parallel windows; publish parent retirement; release the execution pin; publish the terminal result.

The concurrency budget must bound active children, exact pending command bytes, decoded result bytes, route-resolution work, and reorder-buffer bytes independently. Increasing participant count must increase elapsed work and persisted rows, not peak coordinator memory.
