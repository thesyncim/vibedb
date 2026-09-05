# Guarded point-update preparation and compact batch application

[Documentation](README.md) / [Research records](design/research.md)

**Record scope:** This page retains a dated proposal or investigation. Its
revision-specific findings and future work are not the current operating guide.
See [architecture](architecture.md) and [operations](operations/README.md).

This experiment follows the SIMD write profile at `0383eb86`. It targets two
separate costs: the quorum read before a conditional UPDATE proposal, and
reconstructing unchanged compressed columns during application. It does not
establish the full performance, transaction, recovery or scalability objective.

The compact batch experiment was implemented in `7aa4f496`, then reverted in
`d6374d33` after the matched campaign failed the performance guardrails. Its
after/before throughput geomeans were 0.884× and 0.852×. The reverse order had
mismatched locality; the first order had matched local reads but unexplained
slow bursts. This is insufficient evidence to retain a performance change.
Only private guarded preimage preparation remains in the active implementation.

## Private committed preimages

An eligible autocommit UPDATE has one exact primary-key equality, column
assignments, no maintained global index and no primary-key movement. Its only
data read is the complete existing row. Preparation selects the authenticated
leader through the existing leader-discovery and hint checks, then uses the
existing committed-read operation. Public reads and coordinated/indexed SQL
retain their existing linearizable behavior.

The resulting recipe must contain exactly one participant, relation batch and
`MutationPutDigestEqual`, with the old row's complete length and SHA-256 digest.
Replicated application compares this guard with the current row atomically
before applying the replacement. If the row changed, the command aborts. If
the row matches, evaluating the assignment from those bytes gives the same
row-dependent result as evaluating it at this conditional write's commit
point. An old preimage alone never establishes a successful SQL result.

A missing preimage, a row-dependent evaluation/validation error, or an
ineligible candidate must discard private preparation and run the existing
linearizable lowering before returning a recipe or definitive data-dependent
error. Authorization, identity, serving fences, applied floors, cancellation,
intent checks and response bounds still apply to the committed read. A leader
that has lost quorum cannot acknowledge the subsequent write merely because
it answered the private preparation read.

Execution retains the exact prepared command and request identity. An unknown
outcome never causes replanning. The existing PostgreSQL retry policy may use
a fresh identity only after a definitive abort. No request-ledger retention,
append, journal-decision, commit, checkpoint, apply or acknowledgment fence is
relaxed.

## Reverted compact batch experiment

Before rendering a complete compact leaf, qualify the complete replacement
vector and call the existing compact-column patcher once. It preserves admitted
posting slots and copies unchanged encoded columns. The ordinary batch path
still handles unsupported shapes, insertions, deletions and overflow cases.
Patch errors remain errors. Existing exact-index preparation, uniqueness
checks, staged-image admission, journal decisions and atomic publication remain
in place. Scratch references must be cleared and the staged image must own its
bytes before source leases or scratch are reused.

Logical row and index parity are the relevant oracle: historical posting-slot
placement and conservative summaries can differ from a fresh full rebuild.
The patcher still copies a page and replans changed columns; its CPU reduction
must be measured rather than inferred from the entire leaf-build profile.

## Evidence required

Validate stale-row guard rejection, missing/error fallback, public read-mode
preservation and exact recipe replay. Validate compact patch/fallback behavior,
indexes, snapshot visibility and the existing batch recovery fences. Compare
clean before/after binaries with the same Go 1.27 SIMD settings, Linux/ARM64
runtime, client and resource limits. Report both fixture orders and observed
locality separately, along with any failures or tail-latency regressions.

The guarded-preimage review passed focused Go 1.27 SIMD tests and race checks
for gateway and replicated-state behavior. They cover stale evaluation errors,
cancellation, missing-row fallback, cached leader selection, unchanged public
and indexed/coordinated reads, complete guards, durable recipe replay, and
actual replicated guard rejection. The compact batch experiment also passed
its focused snapshot/index/reopen and batch recovery checks before measurement;
correctness checks alone did not qualify its performance.
