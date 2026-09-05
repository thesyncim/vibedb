# Read-path redesign, active investigation

[Documentation](README.md) / [Research records](design/research.md)

**Record scope:** This page retains a dated proposal or investigation. Its
revision-specific findings and future work are not the current operating guide.
See [architecture](architecture.md) and [operations](operations/README.md).

The admission change removes a measured concurrency limit, but does not remove
the per-statement quorum round trip or SQL execution cost. The 2x CRDB goal is
not achieved. Trace regions now separate admission, quorum/cut acquisition,
SQL execution and response encoding. Profile before selecting the next change.

## Relevant current research

CockroachDB's SIGMOD 2026 [Leader Leases paper](https://assets.ctfassets.net/00voh0j35590/2EHUM3hbvPrvrYle4IAB5c/f5ad2be3b27726d04715e40785dd086c/leader-leases-paper-2.pdf)
uses node/store liveness support shared across Raft groups and explicit election
promises. Support expiration, durable state, leadership transfer and successive
membership changes are part of the protocol. The architectural benefit for our
goal is amortizing liveness overhead as the number of hosted groups grows.

[LeaseGuard](https://arxiv.org/html/2512.15659v1) explores reads during leadership
transitions by delaying commits and identifying reads unaffected by uncertain
log entries. Its clock bounds and read/write dependency checks are prerequisites,
not implementation details we can omit for SQL.

The [etcd Raft API](https://github.com/etcd-io/raft/blob/main/raft.go) documents
clock-drift assumptions for ReadOnlyLeaseBased and requires CheckQuorum. Merely
changing the enum does not establish the failure guarantees required here.

## Local implementation work to evaluate

1. Measure SQL preparation/execution versus owner scheduling, quorum wait and
   storage snapshot capture. Read-only timing must be distinguished from writes.
2. Fuse the separate server Probe with authorized data-read acquisition, as
   native point reads already do. Preserve live serving authorization and every
   identity, generation, term, capability and applied-floor check.
3. Pin only requested data relations; avoid retaining private system snapshots
   for data-only reads. Keep publication/intent checks atomic with capture.
4. Design shared-store durable election support and lease fencing only after
   recording clock, restart, transfer, reconfiguration and partition invariants.
   Preserve quorum reads as the fallback whenever lease validity is unproven.

These are local engineering hypotheses, not claims of implemented lease support
or measured speedups. Multi-group transaction visibility and nonblocking schema
evolution remain separate acceptance gates in performance-and-scale-goal.md.
