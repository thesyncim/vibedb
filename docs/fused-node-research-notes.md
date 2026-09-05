# Research notes for the distributed redesign

Checked 2026-09-04. These are architecture inputs and hypotheses, not measured
VibeDB advantages. The current fused-node acceptance thresholds remain in
`fused-node-runtime-plan.md`; this note does not expand that tranche or replace
its required process and performance evidence.

## Read coordination is a separate structural cost

CockroachDB's SIGMOD 2026 Leader Leases work combines node-level liveness
tracking, fortified Raft leadership, and unified leader/leaseholder ownership.
Its reported lease-maintenance CPU improvements concern that subsystem and
must not be treated as whole-database throughput improvements. The protocol
also has a formal liveness-fabric model. See the
[paper](https://assets.ctfassets.net/00voh0j35590/2EHUM3hbvPrvrYle4IAB5c/f5ad2be3b27726d04715e40785dd086c/leader-leases-paper-2.pdf)
and the authors' [overview](https://www.cockroachlabs.com/blog/distributed-database-leader-leases/).

VibeDB's fused dispatcher deliberately retains quorum ReadIndex. Therefore
removing the local SQL socket does not remove its quorum latency. After the
fused comparison, separate local dispatch, quorum wait, owner scheduling and
SQL execution in the measurements. A future lease proposal requires an exact
failure model, fencing, restart rules and partition tests before replacing
ReadIndex. A cached leader bit is not an equivalent proof.

## Planner quality requires maintained evidence

CockroachDB's current optimizer documentation describes full, partial and
forecasted statistics; size-aware scan/join costs; locality-oriented lookup;
and cached plans whose reuse depends on schema/statistics changes. Partial
statistics target changed portions such as index extremes. Forecasts use
historical trends. Multi-column histograms remain unsupported in that document.
See [optimizer documentation](https://docs.cockroachlabs.com/docs/stable/cost-based-optimizer).

Candidate VibeDB design, to evaluate separately: merge bounded per-shard
statistics with explicit generation, sampling coverage, staleness and error
metadata; preserve schema and distribution identity across splits; price
network bytes and fanout alongside rows; and compare estimated versus actual
cardinality. A planner must fall back when evidence is missing or invalid.
Uniform, correlated, skewed and changing distributions need separate plan and
runtime tests. More statistics are useful only if plan quality improves enough
to justify collection, maintenance and planning cost.

## SIMD is available but must target measured work

Go 1.27 provides experimental portable `simd` and architecture-specific
`simd/archsimd`, enabled with `GOEXPERIMENT=simd`. Architecture-specific support
includes amd64, arm64 Neon and WebAssembly; the portable API also supports
other architectures through its documented implementation. These APIs are
experimental. See [Go 1.27 release notes](https://go.dev/doc/go1.27).

The minimum toolchain stays Go 1.27. Use SIMD when a measured typed scan,
predicate, aggregation, hashing or index-search kernel justifies it. Keep the
scalar reference, exact NULL/type/overflow behavior, tail handling and
architecture coverage. Record toolchain patch version, experiment flags and
CPU features for both baseline and candidate. SIMD cannot remove consensus
latency or fix a poor distributed plan.

## Completion still requires comparable behavior

CockroachDB documents ACID transactions and serializable isolation by default.
See [developer basics](https://www.cockroachlabs.com/docs/stable/developer-basics.html).
VibeDB's current single-statement benchmark does not establish interactive
transaction parity, nonblocking schema evolution, or independent-machine
scalability. Those remain explicit parts of the overall goal even if this
tranche passes its narrower performance gate.
