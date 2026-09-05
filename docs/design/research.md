# Research and proposals

[Documentation](../README.md) / [Design](README.md) / Research

These records preserve design rationale, experiment constraints, and historical
baselines. Read their dates and commit IDs before applying a finding to current
code. A proposal describes intended behavior; a measured result needs its own
evidence. The [architecture guide](../architecture.md) describes the current
composition.

## Runtime and storage

| Record | Purpose |
| --- | --- |
| [Storage and runtime redesign](../storage-runtime-redesign.md) | Direction for node-owned persistence, versioned reads, and schema evolution. |
| [Physical-node runtime plan](../fused-node-runtime-plan.md) | Original structural contract, baseline changes, and measured checkpoints. |
| [Distributed redesign research](../fused-node-research-notes.md) | External architecture inputs and hypotheses. |
| [Catalog visibility](../catalog-miss-refresh-plan.md) | Cross-frontend catalog-miss failure and refresh work. |
| [Checkpoint batch overlays](../checkpoint-batch-overlay-plan.md) | Proposed reduction in compressed-leaf reconstruction. |

## Query and write paths

| Record | Purpose |
| --- | --- |
| [Read-path investigation](../read-path-redesign.md) | Admission, quorum, and execution cost investigation. |
| [Durable SQL write domains](../distributed-write-lane-proposal.md) | Protocol design introducing separate direct and coordinated identities. |
| [Guarded point updates](../guarded-point-update-plan.md) | Preparation work and the recorded compact-batch experiment/revert. |
| [Wide-update workload plan](../benchmarks/wide-update-workload-plan.md) | Proposed workload coverage and verification. |

## Evaluation goals and tooling

- [Performance and scale targets](../performance-and-scale-goal.md).
- [CI performance investigation](../ci-performance.md).
- [Distributed SQL bottleneck investigation](../benchmarks/distributed-sql-bottlenecks-2026-09-04.md).
- [Benchmark reports](../benchmarks/README.md) and [qualification records](../qualification/README.md).

For an implementation change, update the current guide and keep historical
measurements attached to their original revisions. Record superseding results
with links instead of rewriting earlier numbers.
