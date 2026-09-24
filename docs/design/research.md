# Research and proposals

[Documentation](../README.md) / [Design](README.md) / Research

These records hold live proposals, accepted decisions, and targets that still
guide work. Read their dates and commit IDs before applying a finding to
current code. A proposal describes intended behavior; a measured result needs
its own evidence. The [design guide](README.md) describes the current system.

Superseded proposals and dated run reports are in the
[history index](../history/README.md).

## Current records

| Record | Recorded | Purpose |
| --- | --- | --- |
| [Storage and runtime redesign](../storage-runtime-redesign.md) | 2026-09-04 | Target for node-owned persistence, versioned reads, and non-blocking schema evolution. The node log exists ([replication](replication.md)); versioned reads do not. |
| [Checkpoint batch overlays](../checkpoint-batch-overlay-plan.md) | 2026-09-04 | Proposed reduction in compressed-leaf reconstruction during RF3 apply. |
| [Packed exact-index storage](../exact-index-packed-storage-plan.md) | 2026-09-08 | Decision to compress durable exact-index leaf images while keeping canonical resident indexes. Durable pack integration and its performance qualification are pending. |
| [Distributed optimizer](../distributed-optimizer.md) | from `09e60689` | Locality, global-index costing, and statistics notes with measurements. |
| [Performance and scale targets](../performance-and-scale-goal.md) | 2026-09-04 | Comparative performance, space, scaling, and schema-change goals. Targets, not measured capabilities. |
| [Wide-update workload plan](../benchmarks/wide-update-workload-plan.md) | 2026-09-04 | Proposed workload coverage and verification. |

## Investigations and tooling

- [CI performance investigation](../ci-performance.md) (2026-09-04), referenced by the [developer guide](../development/README.md).
- [Distributed SQL bottleneck investigation](../benchmarks/distributed-sql-bottlenecks-2026-09-04.md) (historical).
- [Benchmark reports](../benchmarks/README.md) and [qualification records](../qualification/README.md).

For an implementation change, update the current guide and keep historical
measurements attached to their original revisions. Record superseding results
with links instead of rewriting earlier numbers. When a proposal is implemented
or replaced, move it to [history](../history/README.md) with a banner that
links its replacement.
