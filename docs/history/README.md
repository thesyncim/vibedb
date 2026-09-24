# Historical records

[Documentation](../README.md) / History

These pages are dated proposals, investigations, and run reports that no
longer describe current work. They are kept for their rationale and evidence.
Their numbers apply to their recorded revisions; do not apply a finding to
current code without checking the current guide. Live plans stay in
[research and proposals](../design/research.md).

- **Superseded**: implemented or replaced; the linked page is authoritative.
- **Historical**: a dated record kept for its evidence and rationale.

## Superseded proposals

| Record | Dates | Replaced by |
| --- | --- | --- |
| [Read-path redesign](read-path-redesign.md) | 2026-09-04 to 2026-09-05 | [Reads, leases, and time](../design/reads-and-time.md); its lease direction became [read authority](../design/reads-and-time.md#read-authority). |
| [Durable SQL write domains](distributed-write-lane-proposal.md) | 2026-09-04 to 2026-09-05 | [Exactly-once writes](../design/exactly-once-writes.md) |
| [Catalog visibility across physical frontends](catalog-miss-refresh-plan.md) | 2026-09-04 to 2026-09-05 | Implemented in `6402842cb`; see [request routing](../design/routing.md#catalog-generations). |

## Historical records

| Record | Date | Content |
| --- | --- | --- |
| [Fused physical-node runtime plan](fused-node-runtime-plan.md) | 2026-09-04 | Original structural contract, baselines, and checkpoints for the physical node. Current design: [replication](../design/replication.md) and [request routing](../design/routing.md). |
| [Distributed redesign research notes](fused-node-research-notes.md) | 2026-09-04 | External architecture inputs and hypotheses. |
| [Guarded point updates](guarded-point-update-plan.md) | 2026-09-04 | Preimage preparation and the reverted compact-batch experiment. |
| [Batch slot preservation results](batch-slot-preservation-results.md) | 2026-09-08 | Measured update, read, and insert results for the September 8 storage round. |
| [String compression and shared index fields](string-compression-and-index-sharing.md) | 2026-09-09 | Rejected per-string compression experiment and the decision to pursue [packed exact-index storage](../exact-index-packed-storage-plan.md). |
| [Cancelled 10M-row comparison](ten-million-row-run-2026-09-09.md) | 2026-09-09 | Partial insertion-scaling measurements from a run stopped during loading. |

Dated benchmark and qualification evidence lives in the
[benchmark archive](../benchmarks/README.md) and
[qualification index](../qualification/README.md).
