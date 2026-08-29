# Competitive results

No current competitive result is published for this documentation revision.

There is no linked immutable evidence bundle or validated competitive summary
from which to claim a performance, cost, storage, or scaling win. The generated
38-cell coverage count describes executable harness shapes only; it does not
include horizontal weak scaling or establish that any comparison was run.

This file intentionally contains no copied historical number. A new result
must come from the current harness and must include the metadata and raw rows
that [the performance guide](../../docs/performance.md) requires.

## Publication template

Record:

- Commit and dirty state
- Toolchain and dependency versions
- Machine, operating system, architecture, and filesystem
- Storage device and durability configuration
- Workload and read/write mix
- Corpus size, cardinality, document shape, and seed
- Index count/definitions, cache budget, and storage/compression profile
- Client count, warmup, measured operation count, conditioning, and checkpoint
  cadence
- For distributed evidence: process, node, replica, group, and shard counts,
  placement, and network topology
- Complete command line
- Raw result artifact
- Repetition count and summary method
- Any diagnostic or non-publishable flag

Then add a table that links each summary row to its raw artifact. Keep latency,
throughput, storage, allocation, and residency units explicit.

A full embedded/RF3 publication must also link the immutable `VALIDATED.tsv`
receipt produced by `cmd/publishcheck`. Do not hand-create that receipt or copy
selected rows into this file when the complete bundle does not validate.

The current receipt validates artifact inventory, provenance, and selected
contracts; it does not independently enforce every runner flag. Review the
complete commands and raw metadata before asserting equivalence, and do not use
the present single-group in-process RF3 latency matrix as horizontal-scaling
evidence.
