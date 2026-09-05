# Competitive results

> **Development status:** VibeDB and this evidence format can change or break at
> any commit. Never carry a result forward to a different revision without a new
> run and review.

## Registry status: no endorsed entries

**There are currently zero endorsed competitive results.**

This registry has no admitted publication bundle. The repository separately
retains [dated engineering benchmark reports](../../docs/benchmarks/README.md),
including measurements with raw evidence. Those reports are scoped to their
own methods and have not been admitted through this registry's contract.

For this harness:

- the generated 38-cell coverage reference records harness source shapes, not
  measurements;
- pull-request qualification artifacts are transient, claim-free regression
  evidence retained by CI for 30 days; and
- an allocation-gate pass is not a latency or throughput result.

Do not restore historical numbers here without rerunning the current harness.

## Admission contract for a future result

A result belongs here only when a reviewer can reproduce the claim from:

1. a clean, exact VibeDB revision and the pinned tool/dependency graph;
2. the complete command line and raw machine-readable rows;
3. host, kernel, architecture, filesystem, storage device, and cache-control
   metadata;
4. workload, corpus, seed, document shape, cardinality, index, durability,
   storage-profile, client, warmup, operation, conditioning, and checkpoint
   configuration;
5. process/node/replica/group/shard placement and network topology for
   distributed evidence;
6. all hard-bound and diagnostic/non-publishable flags;
7. repetition count and summary method; and
8. an immutable `VALIDATED.tsv` receipt from `cmd/publishcheck`, with the raw
   artifact bundle it digests.

Keep latency, throughput, logical bytes, allocated filesystem bytes, apparent
bytes, RSS, Go heap, and process-write units separate. Link every summary row to
its raw inputs.

A validator receipt is necessary but not sufficient for a claim. The current
validator checks a fixed inventory, provenance, selected row contracts, and
digests; it does not independently verify every runner flag or establish
cross-engine equivalence. The present RF3 matrix is a single in-process group
and cannot support a horizontal-scaling claim.

See [the harness reference](README.md) for the evidence levels and current
limitations.
