# Astra max storage-format research

Research baseline: latest fetched main `f05df25e8bebc13d9bfe11a2038bab43805f6c3d`.
The task worktree was rebased onto that revision before the final source review.
Two independent researchers used `gpt-6-astra` with reasoning effort `max`:
one examined primary encoding and allocation; the other examined the replicated
write/recovery format. The parent challenged their proposals against current
recovery and allocator code and ran the measurements retained here.

This is a completed research record, not a claim that a new format is deployed
or performance-qualified. The earlier node-log maintenance candidate remains
unmerged: it saved allocated space but failed the matched latency comparison.

## The strongest designs

| Design | Bytes it removes | Why it could preserve or improve foreground cost | What must still be proved |
| --- | --- | --- | --- |
| A coherent group checkpoint containing complete collection-root images, with Raft replay after that cut | Per-apply collection redo copies, transaction-marker records, and their sidecar files on each replica | Keeps current primary reads and mutation publication; removes duplicate recovery encoding, checksumming and appends | All member graphs remain recoverable, every fallback cut retains its Raft suffix, explicit-root reopen precedes allocation, and root retention does not cause extra checkpoint/space pressure |
| An explicit numeric stream derived from physical row rank | Repeated ID/sequence values split into separate shape-local delta streams | A certified base-plus-step calculation replaces up to 63 delta steps on point access | Every projection, count, group, patch and index path uses the correct row coordinate; failed detection does not slow updates/inserts |
| Implicit node-wave geometry | Repeated extent coordinates and absolute entry indexes; about 11.34 bytes/entry in the report's concrete 64-entry example | Fewer varint operations and copied bytes; existing sealed point-lookup index stays intact | Mixed-format recovery, exact-retry identity, empty entries and extent boundaries |
| Bounded inline admission or immutable packed raw-value pages for medium values | The 4-KiB allocation per value immediately above the 512-byte inline threshold | Potentially fewer pages or direct raw-byte reads without an entropy decoder | Cold bytes read, COW cost, shared-page ownership, snapshots and reclamation stay bounded |

For a fully backed basic two-member profile, the proposed root-vector pair
would remove about 99.01 MiB across RF3 before accounting for any additional
retained graph space. This is a format budget, not a measured net saving. It
also removes K journal appends and one decision append per physical apply;
the current implementation does not issue a sync for each of those appends.

The replicated checkpoint design has the largest architectural upside. The
rank-derived stream is the smallest tractable true primary-format experiment.
Neither is equivalent to merely increasing compression or running more GC.
The detailed [primary analysis](astra-max-primary-format.md) and
[replicated-format analysis](astra-max-replicated-format.md) include rejected
alternatives, byte equations, exact source references and falsification tests.
The [second Astra-max pass](astra-max-second-pass.md) records the sealed-route
and immutable-overflow-suffix ideas; its sealed-route patch is retained only
as an uncompiled research artifact. The [third pass](astra-max-third-pass.md)
records the writer-work proof and the exception-design caveats.

## Measured: the medium-value boundary

The parent ran sixteen current-main cases, each containing 256 distinct
canonical JSON documents loaded through four 64-document updates, flushed and
read back byte for byte. Sizes were 512, 513, 1,024 and 4,096 bytes; the inline
limit was either the default 512 or a 4,096-byte configuration counterfactual.
Payloads used a repetitive spelling or a deliberately wide deterministic
alphabet. The latter is not a random-entropy or incompressibility claim.

| Case | Live leaf + overflow bytes |
| --- | ---: |
| 512-byte repetitive documents, default | 4,096 |
| 513-byte documents, default, either fixture | 1,060,864 |
| 513-byte repetitive documents, inline limit 4,096 | 4,096 |
| 513-byte wide-alphabet documents, inline limit 4,096 | 135,168 |
| 4,096-byte documents, default, either fixture | 2,109,440 |
| 4,096-byte wide-alphabet documents, inline limit 4,096 | 1,105,920 |

These are measured live primary extents. The default 513-byte cases contain
1,048,576 bytes of overflow extents plus 12,288 bytes of leaves. Whole-file and
ordinary-journal apparent/allocated totals are retained separately in the
[JSON census](primary-boundary-census.json) and [raw output](primary-boundary-census.txt).
The [exact test source](primary-boundary-census_test.go.txt) is preserved outside
the active test package. These standalone buffered-visible cases are not an
RF3 SQL result or a latency comparison. Increasing the inline limit is not
being adopted on this evidence alone.

## Estimated: smaller primary numeric streams

The retained 100,000-row corpus contains values that are exact functions of
physical rank even after rows are partitioned into different JSON shapes.
The proposed stream stores a checked base and step plus exact spelling
metadata. It does not infer anything from field names or SQL declarations.

The primary researcher estimates about 0.16 MB less primary data per 100,000
rows: roughly 16% of the repetitive complete primary file and 2.3% of the
high-cardinality file. These are model estimates using existing codec totals,
not measurements from an implemented encoder. Exact per-field attribution,
page repacking and coverage on other workloads remain to be measured.
RF3 multiplies absolute savings by three, not the percentage of the entire
storage footprint.

## Reproduced and fixed: integer spelling corruption

The research exposed an existing overflow check that multiplied before
checking the numeric bound. Some 20-digit values wrapped without satisfying
that check. A direct regression reproduced a changed value:

```
input:  "ticket:46000000000000000000"
output: "ticket:09106511852580896768"
```

The parser now checks `(MaxInt64 - digit) / 10` before multiplication and lets
an exact nonnumeric codec preserve oversized spellings. The regression also
checks the signed maximum and valid zero-padded strings. The
[failing baseline](prefix-overflow-before.txt) and
[passing focused codec run](prefix-overflow-after.txt) are retained.
This correctness fix is implemented; the proposed new format designs are not.
It prevents future misencoding and cannot recover an original spelling already
lost by an older writer.

## Whole-architecture accounting and acceptance

Account separately for current primary graphs, retired or snapshot-pinned
graphs, sealed Raft history, the active segment and two reserves, user/system
journals, transaction metadata, and catalog/control files across all replicas.
The earlier RF3 comparison counted node-log roots only; it cannot establish a
whole-database compression ratio.

Latest main retains exact replicated sidecar file geometry but now permits
portable capacity with an explicit persisted flag. Therefore the historical
approximately 33-MiB-per-node sidecar profile is a logical geometry budget,
not a universal physical-allocation minimum on every filesystem. Record
allocated blocks on the actual filesystem and keep reserve capacity visible.

Do not remove any RF3 replica or weaken acknowledgement, checksums, identity,
snapshot or retention semantics. Before adoption, require crash/reopen and
fault histories plus matched reads/inserts/updates under sustained checkpoint
pressure, cold reads, exact indexes, schema variants and held snapshots.
Retain failed trials, errors and latency tails. Native query fallback and
checkpoint pressure are regressions to investigate, not acceptable hidden costs
of a smaller file.
