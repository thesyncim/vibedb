# Fourth unqualified rank-affine all-suite comparison

This directory preserves the fourth independent rank-affine performance
record. It compares candidate head
`0cb0d16a9a58944d79feeeecd063ff8aa239f5da` with exact main base
`02b75683f227470d6d198274f532bf39d5fa417d` in GitHub Actions run
[33995838996](https://github.com/thesyncim/vibedb/actions/runs/33995838996).
The run completed successfully, but the candidate remains unqualified because
three timing controls have paired confidence intervals above 1. A separate
packed-node diagnostic also measured six AMD scan medians 9–22% slower on an
EPYC 7763; that independent result is archived below and is part of the
qualification review. Samples from this run remain separate from the
[initial comparison](../initial-comparison/README.md), the
[second rejected comparison](../second-rejected-comparison-1ebcfc0e/README.md),
and the [third rejected comparison](../third-rejected-comparison-4627a391/README.md).

The paired job ran six alternating base/candidate pairs on one Ubuntu runner,
with a 200 ms benchmark window, `GOMAXPROCS=1`, and identical benchmark source
in both arms. The selected `all` suite ran the primary storeio and durable
benchmarks plus the independent query benchmark. Each pair produced three
binary logs per arm, for 36 unchanged raw logs in
`primary-format-comparison/`. The build identities, runner metadata, suite
selection, and binary SHA-256 values are in `primary-format-build/` and
`primary-format-comparison/environment.json`.

The run contains 53 `ns/op` cases: 30 storeio, 9 durable, and 14 query. All 30
storeio timing rows are clear under the rejection gate: none is a significant
regression, 28 are significant wins, and two intervals cross 1. The 48
`allocs/op` rows are unchanged at zero in both arms. The candidate also retains
the measured compact-format reductions in the build rows, including low
payload 36,661 -> 29,833 bytes and negative payload 13,518 -> 9,414 bytes.
These byte and allocation results do not qualify the timing result.

## Remaining timing controls

The following rows have candidate/base geomean above 1 and a paired bootstrap
95% interval entirely above 1. All values are `ns/op`; the complete report and
paired samples are retained without rewriting.

| Benchmark | Base median | Candidate median | Ratio | Paired 95% interval |
| --- | ---: | ---: | ---: | ---: |
| `durable/BenchmarkFileStoreBatchWrite/update/batch=100` | 10,569,794.0 | 10,662,127.5 | 1.00958 | [1.00269, 1.01712] |
| `query/BenchmarkRankAffineQueryFormat/ordinary-fixed-shape/numeric-eq` | 4,053,247.5 | 4,214,633.0 | 1.06857 | [1.03038, 1.13111] |
| `query/BenchmarkRankAffineQueryFormat/ordinary-fixed-shape/spelling-eq` | 9,486.0 | 9,626.0 | 1.01462 | [1.00239, 1.02906] |

The durable `update/batch=100` benchmark inserts 100 new fixed-shape documents
inside each `Update` transaction; it is not a replacement-patch benchmark.

The paired `comparison.json` also records read-side wins, query sparse rank
wins, all benchmark allocation counters, and every raw paired ratio. Those
wins are context; they do not cancel the three controls above under the
no-regression requirement.

## Fused-node and packed diagnostics

The same head passed the fused-node RF3 qualification job in the run; its
unchanged command output and metadata are preserved under `fused-node-rf3/`.
The separate packed diagnostics are retained under
`packed-diagnostic/amd64/` and `packed-diagnostic/arm64/`. The [AMD candidate
profile](packed-diagnostic/amd64/head-profile-top.txt), [AMD candidate raw
output](packed-diagnostic/amd64/head.txt), and [ARM candidate raw
output](packed-diagnostic/arm64/head.txt) are the primary correlation files;
their provenance files record the exact base and head. In the independent
[packed diagnostic run 33995831284](https://github.com/thesyncim/vibedb/actions/runs/33995831284),
six AMD scan medians regressed 9–22% on EPYC 7763 while all seven ARM
medians improved. CPU profiles are present for the AMD run; the ARM directory
contains raw output and provenance only. The archived amd64 codegen shows the
same 54-byte nested callback size in both binaries: base
`0xc424c0`–`0xc424f5` and candidate `0xc472e0`–`0xc47315`. Its 16-byte loop
body is at base `0xc424d8`–`0xc424e7` (start 24 modulo 64) and candidate
`0xc472f8`–`0xc47307` (start 56 modulo 64), with identical loop bytes.
Profile sample sums attributed to these loop PCs are approximately
1.31 s/175 operations for base versus 2.13 s/150 operations for candidate.
The source-identical consumer loop therefore crossed a 64-byte boundary in
the candidate. This supports an alignment or consumer interaction hypothesis,
but does not establish causation; an isolated control is still pending. These
independent measurements are not pooled with the paired comparison.

## Evidence map

- `primary-format-comparison/comparison.json` contains the paired medians,
  geomeans, intervals, and ratios.
- `primary-format-comparison/pairs.json` contains every paired sample, and
  `primary-format-comparison/01-*.txt` through `06-*.txt` are the unchanged
  base, candidate, storeio, durable, and query logs.
- `primary-format-build/` records exact base/head revisions, the selected
  suite, Go and runner metadata, and binary hashes.
- `fused-node-rf3/` records the completed physical-node qualification output.
- `packed-diagnostic/amd64/` retains AMD CPU metadata, base/head raw output,
  and CPU profiles; `packed-diagnostic/amd64/codegen/` retains the text-only
  callback dumps, exact build metadata, and normalized instruction summary;
  `packed-diagnostic/arm64/` retains the corresponding ARM control/candidate
  output and provenance.
- [validation notes](../validation-notes.md) tracks this record with the
  earlier campaigns and keeps the unqualified status explicit.

No samples from this directory are pooled with any earlier campaign.
