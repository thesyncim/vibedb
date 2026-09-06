# Third rejected rank-affine paired comparison

This is the third independent rank-affine performance record. It compares
candidate `4627a3918ded255d1aa8cf21e2b9eed82d6d2730` with base
`47cedea615c9642eb3afc3b178d8da414f47e457` in GitHub Actions run
[33993560607](https://github.com/thesyncim/vibedb/actions/runs/33993560607).
It is a rejected result and does not qualify the format for adoption.
Samples from this run must remain separate from the
[initial comparison](../initial-comparison/README.md) and the
[second rejected comparison](../second-rejected-comparison-1ebcfc0e/README.md).

The remote job ran six alternating base/candidate pairs on one Ubuntu runner,
with a 200 ms benchmark window, `GOMAXPROCS=1`, and the same benchmark source
in both arms. Odd pairs ran base then candidate; even pairs ran candidate then
base. The raw build and runner metadata are retained in
`primary-format-build/`; the six logs, paired samples, and calculated report
are retained in `primary-format-comparison/`.

The candidate does produce real compact-format byte reductions in the recorded
build rows. For example, the low-cardinality payload is 36,661 -> 29,833 bytes
and the negative payload is 13,518 -> 9,414 bytes. The byte result is not a
qualification by itself: the paired control timings below still regress.

## Rejection gate

The following are the ten `ns/op` controls whose candidate/base geomean is
above 1.0 and whose paired bootstrap 95% interval is entirely above 1.0.
All values in this table are `ns/op`; byte, device-byte, and allocation
metrics are kept in the raw report and are not combined with these timings.

| Benchmark | Base median | Candidate median | Ratio | Paired 95% interval |
| --- | ---: | ---: | ---: | ---: |
| `high/Patch-score` | 27,049.5 | 27,388.5 | 1.01265 | [1.00873, 1.01566] |
| `late-miss/Patch-score` | 203,854.0 | 207,792.5 | 1.02096 | [1.01656, 1.02697] |
| `late-miss/Scan` | 318,952.0 | 324,127.5 | 1.01482 | [1.01057, 1.01726] |
| `low/Patch-score` | 165,047.5 | 167,896.0 | 1.01803 | [1.01112, 1.02513] |
| `negative/Patch-score` | 205,250.5 | 207,636.0 | 1.01429 | [1.01063, 1.01811] |
| `single/Patch-id` | 544,203.0 | 550,511.5 | 1.01375 | [1.00760, 1.01994] |
| `single/Patch-score` | 459,599.0 | 468,202.0 | 1.01875 | [1.01567, 1.02164] |
| `unrelated/Patch-id` | 32,158.0 | 33,455.5 | 1.03762 | [1.02953, 1.04559] |
| `unrelated/Patch-score` | 24,138.0 | 24,954.0 | 1.03432 | [1.02729, 1.04193] |
| `unrelated/Scan` | 45,407.5 | 46,214.5 | 1.01454 | [1.00421, 1.02445] |

There are material read-side wins in the same report, including
`durable/BenchmarkUnifiedGetRaw` at ratio 0.78587 [0.77784, 0.79541],
low-cardinality durable scan at 0.97911 [0.97774, 0.98093], and
high-cardinality durable scan at 0.95851 [0.95687, 0.96052]. They are recorded
as context and do not cancel the ten significant control regressions.

The report has 34 `allocs/op` rows, and every row is unchanged at `0.0` in
both arms. The independent `TestFileStoreWarmedPointMutationAllocations`
check passed on committed candidate `4627a391` in 2.152 seconds and restored
the same-size physical Put contract to one allocation. This record therefore
keeps that allocation result separate from the Put timing, whose paired
`ns/op` interval crosses 1.0 (`18,548.0 -> 18,954.5`, ratio 1.01940,
[0.99531, 1.03985]).

## Compiler proof

The local amd64 code-generation check for the final scan source is summarized
in [compiler-proof-4627-holes.txt](compiler-proof-4627-holes.txt). It records
the exact commands, source hashes, and dump locations without adding binaries
to this evidence directory. The post-refinement candidate has one cached
`meta.holes` load, with both per-loop `ends[hole]` bounds checks and the final
`ends[holes]` check eliminated; the earlier 4627 candidate dump retained those
checks. The dump comparison is implementation evidence only and does not
change the remote rejection result.

## Evidence map

- [validation notes](../validation-notes.md) tracks this campaign alongside
  the earlier records and keeps qualification status explicit.
- `primary-format-build/` contains base/candidate revisions, Go and runner
  metadata, and the four remote benchmark binary hashes.
- `primary-format-comparison/comparison.json` contains the paired medians,
  geomeans, intervals, and ratios; `pairs.json` contains every paired sample;
  `environment.json` records the runner, ordering, and benchmark settings.
- `primary-format-comparison/01-*.txt` through `06-*.txt` are the unchanged
  raw benchmark outputs for the storeio and durable suites.

The archived previous campaigns remain historical records. No samples from
those directories were pooled with this run.
