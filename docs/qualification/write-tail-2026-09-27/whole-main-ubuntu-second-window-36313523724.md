# Second whole-main Ubuntu write comparison, run 36313523724

[Workflow run 36313523724](https://github.com/thesyncim/vibedb/actions/runs/36313523724)
completed the second fixed-work comparison between full main `f9a9627a` and
candidate `afa3eeec`. Both used the same pinned benchmark harness, Go 1.27.1,
`GOEXPERIMENT=simd`, and `GOMAXPROCS=2`. The run completed twelve unprofiled
samples and four separate one-shot CPU profiles.

Each unprofiled operation applied 131,072 rows in 2,048 batches of 64 through
SQL `ReplicatedApply` across three durable collections, then performed the final
checkpoint and fold. These collections are not physical replicas. The workload
excludes Raft, network transport, proposal admission, and RF3 runtime behavior.
Three pairs per workload alternated baseline/candidate, candidate/baseline,
then baseline/candidate. Setup, final row verification, and file-space sampling
were outside benchmark time; apply preparation and the final fold were timed.

| Workload | Baseline median | Candidate median | Baseline/candidate ratio | Candidate time change |
| --- | ---: | ---: | ---: | ---: |
| Distinct varied payloads | 9.391908 s | 5.315740 s | 1.766811× | -43.42% |
| Shared 256-byte payloads | 8.436975 s | 7.569802 s | 1.114557× | -10.27% |

Ratios are the baseline median divided by the candidate median. This second
window's shared-payload median is faster on the candidate, while the first
window's shared-payload median was 4.75% slower. The shared result changes sign
across these two windows. This evidence does not establish a consistent shared
workload regression, explain the variation, or show a 10× speedup.

The following medians are baseline → candidate. File sizes cover all three
collections; user relation device bytes are not total device bytes across the
collections. RSS is whole-process maximum resident set from GNU `time`, including
setup and verification, and is diagnostic rather than a memory bound. The
sampled peak file sizes are observations rather than hard maxima.

| Metric | Distinct varied | Shared 256-byte payloads |
| --- | ---: | ---: |
| Allocated bytes per operation | 501,515,120 → 222,508,416 B | 307,970,952 → 189,866,952 B |
| Allocations per operation | 1,995,666 → 1,326,333 | 1,325,838 → 1,173,026 |
| Final allocated file bytes | 72,622,080 → 62,992,384 B | 42,123,264 → 37,765,120 B |
| Final apparent file bytes | 72,962,560 → 63,431,168 B | 43,254,272 → 38,322,688 B |
| Whole-process maximum RSS | 143,028 → 126,276 KiB | 103,112 → 102,900 KiB |
| User relation device bytes | 167,862,272 → 28,471,296 B | 134,651,904 → 2,322,432 B |
| System relation device bytes | 204,800 → 1,945,600 B | 204,800 → 1,740,800 B |
| Device commits | 1,024 → 21 | 1,256 → 17 |
| Certificates | 513 → 19 | 629 → 17 |
| Barrier syncs | 2,052 → 76 | 2,516 → 68 |
| Journal syncs | 1,539 → 57 | 1,887 → 51 |
| Physical structural leaf-split transactions | 511 → 2 | 627 → 0 |
| Final-fold nanoseconds per row | 37.83 → 39.53 | 30.82 → 69.39 |

The physical leaf-split counter does not count every logical volatile tail
split. The full comparison summary and every metric from all twelve samples are
in [comparison medians](whole-main-ubuntu-second-window-36313523724-comparison.json)
and [run records](whole-main-ubuntu-second-window-36313523724-runs.json). Each
record links its captured stdout and stderr in the
[raw-output directory](whole-main-ubuntu-second-window-36313523724-raw/).
Published stderr copies replace only the ephemeral runner binary pathname with
`<baseline-driver.test>` or `<candidate-driver.test>`. The JSON records original
and published SHA-256 values for each capture; stdout metric rows are unchanged.

## Diagnostic profiles

The four one-shot CPU profiles are diagnostic and are separate from the twelve
timed samples. Their CPU samples include setup and the final row oracle, which
are outside the benchmark's reported `ns/op`. Do not compare their reported
`ns/op` values as an independent timing sample or interpret profile cumulative
costs as additive.

| Workload | Baseline CPU samples | Candidate CPU samples | Candidate `buildPrimaryBatchLeaf` cumulative samples |
| --- | ---: | ---: | ---: |
| Distinct varied payloads | 7.83 s | 5.90 s | 1.25 s (baseline 0.74 s) |
| Shared 256-byte payloads | 6.43 s | 7.91 s | 5.12 s (baseline 0.83 s) |

The candidate shared profile attributed about 1.17 s cumulatively to
`scanner.ValidUTF8NoLineSeparator` (its inlined and generic entries overlap). In
the candidate varied profile, `scanner.validUTF8NoLineSeparatorGeneric` has about
0.36 s cumulative samples. These entries overlap with their callers. Profile
samples identify areas for further investigation but do not establish a single
cause for the difference between windows, and no proposed optimization is
measured here.

| Workload and arm | Flat profile | Cumulative profile |
| --- | --- | --- |
| Varied, baseline | [flat](whole-main-ubuntu-second-window-36313523724-profiles/01-sequential-insert-baseline.flat-top.txt) | [cumulative](whole-main-ubuntu-second-window-36313523724-profiles/01-sequential-insert-baseline.cumulative-top.txt) |
| Varied, candidate | [flat](whole-main-ubuntu-second-window-36313523724-profiles/02-sequential-insert-candidate.flat-top.txt) | [cumulative](whole-main-ubuntu-second-window-36313523724-profiles/02-sequential-insert-candidate.cumulative-top.txt) |
| Shared, baseline | [flat](whole-main-ubuntu-second-window-36313523724-profiles/03-compact-shared-payload-baseline.flat-top.txt) | [cumulative](whole-main-ubuntu-second-window-36313523724-profiles/03-compact-shared-payload-baseline.cumulative-top.txt) |
| Shared, candidate | [flat](whole-main-ubuntu-second-window-36313523724-profiles/04-compact-shared-payload-candidate.flat-top.txt) | [cumulative](whole-main-ubuntu-second-window-36313523724-profiles/04-compact-shared-payload-candidate.cumulative-top.txt) |

Profile-run stdout/stderr captures and SHA-256 hashes for the four original
`.pprof` files are recorded in the [profile provenance JSON](whole-main-ubuntu-second-window-36313523724-profiles.json).
The binary profiles themselves remain in the workflow artifact rather than the
repository; the JSON identifies their original names, byte lengths, and hashes.
Source-extract command provenance, binary digests, harness digest, and workflow
revision are included there as well.

## Provenance

The baseline is full main revision
`f9a9627a513a1599bc00bdaf59f4eb93470d9fbf`; the candidate is
`afa3eeecb1d756b7732e2d510715913512d230a3`. Workflow source was
`75294617bbb593eddf6e355168dd8045a615fc21`. The only source overlay in both
checkouts was the benchmark file at harness revision
`9a136523919e89187edd2bd70d8645caeb22c579` (SHA-256
`729aecb98499032f9e667a91d5e71a694e1470ae31ec93cb48a3faecef3ece27`). The
exact baseline/candidate binary digests, toolchain and runner metadata, source
artifact digests, all per-process argv and timing fields are in the linked JSON
records. The output packages do not contain test binaries.
