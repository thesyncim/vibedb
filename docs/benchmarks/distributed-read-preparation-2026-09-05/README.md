# Prepared distributed-read reuse microbenchmark

This archive measures the local `sql/driver` prepared-only read-reuse path. It
compares the frozen fresh baseline with the candidate fresh control and the
candidate's retained prepared reader for point hits, point misses, and ordered
ranges of 32, 64, and 256 rows. It is a driver microbenchmark; it does not
measure CockroachDB, RF3 service latency, end-to-end SQL latency, or horizontal
scaling.

The four runs are an ABBA sequence: `before-1`, `after-1`, `after-2`, and
`before-2`. Each benchmark cell has three samples at `-test.benchtime=1000x`
with `-test.benchmem`, `GOMAXPROCS=4`, and Linux/ARM64 Docker. The exact
commands and timestamps are in [trials.json](trials.json); raw output is kept
in [before-1.log](before-1.log), [after-1.log](after-1.log),
[after-2.log](after-2.log), and [before-2.log](before-2.log).

The baseline benchmark binary was built from the baseline manifest's
`8e4e60f6bf5baa1eb2489e4a84a7eaec065ec51f`; the candidate fresh and reuse
binary was built from
`2402049ef159bd12421b5aee68d5d8692f945504`. The later review revision
`0da6e75c26bf34f6ec59ff7f1738c3c2d2f092ea` contains the final no-copy and
test copylock fixes. The ABBA timing logs are therefore attributed to the exact
baseline and 2402049e binaries. The final revision's 30-line benchmark confirmation
is retained separately in [final-revision-bench.log](final-revision-bench.log),
with its manifest in [final.json](final.json). Baseline and candidate manifests are in
[baseline.json](baseline.json) and [candidate.json](candidate.json). The
pinned runtime image is
`golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`.

The local quiet-window requirement was not met: concurrent build/test work and
unrelated containers shared the host during the campaign. Treat latency as
contended diagnostic evidence.
Allocation counts and the within-process fresh-versus-reuse characterization
remain useful. No claim here compares VibeDB with CRDB or reports RF3 service
performance.

Both benchmark arms pass nil explicit parameter descriptors and changing Go
string bindings to the driver APIs. Cursor consumption verifies every returned
JSON cell (and range stats) outside the Go benchmark timer; the frozen fresh
arm's historical `encode` phase is the same verification work. The candidate
checks retained slot, reader, and prepared identities on the probe and every
timed acquire. The production cache is bounded at eight slots and 2 MiB per
`ReplicatedApply`; each request releases its `Exec` and compiler workspace and
attaches to a fresh cut.

## Aggregate medians

The table reports `ns/op`; the complete `B/op`, `allocs/op`, per-order samples,
and all ratios are in [summary.json](summary.json) and
[summary.tsv](summary.tsv). Ratios are candidate divided by the named
baseline, so values below 1 are lower measured cost.

| Workload | Baseline fresh | Candidate fresh | Candidate prepared reuse | Fresh / baseline | Reuse / baseline | Reuse / candidate fresh |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| point_hit | 41,006.5 | 28,343 | 25,521.5 | 0.691x | 0.622x | 0.900x |
| point_miss | 21,712 | 21,888.5 | 15,274.5 | 1.008x | 0.704x | 0.698x |
| range_32 | 29,107.5 | 31,353.5 | 27,746.5 | 1.077x | 0.953x | 0.885x |
| range_64 | 42,874 | 40,177.5 | 42,753 | 0.937x | 0.997x | 1.064x |
| range_256 | 94,018.5 | 86,677 | 97,699 | 0.922x | 1.039x | 1.127x |

Prepared reuse reduced allocations in every workload. The medians are:

| Workload | Baseline B/op | Candidate fresh B/op | Reuse B/op | Baseline allocs/op | Candidate fresh allocs/op | Reuse allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| point_hit | 79,992 | 80,024 | 61,544 | 202 | 202 | 165 |
| point_miss | 31,184 | 31,216 | 12,720 | 172 | 172 | 135 |
| range_32 | 47,290 | 47,322 | 29,266 | 120 | 120 | 87 |
| range_64 | 57,464 | 57,496 | 39,440 | 124 | 124 | 91 |
| range_256 | 121,208 | 121,240 | 103,184 | 132 | 132 | 99 |

## Validation and retained files

Focused Linux package tests and the corresponding race run passed; their raw
logs are [final-linux-tests.log](final-linux-tests.log) and
[final-linux-race.log](final-linux-race.log). The ABBA benchmark logs retain
all 90 benchmark lines and every Go benchmark allocation result. The test
executables are deliberately omitted from this archive; their hashes remain in
[candidate.json](candidate.json) and the source benchmark manifest.

The final revision's focused Linux proof, including 19 passing tests and zero
skips, is [final-revision-linux.log](final-revision-linux.log). The standalone
[summary parser](summarize.py) validates all four orders, all five workloads,
all three samples per cell, and writes `summary.json` and `summary.tsv`.
[sha256.json](sha256.json) and [SHA256SUMS](SHA256SUMS) record sizes and
SHA-256 values for every retained text artifact; the benchmark executables are
not included.
