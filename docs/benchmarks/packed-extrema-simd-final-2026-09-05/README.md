# Packed integer extrema final qualification

This report qualifies the storage-native integer `MIN`/`MAX` path after the
packed-kernel layout repair and the merge of latest `main`. The immutable
baseline is `e1784046dd883ce9b050beae87b119d8855f3dea`; the measured candidate is
`b62b38e646a3c1a1d5dccf43af5f89da6af63195`. The subsequent release commits
`6ae83958` and `b1d42d8b` change only the AMD64 benchmark workflow and shell
harness, so they do not change either measured Go test binary.

Both sides were freshly compiled with Go 1.27.0 and `GOEXPERIMENT=simd` on an
Apple M4 Max running Darwin arm64. The baseline query test binary used the
candidate's exact benchmark fixture source, copied into the detached baseline
worktree before compilation. The candidate was also compiled with
`GOEXPERIMENT=nosimd` for the scalar control.

## Final result

Each role has five 250 ms samples with one benchmark CPU. Base and candidate
order alternated by round. No sample was dropped. The headline timings are
pooled medians; the existing-lane release guard uses the median of the five
same-round candidate/base deltas so changes in host speed between rounds do not
turn into a false regression.

| Query | Latest main ns/op | Candidate SIMD ns/op | End-to-end speedup | Candidate nosimd ns/op | SIMD/nosimd |
| --- | ---: | ---: | ---: | ---: | ---: |
| FOR10 `MIN` | 2,380,006 | 3,273 | 727.16x | 11,355 | 3.47x |
| FOR10 `MAX` | 2,378,677 | 3,273 | 726.76x | 11,347 | 3.47x |
| FOR10 `MIN` + `MAX` | 2,565,698 | 3,356 | 764.51x | 11,423 | 3.40x |
| FOR16 `MIN` | 2,307,256 | 3,333 | 692.24x | 12,370 | 3.71x |
| FOR16 `MAX` | 2,315,109 | 3,345 | 692.11x | 12,340 | 3.69x |
| FOR16 `MIN` + `MAX` | 2,477,982 | 3,409 | 726.89x | 12,520 | 3.67x |

All 110 timed benchmark lines report `0 B/op` and `0 allocs/op`. Before and
after each timed sub-benchmark, the fixture asserts the exact result cells.
Candidate SIMD and nosimd runs additionally require 16,384 rows scanned, one
covering column, one worker, zero batches, and no token or index work.

The two existing lanes that previously exceeded the 3% pooled-median guard now
pass the paired release rule against exact latest main:

| Existing lane | Base samples (ns/op) | Candidate samples (ns/op) | Median paired delta |
| --- | --- | --- | ---: |
| primary-stripe packed equality8 | 514.0, 511.5, 529.8, 551.4, 527.3 | 514.2, 540.2, 515.0, 516.1, 543.7 | +0.04% |
| ordered wide sparse less-than | 2,822, 2,816, 2,800, 2,930, 3,144 | 2,784, 2,834, 2,985, 3,094, 2,998 | +0.64% |

The equality kernel is again at offset `0x10` within its 64-byte block, matching
the original baseline layout. The separate layout report records the code move
and longer isolated control run.

## Validation

The integrated source passed the complete `internal/storeio` and `query` suites
under both `GOEXPERIMENT=simd` and `GOEXPERIMENT=nosimd`. The focused packed
suite passed with `-race -gcflags=all=-d=checkptr=2`. Linux AMD64 v1 SIMD test
binaries cross-compiled for both packages. `bash -n`, ShellCheck 0.11.0,
actionlint, and `git diff --check` passed for the final workflow and scripts.

The packed SIMD workflow now performs an additional native AMD64 qualification
using the same precompiled candidate binary. It proves AVX2 dispatch is enabled,
proves `GODEBUG=cpu.avx2=off` selects the scalar fallback, retains five
alternating zero-allocation samples for FOR10 and FOR16 combined extrema, and
requires the unrounded median of paired scalar/SIMD ratios to be at least 1.5x.
Its exact native result is retained as a CI artifact for the pull-request head.

Raw outputs are in [`raw/`](raw/). Consolidated values are in
[`summary.tsv`](summary.tsv), and exact revisions and commands are in
[`metadata/`](metadata/).
