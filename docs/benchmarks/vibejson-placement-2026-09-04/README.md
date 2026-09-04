# Retained JSON placement index comparison

Ten alternating baseline/candidate passes on Apple M4 Max (`darwin/arm64`),
released Go 1.27.1, one CPU, and a 250 ms target per sample. Matching document
placement or range-split tests passed with a different shuffle seed before
each measured process. Input setup is outside the timed loops.

VibeDB baseline: `fff5d6892e27db30f919c3ff7081b291cf71e4a1`.
Measured placement change: `680f7354d27ea64c9a45821fab250d42a26ae016`.
Both sides use vibejson `1c8648e377e6575dc1a81ad41afdd415e270faba` to isolate
the VibeDB change. The subsequent generic cursor migration is separate from
these measurements and carries no additional throughput claim.

| Existing benchmark | Mode | Before | After | Time change |
| --- | --- | ---: | ---: | ---: |
| `DocumentPointProgram` | Portable | 421.1 ns | 214.9 ns | -48.97% |
| `PartitionRowsOnePass` | SIMD | 374.2 ns | 239.4 ns | -36.00% |
| `TranslateTailEntryMove` | SIMD | 1.919 µs | 1.652 µs | -13.94% |

All rows retain zero bytes and zero allocations per warm operation. Benchstat
reports `p < 0.001` for each timing change. These fixtures measure document
placement, one-row partitioning, and one move transition; they do not measure
aggregate database, RF3, or disk throughput.

The worker now builds directly into its retained index capacity, counts and
grows only after `ErrIndexFull`, and reads validated node spans directly.
Correctness coverage includes invalid suffixes, depth limits, escaped values,
last-duplicate lookup, workspace growth, and clearing borrowed references.

Reproduce the statistical comparison from this directory:

```sh
benchstat placement-before.txt placement-after.txt
benchstat split-before.txt split-after.txt
```

Run the existing benchmarks from the repository root with Go 1.27:

```sh
go test ./distribution -run '^$' -bench '^BenchmarkDocumentPointProgram$' -benchmem -cpu=1 -count=10
GOEXPERIMENT=simd go test ./internal/rangesplit -run '^$' -bench '^Benchmark(PartitionRowsOnePass|TranslateTailEntryMove)$' -benchmem -cpu=1 -count=10
```
