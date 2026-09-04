# Wide packed equality count SIMD measurements

This is an incremental development measurement of the packed equality counters
on an Apple M4 Max, Darwin ARM64, with Go 1.27.0. The baseline is commit
`4b2339b6674118454a21bcaab5f1183497eed2b5`; its tree was identical to main
`c0675b8af96e3226919ef0b1685ac2b1c85b2d64` when the run was collected. The
candidate is commit
`1a9092c988243adf492d30a03a68f89d9dfc46b6`, which contains the implementation
and fixture changes measured here.

Both revisions used `GOEXPERIMENT=simd`. The four benchmark binaries were
compiled before timing, then run with one CPU, five 250 ms samples per side,
and alternating base/head order. Power and thermal conditions were not
controlled. The exact command and order manifests are
[`benchmark-commands.tsv`](benchmark-commands.tsv) and
[`benchmark-order.tsv`](benchmark-order.tsv); the raw output is in
[`raw/`](raw/). These are warmed immutable, unindexed equality COUNT and
packed-counter measurements on one machine. They do not make cold-storage,
concurrent, SQL, or other-CPU claims.

The comparison is against the prior SIMD checkpoint. The baseline already had
the 7-bit and 10-bit ARM64 counters, so those rows measure the incremental
change over that checkpoint rather than repeating the original scalar-to-SIMD
comparison in the [initial packed-count report](../packed-count-simd-2026-09-04/README.md).
At the baseline, widths 8 and 16 used the generic scalar dispatcher; the
candidate adds their ARM64 counters and improves the width10 four-load path.
The candidate keeps direct byte and little-endian `uint16` scalar fallbacks for
short or impossible width8/16 calls. Escape analysis reports that data in all
four SIMD kernel paths does not escape.

## Fixtures

The raw stream fixture contains 4,096 values. The primary-stripe fixture
contains 4,096 records and the durable query fixture contains 16,384 records;
those record fixtures use keys `row-%07d`. The wide label is the 17-byte spelling `c%016x`, with
`id=(row*73)&255` and value `(uint64(id)+1)*0x9e3779b97f4a7c15`; the encoder
must select a 256-entry dictionary with width 8. The wide integer is
`((row*32749)&65535)-32768`; its scrambled signed span selects FOR width 16.
The target is the value generated at row 17 (`-323`), so the exact raw-stream
match count is one. The query fixture uses 16,384 rows, giving 64 dictionary8
matches and one FOR16 match. Expected counts are enumerated from the generated
rows rather than derived by flooring the 65,536-row period.

The existing 7/10 fixture remains unchanged: its 4,096-row raw and stripe
cases use dictionary7 and FOR10, while its 16,384-row durable query has 128
and 16 matches. Every timed operation reports zero bytes and zero allocations;
each operation verifies its exact expected count.

## Raw counter and stripe results

These are medians of the five samples in each role's raw files. Speedup is
`baseline/candidate`; change is `(candidate/baseline)-1`, so a negative change
means lower candidate latency.

| Operation, 4,096 rows | Baseline ns/op | Candidate ns/op | Speedup | Change |
| --- | ---: | ---: | ---: | ---: |
| Stream packed equality, width 7 | 352.1 | 357.0 | 0.986× | +1.4% |
| Stream spelling equality, width 7 | 582.4 | 584.8 | 0.996× | +0.4% |
| Stream packed equality, width 10 | 742.9 | 165.9 | 4.48× | −77.7% |
| Stream integer equality, width 10 | 746.3 | 166.2 | 4.49× | −77.7% |
| Stream packed equality, width 8 | 3,178 | 50.24 | 63.26× | −98.4% |
| Stream spelling equality, width 8 | 3,602 | 487.2 | 7.39× | −86.5% |
| Stream packed equality, width 16 | 4,243 | 246.2 | 17.23× | −94.2% |
| Stream integer equality, width 16 | 4,309 | 249.0 | 17.31× | −94.2% |
| Primary stripe dictionary equality, width 7 | 652.6 | 647.5 | 1.008× | −0.8% |
| Primary stripe integer equality, width 10 | 867.1 | 247.6 | 3.50× | −71.4% |
| Primary stripe dictionary equality, width 8 | 3,719 | 537.9 | 6.91× | −85.5% |
| Primary stripe integer equality, width 16 | 4,448 | 332.4 | 13.38× | −92.5% |

The width7 stream counter itself is 1.4% slower in this five-sample run,
while the width7 stripe result is 0.8% faster and the integrated query is 2.2%
faster. Taken together, width7 is effectively unchanged here; no established
width7 speedup is claimed. These rows should be read as the incremental
checkpoint comparison; they do not replace the historical width7/10 scalar
comparison.

## Durable query results

Each query is `Select(Count()).Where(Cmp(field, Eq, value))` over a snapshot
created with `durable.CreateFromPrimary` and reopened without overlays or
declared indexes. A reused executor runs with one worker. Every case returns
one result row, scans all 16,384 rows through the token filter, and reports no
fallback rows, batches, index work, candidate work, or data skipping.

| Durable query, 16,384 rows | Baseline ns/op | Candidate ns/op | Speedup | Change |
| --- | ---: | ---: | ---: | ---: |
| Dictionary7 equality COUNT, 128 matches | 4,878 | 4,772 | 1.022× | −2.2% |
| FOR10 integer equality COUNT, 16 matches | 5,609 | 3,232 | 1.735× | −42.4% |
| Dictionary8 equality COUNT, 64 matches | 16,981 | 4,373 | 3.883× | −74.2% |
| FOR16 integer equality COUNT, 1 match | 20,005 | 3,519 | 5.685× | −82.4% |

The query rows include graph traversal, template resolution, packed counting,
and count aggregation. They exclude SQL parsing, client protocols, cold I/O,
and concurrent writes.

## Short-input diagnostics and profiles

The [threshold baseline](threshold-base.txt) and [threshold candidate](threshold-head.txt)
logs retain five 100 ms samples per side at one CPU for widths 6, 7, 8, 10,
16, and 32 at counts 0, 1, 8, 16, 31, 32, and 64; baseline ran before
candidate. Short inputs retain scalar counting. The measured one-row
candidate overhead is about 0.19 ns for width6, 0.15 ns for width8, and
0.24 ns for width16; width10 is about 0.30 ns slower at counts 8, 16, and 31.
Width32 stayed within 0.25 ns through 32 rows
and was 1.8 ns faster at 64 rows. These are local diagnostics,
not universal thresholds or timing guarantees.

The separate [dictionary8 profile](profile-label.txt) and [FOR16 profile](profile-number.txt),
with their [label run output](profile-label-run.txt) and [FOR16 run output](profile-number-run.txt),
show the durable query path reaching
`runDirectFileTokenScalarCount`/`FilterCountEq` and then
`countCompactPacked8EqualNEON` or `countCompactPacked16EqualNEON`. Profiling
runs are diagnostic and are separate from the timing samples.

## Validation and reproduction

SIMD and portable focused storeio/query tests passed, including exact codec
selection and query statistics. The packed storeio suite passed race detection
with strict pointer checking; the root build and vet and the full query suite
also passed. Disassembly review found no hot-loop calls or spills, and the
width boundary and scalar-oracle checks passed. AMD64 retains the scalar
dispatcher and is outside this ARM64 measurement.

To reproduce the paired run, compile identical benchmark files into test
binaries for the selected baseline and candidate revisions, then run:

```sh
GOEXPERIMENT=simd \
PACKED_SIMD_BENCHTIME=250ms \
PACKED_SIMD_REPETITIONS=5 \
bash scripts/bench/run-packed-simd.sh \
  /absolute/evidence-directory \
  /absolute/base-storeio.test \
  /absolute/head-storeio.test \
  /absolute/base-query.test \
  /absolute/head-query.test
```

The script alternates base/head order, selects the 12 storeio and 4 query
benchmarks shown above, validates the benchmark metrics, and writes the raw
files plus command/order manifests. Direct package commands use the same
selection:

```sh
GOEXPERIMENT=simd go test ./internal/storeio -run '^$' \
  -bench '^BenchmarkCompact(Stream(Packed|Spelling|Integer)|PrimaryStripePacked)Equality(7|8|10|16)$' \
  -benchmem -benchtime=250ms -count=1 -cpu=1

GOEXPERIMENT=simd go test ./query -run '^$' \
  -bench '^BenchmarkFilePackedEqualityCount(Wide)?$' \
  -benchmem -benchtime=250ms -count=1 -cpu=1
```

The paired native [workflow](../../../.github/workflows/packed-simd.yml) stores
the same revisions, SIMD environment, focused checks, and raw benchmark
provenance in CI.
Portable parity for `GOEXPERIMENT=nosimd` remains in the regular CI workflow.

## Source map

- [`internal/storeio/compact_stream_codec_packed_equal_bench_test.go`](../../../internal/storeio/compact_stream_codec_packed_equal_bench_test.go): raw and stripe fixtures, exact codec assertions, and benchmarks.
- [`query/file_packed_count_bench_test.go`](../../../query/file_packed_count_bench_test.go): durable query fixture, exact counts, scan statistics, and benchmarks.
- [`internal/storeio/compact_stream_codec_simd_arm64.go`](../../../internal/storeio/compact_stream_codec_simd_arm64.go): Go 1.27 ARM64 SIMD counters.
- [`docs/simd.md`](../../simd.md): implementation overview and SIMD gating.
