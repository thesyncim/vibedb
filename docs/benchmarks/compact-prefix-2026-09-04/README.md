# Compact scalar prefix comparison

The shared-node CPU profile identified repeated byte-by-byte prefix scanning
while encoding primary stripes on the replicas. The replacement compares
bounded 32-byte groups of four little-endian words, then words and remaining
bytes. XOR and trailing-zero counts locate the first mismatch exactly. It uses
portable Go and changes neither encoded bytes nor persistence barriers.

Go 1.27, macOS arm64 M4 Max; 100 ms trials, three repetitions, zero allocations.
This is a microbenchmark, not a SQL throughput claim. All iterations, including
the two intermediate variants, are retained. Median before/final nanoseconds:

| Input bytes | Common prefix | Before | Final |
|---:|---:|---:|---:|
| 12 | 0 | 0.509 | 1.213 |
| 12 | 12 | 3.776 | 3.972 |
| 32 | 32 | 8.928 | 4.520 |
| 256 | 128 | 39.50 | 12.86 |
| 256 | 256 | 74.47 | 19.76 |
| 4096 | 4096 | 1032.0 | 312.9 |

Immediate mismatches regress by about 0.7 ns; long prefixes improve about 3–4×.
The result must be checked at the SQL workload level before attributing a gain.
Tests cover lengths 0–257, all eight alignments, every mismatch position with
both high and low differing bits, and both unequal-length orientations. Existing
compact-codec and primary-leaf format/mutation tests also pass (3.159 seconds).
