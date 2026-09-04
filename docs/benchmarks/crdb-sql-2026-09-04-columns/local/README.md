# Durable column extraction: local diagnostics

The shipped candidate is source `d175605579ed3b843efeee7c5e04158f26cabff7`.
The baseline binary uses `fff5d689` plus only the new benchmark function.
Go 1.27, Darwin arm64, Apple M4 Max; 8,192 documents with 18 fields, no
persistent index; complete durable filter, group and ordered projection scans.
Each subbenchmark warms 16 executions and runs three 300 ms repetitions.
No build, test or profile processing overlapped the timed runs.

**These local timings do not establish a reliable speedup.** Repeating the
unchanged baseline at the end increased its medians by roughly 44–49%.
The final candidate is only 0.3–2.7% faster than that repeated baseline.
The initial apparent 3–7% win and later apparent regression of sorted lookup
cannot be cleanly separated from changing host execution conditions. All
intermediate results remain here; they are not acceptance evidence for the
2× database goal.

Median milliseconds per complete scan:

| Query / workers | Original baseline | Initial linear lookup | Sorted lookup | Final hybrid lookup | Repeated baseline |
|---|---:|---:|---:|---:|---:|
| filter/workers=1 | 5.614 | 5.431 | 5.854 | 8.124 | 8.348 |
| group/workers=1 | 6.010 | 5.739 | 6.517 | 8.705 | 8.849 |
| project/workers=1 | 6.098 | 5.799 | 6.880 | 8.810 | 8.903 |
| filter/workers=4 | 5.782 | 5.401 | 5.843 | 8.126 | 8.346 |
| group/workers=4 | 6.073 | 5.687 | 6.661 | 8.663 | 8.804 |
| project/workers=4 | 6.124 | 5.726 | 6.814 | 8.856 | 8.887 |

The final implementation compares at most four requested columns directly;
wider projections use a sorted directory. It borrows fields from validated
batch bytes, avoiding a temporary document copy and structural index. Numeric
columns keep exact source decimals and integer caches. Duplicate keys resolve
to the last value. Escaped keys, nested paths and joined paths retain the
structural fallback. Returned partials still detach retained data before batch
reuse; both decoded-text arenas rewind per batch. The directory lives only in
durable workers, leaving heap and join workspace layouts unchanged, and drops
borrowed plan names when workers park.

Validation at the sorted-directory stage: query, SQL and shard-service suites passed (81.375 s, 0.742 s,
8.014 s). The final tiny-directory selection and expanded numeric tests passed
with steady-allocation and join-layout gates (10.916 s). At that same stage, targeted race tests
for batch reuse and durable cancellation passed (7.427 s). A 10-second
valid-input differential fuzz campaign completed 70,260 executions, including
invalid-input skips. New deterministic comparisons include duplicate keys,
root scalars/arrays, exact decimals and integer overflow, escaped strings,
1–129-column directories, decoded-text admission and fallback without double
charging. Existing spill/ring tests and containment limits cover direct and
nested-path fallback execution.

Some early benchmark repetitions retain worker warm-up allocations; use the
separate steady-allocation gate rather than reading rounded benchmark counts
as a proof of zero allocation.

```sh
go test ./query ./sql ./shardservice -count=1
go test ./query -run '^$' -fuzz '^FuzzFileColumnsDifferential$' -fuzztime=10s -parallel=4
go test ./query -run '^$' -bench '^BenchmarkFileColumnScan$' -benchtime=300ms -count=3
```
