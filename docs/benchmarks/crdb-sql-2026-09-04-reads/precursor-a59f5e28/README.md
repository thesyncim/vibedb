# Tiered-admission precursor: retained regression

Clean measured source `a59f5e28`. All 120,000 samples and full-table checks passed.
Same RF3 single-host method and 8192-row, C1/C8, 2000-operation, 1000-warmup,
three-repetition matrix as the concurrent-autocommit baseline. No tests, builds
or profile processing overlapped timed trials. Subsequent source edits were not
used by the already built benchmark binaries; the manifest identifies those binaries.

Concurrent reads improved, but C1 grouped scans fell to 114.7 ops/s from the prior
170.8 observation because each query repeated smaller workspace tiers. This run
is retained as a performance regression, not used to claim completion.
See `summary.md` and the full compressed raw samples. No workload beats CRDB.
