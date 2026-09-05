# Point-read compiler reuse evidence

This artifact records one bounded local driver diagnostic for the exact
baseline `3a7d3b15dfc82579c7080b4acf95a12383718737` and the frozen candidate
`371de0af92bb419126cf49c0843da9223f39043d`. It measures the prepared local
replicated point-read caller only. The timed loop contains cache Acquire,
candidate-key query execution, complete cursor verification, cursor Close, and
lease Finish for changing point keys in separate hit and miss workloads. It is not a distributed
throughput result and does not establish a CRDB win or an RF3 end-to-end win.

The measured candidate is the PR head
`371de0af92bb419126cf49c0843da9223f39043d`. The later merge commit is
recorded separately as `47cedea615c9642eb3afc3b178d8da414f47e457`; it is not
the source used to build the measured candidate binary.

The candidate and baseline were compiled as clean committed source archives
with the identical benchmark injected at
`sql/driver/replicated_point_cpu_bench_test.go`. The pinned runtime is
`golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`,
reporting Go 1.27.1, `GOOS=linux`, `GOARCH=arm64`, `CGO_ENABLED=0`,
`GOEXPERIMENT=simd`, and `-trimpath`. Builds used the project-scoped cache
derived from the shared Git common directory and the agent-specific
`luna-point-validation` temporary directory.

Native correctness passed for both binaries: 38 started, 38 passed, 0
skipped, and 0 failed. The valid timing order was four sequential blocks:
baseline, candidate, candidate, baseline. Each block used `-test.benchtime
5000x`, `-test.count=3`, and `-test.cpu=1`, with a dedicated Docker named
volume for `TMPDIR=/testtmp`. There are six samples per workload and arm,
24 benchmark rows total, and 120,000 timed verified operations. Every valid row has
positive `ns/op`, `B/op`, and `allocs/op` fields.

The four valid timing subprocesses ran sequentially in the order baseline,
candidate, candidate, baseline during the coordinator quiet window
2026-09-05 21:26–21:31 UTC and completed before its hard end (the handoff was
reported at approximately 21:30:29 UTC). Runtime `TMPDIR=/testtmp` was backed
by the owned named volume `vibedb-point-validation-luna-371de0af`; runtime
`GOTMPDIR` was omitted so the test could not select a host bind mount. The
exact replay commands and environment are in `timing-commands.txt` and
`provenance/runtime-provenance.json`.

The compiler-reuse change reduces the measured local median from 12,975 to
10,655 ns/op for hits (17.88% lower) and from 9,371 to 8,698 ns/op for misses
(7.18% lower). Hit allocation falls from 20,101 B / 131 allocs to 16,525 B /
110 allocs. Miss allocation falls from 11,560 B / 110 allocs to 7,984 B /
89 allocs. The six-sample ranges are retained in
`provenance/timing-summary.json` so
the result can be reviewed with the observed run-to-run variation.
The observed `ns/op` ranges were 10,399–13,421 before and 10,119–12,405
after for hits, and 9,100–9,573 before and 7,648–10,117 after for misses.

The implementation keeps scrubbed compiler storage only under its bounded
64 KiB budget; oversized compiler storage is released before the next bind.
That trades a bounded amount of retained memory for the allocation reduction,
with large statements falling back to fresh compiler storage.

The first four timing attempts are retained under `excluded/`. They produced
no benchmark rows because the agent temporary directory was mounted read-only
and `testing.TempDir` failed. A fifth attempt produced rows but is excluded
because it used the host agent bind for runtime temporary files rather than
the named-volume-only policy. Only `valid/abba-06-before.log` through
`valid/abba-09-before.log` are timing evidence.

See `evidence-manifest.json` for machine-readable provenance, counts, hashes,
and the explicit no-claim scope. `provenance/commands.jsonl` records the
build and image commands; `timing-commands.txt` records the four exact timing
commands. `ci-pr190.json` retains the merge and all 40 successful CI checks.
