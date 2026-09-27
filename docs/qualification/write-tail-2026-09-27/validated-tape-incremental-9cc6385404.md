# Incremental validated-tape write comparison, 2026-09-27

[Qualification index](../README.md) / [Write-tail qualification](README.md)

This report compares the incremental change from `e9a718b11fb2efac2a03927ec5402856159d37f9` to `9cc6385404a8c6c9c480f7179037748c2791aa1b`. It is not a whole-main comparison. Its ratios describe this revision pair only and are not multiplied by results from other reports.

The [comparison job in workflow run 36315886514](https://github.com/thesyncim/vibedb/actions/runs/36315886514) completed successfully. Each workload ran three alternating baseline/candidate pairs. Both binary preflights passed. All twelve timed processes completed exactly 131,072 rows in 2,048 batches of 64 and passed the final full-row oracle; the four profile processes were separate diagnostics.

| Workload | Baseline median | Candidate median | Baseline/candidate ratio of medians | Paired ratios |
| --- | ---: | ---: | ---: | --- |
| Distinct sequential inserts | 6.284958485 s | 5.697201289 s | 1.103166× | 1.107944× / 1.094083× / 1.104576× |
| Shared 256-byte payloads | 9.067105249 s | 7.702188543 s | 1.177212× | 1.175164× / 1.177791× / 1.176161× |

The ratio is baseline median divided by candidate median. These results apply only to the incremental `e9a718b1`→`9cc63854` change; they are not cumulative and do not establish a 10× speedup.

| Workload | Pair | Baseline time | Candidate time |
| --- | ---: | ---: | ---: |
| Distinct sequential inserts | 1 | 6.312181165 s | 5.697201289 s |
| Distinct sequential inserts | 2 | 6.255139102 s | 5.717242625 s |
| Distinct sequential inserts | 3 | 6.284958485 s | 5.689926898 s |
| Shared 256-byte payloads | 1 | 9.069643640 s | 7.717766213 s |
| Shared 256-byte payloads | 2 | 9.067105249 s | 7.698402133 s |
| Shared 256-byte payloads | 3 | 9.059010730 s | 7.702188543 s |

The [run JSON](validated-tape-incremental-9cc6385404-runs.json) retains all 61 metrics for all twelve samples, the exact pair order and command forms, timestamps, GNU time fields, and links to all raw stdout/stderr captures. The [comparison summary](validated-tape-incremental-9cc6385404-summary.json) includes each metric's median and paired ratios.

Median allocation and whole-process RSS values were close:

| Workload | B/op, baseline → candidate | Allocs/op, baseline → candidate | Candidate B/op delta | Candidate allocs/op delta | Maximum RSS, baseline → candidate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Distinct sequential inserts | 222,691,184 → 222,509,520 | 1,326,438 → 1,326,350 | -181,664 (-0.0816%) | -88 (-0.0066%) | 126,028 → 126,508 KiB (+480 KiB) |
| Shared 256-byte payloads | 189,873,600 → 190,047,064 | 1,173,048 → 1,173,137 | +173,464 (+0.0913%) | +89 (+0.0076%) | 103,668 → 104,384 KiB (+716 KiB) |

Final file sizes matched exactly between arms: the sequential workload ended at 62,992,384 allocated bytes and 63,431,168 apparent bytes; the shared workload ended at 37,765,120 allocated bytes and 38,322,688 apparent bytes. Both had eight files. Each reported peak is based on 32 samples taken every 64 batches, not a continuous maximum. GNU time RSS covers the whole process, including setup and row-oracle work, and is diagnostic rather than a bound.

There are 61 metrics in each sample. In every paired run, 55 non-timing/allocation metrics matched exactly, including rows, topology/routing, certificate and sync counts, device commits/bytes, and final/sampled file-space values. The six varying metrics were `ns/op`, `prepare-apply-ns/row`, `total-ns/row`, `final-fold-ns/row`, `B/op`, and `allocs/op`.

## Separate CPU profiles

Four one-shot profiles were collected separately from the twelve timed samples: one profile per workload and arm. Profile capture includes benchmark setup and the final row oracle, so profile durations and CPU samples are not throughput samples. The [profile manifest](validated-tape-incremental-9cc6385404-profiles.json) links all four original `.pprof` files and the exported pprof text outputs.

| Workload | Arm | Pprof duration / CPU samples | `extractRows` cumulative | UTF-8 scanner cumulative | Raw profile |
| --- | --- | ---: | ---: | ---: | --- |
| Sequential inserts | Baseline | 7.12 / 7.12 s | 0.91 s | 0.47 s | [pprof](validated-tape-incremental-9cc6385404-profiles/01-sequential-insert-baseline.pprof) |
| Sequential inserts | Candidate | 6.52 / 6.48 s | 0.52 s | 0.10 s | [pprof](validated-tape-incremental-9cc6385404-profiles/02-sequential-insert-candidate.pprof) |
| Shared payloads | Baseline | 9.72 / 9.69 s | 3.22 s | 1.26 s | [pprof](validated-tape-incremental-9cc6385404-profiles/03-compact-shared-payload-baseline.pprof) |
| Shared payloads | Candidate | 8.41 / 8.39 s | 2.15 s | 0.11 s | [pprof](validated-tape-incremental-9cc6385404-profiles/04-compact-shared-payload-candidate.pprof) |

The baseline shared profile sampled `IndexIsCanonical` for 1.70 s cumulatively. Candidate samples included the validated-tape path (0.48 s cumulative) and remaining private canonical path (0.56 s); the UTF-8 scanner had 0.11 s of candidate samples. Thus the scanner work was reduced but not eliminated. Cumulative pprof entries overlap and are not additive. These diagnostics include setup/oracle work and do not prove causal throughput attribution.

## Scope and provenance

The benchmark executes local SQL `ReplicatedApply` across three durable collections; those collections are not physical replicas. It excludes Raft, network transport, proposal admission, and RF3 runtime. Per-batch row/value generation, mutation and command construction, apply, completion/publication checks, and the final full checkpoint/fold are timed. Database/schema setup, fixture construction, final full-row verification, statistics, and file-space sampling are outside the timer.

The baseline is clean revision `e9a718b11fb2efac2a03927ec5402856159d37f9`; the candidate is clean revision `9cc6385404a8c6c9c480f7179037748c2791aa1b`. Both overlaid only `sql/driver/replicated_apply_batch64_bench_test.go` from harness revision `9a136523919e89187edd2bd70d8645caeb22c579`, SHA-256 `729aecb98499032f9e667a91d5e71a694e1470ae31ec93cb48a3faecef3ece27`. Both preflights passed. Binary hashes, settings, source status, commands, artifact hashes, and the harness patch hash are in [metadata](validated-tape-incremental-9cc6385404-metadata.json).

The runner was a four-vCPU Intel Xeon Platinum 8370C virtual machine on GitHub Actions Ubuntu 24.04, Linux 6.17.0-1022-azure. It used Go 1.27.1, `GOEXPERIMENT=simd`, `GOMAXPROCS=2`, `LC_ALL=C`, and `/usr/bin/time -v`. Temporary files used a dedicated directory on ext4 with 4 KiB blocks; filesystem capacity is a single pre-run snapshot.

### Reproduction

Use two clean checkouts at the recorded revisions. In both, overlay the pinned harness file, build `./sql/driver`, and run the preflight once per binary. Then run one workload per process, following the pair order in the run JSON:

```sh
TMPDIR=<dedicated-temp-outside-evidence> LC_ALL=C GOEXPERIMENT=simd GOMAXPROCS=2 \
  /usr/bin/time -v <baseline-driver.test-or-candidate-driver.test> \
  -test.run='^$' -test.bench='^<one-exact-workload>$' \
  -test.benchtime=1x -test.count=1 -test.benchmem -test.timeout=20m
```

The full command forms and environment are in [metadata](validated-tape-incremental-9cc6385404-metadata.json). Public text captures replace ephemeral runner paths with placeholders; the JSON records original-artifact and published-copy hashes. Native pprof captures are byte-identical to the downloaded workflow artifact.
