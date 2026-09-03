# CI performance

The target is at least a 5x reduction from the historical passing CI run of
27m40s: **5m32s end to end**. The changes use native Go caches and GitHub Actions
scheduling. No Bazel migration or additional runner service is required.

## Measurements

| Run | Result | End to end | Initial queue | Runner-minutes |
| --- | --- | ---: | ---: | ---: |
| [Historical baseline](https://github.com/thesyncim/vibedb/actions/runs/33570912428) | pass | 27m40s | 3s | 50.8 |
| [First wide, cold split](https://github.com/thesyncim/vibedb/actions/runs/33804436030) | fail | 8m41s | 1m18s | 80.9 |
| [Staged main run](https://github.com/thesyncim/vibedb/actions/runs/33808353104) | fail | 6m00s | 1m37s | 36.4 |
| [Clean warm measurement](https://github.com/thesyncim/vibedb/actions/runs/33808998083) | pass | 4m07s | 3s | 32.7 |

**The clean warm run passed all 27 jobs in 4m07s end to end: 6.72x faster
than the 27m40s historical passing baseline.** Initial queueing was 3 seconds
in both runs. Runner usage fell from 50.8 to 32.7 minutes, approximately 36%.
The measured warm run exceeds the 5x target without reducing coverage or
relaxing qualification limits. Earlier hot-shard failures remain documented
below; one passing run is not a claim that those intermittent failures cannot
recur.

The baseline was `77cb367d0d9e36f85dedbf95754b39ea0bcd6a70`, on 2026-09-01.
The clean measurement uses `40ace7cdaf7c8d1f88039053f9eb97199d93fd10`, on
2026-09-03, including concurrent backend fixes and additional SIMD validation.
This is a historical comparison, not a controlled same-source A/B experiment.
Warm-cache results do not guarantee the same latency after cache eviction, a
Go toolchain change, or a long GitHub runner queue.

The failed staged run took 4m23s from first job start to last finish; its queue
wait made the end-to-end time 6m. It used 28% fewer runner-minutes than the
historical baseline. Its only substantive failure was the hot-shard latency
gate: 7.51s foreground p99 against a 5s bound. Earlier runs also exposed
intermittent hot-shard request deadlines. No timeout, latency bound, or retry
policy was relaxed to obtain a faster result. Failed and cancelled runs are
not evidence of a completed passing-suite speedup.

## What changed

- Build caches previously shared an immutable dependency-only key. All x86
  jobs restored the same roughly 64 MiB snapshot and then declined to save
  newly compiled source. The new build keys include exact Go version, host
  OS/architecture, lane, and revision, with a lane-prefix restore from earlier
  revisions. Go validates individual cached entries. Module downloads use a
  separate shared key covering all `go.sum` files.
- Completed compilation is saved after test failures and cancellation too.
  Superseded revisions therefore do not discard all compilation work. Failed
  test results are not reusable Go test results.
- Four cross-compilation targets run in two lanes, covering the same root and
  PostgreSQL client modules. They no longer precede native tests. Both native
  architectures retain full build/vet checks.
- Native testing has five disjoint test shards on each architecture: durable,
  durable pressure, SQL, process, and core. Package discovery uses
  `go list ./...`; new packages default to core. Every runner retains `-p=1`
  and the 25-minute package timeout to avoid overlapping large mmap arenas.
- `TestFilePrimaryChurnQualification` and
  `TestFilePrimaryLargerThanCacheQualification` consumed 113.82s and 53.57s in
  the measured x86 package. They have their own runner. The ordinary durable
  shard uses the complementary exact-name exclusion; neither test, corpus,
  assertion, nor repetition count was changed.
- The new `GOEXPERIMENT=simd` planner/query/gateway tests run independently on
  both architectures, retaining their command and `-count=1`. Previously they
  delayed core tests by 124s on x86 and 99s on ARM.
- Sixteen work jobs are initially runnable, leaving room for the other four
  RF3 workflows triggered by main pushes. Once the SQL pair finishes,
  contracts/restore/LATERAL, recovery/client, storage-race, and distributed-race
  lanes start. This scheduling dependency does not suppress them after a SQL
  failure. The first 30-job experiment left nine jobs waiting for runner slots.
- Short qualification groups share runners. Independent groups explicitly
  continue after an earlier group's failure. Overall job/check failure is
  preserved. Qualification commands, environment switches, repeat counts,
  evidence validation, and artifact names are retained.
- Matrix failures do not cancel sibling coverage. Existing required check
  names remain as fail-closed aggregators, including the native test,
  distributed-race, restore-activation, and LATERAL checks. Skipped or failed
  required dependencies cannot produce a successful aggregate.
- New pushes cancel superseded runs on main and PRs. An older in-flight main
  revision may therefore have no complete evidence artifact; the latest
  revision still runs all checks. Manual dispatch supports a fixed-revision
  measurement while other main updates continue.

## Validation

Local validation includes actionlint v1.7.7 **with ShellCheck**, six selector
checks, and shell syntax validation. The base package partition was compared
with real `go list ./...`: all 83 packages exactly once. The extra pressure lane
shares only the durable package through complementary filters. The checks cover
new packages, invalid arguments, discovery failure, empty shards, serial
execution, failure propagation, and pressure-filter disjointness.

Workflow comparisons verified that moved test/evidence commands, environments,
artifact settings, and repeat counts remained unchanged. The concurrent feature
merge also required refreshing the generated `UNSAFE.md` inventory for
`planner/statistics_groups.go`, whose unsafe use is `unsafe.Sizeof`; the audit
suite passed after regeneration.

## Repeating the measurement

Wait for other CI runs to finish to measure an uncontended warm run:

```sh
gh workflow run ci.yml --ref main
gh run list --workflow ci.yml --event workflow_dispatch --limit 1
gh run view RUN_ID --json createdAt,updatedAt,status,conclusion,jobs | \
  python3 scripts/ci/summarize-run.py --baseline-seconds 1660
```

Use job/step timestamps for wall time; cached Go test output can replay prior
results. The report separates initial queue time and flags unsuccessful runs.
Unit JSON logs are uploaded as `unit-timings-*` for package/test analysis.
Compare runner-minutes and cache hit rates as well as elapsed time. The newest
snapshots across observed lane keys occupied about 4.6 GiB during rollout;
revision history and old lanes add storage and are subject to cache eviction.

Go already supplies build and successful-test caching. Bazel remote caching
would require additional build targets and declared inputs, and cannot remove
the execution time of fresh crash/fault qualifications. Further performance
work should follow the measured critical path, currently including Kubernetes
qualification, rather than assuming a build-system migration provides 5x.

References: [Go caching](https://pkg.go.dev/cmd/go#hdr-Build_and_test_caching),
[setup-go caching](https://github.com/actions/setup-go/blob/main/docs/advanced-usage.md#caching),
[Bazel remote caching](https://bazel.build/remote/caching).
