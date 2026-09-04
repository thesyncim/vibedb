# CI performance

The original target was a 5x reduction from the 27m40s historical passing
run, or 5m32s end to end. The later same-source-era target was a 2x reduction
from 11m08s, or 5m34s. The implementation uses Go's native caches, bounded
package concurrency, and GitHub Actions scheduling. A Bazel migration is not
required.

## Measurements

| Run | Result | End to end | Initial queue | Runner-minutes |
| --- | --- | ---: | ---: | ---: |
| [Historical baseline](https://github.com/thesyncim/vibedb/actions/runs/33570912428) | pass | 27m40s | 3s | 50.8 |
| [Earlier clean warm run](https://github.com/thesyncim/vibedb/actions/runs/33808998083) | pass | 4m07s | 3s | 32.7 |
| [Same-era baseline](https://github.com/thesyncim/vibedb/actions/runs/33904507541) | pass | 11m08s | 1m43s | 83.4 |
| [Low-queue intermediate](https://github.com/thesyncim/vibedb/actions/runs/33918818306) | pass | 4m42s | 2s | 30.7 |
| [Final CI configuration proof](https://github.com/thesyncim/vibedb/actions/runs/33921423635) | pass | 10m33s | 1m52s | 39.3 |

The final CI configuration run passed all 25 jobs and retained the full explicit
`go vet ./...` pass on x86 and ARM. It used 39.3 runner-minutes, **2.12x less
compute than the same-era 83.4 runner-minute baseline**. Its wall time was
dominated by repository-wide runner contention: three unrelated branch CI
matrices occupied the shared runner allocation, leaving this run with one to
six jobs active at a time. The report records that 1m52 initial wait and does
not present the 10m33 contended wall time as a scheduler speedup.

The low-queue intermediate passed in 4m42, **2.37x faster than the 11m08
same-era baseline** and **5.89x faster than the historical 27m40 baseline**.
That intermediate briefly omitted the full explicit vet pass, so it is useful
as a scheduling measurement rather than the final coverage proof. In the final
configuration, the corrected core jobs run tests and then reuse their build
objects for full vet; they completed in 1m04 on x86 and 46s on ARM. The
corresponding low-queue core jobs were 1m00 and 44s. From those measured job
durations and the reduced final job count, a low-queue final run should remain
under the 5m34 target, but that is an inference until shared capacity permits
an uncontended run.

No timeout, latency bound, retry count, assertion, race instrumentation, or
qualification corpus was relaxed. Failed and cancelled runs are excluded from
passing-suite claims.

## What changed

- Go module downloads have one dependency key. Build caches use exact Go
  version, OS, architecture, lane, and revision, with a lane-prefix restore
  from earlier revisions. Completed compilation is saved after failures and
  cancellation; failed test results are not reusable test results.
- Native tests discover packages with `go list ./...` and assign every package
  to exactly one base shard. Durable mmap-heavy packages remain serial.
  Process uses two package workers; SQL and core use four. Core then runs the
  full repository-wide vet pass from the objects its tests just compiled.
- The two large durable pressure qualifications have complementary anchored
  filters and independent lanes on both architectures. Every ordinary,
  pressure, example, and fuzz test name is selected exactly once.
- Exact SIMD-mode reruns were removed from the dedicated SIMD jobs because the
  global unit matrix already executes them on x86 and ARM. The dedicated jobs
  retain unique `nosimd`, AVX2-disabled, portable JSON, and placement coverage.
  Native x86 AVX2 dispatch is required in the existing durable unit shard.
- Storage race coverage uses one job per architecture. It compiles the durable
  race binary once and runs its complementary heavy/rest filters concurrently
  with the independent store-I/O race suite. The original selector and
  qualification exclusions are unchanged.
- Five distributed serving packages share one race job with four package
  workers, avoiding two copies of their dependency compilation. Focused
  LATERAL race packages use three package workers.
- Each of the four cross targets is independently schedulable. Native and
  race jobs no longer wait for SQL, removing the old second wave.
- Kubernetes builds the same five static binaries once through the restored Go
  cache, packages the same distroless runtime image, creates Kind concurrently
  with the remaining build, and rolls all RF3 roles before waiting for every
  existing readiness and ordinal check.
- Five aggregate jobs that executed no tests or validation were removed after
  confirming main had no required branch-protection check names. Superseded
  main and pull-request revisions cancel so stale work does not queue ahead of
  the newest commit. The four standalone RF3 workflows use the same policy.

## Coverage and validation

The final workflow keeps all unit packages on x86 and ARM, the full explicit
vet pass on both architectures, four cross targets, SIMD fallback modes,
storage and distributed race instrumentation, PostgreSQL clients, Kubernetes
RF3, recovery, replica replacement, hot-shard, transport, quorum, restore, and
WAL evidence gates.

Local validation includes actionlint v1.7.7, pinned ShellCheck, shell syntax,
and ten selector/orchestration tests. The package partition covers all real
packages exactly once. Tests also prove pressure and storage-race selectors are
disjoint and complete, failure status propagates, full vet follows core tests,
and the compiled storage race binary executes all three partitions. The merged
storage and distributed race commands passed locally; network-bound race tests
were rerun with loopback access. The final CI configuration Actions run passed
all 25 jobs.

## Repeating the measurement

Wait until other repository CI matrices finish, then run:

```sh
gh workflow run ci.yml --ref main
gh run list --workflow ci.yml --event workflow_dispatch --limit 1
gh run view RUN_ID --json createdAt,updatedAt,status,conclusion,jobs | \
  python3 scripts/ci/summarize-run.py --baseline-seconds 668
```

Use job timestamps for wall time and record initial queue time separately.
Unit JSON logs are uploaded as `unit-timings-*` for package analysis. Compare
runner-minutes as well as elapsed time because repository-wide runner scarcity
can dominate the latter.

Go already supplies content-addressed build caching and successful-test
caching. Bazel remote caching would require a second target graph and declared
inputs, while fresh crash, fault, race, and Kubernetes qualifications would
still execute. The measured bottlenecks were duplicated compilation, serial
package scheduling, container rebuilds, and GitHub runner allocation.

References: [Go caching](https://pkg.go.dev/cmd/go#hdr-Build_and_test_caching),
[setup-go caching](https://github.com/actions/setup-go/blob/main/docs/advanced-usage.md#caching),
[Bazel remote caching](https://bazel.build/remote/caching).
