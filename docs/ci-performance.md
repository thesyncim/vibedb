# CI performance

This change starts at `a069351bbd190c925fa81f8205c0b66ffce24911` (main,
2026-09-03). Optimize the existing Go workflow before considering a Bazel
migration. The first measurable opportunity is scheduling and cache freshness.

## Baseline evidence

The most recent passing baseline found before these increments was
[33570912428](https://github.com/thesyncim/vibedb/actions/runs/33570912428),
at `77cb367d0d9e36f85dedbf95754b39ea0bcd6a70` (2026-09-01): **27m40s** from
workflow creation through completion. Its x86 test job took 27m37s, including
483 s of cross-compilation, 757 s of ordinary tests, and 213 s of storage race
tests. A 5x improvement against this historical run requires <=332 seconds
end to end. Source changes since that revision make this a historical baseline,
not a controlled same-revision benchmark.

GitHub Actions runs [33801464065](https://github.com/thesyncim/vibedb/actions/runs/33801464065)
and [33730074031](https://github.com/thesyncim/vibedb/actions/runs/33730074031)
show these step durations:

| Step | Recent run | Earlier run |
| --- | ---: | ---: |
| x86 build and vet | 66 s | 61 s |
| x86 four cross-compilation targets, sequential | 516 s | 459 s |
| ARM serial package tests | 696 s | 722 s |
| Distributed serving race suite | 326 s | 534 s |
| Kubernetes topology qualification | 255 s | 321 s |

Both runs failed on ARM and cancelled the x86 matrix member before its ordinary
tests finished. The later qualification/race steps in that member did not run.
These are partial baselines, **not complete passing-suite durations**. Their
existing test failures are outside this workflow change.

In the recent ARM log, reported package times total about 607 s: durable storage
249 s, SQL/store/driver/pgwire 153 s, gateway/shard commands 112 s, and the remaining
packages about 93 s. These sums exclude compilation and runner overhead. The
largest shard is about 2.4 times shorter than the serial package-time sum; that
is a scheduling estimate, not a measured speedup.

The x86 jobs all restored the same approximately 64 MB `setup-go` cache, keyed
by Go version and `go.sum`. Their post steps said the primary key was already
present and did not save updated build outputs.

## Changes

- Four cross-compilation targets run in two independent compiler lanes, covering
  the same root and PostgreSQL client modules. They no longer delay native tests.
- Native tests use four disjoint shards on each of the existing architectures.
  `go list ./...` discovers the complete package set; new packages default to the
  core shard. Each runner retains `-p=1` and the 25-minute package timeout.
- Slow Linux process qualifications and both storage-race lanes run independently.
  Short recovery/client checks share a runner; distributed race packages use two
  balanced lanes. Build/vet runs in the core shard on each architecture. This
  limits initial fan-out to 20 jobs; the first 30-job experiment left nine jobs
  queued while only about 20 ran. Restore activation shares the contract runner
  and retains its prior required check name through an aggregator.
- Modules have a shared dependency cache. Build/test cache snapshots have separate
  toolchain, host architecture, and lane keys, with a revision suffix and a prefix
  restore from an earlier revision. Go validates each restored cache entry.
  Explicit saves run after test failures too, so successful compilation survives
  a failing suite. Failed test results are not reusable Go test results.
- Matrix failures do not cancel siblings. Existing `test (ubuntu-latest)`,
  `test (ubuntu-24.04-arm)`, and `race (distributed serving)` check names are
  retained as aggregators. The test aggregators both require every split test
  lane to succeed, including cross-compilation and process evidence. Failure,
  cancellation, and skipped dependencies cannot make these checks pass.
- Qualification commands, environment switches, repeat counts, evidence
  validation, and artifact names remain unchanged. No suite moves off pull
  requests. Unit logs are uploaded for subsequent package timing analysis.

## Validation and measurement

Local validation: pinned actionlint v1.7.7; shell syntax; selector tests for
complete/disjoint coverage, new packages, invalid arguments, discovery failure,
empty shards, serial invocation and test failure propagation. The selector's
union was also compared with real `go list ./...`: all 83 packages exactly once.
The original and revised workflow steps were compared for unchanged evidence
commands, environment, artifact paths, repeat counts, and timeouts.

The first split workflow is running on GitHub; completed comparisons will be
recorded here once available. A macOS run cannot validate the
Linux filesystem qualifications or ARM runner performance. Compare the first
cold-cache run and at least two subsequent source revisions, separating queue
time, cache restore/save, compilation, and package execution. Compare the same
successful checks before claiming an end-to-end gain. Use the uploaded
`unit-timings-*` logs and the run's job/step timestamps.

Parallel runners duplicate some compilation and use more concurrent job slots; more
revision caches consume storage and can cause eviction. Observe queue time,
runner minutes, and cache hit rates as well as latency. Account concurrency
limits can absorb much of the scheduling gain. Cold caches and substantial
shared-package changes will be slower than warm leaf changes.

## Bazel and the 10x target

Go already caches builds and successful cacheable tests. Bazel's remote cache
can be useful, but it requires build targets and declared inputs; process tests
that invoke Go, use real filesystems, sockets, subprocesses, and external tools
need additional integration. It does not remove the runtime of fresh fault,
crash, or throughput qualifications. Those currently force repeat execution.

A 10x end-to-end gain is not supported by these measurements. The unchanged
Kubernetes lane alone took 4.25–5.35 minutes, and the durable package took about
4.15 minutes before build/setup overhead. After measuring this split, profile
those remaining critical paths. Further options include splitting expensive
tests within the durable package, caching the Kubernetes image build, or faster
runners. Treat each as a separate measured experiment; changing which suites
run on a PR would change the coverage policy and is not part of this change.

References:
[setup-go caching](https://github.com/actions/setup-go/blob/main/docs/advanced-usage.md#caching),
[Go build and test caching](https://pkg.go.dev/cmd/go#hdr-Build_and_test_caching),
[Bazel remote caching](https://bazel.build/remote/caching).

To report a run without counting later metadata edits as execution time:

```sh
gh run view RUN_ID --json createdAt,updatedAt,status,conclusion,jobs | \
  python3 scripts/ci/summarize-run.py --baseline-seconds 1660
```

The report separates initial queue time and identifies failed/incomplete runs;
an elapsed-time ratio for a failed run is not a passing-suite speedup.
