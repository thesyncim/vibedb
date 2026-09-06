# Follow-up validation record

The final sustained comparison uses main
`b2f716ecb6163539315d7863e806b428a17804cd` and candidate
`858a834d6acc6f335f9a0e7f57ea1387caf1d256`. The candidate connects background
maintenance and fixes completed-seal admission. Application checkpoint
scheduling is identical to main. The integration rebase onto `3021bece` changes
none of the storage/runtime source or the benchmark harness.

Both binaries use the same qualification test copied from the candidate.
Test binaries shorten checkpoint cadence to two seconds; production retains
ten minutes and the same segment geometry.

## Exploratory runs

- Local initial candidate: 4,096 writes and 2,048 reads completed, including
  restart checks, but initial segment removals were `[0, 0, 0]`. This was an
  exploratory pilot on a shared host, not a performance comparison.
- [First remote follow-up](https://github.com/thesyncim/vibedb/actions/runs/33980653346):
  application checkpoints reached `[4140, 4120, 4136]`, yet initial segment
  removals remained `[0, 0, 0]`. The new assertion failed as intended. Inspection
  identified the unread seal completion blocking reclamation.
- [Completed-seal fix](https://github.com/thesyncim/vibedb/actions/runs/33981098824):
  the separate correctness job passed with segment removals `[3, 3, 3]` and
  checkpoints `[4016, 4028, 4044]`. Allocated node-log bytes, including reserves,
  were 580,288,512 at the detached final cut. This one run is not the matched
  performance summary.

## Broader CI retries

Failures are retained rather than silently removed from the validation history:

- [External gateway recovery](https://github.com/thesyncim/vibedb/actions/runs/33981101099):
  attempt 1 timed out reading `terminal-a` under a partition in two of three
  repetitions. Attempt 2 passed. A fresh [main control](https://github.com/thesyncim/vibedb/actions/runs/33981467876)
  at `b2f716ec` passed too. This fixture uses per-group WALs rather than the
  shared-node engine changed here.
- [Legacy WAL retention](https://github.com/thesyncim/vibedb/actions/runs/33981101110):
  attempt 1 lost a leader after a child rejected an unsupported in-band
  snapshot; attempt 2 passed. This test also uses the unchanged per-group WAL
  path. Its initial failure remains visible in the run's attempt history.
- [ARM process suite](https://github.com/thesyncim/vibedb/actions/runs/33981101111):
  an initial `TestGatewayHotShardLiveRF3NetworkPartition` run failed to publish
  the final move catalog. The corresponding ARM process suite passed on the newer `858a834d` candidate.

## Earlier scheduling experiment

[Run 33981098824](https://github.com/thesyncim/vibedb/actions/runs/33981098824)
compared `3548ad29` against the same main. All nine pairs passed and saved 48.5%
of allocated node-log bytes. However, paired write-p99 point ratios remained
1.016 for inserts and 1.034 for updates, with wide confidence intervals crossing
1. The full evidence is retained under
[sustained-with-scheduler](sustained-with-scheduler/node-space-comparison/comparison.json).

That candidate also retained due checkpoints across busy runtime ticks. Every
baseline run already advanced its application checkpoints. To preserve main's
scheduling and avoid adding capture work, that extra change was removed before
running the final comparison. These earlier results are not pooled with the
final candidate's samples.

## Final performance comparison

The dedicated job runs nine alternating pairs on one GitHub-hosted Linux
runner. Builds finish before measurements begin. Each trial starts fresh
storage and three real server processes, with two persistent authenticated
clients performing 1,024 inserts, 3,072 replacements, and 2,048 full-value
linearizable reads. Every trial checks all-server restart plus three individual
crash/restart paths outside its timed interval.

The sampler retains every operation, including stalls. Statistics resample
complete paired trials, not individual operations, and report paired geometric
ratios and 95% bootstrap intervals. Physical accounting includes every node-log
inode and active/spare allocation at a detached cut. It excludes SQL files,
gateway journals, TLS setup, fixture construction, and recovery time from the
foreground latency comparison. The primary-file architecture findings are in
[the initial investigation](initial-investigation.md).

All nine pairs completed in
[run 33982508361](https://github.com/thesyncim/vibedb/actions/runs/33982508361).
Correctness passed, but the candidate failed the no-slowdown requirement:
paired total-time ratio 1.0503 (95% interval 1.0080–1.1040), insert p99 1.1253,
update p99 1.0933, read p99 1.1635. Allocated-byte ratio was 0.5144. Full raw
results are retained in
[final-sustained](final-sustained/node-space-comparison/comparison.json).
Profiling and a deeper architectural review follow; no merge is authorized by
these timing results.

## Local diagnostic profile

The instrumented Linux/ARM64 profile in
[exploratory-profile](exploratory-profile/instrumentation.patch) is not a
performance comparison. It coincided with an unrelated Docker build consuming
about nine CPU cores on the shared host. The trial lost quorum at roughly index
680 and failed after 157 seconds, so no latency or space qualification follows.
Its partial samples, CPU profiles, maintenance timings, and instrumentation diff
are retained. Three processes recorded 43 maintenance calls totaling about
84 ms; the incomplete workload does not establish reclamation cost at scale.

The instrumented binary was built from the rebased `03c1c50d` candidate with
`GOEXPERIMENT=simd`, then run in Linux Docker with six CPU quota cores and
`GOMAXPROCS=2` per process. Subsequent Go commands use the project-wide shared
cache at `/Users/thesyncim/Library/Caches/go-build`.

The final candidate's Ubuntu process suite also reported
`TestServeRF3WALPressureBeforeMaintenance`: one follower did not catch up
through index 67. This fixture uses the legacy per-group WAL and the production
ten-minute cadence, rather than the new shared-node reclamation path. The
failure remains visible in
[run 33982511434](https://github.com/thesyncim/vibedb/actions/runs/33982511434);
it will be rechecked with the next candidate.
