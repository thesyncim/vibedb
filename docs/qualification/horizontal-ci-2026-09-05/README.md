# Horizontal CI checkpoint — 2026-09-05

[Qualification index](../README.md)

This record retains the failed CI lanes from candidate head `2176933d` and
their bounded Linux follow-up checks. The initial run was CI `33970939319`.
The failed outputs are copied unchanged. The follow-up changes are test
fixture and test routing corrections only; they are not production performance
evidence.

## Initial CI run

| Lane | Recorded result | Failure retained |
| --- | --- | --- |
| Process | Failed | `TestServeRF3WALPressureBeforeMaintenance` failed after 31.98s; the diagnostics include a missing unstable entry at index 67. A later process rerun passed, while the pre-kill `waitAllApplied` check at index 67 remained under investigation. |
| Hot-shard | Failed | `TestGatewayHotShardMutationProcesses` failed with voter diagnostics and `retry-retired` admission messages. Its evidence validator then failed because the required `write-driven hot move` result was absent. |
| Distributed race | Failed | `TestAuthenticatedThreeVoterServingPutSurvivesLeaderLossAndExactRetry` failed at `owner_rf3_test.go:241` with `raftmodel: local member is not leader`. |

The raw lanes are [process](vibedb-horizontal-ci-process-failed.log),
[hot-shard](vibedb-horizontal-ci-hot-failed.log), and
[distributed race](vibedb-horizontal-ci-race-failed.log). They remain the
source for the initial failures; this record does not reinterpret them as
production defects or silently replace them with reruns.

## Focused Linux follow-up checks

The focused checks ran on Linux/arm64 with `go test -race`, read-only source
and module-cache mounts, and a fresh anonymous `/tmp` volume for the strict
allocation fixtures.

| Revision | Scope | Result |
| --- | --- | --- |
| `b463ef08` | `TestAuthenticatedThreeVoterServingPutSurvivesLeaderLossAndExactRetry`, five repetitions | Pass, no skips or race reports; [log](vibedb-horizontal-safe-checkpoint-rf3-ready-race-repeat.log) |
| `53c2f2f1` | `TestRF3TransactionSurvivesLeaderLossAndPublishesRelationBundleAtomically`, five repetitions | Pass, no skips or race reports; [log](vibedb-horizontal-safe-checkpoint-transaction-race.log) |
| `5e58d21f` | The same RF3 transaction test after follower-role refresh, one repetition | Pass, no skips or race reports; [log](vibedb-horizontal-safe-checkpoint-transaction-role-refresh-race.log) |
| `5e58d21f` | Six-package focused Linux race summary: `raftauthority`, `raftmember`, `raftmodel`, `multiraft`, `raftservice`, and `shardservice` | All six packages passed; [log](vibedb-horizontal-safe-checkpoint-race.log) |

The fixture changes in `b463ef08`, `53c2f2f1`, and `5e58d21f` wait for a usable
leader or route an exact command after a documented leadership change. They
do not establish whole-CI stability, SQL throughput, or a performance result.

## Limits and artifacts

The Linux follow-ups are bounded regression evidence for the named tests. They
do not replace the failed full CI lanes, cover every process or hot-shard
scenario, or prove the horizontal benchmark objective. Native macOS runs can
skip strict physical-allocation fixtures; the Linux logs above are the runs
used to execute those fixtures.

Checksums and archived byte counts are in
[checksums.json](checksums.json). The archived logs are:

- [Initial process CI log](vibedb-horizontal-ci-process-failed.log)
- [Initial hot-shard CI log](vibedb-horizontal-ci-hot-failed.log)
- [Initial distributed-race CI log](vibedb-horizontal-ci-race-failed.log)
- [RF3 serving leader readiness race log](vibedb-horizontal-safe-checkpoint-rf3-ready-race-repeat.log)
- [RF3 transaction five-repeat race log](vibedb-horizontal-safe-checkpoint-transaction-race.log)
- [RF3 transaction follower-role refresh race log](vibedb-horizontal-safe-checkpoint-transaction-role-refresh-race.log)
- [Six-package focused race summary](vibedb-horizontal-safe-checkpoint-race.log)
