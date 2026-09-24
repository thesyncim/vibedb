# Debug distributed failures

[Documentation](../README.md) / [Developer guide](README.md)

Use this page when a multi-process test or qualification fails in CI or on
your machine. The goal is to find which invariant broke and the class of bug
behind it before you change code. For an operator-facing cluster, see
[troubleshooting](../operations/troubleshooting.md) and
[observability](../operations/observability.md).

## 1. Classify the CI failure

| Symptom in the run | Likely meaning | First action |
| --- | --- | --- |
| Job conclusion `cancelled` | A newer push superseded the run (`cancel-in-progress`), or the job hit `timeout-minutes` | Check whether a newer run exists for the same ref. A timeout shows as cancelled on the step that was running. |
| `… qualification skipped` or `emitted a skip` | The gate was set but a prerequisite was missing, or the test called `t.Skip` | Read the skip message in `go-test.jsonl`. It is usually strict allocation or `/proc`. |
| A `jq … \| wc -l` count check fails | Fewer passes than required: one of the `count=3` repetitions failed | Search `go-test.jsonl` for `"Action":"fail"`, then read that run's output. |
| A `grep -Fqx` or `contains(…)` step fails after the test passed | The summary line is missing a required fact, or a metric exceeded its bound | Compare the test's summary line with the validator's list in the workflow YAML. |
| Unit shard fails with no failing test | Package build failure, panic in `TestMain`, or the 25-minute package timeout | Look for `panic: test timed out` and the goroutine dump in the package output. |
| `core` shard fails after all tests pass | `go vet ./...` failed. It runs after the tests in the same job. | Run `go vet ./...` locally. |

Download evidence with the GitHub CLI:

```sh
gh run view RUN_ID --log-failed
gh run download RUN_ID -n hot-shard-mutation-COMMIT_SHA -D /tmp/hot-shard-evidence
```

Artifact names follow `<lane>-<commit sha>`, for example
`seamless-scale-in-out-<sha>` or `durable-rf3-external-<sha>`. Each artifact
contains the evidence the workflow recorded:

| File | Contents |
| --- | --- |
| `revision.txt`, `go-version.txt`, `go-env.txt`, `uname.txt` | Exact source and toolchain |
| `filesystem.txt`, `allocation.txt` | Filesystem type and the strict-allocation preflight |
| `command.txt` | The exact command to rerun |
| `test-list.txt` | Proof that the test name existed |
| `go-test.jsonl` | Complete `go test -json` stream |
| `summary.tsv`, `qualification.tsv`, `runs/run-*.tsv` | Validated metrics, when the test writes them |

Unit shards upload their output as `unit-timings-<runner>-<shard>`
(`go test -json`, 7-day retention).

## 2. Read a `go test -json` stream

Extract one test's output in order:

```sh
jq -r 'select(.Test == "TestGatewayHotShardMutationProcesses" and .Action == "output") | .Output' go-test.jsonl
```

Find each failing test and subtest:

```sh
jq -r 'select(.Action == "fail") | "\(.Package) \(.Test // "(package)")"' go-test.jsonl
```

Output from several child processes can interleave in one test log. Before you
read a sequence of events, filter by process name or PID.

## 3. Reproduce with the recorded command

Run the line from `command.txt` on Linux with the same `count`. From macOS, use
the [Linux container recipe](build-and-test.md#reproduce-linux-ci-from-macos).
Before calling a failure a flake, run it at least as many times as CI did.
Keep the whole log for every run, not only the failing one.

If the failure depends on timing, record the host's CPU count and load along
with the result. A four-core hosted runner and a 16-core workstation schedule
very differently. A timing failure that reproduces only on one of them still
points at a real bound or wait. Do not answer it by relaxing the threshold.

## 4. Collect RF3 node diagnostics

`vibedb-shard` writes a detached diagnostic snapshot when it receives
`SIGUSR1` on a Unix host. The snapshot is one stderr line with a stable prefix:

```text
VIBEDB_RF3_DIAGNOSTIC {"utc":"…","event":"snapshot","serial":7,"pid":…,"groups":…, …}
```

The same JSON replaces `rf3-diagnostics.json` next to the node log, through a
temporary file, `fsync`, and rename. The record includes Ready waves and
series histograms, proposal queue depths, per-group Raft status (leader, term,
configuration, and peer progress, including groups adopted after startup),
read-authority evidence, native server and embedded-gateway dispatch counters,
remote connection-pool counters, and storage-overlay fold counters. A startup
record with `"event":"read_authority_startup"` uses the same prefix.

Environment switches for the serving process:

| Variable | Effect |
| --- | --- |
| `VIBEDB_RF3_DIAGNOSTIC_STACKS=1` | Adds all goroutine stacks (up to 2 MiB; `goroutine_stacks_truncated` marks overflow) to every snapshot. Use it for stalls and deadlocks. |
| `VIBEDB_RF3_DIAGNOSTIC_ABORT_REASON=1` | Adds `VIBEDB_RF3_DIRECT_ABORT result_code=N` to abort errors and prints one `VIBEDB_RF3_DIRECT_SQL_OUTCOME …` line per aborted or unknown-outcome direct write (request ID, issuer sequence, group, generations, digests, result code). The pgwire direct pool prints `VIBEDB_RF3_DIRECT_ABORT_ATTEMPT …` lines. |
| `VIBEDB_RF3_DIAGNOSTIC_ABORT_TABLE=substring` | Limits outcome lines to distributions whose name contains the substring. Without it, outcome lines are not printed. |

The seamless-scale qualification sets all three for its child processes. The
abort table defaults to `scale_`. When a workload window fails, the test sends
`SIGUSR1` to every node, waits for the new serial, and stores the snapshots
and direct-outcome lines in its failure evidence. Other process tests collect
snapshots at their own checkpoints. Search a failed log for
`VIBEDB_RF3_DIAGNOSTIC ` to find them.

Compare two snapshots only when they have the same PID and a compatible group
inventory. Check the `*_available` and coverage fields before you trust a
counter. See [observability](../operations/observability.md#collect-a-physical-node-diagnostic)
for field meanings.

For CPU and scheduler investigations, `vibedb-shard` and `vibedb-gateway`
write CPU and execution-trace profiles when `VIBEDB_PROFILE_DIRECTORY` names an
existing directory. `VIBEDB_PROFILE_DURATION` sets the duration: default `60s`,
range `1s` to `5m`. Profiled runs are diagnostics, not benchmark baselines.

## 5. Keep failed state

Most process tests put their state under `t.TempDir()`, which `go test`
deletes afterwards. Set `TMPDIR` to a disk you can inspect, and prefer tests
that retain state on failure:

- `VIBEDB_SEAMLESS_SCALE_FAILURE=/abs/path` copies the test's whole temporary root
  to that path when the test fails.
- `VIBEDB_SEAMLESS_SCALE_EVIDENCE`, `VIBEDB_REPLICA_REPLACEMENT_EVIDENCE`,
  `VIBEDB_WAL_RETENTION_EVIDENCE`, and `VIBEDB_RF3_QUALIFICATION_PATH` write
  bounded TSV or JSON evidence to a path you choose, outside the test's
  temporary directory.
- `go run ./bench/rf3chaos -output /abs/new.tsv -runs N -timeout 5m` repeats
  the shipped three-process fault harness and writes canonical evidence. The
  output path must be absolute and must not already exist.

Inspect a retained, quiescent store with `vibedb-verify` (see the
[verification guide](../operations/verification.md)) before you reopen it with a
serving binary. Opening a store for writing can run recovery and change the
state you want to inspect.

## 6. Find the bug class, not only the symptom

A process failure usually shows the last symptom of a longer chain. Before you
patch, identify which of these broke:

| Question | Typical defect |
| --- | --- |
| Did a retry keep the original request identity and bytes? | A new ID was minted after a timeout, so an acknowledged write could apply twice or be reported lost. |
| Was an `ErrOutcomeUnknown` treated as failure or as success? | The caller either dropped a committed write or reported an unknown outcome as success. |
| Is a bound derived from state sized at boot? | A capacity or table sized at startup is exceeded after a node joins, a group is adopted, or a split doubles the groups. |
| Is there an explicit convergence barrier? | The test or controller waited on elapsed time instead of a durable, observable condition (applied index, published generation, `safe_to_stop`). |
| Is a stale fence rejected? | A former leader, retired replica, or old route generation served a read or accepted a write. |
| Does a restart resume from durable state alone? | Process-local transition state was lost across a restart. Look for `controller_restarted` and `target_restarted` evidence. |

If the fix for one symptom does not rule out the others in the same class,
the next E2E run tends to expose the next member. Add a focused test for the
class: an in-process or `synctest` reproduction of the interleaving, not
only a rerun of the qualification.

## Source map

- [cmd/vibedb-shard/rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go) and [serve_node_signal_unix.go](../../cmd/vibedb-shard/serve_node_signal_unix.go)
- [gateway/durable_sql_request_executor.go](../../gateway/durable_sql_request_executor.go) and [internal/gatewayruntime/pgwire_direct_pool.go](../../internal/gatewayruntime/pgwire_direct_pool.go)
- [internal/gatewayruntime/seamless_scale_process_test.go](../../internal/gatewayruntime/seamless_scale_process_test.go)
- [internal/processprofile/profile.go](../../internal/processprofile/profile.go)
- [bench/rf3chaos](../../bench/rf3chaos)
