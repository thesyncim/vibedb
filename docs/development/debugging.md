# Debug distributed failures

[Documentation](../README.md) / [Developer guide](README.md)

Use this workflow to diagnose failed multi-process tests or CI qualifications.
For live cluster operations, see [troubleshooting](../operations/troubleshooting.md).

## 1. Classify and download CI evidence

| CI symptom | Likely cause | First check |
| --- | --- | --- |
| `cancelled` | Superseded push or `timeout-minutes` hit | Check newer runs / step timestamps |
| `emitted a skip` | Missing prerequisite (`fallocate`, `/proc`) or `t.Skip` | Inspect skip message in `go-test.jsonl` |
| `jq ... \| wc -l` fails | One of `count=3` runs failed | Filter `"Action":"fail"` in `go-test.jsonl` |
| Summary / TSV check fails | Missing required fact or bound exceeded | Compare summary line vs workflow YAML |
| `core` shard fails after tests | `go vet ./...` failed | Run `go vet ./...` locally |

```sh
gh run view RUN_ID --log-failed
gh run download RUN_ID -n <lane>-<sha> -D /tmp/evidence
jq -r 'select(.Action == "fail") | "\(.Package) \(.Test // "(package)")"' go-test.jsonl
jq -r 'select(.Test == "TestName" and .Action == "output") | .Output' go-test.jsonl
```

Reproduce on Linux using the exact line in `command.txt` (or via the
[Docker container recipe](build-and-test.md#reproduce-linux-ci-from-macos)).

## 2. Collect RF3 diagnostics and profiles

Sending `SIGUSR1` to `vibedb-shard` writes a `VIBEDB_RF3_DIAGNOSTIC` JSON line
and updates `<node-dir>/rf3-diagnostics.json`
([observability](../operations/observability.md#native-metrics-and-sigusr1)).
Useful environment variables:

| Variable | Effect |
| --- | --- |
| `VIBEDB_RF3_DIAGNOSTIC_STACKS=1` | Include up to 2 MiB of goroutine stacks in snapshots (for deadlocks/stalls) |
| `VIBEDB_RF3_DIAGNOSTIC_ABORT_REASON=1` | Log `VIBEDB_RF3_DIRECT_SQL_OUTCOME` / `ABORT_ATTEMPT` details |
| `VIBEDB_RF3_DIAGNOSTIC_ABORT_TABLE=sub` | Filter direct-abort logs to matching table names |
| `VIBEDB_PROFILE_DIRECTORY=/dir` | Write CPU & execution traces (`VIBEDB_PROFILE_DURATION`, default `60s`) |
| `VIBEDB_SEAMLESS_SCALE_FAILURE=/abs` | Preserve full temp root on seamless-scale failure |

Inspect retained quiescent store files with `vibedb-verify` before reopening
them for writing.

## 3. Identify the bug class

Before patching a symptom, check which invariant broke:
1. Did a retry mint a new request ID or alter canonical bytes?
2. Was `ErrOutcomeUnknown` conflated with definite failure or success?
3. Was a table/bound sized from startup state instead of dynamic max?
4. Did code wait on wall time instead of an observable durable barrier?
5. Was a stale fence (term, epoch, route, schema) admitted?
6. Was process-local transition state lost across restart?

Add a focused unit or `synctest` regression test for the bug class alongside
the fix.

## Source map

- [cmd/vibedb-shard/rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go) · [internal/processprofile/profile.go](../../internal/processprofile/profile.go) · [bench/rf3chaos/](../../bench/rf3chaos/)
