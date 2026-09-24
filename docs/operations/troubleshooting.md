# Troubleshoot a cluster

[Documentation](../README.md) / [Operations](README.md) / Troubleshooting

Identify the failed phase before changing persistent state. Keep the exact
build, the original files, every operation or request identity, and the
relevant logs. These checks cover the local launcher, the RF3 serving
processes, and the operator control commands.

## Launcher messages

| Message | Cause | Fix |
| --- | --- | --- |
| `cluster dev: vibedb: invalid local development cluster` | Root is relative or unclean, not empty on first start, or its retained manifest does not match the flags or build. | Use an absolute clean path. Reopen with the original `--replicas` and `--physical-nodes`; use a fresh root for another topology or build. |
| `cluster dev: --physical-nodes requires 3 or 6 for RF3` | Unsupported node count. | Use 3 or 6. |
| `PostgreSQL endpoints must be distinct literal loopback addresses` | `localhost`, `0.0.0.0`, a remote address, or a duplicate. | Use literal addresses such as `127.0.0.1:7432`. |
| `PostgreSQL endpoints differ from retained node configuration` | SQL endpoints changed on restart. | Omit the flag or pass the original endpoints. |
| `cluster root is too long for the PostgreSQL DDL Unix socket` | Root path too long for `<root>/node-1/pg-ddl.sock`. | Use a shorter root. |
| `read authority flag differs from retained cluster policy` | Explicit `--read-authority` disagrees with the root. | Omit the flag on restart. |
| `acquire supervisor ownership: ...` | Another supervisor holds `.supervisor.lock`, or the root is not writable. | Stop the other supervisor; check permissions. |
| `reserve PostgreSQL listener ...: bind: ...` | The SQL port is in use or binding is not permitted. | Free the port or choose another on first start. |
| `development cluster readiness timeout` | A node did not print its ready marker within 30 s. | Rerun with `--diagnostics-on-exit` and read the node's tail. |
| `physical node <n> exited before cluster readiness` or `physical node <n> exited` | A node process stopped; the supervisor then stops the rest. | Read the bounded child output that follows; fix and restart the same root. See [node failure](node-failure.md). |

Usage errors exit 2 and print the usage block; runtime failures exit 1.

## Symptoms

| Symptom | First check | Next step |
| --- | --- | --- |
| `psql` cannot connect | Readiness line printed; literal loopback address; SQL enabled on that node; user `local`, database `vibedb`, `sslmode=disable`. | Retry from the [local tutorial](local-cluster.md#3-write-and-read-a-row). |
| A statement fails | Dialect support and write restrictions (autocommit only, no `RETURNING`). | Reduce it to a form in the [SQL reference](../reference/sql.md). |
| A write times out or disconnects | Whether it could have been admitted. | Resolve that request; see [below](#resolve-an-uncertain-write). |
| Reads and writes stop after a node fails | Which groups still have two reachable voters. | Restore quorum; see [node failure](node-failure.md). |
| A request returns a stale fence | Catalog generation and route identity changed under it. | Re-observe the catalog and rebuild from the original request. |
| Admission or resource errors increase | Connection, read, frame, result, and WAL bounds. | Check [limits](../reference/limits.md); release held resources before raising a bound. |
| Metrics stop changing | Process identity, `collection_faults`, sample coverage. | Distinguish a cached sample from a fresh observation. |
| Reopen reports corruption or an identity mismatch | Same build, complete recovery unit, original paths and keys. | Preserve the files; follow the recovery procedure for that layer. |
| Replica moves are slow | Migration budget throttling and pressure pause. | See [migration](migration.md#observability). |

## Cluster control errors

| Output | Meaning | Action |
| --- | --- | --- |
| `load profile: clustercontrol: invalid auth-client profile` | Profile missing, non-canonical, or with a bad field. | Check the [profile shape](scaling.md#prerequisites). |
| `error=gatewayruntime: cluster control authorization denied` | The credential lacks `topology`, or `membership` for a mutation. | Grant the capability in the policy. |
| `error=gatewayruntime: cluster control is unavailable` | The frontend has no scaling backend or cannot read the catalog. | Target the controller-configured frontend; check catalog quorum. |
| `--wait exceeds the 24-hour bound` | `--wait` too long. | Use at most `24h`. |
| Response with blockers | Operation cannot yet finish. | Read the [blocker table](scaling.md#read-blockers). |
| Transport error or timeout | Response lost; the operation may exist. | Rerun with the same `--request-id`, or poll `status --operation`. |

## Collect evidence

Record the build from the checkout that produced the binaries:

```sh
git rev-parse HEAD
git status --short
go version
go env GOOS GOARCH GOEXPERIMENT
```

Then collect:

- the launch command and failure time;
- `--diagnostics-on-exit` output (last 64 KiB per node);
- a node diagnostic: send `SIGUSR1` to the exact `vibedb-shard serve-node`
  PID and read `node-<n>/rf3-diagnostics.json`; see
  [observability](observability.md#collect-a-physical-node-diagnostic);
- affected node, group, operation, and request identities.

Keep private keys, certificates, data, and credential-bearing manifests out of
shared reports. Every cluster root contains keys.

## Resolve an uncertain write

A disconnect can happen after a durable commit. The recovery depends on how
the write was submitted:

- **Embedded database:** follow the close and reopen rules in
  [durability and recovery](../durability.md).
- **Durable native request (`exec_batch`):** keep the canonical bytes, issuer
  lane, sequence, and request ID. Replay that exact request; acknowledgement
  is a separate step.
- **SQL over pgwire:** the gateway propagates an unknown commit outcome as an
  error. The SQL connection carries no request identity you can replay, so
  check the row's state with a read before writing again.
- **Backup, schema rollout, scaling:** reuse the same backup ID, plan, or
  request ID; the replicated journal decides the next step.

See [distributed retries](distributed.md#retries-and-outcome-unknown) and the
[protocol reference](../reference/protocols.md).

## Recover at the right layer

| State to recover | Procedure |
| --- | --- |
| Closed embedded directory | [Embedded backup and restore](embedded-backup.md) |
| Damaged local store file | [Verify and salvage](verification.md) |
| Lost or untrusted RF3 node | [Node failure and replacement](node-failure.md) |
| RF3 backup into fresh identities | [Distributed restore](backup-restore.md#restore-into-fresh-identities) |
| Interrupted schema installation | [Resume the same rollout](schema-rollouts.md#recover-an-interrupted-rollout) |
| Interrupted scaling operation | [Poll or resubmit with the same request ID](scaling.md#retry-and-outcome-rules) |

A structurally readable file is not enough to let a replica serve.

## Limitations

- Node processes write unstructured text to stderr. The launcher captures it
  in memory and keeps only the last 64 KiB per node; nothing is written to a
  log file.
- There is no single command that reports cluster health.

## Source map

| Concern | Source |
| --- | --- |
| Launcher validation and supervision | [cluster_dev.go](../../cmd/vibedb/cluster_dev.go), [cluster_dev_physical.go](../../cmd/vibedb/cluster_dev_physical.go) |
| Control CLI | [cluster_control.go](../../cmd/vibedb/cluster_control.go) |
| Node diagnostics | [rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go) |
| Gateway runtime | [internal/gatewayruntime/](../../internal/gatewayruntime/) |
