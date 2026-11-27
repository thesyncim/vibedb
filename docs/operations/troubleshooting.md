# Troubleshoot a cluster

[Documentation](../README.md) / [Operations](README.md) / Troubleshooting

Identify the failed phase before touching persistent files. Preserve the exact
build, original directories, operation/request IDs, and logs.

## Common failures

| Symptom / message | Cause | Action |
| --- | --- | --- |
| `invalid local development cluster` | Relative/unclean root, nonempty first start, or mismatched `--replicas`/`--physical-nodes`/build | Use a clean absolute path; match original flags or start a fresh root |
| `cluster root is too long for the PostgreSQL DDL Unix socket` | `<root>/node-1/pg-ddl.sock` exceeds OS socket path limit | Use a shorter root (≤80 chars) |
| `PostgreSQL endpoints must be distinct literal loopback addresses` | Non-loopback, `localhost`, `0.0.0.0`, or duplicate | Use distinct `127.0.0.1:<port>` addresses |
| `development cluster readiness timeout` / `physical node <n> exited` | Child failed to start or crashed | Rerun with `--diagnostics-on-exit` for last 64 KiB of each node's stderr |
| `psql` cannot connect | Node not ready, wrong port/user, or TLS mismatch | Check readiness line; loopback dev uses `user=local db=vibedb sslmode=disable` |
| `cluster control authorization denied` | Principal lacks `topology` (or `membership` for mutations) | Update `authorization-policy.vibejson` via `vibedb cluster policy grant` |
| `cluster control is unavailable` | Frontend has no scaling backend or lost catalog quorum | Target active controller frontend; check catalog quorum |
| Scaling response has blockers | In-flight moves, sessions, or readiness pending | Check [blocker table](scaling.md#read-blockers) |
| Reads/writes stall after node loss | Group dropped below 2 reachable voters | Restore quorum ([node failure](node-failure.md)); never force membership |
| Stale fence refusal | Catalog/route changed under request | Re-observe catalog and rebuild from original request |
| Admission / watermark errors | Frame, result, or `ResourceGuard` memory/disk cap hit | Check `/metrics`, [limits](../reference/limits.md), and release open handles |
| Slow replica moves | Migration budget or foreground pressure throttle | Check `vibedb cluster status --json` (`budget` object) |

## Resolve an uncertain write

A disconnect or timeout may occur after durable commit:

- **Embedded:** Close, reopen, and inspect state ([durability](../durability.md)).
- **Native `exec_batch`:** Replay the exact canonical bytes and request ID; ACK
  once settled.
- **SQL over `pgwire`:** Read back the row by application key before retrying
  (SQL connections carry no client replay ID).
- **Backup / schema rollout / scaling:** Resubmit with the exact same
  `backup_id`, rollout plan, or `--request-id`.

## Collect diagnostics

```sh
git rev-parse HEAD && git status --short && go version && go env GOOS GOARCH GOEXPERIMENT
kill -USR1 <serve-node-pid>   # writes <node-dir>/rf3-diagnostics.json
```

Strip private keys, certificates, and credential manifests before sharing
reports. Use the recovery procedure matching the failed layer:
[embedded backup](embedded-backup.md), [verification](verification.md),
[node failure](node-failure.md), [RF3 restore](backup-restore.md#restore-into-fresh-identities),
[schema rollouts](schema-rollouts.md#recover-an-interrupted-rollout), or
[scaling](scaling.md#retry-and-exit-rules).

## Source map

- [cmd/vibedb/cluster_dev.go](../../cmd/vibedb/cluster_dev.go) · [cmd/vibedb/cluster_control.go](../../cmd/vibedb/cluster_control.go) · [cmd/vibedb-shard/rf3_diagnostics.go](../../cmd/vibedb-shard/rf3_diagnostics.go)
