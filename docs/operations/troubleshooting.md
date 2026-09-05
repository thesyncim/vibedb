# Troubleshoot a development cluster

[Documentation](../README.md) / [Operations](README.md) / Troubleshooting

Identify the failed phase before changing persistent state. Keep the exact
build, original files, operation identity, and relevant logs available.
These checks cover the local launcher and authenticated RF3 services.

## Start with the symptom

| Symptom | First check | Next step |
| --- | --- | --- |
| Launcher rejects the root | Absolute path, retained manifest format, replication factor, physical-node count, and SQL endpoints. | Use the original configuration to reopen; use a fresh root for a different topology. |
| Preparation reports `strict physical allocation proof is unsupported` | Host platform and filesystem support for sealed journals. | Use Linux with supported allocation, including a suitable data volume in containers; native macOS cannot prepare this RF3 profile. |
| A child never becomes ready | Child-log tails, port conflicts, filesystem errors, and manifest validation. | Restart the launcher with `--diagnostics-on-exit`; resolve the reported refusal. |
| psql cannot connect | Readiness, literal loopback address, enabled SQL endpoint, user `local`, database `vibedb`, and `sslmode=disable`. | Retry the connection from the [local tutorial](local-cluster.md#3-write-and-read-a-row). |
| SQL connects but a statement fails | Dialect support and the adapter executing it. | Reduce the statement to a form in the [SQL reference](../reference/sql.md). |
| A request returns a stale fence | Catalog generation and exact route, ownership, schema, and replica identities. | Re-observe the catalog and rebuild from the original request. |
| A write times out or disconnects | Whether submission occurred and which write domain owns the retained identity. | Resolve that request; do not assume rollback or generate a new identity. |
| Reads stop after a node failure | Reachable voters and the affected group's leadership/apply progress. | Restore quorum; one RF3 voter cannot elect or commit. |
| Admission or capacity errors increase | Effective request, connection, query, WAL, and storage limits. | Inspect [limits](../reference/limits.md) and release held resources before changing bounds. |
| Metrics stop changing | Process identity, collection faults, and sample coverage. | Distinguish a cached sample from a fresh observation. |
| Reopen reports corruption or an identity mismatch | Matching build, complete recovery unit, original paths, and keys. | Preserve the files and follow the relevant recovery procedure. |

## Collect useful evidence

Record the commit and build settings from the checkout that produced the binary:

```sh
git rev-parse HEAD
git status --short
go version
go env GOOS GOARCH GOEXPERIMENT
```

Record the launch command, failure time, affected node/group identities, and
bounded logs separately. Keep private keys, certificates, data, and complete
credential-bearing manifests out of public reports.

[Observability](observability.md) explains authenticated gateway metrics and
physical-node diagnostics. Counter changes are meaningful only when source
identity and inventory remain comparable. A telemetry aggregate cannot prove
quorum or authorize a recovery action.

## Resolve an uncertain write

A disconnect can occur after durable commit. Native embedded, direct RF3, and
coordinated RF3 writes have different recovery mechanisms:

- For embedded persistence errors, follow the close/reopen rules in
  [durability and recovery](../durability.md).
- For a durable gateway request, retain its canonical bytes, issuer domain,
  sequence, and request ID. Retry or inspect that exact request using its
  protocol; acknowledgement and cleanup are separate steps.
- For backup or schema rollout, reuse the operation ID and original plan.
  Their replicated journals determine the next step.

See [distributed retries](distributed.md#retries-and-outcome-unknown) and the
[protocol reference](../reference/protocols.md). An application SQL connection
alone is not a general request-recovery client.

## Recover at the right layer

| State to recover | Procedure |
| --- | --- |
| Closed embedded directory | [Embedded backup and restore](embedded-backup.md) |
| Damaged local collection image | [Verify and salvage](verification.md) |
| RF3 backup vector into fresh identities | [Distributed restore](backup-restore.md#restore-into-fresh-identities) |
| Interrupted schema installation | [Resume the same rollout](schema-rollouts.md#recover-an-interrupted-rollout) |
| Replica loss and membership replacement | [Replica replacement design](distributed.md#rf3-quorum-and-replica-replacement) |

A structurally readable file is not enough to let a replica serve. Keep
membership, recovery lineage, and catalog activation checks in the recovery
path. Avoid editing manifests or copying identities to bypass a refusal.

## Source map

- [Launcher validation and diagnostics](../../cmd/vibedb/cluster_dev.go).
- [Physical-node validation](../../cmd/vibedb/cluster_dev_physical.go).
- [Node diagnostics](../../cmd/vibedb-shard/rf3_diagnostics.go).
- [Gateway runtime](../../internal/gatewayruntime/).
- [Serving and recovery](../../internal/raftmember/).
