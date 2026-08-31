# Operations

> [!CAUTION]
> These are development and qualification procedures for one exact build, not
> production runbooks. Commands, wire protocols, and disk state may break at
> any commit. There is no supported upgrade path, SLO, or production topology;
> use only disposable or recoverable data.

## Embedded data

| Task | Guide |
| --- | --- |
| Choose acknowledgement and recovery semantics | [Durability](../durability.md) |
| Inspect an offline store or database | [Verify, salvage, and repack](verification.md) |
| Understand the files you must preserve | [On-disk format](../format.md) |

Stop and close writers before copying data. Primary files, recovery journals,
transaction logs, catalogs, and keys form one recovery unit. Writer locks do
not make a live raw-file copy coherent.

## Distributed development

| Task | Guide |
| --- | --- |
| Start disposable RF3 locally | [Local cluster](local-cluster.md) |
| Understand serving, failure, and retry behavior | [Distributed runtime](distributed.md) |
| Collect and restore a certified group vector | [Backup and restore](backup-restore.md) |
| Exercise one exact schema successor | [Schema rollouts](schema-rollouts.md) |
| Read bounded metrics and evidence | [Observability](observability.md) |
| Run the Kind RF3 lane | [Kubernetes qualification](kubernetes.md) |

The checked-in commands expose real replication and recovery primitives, but
they do not supply production PKI, discovery, live-capacity integration,
upgrade orchestration, automated repair, or a general operator control plane.

## Before any destructive operation

1. Confirm the exact commit and build identity on every participant.
2. Resolve exact file, group, allocation, catalog, and operation identities.
3. Preserve the complete original state in a separate trusted location.
4. Use a quiescent source unless the protocol explicitly certifies a live cut.
5. Treat a lost response after send as potentially committed.
6. Verify the resulting state before replacing the original.

Use the [CLI reference](../reference/cli.md) for command syntax and
[current status](../status.md) for known defects.
