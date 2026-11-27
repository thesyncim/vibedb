# Back up and restore RF3 data

[Documentation](../README.md) / [Operations](README.md) / Backup and restore

Export a running RF3 cluster as a certified **vector of per-group Raft cuts**
(not a global point-in-time snapshot), and restore it into a fresh cluster with
brand-new identities. Same-build only; cross-build restore is unsupported. See
[backup internals](../design/backup-restore.md) for protocol design.

## Configure live backup

Requires a replicated-catalog mTLS gateway with `-replica-control-manifest` and
a server-local absolute repository (configured automatically on node 1 by
`vibedb cluster dev` at `<root>/backups`):

```sh
vibedb-gateway serve <replicated-catalog-and-TLS-options> \
  -replica-control-manifest /etc/vibedb/replica-control.vibejson \
  -backup-repository /var/lib/vibedb/backups \
  -backup-max-backups 16 \
  -backup-max-artifacts 4096 \
  -backup-max-artifact-bytes 68719476736 \
  -backup-max-disk-bytes 274877906944
```

## Start and inspect a backup

Send canonical newline-delimited JSON over the gateway mTLS listener with a
`backup`-capable principal (`backup_id` is a 64-hex idempotency key; always
reuse the **same** ID on retry):

```json
{"op":"backup","backup_id":"0101010101010101010101010101010101010101010101010101010101010101"}
{"op":"backup_status","backup_id":"0101010101010101010101010101010101010101010101010101010101010101"}
```

Each group leader takes a `ReadIndex` cut and streams a two-pass artifact
(`backup_scan_bytes = 2 × backup_logical_bytes`). The repository publishes the
**certificate last**; orphan drafts before certificate publication have no
authority, and replay after certificate publication is idempotent. Never delete
repository files by hand.

## Restore into fresh identities

Restore requires fresh cluster, group, member, node, store, and incarnation IDs
(never reuse source serving authority). Build the sealed operation and schema
set via `internal/clusterrestore`, then run:

1. **Materialize each group ordinal (0..N-1):**

   ```sh
   vibedb-operator restore-group \
     -root /abs/private/restore \
     -template /abs/private/restore-schema.vibejson \
     -operation /abs/private/restore-operation.bin \
     -artifact /abs/private/certified-artifact \
     -group-ordinal 0
   ```

2. **Adopt each restored replica** (after provisioning fresh TLS/WAL keys):

   ```sh
   vibedb-operator adopt-restore -manifest /abs/private/prepare-restored-member.vibejson
   ```

3. **Start all `serve-rf3` processes** (they start **closed** to traffic), then
   **activate** with a `restore_activate`-capable gateway:

   ```sh
   vibedb-gateway restore-activate -manifest /abs/private/restore-activation.vibejson
   ```

Activation writes one activation witness into the target catalog RF3 group,
confirms it via a separate `ReadIndex` read, and installs process-local serving
grants (a restarted replica re-observes before serving). On interruption, rerun
the byte-identical manifest and journals.

## Rules and limitations

- A learner snapshot or live filesystem copy is **not** a backup.
- Copying backups off-host, scheduling, and target plan assembly remain
  operator-provided. No RPO/RTO or cross-build restore is provided.

## Source map

- Backup: [internal/clusterbackup/repository.go](../../internal/clusterbackup/repository.go), [gateway/backup_operation.go](../../gateway/backup_operation.go), [internal/gatewayruntime/serve_backup.go](../../internal/gatewayruntime/serve_backup.go)
- Restore: [internal/clusterrestore/controller.go](../../internal/clusterrestore/controller.go), [cmd/vibedb-shard/adopt_restored_rf3.go](../../cmd/vibedb-shard/adopt_restored_rf3.go)
- CI gate: [`restore-rf3-external.yml`](../../.github/workflows/restore-rf3-external.yml)
