# Back up and restore RF3 data

> [!CAUTION]
> This is a development-only, same-build recovery workflow. Commands, manifests,
> disk grammar, wire grammar, and authorization rules can change or break at any
> commit. It is not a production backup service, upgrade path, certificate
> provisioner, or disaster-recovery guarantee.

## Contract at a glance

- A live backup is a catalog-authorized **vector of exact per-group Raft cuts**.
  It is not a simultaneous cluster snapshot or a wall-clock snapshot.
- The backup repository publishes every authenticated artifact first and the
  cluster certificate last. Only the certificate is publication authority.
- Restore verifies the complete vector and creates fresh cluster, group, member,
  node, store, and process-incarnation identities. Source serving authority is
  never copied.
- Constructed and adopted roots remain non-serving. Only a separately observed
  target-catalog activation witness can authorize traffic.
- The operation embeds the current build grammar. Cross-build restore,
  migration, rolling upgrade, downgrade, and mixed-version recovery are not
  supported.

## Authority boundaries

| Action | Required authority | Authority it does not grant |
| --- | --- | --- |
| Export a live group cut | Authenticated shard-control peer with `backup` capability | data read/write, schema, topology, membership, or serving |
| Start or inspect a gateway backup | Authenticated gateway caller with `backup`; gateway identity also needs `backup` for catalog lifecycle writes | selection of arbitrary groups or repository paths |
| Build restore roots | Exact backup certificate, complete artifact vector, sealed restore operation, and private local paths | membership, routing, ownership, or serving |
| Activate restored roots | Gateway identity with `restore_activate`, exact target catalog and policy, authenticated replica controls | backup, ordinary data, schema, or membership authority |

TLS identity and policy generation are part of these checks. Development
plaintext is not accepted for the composed backup/restore path.

## Configure live backup

Backup is available only on a gateway using a replicated catalog, TLS, stable
catalog-session identity, and a replica-control manifest. Add an absolute,
server-local repository and explicit retention ceilings:

```text
vibedb-gateway serve <replicated-catalog-and-TLS-options> \
  -replica-control-manifest /etc/vibedb/replica-control.vibejson \
  -backup-repository /var/lib/vibedb/backups \
  -backup-max-backups 16 \
  -backup-max-artifacts 4096 \
  -backup-max-artifact-bytes 68719476736 \
  -backup-max-disk-bytes 274877906944
```

The gateway refuses a relative or unclean repository path, static catalog,
plaintext serving, missing replica controls, invalid limits, or insufficient
local authority.

## Start and inspect a backup

Send canonical newline-delimited `vibejson` to the authenticated gateway
listener. `backup_id` is a nonzero 32-byte idempotency key encoded as 64
lowercase hexadecimal characters:

```json
{"op":"backup","backup_id":"0101010101010101010101010101010101010101010101010101010101010101"}
```

After a disconnect, timeout, gateway restart, or outcome-unknown catalog write,
retry with the **same** `backup_id`. To inspect replicated state without starting
new work:

```json
{"op":"backup_status","backup_id":"0101010101010101010101010101010101010101010101010101010101010101"}
```

The response echoes `backup_id` and returns numeric `backup_stage` plus a
32-byte `backup_proof`. Stage numbers are unstable internal protocol values; do
not build external retention policy around them. Unknown fields, reordered
fields, uppercase/noncanonical identifiers, SQL fields, parameters, or result
budgets are rejected.

## What backup actually does

1. The replicated catalog supplies one immutable, complete inventory, including
   catalog, request-ledger, and data groups.
2. The controller resolves a current leader for every group over authenticated
   shard control.
3. Each leader reaches a linearizable `ReadIndex` cut and pins that immutable
   snapshot.
4. The exporter scans the same pinned snapshot once to compute exact artifact
   geometry and hash, sends the header, then scans it again to stream bytes.
5. The repository validates and fsyncs every operation-scoped artifact draft.
6. The repository publishes and fsyncs the certificate **last**. The catalog
   lifecycle then advances to the certified/exported cut.

The double scan is deliberate: for a successful artifact,
`backup_scan_bytes = 2 × backup_logical_bytes`. It is read amplification, not a
second repository copy. The repository streams directly to its draft files.

There is no cross-group common applied index. The certificate binds the catalog
generation and ordered group inventory to each group's own snapshot index,
term, lineage, relation manifest, artifact hash/size, and artifact-manifest
digest. Treat it as a recovery vector, not a globally consistent analytical
snapshot.

### Failure and retry behavior

- Before certificate publication, artifacts and temporary files have no
  authority; repository recovery removes orphan state.
- After certificate publication, exact replay is idempotent. A replacement
  gateway settles the replicated lifecycle from the certificate without
  rescanning or copying certified artifacts.
- A failed catalog compare-and-swap is outcome-unknown until the replicated
  operation is reread. Local phase memory is never authoritative.
- Release removes publication authority durably before reclaiming artifact
  bytes. Do not delete repository files by hand.
- A backup request may fail because leadership moved, bounds were reached,
  authorization changed, a deadline expired, the repository filled, or an
  artifact/certificate check failed. Retry the same identity after fixing the
  cause; do not mint a new ID to bypass retained state.

## Restore into fresh identities

Restore is a composed workflow, not one provisioning command. Inputs are built
by the `clusterrestore` APIs and strict canonical manifests; the tools do not
invent production PKI or topology.

### 1. Seal the target plan

Verify every artifact against the certificate, choose a new cluster identity,
plan one fresh target group with exactly three fresh replicas per source cut,
construct the generation-one target catalog and exact per-group schema set, and
seal them into one operation. The target cluster/incarnation, group/shard IDs,
node IDs, store IDs, and replica roots must not reuse the source identities.

The schema set carries the exact target policy bytes, catalog digest, ordered
DDL, relation/index descriptors, validation profile, and apply bounds. Restore
does not infer missing schema from rows and does not translate between build
grammars.

### 2. Build each group

For every group ordinal:

```text
vibedb-operator restore-group \
  -root /absolute/private/restore \
  -template /absolute/private/restore-schema.vibejson \
  -operation /absolute/private/restore-operation.bin \
  -artifact /absolute/private/certified-artifact \
  -group-ordinal 0
```

The command materializes and authenticates the artifact, creates or resumes
three fresh SQL roots, verifies source relation/image evidence, rebinds and
rehashes the target image under the fresh validation profile, and emits one
root witness. The catalog group discards source catalog rows and installs only
the sealed fresh catalog projection. It does not activate a replica.

Private roots must be canonical directories with restrictive permissions.
Symlinks, substituted artifacts, mismatched manifests, partial identity reuse,
or divergent replica results fail closed. Crash receipts are resumable; repeat
the exact operation and paths.

### 3. Adopt each restored replica

After provisioning credentials for the fresh identities:

```text
vibedb-operator adopt-restore \
  -manifest /absolute/private/prepare-restored-member.vibejson
```

Adoption authenticates the operation/roster, settles the restored SQL and apply
checkpoint, creates or resumes staged WAL state, and publishes the ordinary
`serve-rf3` manifest last. Neither a successful command nor that manifest grants
client-serving authority.

### 4. Start closed, then activate

Start every adopted `serve-rf3` process. Restored reads and writes must remain
closed. Then run:

```text
vibedb-gateway restore-activate \
  -manifest /absolute/private/restore-activation.vibejson
```

The activation manifest binds the same sealed operation/schema set, target
catalog and policy, staging and activation journals, fresh identities, TLS,
three control endpoints per group, deadlines, retries, and byte/concurrency
limits. Paths must be absolute canonical regular files/directories.

Activation installs every group, conditionally writes one immutable activation
row through target-catalog RF3, then performs a separate linearizable observation
of that exact row. A successful proposal response or local `serving.permit` file
is not enough. Only the observed catalog witness can mint the complete
per-replica grant vector.

Grants bind operation, catalog witness, group, member, node, store, and current
process incarnation. They are process-local. A restored replica restart closes
serving again until the gateway re-observes catalog activation and reinstalls the
exact grant. On interruption, rerun the byte-identical activation manifest with
the same operation and retained journals; never delete journals or rotate
session identities to force progress.

## Unsupported recovery shortcuts

- A learner snapshot is target-bound replica-movement state, not a cluster
  backup.
- A live filesystem copy is not a backup. For an offline same-identity experiment,
  stop all gateways and members, copy every root/WAL/journal/key/policy/certificate/
  manifest, and restart only the exact same build in an isolated network.
- Do not bootstrap over a nonempty root, copy member identity into a fresh
  cluster, treat one group's snapshot as a database snapshot, or manually create
  a serving grant.
- No recovery-time objective, recovery-point objective, key-management service,
  cross-build archive, or mixed-version procedure is provided.

## Source map

| Boundary | Source |
| --- | --- |
| Complete group vector and certificate grammar | [`internal/clusterbackup/certificate.go`](../../internal/clusterbackup/certificate.go) |
| Linearizable export and double scan | [`internal/clusterbackupservice/service.go`](../../internal/clusterbackupservice/service.go), [`internal/clusterbackup/source_export.go`](../../internal/clusterbackup/source_export.go) |
| Certificate-last repository and crash recovery | [`internal/clusterbackup/repository.go`](../../internal/clusterbackup/repository.go), [`internal/clusterbackup/live_collect.go`](../../internal/clusterbackup/live_collect.go) |
| Replicated backup lifecycle and resume | [`gateway/backup_operation.go`](../../gateway/backup_operation.go), [`gateway/backup_repository_coordinator.go`](../../gateway/backup_repository_coordinator.go) |
| Gateway request grammar and runtime | [`cmd/vibedb-gateway/serve_backup.go`](../../cmd/vibedb-gateway/serve_backup.go), [`cmd/vibedb-gateway/backup_operator.go`](../../cmd/vibedb-gateway/backup_operator.go) |
| Non-serving restore admission | [`internal/clusterbackup/restore.go`](../../internal/clusterbackup/restore.go), [`internal/clusterbackup/staging_root.go`](../../internal/clusterbackup/staging_root.go) |
| Fresh target operation and root installation | [`internal/clusterrestore/operation.go`](../../internal/clusterrestore/operation.go), [`internal/restoreservice/installer.go`](../../internal/restoreservice/installer.go) |
| Catalog observation and serving grants | [`internal/clusterrestore/controller.go`](../../internal/clusterrestore/controller.go), [`internal/clusterrestore/serving.go`](../../internal/clusterrestore/serving.go), [`internal/clusterrestore/serving_grant.go`](../../internal/clusterrestore/serving_grant.go) |
| Activation/adoption manifests | [`cmd/vibedb-gateway/restore_activate_manifest.go`](../../cmd/vibedb-gateway/restore_activate_manifest.go), [`cmd/vibedb-shard/adopt_restored_rf3.go`](../../cmd/vibedb-shard/adopt_restored_rf3.go) |
