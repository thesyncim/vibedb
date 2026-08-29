# Back up and restore distributed data

VibeDB ships authenticated live RF3 backup export through the gateway and
commands to construct, adopt, and activate fresh-identity restore replicas.
A constructed replica is not serving authority. The activation and qualification boundary
below still applies. Do not copy live files or start an uncertified staging
root.

The shipped RF3 shard process now exposes a target-free live snapshot stream
over its mutually authenticated control listener. Only an independent
`backup` principal is admitted. The serving leader obtains a quorum
`ReadIndex`, pins the reached apply cut, scans it once to derive artifact
geometry and hash, then scans the same immutable cut again while streaming.
It never invents learner member/store/incarnation identity.

The `internal/clusterbackup` boundary certifies one complete catalog group
inventory against exact per-group snapshot index/term, lineage, relation
manifest, artifact hash/bytes, and artifact-manifest digest. The canonical
certificate has a 16 MiB total byte bound; group count is derived from that
bound rather than an arbitrary participant limit. Restore admission requires
the complete ordered verified-artifact vector and returns only a non-serving
staging permit for a new cluster identity. Its repository streams leader
exports into operation-scoped drafts without a second artifact copy and
publishes the certificate last. It cannot mint a member, store,
ownership epoch, route generation, or membership grant.

Authorization uses the independent `backup` policy capability. A backup
principal does not acquire data read/write, schema, topology, membership,
delegation, transaction-recovery, request-ledger, execution-pin, or serving
authority. The gateway backup controller rechecks backup authority whenever it
conditionally advances the catalog-RF3 operation record.

## Run and inspect a live backup

Start the gateway with its normal replicated-catalog and replica-control
configuration plus an absolute, server-local repository and explicit retention
bounds:

```text
vibedb-gateway serve ... \
  -backup-repository /var/lib/vibedb/backups \
  -backup-max-backups 16 \
  -backup-max-artifacts 4096 \
  -backup-max-artifact-bytes 68719476736 \
  -backup-max-disk-bytes 274877906944
```

The gateway refuses this mode with plaintext serving, a static catalog, a
relative or unclean repository path, a missing replica-control manifest, or a
local identity without `backup` authority. The authenticated gateway endpoint
accepts exactly these canonical `vibejson` requests:

```json
{"op":"backup","backup_id":"0101010101010101010101010101010101010101010101010101010101010101"}
{"op":"backup_status","backup_id":"0101010101010101010101010101010101010101010101010101010101010101"}
```

The caller also needs `backup` authority. `backup_id` is the caller's stable
idempotency identity: retry it unchanged after disconnect or gateway restart.
The response returns the same fixed identity, the replicated numeric lifecycle
stage, and a fixed 32-byte proof encoded as lowercase hexadecimal. Unknown
fields, reordered fields, mixed SQL/query fields, noncanonical IDs, and
oversized requests are rejected.

Before publication, the gateway observes the current leader of every group in
one immutable catalog inventory over authenticated shard control, streams each
bounded artifact directly to operation-scoped repository drafts, fsyncs the
complete artifact set, and publishes the certificate last. A restart resumes
from the replicated operation plus repository certificate and does not rescan
or recopy already certified shard artifacts.

## Why replica snapshots are not backups

Learner bootstrap creates an authenticated snapshot artifact for one exact RF3
group and one enrolled target incarnation. Its descriptor, catalog generation,
membership grant, snapshot fence, repository cursor, release, and abandonment
records belong to replica movement. Reusing that artifact as a backup would
silently omit required cluster properties:

- one catalog-authorized inventory covering the catalog, request ledger, and
  every data group;
- a durable cut for each group and the catalog generation that names those
  cuts;
- transaction/request-ledger recovery state needed to resolve in-flight or
  acknowledged work;
- encrypted WAL key references and authorization material needed to open the
  exact images;
- an independent restore identity that cannot impersonate the old member or
  consume a learner grant;
- bounded retention and garbage collection that cannot delete an artifact
  before the backup manifest is durably complete.

There is no global wall-clock snapshot contract. A live backup publishes a
vector of exact per-group Raft cuts through catalog authority. This matches the
database's clock-free consistency model: Raft order is authoritative inside a
group, while a catalog-bound vector defines the cross-group recovery cut.

## Construct restore replicas

Restore construction requires a certified backup artifact, an authenticated
binary restore operation, and an exact canonical `restore-schema.vibejson`
schema set. The set has one dense ordinal for every operation group. Each
schema contains ordered DDL, the base relation, explicit global-index
descriptors, and apply bounds, so catalog, ledger, and data groups may differ.
The set also embeds the canonical fresh generation-one catalog and exact
authorization policy bytes. The complete set's digest must match the target
catalog digest in the operation, and the policy has its own operation-bound
digest. The importer does not infer schema or indexes from source rows and
does not migrate between builds.

The operation supplies fresh target member, node, store, and group identities.
Operation assembly plans those identities with `PlanFreshTargets`, constructs
the exact target catalog and schema set, then seals them with `NewOperation`.
This avoids a circular identity/catalog hash. These are explicit builder APIs,
not an automatic certificate provisioner.

For each operation group ordinal, run:

```text
vibedb-operator restore-group \
  -root /absolute/private/restore \
  -template /absolute/private/restore-schema.vibejson \
  -operation /absolute/private/restore-operation.bin \
  -artifact /absolute/private/certified-artifact \
  -group-ordinal 0
```

This command streams a certified singleton or base-plus-global-index relation
bundle into three fresh SQL roots and returns one exact root witness. It
verifies the source and destination relation-manifest digests, strips source
serving authority, and retains bounded crash-resumable receipts. In the catalog
group it discards every source row and installs only the sealed fresh head,
head witness, genesis proof, and restore-policy projection. Old routes,
ownership, operation records, and catalog history do not survive that import.
It does not publish catalog activation or activate any replica.

The logical SQL relation manifest is not the replicated machine manifest.
The machine digest also binds the schema/index definitions and validation
profile. Restore verifies the artifact against the exact source machine
manifest under its authenticated source binding, then computes a separate
fresh machine manifest under the target binding. The sealed target catalog
descriptor must match that fresh digest before installation. Source and target
machine digests must not be treated as interchangeable.

Canonical image hashes also include their validation profile. Even unchanged
base/index rows are verified once per group against the source artifact and
rehashed under the fresh target profile with bounded memory. The catalog
instead hashes its fresh projection after discarding source rows. A sealed
retry revalidates the source artifact, fresh image digest, and exact receipt
machine digest without reopening live SQL/WAL stores. This is identity-domain
rebinding, not cross-build schema migration.

After provisioning certificates for the operation's fresh target identities,
adopt each replica with its own exact preparation manifest:

```text
vibedb-operator adopt-restore \
  -manifest /absolute/private/prepare-restored-member.vibejson
```

The operator invokes `vibedb-shard adopt-restored-rf3`. Adoption authenticates
the retained operation and roster, creates or resumes the certified staged
WAL, settles the restored SQL/apply checkpoint, and publishes the serving
manifest last. It retains the `restore_preparing` receipt, but restored-root
classification comes from the authenticated immutable bootstrap and survives
later snapshots. Removing the receipt cannot bypass the serving fence.
Neither a successful command nor the presence of `serve-rf3.vibejson` permits
client traffic. Retrying construction after adoption verifies the sealed
live-root binding read-only instead of replacing the active SQL or WAL.

## Activate only through target catalog authority

The gateway activation driver requires independent `restore_activate`
authority. It installs the complete group vector, conditionally writes one
immutable `restore/activation` row through the target catalog RF3 group, and
then performs a separate linearizable observation of that row. A proposal
response alone is not an activation proof. Before activation, the target
catalog admits only this exact operation-bound row and its bounded session
lifecycle under both topology and restore authority. It cannot serve ordinary
catalog reads, DDL, data, or other topology mutations through that exception.

Only that observed witness can mint the complete, group-major vector of
per-replica serving grants. The driver installs every grant before reporting
activation success. Each grant binds the operation, catalog witness, group,
member, node, store, and process incarnation. A two-phase authenticated
handshake binds the grant to the target's current restart incarnation.
`serve-rf3` refuses restored client reads and writes until its exact grant is
installed over authenticated shard control. Grants are deliberately
process-local: restarting a restored
process closes serving until the catalog-observed grant is reinstalled.

After constructing and adopting every replica, start their `serve-rf3`
processes. They must remain closed to ordinary client traffic. Then run:

```text
vibedb-gateway restore-activate \
  -manifest /absolute/private/restore-activation.vibejson
```

The canonical manifest binds the operation and schema-set files, verified
staging root, separate activation journal root, target catalog file, exact
authorization policy and TLS identity, two distinct durable catalog-session
identities/journals, all group roots and three control endpoints per group,
repository byte limits, timeout, retry, and connection bounds. The target
catalog must canonically equal the catalog sealed into the schema set. All
paths must be canonical and absolute. Input files must be regular files, not
symlinks.
See the [manifest definition](../../cmd/vibedb-gateway/restore_activate_manifest.go)
and [canonical fixture](../../cmd/vibedb-gateway/restore_activate_test.go).

The command revalidates the complete operation-bound inputs, resumes exact
sealed roots without rewriting live state, publishes and independently
observes target-catalog activation, and broadcasts the complete grant vector.
Success emits canonical `operation`, `groups`, and `catalog_witness` fields.
After failure or a replica restart, rerun the same manifest with the same
operation and retained journals. Do not mint replacement session identities or
delete journals to force a retry.

This command activates supplied certified state. It is not a one-command
backup-to-new-cluster provisioner or a production certificate issuer.

## Restore qualification boundary

A complete backup and restore path requires external kill/partition proof of:

- a replicated backup intent and immutable group inventory;
- bounded parallel group cuts pinned without stopping foreground traffic;
- catalog publication only after every exact artifact is durable;
- controller crash/restart and idempotent resume at every phase;
- transaction and request-ledger recovery after restore;
- restore into new member/store incarnations without copied-owner authority;
- key-loss and unauthorized-operator refusal;
- retention release only after an authenticated completed or abandoned backup;
- bounded foreground p99.9 impact, memory, network, WAL retention, and artifact
  space amplification.

The catalog-RF3 backup operation, live collector, bounded repository, public
request/status API, artifact verifier, fresh-identity root builder, staged-WAL
adopter, one-time catalog activation, and transient serving gate are composed.
External process exit before certificate publication and stalled
network-stream gates cover fail-closed backup recovery. Mandatory Ubuntu gates
exercise six activation-publication crash cuts and concrete three-root
installation. A separate external gate boots six restored RF3 processes for
independent catalog and base/global-index data groups. It runs the actual
`restore-activate` command, checks refusal before grants and after marker
removal, verifies fresh catalog projection and restored base/index data,
preserves a new acknowledged write across leader kill, and requires
re-observation and regrant after a process restart under hard total, write, and
failover latency bounds plus aggregate RSS, storage, and WAL bounds. That gate
uses actual target-catalog RF3 sessions and a separate ReadIndex observation
before the gateway broadcasts serving
grants. A verified Ubuntu receipt for tested base commit
`4672dbd67ee2e49291d410cb34905aafe1e24135` records three unskipped
restored-RF3 runs (`count=3`). Qualification remains Partial because this is
not a production recovery, key-management, or cross-build migration claim.

## Offline same-identity recovery

For disaster-recovery experiments, an offline copy is a separate option:

1. Stop client admission and wait for application-visible requests to reach a
   terminal acknowledged or explicitly retained outcome.
2. Stop every gateway and shard process cleanly. Do not copy a member while it
   is serving.
3. Copy every complete prepared member root, its WAL generations, SQL/store
   files and recovery journals, catalog seed/proof, gateway session journals,
   durable ACK key, WAL key material or recoverable key references,
   authorization policy, certificates, and exact manifests.
4. Record the exact build artifact digest. The restore must use the same build
   grammar. Mixed-build restore and migration are unsupported.
5. Restore into an isolated network with the same cluster and store identities.
   Validate file inventories and command manifests before starting any member.
6. Start one quorum at a time, verify leader election and catch-up, then start
   gateways. Do not regenerate identities or bootstrap over a non-empty root.

This procedure provides no online recovery-point objective and no portable
cross-build archive. A filesystem copy taken before all processes stop is not a
backup, even if each individual file appears readable.
