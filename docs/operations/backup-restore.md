# Back up and restore distributed data

VibeDB ships authenticated live RF3 backup export through the gateway. Restore
activation is not yet shipped. This boundary is not an invitation to copy live
files or to start a non-serving restore staging root.

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

## Offline recovery procedure

For disaster-recovery experiments, use an offline copy:

1. Stop client admission and wait for application-visible requests to reach a
   terminal acknowledged or explicitly retained outcome.
2. Stop every gateway and shard process cleanly. Do not copy a member while it
   is serving.
3. Copy every complete prepared member root, its WAL generations, SQL/store
   files and recovery journals, catalog seed/proof, gateway session journals,
   durable ACK key, WAL key material or recoverable key references,
   authorization policy, certificates, and exact manifests.
4. Record the exact build artifact digest. The restore must use the same build
   grammar; mixed-build restore and migration are unsupported.
5. Restore into an isolated network with the same cluster and store identities.
   Validate file inventories and command manifests before starting any member.
6. Start one quorum at a time, verify leader election and catch-up, then start
   gateways. Do not regenerate identities or bootstrap over a non-empty root.

This procedure provides no online recovery-point objective and no portable
cross-build archive. A filesystem copy taken before all processes stop is not a
backup, even if each individual file appears readable.

## Restore activation exit gate

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

The catalog-RF3 operation record, authenticated leader resolution, complete
live collector, durable bounded repository, gateway request/status API, full
artifact verifier, and non-serving staging-root builder are composed. External
process exit before certificate publication and stalled network-stream gates
prove fail-closed backup recovery. What remains is restore activation: generate
fresh member/store/node identities, install every staged group, and publish one
catalog-backed one-time activation witness before any restored process receives
serving authority. No command claims that activation today.
