# Back up and restore distributed data

VibeDB does not yet ship a safe live RF3 cluster backup or restore command.
This is an explicit unreleased boundary, not an invitation to copy live files.

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

There is no global wall-clock snapshot contract. A future backup must publish a
vector of exact per-group Raft cuts through catalog authority. This matches the
database's clock-free consistency model: Raft order is authoritative inside a
group, while a catalog-bound vector defines the cross-group recovery cut.

## Only supported pre-release procedure

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

## Required live-backup exit gate

A live command is complete only when an external kill/partition test proves:

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

