# Roll out a replicated schema

`vibedb-gateway schema-rollout` installs one exact catalog generation across
every replica of every changed RF3 shard group. It prepares immutable
replica-local SQL catalog images before the rollout's no-return boundary and
publishes the target global catalog only after every affected replica reports
that exact generation active.

The distributed commands remain experimental and have no released compatibility
contract. Pin one tested commit across the gateway and shard fleet.

## Before you begin

You need:

- a running authenticated RF3 catalog and shard fleet;
- the gateway's normal replicated-catalog flags and stable controller identity;
- a replica-control manifest covering every target shard node;
- an authorization policy granting the gateway identity schema capability;
- the next canonical global catalog snapshot;
- one canonical `vibejson` SQL catalog image for each replica of every changed
  shard group; and
- the exact apply-contract digest supported by the fleet.

A changed group must have exactly three distinct replica plans. Replica-local
images may differ because their immutable storage identities differ. The
gateway derives allocation generations, group identities, and old/new relation
manifests from the authenticated old and target catalogs; the operator cannot
override them in the rollout plan.

Prepare and backfill every new global-index relation before rollout. Its
immutable image must carry the expected cardinality, root, placement, and apply
identity. Activation refuses a missing, empty, or substituted global-index
image. Base tables and local exact indexes may be materialized deterministically
from the replica-local catalog image.

## Create the rollout plan

### Build local-index and TRUNCATE images

The shard-control listener also exposes an authenticated build operation for
`CREATE INDEX`, `DROP INDEX`, and `TRUNCATE`. It is an internal coordinator API,
not yet a PostgreSQL DDL endpoint. It does not add ALTER/DROP TABLE support or
complete the durable SQL coordinator described below.

The caller first fences new writes with the exclusive route gate and obtains
an exact applied cut from the shard quorum. The build request binds that cut,
the source allocation/schema/manifest, the operation ID, and exact SQL bytes.
Each replica reserves fresh physical storage identities in `.schema-ddl-build`
before materializing files, then persists its certified receipt before replying.
Retries reuse those identities, including after process replacement or an
incomplete image write. Neither a successful build nor its receipt authorizes
activation. Keep the gate held and retain all receipts in the coordinator's
operation record before preparing the existing rollout.

Installation uses the journaled source applied cut, not the replica's current
position. If that position advanced, preparation refuses the stale image. A
pending build or an unselected ready target prevents a different operation from
replacing the journal slot; there is no automatic abandon/overwrite policy.
Do not delete the journal or target images to bypass that refusal.

This cold path uses a 208-byte request header, at most 64 KiB of SQL, and a
bounded 32 MiB canonical receipt. Authentication and admission happen before
reading SQL. Each node admits at most two builds, each with a two-minute
execution deadline. Ordinary query and write execution do not access this
journal. These bounds are not a guarantee that an arbitrarily large index will
finish within the deadline.

`gateway.BuildReplicatedSchemaDDLPlan` assembles the target global catalog and
installation plans from the authenticated build receipts. It requires one
receipt per replica of every affected shard and validates source fences, exact
SQL, portable schema, declared columns, placement, and local-index metadata.
Replicas are identified by group plus node/member: a multigroup node can reuse
the same member number in several groups. Response arrival order does not change
the resulting plan. This is a cold planning API, not PostgreSQL execution or a
durable coordinator journal.

### Supply the exact target plan

The plan is strict canonical `vibejson`. Whitespace, reordered object members,
duplicate fields, malformed hexadecimal identities, and trailing bytes are
rejected. `operation` is a nonzero 32-byte identifier encoded as 64 lowercase
hexadecimal characters. `node` is a 16-byte node identity and
`apply_contract` is a 32-byte digest, both in lowercase hexadecimal.

This shortened example shows one three-replica changed group. Use absolute paths
so retries do not depend on the process working directory:

```json
{"operation":"1111111111111111111111111111111111111111111111111111111111111111","replicas":[{"apply_contract":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bundle":"/var/lib/vibedb/rollouts/group-7-member-1.vibejson","member":1,"node":"01010101010101010101010101010101"},{"apply_contract":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bundle":"/var/lib/vibedb/rollouts/group-7-member-2.vibejson","member":2,"node":"02020202020202020202020202020202"},{"apply_contract":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","bundle":"/var/lib/vibedb/rollouts/group-7-member-3.vibejson","member":3,"node":"03030303030303030303030303030303"}],"target_catalog":"/var/lib/vibedb/catalog/catalog-42.vibejson"}
```

Include three entries for each additional changed group. Unchanged groups do
not need entries. The manifest is capped at 4 MiB and each replica-local bundle
at 64 MiB.

## Run the rollout

Use the same authenticated catalog, TLS, shard-peer, and stable session flags as
`vibedb-gateway serve`, then add the replica-control manifest and rollout plan:

```bash
vibedb-gateway schema-rollout \
  -catalog /var/lib/vibedb/catalog/current.vibejson \
  -catalog-relation 1 \
  -catalog-session-journal /var/lib/vibedb/gateway/catalog-session \
  -catalog-client-id 00112233445566778899aabbccddeeff \
  -catalog-retry-home 0011223344556677 \
  -tls-certificate /etc/vibedb/gateway.crt \
  -tls-key /etc/vibedb/gateway.key \
  -tls-roots /etc/vibedb/cluster-ca.crt \
  -tls-identity-oid 1.3.6.1.4.1.example \
  -authorization-policy /etc/vibedb/authorization.vibejson \
  -replica-control-manifest /etc/vibedb/replica-control.vibejson \
  -schema-rollout-plan /var/lib/vibedb/rollouts/catalog-42.vibejson \
  -shard-peer 10.0.0.11:7432=01010101010101010101010101010101 \
  -shard-peer 10.0.0.12:7432=02020202020202020202020202020202 \
  -shard-peer 10.0.0.13:7432=03030303030303030303030303030303
```

Replace the example identity OID and endpoints with values certified for your
cluster. The command does not accept plaintext mode.

The command performs this ordered protocol:

1. Each affected replica durably stages and validates its immutable local
   bundle without changing serving state.
2. The gateway folds the three authenticated replica receipts into one
   constant-size witness per changed group.
3. Catalog RF3 records the bounded rollout intent as planned.
4. Catalog RF3 advances it to running. This is the no-return boundary.
5. Every shard authorizes its exact transition through Raft, atomically swaps
   the pinned live generation, and reports the target active.
6. Only after all replicas are active does catalog RF3 compare-and-swap the
   global catalog head and mark the operation complete.

The command prints the target catalog generation, operation revision, and
elapsed time after completion.

## Recover from interruption

Retry the same command with the byte-identical plan and stable gateway catalog
session identity. The operation identifier, catalog digests, group receipts,
and replica installation digests make prepare, authorize, activation, and
catalog publication idempotent.

Before the operation becomes running, catalog authority may abort it. Once it
is running, rollback is deliberately refused: some replicas may already serve
the new generation while the global catalog still points at the old one. The
safe recovery is to finish forward. Stale-generation proposal and read fences
prevent a mixed replica from silently applying the wrong contract.

`serve-rf3` also handles a crash after the exact schema command commits at
source applied position N+1 but before the local SQL catalog publishes its
replacement. Startup authenticates the retained command, checkpoint membership,
authorization, and catalog compare-and-swap proof. It opens the old source only
as a fenced recovery handle, finishes that exact catalog publication, closes
the source, and then opens the target before creating the serving runtime.
An authenticated prepared-but-uncommitted source follows the old-schema path;
mismatched or ambiguous proofs fail closed.

The shard retains both generations until an exact drain proof establishes that
old catalog leases and execution pins are gone. Draining is separate from
catalog publication; never delete old relation files manually.

After exact source drain, the shard retains a bounded `.schema-lineage` record
containing the selected catalog and its activation/membership proofs. The
immutable `.schema-origin` binds it to the original startup identity. Only this
durably selected, drained generation authorizes replacement of the old
`.schema-target-catalog`, `.schema-membership-stage`, and `.schema-activation`
slots. Every replacement must be its exact successor; a newer generation number
alone is insufficient. Restart authenticates the original identity before
adopting the retained lineage, and still requires an exact one-generation
transition for an undrained successor. Do not delete these files.

The local repeated-generation lifecycle is covered by a 1,000-row test with two
index creations, index removal, TRUNCATE, another index removal, and restarts at
prepared, pre-proposal, and drained boundaries. It also tests an interrupted
lineage directory fence and refusal to replace undrained proofs. This does not
by itself qualify a live quorum. A separate three-member physical SQL/WAL test
seeds 1,000 typed rows per member, recovers committed-source interruptions before
catalog publication across three successive DDL operations, and adopts each
recovered generation into the Raft runtime. This still does not complete the
PostgreSQL coordinator: durable gateway operation records, route
gate acquisition/recovery, catalog publication, and multi-replica drain must be
wired and qualified together before exposing these statements through SQL.

## Resource bounds

The serving path applies hard bounds:

- gateway rollout fan-out: at most 64 concurrent replica operations;
- shard installer concurrency: 8 operations;
- control protocol: fixed 592-byte requests and 652-byte responses, plus the
  bundle on prepare;
- bundle size: at most 64 MiB per replica;
- shard artifact store: at most 16 artifacts and 1 GiB;
- shard rollout journal: at most 256 records; and
- persisted catalog operation state and group receipt roots: constant-size,
  independent of relation cardinality.

These are admission and storage ceilings, not recommended saturation targets.
Measure foreground p99.9 latency, checkpoint time, network bytes, retained old
generation bytes, and recovery duration before increasing cluster concurrency.

## Verify the safety contract

The logical SQL relation digest is not the exact replicated machine-schema
digest used in command fences. Initial routing and restore now compute those
domains separately. Rollout validation requires a common logical schema cut
across a distribution while retaining each shard's exact machine manifest;
different shard ranges legitimately have different machine digests. Targeted
normal and race tests cover the DDL planner with two shards on the same three
nodes, including index creation/removal and TRUNCATE. These gates are not a
claim that every logical-to-machine digest caller is qualified.

Schema-directory publication and retry paths now sync their directory entries
before exposing authority. Local normal and race tests cover exact committed
source recovery, fenced handles, and settlement before runtime startup. They do
not establish power-loss safety at every filesystem publication cut. Required
Linux physical integration runs are still failing or pending; this remains an
experimental rollout boundary.

Repository gates cover restart after a leader-loss outcome-unknown error, the
mixed-generation interval, refusal to roll back after authorization, an old
global catalog until every replica is active, and exact completion from a fresh
process. The gate also bounds elapsed time, encoded protocol bytes, process
memory, and durable controller state.

See:

- [`gateway/schema_rollout_process_test.go`](../../gateway/schema_rollout_process_test.go)
- [`gateway/schema_rollout_controller.go`](../../gateway/schema_rollout_controller.go)
- [`cmd/vibedb-shard/schema_install_rf3.go`](../../cmd/vibedb-shard/schema_install_rf3.go)
- [`cmd/vibedb-shard/schema_startup_recovery.go`](../../cmd/vibedb-shard/schema_startup_recovery.go)
- [`cmd/vibedb-shard/schema_startup_recovery_linux_test.go`](../../cmd/vibedb-shard/schema_startup_recovery_linux_test.go)
- [`internal/schemainstall/control.go`](../../internal/schemainstall/control.go)
