# Operate the distributed runtime

The distributed runtime is experimental and unreleased. It combines a routing
gateway, a compatibility path for static SQL shards, and serving RF3 groups
for replicated catalog, request-ledger, exact-key data, topology control, and
live backup. The generated [distributed feature state](../distributed-feature-state.md)
separates primitives, internal integration, command integration, and test
evidence.

For a task-oriented setup, start with:

- [Start a local replicated cluster](distributed-quickstart.md)
- [Operate replica lifecycle](replica-lifecycle.md)
- [Observe bounded distributed metrics](observability.md)
- [Back up and restore distributed data](backup-restore.md)

## Choose a serving path

| Path | Command | Contract |
| --- | --- | --- |
| Local development | `vibedb cluster dev --replicas 1\|3 --root <absolute-path>` | Resumable one-host process orchestration. Replica count 1 starts one no-HA member for each of the catalog, request-ledger, and data roles. Replica count 3 starts three independent RF3 groups and one gateway, with distinct process identities and a generated authenticated replica-control manifest. |
| Static shard | `vibedb-shard serve` | One local store and a local ownership fence. No Raft election or copied-store revocation. |
| RF3 preparation | `vibedb-shard prepare-rf3 -manifest <path>` | Atomic, fail-if-present creation of one member's WAL, SQL root, retained identities, key copy, and serving manifest. |
| Replicated shard | `vibedb-shard serve-rf3 -manifest <path>` | One prepared Raft member with quorum writes, leader `ReadIndex`, authenticated peer, native, snapshot, and control traffic, and replica-move services. |
| Cold learner | `vibedb-shard bootstrap-rf3 -manifest <path>` | One enrolled empty target that installs an authorized snapshot before reopening through `serve-rf3`. |
| Gateway | `vibedb-gateway serve -catalog <genesis> -catalog-route-seed <private-path> ...` | Catalog-pinned routing, bounded fanout, leader-aware RF3 requests, distributed exact-key reads, durable sequenced transactions with client ACK, and optional replica-move execution. |

RF3 means a replication factor of three: one shard has three voters. It is not
a VibeDB format or API version.

## Security boundary

Gateway and shard commands require mutual TLS and a canonical `vibejson`
authorization policy by default. The certificate carries one critical binary
identity extension that binds the trust domain and node identity. Store and
node incarnations are separate retained/catalog fences.
The policy binds exact node IDs to ordered capabilities.

`-dev-plaintext-loopback` is an explicit development mode for the static
protocol. It accepts only loopback listeners and is mutually exclusive with TLS
and policy flags. RF3 has no plaintext mode. Do not expose a development
listener through an untrusted proxy or port forward.

The gateway identity needs the capabilities consumed by its configured paths:
`data_read`, `data_write`, `delegate`, `topology`, `transaction_recovery`, and
`request_ledger`. Replica control additionally requires `membership`. The
durable request service also needs `execution_pin`. `runServe` constructs that
service for replicated-catalog mode and fails startup if its catalog,
request-ledger, execution-pin, or ACK authority is incomplete. An application
principal should receive only the data or schema operations it needs.

An operator principal with `topology` capability can read the gateway's
bounded process counters with `{"op":"metrics"}`. See
[Observe a distributed cluster](observability.md). This covers routing,
fan-out, result volume, and retries. Each `serve-rf3` process also exposes a
fixed topology-authorized shard-control counter frame through
`internal/servicemetrics.Client`. See the observability guide for the exact
per-group, per-node, and gateway-controller aggregation boundary. Leadership,
exact replicated operation phase, total network, and physical device-write
cuts remain absent rather than inferred from request latency.

## Catalog authority

The bootstrap catalog contains the distribution definitions, table placement,
shard manifests, endpoint addresses, replicated shard descriptors, and table
relation profiles needed to locate the catalog RF3 group. In normal mode the
gateway reads the authoritative head from that replicated group.

The repository has no general catalog-creation CLI. Trusted application or
operator code must build a valid `gateway.Snapshot`. The local snapshot helpers
write operator-owned files; they do not authorize a replicated catalog change.
A catalog file is limited to 16 MiB and is replaced through a synced sibling
and parent-directory sync.

Replicated mode deliberately keeps two catalog files. `-catalog` is the
immutable generation-one bootstrap and attestation seed. Never replace it with
a newer head. `-catalog-route-seed` is a separate mutable, per-gateway regular
file containing the last authenticated catalog head that can locate catalog
RF3. Do not share this path, or the catalog session journal, between gateway
process identities.

Route-seed control installation performs one attested catch-up read before the
gateway starts serving. After installation, every subsequent authenticated
authoritative catalog read and successful publication is fed through the same
certified-head persistence gate. A byte-identical head performs no disk write.
A newer head that retains the exact catalog self-route is staged, synced, and
promoted while the gateway remains live. Any catalog self-route change is
staged first and seals catalog authority before the new head can reach the
holder. The command then stops accepting work, drains all catalog users,
retires and releases the old replicated session, destroys its journal, promotes
the staged seed, and exits with
`gateway.ErrReplicatedCatalogRouteRestartRequired`. Run the command under a
supervisor that restarts it after this fail-closed handoff. Startup resumes the
same transition from the pending seed and journal after a crash at any step.

Use the command validators before startup:

```bash
./bin/vibedb-gateway validate -catalog ./cluster.vibejson
./bin/vibedb-gateway inspect -catalog ./cluster.vibejson
```

`inspect` reads the file supplied on the command line. It is not a live
inspection of a newer replicated catalog head.

The repository has an experimental exact schema rollout command. A shard installer can
prepare an immutable relation bundle, persist its authorization, activate it,
drain the old generation, and recover after restart. Catalog authority can bind
the exact prepared receipts, activate one target catalog generation, or abort
before activation. `vibedb-gateway schema-rollout` consumes a strict canonical
plan, contacts authenticated shard schema-control handlers with bounded
concurrency, and publishes the exact authorized catalog cut. The command needs
the replicated-catalog flags, `-replica-control-manifest`, and
`-schema-rollout-plan`. It is not a general SQL DDL endpoint.

## Gateway startup contract

Authenticated replicated-catalog mode requires all of these flags:

- `-catalog`
- `-catalog-route-seed`
- `-catalog-relation`
- `-catalog-session-journal`
- `-durable-ack-key`
- `-catalog-client-id`
- `-catalog-retry-home`
- `-tls-certificate`, `-tls-key`, `-tls-roots`, and `-tls-identity-oid`
- `-authorization-policy`
- one `-shard-peer address=node-id` for every address the catalog can use

The stable catalog client ID is 32 lowercase hexadecimal characters. The retry
home is 16 lowercase hexadecimal characters. The durable ACK key file contains
exactly 64 lowercase hexadecimal characters and must be shared by replacement
gateways. Values must remain stable across gateway restarts. The route-seed
path must be a private regular file distinct from `-catalog`; path or inode
aliasing fails closed.

Use `-catalog-bootstrap-if-missing` only when the local generation-one seed is
authorized to initialize an empty catalog RF3 group. The first publication
atomically stores the head, its witness, and an immutable genesis proof. Later
starts attest the local seed against that replicated proof even when the current
catalog head has advanced. A missing head beside an existing genesis proof is
corruption and fails closed. The mutable route seed never replaces or weakens
this generation-one proof.

`-catalog-attempts` and `-catalog-attempt-timeout` bound leader routing. A
definite stale serving fence coalesces one authenticated catalog refresh and
one re-resolved read. An ambiguous write transport failure is not blindly
replayed.

The RF3 and static SQL clients use separate authenticated connection pools.
`-max-shard-connections-per-pool` and
`-max-shard-handshakes-per-pool` are per-pool bounds, not process-wide bounds.

Add `-replica-control-manifest <path>` to run the authenticated, resumable
replica-move controller and cluster catalog-drain service. The control manifest
is not accepted in plaintext mode. See [Operate replica lifecycle](replica-lifecycle.md).

Add `-hot-shard-capacity <path>` to record routed request pressure and publish a
bounded canonical pressure cut through catalog RF3. `-hot-shard-interval`
controls when publication is attempted. It is not correctness authority. When
`-replica-control-manifest` is also present, the gateway consumes each cut with
the clockless controller and submits at most one idempotent split or replica
move for the current catalog generation through the replicated catalog
operation journal. Without the control manifest, startup fails closed instead
of inventing topology authority. Scheduling is per physical RF3 allocation;
tenants are neither pinned to nor used as the unit of shard ownership.

## Online range-split status

The repository has durable split intent and runtime records, source capture,
immutable child artifacts, resumable child staging, tail catch-up, an exact
source ownership seal, child activation, catalog publication, and retained
pruning primitives. Child image and global-index placement accumulators keep
the cutover proof constant-size: the initial source partition is one bounded
scan, while sealing and activation do not rescan or rewrite the child image.
Before publication, the reconciler requires a coherent voting quorum for each
child at or beyond the sealed source applied position.

The serving composition includes durable source and child observation, plan
admission, exact action grants, authenticated child preparation, and dispatch
across the source and child lifecycle. `serve-rf3` installs the source,
artifact, admission, tail, child-preparation, and terminal-retirement services.
With a strict replica-control manifest and hot-shard policy, the gateway can
derive one bounded split, persist its replicated operation, run the controller,
publish the catalog successor, and retire the source after the drain witness.
This is automatic pressure-driven intake, not a general operator split CLI.
Mandatory Linux split-under-load fault gates remain Partial until CI records
their required unskipped runs.

## Send requests

The gateway accepts one bounded `vibejson` object per line. The maximum request
line is 1 MiB, with a separate 8 MiB conservative decode-metadata admission
budget. There is no additional global statement-count ceiling at ingress.
Semantic placement comes from SQL and the catalog. A client
cannot provide a shard ID or serialized plan.

### General SQL read

```vibejson
{"op":"query","sql":"SELECT * FROM users WHERE tenant_id = $1","class":"interactive","params":[{"kind":"string","text":"acme"}]}
```

The static SQL path's `ReadStrong` policy means a statement-level snapshot from
the configured static endpoint. It is not cluster-wide linearizability because
that endpoint has no Raft election or distributed lease.

### RF3 point read

Use a canonical ordered scalar primary-placement key encoded as unpadded
base64url:

```vibejson
{"op":"get","table":"users","key":"QGFjbWUAAA","consistency":"linearizable"}
```

`linearizable` follows the current leader and runs Raft `ReadIndex`. A successful
response returns an exact route lineage and applied index:

```vibejson
{"ok":true,"route_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","applied":42,"found":true,"document":{"id":"acme"}}
```

Retain `route_id` and `applied` together for a monotonic follower read:

```vibejson
{"op":"get","table":"users","key":"QGFjbWUAAA","consistency":"at_least_applied","route_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","applied":42}
```

An applied index has no meaning outside its exact route lineage.

### Multi-table and multi-shard RF3 read

`read_batch` accepts ordered exact-primary-key `SELECT *` statements:

```vibejson
{"op":"read_batch","class":"interactive","max_result_bytes":1048576,"statements":[{"sql":"SELECT * FROM users WHERE id = ?","params":[{"kind":"string","text":"user-1"}]},{"sql":"SELECT * FROM orders WHERE id = ?","params":[{"kind":"string","text":"order-9"}]}]}
```

The gateway lowers the entire request against one immutable catalog generation,
groups points by exact RF3 group, and performs one leader `ReadIndex` read per
group. Multiple relations in one group share one coherent applied cut. A
cross-group result returns a sorted observation vector with each group, route
lineage, and applied index. The vector is the consistency contract. It is not a
global timestamp or one MVCC snapshot.

The operation returns no partial values. Unsupported projections, joins,
ranges, aggregates, mixed static/RF3 authority, active intents, stale routes,
or any failed group reject the whole batch. Group count has no policy cap.
Request/result byte limits, aggregate response reservations, and
`-max-native-scatter-concurrency` bound memory and work.

### Single-owner write

`exec` is admitted only after the gateway proves one owner. DDL,
`INSERT ... SELECT`, scatter updates/deletes, and replacement documents that
move the placement key are refused before dispatch.

```vibejson
{"op":"exec","sql":"DELETE FROM users WHERE tenant_id = $1 AND id = $2","class":"interactive","params":[{"kind":"string","text":"acme"},{"kind":"string","text":"user-1"}]}
```

### Multi-table and multi-shard write

`exec_batch` is an atomic, fixed-request distributed transaction. The strict
RF3 lane supports single- or multi-row whole-document `INSERT`,
exact-primary-key whole-document `UPDATE`, and exact-primary-key `DELETE` with
equality or a finite `IN` key set. One statement may fan rows or keys across
RF3 shards, and an ordered request may touch multiple tables. Every resulting
base and index mutation belongs to the same persisted transaction. Co-located
relation mutations form one participant and apply atomically. There is no
participant-count contract. Mutation, byte, deadline, journal, and concurrency
limits provide the bounds.

Ready unique and non-unique global indexes are supported on this lane. The
gateway lowers index maintenance into independently routed relation
participants. Update and delete use an exact prior-value digest so a stale
index removal cannot apply to a changed base row.

This lane refuses projections or reads inside the transaction, `RETURNING`,
column-list inserts, `INSERT ... SELECT`, conflict clauses, partial-document
updates, replacement documents that move a primary key, and predicates that
require row discovery. Those shapes never fall back after RF3 admission.

Before the first write on a client lane, generate and durably retain one
nonzero 128-bit installation ID. Open epoch 1 and one fixed lane ordinal:

```vibejson
{"op":"issuer_open","installation_id":"11111111111111111111111111111111","issuer_epoch":1,"lane_ordinal":0}
```

The response echoes the installation, epoch, and lane ordinal and adds one
64-character lowercase hexadecimal `grant_digest`. Persist that exact grant.
Then allocate strictly monotonic sequence numbers on the lane and send the
complete structured identity:

```vibejson
{"op":"exec_batch","request_id":"0123456789abcdef0123456789abcdef","installation_id":"11111111111111111111111111111111","issuer_epoch":1,"lane_ordinal":0,"grant_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","issuer_sequence":1,"class":"interactive","statements":[{"sql":"INSERT INTO orders VALUES (?)","params":[{"kind":"document","text":"{\"id\":\"order-1\"}"}]},{"sql":"DELETE FROM ledger WHERE id = ?","params":[{"kind":"string","text":"ledger-1"}]}]}
```

The request ID and installation ID are 32 lowercase hexadecimal characters.
The grant digest is 64 lowercase hexadecimal characters. The first sequence is
1. A duplicate must carry the same request ID, grant, sequence, ordered
statements, parameter kinds, and parameter bytes. A gap, rewind, changed
request body, foreign principal, or forged grant fails closed. There is no
unsequenced RF3 fallback.

A committed response carries `transaction_id`, `committed:true`, and the exact
durable handle. The handle contains `request_id`, `request_digest`, the issuer
grant reference and sequence, `terminal_revision`, `result_digest`, and an
opaque 64-character `ack_token`. If the response is lost, reconnect to any
gateway that shares the catalog, ledger, and ACK authority and retry the exact
request. The gateway recovers the sealed program and terminal result from RF3
state instead of replanning it against a newer catalog.

After the application has durably consumed the terminal result, send
`ack_exec_batch` with every field from that handle. The ACK is authenticated.
It advances bounded collection and can be retried exactly after a lost response.
An exact completed ACK retry is write-free. Never ACK a result that the
application cannot reconstruct.

## Operation classes

| Class | Scatter | Fanout concurrency | Total rows | Total bytes | Global deadline | Shard deadline |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `interactive` | Refused | 4 | 50,000 | 16 MiB | 5 s | 2 s |
| `batch` | Allowed | 16 | 1,000,000 | 256 MiB | 60 s | 30 s |
| `admin` | Allowed | 32 | 10,000,000 | 1 GiB | 5 min | 2 min |

The generic SQL router admits at most 64 target shards for one targeted route
by default. This is separate from the RF3 `read_batch` and distributed
transaction participant contracts, which do not impose a 64-group policy cap.

## Restart and failure behavior

Restart an RF3 member with the exact same manifest and retained artifacts. The
process reopens its WAL and apply state, then catches up through Raft. An
isolated former leader must refuse writes and linearizable reads.

Run each gateway under a restart supervisor. A certified catalog self-route
change deliberately ends the serving process only after public and control
work has quiesced, the old native session has reached Retire then Release, its
journal has been removed, and the staged route seed has been promoted. The
nonzero exit reports `gateway.ErrReplicatedCatalogRouteRestartRequired`; the
next process opens the promoted binding. Preserve both catalog files, the
route-seed pending sibling, and the session journal across an interrupted
handoff so startup can settle it exactly.

For an outcome-unknown write, retry the exact request bytes with the same
request ID, issuer grant reference, and lane sequence. For a lost ACK response,
retry the exact ACK handle.
For a cold learner or replica move, retain every bootstrap, source-export,
action, and catalog journal. Deleting a journal destroys the operation's resume
evidence.

The repository includes deterministic and selected external-process tests for
leader failure, stale former-leader refusal, retry identity, follower catch-up,
snapshot resume, and move-action reconciliation.
`TestGatewayDurableRF3ExternalProcessRecovery` is the shipped durable-SQL gate:
three shard processes each host catalog, request-ledger, and two data RF3
groups; two gateways use distinct principals and retained sessions; all tested
traffic uses native mutual TLS. It covers a stopped catalog voter, killed shard
leaders, gateway replacement, lost terminal and ACK responses, exact replay,
ACK collection, and rolling voter restarts. Its bounds cover foreground p99,
RSS, allocated storage, WAL allocation, exact public client request/response
wire bytes, and snapshot payload bytes. It does not measure total Raft or total
network bytes. The mandatory Ubuntu workflow requires three unskipped runs;
the generated feature state remains Partial until those runs are recorded.
Docker Desktop containerd corruption prevented an equivalent local Linux run,
so no local substitute is counted as qualification evidence. The repository
does not yet provide an exhaustive production fault or upgrade qualification
matrix.

## Metrics and limits

The libraries expose bounded counters and the commands emit progress to
standard error. The commands do not expose a stable Prometheus or HTTP metrics
endpoint. Collect CPU, RSS, disk allocation, device writes, and network metrics
through the host or test harness.

Current operating gaps include:

- A general public operator split-intake CLI. Current intake is the bounded
  automatic hot-shard policy
- One global MVCC snapshot across RF3 groups
- A time-based durable request expiry policy. The shipped lifecycle reclaims
  only after an authenticated explicit ACK and contiguous issuer collection.
- General public DDL beyond the experimental exact schema rollout command
- A public move, live-status, or leader-transfer CLI
- A fully qualified live RF3 restore contract. Live backup export is shipped.
  See [Back up and restore distributed data](backup-restore.md) for the exact
  activation and evidence boundary.
- A mixed-build rolling disk- and wire-format upgrade or migration policy.
  Only the exact same-build pre-release restart boundary is qualified. See
  [Unreleased compatibility and rolling restarts](unreleased-compatibility.md).
- A released or production-supported distributed contract
- A complete production qualification matrix

Routing, catalog validation, authorization, membership grants, and shard
admission fail closed. A distributed read never returns partial documents.

## Implementation references

- `cmd/vibedb-gateway/serve.go`, `catalog_route_seed_test.go`,
  `replica_move_controller.go`, and `replica_move_remote.go`
- `cmd/vibedb-shard/serve_rf3.go`, `bootstrap_rf3.go`, and `rf3_manifest.go`
- `gateway/replicated_data_read.go`, `replicated_sql_read.go`,
  `replicated_sql_transaction.go`, `replicated_transaction.go`,
  `replicated_request_service.go`, `durable_sql_request_executor.go`,
  `replicated_request_ledger_catalog.go`, and
  `replicated_request_issuer_collector.go`
- `cmd/vibedb-gateway/durable_request_runtime.go`,
  `durable_exec_batch_wire.go`, `issuer_open_wire.go`, and
  `exec_batch_ack_wire.go`
- `gateway/schema_rollout.go` and `internal/schemainstall`
- `gateway/replicated_catalog_authority.go` and
  `replicated_catalog_route_seed.go`
- `internal/hotshard`
- `internal/splitcontroller/local_observation_provider.go` and
  `composite_shard_executor.go`
- `internal/rebalance`, `internal/rebalanceexec`, `internal/replicacontrol`,
  `internal/replicaaction`, and `internal/snapshottransfer`
- `internal/raftservice`, `internal/multiraft`, and `internal/rafttransport`
