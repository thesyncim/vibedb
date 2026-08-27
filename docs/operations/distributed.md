# Operate the distributed runtime

The distributed runtime is experimental. It combines a
routing gateway, static shard services, and prepared RF3 shard
groups. The generated [distributed feature state](../distributed-feature-state.md)
separates primitives, internal integration, command integration, and test
evidence.

For a task-oriented setup, start with:

- [Start a three-node replicated shard](distributed-quickstart.md)
- [Operate replica lifecycle](replica-lifecycle.md)

## Choose a serving path

| Path | Command | Contract |
| --- | --- | --- |
| Local development | `vibedb cluster dev --replicas 1\|3 --root <absolute-path>` | Resumable one-host process orchestration. Replica count 1 is explicitly no-HA and starts no gateway. Replica count 3 starts an RF3 development topology and gateway, including its dedicated request-ledger group. |
| Static shard | `vibedb-shard serve` | One local store and a local ownership fence. No Raft election or copied-store revocation. |
| RF3 preparation | `vibedb-shard prepare-rf3 -manifest <path>` | Atomic, fail-if-present creation of one member's WAL, SQL root, retained identities, key copy, and serving manifest. |
| Replicated shard | `vibedb-shard serve-rf3 -manifest <path>` | One prepared Raft member with quorum writes, leader `ReadIndex`, authenticated peer, native, snapshot, and control traffic, and replica-move services. |
| Cold learner | `vibedb-shard bootstrap-rf3 -manifest <path>` | One enrolled empty target that installs an authorized snapshot before reopening through `serve-rf3`. |
| Gateway | `vibedb-gateway serve -catalog <path> ...` | Catalog-pinned routing, bounded fanout, leader-aware RF3 requests, distributed exact-key reads and transactions, and optional replica-move execution. |

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
internal durable request service also needs `execution_pin`, but `runServe` does
not yet construct that service. An application principal should receive only
the data or schema operations it needs.

## Catalog authority

The bootstrap catalog contains the distribution definitions, table placement,
shard manifests, endpoint addresses, replicated shard descriptors, and table
relation profiles needed to locate the catalog RF3 group. In normal mode the
gateway reads the authoritative head from that replicated group.

The repository has no catalog administration CLI. Trusted application or
operator code must build a valid `gateway.Snapshot` and publish it with
`SaveSnapshot` or `SaveSnapshotAfter`. A catalog file is limited to 16 MiB and
is replaced through a synced sibling and parent-directory sync.

Use the command validators before startup:

```bash
./bin/vibedb-gateway validate -catalog ./cluster.vibejson
./bin/vibedb-gateway inspect -catalog ./cluster.vibejson
```

`inspect` reads the file supplied on the command line. It is not a live
inspection of a newer replicated catalog head.

The repository has exact schema rollout primitives. A shard installer can
prepare an immutable relation bundle, persist its authorization, activate it,
drain the old generation, and recover after restart. Catalog authority can bind
the exact prepared receipts, activate one target catalog generation, or abort
before activation. These are internal contracts. `serve-rf3` passes no schema
handler to its control mux, and no gateway command gathers receipts or drives a
rollout.

## Gateway startup contract

Authenticated replicated-catalog mode requires all of these flags:

- `-catalog`
- `-catalog-relation`
- `-catalog-session-journal`
- `-catalog-client-id`
- `-catalog-retry-home`
- `-tls-certificate`, `-tls-key`, `-tls-roots`, and `-tls-identity-oid`
- `-authorization-policy`
- one `-shard-peer address=node-id` for every address the catalog can use

The stable catalog client ID is 32 lowercase hexadecimal characters. The retry
home is 16 lowercase hexadecimal characters. Values must remain stable across
gateway restarts.

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
controls when publication is attempted. It is not correctness authority. The
command does not run the internal pressure controller or its operation sink, so
this option does not automatically split or move a shard.

## Range-split status

The repository has durable split intent and runtime records, source capture,
immutable child artifacts, resumable child staging, tail catch-up, an exact
source ownership seal, child activation, catalog publication, and retained
pruning primitives. Child image and global-index placement accumulators keep
the cutover proof constant-size: the initial source partition is one bounded
scan, while sealing and activation do not rescan or rewrite the child image.
Before publication, the reconciler requires a coherent voting quorum for each
child at or beyond the sealed source applied position.

The internal composition now includes durable source and child observation,
plan admission, exact action grants, and dispatch across the source and child
lifecycle. This is not yet an operable online-split command. There is no public
split intake, and `serve-rf3` still passes nil split and plan-admission handlers
to its control mux. The gateway's replicated-operation scanner cannot complete
a split until the command constructs those handlers. External split-under-load
kill, partition, and foreground-latency gates are also absent.

## Send requests

The gateway accepts one bounded `vibejson` object per line. The maximum request
line is 1 MiB. Semantic placement comes from SQL and the catalog. A client
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

```vibejson
{"op":"exec_batch","request_id":"0123456789abcdef0123456789abcdef","class":"interactive","statements":[{"sql":"INSERT INTO orders VALUES (?)","params":[{"kind":"document","text":"{\"id\":\"order-1\"}"}]},{"sql":"DELETE FROM ledger WHERE id = ?","params":[{"kind":"string","text":"ledger-1"}]}]}
```

The nonzero request ID is 32 lowercase hexadecimal characters. Retry the exact
ordered statements, parameter kinds, and parameter bytes with the same ID.
Never create a new request ID merely because the response was lost.

A committed response carries `transaction_id` and `committed:true`. An
ambiguous failure carries `transaction_id`, `outcome_unknown:true`, and an
error. A committed cleanup failure carries `committed:true` and an error. The
gateway recovery path uses the original group identities instead of replanning
an admitted request against a newer catalog.

The ordinary request-ID form still retains RF3 request coalescing and terminal
results in a bounded process-local registry. A gateway restart therefore loses
that registry. Treat this as a material availability and exact-retry
limitation.

The durable request-ledger implementation is more complete than that legacy
path. Catalog metadata stores exact ledger-home ranges. Replicated state stores
streamed plans, pending waves, terminal results, ACK state, issuer lanes, and
contiguous issuer high-water. A logical execution pin fences the complete
transaction across gateway replacement. Internal tests drive concurrent
gateways, recovery, ACK, collection, and high-water restart.

The public command does not yet connect that implementation. The wire decoder
recognizes issuer-open, structured `exec_batch`, and `exec_batch_ack`, but
`runServe` passes a nil durable request service. Those operations return
unavailable. No command-level gateway-replacement test covers the wire path.

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

For an outcome-unknown write, retry the exact request bytes and request ID.
For a cold learner or replica move, retain every bootstrap, source-export,
action, and catalog journal. Deleting a journal destroys the operation's resume
evidence.

The repository includes deterministic and selected external-process tests for
leader failure, stale former-leader refusal, retry identity, follower catch-up,
snapshot resume, and move-action reconciliation. It does not yet provide an
exhaustive production fault or upgrade qualification matrix.

## Metrics and limits

The libraries expose bounded counters and the commands emit progress to
standard error. The commands do not expose a stable Prometheus or HTTP metrics
endpoint. Collect CPU, RSS, disk allocation, device writes, and network metrics
through the host or test harness.

Current operating gaps include:

- Public range-split intake and complete shard-side split action routing
- External split-under-load kill, partition, and latency qualification
- Pressure-to-operation controller command composition
- One global MVCC snapshot across RF3 groups
- Durable request-ledger use by the public gateway command
- Durable request-ledger expiry policy after explicit ACK
- Schema rollout command composition and public DDL
- A public move, live-status, or leader-transfer CLI
- Live RF3 backup/restore procedures
- A mixed-build rolling disk- and wire-format upgrade or migration policy.
  Only the exact same-build pre-release restart boundary is qualified; see
  [Unreleased compatibility and rolling restarts](unreleased-compatibility.md).
- A released or production-supported distributed contract
- A complete production qualification matrix

Routing, catalog validation, authorization, membership grants, and shard
admission fail closed. A distributed read never returns partial documents.

## Implementation references

- `cmd/vibedb-gateway/serve.go`, `replica_move_controller.go`, and
  `replica_move_remote.go`
- `cmd/vibedb-shard/serve_rf3.go`, `bootstrap_rf3.go`, and `rf3_manifest.go`
- `gateway/replicated_data_read.go`, `replicated_sql_read.go`,
  `replicated_sql_transaction.go`, `replicated_transaction.go`,
  `replicated_request_service.go`, `replicated_request_ledger_catalog.go`, and
  `replicated_request_issuer_collector.go`
- `gateway/schema_rollout.go` and `internal/schemainstall`
- `internal/hotshard`
- `internal/splitcontroller/local_observation_provider.go` and
  `composite_shard_executor.go`
- `internal/rebalance`, `internal/rebalanceexec`, `internal/replicacontrol`,
  `internal/replicaaction`, and `internal/snapshottransfer`
- `internal/raftservice`, `internal/multiraft`, and `internal/rafttransport`
