# Operate the distributed runtime

The shipped distributed runtime is experimental and unreleased. It combines a
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
`data_read`, `data_write`, `delegate`, `topology`, `transaction_recovery`,
`request_ledger`, and `execution_pin`. Replica control additionally requires
`membership`. An application principal should receive only the data or schema
operations it needs.

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
RF3 lane supports whole-document single-row insert, exact-primary-key
whole-document update, and exact-primary-key delete. Co-located relation
mutations form one participant and apply atomically. The coordinator manifest
can span more than 64 groups. There is no participant-count contract. Mutation,
byte, deadline, journal, and concurrency limits provide the bounds.

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

The current shipped command still retains RF3 request coalescing and terminal
results in a bounded process-local registry. A gateway restart therefore loses
that registry. The durable replicated request-ledger machinery is not yet the
public command entrypoint, and no client acknowledgement/expiry operation is
shipped. Treat this as a material availability and exact-retry limitation.

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

- RF3 repair and copied-store revocation
- One global MVCC snapshot across RF3 groups
- RF3 global-index mutation lowering
- Durable request-ledger use by the public gateway command
- Client acknowledgement and terminal-result expiry
- Distributed DDL and schema rollout
- A public move, live-status, or leader-transfer CLI
- Live RF3 backup/restore procedures
- A rolling disk- and wire-format upgrade policy
- A released or production-supported distributed contract

Routing, catalog validation, authorization, membership grants, and shard
admission fail closed. A distributed read never returns partial documents.

## Implementation references

- `cmd/vibedb-gateway/serve.go`, `replica_move_controller.go`, and
  `replica_move_remote.go`
- `cmd/vibedb-shard/serve_rf3.go`, `bootstrap_rf3.go`, and `rf3_manifest.go`
- `gateway/replicated_data_read.go`, `replicated_sql_read.go`,
  `replicated_sql_transaction.go`, and `replicated_transaction.go`
- `internal/rebalance`, `internal/rebalanceexec`, `internal/replicacontrol`,
  `internal/replicaaction`, and `internal/snapshottransfer`
- `internal/raftservice`, `internal/multiraft`, and `internal/rafttransport`
