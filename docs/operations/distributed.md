# Operate the distributed runtime

The shipped distributed runtime is experimental. SQL uses a routing gateway
and static-ownership shard services. A canonical point `get` can instead use an
externally prepared, stable three-voter Raft group when the gateway consumes
the replicated catalog. The generated [distributed feature
state](../distributed-feature-state.md) separates those paths and their
qualification evidence.

The gateway and shard commands require mutually authenticated TLS by default.
Authenticated mode also requires one canonical `vibejson` authorization policy.
An explicit `-dev-plaintext-loopback` mode permits unauthenticated development
traffic only on loopback listeners. Do not expose that development listener
through a proxy or port forward to an untrusted network.

`vibedb-shard serve` serves one local store with one static ownership identity.
`vibedb-shard serve-rf3` constructs bounded proposal admission, quorum commit,
committed apply, result settlement, authenticated peer transport, and an
authenticated native replicated endpoint over exact prepared artifacts. It
does not create or repair those artifacts. `vibedb-gateway serve` routes only
the canonical point `get` envelope through that endpoint. It sends SQL queries
and writes through the static SQL shard protocol. The RF3 point-read path has
no SQL fallback.

## Build the commands

```bash
go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
go build -o ./bin/vibedb-gateway ./cmd/vibedb-gateway
```

## Prepare the topology

You need an authoritative gateway catalog before you can start the gateway.
The catalog contains these items:

- One monotonic catalog generation
- Distribution specifications
- Table placements
- One full-range shard manifest for each distribution
- Endpoint IDs and addresses
- Optional index metadata and planner statistics
- Optional exact RF3 shard descriptors
- Optional replicated base-table profiles

This repository has no catalog administration CLI. Application or operator
code must construct and publish a valid `gateway.Snapshot`. Use
`NewSnapshotWithReplicatedTableMetadata` when a table must serve RF3 point
reads. Each profile binds one table and one scalar string/number
primary-placement path to a dense relation, schema generation,
relation-manifest digest, and exact key and document limits. Composite
placement tuples and tenant-path placement are not accepted by this public
lane. Each routed allocation must also have one matching RF3 shard descriptor
and three native replica endpoints.

`SaveSnapshot` and `SaveSnapshotAfter` publish the bootstrap file for
cooperating writers. In the normal gateway mode, that file locates the
dedicated catalog RF3 group. The gateway then reads the authoritative catalog
head from that group. `-dev-static-catalog` is the explicit loopback-only mode
that treats the file as the authority.

The catalog publisher writes a temporary sibling, syncs it, replaces the
catalog, and syncs the parent directory. The file has a 16 MiB limit. Do not
remove or replace the catalog lock entry while a publisher is active.

## Initialize a shard store

Run `init` one time for each local shard store:

```bash
./bin/vibedb-shard init \
  -store ./shard-a.vdb \
  -distribution tenants \
  -shard shard-a \
  -allocation-generation 1
```

The command writes a permanent binding to the store. The binding contains the
distribution, shard ID, allocation generation, and SQL log ID.

`serve` does not initialize a missing store. This rule prevents an operator
from starting an empty replacement by accident.

`init` does not create tables. Before serving, trusted application or operator
code must open each shard store locally and create the required tables. The
gateway does not distribute DDL, and the runtime does not prove that schemas
match across shards.

## Prepare the authorization policy

Both services load the same immutable policy generation. Principal IDs are the
32-character hexadecimal node identities carried by their TLS certificates.
The array and capability order below are canonical. The gateway service
identity needs `delegate`, `data_read`, and `data_write`: it delegates the exact
application principal to a shard and uses its own authority for recovery. It
also needs `topology` when it consumes the replicated catalog and runs the
split controller. A client receives only the operations listed for that
principal.

```json
{"generation":1,"principals":[{"node":"11111111111111111111111111111111","capabilities":["data_read","data_write","delegate","topology"]},{"node":"33333333333333333333333333333333","capabilities":["data_read","data_write","schema"]}]}
```

Save that exact document as `./authorization-policy.vibejson`. Unknown,
duplicate, escaped, or reordered security fields fail closed. The policy is
mandatory in authenticated mode and is mutually exclusive with
`-dev-plaintext-loopback`.

## Start the shard service

```bash
./bin/vibedb-shard serve \
  -store ./shard-a.vdb \
  -listen 127.0.0.1:7401 \
  -distribution tenants \
  -shard shard-a \
  -allocation-generation 1 \
  -epoch 1 \
  -routing-version 1 \
  -tls-certificate ./shard.pem \
  -tls-key ./shard-key.pem \
  -tls-roots ./cluster-roots.pem \
  -tls-identity-oid 1.3.6.1.4.1.32473.1.1 \
  -authorization-policy ./authorization-policy.vibejson
```

The service makes a durable local claim before it listens. The store retains
independent high-water marks for ownership epoch and routing version. A later
claim can reuse equal coordinates or advance either coordinate, but cannot
lower either one. Only one live claim can exist for one open store.

This claim is a local fence. It is not an election or a distributed lease. It
cannot stop another process that serves a copied store.

The default connection limit is 128. Authenticated serving requires a positive
bounded connection limit. Development plaintext accepts `-max-connections -1`
only when an external control gives a safe bound. The service always bounds
read fences and worker exchange resources.

## Start a prepared RF3 member

There is no RF3 initializer or provisioner command. Before invoking
`serve-rf3`, an external trusted provisioning workflow must have created and
retained all of these mutually matching artifacts:

- One encrypted Raft WAL and its exact five sealed reopen bounds
- The bound replicated SQL root
- The complete replicated shard-store identity returned at bind
- The complete replicated apply identity returned at activation
- Exactly 32 raw bytes of WAL key material, stored outside the manifest
- A TLS certificate and key whose critical binary peer identity matches the
  local member and the retained cluster trust domain
- Trust roots and a canonical authorization policy with at least one
  delegate-capable gateway principal

The durable apply publication must contain exactly three sorted voters, no
learners or joint configuration, and no committed-but-unapplied membership
entry. The command compares that publication with the manifest roster and
fails closed on any mismatch. It does not add, promote, remove, or transfer a
member.

Create one canonical manifest per member. Object fields and member fields must
appear in the exact order shown. The manifest is limited to 64 KiB, contains
exactly three members ordered by `member_id`, and uses lowercase 32-character
hexadecimal node IDs. Unknown, duplicate, escaped, missing, or reordered fields
are rejected.

```json
{
  "wal": {
    "path": "/srv/vibedb/member-1.wal",
    "key_id": "cluster-wal-key",
    "key_material_path": "/run/secrets/vibedb-wal-key",
    "max_file_bytes": 268435456,
    "max_record_bytes": 83886080,
    "max_records": 4096,
    "max_entries": 16384,
    "max_live_bytes": 134217728
  },
  "sql": {
    "path": "/srv/vibedb/member-1.vdb",
    "identity_path": "/srv/vibedb/member-1-sql-identity.json",
    "apply_identity_path": "/srv/vibedb/member-1-apply-identity.json"
  },
  "listeners": {
    "peer": "0.0.0.0:7411",
    "native": "0.0.0.0:7511"
  },
  "tls": {
    "certificate": "/run/secrets/member-1-cert.pem",
    "key": "/run/secrets/member-1-key.pem",
    "roots": "/run/secrets/cluster-roots.pem",
    "identity_oid": "1.3.6.1.4.1.32473.1.1"
  },
  "authorization_policy": "/etc/vibedb/authorization-policy.vibejson",
  "members": [
    {"member_id": 1, "node_id": "44444444444444444444444444444444", "peer_address": "member-1.internal:7411"},
    {"member_id": 2, "node_id": "55555555555555555555555555555555", "peer_address": "member-2.internal:7411"},
    {"member_id": 3, "node_id": "66666666666666666666666666666666", "peer_address": "member-3.internal:7411"}
  ]
}
```

The five WAL bounds must be the exact nonzero values used when the WAL was
created; they are authenticated by the WAL and are not runtime tuning knobs.
`key_material_path` must name a regular file containing exactly 32 bytes. The
manifest carries no key bytes.

Start the member with only the manifest flag:

```bash
./bin/vibedb-shard serve-rf3 -manifest ./member-1-rf3.vibejson
```

The command validates and opens the retained identities, WAL, SQL root, apply
state, certificate, policy, and roster before serving. It reserves both TCP
listeners before adopting the runtime, because adoption durably advances the
local node incarnation. Both the ordinary Raft peer listener and native shard
listener require mutual TLS; there is no RF3 plaintext mode. The native
listener authorizes both the delegate gateway and the exact forwarded
principal on every request.

This command is a fixed-RF3 serving boundary, not cluster lifecycle control.
It provides no artifact preparation, copied-store revocation, membership
changes, snapshot transfer or installation, repair, or automatic catalog
publication. A three-process command-composition gate opens prepared retained
state through this serving path, forms a natural election over mutually
authenticated peer traffic, probes every authenticated native endpoint, and
requires clean process shutdown. Deeper fault gates still live below the
command boundary; membership and snapshot lifecycle qualification remain
separate work.

## Validate and inspect the gateway catalog

```bash
./bin/vibedb-gateway validate -catalog ./cluster.json
./bin/vibedb-gateway inspect -catalog ./cluster.json
```

`validate` exits with a nonzero status when the catalog is invalid. `inspect`
prints generation, distribution, shard, allocation, epoch, routing, and
endpoint information.

## Start the gateway

```bash
./bin/vibedb-gateway serve \
  -catalog ./cluster.json \
  -catalog-relation 1 \
  -catalog-session-journal ./gateway-catalog-session \
  -catalog-client-id 77777777777777777777777777777777 \
  -catalog-retry-home 8888888888888888 \
  -listen 127.0.0.1:7400 \
  -tls-certificate ./gateway.pem \
  -tls-key ./gateway-key.pem \
  -tls-roots ./cluster-roots.pem \
  -tls-identity-oid 1.3.6.1.4.1.32473.1.1 \
  -authorization-policy ./authorization-policy.vibejson \
  -shard-peer 127.0.0.1:7401=22222222222222222222222222222222
```

Replace the example private-enterprise OID with an identity OID under an IANA
Private Enterprise Number that the operator owns. The shard peer address must
match the endpoint address in the catalog.

Each `Query` or `Exec` attempt pins one immutable catalog generation. The
default executor retries a stale refusal at most twice, for at most three
attempts and two reloads. Each retry must load a strictly newer generation. A
multi-statement `ExecBatch` pins one generation and has no stale-retry loop. A
one-statement batch delegates to `Exec`.

The catalog and public RF3 point reads share one bounded authenticated native
connection pool. The pool keys a physical connection by authenticated node and
address. The RF3 executor also keeps bounded four-way exact leader hints. It
validates the complete route and serving fence, follows `NotLeader` responses,
and retries within `-catalog-attempts` and `-catalog-attempt-timeout`. A
definite stale serving fence coalesces one authenticated catalog refresh and
permits exactly one re-resolved read attempt; ambiguous transport failures are
never replayed by that refresh path.

The gateway runs distributed-transaction recovery every five seconds. It logs
the number of coordinators that recovery resolves.

## Send an NDJSON request

The gateway accepts one JSON object on each line. A request contains SQL, typed
parameters, an operation, and an operation class.

```json
{"op":"query","sql":"SELECT * FROM users WHERE tenant_id = $1","class":"interactive","params":[{"kind":"string","text":"acme"}]}
```

The allowed operations are:

| `op` value | Action |
| --- | --- |
| Empty or `query` | Run a read query. |
| `exec` | Run one write that the gateway proves has one owner. |
| `exec_batch` | Run a fixed-participant atomic write batch. |

The gateway also accepts one canonical RF3 point-read envelope. The object and
fields must use the exact order shown. `key` is the unpadded base64url encoding
of the canonical ordered scalar primary-placement key. The table profile, not
the client, selects the relation and RF3 route.

```json
{"op":"get","table":"users","key":"QGFjbWUAAA","consistency":"linearizable"}
```

A successful response returns the exact route lineage and applied index:

```json
{"ok":true,"route_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","applied":42,"found":true,"document":{"id":"acme"}}
```

Use that pair for an explicit monotonic follower read:

```json
{"op":"get","table":"users","key":"QGFjbWUAAA","consistency":"at_least_applied","route_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","applied":42}
```

The RF3 response uses closed typed error codes. Examples include
`table_not_replicated`, `position_mismatch`, `read_behind`, `stale_catalog`,
`overloaded`, and `unavailable`. The `retryable` field states whether a caller
can retry the same operation. The public RF3 boundary does not accept `put` or
`delete` yet.

The parameter kinds are `null`, `bool`, `number`, `string`, and `document`.
The client cannot select a shard or supply a serialized plan. SQL and catalog
placement determine the route.

A response can contain columns, raw JSON rows, affected-row count, route type,
catalog generation, fanout count, retry count, or an error.

One request is limited to 1 MiB before JSON decoding. The shard wire protocol
has a separate frame limit of 16 MiB plus 64 KiB. Public point reads also have
operator-configurable concurrent-request and aggregate response reservations:
`-max-native-read-concurrency` and `-max-native-read-bytes`. Each admitted read
retains its schema-authenticated maximum-document reservation until the
response has been written to the client.

## Select an operation class

| Class | Scatter | Fanout concurrency | Total rows | Total bytes | Global deadline | Shard deadline |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `interactive` | Refused | 4 | 50,000 | 16 MiB | 5 s | 2 s |
| `batch` | Allowed | 16 | 1,000,000 | 256 MiB | 60 s | 30 s |
| `admin` | Allowed | 32 | 10,000,000 | 1 GiB | 5 min | 2 min |

An empty route returns an empty result without shard I/O. Routing expands at
most 256 deduplicated finite-domain combinations and admits at most 64 shards
for a targeted route by default. The zero-value admission policy refuses
scatter and overflow.

## Read consistency

The canonical RF3 point-read envelope has two explicit policies:

- `linearizable` follows the current leader and uses Raft `ReadIndex`. It does
  not accept a caller position.
- `at_least_applied` requires a nonzero applied index and the exact `RouteID`
  returned by an earlier result. The gateway rejects a different route lineage
  before shard I/O. It prefers a follower that has reached the requested index.

Every successful RF3 point read returns a new `route_id` and `applied` pair.
Retain the pair together. An applied index has no meaning outside its exact
route lineage.

The SQL path has a different contract. It accepts only the `ReadStrong` policy
and serves a statement-level snapshot from its statically configured leader
endpoint. Because the SQL shard service has no Raft election or replication,
`ReadStrong` does not prove cluster-wide linearizability or revoke a copied
store.

A multi-shard read uses a short-lived coherent read fence. The gateway acquires
the same scoped fence on all target shards, runs the reads, and releases the
fence. This protocol establishes a scoped vector cut. It does not assign a
distributed MVCC timestamp or prove one wall-clock snapshot instant.

Read fences are in-memory leases. Their default capacity is 4096 per shard.
The gateway requests a lease equal to the operation-class global deadline: 5
seconds, 60 seconds, or 5 minutes for the shipped profiles. The shard uses one
second only when a caller supplies no positive lease. It clamps all leases to
ten minutes.

## Write rules

The gateway sends an ordinary write only after it proves one owner:

- All rows of `INSERT ... VALUES` must resolve to the same shard.
- `INSERT ... SELECT` is refused.
- `UPDATE` and `DELETE` must route to one shard or an empty route.
- A whole-document update must keep the same shard owner.
- DDL is refused on this path.

`exec_batch` supports fixed-participant atomic writes. It permits at most 64
participants. The first participant in sorted order is the coordinator.

An ordinary `exec` can also use the distributed transaction protocol. The
base-table mutation must still prove one base owner, but active global-index
maintenance can add independently placed index participants.

Do not retry an unknown transaction as new SQL. Go API callers must retain the
ID from `TransactionOutcomeUnknownError` or `CommittedTransactionError` and
call `RecoverTransaction`. The NDJSON protocol exposes only error text and has
no recover-by-ID operation. The shipped gateway runs `RecoverAll` at startup
and every five seconds. A durable coordinator commit can succeed before apply,
release, or retirement finishes.

## Operational limits

The shipped runtime does not provide these features:

- RF3 initialization, artifact provisioning, or repair
- Public RF3 writes
- RF3 scatter reads or multi-table reads
- Composite or tenant-path public RF3 placement keys
- A common distributed RF3 read timestamp
- Static SQL endpoint failover or load balancing
- Automatic catalog-group provisioning
- Distributed DDL or schema-rollout validation
- A replica-move controller
- RF3 membership changes or leader-transfer control
- Cross-host worker exchange from CLI configuration
- Online empty-learner snapshot installation and activation

Routing, catalog validation, and shard admission fail closed. A distributed
read returns no partial result when one shard fails.

## Implementation references

- `cmd/vibedb-gateway/main.go` and `serve.go`
- `cmd/vibedb-shard/main.go`, `serve_rf3.go`, and `rf3_manifest.go`
- `gateway/catalog.go`, `executor.go`, `profile.go`, `transaction.go`,
  `replicated_data_read.go`, `replicated_table.go`,
  `replicated_leader_cache.go`, and `replicated_transport.go`
- `gateway/recovery.go` and `read_snapshot.go`
- `distribution/manifest.go`, `placement.go`, `policy.go`, and `router.go`
- `shardservice/admit.go`, `server.go`, `wire.go`, and `read_fence.go`
- `sql/driver/shard_store.go`
