# Start a local three-node replicated cluster

> The distributed runtime is experimental and unreleased. Its command and
> retained-state contracts can change before the first release.

The shortest path prepares and starts three independent three-voter Raft
groups and one gateway on a single host. The groups own the replicated catalog,
the durable request ledger, and public data. Use this path for development and
fault testing. The manual path later in this guide exposes the retained
identities and manifests that an operator must own.

VibeDB calls this topology **RF3**: one shard has three voting replicas. A
write is acknowledged after Raft commits it to a quorum and the replicated
state machine settles its durable result. RF3 is a replication factor, not a
version number.

## One-command development cluster

Build the local orchestrator and the two serving commands beside it:

```bash
mkdir -p ./bin
go build -o ./bin/vibedb ./cmd/vibedb
go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
go build -o ./bin/vibedb-gateway ./cmd/vibedb-gateway
```

Choose an empty absolute directory and start the cluster:

```bash
./bin/vibedb cluster dev --replicas 3 --root /tmp/vibedb-dev
```

The command generates a local CA and node certificates, a canonical
authorization policy, a WAL key, a durable ACK key, nine prepared member roots,
a generation-one catalog seed, a private mutable gateway route-seed path, a
strict replica-control manifest, provisioned hot-shard capacity, and durable
gateway journals. Every role process has a distinct authenticated NodeID and
exact control/snapshot inventory. It reserves loopback ports and starts three
members for each role. It waits for all nine members before it starts the
gateway. The gateway publishes the catalog seed only when catalog RF3 is empty.
The same replicated mutation publishes the generation-one head, head witness,
and immutable genesis proof. The command prints the client endpoint only after
every child reports ready.
`SIGINT` or `SIGTERM` drains and reaps the child processes.

The generated control inventory is deliberately split-only. The local topology
has no cold target process that can truthfully accept another RF3 group, so it
does not advertise replica-move candidates. Automatic replica movement requires
an operator-supplied inventory containing certified target hosts.

Run the same command with the same root to reopen the retained cluster. It
validates the canonical `cluster.vibejson` and completes a previously
interrupted preparation without replacing existing member roots or identities.
It exact-opens all nine retained SQL/apply profiles and compares their portable
schema witnesses with the catalog seed. It also attests that the local
generation-one seed matches the immutable replicated genesis proof. This
attestation remains valid after the mutable catalog head advances. After
catalog genesis completes, a missing replicated catalog head fails closed
instead of silently republishing the seed. RF3 mode supplies
`catalog.vibejson.route-seed` as the gateway's separate mutable route seed; the
gateway creates or advances it only from an authenticated certified catalog
head. The generated credentials and `local-development-only` key reference are
for one-host tests, not an operator credential lifecycle.

### Connect to the development gateway

For RF3, read the connection settings from `<root>/cluster.vibejson`:

| Field | Client use |
| --- | --- |
| `client_endpoint` | Gateway TCP address |
| `client_certificate`, `client_key` | Application client certificate and private key |
| `client_node` | Exact application identity authenticated by that certificate |
| `roots` | Generated development CA trust roots |
| `gateway_node` | Exact expected server identity |

Use the VibeDB gateway-client TLS profile with identity OID
`1.3.6.1.4.1.32473.1.1`, then send the newline-delimited requests described in
[Send requests](distributed.md#send-requests). The generated application
principal has only `data_read` and `data_write`; it cannot act as a schema,
topology, membership, backup, restore, or internal coordinator principal.

Do not connect with `gateway_certificate` and `gateway_key`: those are the
gateway's internal service credentials, and authenticating a service to itself
is rejected. The application identity is distinct from the gateway and every
Raft member. There is no standalone interactive client command yet; the
[development process client](../../cmd/vibedb-gateway/hot_shard_dev_process_test.go)
shows loading the client profile, verifying `gateway_node`, and issuing reads
and durable writes. Generic TLS tools do not implement this authenticated
identity profile automatically.

### Single-replica smoke test

For a lighter local smoke test, `--replicas 1` prepares and serves one genuine
Raft member for each of the same three roles and prints the data member's
authenticated native endpoint. This mode is explicitly development-only and
has no high availability, quorum-failure, or follower-catch-up coverage. It
does not start the distributed gateway.
`--nodes 1|3` remains a deprecated unambiguous alias. A retained root is bound
to its original replica count and cannot be silently reopened with another
topology. To run processes separately, use the manual path below.

## Manual preparation

### Before you begin

You need:

- Go 1.26 or a compatible newer toolchain.
- Five distinct 128-bit identities: three voter nodes, one gateway, and one
  application client. The same three voter node identities can host catalog,
  request-ledger, and data groups, but every role needs distinct group, shard,
  store, retained-path, and listener authority. Prepare a sixth node identity
  for the optional learner in the replica-lifecycle guide.
- One CA, and one TLS certificate and private key for each identity.
- A critical binary certificate extension at an operator-owned OID. The
  extension contains the exact VibeDB trust domain and node identity.
- One canonical `vibejson` authorization policy shared by all processes.
- One regular file that contains exactly 32 raw bytes of WAL key material.
- Four empty TCP ports per role member for peer, native, snapshot, and control
  traffic. Three separately served RF3 roles need 36 ports.
- Separately prepared and serving RF3 catalog, request-ledger, and data groups.
- One catalog bootstrap file that points to that catalog group and contains the
  catalog, request-ledger, and data descriptors, one full ledger-home range,
  and the public data table profile used by the gateway.
- One private route-seed path per gateway. It must be a regular-file path that
  is distinct from the catalog bootstrap and every other gateway's route seed.
- One private catalog session journal per gateway, retained across restarts.
- One regular file that contains exactly 32 raw bytes for the durable ACK
  derivation key.

The development commands can generate disposable test credentials, but there
is no production certificate enrollment/rotation or general catalog
administration command. Test credential fixtures are not an operator API. The
certificate extension, catalog identities, and preparation manifest must agree
exactly.

The singleton manifest emitted by `prepare-rf3` opens one group. `serve-rf3`
also accepts a strict common envelope with `groups` containing 1 through 64
retained group bundles. Those groups share the process TLS identity, policy,
listeners, bounded execution lanes, and one authenticated transport per peer.
Each group retains distinct WAL, SQL, apply, membership, and Raft identities.
Duplicate retained paths, inconsistent node addresses, and duplicate group
identities fail before any listener opens. Multi-group serving and cold
bootstrap route snapshot and replica-control operations by exact group over
shared physical listeners. Enrolled targets retain distinct group/member/store
authority even when they share one authenticated node identity and endpoint.
The 64-group manifest bound is per process, not a transaction participant or
cluster-wide shard limit.

The serving manifest keeps split-child provisioning separate from shared
control admission:

| Manifest | Split-control fields | Child registry |
| --- | --- | --- |
| Singleton | `journal_path`, `max_records`, `max_file_bytes`, `grants`, `child_registry` | Retained inside `split_control`, as before |
| Multi-group | `journal_path`, `max_records`, `max_file_bytes`, `grants`, `max_operations` | Required separately in every `groups[]` entry |

A grouped entry has canonical field order `wal`, `sql`, `route`,
`child_registry`, `members`, then optional `enrolled_target`. Each registry
uses that group's own roster, table, WAL/apply profile, and private root at
`route.member_root/split-children`. Its bootstrap file stays under that root.
The top-level `max_operations` is a process-wide bound from 1 through 64, and
each registry's limit must fit within it. Relabeling an operation for another
group does not create a second admission slot. Do not reuse one group's
registry as a shared template for heterogeneous groups.

These are `serve-rf3` manifests. `prepare-rf3` accepts optional
`schema_statements` and `global_indexes` immediately after `create_table`.
`schema_statements` contains explicit named local exact-index definitions and
global-index image table definitions; it cannot contain data mutations.
`global_indexes` binds those image tables to dense relation IDs starting at 2,
index identities, locator shape, and canonical tuple placement. The complete
schema is limited to 64 KiB and is retained in the child registry. Child
preparation validates it against the exact authenticated relation identity
before creating storage; it does not infer a schema from table names.

The base-only example below omits those optional fields. Full-schema child
preparation uses the gateway's explicit `split_sources` inventory, including
the actual prepared SQL identity, local-index definitions, and immutable
placement profile. The composed Linux serving-split gate remains unqualified. See
[Online range-split status](distributed.md#online-range-split-status).

A manually configured gateway-backed test needs catalog, request-ledger, and
data groups. Role replicas can share one multi-group process or run as nine
separate `serve-rf3` processes. Repeat the preparation procedure with distinct
group, shard, store, listener, and retained-path identities for each role. The
request-ledger group alone carries the ledger capacity and home-range fields.
The catalog exposes only the data table profile. Trusted code must build the
exact generation-one catalog seed. `-catalog-bootstrap-if-missing` authorizes
publication only when catalog RF3 has no head or immutable genesis proof. The
gateway then attests the exact seed against the replicated proof on every
restart. Keep that file immutable. A different per-gateway mutable route seed
tracks later certified heads without changing the genesis proof.

### 1. Build the commands

From the repository root:

```bash
mkdir -p ./bin
go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
go build -o ./bin/vibedb-gateway ./cmd/vibedb-gateway
```

### 2. Create the authorization policy

Use the canonical capability order shown below. Principal entries must be
ordered by binary node ID. This example grants the gateway the data and control
capabilities required by the current shipped composition. It grants the three
initial shard identities and one future learner `membership` so they can
authenticate snapshot and transition traffic. It grants one application client
read/write access.

```vibejson
{"generation":1,"principals":[{"node":"01000000000000000000000000000000","capabilities":["data_read","data_write","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]},{"node":"10000000000000000000000000000000","capabilities":["data_read","data_write"]},{"node":"11000000000000000000000000000000","capabilities":["membership"]},{"node":"12000000000000000000000000000000","capabilities":["membership"]},{"node":"13000000000000000000000000000000","capabilities":["membership"]},{"node":"14000000000000000000000000000000","capabilities":["membership"]}]}
```

Save the bytes exactly as `/srv/vibedb/authorization-policy.vibejson`, without
a trailing newline. Unknown, duplicate, escaped, or reordered security fields
fail closed. Only the canonical `vibejson` spelling is security authority.

### 3. Prepare three members

`prepare-rf3` creates one complete member root atomically. It refuses an
existing root. The input is exact output from `vibejson.Marshal`, with no
whitespace or trailing newline.

The following member-1 input is complete. Replace the example private-enterprise
OID with an identity OID under an IANA Private Enterprise Number that you own.
All file paths must be absolute and clean.

```vibejson
{"root":"/srv/vibedb/member-1","distribution":"data","shard":"all","cluster_id":"a1000000000000000000000000000000","cluster_incarnation":"a2000000000000000000000000000000","topology_recovery_epoch":1,"allocation_generation":1,"shard_incarnation":"a3000000000000000000000000000000","group_id":"a4000000000000000000000000000000","member_id":1,"store_id":"b1000000000000000000000000000000","table":"docs","create_table":"CREATE TABLE docs (PRIMARY KEY (id))","authority":{"active_policy_generation":1,"protection_epoch":1,"ownership_epoch":1,"schema_generation":1,"routing_version":1,"route_generation":1},"wal":{"key_id":"cluster-wal-key","key_material_path":"/run/secrets/vibedb/wal-key-source","wrapped_key":"operator-key-reference","max_file_bytes":268435456,"max_record_bytes":83886080,"max_records":4096,"max_entries":16384,"max_live_bytes":134217728},"apply":{"max_sessions":32,"retry_window":8,"max_collections":16,"max_documents":1024,"max_bytes":402653184,"shard_key":"/id"},"listeners":{"peer":"127.0.0.1:7411","native":"127.0.0.1:7511","snapshot":"127.0.0.1:7611","control":"127.0.0.1:7711"},"tls":{"certificate":"/run/secrets/vibedb/member-1-cert.pem","key":"/run/secrets/vibedb/member-1-key.pem","roots":"/run/secrets/vibedb/cluster-roots.pem","identity_oid":"1.3.6.1.4.1.32473.1.1"},"authorization_policy":"/srv/vibedb/authorization-policy.vibejson","split_control":{"max_records":4096,"max_file_bytes":67108864,"grants":[{"node_id":"11000000000000000000000000000000","actions":65535},{"node_id":"12000000000000000000000000000000","actions":65535},{"node_id":"13000000000000000000000000000000","actions":65535}],"max_child_operations":8,"stage_checkpoint_bytes":33554432},"members":[{"member_id":1,"node_id":"11000000000000000000000000000000","peer_address":"127.0.0.1:7411"},{"member_id":2,"node_id":"12000000000000000000000000000000","peer_address":"127.0.0.1:7412"},{"member_id":3,"node_id":"13000000000000000000000000000000","peer_address":"127.0.0.1:7413"}]}
```

Create member-2 and member-3 inputs with the same cluster, shard, authority,
WAL, apply, table, and sorted roster values. Change only these local fields:

| Field | Member 2 | Member 3 |
| --- | --- | --- |
| `root` | `/srv/vibedb/member-2` | `/srv/vibedb/member-3` |
| `member_id` | `2` | `3` |
| `store_id` | `b2000000000000000000000000000000` | `b3000000000000000000000000000000` |
| `listeners.peer` | `127.0.0.1:7412` | `127.0.0.1:7413` |
| `listeners.native` | `127.0.0.1:7512` | `127.0.0.1:7513` |
| `listeners.snapshot` | `127.0.0.1:7612` | `127.0.0.1:7613` |
| `listeners.control` | `127.0.0.1:7712` | `127.0.0.1:7713` |
| `tls.certificate` | `/run/secrets/vibedb/member-2-cert.pem` | `/run/secrets/vibedb/member-3-cert.pem` |
| `tls.key` | `/run/secrets/vibedb/member-2-key.pem` | `/run/secrets/vibedb/member-3-key.pem` |

Each certificate identity must match its roster node. Run the command once for
each input:

```bash
./bin/vibedb-shard prepare-rf3 -manifest ./prepare-member-1.vibejson
./bin/vibedb-shard prepare-rf3 -manifest ./prepare-member-2.vibejson
./bin/vibedb-shard prepare-rf3 -manifest ./prepare-member-3.vibejson
```

Each successful command syncs and publishes one root containing
`member.wal`, `member.vdb`, `sql-identity.vibejson`,
`apply-identity.vibejson`, `wal-key`, and `serve-rf3.vibejson`. It refuses to
overwrite an existing root. The source WAL key is copied as a mode-0600 file.
Protect both the source and retained copies.

The WAL bounds become authenticated reopen parameters. They are not live
tuning knobs after preparation.

### 4. Start the three members for each role

Open one terminal for each process:

```bash
./bin/vibedb-shard serve-rf3 -manifest /srv/vibedb/member-1/serve-rf3.vibejson
```

```bash
./bin/vibedb-shard serve-rf3 -manifest /srv/vibedb/member-2/serve-rf3.vibejson
```

```bash
./bin/vibedb-shard serve-rf3 -manifest /srv/vibedb/member-3/serve-rf3.vibejson
```

Each process validates all retained identities before it listens. A ready
member logs its member ID, replica-set version, and peer, native, snapshot, and
control listener addresses. Start the three catalog manifests and the three
request-ledger manifests in the same way. Do not start the gateway until all
nine members report ready. No RF3 plaintext mode exists.

### 5. Validate and inspect the catalog bootstrap

```bash
./bin/vibedb-gateway validate -catalog ./cluster.vibejson
./bin/vibedb-gateway inspect -catalog ./cluster.vibejson
```

The catalog must bind the exact group and replica identities prepared above.
It must also provide peer, native, and control endpoint IDs and addresses for
each replica. The file is only a bootstrap locator in normal operation. The
replicated catalog group is authoritative after startup.
`-dev-static-catalog` is a separate loopback-only development mode and does not
provide the RF3 operating contract described here.

### 6. Start the gateway after the catalog group is ready

List every native or SQL shard address the catalog can resolve. Each
`-shard-peer` binds an address to the expected TLS node identity.

```bash
./bin/vibedb-gateway serve \
  -catalog ./cluster.vibejson \
  -catalog-route-seed ./state/gateway-catalog-route-seed.vibejson \
  -catalog-bootstrap-if-missing \
  -catalog-relation 1 \
  -catalog-session-journal ./state/gateway-catalog-session \
  -catalog-client-id 21000000000000000000000000000000 \
  -catalog-retry-home 2200000000000000 \
  -durable-ack-key ./secrets/durable-ack-key.hex \
  -listen 127.0.0.1:7400 \
  -tls-certificate ./secrets/gateway-cert.pem \
  -tls-key ./secrets/gateway-key.pem \
  -tls-roots ./secrets/cluster-roots.pem \
  -tls-identity-oid 1.3.6.1.4.1.32473.1.1 \
  -authorization-policy /srv/vibedb/authorization-policy.vibejson \
  -shard-peer 127.0.0.1:7511=11000000000000000000000000000000 \
  -shard-peer 127.0.0.1:7512=12000000000000000000000000000000 \
  -shard-peer 127.0.0.1:7513=13000000000000000000000000000000
```

The bootstrap must resolve the separately serving catalog group. The
authoritative catalog document must describe the request-ledger and `docs`
groups prepared above. The certificate extension, catalog, preparation
manifests, and `-shard-peer` mappings must agree exactly. Add the catalog and
request-ledger native endpoints to `-shard-peer` as well. They are omitted from
this example because their operator-assigned addresses and identities are not
derivable from the data group. The durable ACK key file contains exactly 64
lowercase hexadecimal characters. Every replacement gateway must use the same
key.

The two catalog paths have different jobs. `-catalog` remains the immutable
generation-one bootstrap and attestation seed. `-catalog-route-seed` is this
gateway identity's crash-safe locator for the latest authenticated catalog
head; never share it or `-catalog-session-journal` with a replacement gateway.
Route-seed control installation performs an attested catch-up read before
serving. After installation, every subsequent certified read or publication
advances the mutable seed through a staged file. A byte-identical head does no
disk work, and a newer head with the exact same catalog self-route is promoted
while serving continues.

If the catalog self-route changes, the gateway first durably stages the
certified head and seals catalog authority. It then quiesces public and control
work, settles Retire then Release for the old native session, removes the old
journal, promotes the staged seed, and exits nonzero with
`gateway.ErrReplicatedCatalogRouteRestartRequired`. Run the gateway under a
supervisor that restarts it. Startup recovers the pending seed and exact old
journal state after a crash; it never opens a fresh session through a stale
route.

### 7. Check a linearizable read

The gateway protocol is newline-delimited canonical `vibejson`. Use a client
certificate whose node identity has `data_read` authority.

```vibejson
{"op":"get","table":"docs","key":"QGRvYy0xAAA","consistency":"linearizable"}
```

A successful result includes an exact `route_id` and Raft `applied` index. Keep
them together if you later request an `at_least_applied` follower read.

For multi-table or multi-shard exact-primary-key reads, use `read_batch`. One
leader `ReadIndex` cut is taken per RF3 group. Relations in one group share a
coherent applied cut. Different groups return a sorted observation vector.
That vector is not a global MVCC timestamp or a single wall-clock snapshot.
The batch is all-or-nothing and has no participant-count policy cap. Request,
result, worker, and in-flight byte bounds provide admission control.

### 8. Stop and restart a member

Send `SIGTERM` for an orderly stop or `SIGKILL` for a fault test. Restart with
the generated manifest:

```bash
./bin/vibedb-shard serve-rf3 -manifest /srv/vibedb/member-2/serve-rf3.vibejson
```

Do not change the WAL bounds, key ID, key bytes, node identity, store identity,
member ID, or group identity between runs. A follower reopens its WAL and apply
state, reconnects to the other members, and catches up through Raft. See
[Operate replica lifecycle](replica-lifecycle.md) for learner replacement and
failure exercises.

Restart a gateway with the same immutable catalog seed, private mutable route
seed, session journal, client ID, and retry home. A replacement gateway may
share the immutable genesis and durable ACK key, but it needs its own route seed,
session identity, and journal.

## Next steps

- [Operate the distributed runtime](distributed.md)
- [Operate replica lifecycle](replica-lifecycle.md)
- [Run the Kubernetes RF3 test lane](kubernetes.md)
- [Check distributed feature state](../distributed-feature-state.md)
