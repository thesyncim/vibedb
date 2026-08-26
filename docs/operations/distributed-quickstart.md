# Start a local three-node replicated cluster

The shortest path prepares and starts one three-voter Raft group and one
gateway on a single host. Use it for development and fault testing. The manual
path later in this guide exposes the retained identities and manifests that an
operator must own.

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
./bin/vibedb cluster dev --nodes 3 --root /tmp/vibedb-dev
```

The command generates a local CA and node certificates, a canonical
authorization policy, a WAL key, three prepared member roots, a bootstrap
catalog, and a durable gateway session journal. It reserves loopback ports,
starts all three members, waits for them, starts the gateway, and prints the
client endpoint only after every child reports ready. `SIGINT` or `SIGTERM`
drains and reaps the child processes.

Run the same command with the same root to reopen the retained cluster. It
validates the canonical `cluster.vibejson` and completes a previously
interrupted preparation without replacing existing member roots or identities.
The generated credentials and `local-development-only` key reference are for
one-host tests, not an operator credential lifecycle.

`--nodes` is explicit but currently accepts exactly `3`. There is no RF1
shortcut: a one-process mode would not exercise quorum commit, leader loss, or
follower catch-up. To run processes separately, use the manual path below.

## Manual preparation

### Before you begin

You need:

- Go 1.26 or a compatible newer toolchain.
- Five distinct 128-bit identities for the data example: three shard nodes,
  one gateway, and one application client. Prepare a sixth data identity for
  the optional learner in the replica-lifecycle guide. The separately serving
  catalog group needs three additional node identities.
- One CA, and one TLS certificate and private key for each identity.
- A critical binary certificate extension at an operator-owned OID. The
  extension contains the exact VibeDB trust domain and node identity.
- One canonical `vibejson` authorization policy shared by all processes.
- One regular file that contains exactly 32 raw bytes of WAL key material.
- Four empty TCP ports per shard member for peer, native, snapshot, and control
  traffic.
- A separately prepared and serving RF3 catalog group.
- One catalog bootstrap file that points to that catalog group and contains the
  data-shard descriptors used by the gateway.

The repository does not provide a certificate-generation or catalog
administration command. Test credential fixtures are not an operator API. The
certificate extension, catalog identities, and preparation manifest must agree
exactly.

The singleton manifest emitted by `prepare-rf3` opens one group. `serve-rf3`
also accepts a strict common envelope with `groups` containing 1 through 64
retained group bundles. Those groups share the process TLS identity, policy,
listeners, bounded execution lanes, and one authenticated transport per peer;
each group retains distinct WAL, SQL, apply, membership, and Raft identities.
Duplicate retained paths, inconsistent node addresses, and duplicate group
identities fail before any listener opens. A multi-group process cannot carry
an enrolled replacement target yet because snapshot listeners and source
control are still process-scoped rather than group-scoped.

A manually configured gateway-backed data test needs this data group plus a
separately serving catalog group. The catalog replicas may be colocated with
data replicas through a multi-group manifest or run as three separate
`serve-rf3` processes. Repeat the preparation procedure with distinct group,
shard, store, and retained-path identities for that group. Trusted code must
then publish its initial catalog document. This guide does not invent a catalog
creation command that the repository does not provide.

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
{"root":"/srv/vibedb/member-1","distribution":"data","shard":"all","cluster_id":"a1000000000000000000000000000000","cluster_incarnation":"a2000000000000000000000000000000","topology_recovery_epoch":1,"allocation_generation":1,"shard_incarnation":"a3000000000000000000000000000000","group_id":"a4000000000000000000000000000000","member_id":1,"store_id":"b1000000000000000000000000000000","table":"docs","create_table":"CREATE TABLE docs (PRIMARY KEY (id))","authority":{"active_policy_generation":1,"protection_epoch":1,"ownership_epoch":1,"schema_generation":1,"routing_version":1,"route_generation":1},"wal":{"key_id":"cluster-wal-key","key_material_path":"/run/secrets/vibedb/wal-key-source","wrapped_key":"operator-key-reference","max_file_bytes":268435456,"max_record_bytes":83886080,"max_records":4096,"max_entries":16384,"max_live_bytes":134217728},"apply":{"max_sessions":32,"retry_window":8,"max_collections":16,"max_documents":1024,"max_bytes":402653184,"shard_key":"id"},"listeners":{"peer":"127.0.0.1:7411","native":"127.0.0.1:7511","snapshot":"127.0.0.1:7611","control":"127.0.0.1:7711"},"tls":{"certificate":"/run/secrets/vibedb/member-1-cert.pem","key":"/run/secrets/vibedb/member-1-key.pem","roots":"/run/secrets/vibedb/cluster-roots.pem","identity_oid":"1.3.6.1.4.1.32473.1.1"},"authorization_policy":"/srv/vibedb/authorization-policy.vibejson","members":[{"member_id":1,"node_id":"11000000000000000000000000000000","peer_address":"127.0.0.1:7411"},{"member_id":2,"node_id":"12000000000000000000000000000000","peer_address":"127.0.0.1:7412"},{"member_id":3,"node_id":"13000000000000000000000000000000","peer_address":"127.0.0.1:7413"}]}
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

### 4. Start the three members

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
control listener addresses. No RF3 plaintext mode exists.

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
  -catalog-relation 1 \
  -catalog-session-journal ./state/gateway-catalog-session \
  -catalog-client-id 21000000000000000000000000000000 \
  -catalog-retry-home 2200000000000000 \
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
authoritative catalog document must describe the `docs` group prepared above.
The certificate extension, catalog, preparation manifests, and `-shard-peer`
mappings must agree exactly. Add the catalog-group native endpoints to
`-shard-peer` as well. They are omitted from this example because their
operator-assigned addresses and identities are not derivable from the data
group.

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

## Next steps

- [Operate the distributed runtime](distributed.md)
- [Operate replica lifecycle](replica-lifecycle.md)
- [Run the Kubernetes RF3 test lane](kubernetes.md)
- [Check distributed feature state](../distributed-feature-state.md)
