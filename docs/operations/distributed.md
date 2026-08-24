# Operate the distributed runtime

The shipped distributed runtime is experimental. It consists of a stateless
gateway and leader-only shard services. It uses static ownership data from a
catalog file.

> **WARNING:** The gateway and shard protocols do not have authentication or
> TLS. The commands refuse non-loopback listeners. Do not expose these
> listeners through a proxy or port forward to an untrusted network.

The shipped commands do not start Raft replication. One `vibedb-shard` process
serves one local store with one static ownership identity. The Raft and
replicated-state packages are a separate non-serving kernel. That kernel can
coalesce a currently queued normal-proposal prefix into one `Ready` under
fixed entry and byte targets, but still has no serving result waiters,
committed-entry apply batch, or outbound-frame batch.

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

This repository has no catalog administration CLI. Application or operator
code must construct and publish a valid `gateway.Snapshot`. `SaveSnapshot` and
`SaveSnapshotAfter` publish the file for cooperating writers.

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

## Start the shard service

```bash
./bin/vibedb-shard serve \
  -store ./shard-a.vdb \
  -listen 127.0.0.1:7401 \
  -distribution tenants \
  -shard shard-a \
  -allocation-generation 1 \
  -epoch 1 \
  -routing-version 1
```

The service makes a durable local claim before it listens. The store retains
independent high-water marks for ownership epoch and routing version. A later
claim can reuse equal coordinates or advance either coordinate, but cannot
lower either one. Only one live claim can exist for one open store.

This claim is a local fence. It is not an election or a distributed lease. It
cannot stop another process that serves a copied store.

The default connection limit is 128. Use `-max-connections -1` only when an
external control gives a safe bound. The service always bounds read fences and
worker exchange resources.

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
  -listen 127.0.0.1:7400
```

Each `Query` or `Exec` attempt pins one immutable catalog generation. The
default executor retries a stale refusal at most twice, for at most three
attempts and two reloads. Each retry must load a strictly newer generation. A
multi-statement `ExecBatch` pins one generation and has no stale-retry loop. A
one-statement batch delegates to `Exec`.

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

The parameter kinds are `null`, `bool`, `number`, `string`, and `document`.
The client cannot select a shard or supply a serialized plan. SQL and catalog
placement determine the route.

A response can contain columns, raw JSON rows, affected-row count, route type,
catalog generation, fanout count, retry count, or an error.

One request is limited to 1 MiB before JSON decoding. The shard wire protocol
has a separate frame limit of 16 MiB plus 64 KiB.

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

The service accepts only the `ReadStrong` policy and serves a statement-level
snapshot from its statically configured leader endpoint. It refuses session
reads, stale reads, and minimum applied positions. Because the service has no
Raft election or replication, `ReadStrong` does not prove cluster-wide
linearizability or revoke a copied store.

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

- Raft replication for `vibedb-shard`
- Follower reads or bounded-staleness reads
- Endpoint failover or load balancing
- Catalog replication between gateway hosts
- Automatic shard split execution or data movement
- Distributed DDL or schema-rollout validation
- A replica-move controller
- Authenticated gateway, shard, or Raft networking
- Cross-host worker exchange from CLI configuration
- Online Raft snapshot transport

Routing, catalog validation, and shard admission fail closed. A distributed
read returns no partial result when one shard fails.

## Implementation references

- `cmd/vibedb-gateway/main.go` and `serve.go`
- `cmd/vibedb-shard/main.go`
- `gateway/catalog.go`, `executor.go`, `profile.go`, and `transaction.go`
- `gateway/recovery.go` and `read_snapshot.go`
- `distribution/manifest.go`, `placement.go`, `policy.go`, and `router.go`
- `shardservice/admit.go`, `server.go`, `wire.go`, and `read_fence.go`
- `sql/driver/shard_store.go`
