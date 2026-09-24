# Run a local RF3 cluster

[Documentation](../README.md) / [Operations](README.md) / Local cluster

Start three physical serving nodes on one host, connect with `psql`, and check
that a write survives a clean restart. `vibedb cluster dev` creates the
credentials, manifests, and data for a disposable development cluster and
supervises its processes. It is a development tool: all nodes share one host,
SQL is loopback-only with trust authentication, and the supervisor stops the
whole cluster if any node exits. See [deployment readiness](production-readiness.md)
for what this does and does not establish.

## Prerequisites

- macOS or Linux on a local writable filesystem. CI runs this procedure's
  create, write, restart, and read cycle on both (`portable-rf3` in
  `.github/workflows/ci.yml`). RF3 recovery journals use portable
  fixed-capacity allocation, so APFS and ordinary Linux filesystems work.
  Disk-full and I/O errors still fail the operation. Native Windows is not
  supported.
- Go 1.27 or later and a checkout of the exact revision you want to run.
- `psql` for the SQL steps.
- A short absolute path for the cluster root, about 80 characters or fewer.
  With SQL enabled, the DDL socket lives at `<root>/node-1/pg-ddl.sock`, and
  Unix socket paths are limited to 104 bytes on macOS and 108 on Linux. A root
  that is too long is refused with
  `cluster root is too long for the PostgreSQL DDL Unix socket`.
- A free loopback port for SQL (this guide uses `7432`). The launcher also
  reserves ephemeral loopback ports for internal listeners at first start and
  records them for every restart.

Run every command from the repository root.

## 1. Build the binaries

Build the launcher and the node binary into one directory. The launcher looks
for `vibedb-shard` beside itself first, then on `PATH`.

```sh
mkdir -p ./bin
GOEXPERIMENT=simd go build -o ./bin/vibedb ./cmd/vibedb
GOEXPERIMENT=simd go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
```

Use `GOEXPERIMENT=nosimd` for a portable build; see [SIMD](../simd.md). RF3
nodes embed their frontend, so `vibedb-gateway` is not needed for this guide.

## 2. Start the cluster

Choose a root that does not exist or is empty:

```sh
./bin/vibedb cluster dev \
  --replicas 3 \
  --physical-nodes 3 \
  --root /tmp/vibedb-dev \
  --pg-listen 127.0.0.1:7432
```

`--replicas 3 --physical-nodes 3` are the defaults and are shown for clarity.
First start prepares every node before serving. Wait for:

```text
VibeDB development RF3 physical cluster ready: 127.0.0.1:<port> (3 nodes)
```

The address is node 1's native TLS client listener, not the SQL port. What is
now running:

- The supervisor (`vibedb cluster dev`) and three `vibedb-shard serve-node`
  processes, one per physical node.
- Three RF3 groups: catalog, request ledger, and the built-in data table.
  Each has one voting replica on every node.
- An embedded frontend in every node. Node 1 is the designated controller for
  moves, splits, scaling, and backups.
- One PostgreSQL listener on node 1 at the `--pg-listen` address: user
  `local`, database `vibedb`, trust authentication, no TLS. The other nodes
  have SQL disabled.

Each node must report ready within 30 seconds or the launcher stops with
`development cluster readiness timeout`.

To give every node a SQL listener, use `--pg-listens` with one distinct
literal loopback endpoint per node instead of `--pg-listen`:

```sh
./bin/vibedb cluster dev --root /tmp/vibedb-dev \
  --pg-listens 127.0.0.1:7432,127.0.0.1:7532,127.0.0.1:7632
```

The two flags are mutually exclusive. `localhost`, `0.0.0.0`, and non-loopback
addresses are refused.

### Read-authority policy

On Linux, a fresh RF3 physical cluster enables a quorum read-authority fast
path for eligible SQL point and batch reads when the `CLOCK_BOOTTIME` elapsed
clock is available. On macOS the clock is unavailable, so fresh clusters use
ordinary ReadIndex reads. Both paths return linearizable reads.

| Flag on first start | Effect |
| --- | --- |
| omitted | Platform default, recorded in the root |
| `--read-authority=false` | ReadIndex only, recorded |
| `--read-authority=true` | Required; refused before any marker is written if unsupported |

On restart, omit the flag to reuse the recorded policy. An explicit value that
differs from the recorded one is refused. The policy assumes every node's
elapsed clock runs within ±10% of real time, including across VM suspension;
the launcher cannot verify that. The grant is 5 s of elapsed-clock time and a
restarted voter waits about 6.11 s before voting. A missing or expired
observation, a membership change, or any failed check falls back to ReadIndex.
Details and evidence: [read-authority qualification](../qualification/read-authority-2026-09-05/README.md).

## 3. Write and read a row

In another terminal:

```sh
psql 'postgresql://local@127.0.0.1:7432/vibedb?sslmode=disable'
```

```sql
CREATE TABLE employees (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  team TEXT NOT NULL,
  city TEXT,
  score INTEGER NOT NULL,
  active BOOLEAN NOT NULL
);

INSERT INTO employees (id, name, team, city, score, active)
VALUES ('employee-0001', 'Ada', 'Platform', 'Lisbon', 1, true);

SELECT id, name, team, city, score, active
FROM employees
WHERE id = 'employee-0001';
```

The launcher terminal prints `development DDL preparing table="employees"`
while it provisions a new replicated group for the table on the existing
nodes. Expect one row with ID `employee-0001` and score `1`.

Tables need lower-case identifiers and one primary key. Writes are durable
autocommit statements; explicit transactions cannot contain writes, and
`RETURNING` is refused. See the [SQL reference](../reference/sql.md) and the
[protocol reference](../reference/protocols.md#postgresql-wire-adapter).

To provision tables at startup instead, pass `--table-schema <file>` once per
file; each file holds one `CREATE TABLE` with one primary key. The list is
retained and must be repeated on restart.

## 4. Stop and verify a restart

Exit `psql` with `\q`, then press `Ctrl-C` in the launcher terminal. The
supervisor sends SIGTERM to every node, waits up to 10 seconds in total, then
kills any that remain.

Restart with the same build and root:

```sh
./bin/vibedb cluster dev --root /tmp/vibedb-dev
```

After the readiness line, reconnect and run only the `SELECT` from step 3. The
row must still be present. Do not repeat the `CREATE TABLE` or `INSERT`.

Restart rules:

- `--replicas` and `--physical-nodes` must match the root. Because
  `--physical-nodes` defaults to 3, a six-node root must be restarted with
  `--physical-nodes 6`.
- Omitting `--pg-listen`/`--pg-listens` keeps the recorded SQL endpoints.
  A different endpoint is refused with
  `PostgreSQL endpoints differ from retained node configuration`.
- Internal ports are fixed at first start. If another process holds one, the
  restart fails; free the port rather than editing manifests.
- A root written by another build is refused; see [upgrades](upgrades.md).

**Success check:** the readiness line appears and the `SELECT` returns the row
written before the stop.

For startup or shutdown failures, add `--diagnostics-on-exit` to print the
last 64 KiB of each node's output when the supervisor stops, then follow
[troubleshooting](troubleshooting.md).

## Files in the root

The root is private (`0700`) and contains keys. Do not copy it into reports.

| Path | Contents |
| --- | --- |
| `cluster.vibejson` | Topology manifest; format checked exactly on every start |
| `authorization-policy.vibejson` | Node IDs and their capabilities |
| `durable-ack-key`, `wal-key-source` | Cluster secrets |
| `hot-shard-capacity.vibejson`, `replica-control.vibejson` | Controller inputs |
| `node-<n>/` | One physical node: manifests, node log, SQL state, `rf3-diagnostics.json` |
| `backups/` | Backup repository of node 1's frontend |
| `.supervisor.lock` | Held by the running supervisor; a second supervisor is refused |

## Other development topologies

| Flags | Layout | SQL |
| --- | --- | --- |
| `--physical-nodes 3` (default) | Three nodes, each hosting every group's replica | `--pg-listen` on node 1, or `--pg-listens` on all |
| `--physical-nodes 6` | Six nodes; each group still has three replicas, spread across subsets | As above, with six endpoints for `--pg-listens` |
| `--replicas 1` | Three independent single-member groups; no HA, no frontend | None; physical-node and SQL flags are refused |

Use a fresh root for each topology. Changing the replica count, node count, or
SQL endpoints of an existing root is refused, not migrated.

## Limitations

- All nodes run on one host; this demonstrates replication, not host-failure
  tolerance.
- If any node exits, the supervisor stops the cluster. It never restarts a
  node, so you cannot watch the cluster serve with a node down.
- The launcher cannot add or remove nodes. [Online scaling](scaling.md)
  needs an operator credential and hand-built join inputs.
- The generated client identity can read and write data only. It cannot
  request metrics, run cluster control, or start backups.
- SQL is loopback-only, unauthenticated, and unencrypted.
- The migration budget and hot-shard capacity profile are fixed; there are no
  flags to change them.

## Next steps

- [Observe node activity](observability.md).
- [Look up launcher flags](../reference/cli.md#cluster-dev).
- [Understand routing, quorum, and retries](distributed.md).
- [Plan for failures](node-failure.md).

## Source map

| Behavior | Source |
| --- | --- |
| Flags, root validation, supervision, shutdown | [cluster_dev.go](../../cmd/vibedb/cluster_dev.go) |
| Placement, credentials, SQL endpoints | [cluster_dev_physical.go](../../cmd/vibedb/cluster_dev_physical.go), [port tests](../../cmd/vibedb/cluster_dev_ports_test.go) |
| Online table provisioning | [cluster_dev_ddl.go](../../cmd/vibedb/cluster_dev_ddl.go), [cluster_dev_physical_tables.go](../../cmd/vibedb/cluster_dev_physical_tables.go) |
| Node composition | [serve_node.go](../../cmd/vibedb-shard/serve_node.go) |
| Read-authority policy | [cluster_dev_read_authority.go](../../cmd/vibedb/cluster_dev_read_authority.go), [rf3_read_authority.go](../../cmd/vibedb-shard/rf3_read_authority.go) |
| Create, write, restart qualification | [pgwire_ddl_process_test.go](../../internal/gatewayruntime/pgwire_ddl_process_test.go) |
