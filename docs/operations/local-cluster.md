# Run a local RF3 cluster

[Documentation](../README.md) / [Operations](README.md) / Local cluster

Start three physical serving nodes, connect with psql, and verify a write
survives a clean restart. Each node combines a SQL frontend, Raft replicas,
and shared node storage. The launcher creates local credentials and manifests.

This is a macOS/Linux development environment with loopback SQL trust authentication.
Use the same build to reopen its data; see [compatibility](../status.md).

## Prerequisites

- A macOS or Linux host with a local writable filesystem that supports the
  WAL's native preallocation and durability operations. A normal local
  directory works on macOS/APFS and ordinary Linux container overlay storage.
- Go 1.27 or later and a repository checkout.
- `psql` for the SQL steps.
- A short, absolute path for disposable state, with space for node files.
- Free loopback port `7432` for the SQL listener.

Run the build and launcher commands from the repository root.

RF3 journals use portable fixed-capacity allocation. A normal local directory
is sufficient on macOS/APFS and ordinary Linux filesystems; the journals do not
require Linux's reflink-unsharing operation or an ext4-specific volume.
Their byte limits, checksums, sync barriers, and recovery rules remain enforced.
Portable allocation does not promise that future overwrites cannot run out of
space: disk-full and I/O errors fail the operation, and uncertain outcomes must
be recovered before retrying. The explicit strict allocation APIs retain their
stronger physical-reservation requirement.

Portable journals carry a distinct on-disk flag, and the build's disk grammar
identity has changed. Use the same build for all processes and a fresh development
root when moving from a build with the previous disk identity. Existing roots
are not silently migrated. Native Windows RF3 still requires a separate WAL
namespace/publication port.

## 1. Build the launcher and servers

Keep the binaries together: the launcher resolves companion executables beside
itself, then on `PATH`.

```sh
mkdir -p ./bin
GOEXPERIMENT=simd go build -o ./bin/vibedb ./cmd/vibedb
GOEXPERIMENT=simd go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
GOEXPERIMENT=simd go build -o ./bin/vibedb-gateway ./cmd/vibedb-gateway
```

Use `GOEXPERIMENT=nosimd` in all three commands for a portable build. See
[SIMD](../simd.md) for CPU support and fallback behavior.

## 2. Start the cluster

Choose an absent or empty root on first start. This example uses `/tmp` for
disposable state; files there may be removed by the operating system. Confirm
that this path is on a supported filesystem with enough free space.

```sh
./bin/vibedb cluster dev \
  --replicas 3 \
  --physical-nodes 3 \
  --root /tmp/vibedb-dev \
  --pg-listen 127.0.0.1:7432
```

Wait for the readiness line:

```text
VibeDB development RF3 physical cluster ready: <gateway-address> (3 nodes)
```

There are three serving processes plus the supervisor. Catalog, request-ledger,
and initial data groups each have three replicas. `--pg-listen` assigns the
first node's SQL endpoint; the other nodes keep SQL disabled. SQL uses user
`local`, database `vibedb`, trust authentication, and no TLS. Internal service
traffic uses the generated identities and TLS.

To choose every SQL endpoint explicitly, replace `--pg-listen` with:

```text
--pg-listens 127.0.0.1:7432,127.0.0.1:7532,127.0.0.1:7632
```

The list must contain one distinct literal-loopback endpoint per physical
node. The two listener flags are mutually exclusive.

## 3. Write and read a row

From another terminal:

```sh
psql 'postgresql://local@127.0.0.1:7432/vibedb?sslmode=disable'
```

In psql:

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

Expect one row with ID `employee-0001` and score `1`. Online `CREATE TABLE`
provisions a replicated group on the existing physical nodes. Development
tables require lower-case identifiers and one primary key. See the
[SQL reference](../reference/sql.md) for supported statements.

## 4. Stop and verify a restart

Exit psql with `\q`, then press `Ctrl-C` in the launcher terminal. The supervisor
stops its children, waits up to ten seconds, and kills any that remain.

Run the same launcher command with the same build and root. After readiness,
reconnect and run only the `SELECT` from step 3. The row should still be present.
Do not repeat the `CREATE TABLE` or `INSERT` to test persistence.

The retained `cluster.vibejson` records the topology. Changing replication
factor, physical-node count, or retained frontend configuration is not a
restart operation. The current launcher uses manifest format 2 and rejects
older development layouts.

For startup or shutdown failures, add `--diagnostics-on-exit` to print bounded
child-log tails, then follow [troubleshooting](troubleshooting.md).

## Other development topologies

| Configuration | Serving layout | SQL |
| --- | --- | --- |
| `--replicas 3 --physical-nodes 3` | Three nodes, each hosting catalog, ledger, and data replicas. | One native frontend per node; SQL listeners are optional. |
| `--replicas 3 --physical-nodes 6` | Six nodes; each group still has three replicas, with placement spread across subsets. | One native frontend per node; SQL listeners are optional. |
| `--replicas 1` | Three independent single-member Raft groups; no HA. | No gateway or SQL listener. |

Use a fresh root for each topology. RF1 rejects physical-node and PostgreSQL
listener flags. Six local processes share one host and therefore do not
establish resilience to host loss or multi-machine scaling.

## Next steps

- [Observe node and gateway activity](observability.md).
- [Understand routing, quorum, and retries](distributed.md).
- [Look up launcher flags](../reference/cli.md#cluster-dev).
- [Run the separate Kind qualification topology](kubernetes.md).

## Source map

| Behavior | Source |
| --- | --- |
| Flags, root validation, lifecycle | [cluster_dev.go](../../cmd/vibedb/cluster_dev.go) |
| Node placement and SQL ports | [cluster_dev_physical.go](../../cmd/vibedb/cluster_dev_physical.go), [port tests](../../cmd/vibedb/cluster_dev_ports_test.go) |
| Node preparation | [cluster_dev_node.go](../../cmd/vibedb/cluster_dev_node.go) |
| Online table placement | [cluster_dev_physical_tables.go](../../cmd/vibedb/cluster_dev_physical_tables.go) |
| Frontend composition | [serve_node.go](../../cmd/vibedb-shard/serve_node.go) |
| Online DDL and restart | [pgwire_ddl_process_test.go](../../internal/gatewayruntime/pgwire_ddl_process_test.go) |
