# Run a local RF3 cluster

[Documentation](../README.md) / [Operations](README.md) / Local cluster

Start three physical serving nodes, connect with psql, and verify a write
survives a clean restart. Each node combines a SQL frontend, Raft replicas,
and shared node storage. The launcher creates local credentials and manifests.

This is a Linux development environment with loopback SQL trust authentication.
Use the same build to reopen its data; see [compatibility](../status.md).

## Prerequisites

- A Linux host or Linux VM/container with a filesystem that supports strict
  allocation for sealed recovery journals. Use a suitable Linux data volume
  in a container; an overlay filesystem can reject the required allocation.
  Native macOS cannot prepare this RF3 profile.
- Go 1.27 or later and a repository checkout.
- `psql` for the SQL steps.
- A short, absolute path for disposable state, with space for node files.
- Free loopback port `7432` for the SQL listener.

Run the build and launcher commands from the repository root.

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
that this path is on a supported filesystem. In a container, mount the Linux
data volume at this path before starting.

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

### Optional read-authority qualification

To exercise the explicit quorum read-authority protocol on the same RF3
topology, add `--read-authority` on the first start and every restart:

```sh
./bin/vibedb cluster dev \
  --replicas 3 \
  --physical-nodes 3 \
  --read-authority \
  --root /tmp/vibedb-read-authority \
  --pg-listen 127.0.0.1:7432
```

This switch is disabled by default and requires Linux `CLOCK_BOOTTIME`. The
deployment assumption is that every participant's elapsed clock rate stays
within ±10% of real elapsed time, including across VM or container suspension
and resume. `CLOCK_BOOTTIME` availability and one successful `Now` call at
startup cannot prove that rate assumption or future suspend behavior; qualify
the host and virtualization environment separately. Every RF3 voter receives
the same persisted v1 contract: a 5 s elapsed-clock maximum grant, 100000 ppm
clock-rate bound, 1 ms rounding margin, and the complete voter set. The
drift-adjusted usable grant is about 4.09 s of elapsed-clock time. A promise
can delay a follower's election edge by the configured 5 s elapsed-clock grant
window; this is not a hard wall-clock upper bound under the ±10% deployment
assumption (a slow clock can make 5 s about 5.56 s of real time). A restarted
voter enters about 6.11 s of configured elapsed-clock quarantine, including
the margin, before it may vote.

Incarnation observations come from bounded, authenticated native probes run by
the serving process outside the SQL owner. A missing or expired observation,
membership transition, or other failed authority check falls back to the
existing quorum-backed ReadIndex path. An explicit enable on an unsupported
platform is refused before any policy marker is written; leaving the switch
off keeps the ordinary ReadIndex path. The current fast path is limited to
eligible SQL point and batch data reads; this option does not change follower,
recovery, backup, topology, or control reads.

The policy and restart marker are part of the strict manifests and each local
member root. Reusing a root with the flag omitted or changing the policy is
refused. New binaries also refuse an old manifest when an enabled marker is
present. A deployment must keep all voters on the feature-aware binary and
must not restore a pre-enrollment manifest with an old binary while the marker
is live; an old binary cannot interpret a marker it does not know. Use a fresh
root for a different qualification contract until an explicit drain procedure
is available. Online group additions are refused while the authority is
enabled; prepare any additional table groups before the initial start.

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
| Read-authority policy, marker, and incarnation cache | [rf3_read_authority.go](../../cmd/vibedb-shard/rf3_read_authority.go) |
| Qualified elapsed clock and quorum protocol | [authority.go](../../internal/raftauthority/authority.go), [clock_linux.go](../../internal/raftauthority/clock_linux.go) |
| Online DDL and restart | [pgwire_ddl_process_test.go](../../internal/gatewayruntime/pgwire_ddl_process_test.go) |
