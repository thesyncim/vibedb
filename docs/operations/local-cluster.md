# Run a local RF3 cluster

[Documentation](../README.md) / [Operations](README.md) / Local cluster

Launch a 3-node RF3 cluster on one host with `vibedb cluster dev`, connect via
`psql`, and verify persistence across restart. This is a development
supervisor (shared host, loopback trust SQL, exits all nodes if any child
stops).

## Prerequisites

- macOS or Linux with Go 1.27+ and `psql`.
- A short absolute root path (≤ ~80 chars so `<root>/node-1/pg-ddl.sock` fits
  within Unix socket limits).
- A free loopback SQL port (e.g. `127.0.0.1:7432`).

## 1. Build binaries

```sh
mkdir -p ./bin
GOEXPERIMENT=simd go build -o ./bin/vibedb ./cmd/vibedb
GOEXPERIMENT=simd go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
```

Use `GOEXPERIMENT=nosimd` for portable builds. `vibedb-shard serve-node`
embeds its gateway, so `vibedb-gateway` is not required here.

## 2. Start the cluster

```sh
./bin/vibedb cluster dev \
  --replicas 3 \
  --physical-nodes 3 \
  --root /tmp/vibedb-dev \
  --pg-listen 127.0.0.1:7432
```

Wait for `VibeDB development RF3 physical cluster ready: 127.0.0.1:<port> (3 nodes)`.
This starts 3 `serve-node` processes hosting the catalog, request-ledger, and
built-in data RF3 groups, with node 1 as controller and loopback SQL on
`127.0.0.1:7432` (user `local`, DB `vibedb`, no TLS). Use `--pg-listens
127.0.0.1:7432,127.0.0.1:7532,127.0.0.1:7632` for SQL on all nodes, or
`--physical-nodes 6` for a 6-node cluster.

### Read-authority policy

On Linux, fresh RF3 clusters enable quorum read authority (`CLOCK_BOOTTIME`,
5 s grant, ~6.11 s restart quarantine) automatically; macOS uses `ReadIndex`.
Override on first start with `--read-authority=false` or `--read-authority=true`
(omit on restart to reuse the recorded policy). See
[reads, leases, and time](../design/reads-and-time.md#read-authority).

## 3. Write and read via `psql`

```sh
psql 'postgresql://local@127.0.0.1:7432/vibedb?sslmode=disable'
```

```sql
CREATE TABLE employees (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  team TEXT NOT NULL,
  score INTEGER NOT NULL,
  active BOOLEAN NOT NULL
);
INSERT INTO employees (id, name, team, score, active)
VALUES ('employee-0001', 'Ada', 'Platform', 1, true);
SELECT id, name, team, score, active FROM employees WHERE id = 'employee-0001';
```

RF3 SQL writes are durable autocommit statements (no explicit write
transactions or `RETURNING`). To pre-create tables at startup, pass
`--table-schema <file>` (must be repeated on restart).

## 4. Stop and verify restart

Exit `psql` (`\q`), stop the launcher with `Ctrl-C`, then restart:

```sh
./bin/vibedb cluster dev --root /tmp/vibedb-dev
```

Reconnect with `psql` and run only the `SELECT`—the row must still be present.
Keep `--replicas`, `--physical-nodes`, and the build identical on restart; add
`--diagnostics-on-exit` if debugging a startup/shutdown failure.

## Root layout and limitations

`<root>` is `0700` and contains private keys (`cluster.vibejson`,
`authorization-policy.vibejson`, `durable-ack-key`, `wal-key-source`,
`node-<n>/`, `backups/`, `.supervisor.lock`)—never share it in bug reports.

- Single host only; if any node exits, the supervisor stops all nodes.
- Generated client identity has `data_read`/`data_write` only (use
  `vibedb cluster init-operator` for `topology`/`membership`/`backup`).

## Source map

- [cmd/vibedb/cluster_dev.go](../../cmd/vibedb/cluster_dev.go), [cluster_dev_physical.go](../../cmd/vibedb/cluster_dev_physical.go), [cluster_dev_ddl.go](../../cmd/vibedb/cluster_dev_ddl.go)
- [cmd/vibedb-shard/serve_node.go](../../cmd/vibedb-shard/serve_node.go)
