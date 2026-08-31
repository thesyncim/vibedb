# Local RF3 cluster

> [!CAUTION]
> **Development and qualification only.** VibeDB is under active development.
> Commands, manifests, wire and disk formats, and persisted state may break at
> any commit. This workflow is not a production deployment and provides no
> upgrade or support contract.

This page starts the smallest useful distributed VibeDB environment: three replicas each for the catalog, request ledger, and data group, plus one gateway and its loopback PostgreSQL endpoint. The command creates disposable local PKI and authority material for you. Do not hand-write manifests for this tutorial.

## Prerequisites

- Go 1.26, as declared by `go.mod`
- a local checkout of this repository
- `psql` if you want to run the SQL examples
- a free loopback TCP port; this page uses `127.0.0.1:7432`

Run all commands from the repository root.

## Build the three processes

The launcher resolves `vibedb-shard` and `vibedb-gateway` beside its own executable, so keep these binaries together:

```bash
mkdir -p ./bin
go build -o ./bin/vibedb ./cmd/vibedb
go build -o ./bin/vibedb-shard ./cmd/vibedb-shard
go build -o ./bin/vibedb-gateway ./cmd/vibedb-gateway
```

## Start RF3 with PostgreSQL

The root must be a clean absolute path. It may be absent or empty on the first run.

```bash
./bin/vibedb cluster dev \
  --replicas 3 \
  --root "$PWD/.vibedb-dev" \
  --pg-listen 127.0.0.1:7432
```

Wait for:

```text
VibeDB development cluster ready: <gateway-address>
```

The launcher has now started nine shard processes and one gateway. The PostgreSQL endpoint is intentionally limited to loopback, trust authentication, user `local`, database `vibedb`, and no TLS.

Connect from another terminal:

```bash
psql 'postgresql://local@127.0.0.1:7432/vibedb?sslmode=disable'
```

Create a table, write one row, and read it back:

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
VALUES ('employee-0001', 'Employee 1', 'Platform', 'Lisbon', 1, true);

SELECT id, name, team, city, score, active
FROM employees
WHERE id = 'employee-0001';
```

Online `CREATE TABLE` provisions an additional replicated group on the same three data processes. Development table names must be lower-case identifiers, each declaration must have one primary key, and the launcher retains at most 63 additional tables.

## Stop and restart

Press `Ctrl-C` in the launcher terminal. The supervisor asks children to stop in reverse order, waits up to ten seconds, and then kills any process that remains.

Run the same start command to reopen the cluster. The topology count must match the retained `cluster.vibejson`; switching an existing root between RF1 and RF3 is rejected. A root with unrelated or noncanonical contents is also rejected.

Add `--diagnostics-on-exit` when debugging. It prints a bounded tail from each child after shutdown.

## RF1 is different

For a faster failure-free development loop:

```bash
./bin/vibedb cluster dev \
  --replicas 1 \
  --root "$PWD/.vibedb-rf1"
```

RF1 starts three independent single-member Raft groups. It has no gateway, accepts no `--pg-listen`, and provides no replication or high availability. Its readiness line explicitly reports `development RF1 ready (no HA)`.

## What this command does not provide

- production PKI, secret rotation, or user authentication
- mixed-version or rolling-upgrade compatibility
- backups, monitoring, capacity planning, or failure-domain placement
- a stable network, manifest, or disk-format contract
- a production PostgreSQL service

Use the [CLI reference](../reference/cli.md) for the complete launcher surface. The Kubernetes path is a [qualification lane](kubernetes.md), not a deployment guide.

## Source map

| Behavior | Source |
| --- | --- |
| launcher flags, root validation, RF1/RF3 topology, lifecycle | `cmd/vibedb/cluster_dev.go` |
| retained table groups | `cmd/vibedb/cluster_dev_tables.go` |
| online development DDL supervisor | `cmd/vibedb/cluster_dev_ddl.go` |
| pgwire identity and resource bounds | `cmd/vibedb-gateway/pgwire.go` |
| end-to-end online DDL and restart test | `cmd/vibedb-gateway/pgwire_ddl_process_test.go` |
