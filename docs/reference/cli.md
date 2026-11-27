# Command-line reference

[Documentation](../README.md) / [Reference](README.md) · [Stability](../status.md)

Shipped binaries tied to one exact build. Usage errors generally exit `2`;
runtime failures exit `1` (`vibedb-operator` exits `2` on all errors;
`vibedb-verify` takes positional args and treats `-h` as a path).

| Binary | Role |
| --- | --- |
| `vibedb` | Local RF3 cluster launcher (`cluster dev`) and operator CLI (PKI, policy, version, scaling) |
| `vibedb-shard` | Physical-node (`serve-node`), RF3 member (`serve-rf3`), and static shard server |
| `vibedb-gateway` | Standalone gateway, schema rollout, and restore activation |
| `vibedb-verify` | Offline `verify`, `salvage`, and `repack` for durable store files |
| `vibedb-operator` | Kind manifest renderer/bootstrapper and RF3 restore helpers |
| `vibedb-kube-qualify` | Kind qualification probe (`write`, `verify`, `measure`, `dns`) |

## `vibedb`

### `cluster dev`

Launches and supervises a local RF3 (or RF1 no-HA) development cluster
([local cluster guide](../operations/local-cluster.md)):

```sh
vibedb cluster dev --root <abs-path> [--replicas 3] [--physical-nodes 3|6] \
  [--pg-listen 127.0.0.1:7432 | --pg-listens addr1,addr2,...] \
  [--table-schema file.sql] [--read-authority=true|false] \
  [--tls-ca-certificate ca.pem --tls-ca-key ca.key] \
  [--shard-binary path] [--diagnostics-on-exit]
```

Child readiness timeout: 30 s; shutdown grace: 10 s; retained child log tail:
64 KiB. Exits all children if any node stops.

### PKI, policy, and version

```sh
# Mint operator cert & operator-profile.vibejson from cluster CA
vibedb cluster init-operator --ca-cert ca.pem --ca-key ca.key \
  --trust-domain-from existing-leaf.pem --address 127.0.0.1:17400 \
  --server-node <32-hex> --out-dir /abs/op [--policy policy.vibejson --update-policy] [--valid-days 30]

# Mint empty storage node credentials, prepare-node.vibejson & node-descriptor.vibejson
vibedb cluster init-node --ca-cert ca.pem --ca-key ca.key \
  --trust-domain-from existing-leaf.pem --root /abs/node-data --out-dir /abs/node-out \
  --policy /abs/authorization-policy.vibejson [--update-policy] --failure-domain rack-4 \
  --peer host:p1 --native host:p2 --snapshot host:p3 --control host:p4 [--gateway host:p5]

# Rotate leaf certificate in place (preserving binary NodeID)
vibedb cluster rotate-cert --ca-cert ca.pem --ca-key ca.key \
  --cert leaf.pem --key leaf.key [--valid-for 8760h] [--node-descriptor node-descriptor.vibejson]

# Inspect or atomically edit authorization-policy.vibejson
vibedb cluster policy show   --policy /abs/authorization-policy.vibejson
vibedb cluster policy grant  --policy /abs/authorization-policy.vibejson --node-id <hex> <cap>...
vibedb cluster policy revoke --policy /abs/authorization-policy.vibejson --node-id <hex> [--all | <cap>...]

# Check binary version window & peer rolling-upgrade compatibility
vibedb cluster version [--check-peers 1.0,1.1] [--active 1.0] [--json]
```

### Online cluster scaling

Authenticate via `--profile <operator-profile.vibejson>` (`topology` for reads;
`topology` + `membership` for mutations; pass `--request-id <64-hex>` for
idempotent retries; `--wait` up to `24h`; `--json` for full structured output):

```sh
vibedb cluster nodes        --profile <f> [--json] [--wait d]
vibedb cluster join         --profile <f> --node-file <desc> [--request-id hex] [--json] [--wait d]
vibedb cluster rebalance    --profile <f> [--desired-node-count n] [--max-moves n] [--max-migration-bytes n] [--hysteresis-ppm n] [--request-id hex] [--json] [--wait d]
vibedb cluster decommission --profile <f> --node <32-hex> --incarnation n [--request-id hex] [--json] [--wait d]
vibedb cluster status       --profile <f> --operation <64-hex> [--json] [--wait d]
```

## `vibedb-shard`

| Subcommand | Purpose |
| --- | --- |
| `prepare-node-rf3` / `prepare-node-group-rf3` | Prepare a physical node (shared node log + 0..64 groups) or add one group |
| `serve-node` | Serve a prepared physical node with embedded frontend |
| `prepare-rf3` / `serve-rf3` / `bootstrap-rf3` / `adopt-restored-rf3` | Prepare, serve, cold-bootstrap, or adopt standalone RF3 member(s) |
| `init` / `serve` | Static development shard (non-RF3) |

### `serve-node` flags

All CLI `RuntimeOps` flags are preserved across `SIGHUP` manifest reloads:

| Flag | Default | Meaning |
| --- | ---: | --- |
| `-manifest` | required | Canonical prepared physical-node manifest |
| `-reload-prepared-groups` | `false` | Allow `SIGHUP` to append durably prepared groups |
| `-execution-lanes` | `8` | Power-of-two Raft execution lanes (1–64) |
| `-metrics-http-listen` | `""` | Loopback HTTP observability listener (`/healthz`, `/readyz`, `/metrics`) |
| `-frontend-drain-grace-period` | `0` | Force-close lingering sessions after grace window (`0` waits indefinitely) |
| `-max-memory-bytes` | `0` | Hard process memory admission watermark (`0` disables) |
| `-min-free-disk-bytes` / `-disk-watermark-path` | `0` / manifest dir | Hard minimum free filesystem bytes admission watermark |
| `-slow-query-threshold` | `0` | Log slow reads/writes exceeding duration (`0` disables) |
| `-controller-lease-dir` | `""` | Shared dir for durable CAS-fenced controller lease failover |
| `-controller-lease-key` / `-controller-lease-ttl` | `vibedb/controller/primary` / `10s` | Lease key and TTL when `-controller-lease-dir` is set |

Signals: `SIGUSR1` dumps `rf3-diagnostics.json`; `SIGHUP` reloads prepared
groups (retaining all `RuntimeOps`); `SIGINT`/`SIGTERM` stop cleanly.

## `vibedb-gateway`

| Subcommand | Purpose |
| --- | --- |
| `inspect` / `validate` | Inspect or validate a catalog file (`-catalog`) |
| `serve` | Standalone gateway (`-catalog`, `-catalog-route-seed`, TLS/policy, `-replica-control-manifest`, `-backup-repository`, `-metrics-http-listen`, `-max-memory-bytes`, `-min-free-disk-bytes`, `-disk-watermark-path`, `-slow-query-threshold`, `-pg-dev-listen`, etc.) |
| `schema-rollout` | Run one `N → N+1` schema rollout (`-schema-rollout-plan`) |
| `restore-activate` | Activate restored RF3 groups (`-manifest`) |

## `vibedb-verify` and Kubernetes helpers

```sh
vibedb-verify verify  <store-file|database-dir>   # read-only structural / txn check
vibedb-verify salvage <store-file> <output-file>  # read-only partial inline salvage
vibedb-verify repack  <store-file> <output-file>  # opens source read/write; use a copy!
```

- `vibedb-operator`: `bootstrap`, `render`, `validate`, `prepare`,
  `prepare-gateway`, `restore-group`, `adopt-restore` ([Kubernetes](../operations/kubernetes.md),
  [backup/restore](../operations/backup-restore.md)).
- `vibedb-kube-qualify`: `write`, `verify`, `measure`, `dns` (Kind lane probes).

## Source map

- [cmd/vibedb/](../../cmd/vibedb/) · [cmd/vibedb-shard/serve_node.go](../../cmd/vibedb-shard/serve_node.go) · [cmd/vibedb-gateway/](../../cmd/vibedb-gateway/)
- [cmd/vibedb-verify/main.go](../../cmd/vibedb-verify/main.go) · [cmd/vibedb-operator/](../../cmd/vibedb-operator/) · [cmd/vibedb-kube-qualify/](../../cmd/vibedb-kube-qualify/)
