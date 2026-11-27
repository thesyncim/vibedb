# Command-line reference

[Documentation](../README.md) / [Reference](README.md) · [Stability](../status.md)

Shipped binaries tied to one exact build. Usage errors generally exit `2`;
runtime failures exit `1` (`vibedb-operator` exits `2` on all errors;
`vibedb-verify` takes positional args and treats `-h` as a path).

| Binary | Role |
| --- | --- |
| `vibedb` | Local RF3 cluster launcher (`cluster dev`) and online operator CLI |
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

### Cluster control, PKI, policy, and version

All online commands authenticate via `--profile <operator-profile.vibejson>`
(`topology` capability for reads; `topology` + `membership` for mutations;
pass `--request-id <64-hex>` for idempotent retries; `--wait` up to `24h`;
`--json` for full structured output):

```sh
vibedb cluster init-operator ...          # mint operator cert & profile from cluster CA
vibedb cluster init-node ...              # mint node cert, WAL key, prepare manifest & descriptor
vibedb cluster rotate-cert ...            # rotate leaf certs preserving binary NodeID
vibedb cluster policy show|grant|revoke   # inspect or atomically edit authorization-policy.vibejson
vibedb cluster version --profile <f> [--check-peers]

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
| `serve-node` | Serve a prepared physical node with embedded frontend (flags: `-manifest`, `-reload-prepared-groups`, `-execution-lanes 8`, `-metrics-http-listen`, `-max-memory-bytes`, `-min-free-disk-bytes`, `-disk-watermark-path`, `-slow-query-threshold`, drain grace) |
| `prepare-rf3` / `serve-rf3` / `bootstrap-rf3` / `adopt-restored-rf3` | Prepare, serve, cold-bootstrap, or adopt standalone RF3 member(s) |
| `init` / `serve` | Static development shard (non-RF3) |

Signals: `SIGUSR1` dumps `rf3-diagnostics.json`; `SIGHUP` reloads prepared
groups when `-reload-prepared-groups` is set; `SIGINT`/`SIGTERM` stop cleanly.

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
  `prepare-gateway`, `restore-group`, `adopt-restore` (see
  [Kubernetes](../operations/kubernetes.md) and
  [backup/restore](../operations/backup-restore.md)).
- `vibedb-kube-qualify`: `write`, `verify`, `measure`, `dns` (Kind lane probes).

## Source map

- [cmd/vibedb/](../../cmd/vibedb/) · [cmd/vibedb-shard/](../../cmd/vibedb-shard/) · [cmd/vibedb-gateway/](../../cmd/vibedb-gateway/)
- [cmd/vibedb-verify/main.go](../../cmd/vibedb-verify/main.go) · [cmd/vibedb-operator/](../../cmd/vibedb-operator/) · [cmd/vibedb-kube-qualify/](../../cmd/vibedb-kube-qualify/)
