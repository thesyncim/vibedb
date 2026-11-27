# Scale out, rebalance, and decommission nodes

[Documentation](../README.md) / [Operations](README.md) / Scaling

Add an empty physical node, rebalance replicas, and decommission a node online
without stopping traffic using `vibedb cluster`. Qualified on Linux via
`seamless-scale-in-out.yml` (3 → 4 → 3 cycles under open-loop traffic). See
[topology changes](../design/topology-changes.md#seamless-scale-out-scale-in-and-frontend-drain)
for internals.

## Prerequisites: bootstrap operator and node credentials

All nodes and CLI binaries must run the exact same build. Mint credentials from
your cluster CA (point `--address` at a frontend you do **not** plan to retire):

```sh
# 1. Mint operator cert + operator-profile.vibejson and grant membership+topology
vibedb cluster init-operator --ca-cert /abs/ca.pem --ca-key /abs/ca.key \
  --trust-domain-from /abs/node-1/node-cert.pem \
  --address 127.0.0.1:17400 --server-node <32-hex-gateway-node-id> \
  --out-dir /abs/operator --policy /abs/authorization-policy.vibejson --update-policy

# 2. Mint empty node cert, prepare-node.vibejson & public node-descriptor.vibejson
vibedb cluster init-node --ca-cert /abs/ca.pem --ca-key /abs/ca.key \
  --trust-domain-from /abs/node-1/node-cert.pem \
  --root /abs/node-4/data --out-dir /abs/node-4/init \
  --policy /abs/authorization-policy.vibejson --update-policy \
  --failure-domain rack-4 --peer 127.0.0.1:18401 --native 127.0.0.1:18402 \
  --snapshot 127.0.0.1:18403 --control 127.0.0.1:18404

# 3. Prepare and start the empty node
vibedb-shard prepare-node-rf3 -manifest /abs/node-4/init/prepare-node.vibejson
vibedb-shard serve-node -manifest /abs/node-4/data/serve-rf3.vibejson -reload-prepared-groups
```

## List nodes

```sh
vibedb cluster nodes --profile /abs/operator/operator-profile.vibejson
```

Record each node's 32-hex ID and `incarnation` (required by `decommission`).

## Add a node (`join`)

Always pass an explicit 64-hex `--request-id` so timeouts or disconnects can be
retried idempotently:

```sh
req_id=$(openssl rand -hex 32)
vibedb cluster join --profile /abs/operator/operator-profile.vibejson \
  --node-file /abs/node-4/init/node-descriptor.vibejson \
  --request-id "$req_id" --wait 10m --json

vibedb cluster status --profile /abs/operator/operator-profile.vibejson \
  --operation <operation-id> --wait 5m --json
```

**Success:** `state=complete`, no blockers, node `active` in `cluster nodes`.

## Rebalance

```sh
vibedb cluster rebalance --profile /abs/operator/operator-profile.vibejson \
  --desired-node-count 4 --max-moves 32 --max-migration-bytes 67108864 \
  --request-id "$(openssl rand -hex 32)" --wait 10m --json
```

Flags: `--desired-node-count` (≤4096), `--max-moves` (default 4096),
`--max-migration-bytes` (default 1 GiB), `--hysteresis-ppm` (default 0). Move
and byte budgets apply across the entire durable operation (including restarts).

## Decommission a node

```sh
vibedb cluster decommission --profile /abs/operator/operator-profile.vibejson \
  --node <32-hex-id> --incarnation <n> \
  --request-id "$(openssl rand -hex 32)" --wait 10m --json
```

Poll `cluster status --operation <id> --json` until `safe_to_stop=true`,
`retiring_references=0`, and blockers are empty. Only then stop the process.
Open client sessions on the retiring frontend hold blocker `gateway_sessions`
until they disconnect (or until `-frontend-drain-grace-period` force-closes
them). Do not retire a sole designated controller
(`sole_designated_controller`).

## Read blockers

| Blocker | Meaning | Action |
| --- | --- | --- |
| `serving_replicas`, `learner_replicas`, `outstanding_moves`, `enrolled_targets`, `catalog_voters`, `control_voters` | Replicas or moves still reference the node | Wait; inspect migration pacing |
| `gateway_sessions` | Active client sessions on retiring frontend | Disconnect clients (or wait for drain grace period) |
| `retirement_fence`, `frontend_drain_terminal_ack_pending` | Final scan or drain ACK not yet durable | Keep polling; do not stop node |
| `move_execution` | Transient move error | Check node health/logs |
| `sole_designated_controller` | Node runs the only controller | Fail over / retire another node |
| `target_not_ready`, `target_not_verified` | Joining node failed readiness probe | Check node process, listeners, TLS |
| `placement_blocked` | No admissible target | Add capacity or lower `--hysteresis-ppm` |

## Retry and exit rules

- `--wait` holds up to `24h` without cancelling the operation on timeout.
- Exit `0` means `ok=true` (still inspect blockers); exit `1` is `ok=false` or
  transport error; exit `2` is usage error. Use `--json` for full phase,
  budget, and moved-group counts.

## Limitations

- No cancel or rollback command for an in-flight scaling operation.
- Request-ledger key ranges do not change online.

## Source map

- [cmd/vibedb/cluster_control.go](../../cmd/vibedb/cluster_control.go) · [cluster_scale_init.go](../../cmd/vibedb/cluster_scale_init.go) · [internal/clustercontrol/control.go](../../internal/clustercontrol/control.go)
- [internal/gatewayruntime/cluster_control.go](../../internal/gatewayruntime/cluster_control.go) · [scaling_controller.go](../../internal/gatewayruntime/scaling_controller.go)
