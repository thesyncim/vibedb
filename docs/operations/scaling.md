# Scale out, rebalance, and decommission nodes

[Documentation](../README.md) / [Operations](README.md) / Scaling

Add an empty physical node, rebalance replicas, and decommission a node online
without stopping traffic using `vibedb cluster`. Qualified on Linux via
`seamless-scale-in-out.yml` (3 → 4 → 3 cycles under open-loop traffic). See
[topology changes](../design/topology-changes.md#seamless-scale-out-scale-in-and-frontend-drain)
for internals.

## Prerequisites

1. **Same build** across all nodes and CLI binaries.
2. **Bootstrap operator & node credentials** from your cluster CA:
   - `vibedb cluster init-operator` — mints operator cert and canonical
     `operator-profile.vibejson` (requires `topology` + `membership` in
     `authorization-policy.vibejson`; pass `--policy <path> --update-policy`).
     Point the profile at a frontend you do **not** plan to retire.
   - `vibedb cluster init-node` — mints node cert, WAL key,
     `prepare-node.vibejson`, and public `node-descriptor.vibejson` (no private
     keys/paths).
   - Prepare & start the empty node with `vibedb-shard prepare-node-rf3` and
     `vibedb-shard serve-node`.

## List nodes

```sh
vibedb cluster nodes --profile /path/operator-profile.vibejson
```

Record each node's 32-hex ID and `incarnation` (required by `decommission`).

## Add a node (`join`)

Always pass an explicit 64-hex `--request-id` so timeouts or disconnects can be
retried idempotently:

```sh
req_id=$(openssl rand -hex 32)
vibedb cluster join --profile /path/operator-profile.vibejson \
  --node-file /path/node-descriptor.vibejson \
  --request-id "$req_id" --wait 10m --json

vibedb cluster status --profile /path/operator-profile.vibejson \
  --operation <operation-id> --wait 5m --json
```

**Success:** `state=complete`, no blockers, node `active` in `cluster nodes`.

## Rebalance

```sh
vibedb cluster rebalance --profile /path/operator-profile.vibejson \
  --desired-node-count 4 --max-moves 32 --max-migration-bytes 67108864 \
  --request-id "$(openssl rand -hex 32)" --wait 10m --json
```

Flags: `--desired-node-count` (≤4096), `--max-moves` (default 4096),
`--max-migration-bytes` (default 1 GiB), `--hysteresis-ppm` (default 0). Move
and byte budgets apply across the entire durable operation (including restarts).

## Decommission a node

```sh
vibedb cluster decommission --profile /path/operator-profile.vibejson \
  --node <32-hex-id> --incarnation <n> \
  --request-id "$(openssl rand -hex 32)" --wait 10m --json
```

Poll `cluster status --operation <id> --json` until `safe_to_stop=true`,
`retiring_references=0`, and blockers are empty. Only then stop the process.
Open client sessions on the retiring frontend hold blocker `gateway_sessions`
until they disconnect (or until `FrontendDrainGracePeriod` force-closes them if
configured). Do not retire a sole designated controller
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

- [cmd/vibedb/cluster_control.go](../../cmd/vibedb/cluster_control.go) · [internal/clustercontrol/control.go](../../internal/clustercontrol/control.go)
- [internal/gatewayruntime/cluster_control.go](../../internal/gatewayruntime/cluster_control.go) · [scaling_controller.go](../../internal/gatewayruntime/scaling_controller.go)
