# Command-line reference

[Documentation](../README.md) / [Reference](README.md) · [Development status](../status.md)

The repository builds six operator-relevant commands. They are development
interfaces tied to one exact build; flags and output can change.

| Binary | Role | Status |
| --- | --- | --- |
| `vibedb` | Local cluster launcher and online cluster control client | Development |
| `vibedb-shard` | Static shard, RF3 member, and physical-node server | Internal server and admin surface |
| `vibedb-gateway` | Standalone gateway, schema rollout, and restore activation | Internal server and admin surface |
| `vibedb-verify` | Offline verification, salvage, and repack | Operator-facing; requires quiescent input |
| `vibedb-operator` | Render and prepare the Kind test topology; restore helpers | Qualification helper, not a controller |
| `vibedb-kube-qualify` | Probe the Kind test topology | Qualification helper, not a general client |

`cmd/vibedb-sql-audit` is a development audit tool and is not covered here.

## Conventions

- Go's flag parser accepts `-flag` and `--flag`.
- Every binary prints a usage block and exits 2 when run without a command.
- Usage errors exit 2 and runtime failures exit 1, with these exceptions:
  `vibedb-operator` exits 2 for every reported failure; `vibedb-kube-qualify`
  exits 1 for errors including `-h`; `vibedb-shard` exits 2 when a manifest
  fails to load.
- `vibedb-verify` has positional syntax; `-h` is parsed as a path.

## `vibedb`

```text
vibedb cluster dev --root <absolute-path> [flags]
vibedb cluster nodes|join|rebalance|decommission|status --profile <file> [flags]
```

### `cluster dev`

Creates or reopens one local development topology and supervises its child
processes until `SIGINT` or `SIGTERM`. Procedure:
[local cluster](../operations/local-cluster.md).

| Flag | Default | Meaning |
| --- | --- | --- |
| `--root` | required | Absolute, clean path. Absent or empty on first start (a leftover `.supervisor.lock` is allowed), or holding the exact retained manifest. |
| `--replicas` | `3` | `1` (development only, no HA) or `3`. |
| `--physical-nodes` | `0`, meaning `3` for RF3 | `3` or `6` for RF3; an explicit `0` and any value with RF1 are refused. Must match the root on restart. |
| `--pg-listen` | disabled | RF3 only. SQL endpoint on physical node 1. Literal loopback IP and port. |
| `--pg-listens` | disabled | RF3 only. Comma-separated distinct loopback endpoints, one per physical node. Exclusive with `--pg-listen`. |
| `--table-schema` | none | RF3 only, repeatable. File with one `CREATE TABLE` and one primary key; provisioned as an extra group and retained. |
| `--read-authority` | omitted | Explicitly enable or disable the RF3 read-authority policy. Omitted: platform default on first start, recorded policy on restart. A value that differs from the recorded policy is refused. |
| `--tls-ca-certificate`, `--tls-ca-key` | generated | Absolute PEM CA pair used to sign this cluster's leaf identities. Both or neither. The key is read locally and never transmitted. |
| `--shard-binary` | beside `vibedb`, then `PATH` | Explicit `vibedb-shard` executable. |
| `--gateway-binary` | unused for RF3 | Only resolved for the legacy RF1 path when given explicitly. RF3 frontends run inside `vibedb-shard`. |
| `--node-log` | `false` | RF3 only; shared node log for a non-physical layout. Physical-node RF3 always uses node logs. |
| `--nodes` | `0` | Deprecated alias for `--replicas`; conflicting values are refused. |
| `--diagnostics-on-exit` | `false` | Print the last 64 KiB of each child's output when the supervisor stops. |

Output and lifecycle:

- Readiness line for physical-node RF3:
  `VibeDB development RF3 physical cluster ready: <native-client-address> (<n> nodes)`.
  RF1 prints `VibeDB development RF1 ready (no HA): <address>`.
- Each child must report ready within 30 seconds.
- If a child exits, the supervisor stops the others and exits 1.
- On shutdown, children receive `SIGTERM`; any still running after 10 seconds
  in total are killed.
- A second supervisor on the same root is refused through `.supervisor.lock`.

The read-authority policy requires Linux `CLOCK_BOOTTIME`; see
[local cluster](../operations/local-cluster.md#read-authority-policy) for the
clock assumption and timing.

### Online cluster control

`nodes`, `join`, `rebalance`, `decommission`, and `status` send one canonical
request to a frontend's authenticated native client listener and print one
bounded response. Procedure: [scale and decommission](../operations/scaling.md).

```text
vibedb cluster nodes        --profile <file> [--json] [--wait d]
vibedb cluster join         --profile <file> --node-file <descriptor> [--request-id hex] [--json] [--wait d]
vibedb cluster rebalance    --profile <file> [--desired-node-count n] [--max-moves n] [--max-migration-bytes n] [--hysteresis-ppm n] [--request-id hex] [--json] [--wait d]
vibedb cluster decommission --profile <file> --node <32 hex> --incarnation n [--request-id hex] [--json] [--wait d]
vibedb cluster status       --profile <file> --operation <64 hex> [--json] [--wait d]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--profile` | required | Operator profile: `format`, `address`, `server_node`, `certificate`, `key`, `roots`, `identity_oid`. |
| `--request-id` | random | 64 lowercase hex characters; the idempotency key. Pass it explicitly for mutations so you can retry. |
| `--wait` | `0` | Server-side progress wait, at most `24h`. Ending the wait never cancels the operation. |
| `--json` | `false` | Print the canonical response, including phase, budget, evidence, and moved-group counts. |
| `--node-file` | join only | Public node descriptor; paths and private keys are rejected. |
| `--node`, `--incarnation` | decommission only | Node ID (32 hex) and nonzero incarnation. |
| `--operation` | status only | Operation ID (64 hex). |
| `--desired-node-count` | `0` | Rebalance target, at most 4096. |
| `--max-moves` | `4096` when `0` | Moves admitted across the whole operation, at most 4096. |
| `--max-migration-bytes` | 1 GiB when `0` | Bytes admitted across the whole operation. |
| `--hysteresis-ppm` | `0` | Minimum placement improvement per move. |

Flags that belong to another operation are refused. `nodes` and `status`
require the `topology` capability; the mutations also require `membership`.
The client deadline is `--wait` plus 15 seconds, or 30 seconds without a wait.
Exit status: 0 for `ok=true`, 1 for `ok=false` or a transport failure, 2 for a
usage error. Text output prints one summary line, one line per node, one line
per blocker, and `error=` when present. Error messages use the internal
operation name, for example `cluster cluster_status: load profile: ...`.

## `vibedb-shard`

### `vibedb-shard` commands

| Command | Required inputs | Behavior |
| --- | --- | --- |
| `init` | `-store`, `-distribution`, `-shard`, nonzero `-allocation-generation` | Creates a static local shard store and prints its persisted binding and log ID to stderr. |
| `serve` | the four `init` fields plus nonzero `-epoch` and `-routing-version` | Serves a statically owned shard. This is ownership fencing, not RF3 election. |
| `prepare-rf3` | `-manifest` | Atomically prepares an RF3 member from a canonical manifest. An exact existing preparation is verified and accepted; it is not blindly overwritten. |
| `prepare-node-rf3` | `-manifest` | Prepares a physical node: its shared node log and zero or more group replicas. With no groups it prepares an empty node for `vibedb cluster join`. |
| `prepare-node-group-rf3` | `-manifest` | Prepares one more group's SQL state for an existing node without opening its live node log; the serving node adopts it. |
| `serve-node` | `-manifest` | Serves a prepared physical node with its embedded frontend. Requires a `node_log`, and either groups or an empty-node incarnation. |
| `serve-rf3` | `-manifest` | Opens exactly prepared artifacts and serves one or more group members. It creates and repairs nothing. |
| `bootstrap-rf3` | `-manifest` | Installs an authenticated snapshot into a cold learner, then reopens through the ordinary serving path. It continues serving until stopped. |
| `adopt-restored-rf3` | `-manifest` | Validates restored state against the target identity, roster, apply state, and snapshot, then publishes or verifies `serve-rf3.vibejson`. |

### `serve` flags

| Flag | Default |
| --- | ---: |
| `-listen` | `127.0.0.1:0` |
| `-max-connections` | `0` selects the service default; `-1` is unlimited only for the plaintext static service, while authenticated serving requires a positive resolved limit |
| `-dev-plaintext-loopback` | `false` |
| `-tls-certificate`, `-tls-key`, `-tls-roots`, `-tls-identity-oid` | none |
| `-tls-handshake-timeout` | `5s` |
| `-max-handshakes` | `32` |
| `-authorization-policy` | none |

Without explicit plaintext development mode, the complete TLS profile and authorization policy are required. Plaintext and TLS are mutually exclusive, and plaintext may bind only loopback.

### `serve-rf3` flags

| Flag | Default | Constraint |
| --- | ---: | --- |
| `-manifest` | none | Required canonical prepared manifest. |
| `-reload-prepared-groups` | `false` | Allows SIGHUP to append or retire durably prepared groups from the same manifest. |
| `-execution-lanes` | `8` | Power of two from 1 to 64. |

A manifest may describe at most 64 groups. A manifest can explicitly
describe RF1 development-only/no-HA; otherwise this is the RF3 path.
`serve-node` accepts the same three flags. The local launcher runs
`serve-node -manifest <root>/node-<n>/serve-rf3.vibejson -reload-prepared-groups`.
Checked-in RF3 listener bounds: native service 64 connections and 16
concurrent handshakes, replica control 32 and 8, 15 s per native request. The
embedded frontend defaults to 1,024 client connections and 64 handshakes.

Signals: `SIGUSR1` writes a node diagnostic (see
[observability](../operations/observability.md#collect-a-physical-node-diagnostic));
`SIGHUP` reloads prepared groups when `-reload-prepared-groups` is set;
`SIGINT` and `SIGTERM` stop the process.

## `vibedb-gateway`

### `vibedb-gateway` commands

| Command | Required inputs | Output or behavior |
| --- | --- | --- |
| `inspect` | `-catalog <path>` | Prints catalog generation, distributions, routes, shards, configured first leader address, and endpoint count. The address is a static manifest value, not a live leader observation. |
| `validate` | `-catalog <path>` | Validates the catalog and prints its generation and endpoint count. |
| `serve` | `-catalog <path>` plus one valid development or authenticated profile | Serves until interrupted. |
| `schema-rollout` | authenticated `serve` flags and `-schema-rollout-plan` | Runs one authenticated rollout and exits; success prints catalog generation, operation revision, and elapsed time. |
| `restore-activate` | `-manifest <path>` | Activates a prepared restore and prints one canonical JSON result with operation, group count, and catalog witness. |

### `serve` flags and defaults

| Area | Flags |
| --- | --- |
| bootstrap | `-initial-node-directory ""` (trusted initial physical-node directory for first bootstrap) |
| catalog | `-catalog ""`; repeatable `-register-table-catalog`; `-catalog-route-seed ""`; `-dev-static-catalog=false`; `-catalog-bootstrap-if-missing=false`; `-catalog-relation=0` |
| catalog attempts | `-catalog-attempts=8`; `-catalog-attempt-timeout=5s` |
| controller identity | `-catalog-session-journal ""`; `-durable-ack-key ""`; `-catalog-client-id ""`; `-catalog-retry-home ""`; `-catalog-session-lease=24h` |
| topology control | `-controller-interval=1s`; `-hot-shard-capacity ""`; `-hot-shard-interval=1s`; `-replica-control-manifest ""` |
| backup | `-backup-repository ""`; `-backup-max-backups=16`; `-backup-max-artifacts=4096`; `-backup-max-artifact-bytes=68719476736`; `-backup-max-disk-bytes=274877906944` |
| schema | `-schema-rollout-plan ""`; `-schema-rollout-once=false` |
| listeners | `-listen=127.0.0.1:0`; `-pg-dev-listen ""`; `-pg-dev-ddl-socket ""`; `-dev-plaintext-loopback=false` |
| client TLS | `-tls-certificate ""`; `-tls-key ""`; `-tls-roots ""`; `-tls-identity-oid ""`; `-tls-handshake-timeout=5s`; `-authorization-policy ""` |
| client limits | `-max-client-connections=1024`; `-max-client-handshakes=64` |
| shard peers | repeatable `-shard-peer address=32hexNodeID`; `-max-shard-connections-per-pool=4096`; `-max-shard-handshakes-per-pool=64` |
| native reads | `-max-native-read-concurrency=256`; `-max-native-read-bytes=268435456`; `-max-native-scatter-concurrency=16` |

Static catalog mode requires explicit development plaintext and rejects replicated-operation flags. Replicated mode requires the route seed, relation, bounded attempts, session journal, ACK key, stable client/retry identities, peer identities, TLS, and authorization appropriate to the enabled operations. Backup additionally requires an absolute repository and replica-control authority. Development pgwire is RF3-only and loopback-only; its DDL socket must be an absolute private Unix socket.

### Native protocol boundary

The client listener uses newline-delimited JSON with a 1 MiB frame bound. Treat it as a strict, unstable protocol tied to the exact source revision—not as a stable public client API.

Recognized operations include `query`, `exec`, `read_batch`, `issuer_open`, `exec_batch`, `ack_exec_batch`, `metrics`, `backup`, `backup_status`, and native `get`, `put`, and `delete` grammar.

The checked-in gateway command executes native `get` only. Native `put` and `delete` decode but are rejected before I/O. Use sequenced durable `exec_batch` or the loopback development pgwire endpoint for writes.

A canonical point read has this shape:

```json
{"op":"get","table":"documents","key":"<raw-url-base64>","consistency":"linearizable"}
```

`at_least_applied` also requires an exact nonzero route ID and applied position. A successful response includes `ok`, `route_id`, `applied`, and `found`, with optional `document`, `request_id`, and `retries`. `read_batch` returns a vector of per-group observations; it does not claim a global snapshot.

`issuer_open`, `exec_batch`, and `ack_exec_batch` have closed, canonical, order-sensitive schemas. There is no unsequenced durable-write fallback. A lost mutation response may be `outcome_unknown`; callers must retain and resolve the exact request identity.

## `vibedb-verify`

This binary uses positional commands:

```text
vibedb-verify verify <store-file|database-dir>
vibedb-verify salvage <store-file> <output-file>
vibedb-verify repack <store-file> <output-file>
```

All operations require a quiescent source or a quiescent copy.

| Command | Source access | Result |
| --- | --- | --- |
| `verify` | read-only | Checks one store or a database directory and reports findings. A finding returns failure. |
| `salvage` | read-only | Creates a new `0600` output with exclusive creation. It may omit data that cannot be proven. |
| `repack` | **read/write** | Creates a new `0600` compact output. Opening the source may perform pending rollback, so do not run it against a live or irreplaceable file. |

`-h` is not help here; it is parsed as a path or positional argument.

## `vibedb-operator`

This is a manifest renderer and init helper for the Kubernetes qualification lane. It does not watch Kubernetes resources, reconcile state, elect leaders, mutate topology, rotate secrets, or implement a production operator lifecycle.

| Command | Flags and defaults | Behavior |
| --- | --- | --- |
| `bootstrap` | required `-state-dir`; `-namespace=vibedb`; `-shard-manifests=vibedb-rf3-manifests`; `-shard-tls=vibedb-rf3-tls`; `-gateway-config=vibedb-gateway-config`; `-gateway-tls=vibedb-gateway-tls` | Emits test bootstrap ConfigMaps/Secrets to stdout and node IDs to stderr. Authority is retained in the private state directory. |
| `render` | required `-image` and either `-bootstrap-state-dir` or exactly nine comma-separated `-shard-node-ids`; names as above; `-storage-class=""`; `-shard-storage=20Gi`; `-gateway-storage=1Gi` | Emits deterministic topology YAML to stdout. |
| `validate` | required `-manifest <path|->` | Performs bounded validation of rendered YAML. |
| `prepare` | `-hostname=$HOSTNAME`; `-manifest-dir=/bootstrap`; `-data-dir=/var/lib/vibedb/member` | Maps `vibedb-{catalog|ledger|data}-{0..2}` to a preparation manifest and invokes `vibedb-shard prepare-rf3`. |
| `prepare-gateway` | `-catalog-source`; `-catalog-target` | Copies and validates the immutable generation-one catalog seed onto the gateway PVC. |
| `restore-group` | required absolute `-root`, `-template`, `-operation`, `-artifact`; `-group-ordinal=0` | Builds three authority-free restored roots and prints a canonical JSON witness. |
| `adopt-restore` | required absolute `-manifest` | Invokes `vibedb-shard adopt-restored-rf3`. |

Generated bootstrap authority uses disposable test PKI and ordinary Kubernetes Secrets. Every reported error exits with status 2.

## `vibedb-kube-qualify`

This binary is a development test probe used by the Kind lane. It is not an application client or monitoring agent.

| Command | Flags and defaults | Output |
| --- | --- | --- |
| `write` | `-address=127.0.0.1:17400`; required `-certificate`, `-key`, `-roots`, `-state`; `-gateway-node` or `-bootstrap-state`; `-samples=128`; `-max-p99=1s`; `-max-latency=5s` | Creates one exact durable request state, resolves its terminal result, samples reads, and emits canonical JSON latency evidence. |
| `verify` | same as `write` | Replays the retained request after restart, verifies visibility, samples reads, and emits evidence with `recovered=true`. |
| `measure` | `-root=/var/lib/vibedb`; `-max-rss-bytes=1073741824`; `-max-storage-bytes=1073741824`; `-max-wal-bytes=536870912` | On Linux, reads `/proc/1/status`, walks at most 100,000 nonsymlink files, and emits canonical JSON resource evidence. |
| `dns` | `-namespace=vibedb-test`; `-timeout=30s` | Resolves nine shard Pod names plus the gateway and emits canonical JSON. Timeout must not exceed two minutes. |

`-address=127.0.0.1:17400`, `-samples=128`, `-max-p99=1s`, and
`-max-latency=5s` apply to `write` and `verify`. Client runs have a two-minute whole-run bound, fifteen-second round trips, a 1 MiB response bound, and 1–4096 samples. The exact request state path must be canonical and absolute. Errors, including flag-help termination, return status 1; a missing command returns 2.

## Limitations

- No command generates an operator client profile, an empty-node preparation
  manifest, or a join descriptor.
- No command rotates certificates, edits authorization policy, or changes a
  node's migration budget after preparation.
- `vibedb-gateway serve` is a standalone surface; RF3 physical-node clusters
  run their frontend inside `vibedb-shard serve-node` with the equivalent
  settings taken from the node manifest.
- `--help` output is Go's default flag listing and is not a stable format.

## Source map

| Surface | Implementation |
| --- | --- |
| Local launcher | [main.go](../../cmd/vibedb/main.go), [flags and lifecycle](../../cmd/vibedb/cluster_dev.go), [physical placement](../../cmd/vibedb/cluster_dev_physical.go) |
| Shard commands | [command dispatch](../../cmd/vibedb-shard/main.go), [node frontend](../../cmd/vibedb-shard/serve_node.go), [RF3 serving](../../cmd/vibedb-shard/serve_rf3.go) |
| Gateway commands | [command entry point](../../cmd/vibedb-gateway/main.go), [shared runtime](../../internal/gatewayruntime/), [serve flags](../../internal/gatewayruntime/serve.go) |
| Native and durable messages | [native grammar](../../internal/gatewayruntime/data_wire.go), [durable batches](../../internal/gatewayruntime/durable_exec_batch_wire.go), [acknowledgement](../../internal/gatewayruntime/exec_batch_ack_wire.go) |
| Offline utility | [vibedb-verify](../../cmd/vibedb-verify/main.go) |
| Kubernetes helpers | [operator helper](../../cmd/vibedb-operator/), [qualification probe](../../cmd/vibedb-kube-qualify/), [renderer](../../internal/kubeoperator/) |
