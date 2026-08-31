# Command-line reference

> [!CAUTION]
> **Development and qualification only.** VibeDB is under active development.
> Commands, flags, output, manifests, network protocols, and persisted state
> may break at any commit. Only exact same-build restart behavior is qualified;
> there is no mixed-build upgrade or downgrade contract.

The repository builds six commands. They are not all end-user tools.

| Binary | Role | Status |
| --- | --- | --- |
| `vibedb` | local cluster launcher | development convenience |
| `vibedb-shard` | static and Raft shard process | internal server/admin surface |
| `vibedb-gateway` | routing, SQL, durable request, and control endpoint | internal server/admin surface |
| `vibedb-verify` | offline verification, salvage, and repack | operator-facing, requires quiescent input |
| `vibedb-operator` | render and prepare the Kubernetes test topology | qualification helper, not a controller |
| `vibedb-kube-qualify` | probe the Kubernetes test topology | qualification helper, not a general client |

Go's flag parser accepts `-flag` and `--flag` for these commands. Usage errors normally return status 2 and runtime failures status 1, except `vibedb-operator`, which returns 2 for every reported failure. `vibedb-verify` has positional syntax and no conventional `--help` command.

## `vibedb`

### `cluster dev`

```text
vibedb cluster dev --root <absolute-path> [flags]
```

Creates or reopens one durable local development topology and supervises its child processes.

| Flag | Default | Meaning |
| --- | ---: | --- |
| `--root` | none | Required clean absolute cluster directory. It must be absent or empty initially, or contain the exact retained development manifest. |
| `--replicas` | `3` | `1` for development-only RF1/no HA, or `3` for RF3. |
| `--nodes` | `0` | Deprecated alias for `--replicas`; conflicting values are rejected. |
| `--shard-binary` | sibling/PATH | Explicit `vibedb-shard` executable. |
| `--gateway-binary` | sibling/PATH | Explicit `vibedb-gateway` executable; used only for RF3. |
| `--diagnostics-on-exit` | `false` | Print bounded child log tails after shutdown. |
| `--pg-listen` | disabled | RF3-only loopback PostgreSQL endpoint. Requires a literal loopback IP and port `1..65535`. |
| `--table-schema` | none | RF3-only file containing one `CREATE TABLE` with one primary key; retained on restart. |

RF1 starts three independent single-member Raft groups and no gateway. RF3 starts three members for each of the catalog, request-ledger, and data groups, plus one gateway. See the [local cluster tutorial](../operations/local-cluster.md).

## `vibedb-shard`

### `vibedb-shard` commands

| Command | Required inputs | Behavior |
| --- | --- | --- |
| `init` | `-store`, `-distribution`, `-shard`, nonzero `-allocation-generation` | Creates a static local shard store and prints its persisted binding and log ID to stderr. |
| `serve` | the four `init` fields plus nonzero `-epoch` and `-routing-version` | Serves a statically owned shard. This is ownership fencing, not RF3 election. |
| `prepare-rf3` | `-manifest` | Atomically prepares an RF3 member from a canonical manifest. An exact existing preparation is verified and accepted; it is not blindly overwritten. |
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
| `-execution-lanes` | `8` | Must be a supported power of two. |

A process may serve 1–64 prepared group members. A manifest can explicitly describe RF1 development-only/no-HA; otherwise this is the RF3 path.

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

Client runs have a two-minute whole-run bound, fifteen-second round trips, a 1 MiB response bound, and 1–4096 samples. The exact request state path must be canonical and absolute. Errors, including flag-help termination, return status 1; a missing command returns 2.

## Source map

| Surface | Primary source |
| --- | --- |
| `vibedb cluster dev` | `cmd/vibedb/main.go`, `cmd/vibedb/cluster_dev.go`, `cmd/vibedb/cluster_dev_tables.go` |
| shard commands | `cmd/vibedb-shard/main.go`, `prepare_rf3.go`, `serve_rf3.go`, `bootstrap_rf3.go`, `adopt_restored_rf3.go` |
| gateway flags and commands | `cmd/vibedb-gateway/main.go`, `serve.go`, `restore_activate.go`, `schema_rollout_admin.go` |
| native and durable wire boundary | `cmd/vibedb-gateway/data_wire.go`, `data_handler.go`, `data_response.go`, `durable_exec_batch_wire.go`, `exec_batch_ack_wire.go` |
| offline utility | `cmd/vibedb-verify/main.go` |
| Kubernetes helpers | `cmd/vibedb-operator/`, `cmd/vibedb-kube-qualify/`, `internal/kubeoperator/` |
