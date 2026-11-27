# Development protocol reference

[Documentation](../README.md) / [Reference](README.md) · [Stability](../status.md)

Every network protocol below is an exact-build development surface (except the
HTTP `/metrics`, `/healthz`, `/readyz` observability endpoint). Do not probe a
listener with a different protocol.

## Protocol map

| Boundary | Transport & framing |
| --- | --- |
| Embedded Go (`vibedb`, `sql/driver`) | Direct Go calls (no wire) |
| Client ↔ gateway | mTLS 1.3 + newline-delimited canonical JSON (1 MiB line cap) |
| PostgreSQL client ↔ adapter | PostgreSQL v3 wire ([pgwire guide](../api/pgwire.md)) |
| Gateway ↔ RF3 replica | mTLS 1.3 (`vibedb-shard-native`) + version-1 binary frames (`P`/`M`/`T`/`L`/`E`/`G` → `A`) |
| Gateway ↔ static shard | `Q`/`R` length-prefixed binary frames (legacy static lane) |
| Peers & control | mTLS 1.3 with per-class ALPN (`vibedb-raft-ordinary`, `vibedb-raft-snapshot`, `vibedb-shard-control`, `vibedb-gateway-control`) |
| Observability | HTTP `/metrics` (Prometheus v0.0.4), `/healthz`, `/readyz` |

## Placement tuple identity

Tuple codec v1 + native mapper v1 hash canonical scalar tuples (1–8 fields of
raw-byte strings `0x01` or exact JSON numbers `0x02`, where `5 == 5.0 == 5e0`
and `+0 == -0`; booleans, null, and containers are refused) via xxHash64 into
an 8–24-bit virtual bucket (20 bits default). Changing tuple encoding or mapper
parameters requires regenerating all placement artifacts.

## Gateway newline-delimited JSON

One canonical JSON object per line (≤1 MiB; responses ≤4 MiB for cluster
control). Recognized `op` values:

- **Reads:** `query` (routed SQL), `read_batch` (RF3 exact-PK whole-doc
  vector), `get` (RF3 native point read: `{"op":"get","table":"documents",
  "key":"<raw-url-base64>","consistency":"linearizable"}` or `at_least_applied`
  with exact `route_id` + `applied`).
- **Writes:** `issuer_open`, `exec_batch` (sequenced durable RF3 batch
  requiring `request_id`, `installation_id`, `issuer_epoch`, `lane_ordinal`,
  `grant_digest`, `issuer_sequence`), `ack_exec_batch`. `put`/`delete` decode
  and are rejected before I/O. `committed` and `outcome_unknown` are mutually
  exclusive—always replay the **exact same** identity and bytes on
  `outcome_unknown`.
- **Control & observability:** `metrics`, `backup`, `backup_status`,
  `cluster_nodes`, `cluster_join`, `cluster_rebalance`, `cluster_decommission`,
  `cluster_status`.

## RF3 native service and static shard

- **RF3 native:** Binary v1 frames carrying exact capability and serving
  fences for discovery, proposals, membership transitions, point/batch/fenced
  SQL reads, and recovery reads. Linearizable reads use leader `ReadIndex` or
  quorum read authority; `at_least_applied` follower reads guarantee only the
  floor; multi-group `read_batch` returns independent per-group cuts.
- **Static shard SQL (`Q`/`R`):** Single-owner leader-only SQL frames with
  optional read-only mutation capture markers (`0xdc` legacy, `0xe4`
  before/after image).

## PostgreSQL wire adapter

- **Embedded `pgwire`:** Full local SQL transaction surface; explicit `Trust`
  (loopback) or `RequireTLS` + `SCRAM-SHA-256`.
- **RF3 gateway `pgwire`:** Autocommit-only writes (`INSERT VALUES`,
  branch-aware `ON CONFLICT`, `DELETE`, exact-PK `UPDATE` guarded by old-value
  CAS); remote access requires `PGTLSConfig` + `PGUsers` (SCRAM).

## TLS, build gate, and control protocols

Internal mTLS 1.3 certificates bind binary 16-byte NodeID + cluster ID +
incarnation in a critical X.509 extension (DNS/CN ignored) and isolate traffic
classes by ALPN. After handshake, streams exchange a 104-byte build preface
(`internal/buildgate`) checking wire/disk grammar digests and capabilities.
Treat the exact Git commit (verified with `vibedb cluster version --check-peers`)
as the compatibility boundary.

Shard split control (`shardcontrol`) uses one TLS stream per request with fixed
fences and ≤1 MiB canonical JSON; `Accepted` proves the durable witness exists.

## Source map

- Gateway NDJSON & control: [internal/gatewayruntime/serve.go](../../internal/gatewayruntime/serve.go), [data_wire.go](../../internal/gatewayruntime/data_wire.go), [durable_exec_batch_wire.go](../../internal/gatewayruntime/durable_exec_batch_wire.go), [internal/clustercontrol/control.go](../../internal/clustercontrol/control.go)
- Shard & RF3 wire: [shardservice/codec.go](../../shardservice/codec.go), [replicated_wire.go](../../shardservice/replicated_wire.go), [shardcontrol/protocol.go](../../shardcontrol/protocol.go)
- Placement, TLS & build gate: [distribution/](../../distribution/), [internal/servicetls/](../../internal/servicetls/), [internal/buildgate/preface.go](../../internal/buildgate/preface.go)
