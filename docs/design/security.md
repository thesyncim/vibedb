# Security model

[Documentation](../README.md) / [Design](README.md) / Security model

Distributed VibeDB processes authenticate every peer and client with mutual
TLS 1.3 and authorize operations via explicit per-NodeID capability policies.
Embedded packages open no network sockets. See [SECURITY.md](../../SECURITY.md)
for trust boundaries and reporting.

## Trust model and transport (`servicetls`)

- **Crash-fault authenticated members:** Consensus is not Byzantine; a
  compromised member with replication authority can inject leader-origin Raft
  messages.
- **Binary identity:** Principals are exact 16-byte NodeIDs + cluster ID +
  incarnation carried in a critical X.509 extension (DNS/CN fields grant
  nothing). `NodeAuthorizer` uses a lock-free atomic snapshot for hot-path
  allowlist checks; removing an identity blocks new handshakes (existing
  streams finish their current handler).
- **ALPN traffic separation:** Every traffic class uses a distinct ALPN so a
  certificate/stream cannot be replayed across classes: `vibedb-raft-ordinary`,
  `vibedb-raft-snapshot`, `vibedb-shard-native`, `vibedb-shard-sql`,
  `vibedb-shard-control`, `vibedb-gateway-client`, `vibedb-gateway-control`.

## Capabilities and delegation (`serviceauthz`)

| Capability | Grants |
| --- | --- |
| `data_read`, `data_write`, `schema` | SQL and native data/DDL access (derived from parsed statement kind; malformed fails closed) |
| `delegate` | Allows a frontend to forward an end-user's `(NodeID, policy generation)` authority |
| `membership`, `topology` | Sealed membership transitions; catalog publication, splits, and moves |
| `transaction_recovery`, `request_ledger`, `execution_pin` | Hidden txn recovery reads, ledger grammar, catalog execution pins |
| `backup`, `restore_activate` | Export / non-serving staging; one-time cluster activation |

Replicas verify **both** the frontend's `delegate` capability and the forwarded
principal's capability at the exact policy generation, blocking confused-deputy
attacks. Frontend internal work (ledger, recovery) runs under the frontend's
own service identity.

## At-rest and client authentication

- **WAL encryption:** Per-group WALs and shared node logs use AES-256-GCM with
  digest chaining bound to group/node identity.
- **PostgreSQL wire:** Supports TLS (`SSLRequest`) + `SCRAM-SHA-256` for remote
  clients (`pgwire.RequireTLS` + `pgwire.SCRAM`); loopback-only `Trust` mode
  runs sessions under the frontend's configured principal.
- **Operator CLI:** `vibedb cluster init-operator`, `init-node`, `rotate-cert`,
  and `policy (show|grant|revoke)` manage credentials and policies from an
  existing CA (`vibedb cluster dev` generates disposable local credentials).

## Limitations

- External CA lifecycle, HSM/KMS integration, and multi-host secret
  distribution are operator-provided.
- Capabilities are cluster-wide per principal (no per-table or row-level ACLs).
- Directory revocation does not sever already-open streams.

## Source map

- Identity, TLS & authorizer: [internal/rafttransport/identity.go](../../internal/rafttransport/identity.go), [internal/servicetls/server.go](../../internal/servicetls/server.go)
- Policies & delegation: [internal/serviceauthz/policy.go](../../internal/serviceauthz/policy.go), [shardservice/replicated_dispatch.go](../../shardservice/replicated_dispatch.go)
- WAL encryption: [internal/raftstore/codec.go](../../internal/raftstore/codec.go)
