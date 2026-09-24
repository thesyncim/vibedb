# Security model

[Documentation](../README.md) / [Design](README.md) / Security model · [Development status](../status.md)

This page describes how distributed VibeDB processes authenticate each other
and authorize operations. The embedded packages open no listeners; an
embedding application owns that boundary. For reporting and deployment
cautions, see the [security policy](../../SECURITY.md).

## Trust model

- **Crash-fault, authenticated members.** Every cluster process is trusted to
  follow the protocol once it is authenticated. Raft is not Byzantine: a
  compromised member holding a valid replication grant can inject
  leader-origin replication messages (see [replication](replication.md#peer-transport)).
- **Identity is binary.** A principal is an exact 16-byte NodeID in a critical
  certificate extension, together with the cluster ID and incarnation (the
  *trust domain*). DNS names, subjects, and common names grant nothing.
- **Capabilities are explicit.** An authorization policy maps each NodeID to a
  capability bitset at one immutable policy generation.

## Transport authentication (`servicetls`)

All distributed service traffic uses mutual TLS 1.3, with mandatory handshake
and stream deadlines and bounded accepted sockets and handshakes. Each traffic
class has its own ALPN:

| ALPN | Traffic |
| --- | --- |
| `vibedb-raft-ordinary` | Raft messages between replicas |
| `vibedb-raft-snapshot` | Snapshot artifact transfer |
| `vibedb-shard-native` | Frontend to RF3 replica |
| `vibedb-shard-sql` | Frontend to static shard |
| `vibedb-shard-control` | Topology, membership, backup, and metrics control |
| `vibedb-gateway-client` | Application clients to a frontend |
| `vibedb-gateway-control` | Frontend-to-frontend catalog-drain fences |

A certificate admitted for one class cannot be replayed against another
listener. The servers carry only fixed binary principals; payloads and
authorization metadata never enter the TLS layer. A node allowlist can be
replaced from the authenticated service directory. An update rejects new
handshakes from removed identities, but already-authenticated streams continue
until their handler returns.

## Authorization (`serviceauthz`)

| Capability | Grants |
| --- | --- |
| `data_read`, `data_write`, `schema` | SQL and native data access. SQL authority is derived from the parsed statement kind; malformed or mixed statements require every SQL capability and fail closed. |
| `delegate` | Lets a service principal (a frontend) forward an exact end-user authority. Grants nothing by itself. |
| `membership` | The sealed learner, promotion, removal, and leader-transfer operation set. |
| `topology` | Catalog publication and split and move journals. Separate from `data_write`, although the catalog is stored in a replicated relation. |
| `transaction_recovery` | Reading hidden transaction control state for recovery. |
| `request_ledger` | The request-ledger grammar only. An ordinary writer cannot forge or acknowledge idempotency state. |
| `execution_pin` | The logical pin lifecycle in the catalog group only. |
| `backup` | Catalog-authorized export and non-serving restore staging. |
| `restore_activate` | The one-time activation of a restored cluster. |

A request that crosses a frontend carries the client's `(NodeID, policy
generation)` authority. The replica checks two things independently: the
frontend must hold `delegate`, and the forwarded principal must hold the
capability for the operation. This blocks confused-deputy requests. Binding
the generation means a retry cannot silently pick up newer privileges.

The frontend's own internal work (ledger, execution pins, recovery reads) runs
under its service identity and needs the matching internal capability. A
replacement frontend therefore recovers another frontend's request under its
*own* identity, not by impersonating the original.

## At-rest protection

Raft WALs and node logs are encrypted and authenticated with AES-GCM under a
key loaded from a key-material file named in the serving manifest. Records are
digest-chained and bound to the group or node identity. Key provisioning,
rotation, and storage are outside the repository.

## Development exceptions

| Exception | Boundary |
| --- | --- |
| Plaintext gateway and static catalog | Explicit flag; literal loopback only. |
| PostgreSQL endpoint on a frontend | Loopback only. Every session runs under the frontend's configured principal, and there is no per-user authentication. |
| Local development credentials | `vibedb cluster dev` generates a disposable CA, leaf certificates, a policy, and a WAL key under the cluster root. |

The RF3 shard command is always authenticated; it has no plaintext mode.

## Limitations

- No production PKI provisioning, certificate rotation workflow, or key
  management service. Tests cover certificate-generation rotation, but the
  complete process gate does not combine rotation with confused-deputy faults
  across the whole gateway command path.
- No per-table or row-level authorization; capabilities are per principal and
  cluster-wide.
- The frontend PostgreSQL endpoint has no client authentication and must stay
  on loopback.
- Directory revocation does not close existing streams.
- Byzantine members are out of scope.
- No audited release or verified private vulnerability channel exists; see
  the [security policy](../../SECURITY.md).

## Evidence

The CI "Hot-shard and transport qualification" job runs
`TestAuthorizedClientTLSNetworkChaosAndThroughputGate`,
`TestAuthenticatedShardBoundaryRotationAndConfusedDeputyFault`, and three runs
of the process gate
`TestAuthenticatedGatewayShardProcessPartitionRotationAndDeputyFaults`, which
covers a directional partition and healing, certificate-generation rotation
with revocation of the old stream, a rogue frontend certificate, and a
confused-deputy request.

## Source map

| Area | Implementation | Decisive tests |
| --- | --- | --- |
| Identity and ALPN | [`rafttransport/identity.go`](../../internal/rafttransport/identity.go) | `internal/rafttransport` tests |
| TLS servers and clients | [`servicetls/server.go`](../../internal/servicetls/server.go), [`servicetls/client.go`](../../internal/servicetls/client.go) | `internal/servicetls` tests |
| Policies | [`serviceauthz/policy.go`](../../internal/serviceauthz/policy.go), [`serviceauthz/sql.go`](../../internal/serviceauthz/sql.go), [`serviceauthz/load.go`](../../internal/serviceauthz/load.go) | `internal/serviceauthz` tests |
| Delegation at the replica | [`shardservice/replicated_dispatch.go`](../../shardservice/replicated_dispatch.go), [`shardservice/replicated_tls.go`](../../shardservice/replicated_tls.go) | `gateway` authenticated-transport tests |
| WAL encryption | [`raftstore/codec.go`](../../internal/raftstore/codec.go) | [`format_test.go`](../../internal/raftstore/format_test.go) |
