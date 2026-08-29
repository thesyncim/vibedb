# Raft peer transport foundation

Status: **Internal foundation with experimental command integration**

`internal/rafttransport` provides a composable authenticated stream foundation
for ordinary Raft messages. `internal/raftservice` wires it to
`internal/multiraft.Host` in the RF3 serving composition.
`vibedb-shard serve-rf3` loads one to 64 local group members from an exact
process manifest and constructs one shared authenticated ordinary-message peer
runtime. Every configured RF3 group begins with three voters; retained,
certified membership state may instead describe an enrolled replacement or
learner. All groups in the process must share the certificate trust domain, and
one remote node must have the same peer address in every group.

The ordinary-message runtime does not carry snapshot, shard-control, or native
client traffic. `serve-rf3` composes those on separate authenticated listeners
and budgets. It still does not enroll or renew certificates or discover peer
addresses dynamically; the manifest and retained membership authority provide
those exact identities.

## Certificate identity

The operator must assign the peer identity extension below its own IANA
Private Enterprise Number. The package accepts only this OID shape:

`1.3.6.1.4.1.<assigned-PEN>.1.1`

The `.1.1` suffix is the fixed VibeDB Raft peer identity leaf. Production
configuration must not use an unassigned PEN or a PEN that belongs to another
organization.

The leaf certificate must contain the extension exactly once. The extension
must be critical. Its value is exactly 48 bytes:

| Offset | Size | Field |
| --- | ---: | --- |
| 0 | 16 | `ClusterID` |
| 16 | 16 | `ClusterIncarnation` |
| 32 | 16 | `NodeID` |

Each field must be nonzero. The parser rejects duplicate extensions, a
different OID, a noncritical extension, and any missing or trailing byte.
`Subject`, `CommonName`, DNS names, IP names, and URI names do not provide a
peer identity.

`PeerTLS` performs normal Go X.509 chain verification against a cloned root
pool. It verifies certificate time through the required `Now` function. The
clock must return a nonzero time. The leaf must not be a CA, must allow digital
signatures, and must explicitly allow both client and server authentication.
The configured private key must implement `crypto.Signer`, match the leaf
public key, and support a TLS 1.3 signature scheme; this is validated before a
listener or dialer can publish the credential. Hardware-backed signers remain
supported. TLS currently requires version 1.3 or newer and disables session
tickets. The client does not perform DNS-name matching. Its
`VerifyConnection` callback still performs chain, time,
key usage, trust-domain, extension, node, and traffic-class checks. A server
rejects a peer certificate whose `NodeID` equals its own `NodeID`.

The presented chain can contain at most eight certificates and 1 MiB of DER in
total. Local profile construction checks these bounds before it copies DER.

The binary trust domain contains both `ClusterID` and
`ClusterIncarnation`. A shared CA does not let a certificate cross that
boundary. One nonempty `StaticRegistry` contains groups from exactly one trust
domain. Every authenticated connection and every frame group must match it.

`PeerTLS` maps its seven traffic classes to seven fixed ALPN values:
`vibedb-raft-ordinary`, `vibedb-raft-snapshot`, `vibedb-shard-native`,
`vibedb-gateway-client`, `vibedb-shard-sql`, `vibedb-shard-control`, and
`vibedb-gateway-control`. Each client or server configuration advertises only
the value for its requested class. Unsupported classes and a negotiated value
that differs from that exact mapping are rejected. The ordinary-message and
snapshot lanes below use the first two values.

## Ordinary outbound streams

`OrdinaryTransport` owns one FIFO queue and one persistent writer per remote
node. `Send` borrows the protobuf graph only for the call. It encodes an owned
canonical frame before it returns.

The caller supplies all queue, coalescing, reconnect, deadline, and peer
settings. Construction rejects settings above these hard limits:

- 4,096 peers
- 65,536 queued frames
- 1 GiB of queued frame bytes
- 256 frames in one coalesced write
- 1 second of coalescing delay
- 1 minute of reconnect delay
- 256 MiB of aggregate retained coalescing scratch

The sum of fixed per-peer ring slots cannot exceed the global frame limit.
`Send` determines the exact destination and encoded length before it reserves
per-peer and global frame and byte ownership. A complete reservation exists
before frame allocation. Reservations are visible in `PeerStats` and in the
global totals. They unwind on encode failure and shutdown before queue and
buffer cleanup can finish.

Retained outbound buffers use fixed power-of-two size classes. The final class
is capped at the configured retention limit. A retained frame therefore owns
less than twice its encoded length, and queue byte admission charges that
capacity rather than the logical length. Active and cached outbound buffers
share the configured global frame and byte ceilings. Warm class lookup is
constant time and does not allocate. Free-buffer clearing and return occur
after the global queue lock is released.

`Send` returns `ErrBackpressure` without waiting when a reservation cannot
fit. A blocked peer has a separate worker, stream, queue, and byte budget, so
it does not block another peer writer.

One write can combine frames only within the configured count, byte, and delay
bounds. The delay and reconnect functions are injected. Core reconnect and
coalescing logic does not read a wall clock. `WaitWithTimer` is the explicit
production timer implementation.

The writer handles short writes. A failed write closes the stream and retains
the complete queued frames for the next authenticated connection. A receiver
can therefore observe a duplicate after an uncertain write result. Raft
message processing must remain idempotent under its normal term and index
rules.

Delivery is best effort at this transport boundary. A successful local stream
write removes the frame without a receiver acknowledgement. Raft protocol
messages and state convergence supply any required retransmission. `SentFrames`
and `SentBytes` count successful local writes, not remote admission or durable
receipt.

`Close` during `Run` initiates shutdown and returns after cancellation starts.
`Run` is the synchronous completion point that waits for active `Send` calls,
drains queued ownership, and releases the outbound cache.

## Ordinary inbound streams

`OrdinaryReceiver` takes ownership of one authenticated ordinary connection.
Each record starts with a four-byte unsigned frame length. The receiver rejects
an invalid length before it obtains a frame buffer. It installs one injected
deadline for the complete header and body read. Context cancellation closes a
blocked connection.

The receiver uses `io.ReadFull` for the header and body. It calls
`StaticRegistry.DecodeInbound` before it calls the handler. This admission
checks the certificate-derived source node, local destination, complete group
identity, roster digest, bounded protobuf graph, unknown fields, and canonical
deterministic encoding.

On successful decode, `Inbound.Message` owns its bytes. The receiver can return
its frame buffer before the handler runs. The handler owns the decoded message
after delivery.

The outbound cache has explicit aggregate frame and byte counters. The current
receiver frame cache and canonical re-encode cache use `sync.Pool`. Their live
memory is bounded per operation, but their aggregate process retention is not
an explicit transport-package limit. The shipped `serve-rf3` composition caps
ordinary inbound streams at eight, which bounds simultaneously live receiver
frames. It does not convert `sync.Pool` retention into an exact process-wide
post-GC memory ceiling.

## Snapshot isolation

Snapshot traffic has a separate ALPN value and a separate
`SnapshotStreamOpener` capability. `OrdinaryTransport` cannot accept that
capability. `OrdinaryReceiver` rejects a snapshot-class connection.

`serve-rf3` reserves a separate snapshot listener before adopting its runtimes,
authenticates it as `TrafficSnapshot`, and gives it a concurrency bound derived
from the configured snapshot-source and split-operation limits. That listener
multiplexes the installed snapshot-transfer data service and split-artifact
service; it does not enter the ordinary per-peer queues. This is traffic-class
and admission isolation, not proof that every storage or network saturation
fault leaves foreground Raft latency unaffected.

## Shipped composition and remaining gaps

The experimental `serve-rf3` command supplies:

- manifest-derived raw TCP dialing and exact node/address checks;
- one bounded ordinary listener and one per-node outbound worker;
- one multi-group `StaticRegistry` and serialized execution lanes;
- atomic registry/owner installation and removal for certified adopted groups;
- separate native, shard-control, and snapshot listeners; and
- detached ordinary transport and inbound-listener counters plus joined
  shutdown.

It does not supply dynamic address discovery, automated certificate enrollment,
renewal, or revocation, a released general topology-administration CLI, total
network-byte accounting, or an exhaustive external partition/saturation matrix.
Membership and snapshot-transfer actions exist only through the separate
experimental, journaled replica-lifecycle composition; they are not powers of
`OrdinaryTransport` itself.

## Qualification boundary

`TestAuthenticatedExecutionPeerTwoGroupsProgressWithTransportPerPeer` proves
that two groups share one per-peer transport without losing group routing.
`TestServeRF3ShippedCompositionThreeProcesses` exercises the shipped command,
mutual TLS, election, authenticated reads, and clean shutdown across three real
processes. The mandatory multi-relation Linux gate adds a deterministic
bidirectional cut of every proxied Raft peer link to one selected process while
that process remains alive, followed by healing and catch-up; it separately
kills and restarts a leader.

That last fault is a test-owned peer-network partition. It is not `SIGSTOP`, a
whole-process partition, an operating-system packet-filter fault, or evidence
for arbitrary cuts or horizontal scaling.

## Implementation references

- `internal/rafttransport/identity.go`: `PeerTLS`, `PeerIdentityExtension`, and
  `ParsePeerIdentity`
- `internal/rafttransport/stream.go`: `OrdinaryReceiver`,
  `TLSOrdinaryDialer`, and `SnapshotStreamOpener`
- `internal/rafttransport/transport.go`: `OrdinaryTransport`
- `internal/rafttransport/frame.go`: `EncodeOutbound` and `DecodeInbound`
- `internal/raftservice/execution_peer.go`:
  `AuthenticatedExecutionPeerRuntime`
- `cmd/vibedb-shard/serve_rf3.go`: `servePreparedRF3WithExecutionLanes`
- `internal/rafttransport/identity_test.go`, `stream_test.go`,
  `transport_test.go`, and `alloc_test.go`
- `internal/raftservice/execution_test.go`:
  `TestAuthenticatedExecutionPeerTwoGroupsProgressWithTransportPerPeer`
- `cmd/vibedb-shard/serve_rf3_process_test.go`:
  `TestServeRF3ShippedCompositionThreeProcesses`
- `cmd/vibedb-gateway/rf3_peer_proxy_test.go`:
  `TestRF3PeerProxyCutsExistingAndNewLinksButLeavesTargetLive`
- `cmd/vibedb-gateway/durable_rf3_multirelation_chaos_process_test.go`:
  `TestGatewayDurableRF3MultiRelationChaosProcess`
