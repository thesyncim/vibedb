# Raft peer transport foundation

Status: **Internal**

`internal/rafttransport` provides a composable authenticated stream foundation
for ordinary Raft messages. `internal/raftservice` wires it to
`internal/multiraft.Host` in the internal RF3 serving composition. No shipped
command constructs that peer service.

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

The fixed ALPN values are `vibedb-raft-ordinary` and
`vibedb-raft-snapshot`. The package does not accept alternate protocol names.

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
yet an explicit transport limit. The serving integration must bound accepted
stream concurrency or replace those pools with listener-owned bounded caches
before claiming a process-wide transport memory ceiling.

## Snapshot isolation

Snapshot traffic has a separate ALPN value and a separate
`SnapshotStreamOpener` capability. `OrdinaryTransport` cannot accept that
capability. `OrdinaryReceiver` rejects a snapshot-class connection. A later
snapshot service must use separate connection, concurrency, and memory budgets
so snapshot transfer cannot starve ordinary Raft messages.

## Remaining integration

A serving integration must still provide these components:

- Raw address discovery and dialing
- A bounded listener and accepted-stream lifecycle
- Certificate enrollment, secure key loading, renewal, and revocation policy
- Trusted topology publication for `StaticRegistry`
- `multiraft.Host` outbound and inbound adapters
- Peer replacement and duplicate-stream policy
- Transport metrics and operational shutdown
- A separately budgeted snapshot transfer service

## Implementation references

- `internal/rafttransport/identity.go`: `PeerTLS`, `PeerIdentityExtension`, and
  `ParsePeerIdentity`
- `internal/rafttransport/stream.go`: `OrdinaryReceiver`,
  `TLSOrdinaryDialer`, and `SnapshotStreamOpener`
- `internal/rafttransport/transport.go`: `OrdinaryTransport`
- `internal/rafttransport/frame.go`: `EncodeOutbound` and `DecodeInbound`
- `internal/rafttransport/identity_test.go`, `stream_test.go`,
  `transport_test.go`, and `alloc_test.go`
