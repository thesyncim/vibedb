# Distributed system design

VibeDB has a runnable static-shard layer and a non-serving replication kernel.
These layers are not connected by the shipped commands.

## Runnable layer

The runnable layer has these components:

1. A catalog snapshot defines distributions, placements, shards, and endpoints.
2. A gateway pins one catalog generation for each attempt.
3. The router derives target shards from SQL constraints and placement tuples.
4. A shard service admits the request against its static ownership identity.
5. The shard runs SQL against one local VibeDB catalog.
6. The gateway merges complete shard results or returns an error.

The gateway has no authoritative row state. The shard store has the row state.
The gateway always selects the first leader endpoint in the manifest. Multiple
endpoint entries do not provide automatic load balancing or failover.

Each physical manifest covers the full unsigned 64-bit range. Ranges are
half-open, ordered, and adjacent. Each shard has a unique nonzero allocation
generation.

A distribution tuple has 1 through 8 fields. The current mapper maps the tuple
to a 64-bit value. Virtual bucket width is 8 through 24 bits. Zero selects 20
bits, which gives 1,048,576 virtual buckets.

## Execution lanes

The gateway has three execution lanes:

- A single-shard read contacts one shard. A targeted read can contact more
  than one, but not all, active shards. An ordinary write must prove one owner.
- A scatter read contacts every active shard or follows an unknown route. The
  fanout executor contacts the admitted bounded shard set and merges results.
- A repartitioned grouped aggregate uses bounded worker exchange.

The last lane uses loopback exchange by default. Cross-host exchange needs an
injected trusted dialer. The shipped gateway CLI does not supply one.

The merge layer supports global limits, ordered results, aggregates, and
grouped partial aggregates. It cancels remaining calls after a hard error or a
total result-bound breach.

## Topology safety

One gateway attempt uses one immutable catalog generation. A stale response
can cause a retry only after a strictly newer generation is available.

The shard admission order is stable:

1. Distribution and shard
2. Allocation generation
3. Routing version
4. Ownership epoch
5. Read policy and position

This order makes stale topology errors deterministic. It also prevents SQL
execution before the service accepts the ownership coordinates.

Catalog publication is crash-safe for cooperating writers. `SaveSnapshotAfter`
also checks an exact current generation. This check is the topology compare and
swap. Publication cannot protect against a process that ignores the lock.

## Coherent multi-shard reads

The gateway creates a random 128-bit fence ID. It acquires the same scoped
lease on all routed shards. If a shard is busy, the gateway releases partial
leases and retries with a new ID.

A writer has priority after it registers. Reacquisition cannot extend a read
fence ahead of that writer. Disjoint bucket scopes can proceed concurrently.

The fence is not durable. It expires if a gateway abandons it. This protocol
establishes a scoped vector cut. It does not assign a distributed MVCC
timestamp or prove one wall-clock snapshot instant.

## Global indexes

A local index is stored with its base table. A global index uses a separate
hidden relation with independent placement.

A global index key has 1 through 4 RFC 6901 paths. A locator has 1 through 8
paths and includes all base placement paths and the primary-key path.

The lifecycle states are `Building`, `CatchingUp`, `Ready`, and `Draining`.
Only a `Ready` index can serve reads. Foreground writes maintain every active
incarnation during build and drain.

A global-index write and its base mutation use the distributed transaction
protocol. A unique global index enforces uniqueness in the hidden relation.

The backfill package plans one bounded task per base shard. The caller must run
all tasks and publish the `Ready` state. The repository has no lifecycle or
backfill CLI.

## Autosplit boundary

The `autosplit` package records fixed-memory pressure evidence and recommends a
bucket boundary. `PlanSplit` validates a desired manifest with at most three
children.

It does not populate children, catch them up, validate a cutover, or publish
the catalog. The repository has no automatic split controller or merge planner.

## Replication kernel

The internal replication kernel contains:

- An encrypted preallocated Raft WAL
- A deterministic replicated SQL state machine
- A bounded single-owner Multi-Raft scheduler
- Static authenticated-identity frame validation
- Offline snapshot artifacts and resumable staging
- A stateless replica-move reconciler

The kernel has no production socket transport, TLS, peer authentication,
snapshot-transfer service, or serving integration. The frame decoder requires
an externally authenticated node ID. The shipped commands do not provide it.

Do not describe this kernel as a turnkey replicated deployment.

## Implementation references

- `gateway/catalog.go`, `executor.go`, `merge.go`, and `global_index_read.go`
- `gateway/read_snapshot.go`, `transaction.go`, `writer.go`, `global_index.go`,
  and `global_index_backfill.go`
- `distribution/manifest.go`, `router.go`, `tuple.go`, and `bucket.go`
- `shardservice/admit.go`, `read_fence.go`, and `server.go`
- `autosplit/recorder.go`, `planner.go`, `tracker.go`, and `action.go`
- `internal/raftstore`, `internal/raftmember`, and `internal/multiraft`
- `internal/rafttransport`, `internal/replicatedstate`, and `internal/rebalance`
