# Online replica migration

[Documentation](../README.md) / [Operations](README.md) · [Development status](../status.md)

Replica movement uses a durable, target-bound snapshot artifact. A scale-out
learner, a scale-in replacement, and node decommissioning all use the same
source export, resumable transfer, and cold install barriers. The operation
does not pause client serving while a snapshot is copied. Traffic admission,
Raft membership, and learner promotion remain separate control-plane steps.

## Node budget

Migration work is bounded per physical serving node. Construct one
`migrationbudget.Budget` for the process and inject the same pointer into every
group provider, source data service, receiver, and cold learner installer on
that node. Constructing one budget per group would multiply the configured
allowance as groups are added and would defeat the foreground protection.

The default is deliberately conservative:

| Class | Sustained rate | Burst |
| --- | ---: | ---: |
| Serialized snapshot work (CPU proxy) | 64 MiB/s | 4 MiB |
| Source artifact reads | 64 MiB/s | 4 MiB |
| Repository and target-stage writes | 64 MiB/s | 4 MiB |
| Snapshot network sends | 32 MiB/s | 2 MiB |
| Snapshot network receives | 32 MiB/s | 2 MiB |
| Concurrent local heavy phases | 2 | — |
| Transient migration workspaces | 16 MiB total | — |

`BytesPerSecond` is the sustained allowance and `BurstBytes` is the largest
actual operation issued to that class. Every accepted operation larger than a
burst is split into smaller reads, writes, hashes, or network body segments;
the caller does not prepay a large token amount and then issue an unbounded
I/O. The accepted configuration bounds rates and bursts before a node starts:
`MaxActive` is 1 through 256, rates are positive and at most 1 TiB/s, and
bursts are positive and at most 1 GiB. Invalid values fail with
`migrationbudget.ErrInvalidConfig`.

The transient workspace pool is a separate node-wide ceiling. A source export
reserves both chunk workspaces atomically, and senders and receivers use
dedicated halves of the pool for their body buffers. This prevents opposite
transfer directions from each holding the last local credit while waiting for
the peer socket. These reservations live only while the operation is active;
idle groups retain no chunk-sized buffer. The default pool is 16 MiB, and
callers that construct a budget directly may lower it subject to the largest
chunk they intend to admit. A buffer wait never holds an active phase permit.
`Metrics()` reports aggregate and directional usage, capacity, waiters,
acquisitions, cancellations, and releases.

Prepared RF3 nodes persist these values in
`replica_control.migration_budget`. The same object is copied to the node
runtime when several groups share one physical node, and all groups must agree
on it. A preparation input may set the optional top-level `migration_budget`
object; omitted input uses the defaults above. Its shape is:

```json
"migration_budget": {
  "max_active": 2,
  "cpu": {"bytes_per_second": 67108864, "burst_bytes": 4194304},
  "disk_read": {"bytes_per_second": 67108864, "burst_bytes": 4194304},
  "disk_write": {"bytes_per_second": 67108864, "burst_bytes": 4194304},
  "network_send": {"bytes_per_second": 33554432, "burst_bytes": 2097152},
  "network_receive": {"bytes_per_second": 33554432, "burst_bytes": 2097152}
}
```

The classes are independent. A source disk read does not spend the source
network-send allowance, and a target network receive is not charged again as
source disk work. A phase can account several classes when it really performs
several kinds of work, but each class is charged once at its corresponding
execution boundary.

The node-log owner also feeds a lightweight foreground-pressure controller
from the lock-free `NodeSubmissionSequencer.Stats()` snapshot every 250 ms. It
uses queue occupancy plus interval deltas for backpressure submissions and
ready-queue wait time; the initial sample establishes a baseline, and a
counter reset never becomes a synthetic pressure spike. A high queue, wait
window, or backpressure event immediately halves the effective migration rate
and clamps accumulated tokens to the new scaled burst. Severe pressure for
two samples pauses new heavyweight phases before their next bounded chunk.
The pause is visible as `Metrics().Pressure.Paused`; it does not revoke work
already in a bounded read, write, or hash. Three quiet samples recover one
additive rate step and wake paused work, without a catch-up token windfall.
The sampler reads detached atomics and never holds a foreground, Raft, or
authority lock while migration waits. The byte-work CPU class remains a proxy,
and pressure feedback does not claim an operating-system CPU quota or a zero
latency penalty under continuous foreground saturation.

`CPU` is a serialized snapshot encoding and hashing byte-work proxy. It does
not reserve cores or impose an operating-system CPU quota. Artifact framing
may perform small encode steps before bytes reach the budgeted writer, and the
repository's final artifact verification remains a serialized repository
operation. Host CPU and storage telemetry are required when those limits need
to be measured directly.

## What is paced

Source export pins one immutable Raft/data cut and performs its existing two
passes. Both passes charge serialized bytes. Repository publication charges
each bounded write before `Repository.Append` and retains the durable cursor.
The source data service limits each response body to the smallest configured
CPU, source-read, and network-send burst, charges the read and hash before
the operation, and writes the body only after network admission.

The target receives a body in network-receive burst segments and refreshes the
transport read deadline for each segment after local budget admission. It
checks the complete chunk hash before acquiring an active permit, then appends
disk-write burst segments. Each successful append advances the repository's
durable offset, so a reconnect resumes at that offset without replaying a
partial append. Cold learner installation uses the same budgeted reader while
the artifact stage applies and checkpoints verified rows.

The receiver does not hold an active local permit while waiting for the source
connection or for network pacing. This avoids a source/target cycle in which
both nodes hold the last permit while waiting for one another. Source and
target operations still share their own node's active capacity across all
groups.

Budget-backed source providers, data services, and cold receivers allocate
chunk workspaces through the node pool and release them after the operation
becomes idle. Source providers reserve their artifact and transfer workspaces
as one request; this keeps logical transfer working memory below the shared
pool instead of retaining a chunk-sized buffer for every group that has ever
moved.
The Go allocator may keep released pages in its process heap; use process RSS
and runtime heap telemetry when sizing the host.

## Cancellation, retry, and shutdown

Pass the operation context through source control, transfer, and installation.
Cancellation wakes active-permit and token waiters immediately. A canceled
lease releases its active permit exactly once. Token reservations are not
refunded: a reservation may have crossed an I/O boundary, and refunding it
would allow a retry to spend the same capacity twice. The durable repository
cursor is the only resume authority.

Allow the operation deadline to include the configured pacing time. A slow but
healthy budget is not treated as a stalled peer: source writes and target body
reads install a fresh transport deadline after their local pacing wait. A
deadline still bounds a peer that stops responding. Closing a node budget wakes
all waiters and leaves already acquired leases safe to release during orderly
shutdown.

Stop source and bootstrap listeners before closing their retained providers.
The provider waits for pinned export plans to return their workspaces, then
closes the repository. It does not hold its provider mutex while a paced
export runs, so observation, release, and shutdown do not serialize behind a
rate wait.

## Observability

The budget exposes a detached `Metrics` value. Read it from
`Budget.Metrics()` or from the transfer surfaces:

- `snapshottransfer.Service.Stats().Budget` reports source-side work;
- `snapshottransfer.Receiver.Metrics().Budget` reports target-side work; and
- `migrationbudget.ResourceMetrics` reports cumulative bytes, throttle events,
  throttled nanoseconds, current available tokens, configured rate, and burst
  for CPU proxy, disk read, disk write, network send, and network receive.

The node-level fields also include active capacity, current active permits,
active waiters, workspace usage and capacity, workspace waiters, acquisitions,
cancellations, releases, budget calls, and budget errors. Counters are
process-local and monotonic until restart. Available tokens are a scheduling
hint and can refill while a status response is being read; the configured rate,
burst, and workspace ceiling are the enforceable limits.

Watch for increasing throttle events or waiters together with migration
progress. If foreground latency rises, the node pressure controller lowers
effective migration rates or pauses new phases while the configured ceilings
remain in force. If transfers repeatedly hit their deadline, increase
the operation deadline to cover the selected rate after checking peer health;
do not mint a new operation identity or discard its cursor.

## Boundaries and limits

The budget bounds the migration paths that call it. It does not make a
filesystem, host scheduler, kernel socket buffer, or every internal encoder
instruction a hard quota. Final repository publish verification scans and
hashes the complete artifact through paced read/hash segments, while the
repository still serializes the metadata transition; large artifacts remain a
possible local latency bottleneck. Measure it separately when sizing a node.

The artifact descriptor, membership fence, source and target identities, and
repository cursor remain authoritative during retries. Budget metrics never
grant membership, serving authority, or a promotion. Only the catalog and
Raft barriers can complete a scale operation.

## Source map

| Boundary | Source |
| --- | --- |
| Node-scoped rates, permits, cancellation, and metrics | [`internal/migrationbudget/budget.go`](../../internal/migrationbudget/budget.go) and [`internal/migrationbudget/pressure.go`](../../internal/migrationbudget/pressure.go) |
| RF3 manifest construction and node pressure sampler | [`cmd/vibedb-shard/rf3_migration.go`](../../cmd/vibedb-shard/rf3_migration.go) and [`cmd/vibedb-shard/rf3_migration_pressure.go`](../../cmd/vibedb-shard/rf3_migration_pressure.go) |
| Two-pass source export and durable cursor writes | [`internal/snapshottransfer/source_export.go`](../../internal/snapshottransfer/source_export.go) |
| Retained source ownership and provider injection | [`internal/snapshottransfer/source_provider.go`](../../internal/snapshottransfer/source_provider.go) |
| Snapshot network sender and receiver pacing | [`internal/snapshottransfer/service.go`](../../internal/snapshottransfer/service.go) |
| Actual I/O segmentation and paced verification readers | [`internal/snapshottransfer/budget_io.go`](../../internal/snapshottransfer/budget_io.go) |
| Target artifact staging and learner install | [`internal/snapshottransfer/learner_install.go`](../../internal/snapshottransfer/learner_install.go) |
| Durable offset, append, publication, and recovery | [`internal/snapshottransfer/repository.go`](../../internal/snapshottransfer/repository.go) |
