# Online replica migration library

[Documentation](../README.md) / [Operations](README.md)

This milestone adds the reusable migration pacing library and wires it into
the snapshot-transfer data path. It covers source artifact export, resumable
transfer, target staging, repository verification, and cold learner install.
Runtime construction of a node budget, manifest persistence, and command-line
configuration are still pending integration in the node runtime.

## Node-scoped budget

Construct one `migrationbudget.Budget` for each physical process or node and
inject that same pointer into every group provider, source service, receiver,
repository, and learner install plan owned by the node. Constructing one
budget per group multiplies the configured allowance as groups are added.
Passing `nil` preserves the package's legacy unpaced behavior for callers that
have not adopted the integration yet.

The default is conservative:

| Class | Sustained rate | Burst |
| --- | ---: | ---: |
| Serialized snapshot work (CPU byte-work proxy) | 64 MiB/s | 4 MiB |
| Source artifact reads | 64 MiB/s | 4 MiB |
| Repository and target-stage writes | 64 MiB/s | 4 MiB |
| Snapshot network sends | 32 MiB/s | 2 MiB |
| Snapshot network receives | 32 MiB/s | 2 MiB |
| Concurrent local heavy phases | 2 | — |
| Transient migration workspaces | 16 MiB total | — |

`BytesPerSecond` is the sustained allowance and `BurstBytes` is the largest
actual operation issued to that class. `ConsumeChunk` returns the exact
admitted segment, so callers split reads, writes, hashes, and network body
operations at the current burst boundary. The accepted configuration bounds
rates and bursts before a budget starts: `MaxActive` is 1 through 256, rates
are positive and at most 1 TiB/s, and bursts are positive and at most 1 GiB.
Invalid values fail with `migrationbudget.ErrInvalidConfig`.

CPU is a serialized snapshot encoding and hashing byte-work proxy. It does not
reserve cores or impose an operating-system CPU quota. Framing may perform
small encode steps before bytes reach the budgeted writer, and repository
verification remains a serialized repository operation; measure host CPU and
storage separately when those limits matter.

Disk and network classes are independent. A source disk read does not spend
the source network-send allowance, and a target network receive is not charged
as source disk work. A phase may account several classes when it really
performs several kinds of work, but each class is charged at its execution
boundary.

## Workspace and active capacity

`BufferBytes` is a node-wide transient workspace ceiling separate from the
active phase semaphore. Source export reserves its artifact and transfer
workspaces atomically. Senders and receivers use directional credits, so
opposite transfer directions cannot each hold the last local credit while
waiting for a peer socket. Buffer waits do not hold an active phase permit,
and reservations are released when the operation becomes idle.

Production callers should supply a budget so idle providers retain no
chunk-sized buffers. The Go allocator may keep released pages in its process
heap; use process RSS and runtime heap telemetry when sizing a host.

## Foreground pressure

`Budget.ApplyPressure` accepts detached samples from a host's foreground
durability lane. The controller uses queue occupancy plus interval deltas for
backpressure submissions and ready-queue wait time. The first sample
establishes a baseline. A high sample downshifts effective rates and clamps
accumulated tokens; severe pressure for the configured number of windows marks
new heavyweight phases paused before their next bounded chunk. Quiet windows
recover one additive rate step and wake paused work without a catch-up token
windfall. The controller's mutex is internal and is never held while
migration work waits. Wiring a sampler into the shipped node runtime remains
pending.

## Pacing boundaries

Source export pins one immutable data cut and performs its existing two passes.
Both passes charge serialized bytes. Repository publication charges each
bounded write before `Repository.Append` and retains the durable cursor.

The source data service limits each response body to the smallest configured
CPU, source-read, and network-send burst. It charges the read and hash before
the operation and writes the body only after network admission. The target
receiver reads network segments, verifies the complete chunk hash, and then
appends disk-write segments. A reconnect resumes at the repository's durable
offset without replaying a partial append. Cold learner installation uses the
same budgeted reader while staging and checkpointing the artifact.

The receiver does not hold an active permit while waiting for the source
connection or network pacing. Source and target operations share their own
node's active capacity across groups, while transport waits remain
cancellation-aware.

## Cancellation and retry

Pass the operation context through source control, transfer, and installation.
Cancellation wakes active, buffer, pressure, and token waiters immediately.
A canceled lease releases its active permit exactly once. Token reservations
are not refunded: a reservation may have crossed an I/O boundary, and
refunding it would let a retry spend the same capacity twice. The durable
repository cursor is the resume authority.

Allow operation deadlines to include configured pacing time. A slow but
healthy budget should not be mistaken for a stalled peer; transport deadlines
are refreshed around locally paced segments, while a peer that stops
responding remains bounded by the operation deadline. Closing a budget wakes
all waiters, and already-acquired leases remain safe to release during
shutdown.

## Observability and limits

`Budget.Metrics()` returns detached node-wide active, buffer, cancellation,
release, throttle, and pressure state. Resource metrics cover CPU proxy, disk
read, disk write, network send, and network receive with cumulative bytes,
throttle events, throttled time, current tokens, configured rate, and burst.
`Service.Stats().Budget` and `Receiver.Metrics().Budget` expose the same
resource accounting at their transfer surfaces. Counters are process-local
and monotonic until restart; available tokens are a scheduling hint.

The budget bounds the migration paths that call it. It does not make a
filesystem, host scheduler, kernel socket buffer, or every encoder instruction
a hard quota. Final artifact verification is paced in bounded read/hash
segments, while repository metadata transitions remain serialized. Budget
metrics do not grant membership, serving authority, or promotion; catalog and
Raft barriers remain the control-plane authority.

## Runtime integration still required

The current library integration is intentionally usable by direct Go callers.
The following deployment work is outside this milestone and must be completed
before claiming shipped node-wide enforcement:

- create one budget owner during node startup and inject it into every group's
  source provider, service, receiver, and learner installer;
- persist and validate operator budget fields in the node preparation/config
  format;
- feed `NodeSubmissionSequencer.Stats()` into `ApplyPressure` on a bounded
  sampler lifecycle; and
- expose budget status in node and migration observability surfaces.

Until those hooks land, callers must explicitly construct and share the
budget. This document makes no zero-latency or zero-impact claim.

## Source map

| Boundary | Source |
| --- | --- |
| Node-scoped rates, permits, cancellation, pressure, and metrics | [`internal/migrationbudget/budget.go`](../../internal/migrationbudget/budget.go) and [`internal/migrationbudget/pressure.go`](../../internal/migrationbudget/pressure.go) |
| Actual I/O segmentation and paced verification readers | [`internal/snapshottransfer/budget_io.go`](../../internal/snapshottransfer/budget_io.go) |
| Two-pass source export and durable cursor writes | [`internal/snapshottransfer/source_export.go`](../../internal/snapshottransfer/source_export.go) |
| Retained source ownership and budget injection | [`internal/snapshottransfer/source_provider.go`](../../internal/snapshottransfer/source_provider.go) |
| Snapshot network sender and receiver pacing | [`internal/snapshottransfer/service.go`](../../internal/snapshottransfer/service.go) |
| Target artifact staging and learner install | [`internal/snapshottransfer/learner_install.go`](../../internal/snapshottransfer/learner_install.go) |
| Durable append, publication, recovery, and verification | [`internal/snapshottransfer/repository.go`](../../internal/snapshottransfer/repository.go) |
