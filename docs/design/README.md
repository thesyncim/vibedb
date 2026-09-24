# Design guide

[Documentation](../README.md) / Design

VibeDB shares one document and storage model across embedded Go, SQL, and
distributed execution. These guides explain where work happens, who owns
state, what a successful operation establishes, and where the design is still
incomplete. Every distributed page ends with its limitations and the tests
that support its claims.

## Read the system in layers

| Layer | Questions answered | Guide |
| --- | --- | --- |
| System | What runs in the application or on a physical node? | [Architecture](../architecture.md) |
| Data | How do collection names, keys, JSON, and indexes relate? | [Data model](../data-model.md) |
| Execution | How does a plan become scans, probes, joins, and aggregates? | [Query execution](query-execution.md) |
| Transactions | How are conflicts detected and participant changes published? | [Transactions](../transactions.md) |
| Persistence | When is an acknowledged write recoverable? | [Durability](../durability.md) |
| Storage | Which handle owns memory, snapshots, files, and checkpoints? | [Storage engines](../store.md) |
| Encoding | What is authenticated and validated during reopen? | [On-disk format](../format.md) |
| Optimization | How do locality, statistics, and packed columns reduce work? | [Distributed optimizer](../distributed-optimizer.md), [SIMD](../simd.md) |

## Distributed design

Read these in order for the RF3 runtime. The
[distributed internals](../operations/distributed.md) guide condenses them for
operators.

| Page | Covers |
| --- | --- |
| [Request routing](routing.md) | Frontends, catalog generations, the replicated catalog and route seeds, command fences, route forwarding, schema generations. |
| [Replication](replication.md) | RF3 groups, the Raft configuration, execution lanes, per-group WALs and the shared node log, retention, snapshots, membership changes, peer transport. |
| [Exactly-once writes](exactly-once-writes.md) | Request identity, direct and coordinated lanes, the request ledger, route-gate sessions, execution pins, outcome-unknown recovery, PostgreSQL writes. |
| [Distributed transactions](distributed-transactions.md) | Fused prepare/decide/apply waves, intents, recovery, and the static lane. |
| [Reads, leases, and time](reads-and-time.md) | Read modes, quorum read authority, follower reads, time domains, clock-fault qualification. |
| [Topology changes](topology-changes.md) | Snapshot transfer, replica moves, seamless scale-out and scale-in, frontend drain, hot-shard splits, migration pacing. |
| [Backup and restore internals](backup-restore.md) | Per-group cut vectors, certificate-last publication, fresh-identity restore. |
| [Security model](security.md) | Trust model, TLS identities and traffic classes, capabilities, delegation. |

## What is proven, and where

The dedicated workflows below reject skipped or incomplete runs of their
named tests. "Partial" in the [distributed feature ledger](../distributed-feature-state.md)
marks what these gates do not yet cover.

| Workflow | Qualification test | What it exercises |
| --- | --- | --- |
| [`fused-node-rf3.yml`](../../.github/workflows/fused-node-rf3.yml) | `TestFusedRF3NodeProcessQualification` | Shipped physical-node launcher (3 and 6 nodes), online group enrollment, concurrent writes, `SIGKILL` of every process, and reopen. |
| [`durable-rf3-external.yml`](../../.github/workflows/durable-rf3-external.yml) | `TestGatewayDurableRF3ExternalProcessRecovery`, `TestGatewayReadBatchRF3ExternalProcessChaos` | Lost terminal and ACK responses, killed leaders, stopped voter, replacement frontend, rolling restarts; exact-key read vectors under partition. |
| [`durable-rf3-multirelation.yml`](../../.github/workflows/durable-rf3-multirelation.yml) | `TestGatewayDurableRF3MultiRelationChaosProcess` | Atomic writes across two tables and cross-hosted global indexes under partition, leader kill, and frontend replacement. |
| [`seamless-scale-in-out.yml`](../../.github/workflows/seamless-scale-in-out.yml) | `TestSeamlessScaleInOutProcessQualification` | Three 3 → 4 → 3 physical-node cycles under open-loop traffic with latency, throughput, and acknowledgement-conservation bounds. |
| [`dev-hot-split.yml`](../../.github/workflows/dev-hot-split.yml) | `TestGatewayZeroConfigDevPressureCompletesReplicatedSplit`, `TestGatewayCustomTableDevPressureCompletesReplicatedSplitAndRestart` | Pressure-driven terminal splits through the launcher. |
| [`clock-fault-matrix.yml`](../../.github/workflows/clock-fault-matrix.yml) | Five gates in [`clock-fault-matrix.sh`](../../scripts/ci/clock-fault-matrix.sh) | UTC steps, logical pulses, leader isolation, process suspend, kill and partition. |
| [`wal-retention.yml`](../../.github/workflows/wal-retention.yml) | `TestServeRF3WALRetentionCrashQualification` | Log generation replacement across repeated `SIGKILL`. |
| [`restore-rf3-external.yml`](../../.github/workflows/restore-rf3-external.yml) | `TestRestoredRF3ExternalProcessServingAndFailover` | Fresh-identity restore, activation, and failover. |
| [`ci.yml`](../../.github/workflows/ci.yml) | Replica replacement, hot-shard moves, two-frontend terminal recovery, authenticated transport, quorum-cut enumeration | See the job names cited on each design page. |

Most process gates run only on Linux. None measures total Raft or network
bytes, and none covers mixed builds.

## Terms used across the guides

| Term | Meaning |
| --- | --- |
| Collection | Named set of keyed JSON documents. |
| Generation | An immutable published view; logical, physical, catalog, and schema generations have different roles. |
| Snapshot | A pinned view whose lifetime belongs to its caller. |
| Shard | A routed portion of a distribution, served by one Raft group. |
| Raft group | One independently ordered replicated log and state machine. |
| Replica | One member's copy of a group. RF3 normally has three voting replicas. |
| Physical node | One serving process that hosts replicas from many groups. |
| Frontend | The request-routing layer (gateway), embedded in each physical node or run standalone. |
| Command fence | Exact routing, ownership, schema, and identity coordinates that a replica checks before admitting a command. |
| Applied cut | A group's committed history through a particular applied index. |
| Request identity | The authenticated key under which a write's first committed outcome is retained. |

A catalog generation can route an operation across groups, but it does not
give those groups a shared snapshot timestamp. Sharing a node log does not
combine separate Raft groups into one consensus domain.

## Design changes and evidence

[Research and proposals](research.md) indexes live plans; the
[history index](../history/README.md) keeps superseded proposals and dated
experiment records. Use [benchmark reports](../benchmarks/README.md) for
measured behavior and [qualification records](../qualification/README.md) for
specific correctness and fault runs. Their revision and scope matter when
deciding whether a result applies to a new change.

For commands and recovery procedures, use the [operator guide](../operations/README.md).
