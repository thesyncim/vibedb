# Design guide

[Documentation](../README.md) / Design

VibeDB shares one document and storage model across embedded Go, SQL, and
distributed execution. These guides explain where work happens, who owns
state, and what a successful operation establishes.

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
| Distribution | How do catalog fences, Raft, retries, and replacement interact? | [Distributed internals](../operations/distributed.md) |
| Optimization | How do locality, statistics, and packed columns reduce work? | [Distributed optimizer](../distributed-optimizer.md), [SIMD](../simd.md) |

Start with architecture and follow the layer relevant to your change. Each
technical guide links the owning implementation and decisive tests.

## Terms used across the guides

| Term | Meaning |
| --- | --- |
| Collection | Named set of keyed JSON documents. |
| Generation | An immutable published view; logical, physical, and durable generations have different roles. |
| Snapshot | A pinned view whose lifetime belongs to its caller. |
| Shard | A routed portion of a distribution. |
| Raft group | One independently ordered replicated log and state machine. |
| Replica | One member's copy of a group. RF3 normally has three voting replicas. |
| Physical node | One serving process that can own multiple replicas from different groups. |
| Catalog fence | Exact routing, ownership, schema, and identity coordinates used to admit an operation. |
| Applied cut | A group's committed history through a particular applied index. |

A catalog generation can route an operation across groups, but it does not
give those groups a shared snapshot timestamp. Likewise, sharing a node log
does not combine separate Raft groups into one consensus domain.

## Design changes and evidence

[Research and proposals](research.md) keeps planned work and dated experiment
records discoverable. Use [benchmark reports](../benchmarks/README.md) for
measured behavior and [qualification records](../qualification/README.md) for
specific correctness and fault runs. Their revision and scope matter when
deciding whether a result applies to a new change.

For commands and recovery procedures, use the [operator guide](../operations/README.md).
