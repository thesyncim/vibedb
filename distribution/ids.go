package distribution

// ShardID is a stable, unique identifier for one physical shard within a
// distribution. It is opaque to this dependency-free package.
type ShardID string

// EndpointID is an opaque handle for a shard endpoint. This package never
// interprets it; transport code resolves it to an address.
type EndpointID string

// OwnershipEpoch fences stale writers independently from routing versions.
type OwnershipEpoch uint64

// ShardAllocationGeneration is a topology-authority allocation sequence within
// one distribution. It is globally monotonic for that distribution and never
// reused, so catalog lineage remains provable after a shard disappears without
// comparing unrelated shards' Raft-derived OwnershipEpoch values.
type ShardAllocationGeneration uint64

// RoutingVersion identifies one immutable manifest generation for a
// distribution.
type RoutingVersion uint64

// DistributionName names a logical keyspace shared by colocated tables.
type DistributionName string
