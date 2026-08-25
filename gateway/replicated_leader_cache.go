package gateway

import (
	"encoding/binary"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
)

// The cache is deliberately direct-mapped and fixed-size. A collision only
// costs one probe; it can never grow with tenant, shard, or failover count.
// Once the executor is constructed, lookup, publication, and invalidation do
// not allocate.
const replicatedLeaderHintSlots = 256

type replicatedLeaderHintKey struct {
	group                raftmember.GroupKey
	allocationGeneration uint64
	command              raftservice.CommandFence
}

type replicatedLeaderHintEntry struct {
	key      replicatedLeaderHintKey
	endpoint ReplicatedEndpoint
	state    shardservice.ReplicatedMemberState
	valid    bool
}

type replicatedLeaderHintSlot struct {
	mu    sync.RWMutex
	entry replicatedLeaderHintEntry
}

type replicatedLeaderHintCache struct {
	slots [replicatedLeaderHintSlots]replicatedLeaderHintSlot
}

func replicatedLeaderKey(route ReplicatedRoute) replicatedLeaderHintKey {
	return replicatedLeaderHintKey{group: route.Group,
		allocationGeneration: route.AllocationGeneration, command: route.Command}
}

func replicatedLeaderSlot(key replicatedLeaderHintKey) uint8 {
	// Mix fixed-width identity and every command generation. This is not a
	// security boundary; exact key comparison below rejects all collisions.
	hash := uint64(0x9e3779b97f4a7c15)
	for _, identity := range [...][16]byte{key.group.ClusterID, key.group.ClusterIncarnation,
		key.group.ShardIncarnation, key.group.GroupID} {
		for offset := 0; offset < len(identity); offset += 8 {
			hash ^= binary.LittleEndian.Uint64(identity[offset : offset+8])
			hash *= 0xbf58476d1ce4e5b9
			hash ^= hash >> 29
		}
	}
	hash ^= key.group.TopologyRecoveryEpoch
	hash ^= key.allocationGeneration
	hash ^= key.command.ReplicaSetVersion * 0x94d049bb133111eb
	hash ^= key.command.ActivePolicyGeneration
	hash ^= key.command.ProtectionEpoch << 7
	hash ^= key.command.OwnershipEpoch << 13
	hash ^= key.command.SchemaGeneration << 19
	hash ^= key.command.RoutingVersion << 23
	hash ^= key.command.RouteGeneration << 31
	for offset := 0; offset < len(key.command.RelationManifestDigest); offset += 8 {
		hash ^= binary.LittleEndian.Uint64(key.command.RelationManifestDigest[offset : offset+8])
		hash *= 0x9e3779b97f4a7c15
	}
	hash ^= hash >> 32
	return uint8(hash)
}

func sameReplicatedEndpoint(left, right ReplicatedEndpoint) bool {
	return left.Member == right.Member && left.Node == right.Node &&
		left.StoreID == right.StoreID && left.NodeIncarnation == right.NodeIncarnation &&
		left.Address == right.Address
}

func (cache *replicatedLeaderHintCache) lookup(
	route ReplicatedRoute,
) (ReplicatedEndpoint, shardservice.ReplicatedMemberState, bool) {
	key := replicatedLeaderKey(route)
	slot := &cache.slots[replicatedLeaderSlot(key)]
	slot.mu.RLock()
	entry := slot.entry
	slot.mu.RUnlock()
	current, _, found := replicatedEndpoint(route, entry.endpoint.Member)
	if !entry.valid || entry.key != key ||
		!found || !sameReplicatedEndpoint(current, entry.endpoint) ||
		!validReplicatedLeaderHint(route, entry.endpoint, entry.state) {
		return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{}, false
	}
	return entry.endpoint, entry.state, true
}

func (cache *replicatedLeaderHintCache) publish(
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	state shardservice.ReplicatedMemberState,
) {
	if !validReplicatedLeaderHint(route, endpoint, state) {
		return
	}
	key := replicatedLeaderKey(route)
	slot := &cache.slots[replicatedLeaderSlot(key)]
	slot.mu.Lock()
	entry := &slot.entry
	// Never let a delayed response for the same allocation overwrite a newer
	// leadership term. A different exact key is a normal bounded collision.
	if !entry.valid || entry.key != key || state.Fence.Term >= entry.state.Fence.Term {
		*entry = replicatedLeaderHintEntry{key: key, endpoint: endpoint, state: state, valid: true}
	}
	slot.mu.Unlock()
}

func (cache *replicatedLeaderHintCache) invalidate(
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	state shardservice.ReplicatedMemberState,
) {
	key := replicatedLeaderKey(route)
	slot := &cache.slots[replicatedLeaderSlot(key)]
	slot.mu.Lock()
	entry := &slot.entry
	// Compare the exact hint consumed by the caller. A slow failure must not
	// erase a newer term concurrently published by another request.
	if entry.valid && entry.key == key && sameReplicatedEndpoint(entry.endpoint, endpoint) &&
		entry.state == state {
		*entry = replicatedLeaderHintEntry{}
	}
	slot.mu.Unlock()
}
