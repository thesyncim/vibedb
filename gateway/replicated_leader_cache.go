package gateway

import (
	"encoding/binary"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
)

const replicatedLeaderHintWays = 4

type replicatedLeaderHintKey struct {
	group                raftmember.GroupKey
	allocationGeneration uint64
	command              raftservice.CommandFence
}

type replicatedLeaderHintEntry struct {
	endpoint ReplicatedEndpoint
	state    shardservice.ReplicatedMemberState
	valid    bool
}

func (entry *replicatedLeaderHintEntry) matches(key replicatedLeaderHintKey) bool {
	return entry.valid && entry.state.Fence.Group == key.group &&
		entry.state.Fence.AllocationGeneration == key.allocationGeneration &&
		entry.state.Fence.Command == key.command
}

// A set owns at most four exact route hints. The final set may expose fewer
// ways when the configured capacity is not a multiple of four. One lock per
// set prevents unrelated shard traffic from contending while keeping lookup,
// publication, and invalidation allocation-free after construction.
type replicatedLeaderHintSet struct {
	mu      sync.RWMutex
	entries [replicatedLeaderHintWays]replicatedLeaderHintEntry
	next    uint8
	ways    uint8
}

type replicatedLeaderHintCache struct {
	sets []replicatedLeaderHintSet
	mask uint64
}

func newReplicatedLeaderHintCache(capacity int) replicatedLeaderHintCache {
	setCount := (capacity + replicatedLeaderHintWays - 1) / replicatedLeaderHintWays
	cache := replicatedLeaderHintCache{sets: make([]replicatedLeaderHintSet, setCount)}
	remaining := capacity
	for index := range cache.sets {
		ways := replicatedLeaderHintWays
		if remaining < ways {
			ways = remaining
		}
		cache.sets[index].ways = uint8(ways)
		remaining -= ways
	}
	if setCount > 1 && setCount&(setCount-1) == 0 {
		cache.mask = uint64(setCount - 1)
	}
	return cache
}

func replicatedLeaderKey(route ReplicatedRoute) replicatedLeaderHintKey {
	return replicatedLeaderHintKey{group: route.Group,
		allocationGeneration: route.AllocationGeneration, command: route.Command}
}

func replicatedLeaderHash(key replicatedLeaderHintKey) uint64 {
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
	return hash
}

func (cache *replicatedLeaderHintCache) set(key replicatedLeaderHintKey) *replicatedLeaderHintSet {
	if cache == nil || len(cache.sets) == 0 {
		return nil
	}
	hash := replicatedLeaderHash(key)
	if cache.mask != 0 {
		return &cache.sets[hash&cache.mask]
	}
	return &cache.sets[hash%uint64(len(cache.sets))]
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
	set := cache.set(key)
	if set == nil {
		return ReplicatedEndpoint{}, shardservice.ReplicatedMemberState{}, false
	}
	set.mu.RLock()
	var entry replicatedLeaderHintEntry
	for way := uint8(0); way < set.ways; way++ {
		candidate := &set.entries[way]
		if candidate.matches(key) {
			entry = *candidate
			break
		}
	}
	set.mu.RUnlock()
	current, _, found := replicatedEndpoint(route, entry.endpoint.Member)
	if !entry.valid || !found || !sameReplicatedEndpoint(current, entry.endpoint) ||
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
	set := cache.set(key)
	if set == nil {
		return
	}
	set.mu.Lock()
	for way := uint8(0); way < set.ways; way++ {
		entry := &set.entries[way]
		if entry.matches(key) {
			// Never let a delayed response for the same allocation overwrite a
			// newer leadership term.
			if state.Fence.Term >= entry.state.Fence.Term {
				*entry = replicatedLeaderHintEntry{endpoint: endpoint, state: state, valid: true}
			}
			set.mu.Unlock()
			return
		}
	}
	way := set.next
	for candidate := uint8(0); candidate < set.ways; candidate++ {
		if !set.entries[candidate].valid {
			way = candidate
			break
		}
	}
	set.entries[way] = replicatedLeaderHintEntry{endpoint: endpoint, state: state, valid: true}
	set.next = (way + 1) % set.ways
	set.mu.Unlock()
}

func (cache *replicatedLeaderHintCache) invalidate(
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	state shardservice.ReplicatedMemberState,
) {
	key := replicatedLeaderKey(route)
	set := cache.set(key)
	if set == nil {
		return
	}
	set.mu.Lock()
	for way := uint8(0); way < set.ways; way++ {
		entry := &set.entries[way]
		// Compare the exact hint consumed by the caller. A slow failure must
		// not erase a newer term concurrently published by another request.
		if entry.matches(key) && sameReplicatedEndpoint(entry.endpoint, endpoint) &&
			entry.state == state {
			*entry = replicatedLeaderHintEntry{}
			break
		}
	}
	set.mu.Unlock()
}
