package main

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicaaction"
)

type rf3RetiredDonors interface {
	Unregister(raftmember.RuntimeIdentity) error
}

// Called only after the action journal and serialized owner have authorized
// and closed this exact source. Schema evolution does not change its physical
// identity, while a replacement member must keep its own service inventory.
func rf3ReplicaRetirementCleanup(schemas *rf3SchemaActivator, donors rf3RetiredDonors, serving *atomic.Int64, native ...*rf3NativeAuthorities) func(context.Context, replicaaction.Request) error {
	var serial sync.Mutex
	return func(ctx context.Context, request replicaaction.Request) error {
		if ctx == nil || schemas == nil || request.Kind != replicaaction.SourceRetirement {
			return replicaaction.ErrControl
		}
		serial.Lock()
		defer serial.Unlock()
		schemas.mu.RLock()
		state := schemas.groups[request.Fence.Group]
		schemas.mu.RUnlock()
		if state == nil {
			return nil
		}
		state.mu.Lock()
		identity := state.identity
		state.mu.Unlock()
		fence := request.Fence
		if identity.Group != fence.Group || identity.MemberID != fence.MemberID || identity.StoreID != fence.StoreID ||
			identity.NodeIncarnation != fence.NodeIncarnation || identity.AllocationGeneration != fence.AllocationGeneration {
			return raftservice.ErrServingFence
		}
		for _, authority := range native {
			if err := authority.unregisterDynamic(identity); err != nil {
				return err
			}
		}
		if donors != nil {
			if err := donors.Unregister(identity); err != nil {
				return err
			}
		}
		removed, err := schemas.RemoveRetired(identity)
		if err != nil {
			return err
		}
		if removed && serving != nil {
			for {
				count := serving.Load()
				if count <= 0 || serving.CompareAndSwap(count, count-1) {
					break
				}
			}
		}
		return nil
	}
}
