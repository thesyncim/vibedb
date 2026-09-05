package replicacontrol

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type capacityProviderSource struct {
	identity raftmember.RuntimeIdentity
	sample   CapacitySourceSample
	err      error
	request  *CapacityRequest
}

func (source *capacityProviderSource) Identity() raftmember.RuntimeIdentity {
	return source.identity
}

func (source *capacityProviderSource) ObserveCapacity(ctx context.Context) (CapacitySourceSample, error) {
	if err := context.Cause(ctx); err != nil {
		return CapacitySourceSample{}, err
	}
	return source.sample, source.err
}

func (source *capacityProviderSource) ObserveCapacityRequest(ctx context.Context, request CapacityRequest) (CapacitySourceSample, error) {
	if source.request != nil {
		*source.request = request
	}
	return source.ObserveCapacity(ctx)
}

func capacityProviderIdentity(group raftmember.GroupKey, member, node byte) raftmember.RuntimeIdentity {
	return raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 10, MemberID: uint64(member), StoreID: [16]byte{member + 20}, NodeIncarnation: uint64(node)}
}

func capacityProviderNode(incarnation byte) NodeCapacity {
	return NodeCapacity{NodeID: rafttransport.NodeID{99}, NodeIncarnation: uint64(incarnation), Revision: 4,
		Capacity: autosplit.CapacityVector{autosplit.ResourceLiveBytes: 1 << 30}, MigrationCapacity: 1 << 30,
		MaxReceives: 8, ActiveReceives: 2}
}

func capacityProviderSample(identity raftmember.RuntimeIdentity, demand, migration uint64, empty bool) CapacitySourceSample {
	result := CapacitySourceSample{Identity: identity, Applied: 20, MigrationBytes: migration, KnownEmpty: empty,
		DemandKind: CapacityDemandMeasured}
	result.Demand[autosplit.ResourceLiveBytes] = demand
	if empty {
		result.Demand = autosplit.CapacityVector{}
	}
	return result
}

func TestCapacityProviderEnumeratesColdNonEmptyAndKnownEmptySources(t *testing.T) {
	group := capacityTestGroup()
	identity := capacityProviderIdentity(group, 8, 14)
	request := capacityTestRequest()
	var seen CapacityRequest
	source := &capacityProviderSource{identity: identity, sample: capacityProviderSample(identity, 4096, 8192, false), request: &seen}
	otherGroup := group
	otherGroup.GroupID[0]++
	otherIdentity := capacityProviderIdentity(otherGroup, 9, 14)
	other := &capacityProviderSource{identity: otherIdentity, sample: capacityProviderSample(otherIdentity, 256, 0, true)}
	provider, err := NewCapacityProvider(CapacitySourceDirectory{
		Sources: func(context.Context) ([]CapacitySource, error) { return []CapacitySource{source, other}, nil },
		Node:    func(context.Context) (NodeCapacity, error) { return capacityProviderNode(14), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	observation, err := provider.ObserveReplicaCapacity(context.Background(), request)
	if err != nil {
		t.Fatalf("nonempty cold observation: %v", err)
	}
	if seen != request || observation.Demand[autosplit.ResourceLiveBytes] != 4096 || observation.MigrationBytes != 8192 ||
		observation.KnownEmpty || observation.DemandKind != CapacityDemandMeasured {
		t.Fatalf("nonempty observation=%+v request=%+v", observation, seen)
	}
	emptyRequest := request
	emptyRequest.Group = otherGroup
	emptyRequest.TargetMember = 9
	emptyObservation, err := provider.ObserveReplicaCapacity(context.Background(), emptyRequest)
	if err != nil {
		t.Fatalf("known-empty cold observation: %v", err)
	}
	if !emptyObservation.KnownEmpty || emptyObservation.Demand != (autosplit.CapacityVector{}) || emptyObservation.MigrationBytes != 0 {
		t.Fatalf("known-empty observation=%+v", emptyObservation)
	}
}

func TestCapacityProviderRejectsIncompleteInventoryAndIdentityMutation(t *testing.T) {
	group := capacityTestGroup()
	identity := capacityProviderIdentity(group, 8, 14)
	source := &capacityProviderSource{identity: identity, sample: capacityProviderSample(identity, 64, 128, false)}
	request := capacityTestRequest()
	provider, err := NewCapacityProvider(CapacitySourceDirectory{
		Sources: func(context.Context) ([]CapacitySource, error) { return []CapacitySource{source}, nil },
		Node:    func(context.Context) (NodeCapacity, error) { return capacityProviderNode(14), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ObserveReplicaCapacity(context.Background(), request); err != nil {
		t.Fatalf("baseline provider observation: %v", err)
	}
	mutated := source.sample
	mutated.Identity.StoreID[0]++
	source.sample = mutated
	if _, err := provider.ObserveReplicaCapacity(context.Background(), request); !errors.Is(err, ErrCapacityStale) {
		t.Fatalf("mutated source identity error=%v", err)
	}
	source.sample = capacityProviderSample(identity, 64, 128, false)
	if _, err := provider.ObserveReplicaCapacity(context.Background(), request); err != nil {
		t.Fatalf("restored provider observation: %v", err)
	}
	source.identity.NodeIncarnation++
	if _, err := provider.ObserveReplicaCapacity(context.Background(), request); !errors.Is(err, ErrCapacityStale) {
		t.Fatalf("source restart error=%v", err)
	}
	missing, err := NewCapacityProvider(CapacitySourceDirectory{
		Sources: func(context.Context) ([]CapacitySource, error) { return nil, nil },
		Node:    func(context.Context) (NodeCapacity, error) { return capacityProviderNode(14), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.ObserveReplicaCapacity(context.Background(), request); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("incomplete inventory error=%v", err)
	}
}

func TestCapacityProviderNodeCutIncludesMultiGroupBudgetAndCancellation(t *testing.T) {
	group := capacityTestGroup()
	identity := capacityProviderIdentity(group, 8, 14)
	source := &capacityProviderSource{identity: identity, sample: capacityProviderSample(identity, 64, 128, false)}
	var nodeCalls int
	provider, err := NewCapacityProvider(CapacitySourceDirectory{
		Sources: func(context.Context) ([]CapacitySource, error) { return []CapacitySource{source}, nil },
		Node: func(ctx context.Context) (NodeCapacity, error) {
			nodeCalls++
			if err := context.Cause(ctx); err != nil {
				return NodeCapacity{}, err
			}
			return capacityProviderNode(14), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.ObserveReplicaCapacity(context.Background(), capacityTestRequest()); err != nil {
		t.Fatalf("node cut: %v", err)
	}
	if nodeCalls != 1 {
		t.Fatalf("node calls=%d, want one bounded node cut", nodeCalls)
	}
	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(context.Canceled)
	if _, err := provider.ObserveReplicaCapacity(canceled, capacityTestRequest()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cut error=%v", err)
	}

	overflow, overflowed := AddCapacity(math.MaxUint64, 1)
	if !overflowed || overflow != math.MaxUint64 {
		t.Fatalf("saturated scalar=(%d,%v)", overflow, overflowed)
	}
	left := autosplit.CapacityVector{}
	left[autosplit.ResourceLiveBytes] = math.MaxUint64
	right := autosplit.CapacityVector{}
	right[autosplit.ResourceLiveBytes] = 1
	vector, overflowed := AddCapacityVectors(left, right)
	if !overflowed || vector[autosplit.ResourceLiveBytes] != math.MaxUint64 {
		t.Fatalf("saturated vector=(%d,%v)", vector[autosplit.ResourceLiveBytes], overflowed)
	}
}
