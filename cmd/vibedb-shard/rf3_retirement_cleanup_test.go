package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicaaction"
)

type retirementDonorProbe struct {
	calls    int
	identity raftmember.RuntimeIdentity
	err      error
}

func (p *retirementDonorProbe) Unregister(identity raftmember.RuntimeIdentity) error {
	p.calls++
	p.identity = identity
	return p.err
}

func TestRF3RetirementCleanupWithdrawsOnlyExactLiveInventory(t *testing.T) {
	identity := raftmember.RuntimeIdentity{Group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5}},
		Distribution: "d", Shard: "s", AllocationGeneration: 6, MemberID: 7, StoreID: [16]byte{8}, NodeIncarnation: 9, RelationManifestDigest: [32]byte{10}}
	request := replicaaction.Request{Kind: replicaaction.SourceRetirement, Fence: raftservice.ServingFence{Group: identity.Group,
		AllocationGeneration: identity.AllocationGeneration, MemberID: identity.MemberID, StoreID: identity.StoreID, NodeIncarnation: identity.NodeIncarnation}}
	// The owner has already closed the retired SQL runtime. Cleanup must not
	// dereference it while withdrawing capacity, schema, and donor inventories.
	schemas := &rf3SchemaActivator{groups: map[raftmember.GroupKey]*rf3SchemaGeneration{identity.Group: {identity: identity}}}
	donors := new(retirementDonorProbe)
	var serving atomic.Int64
	serving.Store(2)
	cleanup := rf3ReplicaRetirementCleanup(schemas, donors, &serving)
	for _, mutate := range []func(*replicaaction.Request){
		func(r *replicaaction.Request) { r.Fence.MemberID++ }, func(r *replicaaction.Request) { r.Fence.StoreID[0]++ },
		func(r *replicaaction.Request) { r.Fence.NodeIncarnation++ }, func(r *replicaaction.Request) { r.Fence.AllocationGeneration++ },
	} {
		changed := request
		mutate(&changed)
		if err := cleanup(context.Background(), changed); !errors.Is(err, raftservice.ErrServingFence) {
			t.Fatalf("replacement identity accepted: %v", err)
		}
	}
	if donors.calls != 0 || serving.Load() != 2 {
		t.Fatal("foreign identity changed live inventories")
	}
	// Metadata may name a successor schema, while the physical source identity
	// still names the retired replica. No schema digest appears in this fence.
	schemas.groups[identity.Group].identity.RelationManifestDigest[0]++
	if err := cleanup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(schemas.groups) != 0 || donors.calls != 1 || serving.Load() != 1 {
		t.Fatalf("retirement cleanup groups=%d donors=%d serving=%d", len(schemas.groups), donors.calls, serving.Load())
	}
	if err := cleanup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if donors.calls != 1 || serving.Load() != 1 {
		t.Fatal("exact retirement replay changed another group count")
	}
	newer := identity
	newer.NodeIncarnation++
	schemas.groups[identity.Group] = &rf3SchemaGeneration{identity: newer}
	if err := cleanup(context.Background(), request); !errors.Is(err, raftservice.ErrServingFence) {
		t.Fatalf("stale replay removed replacement: %v", err)
	}
	if len(schemas.groups) != 1 || donors.calls != 1 {
		t.Fatal("replacement inventory disappeared")
	}
}

func TestRF3RetirementCleanupRetriesFailedDonorClose(t *testing.T) {
	identity := raftmember.RuntimeIdentity{Group: raftmember.GroupKey{GroupID: [16]byte{1}}, MemberID: 2, StoreID: [16]byte{3}, AllocationGeneration: 4, NodeIncarnation: 5}
	schemas := &rf3SchemaActivator{groups: map[raftmember.GroupKey]*rf3SchemaGeneration{identity.Group: {identity: identity}}}
	donors := &retirementDonorProbe{err: errors.New("export close failed")}
	var serving atomic.Int64
	serving.Store(1)
	cleanup := rf3ReplicaRetirementCleanup(schemas, donors, &serving)
	request := replicaaction.Request{Kind: replicaaction.SourceRetirement, Fence: raftservice.ServingFence{Group: identity.Group, MemberID: 2, StoreID: identity.StoreID, AllocationGeneration: 4, NodeIncarnation: 5}}
	if err := cleanup(context.Background(), request); !errors.Is(err, donors.err) {
		t.Fatal(err)
	}
	if len(schemas.groups) != 1 || serving.Load() != 1 {
		t.Fatal("failed cleanup lost retry metadata")
	}
	donors.err = nil
	if err := cleanup(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(schemas.groups) != 0 || serving.Load() != 0 {
		t.Fatal("cleanup retry did not withdraw source")
	}
}
