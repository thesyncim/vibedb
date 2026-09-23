package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicacontrol"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
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

func TestRF3RetirementAfterCompletedLearnerColdRecovery(t *testing.T) {
	certify := func(intent *gateway.GroupEnrollmentIntent) {
		intent.Receipt = &gateway.CertifiedEnrollmentReceipt{IntentID: intent.IntentID, IntentDigest: intent.Digest(),
			BaseCatalogGeneration: intent.CatalogGeneration, BaseCatalogHeadDigest: intent.ExpectedCatalogHeadDigest,
			BaseDescriptorDigest: intent.ExpectedDescriptorDigest, PublicationPredecessorGeneration: intent.CatalogGeneration,
			PublicationPredecessorHeadDigest: intent.ExpectedCatalogHeadDigest, EnrolledCatalogGeneration: intent.CatalogGeneration + 1,
			EnrolledCatalogHeadDigest: replication.Digest{21}, EnrolledDescriptorDigest: replication.Digest{22}, Target: intent.Target,
			InitialReplicaSetVersion: intent.ExpectedCommand.ReplicaSetVersion, GrantDigest: replication.Digest{23}, TransitionID: gateway.EnrollmentTransitionDigest(*intent)}
		if !intent.Valid() {
			t.Fatal("invalid retirement enrollment fixture")
		}
	}
	intent := rf3RecoveryEnrollmentIntent()
	intent.State = gateway.EnrollmentComplete
	intent.MoveOperationID = [32]byte{24}
	certify(&intent)
	identity := raftmember.RuntimeIdentity{Group: intent.Group, MemberID: intent.Target.Member,
		StoreID: intent.Target.StoreID, AllocationGeneration: uint64(intent.AllocationGeneration),
		NodeIncarnation: intent.Target.NodeIncarnation}
	receivers, err := newRF3DynamicBootstrapRegistry(rafttransport.TrustDomain{
		ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation,
	}, func() time.Time { return time.Now().Add(time.Second) }, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Completed cold recovery opens the installed runtime without reactivating
	// its obsolete snapshot receiver. Retirement must not require that receiver.
	runtime := &rf3NodeRuntime{receivers: receivers}
	learner := &rf3DynamicLearnerFactory{runtime: runtime, services: map[raftmember.GroupKey]*rf3DynamicLearnerService{
		intent.Group: {descriptor: snapshottransfer.Descriptor{TargetMember: identity.MemberID, TargetStore: identity.StoreID},
			installer: &rf3DynamicLearnerInstaller{intent: intent, proof: *intent.Proof}},
	}}
	runtime.learner = learner
	survivor := identity
	survivor.Group.GroupID[0]++
	schemas := &rf3SchemaActivator{groups: map[raftmember.GroupKey]*rf3SchemaGeneration{
		identity.Group: {identity: identity}, survivor.Group: {identity: survivor},
	}}
	var serving atomic.Int64
	serving.Store(2)
	cleanup := rf3ReplicaRetirementCleanup(schemas, runtime, &serving)
	request := replicaaction.Request{Kind: replicaaction.SourceRetirement, Fence: raftservice.ServingFence{
		Group: identity.Group, MemberID: identity.MemberID, StoreID: identity.StoreID,
		AllocationGeneration: identity.AllocationGeneration, NodeIncarnation: identity.NodeIncarnation}}
	for i := 0; i < 2; i++ {
		if err := cleanup(t.Context(), request); err != nil {
			t.Fatalf("retire recovered learner attempt %d: %v", i, err)
		}
	}
	if len(learner.services) != 0 || serving.Load() != 1 {
		t.Fatalf("retired learner retained: services=%d serving=%d", len(learner.services), serving.Load())
	}
	directory, err := newRF3CapacitySourceDirectory(schemas, nil,
		func(context.Context) (replicacontrol.NodeCapacity, error) { return replicacontrol.NodeCapacity{}, nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	sources, err := directory.Sources(t.Context())
	if err != nil || len(sources) != 1 || sources[0].Identity() != survivor {
		t.Fatalf("capacity still includes the closed replica: sources=%v err=%v", sources, err)
	}
	// An absent reservation is harmless, but a successor's extant reservation
	// must never be removed by a replay of the predecessor's retirement.
	successor := intent
	successor.IntentID[0]++
	successor.State = gateway.EnrollmentEnrolled
	proof := rf3ReservationProof(successor)
	successor.Proof = &proof
	certify(&successor)
	if err := receivers.Activate(t.Context(), successor, proof); err != nil {
		t.Fatal(err)
	}
	if err := receivers.Remove(t.Context(), intent, *intent.Proof); !errors.Is(err, nodecontrol.ErrConflict) {
		t.Fatalf("predecessor removed successor reservation: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := receivers.Remove(t.Context(), successor, proof); err != nil {
			t.Fatalf("exact receiver removal retry %d: %v", i, err)
		}
	}
}
