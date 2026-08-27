package raftservice

import (
	"context"
	"errors"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecutionOwnersRouteGateReadRejectsUnpublishedGroup(t *testing.T) {
	group := peerServerTestGroup()
	request := RouteGateReadRequest{Fence: ServingFence{Group: group}, MinimumApplied: 1,
		Capability: serviceauthz.CapabilityDataWrite}
	var absent *ExecutionOwners
	for _, owners := range []*ExecutionOwners{absent, {}} {
		result, lease, err := owners.ReadRouteGate(t.Context(), request)
		if !errors.Is(err, ErrExecutionGroup) || lease != nil || result != (RouteGateReadResult{}) {
			t.Fatalf("unpublished group: %+v %v", result, err)
		}
	}
	owners := &ExecutionOwners{}
	ready := &atomic.Bool{}
	owners.byGroup.Store(&executionOwnerGroups{values: map[raftmember.GroupKey]executionOwnerRoute{
		group: {owner: &Owner{}, ready: ready},
	}})
	if _, lease, err := owners.ReadRouteGate(t.Context(), request); !errors.Is(err, ErrExecutionGroup) || lease != nil {
		t.Fatalf("unready group: %v", err)
	}
	ready.Store(true)
	request.Capability = serviceauthz.CapabilityDataRead
	if _, lease, err := owners.ReadRouteGate(t.Context(), request); !errors.Is(err, ErrRouteGateUnauthorized) || lease != nil {
		t.Fatalf("published group did not forward exact capability: %v", err)
	}
	request.Fence.Group.GroupID[0]++
	if _, lease, err := owners.ReadRouteGate(t.Context(), request); !errors.Is(err, ErrExecutionGroup) || lease != nil {
		t.Fatalf("foreign group: %v", err)
	}
}

func TestRouteGateReadCapabilityBeforeAdmission(t *testing.T) {
	owner := &Owner{}
	for _, capability := range []serviceauthz.Capability{0, serviceauthz.CapabilityDataRead, serviceauthz.CapabilityTopology, serviceauthz.CapabilityExecutionPin, serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite} {
		result, lease, err := owner.ReadRouteGate(t.Context(), RouteGateReadRequest{MinimumApplied: 1, Capability: capability})
		if !errors.Is(err, ErrRouteGateUnauthorized) || lease != nil || result != (RouteGateReadResult{}) {
			t.Fatalf("cap %x result %+v %v", capability, result, err)
		}
	}
}

type testRouteGateSource struct {
	result replicatedstate.RouteGateReadResult
	floor  uint64
}

func (s *testRouteGateSource) RouteGateRead(floor uint64) (replicatedstate.RouteGateReadResult, error) {
	s.floor = floor
	return s.result, nil
}
func TestRouteGateReadUsesQuorumFloorAndHoldsBoundedLease(t *testing.T) {
	serving := ServingFence{Group: peerServerTestGroup(), AllocationGeneration: 3, Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5, ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9, RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11}, MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 5}
	g := serving.Group
	source := &testRouteGateSource{result: replicatedstate.RouteGateReadResult{
		Fence:  replicatedstate.SnapshotFence{Binding: replicatedstate.Binding{ClusterID: g.ClusterID, ClusterIncarnation: g.ClusterIncarnation, TopologyRecoveryEpoch: g.TopologyRecoveryEpoch, ShardIncarnation: g.ShardIncarnation, GroupID: g.GroupID, AllocationGeneration: 3, ActivePolicyGeneration: 5, ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9, RoutingVersion: 10, RouteGeneration: 11}, RelationManifestDigest: [32]byte{4}, ReplicaSetVersion: 7, Applied: 19},
		Status: routegate.Status{Epoch: 17},
	}}
	charge, _ := pointReadResponseCharge(routegate.StatusBytes)
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{MaxIngressItems: 1, MaxIngressBytes: 16, MaxPendingReadItems: 1, MaxPendingReadBytes: charge}}
	authorize := func() {
		r := <-owner.ingress
		if r.kind != requestReadRouteGate {
			t.Errorf("kind %d", r.kind)
		}
		owner.release(r.bytes)
		r.reply <- ownerReply{read: readAuthorization{routeGate: source, minimumApplied: 19}}
	}
	go authorize()
	request := RouteGateReadRequest{Fence: serving, Capability: serviceauthz.CapabilityDataWrite, MinimumApplied: 7}
	result, lease, err := owner.ReadRouteGate(t.Context(), request)
	if err != nil || lease == nil || result.Applied != 19 || result.Status.Epoch != 17 || source.floor != 19 {
		t.Fatalf("result %+v floor %d err %v", result, source.floor, err)
	}
	if _, next, err := owner.ReadRouteGate(t.Context(), request); !errors.Is(err, ErrPendingReadsFull) || next != nil {
		t.Fatalf("budget %v", err)
	}
	lease.Release()
	source.result.Fence.Binding.SchemaGeneration++
	go authorize()
	if result, lease, err := owner.ReadRouteGate(t.Context(), request); !errors.Is(err, ErrServingFence) || lease != nil || result != (RouteGateReadResult{}) {
		t.Fatalf("foreign fence %+v %v", result, err)
	}
	if owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 {
		t.Fatal("leaked budget")
	}
}
func TestRF3RouteGateReadRejectsFollowerAndIsolatedLeader(t *testing.T) {
	cluster := newTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := cluster.owners[0].Campaign(ctx, cluster.group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.group)
	state := mustRF3State(t, ctx, cluster.owners[leader], cluster.group)
	request := RouteGateReadRequest{Fence: state.Fence(), Capability: serviceauthz.CapabilityDataWrite, MinimumApplied: max(uint64(1), state.Status.Applied)}
	result, lease, err := cluster.owners[leader].ReadRouteGate(ctx, request)
	if err != nil || lease == nil || result.Applied < request.MinimumApplied || result.Status.Epoch != 1 {
		t.Fatalf("leader %+v %v", result, err)
	}
	lease.Release()
	follower := (leader + 1) % len(cluster.owners)
	request.Fence = mustRF3State(t, ctx, cluster.owners[follower], cluster.group).Fence()
	if result, lease, err := cluster.owners[follower].ReadRouteGate(ctx, request); !errors.Is(err, raftmodel.ErrNotLeader) || lease != nil || result != (RouteGateReadResult{}) {
		t.Fatalf("follower %+v %v", result, err)
	}
	for i := range cluster.owners {
		if i != leader {
			cluster.stop(t, i)
		}
	}
	request.Fence = mustRF3State(t, ctx, cluster.owners[leader], cluster.group).Fence()
	isolated, cancelRead := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelRead()
	if result, lease, err := cluster.owners[leader].ReadRouteGate(isolated, request); !errors.Is(err, context.DeadlineExceeded) || lease != nil || result != (RouteGateReadResult{}) {
		t.Fatalf("isolated %+v %v", result, err)
	}
}
