package raftservice

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestExecutionOwnersPointBatchRejectsUnpublishedGroup(t *testing.T) {
	group := peerServerTestGroup()
	request := PointReadBatchRequest{Fence: ServingFence{Group: group}}
	ready := new(atomic.Bool)
	owners := &ExecutionOwners{}
	owners.byGroup.Store(&executionOwnerGroups{values: map[raftmember.GroupKey]executionOwnerRoute{
		group: {owner: &Owner{}, ready: ready},
	}})
	for _, candidate := range []*ExecutionOwners{nil, {}, owners} {
		result, lease, err := candidate.ReadPointBatch(t.Context(), request)
		if !errors.Is(err, ErrExecutionGroup) || lease != nil || result.Applied != 0 || result.Data != nil {
			t.Fatalf("unpublished group result=%+v lease=%T err=%v", result, lease, err)
		}
	}
	ready.Store(true)
	if _, lease, err := owners.ReadPointBatch(t.Context(), request); !errors.Is(err, ErrInvalidOwner) || lease != nil {
		t.Fatalf("published owner did not validate exact request: %v", err)
	}
	request.Fence.Group.GroupID[0]++
	if _, lease, err := owners.ReadPointBatch(t.Context(), request); !errors.Is(err, ErrExecutionGroup) || lease != nil {
		t.Fatalf("foreign group forwarded: %v", err)
	}
}

func TestExecutionOwnersPointBatchPreservesReadIndexAndLease(t *testing.T) {
	group := peerServerTestGroup()
	serving := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 5}
	packed, err := replicatedstate.AppendPointReadBatch(nil, []replicatedstate.PointRead{
		{Relation: 1, Key: []byte("a")}, {Relation: 2, Key: []byte("b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	value := []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	source := &ownerTestReadSource{batchResult: replicatedstate.PointReadBatchResult{
		Data: value, Fence: replicatedstate.SnapshotFence{Binding: replicatedstate.Binding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, AllocationGeneration: 3,
			ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
			ActivePolicyGeneration: 5, ProtectionEpoch: 6, OwnershipEpoch: 8,
			SchemaGeneration: 9, RoutingVersion: 10, RouteGeneration: 11,
		}, RelationManifestDigest: [32]byte{4}, ReplicaSetVersion: 7, Applied: 19},
	}}
	charge, ok := pointReadResponseCharge(4096)
	if !ok {
		t.Fatal("charge")
	}
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: int64(len(packed)),
		MaxPendingReadItems: 1, MaxPendingReadBytes: charge,
	}}
	owners := &ExecutionOwners{}
	owners.byGroup.Store(&executionOwnerGroups{values: map[raftmember.GroupKey]executionOwnerRoute{group: {owner: owner}}})
	authorize := func() {
		request := <-owner.ingress
		if request.kind != requestReadLinear || request.group != group || request.read.fence != serving || request.read.minimumApplied != 7 {
			t.Errorf("wrong authorization request: %+v", request)
		}
		owner.release(request.bytes)
		request.reply <- ownerReply{read: readAuthorization{source: source, minimumApplied: 19}}
	}
	go authorize()
	request := PointReadBatchRequest{Fence: serving, Packed: packed, MinimumApplied: 7, MaxResultBytes: 4096}
	result, lease, err := owners.ReadPointBatch(t.Context(), request)
	if err != nil || lease == nil || result.Applied != 19 || !bytes.Equal(result.Data, value) || source.batchCalls != 1 {
		t.Fatalf("result=%+v lease=%T calls=%d err=%v", result, lease, source.batchCalls, err)
	}
	if _, next, err := owners.ReadPointBatch(t.Context(), request); !errors.Is(err, ErrPendingReadsFull) || next != nil {
		t.Fatalf("retained result did not retain response budget: %v", err)
	}
	lease.Release()
	source.batchResult.Fence.Binding.SchemaGeneration++
	go authorize()
	if result, lease, err := owners.ReadPointBatch(t.Context(), request); !errors.Is(err, ErrServingFence) || lease != nil || result.Data != nil {
		t.Fatalf("wrong generation escaped: %+v/%v", result, err)
	}
	if owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 {
		t.Fatal("failed generation read leaked response reservation")
	}
}
