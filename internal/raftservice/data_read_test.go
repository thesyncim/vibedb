package raftservice

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// Invoked by the real authenticated three-voter test after a committed write;
// this deliberately uses the production SQL apply source, not a fake barrier.
func testRF3DataReadCut(t *testing.T, ctx context.Context, owner *Owner,
	fence ServingFence, minimum uint64, key, want []byte,
) {
	t.Helper()
	request := LinearizableDataReadRequest{
		Fence: fence, Capability: serviceauthz.CapabilityDataRead,
		Relations: []replication.RelationID{1},
	}
	var cut LinearizableDataReadCut
	defer cut.Close()
	for range 2 {
		if err := owner.ReadLinearizableDataInto(ctx, request, &cut); err != nil {
			t.Fatalf("quorum data cut: %v", err)
		}
		data := cut.Data()
		if data == nil || data.Fence().Applied < minimum || !data.OwnsKey(1, key) {
			t.Fatalf("quorum data cut did not retain the committed ownership/floor")
		}
		snapshot, ok := data.Relation(1)
		if !ok {
			t.Fatal("admitted relation missing")
		}
		got, found, err := snapshot.AppendRaw(nil, key)
		if err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("quorum data row=%q found=%v err=%v", got, found, err)
		}
		if err := owner.ReadLinearizableDataInto(ctx, request, &cut); !errors.Is(err, replicatedstate.ErrDataReadOpen) {
			t.Fatalf("live-cut overwrite=%v", err)
		}
		if err := cut.Close(); err != nil || cut.Data() != nil {
			t.Fatalf("data cut close=%v", err)
		}
		if err := cut.Close(); err != nil {
			t.Fatalf("second data cut close=%v", err)
		}
	}
	for _, capability := range []serviceauthz.Capability{
		0, serviceauthz.CapabilityBackup, serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityDataRead | serviceauthz.CapabilityBackup,
	} {
		denied := request
		denied.Capability = capability
		if err := owner.ReadLinearizableDataInto(ctx, denied, &cut); !errors.Is(err, ErrInvalidOwner) || cut.Data() != nil {
			t.Fatalf("capability %v data cut=%v", capability, err)
		}
	}
	stale := request
	stale.Fence.Command.RelationManifestDigest[0] ^= 1
	if err := owner.ReadLinearizableDataInto(ctx, stale, &cut); !errors.Is(err, ErrServingFence) || cut.Data() != nil {
		t.Fatalf("stale data cut=%v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := owner.ReadLinearizableDataInto(canceled, request, &cut); !errors.Is(err, context.Canceled) || cut.Data() != nil {
		t.Fatalf("canceled data cut=%v", err)
	}
}

type linearizablePointCutTestSource struct {
	result        replicatedstate.PointReadResult
	err           error
	calls         int
	relation      replication.RelationID
	key           []byte
	minimum       uint64
	maxValueBytes int
	dst           []byte
}

func (source *linearizablePointCutTestSource) PointReadInto(
	relation replication.RelationID, key []byte, minimumApplied uint64,
	maxValueBytes int, dst []byte,
) (replicatedstate.PointReadResult, error) {
	source.calls++
	source.relation = relation
	source.key = append(source.key[:0], key...)
	source.minimum = minimumApplied
	source.maxValueBytes = maxValueBytes
	source.dst = dst
	return source.result, source.err
}

func testLinearizablePointServingFence(group raftmember.GroupKey) ServingFence {
	return ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 5}
}

func testLinearizablePointSnapshotFence(serving ServingFence, applied uint64) replicatedstate.SnapshotFence {
	return replicatedstate.SnapshotFence{Binding: replicatedstate.Binding{
		ClusterID: serving.Group.ClusterID, ClusterIncarnation: serving.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: serving.Group.TopologyRecoveryEpoch,
		AllocationGeneration:  serving.AllocationGeneration,
		ShardIncarnation:      serving.Group.ShardIncarnation, GroupID: serving.Group.GroupID,
		ActivePolicyGeneration: serving.Command.ActivePolicyGeneration,
		ProtectionEpoch:        serving.Command.ProtectionEpoch,
		OwnershipEpoch:         serving.Command.OwnershipEpoch,
		SchemaGeneration:       serving.Command.SchemaGeneration,
		RoutingVersion:         serving.Command.RoutingVersion,
		RouteGeneration:        serving.Command.RouteGeneration,
	}, RelationManifestDigest: serving.Command.RelationManifestDigest,
		ReplicaSetVersion: serving.Command.ReplicaSetVersion, Applied: applied}
}

func TestLinearizablePointReadCutUsesOneLeaderBarrierAndPinsGeneration(t *testing.T) {
	group := peerServerTestGroup()
	serving := testLinearizablePointServingFence(group)
	const minimumApplied = 19
	source := &linearizablePointCutTestSource{result: replicatedstate.PointReadResult{
		Fence: testLinearizablePointSnapshotFence(serving, minimumApplied), Found: true,
		Value: []byte("value"),
	}}
	generation := &ownerGeneration{}
	generation.pins.Store(1)
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: 1,
		MaxPendingReadItems: 1, MaxPendingReadBytes: 1,
	}}
	requestSeen := make(chan ownerRequest, 1)
	go func() {
		request := <-owner.ingress
		requestSeen <- request
		owner.release(request.bytes)
		request.reply <- ownerReply{read: readAuthorization{
			source: source, minimumApplied: minimumApplied, generation: generation,
		}}
	}()

	var cut LinearizablePointReadCut
	err := owner.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: serving, Capability: serviceauthz.CapabilityDataRead,
	}, &cut)
	if err != nil {
		t.Fatalf("linearizable point admission: %v", err)
	}
	request := <-requestSeen
	if request.kind != requestReadLinear || request.group != group ||
		request.read.fence != serving || request.read.delivery == nil {
		t.Fatalf("request=%+v", request)
	}
	if cut.Source() != source {
		t.Fatal("cut did not retain the authenticated live source")
	}
	if owner.pendingReadItems != 1 || owner.pendingReadBytes != 1 || generation.pins.Load() != 1 {
		t.Fatalf("retained cut accounting items=%d bytes=%d pins=%d",
			owner.pendingReadItems, owner.pendingReadBytes, generation.pins.Load())
	}

	result, readErr := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, []byte("prefix"))
	if readErr != nil || !result.Found || string(result.Value) != "value" {
		t.Fatalf("result=%+v err=%v", result, readErr)
	}
	if source.calls != 1 || source.relation != 1 || string(source.key) != "key" ||
		source.minimum != minimumApplied || source.maxValueBytes != 64 || string(source.dst) != "prefix" {
		t.Fatalf("source call=%+v", source)
	}

	if err := cut.Close(); err != nil || cut.Source() != nil || generation.pins.Load() != 0 ||
		owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 {
		t.Fatalf("close err=%v source=%v pins=%d pending=%d/%d", err, cut.Source(),
			generation.pins.Load(), owner.pendingReadItems, owner.pendingReadBytes)
	}
	if err := cut.Close(); err != nil {
		t.Fatalf("idempotent close: %v", err)
	}
	if _, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil); !errors.Is(err, ErrServingFence) {
		t.Fatalf("closed point read error=%v", err)
	}
}

func TestLinearizablePointReadCutVerifiesLiveResultAndPropagatesIntent(t *testing.T) {
	group := peerServerTestGroup()
	serving := testLinearizablePointServingFence(group)
	valid := testLinearizablePointSnapshotFence(serving, 19)
	tests := []struct {
		name   string
		result replicatedstate.PointReadResult
		err    error
		want   error
	}{
		{name: "ownership", err: replicatedstate.ErrWrongBinding, want: replicatedstate.ErrWrongBinding},
		{name: "intent", err: replicatedstate.ErrTransactionIntentActive, want: replicatedstate.ErrTransactionIntentActive},
		{name: "fence", result: replicatedstate.PointReadResult{
			Fence: func() replicatedstate.SnapshotFence {
				fence := valid
				fence.RelationManifestDigest[0]++
				return fence
			}(), Value: []byte("value")}, want: ErrServingFence},
		{name: "applied floor", result: replicatedstate.PointReadResult{
			Fence: testLinearizablePointSnapshotFence(serving, 18), Value: []byte("value")}, want: ErrServingFence},
		{name: "value bound", result: replicatedstate.PointReadResult{
			Fence: valid, Value: []byte("too-large")}, want: ErrServingFence},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &linearizablePointCutTestSource{result: test.result, err: test.err}
			cut := &LinearizablePointReadCut{source: source, fence: serving,
				minimumApplied: 19, owner: &Owner{}}
			result, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 4, nil)
			if !errors.Is(err, test.want) || result.Found || len(result.Value) != 0 ||
				result.Fence != (replicatedstate.SnapshotFence{}) {
				t.Fatalf("result=%+v err=%v want=%v", result, err, test.want)
			}
			if source.calls != 1 || source.minimum != 19 {
				t.Fatalf("source calls=%d minimum=%d", source.calls, source.minimum)
			}
		})
	}
}

func TestExecutionOwnersForwardLinearizablePointReadAndCancellation(t *testing.T) {
	group := peerServerTestGroup()
	serving := testLinearizablePointServingFence(group)
	source := &linearizablePointCutTestSource{result: replicatedstate.PointReadResult{
		Fence: testLinearizablePointSnapshotFence(serving, 23), Value: []byte("value"),
	}}
	generation := &ownerGeneration{}
	generation.pins.Store(1)
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: 1,
		MaxPendingReadItems: 1, MaxPendingReadBytes: 1,
	}}
	owners := &ExecutionOwners{}
	owners.byGroup.Store(&executionOwnerGroups{values: map[raftmember.GroupKey]executionOwnerRoute{
		group: {owner: owner},
	}})
	go func() {
		request := <-owner.ingress
		owner.release(request.bytes)
		request.reply <- ownerReply{read: readAuthorization{
			source: source, minimumApplied: 23, generation: generation,
		}}
	}()
	var cut LinearizablePointReadCut
	if err := owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: serving, Capability: serviceauthz.CapabilityDataRead,
	}, &cut); err != nil {
		t.Fatalf("forwarded point admission: %v", err)
	}
	result, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 32, nil)
	if err != nil || string(result.Value) != "value" || source.calls != 1 {
		t.Fatalf("result=%+v calls=%d err=%v", result, source.calls, err)
	}
	cut.Close()

	cancelCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	gotRequest := make(chan struct{})
	hold := make(chan struct{})
	go func() {
		request := <-owner.ingress
		owner.release(request.bytes)
		close(gotRequest)
		<-hold
	}()
	var canceled LinearizablePointReadCut
	resultErr := make(chan error, 1)
	go func() {
		resultErr <- owners.ReadLinearizablePointInto(cancelCtx, LinearizablePointReadRequest{
			Fence: serving, Capability: serviceauthz.CapabilityDataRead,
		}, &canceled)
	}()
	<-gotRequest
	cancel()
	if err := <-resultErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v", err)
	}
	close(hold)
	if owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 || canceled.Source() != nil {
		t.Fatalf("canceled accounting pending=%d/%d source=%v", owner.pendingReadItems,
			owner.pendingReadBytes, canceled.Source())
	}
}
