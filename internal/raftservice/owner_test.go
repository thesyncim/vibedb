package raftservice

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
)

type schemaQuiesceInboundHost struct {
	ownerHost
	adopted int
}

func (host *schemaQuiesceInboundHost) AdoptMessage(raftmember.GroupKey, *pb.Message) error {
	host.adopted++
	return nil
}

func TestOwnerDropsTransferredPeerTrafficOnlyWhileSchemaGenerationQuiesces(t *testing.T) {
	group := peerServerTestGroup()
	host := &schemaQuiesceInboundHost{}
	generation := &ownerGeneration{}
	owner := &Owner{host: host, members: map[raftmember.GroupKey]ownerMember{
		group: {generation: generation},
	}}
	deliver := func() error {
		reply := make(chan ownerReply, 1)
		err := owner.handle(ownerRequest{kind: requestInbound, group: group,
			inbound: rafttransport.Inbound{Group: group, Message: &pb.Message{}}, reply: reply})
		if got := (<-reply).err; !errors.Is(got, err) {
			t.Fatalf("reply=%v handle=%v", got, err)
		}
		return err
	}
	if err := deliver(); err != nil || host.adopted != 1 {
		t.Fatalf("active inbound err=%v adopted=%d", err, host.adopted)
	}
	generation.quiescing.Store(true)
	if err := deliver(); err != nil || host.adopted != 1 {
		t.Fatalf("quiescing inbound err=%v adopted=%d", err, host.adopted)
	}
}

func newDeliveryTestOwner() *Owner {
	return &Owner{
		limits:  Limits{MaxIngressItems: 1, MaxIngressBytes: 1},
		ingress: make(chan ownerRequest, 1), started: true,
	}
}

func TestOwnerGenerationPinsFenceQuiesceRace(t *testing.T) {
	generation := &ownerGeneration{}
	if !generation.acquire() || generation.pins.Load() != 1 {
		t.Fatal("first generation pin refused")
	}
	if generation.quiesce() {
		t.Fatal("generation quiesced with a live pin")
	}
	generation.quiescing.Store(true)
	if generation.acquire() || generation.pins.Load() != 1 {
		t.Fatal("quiescing generation admitted a new pin")
	}
	generation.release()
	if generation.pins.Load() != 0 {
		t.Fatalf("pins after release=%d", generation.pins.Load())
	}
	generation.resume()
	if !generation.quiesce() || generation.pins.Load() != 0 {
		t.Fatal("drained generation did not quiesce")
	}
}

func TestOwnerProposalDeliveryCancellationWinsBeforeOwnerHandoff(t *testing.T) {
	owner := newDeliveryTestOwner()
	ctx, cancel := context.WithCancel(context.Background())
	delivery := &proposalDelivery{}
	reply := make(chan ownerReply, 1)
	done := make(chan error, 1)
	go func() {
		_, err := owner.enqueue(ctx, ownerRequest{
			reply: reply, bytes: 1, delivery: delivery,
		})
		done <- err
	}()
	request := <-owner.ingress
	cancel()
	if err := <-done; !errors.Is(err, ErrOutcomeUnknown) ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("abandoned delivery = %v", err)
	}
	if delivery.state.Load() != proposalDeliveryAbandoned {
		t.Fatalf("delivery state = %d", delivery.state.Load())
	}
	// This is the exact CAS performed by handle after Registry.Enqueue. Losing
	// it obligates handle to cancel the newly-created waiter rather than publish
	// it to a caller that already returned.
	if delivery.state.CompareAndSwap(proposalDeliveryPending, proposalDeliveryReady) {
		t.Fatal("owner acquired an abandoned delivery")
	}
	owner.release(request.bytes)
}

func TestSchemaTransitionFenceAuthenticatesExactServingGeneration(t *testing.T) {
	group := peerServerTestGroup()
	fence := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 5}
	transition := replicatedstate.SchemaTransition{
		From: replicatedstate.Binding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			Distribution:          "docs", Shard: "0000-ffff",
			AllocationGeneration: fence.AllocationGeneration,
			ShardIncarnation:     group.ShardIncarnation, GroupID: group.GroupID,
			ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
			ProtectionEpoch:        fence.Command.ProtectionEpoch,
			OwnershipEpoch:         fence.Command.OwnershipEpoch,
			SchemaGeneration:       fence.Command.SchemaGeneration,
			RoutingVersion:         fence.Command.RoutingVersion,
			RouteGeneration:        fence.Command.RouteGeneration,
			OwnedRange:             distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
		ToSchemaGeneration: 10, ExpectedReplicaSetVersion: 7, MembershipSequence: 1,
		MembershipSource: [32]byte{1}, MembershipTarget: [32]byte{2},
		FromManifest: [32]byte{4}, FromApplyContract: [32]byte{5},
		ToManifest: [32]byte{6}, ToApplyContract: [32]byte{7},
		RequestDigest: [32]byte{8}, AuthorizationDigest: [32]byte{9},
		CatalogCASDigest: [32]byte{10},
	}
	encoded, err := replicatedstate.AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := replicatedstate.OpenSchemaTransition(encoded)
	if err != nil || !schemaTransitionMatchesFence(opened, fence) {
		t.Fatalf("exact transition rejected: %v", err)
	}
	transition.FromManifest[0]++
	encoded, err = replicatedstate.AppendSchemaTransition(encoded[:0], transition)
	if err != nil {
		t.Fatal(err)
	}
	opened, err = replicatedstate.OpenSchemaTransition(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if schemaTransitionMatchesFence(opened, fence) {
		t.Fatal("substituted source manifest accepted")
	}
}

func TestCommittedSchemaFenceSettlesAlreadyActivatedTarget(t *testing.T) {
	group := peerServerTestGroup()
	command := CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
		ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 10,
		RelationManifestDigest: [32]byte{6}, RoutingVersion: 10, RouteGeneration: 11}
	transition := replicatedstate.SchemaTransition{
		From: replicatedstate.Binding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			Distribution:          "docs", Shard: "0000-ffff", AllocationGeneration: 3,
			ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
			ActivePolicyGeneration: command.ActivePolicyGeneration,
			ProtectionEpoch:        command.ProtectionEpoch, OwnershipEpoch: command.OwnershipEpoch,
			SchemaGeneration: 9, RoutingVersion: command.RoutingVersion,
			RouteGeneration: command.RouteGeneration,
			OwnedRange:      distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
		ToSchemaGeneration: 10, ExpectedReplicaSetVersion: command.ReplicaSetVersion,
		MembershipSequence: 1, MembershipSource: [32]byte{1}, MembershipTarget: [32]byte{2},
		FromManifest: [32]byte{4}, FromApplyContract: [32]byte{5},
		ToManifest: command.RelationManifestDigest, ToApplyContract: [32]byte{7},
		RequestDigest: [32]byte{8}, AuthorizationDigest: [32]byte{9}, CatalogCASDigest: [32]byte{10},
	}
	encoded, err := replicatedstate.AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	owner := &Owner{members: map[raftmember.GroupKey]ownerMember{
		group: {identity: raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 3},
			command: command, generation: &ownerGeneration{}},
	}}
	if err = owner.fenceCommittedSchemaGeneration(ownerRequest{group: group, data: encoded}); err != nil {
		t.Fatalf("already activated target did not settle exact commit replay: %v", err)
	}
}

func TestProposeSchemaTransitionDetachesBoundedCommand(t *testing.T) {
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: replicatedstate.MaxSchemaTransitionBytes,
	}}
	command := []byte{1, 2, 3}
	done := make(chan error, 1)
	go func() {
		done <- owner.ProposeSchemaTransition(context.Background(), ServingFence{}, command)
	}()
	request := <-owner.ingress
	command[0] = 9
	if request.kind != requestSchemaTransition || request.data[0] != 1 ||
		cap(request.data) != len(request.data) {
		t.Fatalf("request kind=%d data=%v len/cap=%d/%d",
			request.kind, request.data, len(request.data), cap(request.data))
	}
	request.reply <- ownerReply{}
	owner.release(request.bytes)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := owner.ProposeSchemaTransition(context.Background(), ServingFence{},
		make([]byte, replicatedstate.MaxSchemaTransitionBytes+1)); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("oversized command error=%v", err)
	}
}

func TestObserveSchemaTransitionDetachesBoundedCommand(t *testing.T) {
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: replicatedstate.MaxSchemaTransitionBytes,
	}}
	group := peerServerTestGroup()
	command := []byte{1, 2, 3}
	type result struct {
		committed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		committed, err := owner.ObserveSchemaTransition(context.Background(), group, command)
		done <- result{committed: committed, err: err}
	}()
	request := <-owner.ingress
	command[0] = 9
	if request.kind != requestObserveSchemaTransition || request.group != group ||
		request.data[0] != 1 || cap(request.data) != len(request.data) {
		t.Fatalf("request kind=%d group=%+v data=%v len/cap=%d/%d",
			request.kind, request.group, request.data, len(request.data), cap(request.data))
	}
	request.reply <- ownerReply{committed: true}
	owner.release(request.bytes)
	got := <-done
	if got.err != nil || !got.committed {
		t.Fatalf("observation committed=%t err=%v", got.committed, got.err)
	}
}

func TestOwnerReadOutcomeSettlesExactFixedContextAndCancellationCleansUp(t *testing.T) {
	owner := &Owner{pendingReads: make(map[[16]byte]*readDelivery)}
	var contextKey [16]byte
	contextKey[0], contextKey[15] = 3, 9
	source := &ownerTestReadSource{}
	delivery := &readDelivery{reply: make(chan ownerReply, 1), source: source,
		minimumApplied: 23, generation: &ownerGeneration{}}
	owner.pendingReads[contextKey] = delivery
	owner.finishReadOutcomes([]raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
		Context: contextKey[:], Index: 17,
	}}})
	select {
	case reply := <-delivery.reply:
		if reply.err != nil || reply.read.minimumApplied != 23 || reply.read.source != source {
			t.Fatalf("reply=%+v", reply)
		}
	default:
		t.Fatal("read outcome was discarded")
	}
	if len(owner.pendingReads) != 0 {
		t.Fatal("settled read retained")
	}

	barrierDelivery := &readDelivery{reply: make(chan ownerReply, 1), source: source,
		minimumApplied: 11, generation: &ownerGeneration{}}
	owner.pendingReads[contextKey] = barrierDelivery
	owner.finishReadOutcomes([]raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
		Context: contextKey[:], Index: 19,
	}}})
	if reply := <-barrierDelivery.reply; reply.err != nil ||
		reply.read.minimumApplied != 19 || reply.read.source != source {
		t.Fatalf("barrier-ahead reply=%+v", reply)
	}

	abandoned := &readDelivery{reply: make(chan ownerReply, 1)}
	abandoned.state.Store(readDeliveryAbandoned)
	owner.pendingReads[contextKey] = abandoned
	owner.finishReadOutcomes([]raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
		Context: contextKey[:], Index: 18,
	}}})
	if len(owner.pendingReads) != 0 || len(abandoned.reply) != 0 {
		t.Fatal("canceled read was retained or redelivered")
	}
}

func TestReadIngressLaneDoesNotClassifyControlAsRead(t *testing.T) {
	for _, kind := range []requestKind{requestReadLinear, requestReadFollower, requestReadTransaction,
		requestReadRequestLedger, requestReadExecutionPin, requestReadRouteGate} {
		if !readIngressCandidate(ownerRequest{kind: kind}) {
			t.Fatalf("read kind %d was not classified", kind)
		}
	}
	for _, kind := range []requestKind{requestProposal, requestInbound, requestStatus, requestMembership,
		requestSchemaTransition, requestReplicaRetirement} {
		if readIngressCandidate(ownerRequest{kind: kind}) {
			t.Fatalf("control kind %d entered read lane", kind)
		}
	}
}

type ownerTestReadSource struct {
	result      replicatedstate.PointReadResult
	batchResult replicatedstate.PointReadBatchResult
	batchCalls  int
	err         error
}

func (source *ownerTestReadSource) PointReadBatchInto(
	_ []byte, _ uint64, _ int, _ []byte,
) (replicatedstate.PointReadBatchResult, error) {
	source.batchCalls++
	return source.batchResult, source.err
}

func TestOwnerPointReadBatchUsesOneLinearAuthorization(t *testing.T) {
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
	fence := replicatedstate.SnapshotFence{Binding: replicatedstate.Binding{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		AllocationGeneration:  serving.AllocationGeneration,
		ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
		ActivePolicyGeneration: serving.Command.ActivePolicyGeneration,
		ProtectionEpoch:        serving.Command.ProtectionEpoch,
		OwnershipEpoch:         serving.Command.OwnershipEpoch,
		SchemaGeneration:       serving.Command.SchemaGeneration,
		RoutingVersion:         serving.Command.RoutingVersion,
		RouteGeneration:        serving.Command.RouteGeneration,
	}, RelationManifestDigest: serving.Command.RelationManifestDigest,
		ReplicaSetVersion: serving.Command.ReplicaSetVersion, Applied: 19}
	source := &ownerTestReadSource{batchResult: replicatedstate.PointReadBatchResult{
		Fence: fence, Data: value,
	}}
	charge, ok := pointReadResponseCharge(4096)
	if !ok {
		t.Fatal("charge")
	}
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: int64(len(packed)),
		MaxPendingReadItems: 1, MaxPendingReadBytes: charge,
	}}
	authorizations := 0
	go func() {
		request := <-owner.ingress
		authorizations++
		if request.kind != requestReadLinear {
			t.Errorf("request kind=%d", request.kind)
		}
		request.reply <- ownerReply{read: readAuthorization{
			source: source, minimumApplied: 19,
		}}
		owner.release(request.bytes)
	}()
	result, lease, err := owner.ReadPointBatch(context.Background(), PointReadBatchRequest{
		Fence: serving, Packed: packed, MinimumApplied: 7, MaxResultBytes: 4096,
	})
	if err != nil || result.Applied != 19 || len(result.Data) != len(value) ||
		authorizations != 1 || source.batchCalls != 1 || lease == nil {
		t.Fatalf("result=%+v authorizations=%d sourceCalls=%d lease=%T err=%v",
			result, authorizations, source.batchCalls, lease, err)
	}
	lease.Release()
}

func (source *ownerTestReadSource) PointReadInto(
	replication.RelationID, []byte, uint64, int, []byte,
) (replicatedstate.PointReadResult, error) {
	return source.result, source.err
}

func TestPointReadResponseBudgetSaturatesAcrossLiveGrowthBoundaryLeases(t *testing.T) {
	const (
		maximum       = 3_366_913
		allocatorSlop = (8 << 10) - 1
	)
	charge, ok := pointReadResponseCharge(maximum)
	if !ok {
		t.Fatal("response charge rejected")
	}
	group := peerServerTestGroup()
	serving := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 5}
	source := &ownerTestReadSource{result: replicatedstate.PointReadResult{
		Fence: replicatedstate.SnapshotFence{Binding: replicatedstate.Binding{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			AllocationGeneration:  serving.AllocationGeneration,
			ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
			ActivePolicyGeneration: serving.Command.ActivePolicyGeneration,
			ProtectionEpoch:        serving.Command.ProtectionEpoch,
			OwnershipEpoch:         serving.Command.OwnershipEpoch,
			SchemaGeneration:       serving.Command.SchemaGeneration,
			RoutingVersion:         serving.Command.RoutingVersion,
			RouteGeneration:        serving.Command.RouteGeneration,
		}, RelationManifestDigest: serving.Command.RelationManifestDigest,
			ReplicaSetVersion: serving.Command.ReplicaSetVersion, Applied: 9},
		Found: true, Value: make([]byte, maximum),
	}}
	owner := &Owner{started: true, ingress: make(chan ownerRequest, 1), limits: Limits{
		MaxIngressItems: 1, MaxIngressBytes: 16,
		MaxPendingReadItems: 2, MaxPendingReadBytes: charge,
	}}
	authorize := func() {
		request := <-owner.ingress
		request.reply <- ownerReply{read: readAuthorization{
			source: source, minimumApplied: request.read.minimumApplied,
		}}
		owner.release(request.bytes)
	}
	go authorize()
	request := PointReadRequest{Fence: serving, Relation: 1, Key: []byte("k"),
		MinimumApplied: 9, MaxValueBytes: maximum}
	result, lease, err := owner.ReadPoint(context.Background(), request)
	if err != nil || !result.Found || len(result.Value) != maximum || lease == nil {
		t.Fatalf("first result found=%t bytes=%d lease=%T err=%v",
			result.Found, len(result.Value), lease, err)
	}
	if cap(result.Value) != maximum {
		t.Fatalf("first result capacity=%d want exact=%d", cap(result.Value), maximum)
	}
	retainedBound := int64(cap(result.Value)) + allocatorSlop + int64(maximum) +
		pointReadEncodedFrameFixedBytes + allocatorSlop
	if retainedBound != charge {
		t.Fatalf("result plus rounded frame bound=%d charge=%d", retainedBound, charge)
	}
	if _, secondLease, err := owner.ReadPoint(context.Background(), request); !errors.Is(err, ErrPendingReadsFull) || secondLease != nil {
		t.Fatalf("concurrent lease=%T err=%v", secondLease, err)
	}
	lease.Release()
	go authorize()
	if _, nextLease, err := owner.ReadPoint(context.Background(), request); err != nil {
		t.Fatalf("read after release=%v", err)
	} else {
		nextLease.Release()
	}
	if owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 {
		t.Fatalf("pending reads=%d bytes=%d", owner.pendingReadItems, owner.pendingReadBytes)
	}
}

func TestPointReadLeaseKeepsConcurrentResponseBudgetCharged(t *testing.T) {
	charge, ok := pointReadResponseCharge(4 << 20)
	if !ok || charge <= 2*(4<<20)+8_211 {
		t.Fatalf("resident response charge=%d ok=%t", charge, ok)
	}
	owner := &Owner{started: true, limits: Limits{
		MaxPendingReadItems: 1, MaxPendingReadBytes: charge,
	}}
	if err := owner.reservePendingRead(charge); err != nil {
		t.Fatal(err)
	}
	lease := &pointReadLease{owner: owner, bytes: charge}
	if err := owner.reservePendingRead(1); !errors.Is(err, ErrPendingReadsFull) {
		t.Fatalf("concurrent reservation error=%v", err)
	}
	lease.Release()
	lease.Release()
	if err := owner.reservePendingRead(charge); err != nil {
		t.Fatalf("reservation after release=%v", err)
	}
	owner.releasePendingRead(charge)
	if owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 {
		t.Fatalf("pending reads=%d bytes=%d", owner.pendingReadItems, owner.pendingReadBytes)
	}
}

func TestTransactionRecoveryOwnerRequiresExactDedicatedCapability(t *testing.T) {
	request := TransactionReadRequest{Read: replicatedstate.TransactionRecoveryReadRequest{
		Kind: replicatedstate.TransactionRecoveryLookupTarget,
		ID:   distributedtxn.ID{1}, MinimumApplied: 1, MaxRows: 1,
		MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
	}}
	owner := &Owner{}
	for _, capability := range []serviceauthz.Capability{
		serviceauthz.CapabilityDataRead,
		serviceauthz.CapabilityDataWrite,
		serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityDataRead | serviceauthz.CapabilityTransactionRecovery,
	} {
		request.Capability = capability
		if result, lease, err := owner.ReadTransaction(context.Background(), request); !errors.Is(err, ErrTransactionRecoveryUnauthorized) || lease != nil ||
			len(result.Records) != 0 {
			t.Fatalf("capability %x result=%+v lease=%T err=%v", capability, result, lease, err)
		}
	}
	request.Capability = serviceauthz.CapabilityTransactionRecovery
	if result, lease, err := owner.ReadTransaction(context.Background(), request); !errors.Is(err, ErrOwnerClosed) || errors.Is(err, ErrTransactionRecoveryUnauthorized) ||
		lease != nil || len(result.Records) != 0 {
		t.Fatalf("dedicated capability result=%+v lease=%T err=%v", result, lease, err)
	}
}

func TestTransactionRecoveryResponseChargeIncludesTypedArenaAndMaterialScratch(t *testing.T) {
	target := replicatedstate.TransactionRecoveryReadRequest{
		Kind: replicatedstate.TransactionRecoveryLookupTarget,
		ID:   distributedtxn.ID{1}, MinimumApplied: 1, MaxRows: 1,
		MaxBytes: replicatedstate.TransactionRecoverySummaryBytes,
	}
	charge, records, scratch, ok := transactionReadResponseCharge(target)
	wire, _ := pointReadResponseCharge(replicatedstate.TransactionRecoverySummaryBytes)
	if !ok || records != 1 || scratch != 0 ||
		charge != wire+transactionRecoveryRecordRetainedBytes {
		t.Fatalf("participant charge=%d records=%d scratch=%d ok=%t", charge, records, scratch, ok)
	}
	coordinator := target
	coordinator.Kind = replicatedstate.TransactionRecoveryLookupCoordinator
	coordinator.MaxBytes = replicatedstate.TransactionRecoverySummaryBytes +
		distributedtxn.MaxCoordinatorRecordBytes
	charge, records, scratch, ok = transactionReadResponseCharge(coordinator)
	wire, _ = pointReadResponseCharge(int(coordinator.MaxBytes))
	want := wire + transactionRecoveryRecordRetainedBytes +
		int64(replicatedstate.MaxTransactionRecoveryPayloadArenaBytes)
	if !ok || records != 1 || scratch != replicatedstate.MaxTransactionRecoveryPayloadArenaBytes ||
		charge != want {
		t.Fatalf("coordinator charge=%d want=%d records=%d scratch=%d ok=%t",
			charge, want, records, scratch, ok)
	}
	scan := replicatedstate.TransactionRecoveryReadRequest{
		Kind:           replicatedstate.TransactionRecoveryScanCoordinator,
		MinimumApplied: 1, MaxRows: replicatedstate.MaxTransactionRecoveryScanRows,
		MaxBytes: replicatedstate.MaxTransactionRecoveryScanBytes,
	}
	charge, records, scratch, ok = transactionReadResponseCharge(scan)
	if !ok || records != replicatedstate.MaxTransactionRecoveryScanRows || scratch != 0 || charge <= 0 {
		t.Fatalf("scan charge=%d records=%d scratch=%d ok=%t", charge, records, scratch, ok)
	}
}

func TestPointReadFenceAuthenticatesReplicaSetVersion(t *testing.T) {
	group := peerServerTestGroup()
	serving := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11}}
	fence := replicatedstate.SnapshotFence{Binding: replicatedstate.Binding{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		AllocationGeneration:  serving.AllocationGeneration,
		ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
		ActivePolicyGeneration: serving.Command.ActivePolicyGeneration,
		ProtectionEpoch:        serving.Command.ProtectionEpoch,
		OwnershipEpoch:         serving.Command.OwnershipEpoch,
		SchemaGeneration:       serving.Command.SchemaGeneration,
		RoutingVersion:         serving.Command.RoutingVersion,
		RouteGeneration:        serving.Command.RouteGeneration,
	}, RelationManifestDigest: serving.Command.RelationManifestDigest,
		ReplicaSetVersion: serving.Command.ReplicaSetVersion}
	if !pointReadFenceMatches(fence, serving) {
		t.Fatal("exact replica-set fence rejected")
	}
	fence.ReplicaSetVersion++
	if pointReadFenceMatches(fence, serving) {
		t.Fatal("stale replica-set publication accepted")
	}
}

func TestOwnerProposalDeliveryHandoffWinsBeforeCancellation(t *testing.T) {
	owner := newDeliveryTestOwner()
	ctx, cancel := context.WithCancel(context.Background())
	delivery := &proposalDelivery{}
	reply := make(chan ownerReply, 1)
	done := make(chan error, 1)
	go func() {
		_, err := owner.enqueue(ctx, ownerRequest{
			reply: reply, bytes: 1, delivery: delivery,
		})
		done <- err
	}()
	request := <-owner.ingress
	if !delivery.state.CompareAndSwap(proposalDeliveryPending, proposalDeliveryReady) {
		t.Fatal("owner did not acquire pending delivery")
	}
	cancel()
	reply <- ownerReply{}
	if err := <-done; err != nil {
		t.Fatalf("ready delivery = %v", err)
	}
	if delivery.state.Load() != proposalDeliveryReady {
		t.Fatalf("delivery state = %d", delivery.state.Load())
	}
	owner.release(request.bytes)
}

type deliveryTestProposalHost struct{}

func (deliveryTestProposalHost) EnqueueTrackedProposal(
	raftmember.GroupKey,
	[]byte,
	multiraft.ProposalToken,
) error {
	return nil
}

func TestOwnerAbandonedDeliveryCancelsRegisteredWaiter(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 1,
		MaxOutstandingAttempts: 1, MaxWaiters: 1,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: replicatedstate.MaxCompletionEnvelopeBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	group := peerServerTestGroup()
	command := replication.Command{
		Kind:                  replication.CommandMutationBatch,
		ClusterID:             replication.ID128(group.ClusterID),
		ClusterIncarnation:    replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		Distribution:          "docs", Shard: "0000-ffff", AllocationGeneration: 1,
		ShardIncarnation:  replication.ID128(group.ShardIncarnation),
		GroupID:           replication.ID128(group.GroupID),
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
		ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RoutingVersion: 1, RouteGeneration: 1,
		Tenant: []byte("tenant"), ClientID: replication.ID128{1},
		ClientEpoch: 1, ClientSequence: 1, Fingerprint: replication.Digest{1},
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: []byte("k"), Value: []byte("v"),
			}},
		}},
	}
	data, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	waiter, err := registry.Enqueue(deliveryTestProposalHost{}, group, data)
	if err != nil {
		t.Fatal(err)
	}
	if stats := registry.Stats(); stats.Waiters != 1 {
		t.Fatalf("waiters before abandoned handoff = %d", stats.Waiters)
	}
	delivery := &proposalDelivery{}
	delivery.state.Store(proposalDeliveryAbandoned)
	returned, err := handoffProposalWaiter(delivery, waiter)
	if !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("abandoned waiter handoff = %v", err)
	}
	if _, _, pollErr := returned.Poll(); !errors.Is(pollErr, raftserve.ErrWaiterClosed) {
		t.Fatalf("returned waiter remained live: %v", pollErr)
	}
	if stats := registry.Stats(); stats.Waiters != 0 {
		t.Fatalf("waiters after abandoned handoff = %d", stats.Waiters)
	}
}

func TestOwnerRejectsBeforeSerializedHostLaneStarts(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 1,
		MaxOutstandingAttempts: 1, MaxWaiters: 1,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: replicatedstate.MaxCompletionEnvelopeBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	host, err := registry.NewHost(multiraft.Limits{
		MaxGroups: 1, MaxQueueItems: 1, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 1, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 1, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes,
		MaxPendingTicks: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	identity := raftmember.RuntimeIdentity{
		Group: peerServerTestGroup(), AllocationGeneration: 1, MemberID: 1,
		StoreID: [16]byte{1}, NodeIncarnation: 1, RelationManifestDigest: [32]byte{1},
	}
	options := Options{
		Registry: registry, Host: host, Members: []raftmember.RuntimeIdentity{identity},
		CommandFences: []CommandFence{{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1},
			RoutingVersion:         1, RouteGeneration: 1,
		}},
		Limits: Limits{
			MaxIngressItems: 1, MaxIngressBytes: 1,
			MaxPendingProposalItems: 1, MaxPendingProposalBytes: 1,
			MaxPendingReadItems: 1, MaxPendingReadBytes: 1,
			MaxPendingOutboundBytes: 1,
		},
	}
	mismatched := options
	mismatched.Members = append([]raftmember.RuntimeIdentity(nil), options.Members...)
	mismatched.Members[0].RelationManifestDigest[0] ^= 1
	if owner, err := NewOwner(mismatched); owner != nil || !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("owner accepted storage-bound or cross-machine manifest = %v, %v", owner, err)
	}
	owner, err := NewOwner(options)
	if err != nil {
		t.Fatal(err)
	}
	_, err = owner.Probe(context.Background(), identity.Group)
	if !errors.Is(err, ErrOwnerClosed) {
		t.Fatalf("pre-Run Probe = %v", err)
	}
}

func TestOwnerPendingProposalBudgetIsIndependentAndReclaimable(t *testing.T) {
	owner := &Owner{
		limits:  Limits{MaxPendingProposalItems: 2, MaxPendingProposalBytes: 7},
		started: true,
	}
	if err := owner.reservePendingProposal(4); err != nil {
		t.Fatal(err)
	}
	if err := owner.reservePendingProposal(4); !errors.Is(err, ErrPendingProposalsFull) {
		t.Fatalf("byte-bound reserve = %v", err)
	}
	if owner.pendingProposalItems != 1 || owner.pendingProposalBytes != 4 ||
		owner.ingressItems != 0 || owner.ingressBytes != 0 {
		t.Fatalf("accounting = pending %d/%d ingress %d/%d",
			owner.pendingProposalItems, owner.pendingProposalBytes,
			owner.ingressItems, owner.ingressBytes)
	}
	owner.releasePendingProposal(4)
	if err := owner.reservePendingProposal(3); err != nil {
		t.Fatal(err)
	}
	if err := owner.reservePendingProposal(4); err != nil {
		t.Fatal(err)
	}
	if err := owner.reservePendingProposal(1); !errors.Is(err, ErrPendingProposalsFull) {
		t.Fatalf("item-bound reserve = %v", err)
	}
	owner.releasePendingProposal(3)
	owner.releasePendingProposal(4)
	if owner.pendingProposalItems != 0 || owner.pendingProposalBytes != 0 {
		t.Fatalf("released accounting = %d/%d",
			owner.pendingProposalItems, owner.pendingProposalBytes)
	}
	owner.closed = true
	if err := owner.reservePendingProposal(1); !errors.Is(err, ErrOwnerClosed) {
		t.Fatalf("closed reserve = %v", err)
	}
}

type proposalPrefixOwnerHost struct {
	ownerHost
	firstRunEntered chan struct{}
	releaseFirstRun chan struct{}
	queuedAtRun     chan int
	runs            int
	queued          int
	status          raftmember.RuntimeStatus
}

type deferredReadOwnerHost struct {
	ownerHost
	mu                sync.Mutex
	async             chan struct{}
	firstRunEntered   chan struct{}
	releaseFirstRun   chan struct{}
	secondReadAttempt chan struct{}
	firstRun          bool
	readyPending      bool
	woken             bool
	readCalls         int
	readContexts      [][16]byte
	readContext       []byte
	status            raftmember.RuntimeStatus
}

func (host *deferredReadOwnerHost) AsyncNotify() <-chan struct{} { return host.async }

func (host *deferredReadOwnerHost) WakePipelined() {
	host.mu.Lock()
	host.woken = true
	host.mu.Unlock()
}

func (host *deferredReadOwnerHost) RunOne() (multiraft.Progress, bool, error) {
	if !host.firstRun {
		host.firstRun = true
		close(host.firstRunEntered)
		<-host.releaseFirstRun
		return multiraft.Progress{}, false, nil
	}
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.woken && host.readyPending {
		host.woken = false
		host.readyPending = false
		return multiraft.Progress{Kind: multiraft.ProgressReady,
			ReadyKind: raftmember.DrivePersisted}, true, nil
	}
	if host.woken && len(host.readContext) != 0 {
		host.woken = false
		context := append([]byte(nil), host.readContext...)
		host.readContext = nil
		return multiraft.Progress{Kind: multiraft.ProgressReady,
			ReadyKind: raftmember.DriveReadStatesFinished,
			ReadOutcomes: []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
				Context: context, Index: host.status.Applied,
				Term: host.status.Term, Incarnation: 1,
			}}}}, true, nil
	}
	return multiraft.Progress{}, false, nil
}

func (host *deferredReadOwnerHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}

func (host *deferredReadOwnerHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *deferredReadOwnerHost) ReadIndex(_ raftmember.GroupKey, context []byte) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.readCalls++
	var retained [16]byte
	copy(retained[:], context)
	host.readContexts = append(host.readContexts, retained)
	if host.readCalls == 2 {
		close(host.secondReadAttempt)
	}
	if host.readyPending {
		return raftmodel.ErrReadyPending
	}
	host.readContext = append(host.readContext[:0], context...)
	host.woken = true
	return nil
}

func (host *deferredReadOwnerHost) Close() error { return nil }

func newDeferredReadOwnerFixture() (
	raftmember.GroupKey,
	CommandFence,
	*deferredReadOwnerHost,
	*Owner,
	*ownerTestReadSource,
) {
	group := peerServerTestGroup()
	identity := raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
		MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1,
		RelationManifestDigest: [32]byte{1}}
	command := CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
		ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	host := &deferredReadOwnerHost{
		async: make(chan struct{}, 1), firstRunEntered: make(chan struct{}),
		releaseFirstRun: make(chan struct{}), secondReadAttempt: make(chan struct{}),
		status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2,
			Commit: 7, Applied: 7},
	}
	source := &ownerTestReadSource{}
	owner := &Owner{
		host: host, groups: []raftmember.GroupKey{group},
		members: map[raftmember.GroupKey]ownerMember{group: {
			identity: identity, command: command, generation: &ownerGeneration{}, read: source,
		}},
		limits: Limits{MaxIngressItems: 2, MaxIngressBytes: 64,
			MaxPendingReadItems: 2, MaxPendingReadBytes: 2},
		ingress: make(chan ownerRequest, 2), ready: make(chan struct{}), done: make(chan struct{}),
		pendingReads: make(map[[16]byte]*readDelivery),
	}
	return group, command, host, owner, source
}

func deferredLinearizablePointRequest(
	group raftmember.GroupKey,
	command CommandFence,
) LinearizablePointReadRequest {
	return LinearizablePointReadRequest{
		Fence: ServingFence{Group: group, AllocationGeneration: 1, Command: command,
			MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1, Term: 2},
		Capability: serviceauthz.CapabilityDataRead,
	}
}

func TestOwnerDefersLinearizableReadAcrossAsyncReadyBoundary(t *testing.T) {
	group, command, host, owner, source := newDeferredReadOwnerFixture()
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(ctx) }()
	<-owner.ready
	<-host.firstRunEntered

	host.mu.Lock()
	host.readyPending = true
	host.mu.Unlock()
	result := make(chan error, 1)
	var cut LinearizablePointReadCut
	go func() {
		result <- owner.ReadLinearizablePointInto(context.Background(),
			deferredLinearizablePointRequest(group, command), &cut)
	}()
	close(host.releaseFirstRun)
	<-host.secondReadAttempt
	select {
	case err := <-result:
		t.Fatalf("read settled before Ready completion: %v", err)
	default:
	}
	owner.mu.Lock()
	if owner.ingressItems != 1 || owner.pendingReadItems != 1 {
		t.Fatalf("retained charges ingress=%d reads=%d", owner.ingressItems, owner.pendingReadItems)
	}
	owner.mu.Unlock()

	host.async <- struct{}{}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if cut.Source() != source || cut.minimumApplied != 7 {
		t.Fatalf("cut source=%T applied=%d", cut.Source(), cut.minimumApplied)
	}
	host.mu.Lock()
	if host.readCalls != 3 || host.readyPending || len(host.readContext) != 0 ||
		len(host.readContexts) != 3 || host.readContexts[0] != host.readContexts[1] ||
		host.readContexts[1] != host.readContexts[2] || owner.readSequence != 1 {
		t.Fatalf("read calls=%d ready=%t context=%x attempts=%x sequence=%d",
			host.readCalls, host.readyPending, host.readContext, host.readContexts, owner.readSequence)
	}
	host.mu.Unlock()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	owner.mu.Lock()
	if owner.ingressItems != 0 || owner.pendingReadItems != 0 {
		t.Fatalf("released charges ingress=%d reads=%d", owner.ingressItems, owner.pendingReadItems)
	}
	owner.mu.Unlock()
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner exit=%v", err)
	}
}

func TestOwnerStopSettlesDeferredReadAndReleasesCharges(t *testing.T) {
	group, command, host, owner, _ := newDeferredReadOwnerFixture()
	runCtx, stop := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(runCtx) }()
	<-owner.ready
	<-host.firstRunEntered

	host.mu.Lock()
	host.readyPending = true
	host.mu.Unlock()
	result := make(chan error, 1)
	go func() {
		var cut LinearizablePointReadCut
		result <- owner.ReadLinearizablePointInto(context.Background(),
			deferredLinearizablePointRequest(group, command), &cut)
	}()
	close(host.releaseFirstRun)
	<-host.secondReadAttempt
	stop()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner exit=%v", err)
	}
	if err := <-result; !errors.Is(err, ErrOwnerClosed) || !errors.Is(err, context.Canceled) {
		t.Fatalf("deferred read exit=%v", err)
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.ingressItems != 0 || owner.ingressBytes != 0 ||
		owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 ||
		len(owner.pendingReads) != 0 {
		t.Fatalf("retained accounting ingress=%d/%d reads=%d/%d barriers=%d",
			owner.ingressItems, owner.ingressBytes, owner.pendingReadItems,
			owner.pendingReadBytes, len(owner.pendingReads))
	}
}

type synchronousDeferredReadOwnerHost struct {
	ownerHost
	status       raftmember.RuntimeStatus
	ready        uint8
	readCalls    int
	readContexts [][16]byte
	readContext  [16]byte
}

func (host *synchronousDeferredReadOwnerHost) RunOne() (multiraft.Progress, bool, error) {
	switch host.ready {
	case 1:
		host.ready = 0
		return multiraft.Progress{Kind: multiraft.ProgressReady,
			ReadyKind: raftmember.DrivePersisted}, true, nil
	case 2:
		host.ready = 0
		context := host.readContext
		return multiraft.Progress{Kind: multiraft.ProgressReady,
			ReadyKind: raftmember.DriveReadStatesFinished,
			ReadOutcomes: []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
				Context: context[:], Index: host.status.Applied,
				Term: host.status.Term, Incarnation: 1,
			}}}}, true, nil
	default:
		return multiraft.Progress{}, false, nil
	}
}

func (host *synchronousDeferredReadOwnerHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}

func (host *synchronousDeferredReadOwnerHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *synchronousDeferredReadOwnerHost) ReadIndex(_ raftmember.GroupKey, context []byte) error {
	host.readCalls++
	var retained [16]byte
	copy(retained[:], context)
	host.readContexts = append(host.readContexts, retained)
	if host.readCalls == 1 {
		host.ready = 1
		return raftmodel.ErrReadyPending
	}
	host.readContext = retained
	host.ready = 2
	return nil
}

func (host *synchronousDeferredReadOwnerHost) Close() error { return nil }

func TestOwnerDeferredReadDrainsSynchronousHostWithoutAsyncNotifier(t *testing.T) {
	group := peerServerTestGroup()
	identity := raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
		MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1,
		RelationManifestDigest: [32]byte{1}}
	command := CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
		ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	host := &synchronousDeferredReadOwnerHost{status: raftmember.RuntimeStatus{
		MemberID: 1, LeaderID: 1, Term: 2, Commit: 7, Applied: 7,
	}}
	source := &ownerTestReadSource{}
	owner := &Owner{
		host: host, groups: []raftmember.GroupKey{group},
		members: map[raftmember.GroupKey]ownerMember{group: {
			identity: identity, command: command, generation: &ownerGeneration{}, read: source,
		}},
		limits: Limits{MaxIngressItems: 1, MaxIngressBytes: 1,
			MaxPendingReadItems: 1, MaxPendingReadBytes: 1},
		ingress: make(chan ownerRequest, 1), ready: make(chan struct{}), done: make(chan struct{}),
		pendingReads: make(map[[16]byte]*readDelivery),
	}
	runCtx, stop := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(runCtx) }()
	<-owner.ready
	var cut LinearizablePointReadCut
	if err := owner.ReadLinearizablePointInto(context.Background(),
		deferredLinearizablePointRequest(group, command), &cut); err != nil {
		t.Fatal(err)
	}
	if cut.Source() != source || cut.minimumApplied != 7 || host.readCalls != 2 ||
		len(host.readContexts) != 2 || host.readContexts[0] != host.readContexts[1] ||
		owner.readSequence != 1 {
		t.Fatalf("cut source=%T applied=%d calls=%d contexts=%x sequence=%d",
			cut.Source(), cut.minimumApplied, host.readCalls, host.readContexts, owner.readSequence)
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner exit=%v", err)
	}
}

var errDeferredReadCollectorFlush = errors.New("deferred read collector flush")

type deferredReadCollectorFailureHost struct {
	ownerHost
	firstRunEntered chan struct{}
	releaseFirstRun chan struct{}
	firstRun        bool
	status          raftmember.RuntimeStatus
}

func (host *deferredReadCollectorFailureHost) RunOne() (multiraft.Progress, bool, error) {
	if !host.firstRun {
		host.firstRun = true
		close(host.firstRunEntered)
		<-host.releaseFirstRun
	}
	return multiraft.Progress{}, false, nil
}

func (host *deferredReadCollectorFailureHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}

func (host *deferredReadCollectorFailureHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *deferredReadCollectorFailureHost) ReadIndex(raftmember.GroupKey, []byte) error {
	return raftmodel.ErrReadyPending
}

func (host *deferredReadCollectorFailureHost) EnqueueTrackedProposal(
	raftmember.GroupKey, []byte, multiraft.ProposalToken,
) error {
	return errDeferredReadCollectorFlush
}

func (host *deferredReadCollectorFailureHost) Close() error { return nil }

func TestOwnerCollectorFlushFailureSettlesDetachedDeferredRead(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 1,
		MaxOutstandingAttempts: 1, MaxWaiters: 1,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	group := peerServerTestGroup()
	identity := raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
		MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1,
		RelationManifestDigest: [32]byte{1}}
	commandFence := CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
		ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	host := &deferredReadCollectorFailureHost{
		firstRunEntered: make(chan struct{}), releaseFirstRun: make(chan struct{}),
		status: raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2,
			Commit: 7, Applied: 7},
	}
	owner := &Owner{
		registry: registry, host: host, groups: []raftmember.GroupKey{group},
		members: map[raftmember.GroupKey]ownerMember{group: {
			identity: identity, command: commandFence, generation: &ownerGeneration{},
			read: &ownerTestReadSource{},
		}},
		limits: Limits{MaxIngressItems: 2, MaxIngressBytes: 1 << 20,
			MaxPendingReadItems: 1, MaxPendingReadBytes: 1},
		ingress: make(chan ownerRequest, 2), ready: make(chan struct{}), done: make(chan struct{}),
		pendingReads: make(map[[16]byte]*readDelivery),
	}
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(context.Background()) }()
	<-owner.ready
	<-host.firstRunEntered

	proposalReply := make(chan ownerReply, 1)
	proposal := proposalPrefixCommand(t, group, 1)
	if err := owner.publish(ownerRequest{
		kind: requestProposal, group: group,
		fence: ServingFence{Group: group, AllocationGeneration: 1, Command: commandFence,
			MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1, Term: 2},
		data: proposal, reply: proposalReply, bytes: int64(len(proposal)),
		delivery: &proposalDelivery{}, async: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := owner.reservePendingRead(1); err != nil {
		t.Fatal(err)
	}
	readDelivery := &readDelivery{reply: make(chan ownerReply, 1)}
	if err := owner.publish(ownerRequest{
		kind: requestReadLinear, group: group, reply: readDelivery.reply,
		read: readRequest{
			fence:    deferredLinearizablePointRequest(group, commandFence).Fence,
			delivery: readDelivery,
		},
	}); err != nil {
		owner.releasePendingRead(1)
		t.Fatal(err)
	}
	close(host.releaseFirstRun)
	if runErr := <-runDone; !errors.Is(runErr, errDeferredReadCollectorFlush) {
		t.Fatalf("owner exit=%v", runErr)
	}
	if reply := <-proposalReply; !errors.Is(reply.err, errDeferredReadCollectorFlush) {
		t.Fatalf("proposal reply=%v", reply.err)
	}
	if reply := <-readDelivery.reply; !errors.Is(reply.err, ErrOwnerClosed) ||
		!errors.Is(reply.err, errDeferredReadCollectorFlush) {
		t.Fatalf("read reply=%v", reply.err)
	}
	owner.releasePendingRead(1)
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.ingressItems != 0 || owner.ingressBytes != 0 ||
		owner.pendingReadItems != 0 || owner.pendingReadBytes != 0 ||
		len(owner.pendingReads) != 0 {
		t.Fatalf("retained accounting ingress=%d/%d reads=%d/%d barriers=%d",
			owner.ingressItems, owner.ingressBytes, owner.pendingReadItems,
			owner.pendingReadBytes, len(owner.pendingReads))
	}
}

func (host *proposalPrefixOwnerHost) EnqueueTrackedProposal(
	raftmember.GroupKey, []byte, multiraft.ProposalToken,
) error {
	host.queued++
	return nil
}

func (host *proposalPrefixOwnerHost) RunOne() (multiraft.Progress, bool, error) {
	host.runs++
	if host.runs == 1 {
		close(host.firstRunEntered)
		<-host.releaseFirstRun
	} else if host.runs == 2 {
		host.queuedAtRun <- host.queued
	}
	return multiraft.Progress{}, false, nil
}

func (host *proposalPrefixOwnerHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}

func (host *proposalPrefixOwnerHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return host.status, nil
}

func (host *proposalPrefixOwnerHost) Close() error { return nil }

func proposalPrefixCommand(t *testing.T, group raftmember.GroupKey, sequence uint64) []byte {
	t.Helper()
	command := replication.Command{
		Kind:                   replication.CommandMutationBatch,
		ClusterID:              replication.ID128(group.ClusterID),
		ClusterIncarnation:     replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch:  group.TopologyRecoveryEpoch,
		Distribution:           "docs",
		Shard:                  "0000-ffff",
		AllocationGeneration:   1,
		ShardIncarnation:       replication.ID128(group.ShardIncarnation),
		GroupID:                replication.ID128(group.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: 1,
		ProtectionEpoch:        1,
		OwnershipEpoch:         1,
		SchemaGeneration:       1,
		RoutingVersion:         1,
		RouteGeneration:        1,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{1},
		ClientEpoch:            1,
		ClientSequence:         sequence,
		Fingerprint:            replication.Digest{byte(sequence)},
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: []byte{byte(sequence)}, Value: []byte("v"),
			}},
		}},
	}
	data, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestOwnerQueuesSameGroupPrefixBeforeNextHostRunOne(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 2,
		MaxOutstandingAttempts: 2, MaxWaiters: 2,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: 2 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()

	group := peerServerTestGroup()
	identity := raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
		MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1,
		RelationManifestDigest: [32]byte{1}}
	command := CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
		ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
		RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	host := &proposalPrefixOwnerHost{
		firstRunEntered: make(chan struct{}), releaseFirstRun: make(chan struct{}),
		queuedAtRun: make(chan int, 1),
		status:      raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2},
	}
	owner := &Owner{
		registry: registry, host: host, groups: []raftmember.GroupKey{group},
		members: map[raftmember.GroupKey]ownerMember{group: {
			identity: identity, command: command, generation: &ownerGeneration{},
		}},
		limits:  Limits{MaxIngressItems: 4, MaxIngressBytes: 1 << 20},
		ingress: make(chan ownerRequest, 4), ready: make(chan struct{}), done: make(chan struct{}),
		pendingReads: make(map[[16]byte]*readDelivery),
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- owner.Run(ctx) }()
	<-owner.ready
	<-host.firstRunEntered

	replies := [2]chan ownerReply{make(chan ownerReply, 1), make(chan ownerReply, 1)}
	for index := range replies {
		data := proposalPrefixCommand(t, group, uint64(index+1))
		if err := owner.publish(ownerRequest{
			kind: requestProposal, group: group,
			fence: ServingFence{Group: group, AllocationGeneration: 1, Command: command,
				MemberID: 1, StoreID: [16]byte{1}, NodeIncarnation: 1, Term: 2},
			data: data, reply: replies[index], bytes: int64(len(data)), delivery: &proposalDelivery{},
		}); err != nil {
			t.Fatal(err)
		}
	}
	close(host.releaseFirstRun)
	if queued := <-host.queuedAtRun; queued != len(replies) {
		t.Fatalf("Host.RunOne saw %d queued proposals, want %d", queued, len(replies))
	}
	for _, reply := range replies {
		result := <-reply
		if result.err != nil {
			t.Fatal(result.err)
		}
		result.waiter.Cancel()
	}
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("owner shutdown=%v", err)
	}
	if owner.ingressItems != 0 || owner.ingressBytes != 0 {
		t.Fatalf("ingress accounting after shutdown=%d/%d", owner.ingressItems, owner.ingressBytes)
	}
}

func TestProposalIngressCollectorDrainsAlreadyQueuedSameGroupPrefix(t *testing.T) {
	group := peerServerTestGroup()
	request := func(sequence byte) ownerRequest {
		return ownerRequest{
			kind: requestProposal, group: group, data: []byte{sequence},
			reply: make(chan ownerReply, 1), bytes: 1,
		}
	}
	collector := newProposalIngressCollector(4)
	collector.start(request(1))
	if !collector.active() || collector.bytes != 1 || collector.full() {
		t.Fatalf("initial collector=%+v", collector)
	}
	queue := make(chan ownerRequest, 4)
	for sequence := byte(2); sequence <= 4; sequence++ {
		queue <- request(sequence)
	}
	barrier, present, err := collector.drain(queue, func(ownerRequest) error {
		t.Fatal("same-group prefix invoked independent handler")
		return nil
	})
	if err != nil || present || barrier.kind != 0 || barrier.data != nil {
		t.Fatalf("drain barrier=%+v present=%t err=%v", barrier, present, err)
	}
	if !collector.full() || len(collector.requests) != 4 || collector.bytes != 4 {
		t.Fatalf("full collector=%+v", collector)
	}
	other := request(6)
	other.group.GroupID[0] ^= 0xff
	if collector.accepts(other) {
		t.Fatal("collector accepted a cross-group proposal")
	}
	collector.reset()
}

func TestProposalIngressSingletonHasNoArtificialTimer(t *testing.T) {
	group := peerServerTestGroup()
	collector := newProposalIngressCollector(16)
	defer collector.reset()
	request := ownerRequest{kind: requestProposal, group: group, data: []byte{1}}
	collector.start(request)
	queue := make(chan ownerRequest)
	barrier, present, err := collector.drain(queue, func(ownerRequest) error { return nil })
	if err != nil || present || barrier.kind != 0 || barrier.data != nil || len(collector.requests) != 1 {
		t.Fatalf("singleton drain barrier=%+v present=%t err=%v collector=%+v",
			barrier, present, err, collector)
	}
}

func TestProposalIngressCollectorPreservesIndependentLanesAndOrderingBarrier(t *testing.T) {
	group := peerServerTestGroup()
	proposal := func(group raftmember.GroupKey, sequence byte) ownerRequest {
		return ownerRequest{kind: requestProposal, group: group, data: []byte{sequence}}
	}
	collector := newProposalIngressCollector(8)
	collector.start(proposal(group, 1))
	queue := make(chan ownerRequest, 4)
	queue <- ownerRequest{kind: requestReadLinear}
	queue <- ownerRequest{kind: requestInbound}
	queue <- proposal(group, 2)
	other := group
	other.GroupID[0] ^= 0xff
	queue <- proposal(other, 3)
	independent := make([]requestKind, 0, 2)
	barrier, present, err := collector.drain(queue, func(request ownerRequest) error {
		independent = append(independent, request.kind)
		return nil
	})
	if err != nil || !present || barrier.group != other ||
		len(collector.requests) != 2 || len(independent) != 2 ||
		independent[0] != requestReadLinear || independent[1] != requestInbound {
		t.Fatalf("collector=%+v independent=%v barrier=%+v present=%t err=%v",
			collector, independent, barrier, present, err)
	}
}

func TestProposalIngressCollectorSplitsAtExactByteBound(t *testing.T) {
	group := peerServerTestGroup()
	collector := newProposalIngressCollector(4)
	first := ownerRequest{kind: requestProposal, group: group,
		data: make([]byte, raftmodel.MaxProposalBatchBytes-1)}
	collector.start(first)
	queue := make(chan ownerRequest, 1)
	next := ownerRequest{kind: requestProposal, group: group, data: []byte{1, 2}}
	queue <- next
	barrier, present, err := collector.drain(queue, func(ownerRequest) error { return nil })
	if err != nil || !present || len(barrier.data) != len(next.data) ||
		len(collector.requests) != 1 || collector.bytes != raftmodel.MaxProposalBatchBytes-1 {
		t.Fatalf("collector=%+v barrier bytes=%d present=%t err=%v",
			collector, len(barrier.data), present, err)
	}
}
