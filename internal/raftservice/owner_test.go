package raftservice

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func newDeliveryTestOwner() *Owner {
	return &Owner{
		limits:  Limits{MaxIngressItems: 1, MaxIngressBytes: 1},
		ingress: make(chan ownerRequest, 1), started: true,
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

func TestOwnerReadOutcomeSettlesExactFixedContextAndCancellationCleansUp(t *testing.T) {
	owner := &Owner{pendingReads: make(map[[16]byte]*readDelivery)}
	var contextKey [16]byte
	contextKey[0], contextKey[15] = 3, 9
	source := &ownerTestReadSource{}
	delivery := &readDelivery{reply: make(chan ownerReply, 1), source: source, minimumApplied: 23}
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

	barrierDelivery := &readDelivery{reply: make(chan ownerReply, 1), source: source, minimumApplied: 11}
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

type ownerTestReadSource struct {
	result replicatedstate.PointReadResult
	err    error
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
