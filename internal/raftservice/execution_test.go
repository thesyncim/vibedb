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
)

type executionTestOutbound struct{}

func (executionTestOutbound) Send(raftmember.OutboundMessage) error { return nil }

func TestExecutionOwnersRouteByGroupAndStartEveryLane(t *testing.T) {
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 2, MaxOutstandingIdentities: 2, MaxOutstandingAttempts: 2,
		MaxWaiters: 2, MaxAttemptsPerIdentity: 1,
		MaxRetainedCompletionBytes: 2 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	hostLimits := multiraft.Limits{
		MaxGroups: 2, MaxQueueItems: 2, MaxQueueBytes: raftmodel.MaxInboundMessageBytes,
		MaxGroupItems: 2, MaxGroupBytes: raftmodel.MaxInboundMessageBytes,
		MaxOutboxItems: 2, MaxOutboxBytes: raftmodel.MaxInboundMessageBytes, MaxPendingTicks: 1,
	}
	lanes, err := registry.NewExecutionLanes(2, hostLimits)
	if err != nil {
		t.Fatal(err)
	}
	groups := make([]raftmember.GroupKey, 0, 2)
	for seed := byte(1); len(groups) < 2; seed++ {
		group := peerServerTestGroup()
		group.GroupID[0] = seed
		lane, laneErr := lanes.Lane(group)
		if laneErr != nil {
			t.Fatal(laneErr)
		}
		if lane == len(groups) {
			groups = append(groups, group)
		}
	}
	members := make([]raftmember.RuntimeIdentity, 2)
	commands := make([]CommandFence, 2)
	for index, group := range groups {
		members[index] = raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 1,
			MemberID: uint64(index + 1), StoreID: [16]byte{byte(index + 1)},
			NodeIncarnation: 1, RelationManifestDigest: [32]byte{1}}
		commands[index] = CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1}
	}
	outbound := executionTestOutbound{}
	owners, err := NewExecutionOwners(ExecutionOptions{
		Registry: registry, Lanes: lanes, Members: members, CommandFences: commands,
		Outbound: outbound, Limits: Limits{MaxIngressItems: 2, MaxIngressBytes: 1024,
			MaxPendingProposalItems: 2, MaxPendingProposalBytes: 1024,
			MaxPendingReadItems: 2, MaxPendingReadBytes: 1024, MaxPendingOutboundBytes: 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, group := range groups {
		owner, routeErr := owners.owner(group)
		if routeErr != nil || owner != owners.owners[index] || owner.outbound != outbound {
			t.Fatalf("group %d route owner=%p want=%p err=%v", index, owner, owners.owners[index], routeErr)
		}
	}
	unknown := groups[0]
	unknown.GroupID[15] ^= 0xff
	if _, err := owners.Probe(context.Background(), unknown); !errors.Is(err, ErrExecutionGroup) {
		t.Fatalf("unknown group error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- owners.Run(ctx) }()
	<-owners.Started()
	if !owners.Running() || !owners.owners[0].Running() || !owners.owners[1].Running() {
		t.Fatal("not every lane owner published running")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run shutdown = %v", err)
	}
	select {
	case <-owners.Done():
	default:
		t.Fatal("Done did not close after joined shutdown")
	}
	if owners.Running() {
		t.Fatal("owners remained running after shutdown")
	}
}
