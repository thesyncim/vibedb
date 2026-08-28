package rebalance

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

type fanoutMembershipInstaller struct {
	started chan rafttransport.NodeID
	release <-chan struct{}
	fail    rafttransport.NodeID
}

func (installer *fanoutMembershipInstaller) InstallMembershipGrant(
	_ context.Context, node rafttransport.NodeID, _ membershipgrant.Grant,
) error {
	installer.started <- node
	<-installer.release
	if node == installer.fail {
		return errors.New("install failed")
	}
	return nil
}

type fanoutMembershipApplier struct {
	mu    sync.Mutex
	calls int
}

func (applier *fanoutMembershipApplier) ApplyMembership(
	context.Context,
	gateway.ReplicatedMembershipRoute,
	shardservice.ReplicatedMembershipRequest,
) (gateway.ReplicatedMembershipResult, error) {
	applier.mu.Lock()
	applier.calls++
	applier.mu.Unlock()
	return gateway.ReplicatedMembershipResult{Retries: 1}, nil
}

func (applier *fanoutMembershipApplier) count() int {
	applier.mu.Lock()
	defer applier.mu.Unlock()
	return applier.calls
}

func TestMembershipGrantClientBoundsFanoutBeforeApply(t *testing.T) {
	route, grant, request := membershipGrantFanoutFixture()
	release := make(chan struct{})
	installer := &fanoutMembershipInstaller{
		started: make(chan rafttransport.NodeID, MembershipGrantFanout), release: release,
	}
	applier := new(fanoutMembershipApplier)
	client, err := NewMembershipGrantClient(grant, installer, applier)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.ApplyMembership(context.Background(), route, request)
		done <- err
	}()
	seen := make(map[rafttransport.NodeID]struct{}, MembershipGrantFanout)
	for index := 0; index < MembershipGrantFanout; index++ {
		node := <-installer.started
		seen[node] = struct{}{}
	}
	if len(seen) != MembershipGrantFanout || applier.count() != 0 {
		t.Fatalf("fanout=%d apply calls=%d", len(seen), applier.count())
	}
	select {
	case extra := <-installer.started:
		t.Fatalf("unbounded extra fanout to %x", extra)
	default:
	}
	close(release)
	if err = <-done; err != nil || applier.count() != 1 {
		t.Fatalf("apply calls=%d err=%v", applier.count(), err)
	}
}

func TestMembershipGrantClientFailsClosedBeforeApply(t *testing.T) {
	route, grant, request := membershipGrantFanoutFixture()
	closed := make(chan struct{})
	close(closed)
	installer := &fanoutMembershipInstaller{
		started: make(chan rafttransport.NodeID, MembershipGrantFanout), release: closed,
		fail: route.Serving.Replicas[1].Node,
	}
	applier := new(fanoutMembershipApplier)
	client, err := NewMembershipGrantClient(grant, installer, applier)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.ApplyMembership(context.Background(), route, request); !errors.Is(err, ErrMembershipGrantInstall) {
		t.Fatalf("install failure err=%v", err)
	}
	if applier.count() != 0 {
		t.Fatalf("membership applied after failed install: %d", applier.count())
	}
	bad := route
	bad.EnrolledTarget.Node = bad.Serving.Replicas[0].Node
	if _, err = client.ApplyMembership(context.Background(), bad, request); !errors.Is(err, ErrMembershipGrantInstall) {
		t.Fatalf("duplicate route err=%v", err)
	}
}

func membershipGrantFanoutFixture() (
	gateway.ReplicatedMembershipRoute,
	membershipgrant.Grant,
	shardservice.ReplicatedMembershipRequest,
) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	replicas := []gateway.ReplicatedEndpoint{
		{Member: 1, Node: rafttransport.NodeID{1}},
		{Member: 2, Node: rafttransport.NodeID{2}},
		{Member: 3, Node: rafttransport.NodeID{3}},
	}
	target := gateway.ReplicatedEndpoint{Member: 4, Node: rafttransport.NodeID{4}}
	route := gateway.ReplicatedMembershipRoute{
		Serving:        gateway.ReplicatedRoute{Group: group, Replicas: replicas},
		EnrolledTarget: target, HasEnrolledTarget: true,
	}
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{6}, MetadataEpoch: 7, CatalogGeneration: 8,
		InitialReplicaSetVersion: 9, InitialVoters: [3]uint64{1, 2, 3},
		InitialDescriptorDigest: [32]byte{10}, SourceMember: 1, TargetMember: 4,
		TargetNode: [16]byte(target.Node),
	}
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 9,
		[3]membershipgrant.RosterMember{
			{Member: 1, Node: [16]byte(replicas[0].Node)},
			{Member: 2, Node: [16]byte(replicas[1].Node)},
			{Member: 3, Node: [16]byte(replicas[2].Node)},
		})
	request := shardservice.ReplicatedMembershipRequest{
		TransitionID: grant.TransitionID, MetadataEpoch: grant.MetadataEpoch,
		CatalogGeneration: grant.CatalogGeneration,
		SourceMember:      grant.SourceMember, TargetMember: grant.TargetMember,
	}
	return route, grant, request
}
