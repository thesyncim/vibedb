package shardservice

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type membershipGrantTestConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
	class    rafttransport.TrafficClass
}

func (connection *membershipGrantTestConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}

func (connection *membershipGrantTestConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type membershipGrantOpenFunc func(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)

func (open membershipGrantOpenFunc) OpenShardControl(
	ctx context.Context, node rafttransport.NodeID,
) (rafttransport.PeerConnection, error) {
	return open(ctx, node)
}

func TestMembershipGrantInstallUnblocksFollowerConfChange(t *testing.T) {
	group, grant, members := membershipGrantControlFixture()
	openRegistry := func(local rafttransport.NodeID) *rafttransport.StaticRegistry {
		registry, err := rafttransport.NewStaticRegistry(local, members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: len(members)})
		if err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, follower := openRegistry(members[0].Node), openRegistry(members[1].Node)
	if err := leader.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	message := membershipGrantConfChangeMessage(t, grant, 1, 2)
	frame, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 2, Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaderIdentity := rafttransport.PeerIdentity{TrustDomain: leader.TrustDomain(), Node: members[0].Node}
	if _, err = follower.DecodeInbound(leaderIdentity, frame); !errors.Is(err, rafttransport.ErrUnauthorized) {
		t.Fatalf("follower accepted confchange before grant: %v", err)
	}

	controller := rafttransport.PeerIdentity{TrustDomain: leader.TrustDomain(), Node: rafttransport.NodeID{9}}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{
		Node: controller.Node, Capabilities: serviceauthz.CapabilityMembership,
	}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	service, err := NewMembershipGrantControlService(follower, policy, deadline, deadline)
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	opener := membershipGrantOpenFunc(func(_ context.Context, node rafttransport.NodeID) (rafttransport.PeerConnection, error) {
		if node != members[1].Node {
			return nil, ErrMembershipGrantControl
		}
		clientSide, serverSide := net.Pipe()
		go func() {
			serveDone <- service.Serve(context.Background(), &membershipGrantTestConnection{
				Conn: serverSide, identity: controller, class: rafttransport.TrafficShardControl,
			})
		}()
		return &membershipGrantTestConnection{
			Conn:     clientSide,
			identity: rafttransport.PeerIdentity{TrustDomain: leader.TrustDomain(), Node: members[1].Node},
			class:    rafttransport.TrafficShardControl,
		}, nil
	})
	client, err := NewMembershipGrantControlClient(opener, deadline, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.InstallMembershipGrant(context.Background(), members[1].Node, grant); err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nil {
		t.Fatal(err)
	}
	if err = client.InstallMembershipGrant(context.Background(), members[1].Node, grant); err != nil {
		t.Fatal(err)
	}
	if err = <-serveDone; err != nil {
		t.Fatal(err)
	}
	if _, err = follower.DecodeInbound(leaderIdentity, frame); err != nil {
		t.Fatalf("follower rejected confchange after grant: %v", err)
	}
}

func TestMembershipGrantControlRejectsCorruptForgedAndUnauthorized(t *testing.T) {
	_, grant, members := membershipGrantControlFixture()
	raw, err := AppendMembershipGrantRequest(nil, grant)
	if err != nil {
		t.Fatal(err)
	}
	for _, corrupt := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenMembershipGrantRequest(corrupt); !errors.Is(err, ErrMembershipGrantControl) {
			t.Fatalf("corrupt length %d err=%v", len(corrupt), err)
		}
	}
	corrupt := bytes.Clone(raw)
	corrupt[0] ^= 0xff
	if _, err := OpenMembershipGrantRequest(corrupt); !errors.Is(err, ErrMembershipGrantControl) {
		t.Fatalf("corrupt magic err=%v", err)
	}

	registry, err := rafttransport.NewStaticRegistry(members[1].Node, members,
		rafttransport.Limits{MaxGroups: 1, MaxMembers: len(members)})
	if err != nil {
		t.Fatal(err)
	}
	forged := grant
	forged.TargetNode[0]++
	if err = registry.InstallTransitionGrant(forged); !errors.Is(err, rafttransport.ErrReplicaSet) {
		t.Fatalf("forged target err=%v", err)
	}
	if _, found, err := registry.CurrentTransitionGrant(grant.Group); err != nil || found {
		t.Fatalf("forged grant installed found=%t err=%v", found, err)
	}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{{
		Node: rafttransport.NodeID{9}, Capabilities: serviceauthz.CapabilityTopology,
	}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(time.Second) }
	if _, err = NewMembershipGrantControlService(registry, policy, deadline, deadline); !errors.Is(err, ErrMembershipGrantControl) {
		t.Fatalf("policy without membership err=%v", err)
	}
	controller := rafttransport.PeerIdentity{
		TrustDomain: rafttransport.TrustDomain{
			ClusterID: grant.Group.ClusterID, ClusterIncarnation: grant.Group.ClusterIncarnation,
		},
		Node: rafttransport.NodeID{9},
	}
	policy, err = serviceauthz.NewPolicy(2, []serviceauthz.Entry{{
		Node: controller.Node, Capabilities: serviceauthz.CapabilityMembership,
	}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewMembershipGrantControlService(registry, policy, deadline, deadline)
	if err != nil {
		t.Fatal(err)
	}
	clientSide, serverSide := net.Pipe()
	serveDone := make(chan error, 1)
	unauthorized := controller
	unauthorized.Node[0]++
	go func() {
		serveDone <- service.Serve(context.Background(), &membershipGrantTestConnection{
			Conn: serverSide, identity: unauthorized, class: rafttransport.TrafficShardControl,
		})
	}()
	if err = WriteMembershipGrantRequest(clientSide, grant); err != nil {
		t.Fatal(err)
	}
	_ = clientSide.Close()
	if err = <-serveDone; !errors.Is(err, ErrMembershipGrantUnauthorized) {
		t.Fatalf("unauthorized principal err=%v", err)
	}
	if _, found, err := registry.CurrentTransitionGrant(grant.Group); err != nil || found {
		t.Fatalf("unauthorized grant installed found=%t err=%v", found, err)
	}
}

func membershipGrantControlFixture() (
	raftmember.GroupKey, membershipgrant.Grant, []rafttransport.Member,
) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	members := []rafttransport.Member{
		{Group: group, ReplicaSetVersion: 9, MemberID: 1, Node: rafttransport.NodeID{1}, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 9, MemberID: 2, Node: rafttransport.NodeID{2}, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 9, MemberID: 3, Node: rafttransport.NodeID{3}, Role: rafttransport.MemberVoter},
		{Group: group, ReplicaSetVersion: 9, MemberID: 4, Node: rafttransport.NodeID{4}, Role: rafttransport.MemberEnrolled},
	}
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{6}, MetadataEpoch: 7, CatalogGeneration: 8,
		InitialReplicaSetVersion: 9, InitialVoters: [3]uint64{1, 2, 3},
		InitialDescriptorDigest: [32]byte{10}, SourceMember: 1, TargetMember: 4,
		TargetNode: [16]byte(members[3].Node),
	}
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 9,
		[3]membershipgrant.RosterMember{
			{Member: 1, Node: [16]byte(members[0].Node)},
			{Member: 2, Node: [16]byte(members[1].Node)},
			{Member: 3, Node: [16]byte(members[2].Node)},
		})
	return group, grant, members
}

func membershipGrantConfChangeMessage(
	t *testing.T, grant membershipgrant.Grant, from, to uint64,
) *pb.Message {
	t.Helper()
	member := grant.TargetMember
	digest := grant.Digest()
	change := &pb.ConfChange{Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: &member,
		Context: append([]byte(nil), digest[:]...)}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(change)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Message{Type: pb.MsgApp.Enum(), From: &from, To: &to,
		Term: proto.Uint64(1), LogTerm: proto.Uint64(1), Index: proto.Uint64(8), Commit: proto.Uint64(8),
		Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Term: proto.Uint64(1),
			Index: proto.Uint64(9), Data: data}}}
}
