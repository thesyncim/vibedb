package rafttransport

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestCommittedAuthoritySeparatesEnrollmentAndBoundsAdjacentGenerations(t *testing.T) {
	group := testGroup(31)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 3})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AuthorizeTransition(TransitionGrant{Group: group,
			TransitionID: [16]byte{1}, MetadataEpoch: 7, CatalogGeneration: 9,
			SourceMember: 1, TargetMember: 3}); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, follower, target := open(testNode(1)), open(testNode(2)), open(testNode(3))
	if _, err := target.Role(group, 3); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("enrollment granted a role: %v", err)
	}
	add := authorizedConfigurationMessage(t, pb.ConfChangeAddLearnerNode, 3, 1, 3)
	frame, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: add})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(1)), frame); err != nil {
		t.Fatalf("authorized learner append: %v", err)
	}
	initialFrame := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 2))
	learner := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	for _, registry := range []*StaticRegistry{leader, follower, target} {
		if err := registry.PublishCommittedAuthority(group, 6, learner); err != nil {
			t.Fatal(err)
		}
	}
	if view, _ := target.currentAuthority(group); view.previous == nil || view.previous.previous != nil {
		t.Fatal("authority retained more than one adjacent generation")
	}
	if role, err := target.Role(group, 3); err != nil || role != MemberLearner {
		t.Fatalf("learner role=%d err=%v", role, err)
	}
	// Exactly one prior view remains during additive convergence.
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(1)), initialFrame); err != nil {
		t.Fatalf("adjacent prior generation: %v", err)
	}
	learnerFrame := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 2))
	voters := &pb.ConfState{Voters: []uint64{1, 2, 3}}
	for _, registry := range []*StaticRegistry{leader, follower, target} {
		if err := registry.PublishCommittedAuthority(group, 7, voters); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(1)), initialFrame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("two-generations-old frame = %v", err)
	}
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(1)), learnerFrame); err != nil {
		t.Fatalf("promotion-adjacent frame: %v", err)
	}
	preRemovalSource := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 3))
	preRemovalVote := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgVote, 1, 3))
	removed := &pb.ConfState{Voters: []uint64{2, 3}}
	for _, registry := range []*StaticRegistry{leader, follower, target} {
		if err := registry.PublishCommittedAuthority(group, 8, removed); err != nil {
			t.Fatal(err)
		}
	}
	if view, _ := target.currentAuthority(group); view.allowPrevious || view.previous != nil {
		t.Fatal("source removal retained adjacent old-generation authority")
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(1)), preRemovalSource); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("removed-source frame = %v", err)
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(1)), preRemovalVote); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("removed-source vote = %v", err)
	}
	if _, err := target.Role(group, 1); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("removed source retained role: %v", err)
	}
}

func TestLaggingAuthorityAcceptsOnlyExactGrantedAdjacentConfiguration(t *testing.T) {
	group := testGroup(32)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 3})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AuthorizeTransition(TransitionGrant{Group: group,
			TransitionID: [16]byte{1}, MetadataEpoch: 7, CatalogGeneration: 9,
			SourceMember: 1, TargetMember: 3}); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	sender, lagging := open(testNode(1)), open(testNode(3))
	learner := &pb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3}}
	if err := sender.PublishCommittedAuthority(group, 8, learner); err != nil {
		t.Fatal(err)
	}
	add := authorizedConfigurationMessage(t, pb.ConfChangeAddLearnerNode, 3, 1, 3)
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: add})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lagging.DecodeInbound(testPeerIdentity(lagging, testNode(1)), frame); err != nil {
		t.Fatalf("exact adjacent configuration = %v", err)
	}
	wrong := authorizedConfigurationMessage(t, pb.ConfChangeAddNode, 3, 1, 3)
	wrongFrame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: wrong})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lagging.DecodeInbound(testPeerIdentity(lagging, testNode(1)), wrongFrame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong adjacent configuration = %v", err)
	}
}

func authorizedConfigurationMessage(
	t testing.TB,
	kind pb.ConfChangeType,
	member, from, to uint64,
) *pb.Message {
	t.Helper()
	change := &pb.ConfChange{Type: kind.Enum(), NodeId: &member}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(change)
	if err != nil {
		t.Fatal(err)
	}
	message := frameTestMessage(pb.MsgApp, from, to)
	message.Entries[0].Type = pb.EntryConfChange.Enum()
	message.Entries[0].Data = data
	return message
}
