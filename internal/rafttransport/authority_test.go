package rafttransport

import (
	"encoding/binary"
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
	add := authorizedConfigurationMessage(t, group, pb.ConfChangeAddLearnerNode, 3, 1, 3)
	frame, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: add})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(1)), frame); err != nil {
		t.Fatalf("authorized learner append: %v", err)
	}
	initialFrame := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 2))
	preLearnerIllegal := frameTestReplacePayload(t, initialFrame,
		frameTestMessage(pb.MsgHeartbeat, 3, 2))
	binary.BigEndian.PutUint64(preLearnerIllegal[frameTestFromOffset:frameTestToOffset], 3)
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
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(3)), preLearnerIllegal); !errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrRetiredAuthority) {
		t.Fatalf("old-generation learner-origin leader frame = %v", err)
	}
	learnerFrame := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 2))
	voters := &pb.ConfState{Voters: []uint64{1, 2, 3}}
	for _, registry := range []*StaticRegistry{leader, follower, target} {
		if err := registry.PublishCommittedAuthority(group, 7, voters); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(1)), initialFrame); !errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrRetiredAuthority) {
		t.Fatalf("two-generations-old frame = %v", err)
	}
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(1)), learnerFrame); err != nil {
		t.Fatalf("promotion-adjacent frame: %v", err)
	}
	preRemovalSource := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 3))
	preRemovalVote := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgVote, 1, 3))
	preRemovalRemaining := frameTestEncode(t, follower, group, frameTestMessage(pb.MsgHeartbeat, 2, 3))
	removed := &pb.ConfState{Voters: []uint64{2, 3}}
	for _, registry := range []*StaticRegistry{leader, follower, target} {
		// Normal entries may separate configuration entries, so authority
		// versions are monotonic log positions rather than consecutive counters.
		if err := registry.PublishCommittedAuthority(group, 11, removed); err != nil {
			t.Fatal(err)
		}
	}
	if view, _ := target.currentAuthority(group); view.allowPrevious || view.previous != nil ||
		view.retiredVersion != 7 {
		t.Fatal("source removal retained prior roles or lost exact retired version")
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(1)), preRemovalSource); !errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrRetiredAuthority) {
		t.Fatalf("removed-source frame = %v", err)
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(1)), preRemovalVote); !errors.Is(err, ErrUnauthorized) || errors.Is(err, ErrRetiredAuthority) {
		t.Fatalf("removed-source vote = %v", err)
	}
	if _, err := target.DecodeInbound(testPeerIdentity(target, testNode(2)), preRemovalRemaining); !errors.Is(err, ErrUnauthorized) || !errors.Is(err, ErrRetiredAuthority) {
		t.Fatalf("retained-member retired frame = %v", err)
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
	add := authorizedConfigurationMessage(t, group, pb.ConfChangeAddLearnerNode, 3, 1, 3)
	frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: add})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lagging.DecodeInbound(testPeerIdentity(lagging, testNode(1)), frame); err != nil {
		t.Fatalf("exact adjacent configuration = %v", err)
	}
	wrong := authorizedConfigurationMessage(t, group, pb.ConfChangeAddNode, 3, 1, 3)
	wrongFrame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: wrong})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lagging.DecodeInbound(testPeerIdentity(lagging, testNode(1)), wrongFrame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong adjacent configuration = %v", err)
	}
}

func TestDurablePromotionProofGrantsOnlyTargetElectionExchange(t *testing.T) {
	group := testGroup(33)
	members := []Member{
		{Group: group, ReplicaSetVersion: 6, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 6, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 6, MemberID: 3, Node: testNode(3), Role: MemberLearner},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 3})
		if err != nil {
			t.Fatal(err)
		}
		if err = registry.AuthorizeTransition(TransitionGrant{Group: group,
			TransitionID: [16]byte{1}, MetadataEpoch: 7, CatalogGeneration: 9,
			SourceMember: 1, TargetMember: 3}); err != nil {
			t.Fatal(err)
		}
		proof := raftmember.DurablePromotionProof{Version: 8, TargetMember: 3,
			AuthorizationDigest: raftmember.MembershipTransitionDigest(
				group, [16]byte{1}, 7, 9, 1, 3)}
		wrong := proof
		wrong.AuthorizationDigest[0] ^= 0xff
		if err = registry.PublishDurablePromotion(group, wrong); !errors.Is(err, ErrReplicaSet) {
			t.Fatalf("wrong grant digest = %v", err)
		}
		if err = registry.PublishDurablePromotion(group, proof); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	candidate, target := open(testNode(2)), open(testNode(3))
	vote := frameTestMessage(pb.MsgVote, 2, 3)
	frame, _, err := candidate.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 2, To: 3, Message: vote})
	if err != nil {
		t.Fatal(err)
	}
	header, _, err := parseFrame(frame)
	if err != nil || header.version != 8 {
		t.Fatalf("election frame version=%d err=%v", header.version, err)
	}
	if _, err = target.DecodeInbound(testPeerIdentity(target, testNode(2)), frame); err != nil {
		t.Fatalf("candidate vote request = %v", err)
	}
	response := frameTestMessage(pb.MsgVoteResp, 3, 2)
	responseFrame, _, err := target.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 3, To: 2, Message: response})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = candidate.DecodeInbound(testPeerIdentity(candidate, testNode(3)), responseFrame); err != nil {
		t.Fatalf("target vote response = %v", err)
	}
	for _, disallowed := range []*pb.Message{
		frameTestMessage(pb.MsgVote, 3, 2),
		frameTestMessage(pb.MsgVoteResp, 1, 3),
	} {
		from, to := disallowed.GetFrom(), disallowed.GetTo()
		local := candidate
		if from == 3 {
			local = target
		}
		if encoded, _, encodeErr := local.EncodeOutbound(nil, raftmember.OutboundMessage{
			Group: group, From: from, To: to, Message: disallowed,
		}); encodeErr == nil || encoded != nil || !errors.Is(encodeErr, ErrUnauthorized) {
			t.Fatalf("disallowed %s %d->%d frame=%x err=%v",
				disallowed.GetType(), from, to, encoded, encodeErr)
		}
	}
	heartbeat := frameTestEncode(t, candidate, group, frameTestMessage(pb.MsgHeartbeat, 2, 3))
	heartbeatHeader, _, err := parseFrame(heartbeat)
	if err != nil || heartbeatHeader.version != 6 {
		t.Fatalf("ordinary learner heartbeat version=%d err=%v", heartbeatHeader.version, err)
	}
	if err = candidate.ClearDurablePromotion(group); err != nil {
		t.Fatal(err)
	}
	if revoked, _, revokeErr := candidate.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 2, To: 3, Message: vote,
	}); revokeErr == nil || revoked != nil || !errors.Is(revokeErr, ErrUnauthorized) {
		t.Fatalf("revoked proof frame=%x err=%v", revoked, revokeErr)
	}
}

func TestLearnerWithoutCommittedPromotionWitnessCannotVote(t *testing.T) {
	group := testGroup(34)
	members := []Member{
		{Group: group, ReplicaSetVersion: 6, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 6, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 6, MemberID: 3, Node: testNode(3), Role: MemberLearner},
	}
	registry, err := NewStaticRegistry(testNode(3), members,
		Limits{MaxGroups: 1, MaxMembers: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AuthorizeTransition(TransitionGrant{Group: group,
		TransitionID: [16]byte{1}, MetadataEpoch: 7, CatalogGeneration: 9,
		SourceMember: 1, TargetMember: 3}); err != nil {
		t.Fatal(err)
	}
	response := frameTestMessage(pb.MsgVoteResp, 3, 2)
	frame, _, err := registry.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 3, To: 2, Message: response})
	if err == nil || frame != nil || !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unwitnessed learner vote frame=%x err=%v", frame, err)
	}
}

func authorizedConfigurationMessage(
	t testing.TB,
	group raftmember.GroupKey,
	kind pb.ConfChangeType,
	member, from, to uint64,
) *pb.Message {
	t.Helper()
	digest := raftmember.MembershipTransitionDigest(group, [16]byte{1}, 7, 9, 1, 3)
	change := &pb.ConfChange{Type: kind.Enum(), NodeId: &member,
		Context: append([]byte(nil), digest[:]...)}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(change)
	if err != nil {
		t.Fatal(err)
	}
	message := frameTestMessage(pb.MsgApp, from, to)
	message.Entries[0].Type = pb.EntryConfChange.Enum()
	message.Entries[0].Data = data
	return message
}
