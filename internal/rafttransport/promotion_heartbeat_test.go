package rafttransport

import (
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestPromotionHeartbeatReachesLearnerAfterLostAppendOrCommit(t *testing.T) {
	group := testGroup(34)
	members := []Member{
		{Group: group, ReplicaSetVersion: 6, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 6, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 6, MemberID: 3, Node: testNode(3), Role: MemberLearner},
		{Group: group, ReplicaSetVersion: 6, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.InstallTransitionGrant(authorityTestGrant(group)); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, learner := open(testNode(1)), open(testNode(3))
	promoted := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
	if err := leader.PublishCommittedAuthority(group, 8, promoted); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []uint64{6, 8, 9} {
		// The learner may have missed the append, only its commit, or later
		// commit notifications. None of those receipts proves local apply.
		heartbeat := frameTestMessage(pb.MsgHeartbeat, 1, 3)
		heartbeat.Commit = frameU64(commit)
		frame := frameTestEncode(t, leader, group, heartbeat)
		if _, err := learner.DecodeInbound(testPeerIdentity(learner, testNode(1)), frame); err != nil {
			t.Fatalf("learner could not receive catch-up heartbeat commit=%d: %v", commit, err)
		}
		response := frameTestEncode(t, learner, group, frameTestMessage(pb.MsgHeartbeatResp, 3, 1))
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(3)), response); err != nil {
			t.Fatalf("leader could not receive learner heartbeat response: %v", err)
		}
	}
	// Promotion catch-up grants no vote before the target applies promotion.
	vote := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgVote, 1, 3))
	if _, err := learner.DecodeInbound(testPeerIdentity(learner, testNode(1)), vote); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("learner accepted uncertified voting authority: %v", err)
	}
	if err := learner.PublishCommittedAuthority(group, 8, promoted); err != nil {
		t.Fatal(err)
	}
	heartbeat := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 3))
	if _, err := learner.DecodeInbound(testPeerIdentity(learner, testNode(1)), heartbeat); err != nil {
		t.Fatalf("promoted target rejected adjacent heartbeat: %v", err)
	}
	if err := learner.PublishCommittedAuthority(group, 11, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	if _, err := learner.DecodeInbound(testPeerIdentity(learner, testNode(1)), heartbeat); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("removed source retained heartbeat authority: %v", err)
	}
}
