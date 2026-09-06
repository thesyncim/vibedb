package rafttransport

import (
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestAdditiveMembershipHeartbeatReachesLaggingVoter(t *testing.T) {
	group := testGroup(34)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
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
	leader, follower := open(testNode(1)), open(testNode(2))
	for _, change := range []struct {
		version uint64
		conf    *pb.ConfState
	}{
		{6, &pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}},
		{8, &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}},
	} {
		if err := leader.PublishCommittedAuthority(group, change.version, change.conf); err != nil {
			t.Fatal(err)
		}
		heartbeat := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgHeartbeat, 1, 2))
		if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(1)), heartbeat); err != nil {
			t.Fatalf("existing voter cannot catch up to membership %d: %v", change.version, err)
		}
		response := frameTestEncode(t, follower, group, frameTestMessage(pb.MsgHeartbeatResp, 2, 1))
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(2)), response); err != nil {
			t.Fatalf("leader rejected catch-up response: %v", err)
		}
		if err := follower.PublishCommittedAuthority(group, change.version, change.conf); err != nil {
			t.Fatal(err)
		}
	}
}

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

func TestRemovalHeartbeatReachesSurvivingVoterAfterLostCommit(t *testing.T) {
	group := testGroup(34)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err = registry.InstallTransitionGrant(authorityTestGrant(group)); err != nil {
			t.Fatal(err)
		}
		if err = registry.PublishCommittedAuthority(group, 6, &pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}); err != nil {
			t.Fatal(err)
		}
		if err = registry.PublishCommittedAuthority(group, 8, &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, follower, removed := open(testNode(2)), open(testNode(3)), open(testNode(1))
	if err := leader.PublishCommittedAuthority(group, 11, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	heartbeat := frameTestMessage(pb.MsgHeartbeat, 2, 3)
	heartbeat.Commit = frameU64(11)
	frame := frameTestEncode(t, leader, group, heartbeat)
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(2)), frame); err != nil {
		t.Fatalf("survivor cannot learn removal commit: %v", err)
	}
	response := frameTestEncode(t, follower, group, frameTestMessage(pb.MsgHeartbeatResp, 3, 2))
	if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(3)), response); err != nil {
		t.Fatalf("survivor catch-up response rejected: %v", err)
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgAppResp} {
		frame := frameTestEncode(t, removed, group, frameTestMessage(kind, 1, 2))
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(1)), frame); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("removed source regained %v authority: %v", kind, err)
		}
	}
	vote := frameTestEncode(t, follower, group, frameTestMessage(pb.MsgVote, 3, 2))
	if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(3)), vote); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("old-view election admitted: %v", err)
	}
	if err := follower.PublishCommittedAuthority(group, 11, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	frame = frameTestEncode(t, leader, group, heartbeat)
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(2)), frame); err != nil {
		t.Fatalf("converged survivor heartbeat rejected: %v", err)
	}

}
