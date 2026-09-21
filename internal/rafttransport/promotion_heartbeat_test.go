package rafttransport

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"

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
	if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(3)), vote); err != nil {
		t.Fatalf("current-voter election rejected: %v", err)
	}
	if err := follower.PublishCommittedAuthority(group, 11, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	frame = frameTestEncode(t, leader, group, heartbeat)
	if _, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(2)), frame); err != nil {
		t.Fatalf("converged survivor heartbeat rejected: %v", err)
	}

}

func TestRemovalHeartbeatSurvivesLeaderRestart(t *testing.T) {
	group := testGroup(117)
	grant := authorityTestGrant(group)
	open := func(local byte, version uint64, removed bool) *StaticRegistry {
		members := make([]Member, 4)
		for i := range members {
			role := MemberVoter
			if removed && i == 0 {
				role = MemberEnrolled
			}
			members[i] = Member{Group: group, ReplicaSetVersion: version, MemberID: uint64(i + 1), Node: testNode(byte(i + 1)), Role: role}
		}
		registry, err := NewStaticRegistry(testNode(local), members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if err = registry.InstallTransitionGrant(grant); err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader, survivor, removed := open(2, 420, true), open(3, 372, false), open(1, 372, false)
	if err := leader.PublishCommittedAuthority(group, 420, &pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	heartbeat := frameTestMessage(pb.MsgHeartbeat, 2, 3)
	heartbeat.Commit = frameU64(420)
	frame := frameTestEncode(t, leader, group, heartbeat)
	if _, err := survivor.DecodeInbound(testPeerIdentity(survivor, testNode(2)), frame); err != nil {
		t.Fatalf("cold leader cannot deliver retained removal commit: %v", err)
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeatResp, pb.MsgAppResp} {
		response := frameTestEncode(t, survivor, group, frameTestMessage(kind, 3, 2))
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(3)), response); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeatResp, pb.MsgAppResp, pb.MsgVote} {
		frame := frameTestEncode(t, removed, group, frameTestMessage(kind, 1, 2))
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(1)), frame); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("cold leader admitted removed member: %v", err)
		}
	}
	for _, kind := range []pb.MessageType{pb.MsgApp, pb.MsgVote} {
		frame := frameTestEncode(t, survivor, group, frameTestMessage(kind, 3, 2))
		if _, err := leader.DecodeInbound(testPeerIdentity(leader, testNode(3)), frame); err != nil {
			t.Fatalf("current voter catch-up rejected: %v", err)
		}
	}
}

// A restarted leader has durable learner membership but no process-local
// previous authority. Its survivor may have the AddLearner entry without its
// commit; reconnect must still deliver the heartbeat which commits that entry.
func TestAddLearnerLostCommitSurvivesLeaderRestart(t *testing.T) {
	group := testGroup(116)
	grant := authorityTestGrant(group)
	grant.InitialReplicaSetVersion = 1
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 1, [3]membershipgrant.RosterMember{
		{Member: 1, Node: [16]byte(testNode(1))}, {Member: 2, Node: [16]byte(testNode(2))}, {Member: 4, Node: [16]byte(testNode(4))},
	})
	open := func(local byte, version uint64, targetRole MemberRole, installedGrant *membershipgrant.Grant) *StaticRegistry {
		members := []Member{
			{Group: group, ReplicaSetVersion: version, MemberID: 1, Node: testNode(1), Role: MemberVoter},
			{Group: group, ReplicaSetVersion: version, MemberID: 2, Node: testNode(2), Role: MemberVoter},
			{Group: group, ReplicaSetVersion: version, MemberID: 3, Node: testNode(3), Role: targetRole},
			{Group: group, ReplicaSetVersion: version, MemberID: 4, Node: testNode(4), Role: MemberVoter},
		}
		r, err := NewStaticRegistry(testNode(local), members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		if installedGrant != nil {
			if err = r.InstallTransitionGrant(*installedGrant); err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	leader, follower := open(2, 6377, MemberLearner, &grant), open(1, 1, MemberEnrolled, &grant)
	heartbeat := frameTestMessage(pb.MsgHeartbeat, 2, 1)
	heartbeat.Commit = frameU64(6377)
	frame := frameTestEncode(t, leader, group, heartbeat)
	inbound, err := follower.DecodeInbound(testPeerIdentity(follower, testNode(2)), frame)
	if err != nil || inbound.Message.GetCommit() != 6377 {
		t.Fatalf("lost AddLearner commit cannot catch up: %v", err)
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgAppResp} {
		response := frameTestEncode(t, follower, group, frameTestMessage(kind, 1, 2))
		if _, err = leader.DecodeInbound(testPeerIdentity(leader, testNode(1)), response); err != nil {
			t.Fatalf("restarted leader rejected survivor %v: %v", kind, err)
		}
	}
	for _, kind := range []pb.MessageType{pb.MsgApp, pb.MsgVote, pb.MsgVoteResp, pb.MsgPreVote, pb.MsgPreVoteResp} {
		message := frameTestEncode(t, follower, group, frameTestMessage(kind, 1, 2))
		if _, err = leader.DecodeInbound(testPeerIdentity(leader, testNode(1)), message); err != nil {
			t.Fatalf("current voter traffic rejected %v: %v", kind, err)
		}
	}
	// Once the follower applies it, the same heartbeat must also work across
	// another cold restart without retaining a previous-view cache.
	follower = open(1, 6377, MemberLearner, &grant)
	if _, err = follower.DecodeInbound(testPeerIdentity(follower, testNode(2)), frame); err != nil {
		t.Fatalf("current follower rejected heartbeat: %v", err)
	}
	if _, err = follower.DecodeInbound(testPeerIdentity(follower, testNode(4)), frame); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong authenticated principal admitted: %v", err)
	}
	for _, target := range []struct {
		name     string
		registry *StaticRegistry
	}{
		{"missing-grant", open(1, 6377, MemberLearner, nil)},
		{"promoted", open(1, 6378, MemberVoter, &grant)},
	} {
		if _, err = target.registry.DecodeInbound(testPeerIdentity(target.registry, testNode(2)), frame); err != nil {
			t.Fatalf("%s current voter rejected: %v", target.name, err)
		}
	}
	otherGrant := grant
	otherGrant.InitialReplicaSetVersion = 2
	otherGrant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 2, [3]membershipgrant.RosterMember{
		{Member: 1, Node: [16]byte(testNode(1))}, {Member: 2, Node: [16]byte(testNode(2))}, {Member: 4, Node: [16]byte(testNode(4))},
	})
	other := open(1, 6377, MemberLearner, &otherGrant)
	if _, err = other.DecodeInbound(testPeerIdentity(other, testNode(2)), frame); err != nil {
		t.Fatalf("current voter depends on prior grant: %v", err)
	}
	// This transport-only path does not let the not-yet-applied target vote.
	vote := frameTestMessage(pb.MsgVote, 2, 3)
	if _, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{Group: group, From: 2, To: 3, Message: vote}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("learner gained voting authority: %v", err)
	}
}
