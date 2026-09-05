package rafttransport

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type disappearingGrantSource struct {
	grant membershipgrant.Grant
	reads int
}

func TestRestartedLearnerAcceptsCertifiedConfigurationReplay(t *testing.T) {
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
	appendMessage := authorizedConfigurationMessage(t, group, pb.ConfChangeAddLearnerNode, 3, 1, 3)
	appendMessage.Index = proto.Uint64(5)
	appendMessage.Entries[0].Index = proto.Uint64(6)
	frame, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: appendMessage,
	})
	if err != nil {
		t.Fatalf("restarted sender rejected committed configuration replay: %v", err)
	}
	if _, err := learner.DecodeInbound(testPeerIdentity(learner, testNode(1)), frame); err != nil {
		t.Fatalf("restarted learner rejected committed configuration replay: %v", err)
	}
	appendMessage.Index = proto.Uint64(6)
	appendMessage.Entries[0].Index = proto.Uint64(7)
	if _, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: appendMessage,
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("future learner re-add accepted as historical replay: %v", err)
	}
	appendMessage.Index = proto.Uint64(5)
	appendMessage.Entries[0].Index = proto.Uint64(6)
	appendMessage.Entries[0].Data[len(appendMessage.Entries[0].Data)-1] ^= 1
	if _, _, err := leader.EncodeOutbound(nil, raftmember.OutboundMessage{
		Group: group, From: 1, To: 3, Message: appendMessage,
	}); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("foreign historical grant accepted: %v", err)
	}
}

func (source *disappearingGrantSource) ReadMembershipGrant(
	context.Context, raftmember.GroupKey,
) (membershipgrant.Grant, bool, error) {
	source.reads++
	if source.reads == 1 {
		return source.grant, true, nil
	}
	return membershipgrant.Grant{}, false, nil
}

func TestCommittedAuthoritySeparatesEnrollmentAndBoundsAdjacentGenerations(t *testing.T) {
	group := testGroup(31)
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
	learner := &pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}
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
	learnerFrame := frameTestEncode(t, leader, group, frameTestMessage(pb.MsgAppResp, 1, 2))
	voters := &pb.ConfState{Voters: []uint64{1, 2, 3, 4}}
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
	preRemovalRemaining := frameTestEncode(t, follower, group, frameTestMessage(pb.MsgAppResp, 2, 3))
	removed := &pb.ConfState{Voters: []uint64{2, 3, 4}}
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

func TestTransitionGrantExactCASConcurrentRefreshAndTerminalRevoke(t *testing.T) {
	group := testGroup(91)
	members := []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(testNode(1), members, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	grant := authorityTestGrant(group)
	forged := grant
	forged.TargetMember = 5
	if err = registry.InstallTransitionGrant(forged); !errors.Is(err, ErrMemberNotFound) {
		t.Fatalf("unenrolled target install=%v", err)
	}
	forgedRoster := grant
	forgedRoster.InitialRosterDigest[0] ^= 0xff
	if err = registry.InstallTransitionGrant(forgedRoster); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("forged nonzero roster digest install=%v", err)
	}
	forgedTargetNode := grant
	forgedTargetNode.TargetNode = [16]byte(testNode(9))
	if err = registry.InstallTransitionGrant(forgedTargetNode); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("mismatched target enrollment install=%v", err)
	}
	mismatchedMembers := append([]Member(nil), members...)
	mismatchedMembers[2].Node = testNode(9)
	mismatchedPeer, peerErr := NewStaticRegistry(testNode(1), mismatchedMembers,
		Limits{MaxGroups: 1, MaxMembers: 4})
	if peerErr != nil {
		t.Fatal(peerErr)
	}
	if peerErr = mismatchedPeer.InstallTransitionGrant(grant); !errors.Is(peerErr, ErrReplicaSet) {
		t.Fatalf("peer with different target mapping install=%v", peerErr)
	}

	var wait sync.WaitGroup
	errorsByWorker := make(chan error, 64)
	for worker := 0; worker < cap(errorsByWorker); worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsByWorker <- registry.InstallTransitionGrant(grant)
		}()
	}
	wait.Wait()
	close(errorsByWorker)
	for installErr := range errorsByWorker {
		if installErr != nil {
			t.Fatalf("same-grant concurrent refresh=%v", installErr)
		}
	}
	current, found, err := registry.CurrentTransitionGrant(group)
	if err != nil || !found || current != grant {
		t.Fatalf("current=%+v found=%t err=%v", current, found, err)
	}
	stale := grant
	stale.MetadataEpoch++
	if err = registry.InstallTransitionGrant(stale); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("live grant replacement=%v", err)
	}
	if err = registry.RevokeTransitionGrant(grant); err != nil {
		t.Fatalf("untouched rollback revoke=%v", err)
	}
	if err = registry.InstallTransitionGrant(grant); err != nil {
		t.Fatalf("reinstall after rollback=%v", err)
	}
	if err = registry.PublishCommittedAuthority(group, 6,
		&pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}); err != nil {
		t.Fatal(err)
	}
	if err = registry.RevokeTransitionGrant(grant); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("intermediate learner revoke=%v", err)
	}
	if err = registry.PublishCommittedAuthority(group, 7,
		&pb.ConfState{Voters: []uint64{1, 2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	if err = registry.RevokeTransitionGrant(grant); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("intermediate RF4 revoke=%v", err)
	}
	if err = registry.PublishCommittedAuthority(group, 8,
		&pb.ConfState{Voters: []uint64{2, 3, 4}}); err != nil {
		t.Fatal(err)
	}
	if err = registry.RevokeTransitionGrant(stale); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("foreign terminal revoke=%v", err)
	}
	if err = registry.RevokeTransitionGrant(grant); err != nil {
		t.Fatalf("terminal revoke=%v", err)
	}
	if err = registry.RevokeTransitionGrant(grant); err != nil {
		t.Fatalf("terminal revoke retry=%v", err)
	}
	if err = registry.RevokeTransitionGrant(stale); !errors.Is(err, ErrReplicaSet) {
		t.Fatalf("foreign revoked retry=%v", err)
	}
	if current, found, err = registry.CurrentTransitionGrant(group); err != nil || found || current != (membershipgrant.Grant{}) {
		t.Fatalf("revoked current=%+v found=%t err=%v", current, found, err)
	}
}

func TestTransitionGrantRestartAcceptsOnlyExactLifecycleCuts(t *testing.T) {
	group := testGroup(94)
	grant := authorityTestGrant(group)
	tests := []struct {
		name        string
		version     uint64
		roles       [5]MemberRole
		wantInstall bool
		wantRevoke  bool
	}{
		{name: "initial-rf3", version: 5,
			roles:       [5]MemberRole{MemberVoter, MemberVoter, MemberEnrolled, MemberVoter, MemberEnrolled},
			wantInstall: true, wantRevoke: true},
		{name: "target-learner", version: 6,
			roles:       [5]MemberRole{MemberVoter, MemberVoter, MemberLearner, MemberVoter, MemberEnrolled},
			wantInstall: true},
		{name: "promoted-rf4", version: 7,
			roles:       [5]MemberRole{MemberVoter, MemberVoter, MemberVoter, MemberVoter, MemberEnrolled},
			wantInstall: true},
		{name: "completed-rf3", version: 8,
			roles:       [5]MemberRole{MemberEnrolled, MemberVoter, MemberVoter, MemberVoter, MemberEnrolled},
			wantInstall: true, wantRevoke: true},
		{name: "unrelated-later-rf3", version: 9,
			roles: [5]MemberRole{MemberVoter, MemberEnrolled, MemberVoter, MemberEnrolled, MemberVoter}},
		{name: "unrelated-later-completed-shape", version: 9,
			roles: [5]MemberRole{MemberEnrolled, MemberVoter, MemberVoter, MemberEnrolled, MemberVoter}},
		{name: "progressed-extra-voter", version: 9,
			roles: [5]MemberRole{MemberVoter, MemberVoter, MemberLearner, MemberVoter, MemberVoter}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			members := make([]Member, 5)
			for index := range members {
				members[index] = Member{Group: group, ReplicaSetVersion: test.version,
					MemberID: uint64(index + 1), Node: testNode(byte(index + 1)), Role: test.roles[index]}
			}
			registry, err := NewStaticRegistry(testNode(1), members,
				Limits{MaxGroups: 1, MaxMembers: len(members)})
			if err != nil {
				t.Fatal(err)
			}
			err = registry.InstallTransitionGrant(grant)
			if !test.wantInstall {
				if !errors.Is(err, ErrReplicaSet) {
					t.Fatalf("unrelated restart install=%v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("legal restart install=%v", err)
			}
			err = registry.RevokeTransitionGrant(grant)
			if test.wantRevoke {
				if err != nil {
					t.Fatalf("terminal restart revoke=%v", err)
				}
			} else if !errors.Is(err, ErrReplicaSet) {
				t.Fatalf("intermediate restart revoke=%v", err)
			}
		})
	}
}

func TestTransitionGrantRefreshRollsBackDisappearedUntouchedAuthority(t *testing.T) {
	group := testGroup(93)
	registry, err := NewStaticRegistry(testNode(1), []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	grant := authorityTestGrant(group)
	source := &disappearingGrantSource{grant: grant}
	if _, err = membershipgrant.Refresh(context.Background(), source, registry, group); !errors.Is(err, membershipgrant.ErrRefreshConflict) {
		t.Fatalf("disappeared refresh=%v", err)
	}
	if current, found, lookupErr := registry.CurrentTransitionGrant(group); lookupErr != nil || found || current != (membershipgrant.Grant{}) {
		t.Fatalf("rollback current=%+v found=%t err=%v", current, found, lookupErr)
	}
}

func TestCurrentTransitionGrantWarmLookupAllocationFree(t *testing.T) {
	group := testGroup(92)
	registry, err := NewStaticRegistry(testNode(1), []Member{
		{Group: group, ReplicaSetVersion: 5, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 5, MemberID: 3, Node: testNode(3), Role: MemberEnrolled},
		{Group: group, ReplicaSetVersion: 5, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}, Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	grant := authorityTestGrant(group)
	if err = registry.InstallTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(1000, func() {
		got, found, lookupErr := registry.CurrentTransitionGrant(group)
		if lookupErr != nil || !found || got != grant {
			panic("transition grant lookup")
		}
	})
	if allocations != 0 {
		t.Fatalf("warm transition grant lookup allocations=%.1f", allocations)
	}
}

func TestLaggingAuthorityAcceptsOnlyExactGrantedAdjacentConfiguration(t *testing.T) {
	group := testGroup(32)
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
	sender, lagging := open(testNode(1)), open(testNode(3))
	learner := &pb.ConfState{Voters: []uint64{1, 2, 4}, Learners: []uint64{3}}
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
		{Group: group, ReplicaSetVersion: 6, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	open := func(local NodeID) *StaticRegistry {
		registry, err := NewStaticRegistry(local, members, Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		grant := authorityTestGrant(group)
		if err = registry.InstallTransitionGrant(grant); err != nil {
			t.Fatal(err)
		}
		proof := raftmember.DurablePromotionProof{Version: 8, TargetMember: 3,
			AuthorizationDigest: grant.Digest()}
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
		{Group: group, ReplicaSetVersion: 6, MemberID: 4, Node: testNode(4), Role: MemberVoter},
	}
	registry, err := NewStaticRegistry(testNode(3), members,
		Limits{MaxGroups: 1, MaxMembers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.InstallTransitionGrant(authorityTestGrant(group)); err != nil {
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
	digest := authorityTestGrant(group).Digest()
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

func authorityTestGrant(group raftmember.GroupKey) membershipgrant.Grant {
	grant := membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{1}, MetadataEpoch: 7, CatalogGeneration: 9,
		InitialReplicaSetVersion: 5, InitialVoters: [3]uint64{1, 2, 4},
		InitialDescriptorDigest: [32]byte{2},
		SourceMember:            1, TargetMember: 3, TargetNode: [16]byte(testNode(3)),
	}
	grant.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 5,
		[3]membershipgrant.RosterMember{
			{Member: 1, Node: [16]byte(testNode(1))},
			{Member: 2, Node: [16]byte(testNode(2))},
			{Member: 4, Node: [16]byte(testNode(4))},
		})
	return grant
}
