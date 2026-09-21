package rafttransport

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReplicationParticipantRequiresExactActiveGrant(t *testing.T) {
	group := testGroup(122)
	grant := authorityTestGrant(group)
	open := func(local byte, version uint64, roles map[uint64]MemberRole, installed membershipgrant.Grant) *StaticRegistry {
		t.Helper()
		members := make([]Member, 5)
		for i := range members {
			member := uint64(i + 1)
			members[i] = Member{Group: group, ReplicaSetVersion: version, MemberID: member, Node: testNode(byte(member)), Role: roles[member]}
		}
		r, err := NewStaticRegistry(testNode(local), members, Limits{MaxGroups: 1, MaxMembers: 5})
		if err != nil {
			t.Fatal(err)
		}
		if installed != (membershipgrant.Grant{}) {
			if err := r.InstallTransitionGrant(installed); err != nil {
				t.Fatal(err)
			}
		}
		return r
	}
	initial := map[uint64]MemberRole{1: MemberVoter, 2: MemberVoter, 4: MemberVoter}
	promoted := map[uint64]MemberRole{1: MemberVoter, 2: MemberVoter, 3: MemberVoter, 4: MemberVoter}
	sender := open(3, 9, promoted, grant)
	frame := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgHeartbeat, 3, 2))
	current := open(2, 5, initial, grant)
	if _, err := current.DecodeInbound(testPeerIdentity(current, testNode(3)), frame); err != nil {
		t.Fatalf("exact active target rejected: %v", err)
	}
	if _, err := current.Role(group, 3); !errors.Is(err, ErrMemberNotFound) {
		t.Fatal("replication admission published a Raft role")
	}
	if _, err := current.DecodeInbound(testPeerIdentity(current, testNode(4)), frame); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("wrong principal admitted")
	}
	missing := open(2, 5, initial, membershipgrant.Grant{})
	revoked := open(2, 5, initial, grant)
	if err := revoked.RevokeTransitionGrant(grant); err != nil {
		t.Fatal(err)
	}
	other := grant
	other.TargetMember, other.TargetNode, other.CatalogGeneration = 5, [16]byte(testNode(5)), grant.CatalogGeneration+1
	other.TransitionID[0]++
	wrongTarget := open(2, 5, initial, other)
	removed := open(2, 11, map[uint64]MemberRole{2: MemberVoter, 4: MemberVoter, 5: MemberVoter}, other)
	for name, r := range map[string]*StaticRegistry{"missing-grant": missing, "revoked-grant": revoked, "different-target": wrongTarget, "removed-prior-target": removed} {
		t.Run(name, func(t *testing.T) {
			if _, err := r.DecodeInbound(testPeerIdentity(r, testNode(3)), frame); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("inactive target admitted: %v", err)
			}
		})
	}
	// The same exact installed grant grants neither a local campaign nor
	// leader-origin output before this runtime applies voter membership.
	localTarget := open(3, 5, initial, grant)
	for _, kind := range []pb.MessageType{pb.MsgApp, pb.MsgHeartbeat, pb.MsgVote, pb.MsgPreVote, pb.MsgVoteResp, pb.MsgTimeoutNow} {
		message := frameTestMessage(kind, 3, 2)
		if kind == pb.MsgTimeoutNow {
			message = frameTimeoutNow(3, 2, 5)
		}
		if _, _, err := localTarget.EncodeOutbound(nil, raftmember.OutboundMessage{Group: group, From: 3, To: 2, Message: message}); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("unapplied target emitted %s: %v", kind, err)
		}
	}
	vote := frameTestEncode(t, sender, group, frameTestMessage(pb.MsgVote, 3, 2))
	if _, err := current.DecodeInbound(testPeerIdentity(current, testNode(3)), vote); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("active grant alone authorized election: %v", err)
	}
}
