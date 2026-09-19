package rafttransport

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestSuccessorEnrollmentReusesRemovedPhysicalNode(t *testing.T) {
	group := testGroup(205)
	members := []Member{{Group: group, ReplicaSetVersion: 1, MemberID: 1, Node: testNode(1), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 2, Node: testNode(2), Role: MemberVoter},
		{Group: group, ReplicaSetVersion: 1, MemberID: 3, Node: testNode(3), Role: MemberVoter}}
	registry, err := NewStaticRegistry(testNode(2), members, Limits{MaxGroups: 1, MaxMembers: 4, MaxPeers: 4})
	if err != nil {
		t.Fatal(err)
	}
	first := replacementTestGrant(group, 1, 4, testNode(4), [3]uint64{1, 2, 3})
	digest, _ := StableRosterDigest(members)
	intent := dynamicPeerIntent(registry, testNode(4), 4, group, 4, digest)
	intent.Grant, intent.Digest = first, first.Digest()
	if err := registry.EnrollMember(intent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
		t.Fatal(err)
	}
	if err := registry.InstallTransitionGrant(first); err != nil {
		t.Fatal(err)
	}
	for i, conf := range []*pb.ConfState{{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}, {Voters: []uint64{1, 2, 3, 4}}, {Voters: []uint64{2, 3, 4}}} {
		if err := registry.PublishCommittedAuthority(group, uint64(i+2), conf); err != nil {
			t.Fatal(err)
		}
	}
	next := replacementTestGrant(group, 3, 5, testNode(1), [3]uint64{2, 3, 4})
	next.InitialReplicaSetVersion, next.CatalogGeneration = 4, first.CatalogGeneration+1
	var voters [3]membershipgrant.RosterMember
	for i, id := range next.InitialVoters {
		voters[i] = membershipgrant.RosterMember{Member: id, Node: [16]byte(testNode(byte(id)))}
		members[i] = Member{Group: group, ReplicaSetVersion: 4, MemberID: id, Node: testNode(byte(id)), Role: MemberVoter}
	}
	next.InitialRosterDigest = membershipgrant.CertifiedRosterDigest(group, 4, voters)
	digest, _ = StableRosterDigest(members)
	intent = dynamicPeerIntent(registry, testNode(1), 5, group, 5, digest)
	intent.Grant, intent.Digest, intent.Member.ReplicaSetVersion = next, next.Digest(), 4
	bad := intent
	bad.Grant.InitialRosterDigest[0]++
	bad.Digest = bad.Grant.Digest()
	if err := registry.EnrollMember(bad, EnrollmentVerifierFunc(allowEnrollment)); err == nil {
		t.Fatal("uncertified successor removed old mapping")
	}
	for retry := 0; retry < 2; retry++ {
		intent.DirectoryRevision = registry.PeerDirectoryRevision()
		if err := registry.EnrollMember(intent, EnrollmentVerifierFunc(allowEnrollment)); err != nil {
			t.Fatalf("successor retry %d: %v", retry, err)
		}
	}
	if _, err := registry.Node(group, 1); err == nil {
		t.Fatal("retired member mapping remains")
	}
	if member, err := registry.Member(group, testNode(1)); err != nil || member != 5 {
		t.Fatalf("returning node member=%d err=%v", member, err)
	}
	if err := registry.ReplaceTransitionGrantWithCommit(first, next, nil); err != nil {
		t.Fatal(err)
	}
	if registry.effectiveMemberCount(registry.dynamic.Load()) != 4 {
		t.Fatal("replacement grew live member inventory")
	}
}
