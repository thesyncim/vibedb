package membershipgrant

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestCertifiedRosterDigestBindsExactGroupVersionMembersAndNodes(t *testing.T) {
	grant := refreshTestGrant()
	voters := [3]RosterMember{
		{Member: 1, Node: [16]byte{1}},
		{Member: 2, Node: [16]byte{2}},
		{Member: 4, Node: [16]byte{4}},
	}
	baseline := CertifiedRosterDigest(grant.Group, grant.InitialReplicaSetVersion, voters)
	if baseline == ([32]byte{}) {
		t.Fatal("valid RF3 roster produced a zero commitment")
	}
	checks := []func(*raftmember.GroupKey, *uint64, *[3]RosterMember){
		func(group *raftmember.GroupKey, _ *uint64, _ *[3]RosterMember) { group.GroupID[0] ^= 1 },
		func(_ *raftmember.GroupKey, version *uint64, _ *[3]RosterMember) { *version++ },
		func(_ *raftmember.GroupKey, _ *uint64, voters *[3]RosterMember) { voters[1].Node[0] ^= 1 },
		func(_ *raftmember.GroupKey, _ *uint64, voters *[3]RosterMember) { voters[1].Member++ },
	}
	for index, mutate := range checks {
		group, version, candidate := grant.Group, grant.InitialReplicaSetVersion, voters
		mutate(&group, &version, &candidate)
		if digest := CertifiedRosterDigest(group, version, candidate); digest == baseline {
			t.Fatalf("mutation %d did not change/reject roster commitment", index)
		}
	}
	zeroGroup := raftmember.GroupKey{}
	if digest := CertifiedRosterDigest(zeroGroup, grant.InitialReplicaSetVersion, voters); digest != ([32]byte{}) {
		t.Fatalf("invalid zero group digest=%x", digest)
	}
	duplicateNode := voters
	duplicateNode[2].Node = duplicateNode[1].Node
	if digest := CertifiedRosterDigest(grant.Group, grant.InitialReplicaSetVersion, duplicateNode); digest != ([32]byte{}) {
		t.Fatalf("duplicate node digest=%x", digest)
	}
}

func TestGrantDigestBindsTargetEnrollment(t *testing.T) {
	grant := refreshTestGrant()
	baseline := grant.Digest()
	changed := grant
	changed.TargetNode[0] ^= 0xff
	if baseline == ([32]byte{}) || changed.Digest() == baseline {
		t.Fatal("target node did not change the transition authorization digest")
	}
	changed.TargetNode = [16]byte{}
	if changed.Valid() || changed.Digest() != ([32]byte{}) {
		t.Fatal("zero target node remained a valid transition grant")
	}
}
