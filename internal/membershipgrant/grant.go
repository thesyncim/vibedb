// Package membershipgrant defines the sole bounded authority exchanged between
// the replicated catalog and one RF3 runtime during a replica replacement.
package membershipgrant

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// Grant authorizes exactly one adjacent membership lifecycle for one group.
// TargetNode binds the selected absent member to the same immutable enrollment
// on every peer; the grant cannot add or widen stable node enrollment.
type Grant struct {
	Group             raftmember.GroupKey
	TransitionID      [16]byte
	MetadataEpoch     uint64
	CatalogGeneration uint64
	// InitialReplicaSetVersion and InitialVoters are the exact RF3 catalog
	// cut on which this transition was first authorized. They distinguish a
	// progressed restart from a fresh grant aimed at an already-present voter.
	InitialReplicaSetVersion uint64
	InitialVoters            [3]uint64
	InitialRosterDigest      [sha256.Size]byte
	InitialDescriptorDigest  [sha256.Size]byte
	SourceMember             uint64
	TargetMember             uint64
	TargetNode               [16]byte
}

func (grant Grant) Valid() bool {
	return grant.Group.ClusterID != ([16]byte{}) &&
		grant.Group.ClusterIncarnation != ([16]byte{}) &&
		grant.Group.TopologyRecoveryEpoch != 0 &&
		grant.Group.ShardIncarnation != ([16]byte{}) &&
		grant.Group.GroupID != ([16]byte{}) &&
		grant.TransitionID != ([16]byte{}) && grant.MetadataEpoch != 0 &&
		grant.CatalogGeneration != 0 && grant.InitialReplicaSetVersion != 0 &&
		grant.InitialRosterDigest != ([sha256.Size]byte{}) &&
		grant.InitialDescriptorDigest != ([sha256.Size]byte{}) &&
		validInitialVoters(grant.InitialVoters, grant.SourceMember, grant.TargetMember) &&
		grant.SourceMember != 0 && grant.TargetMember != 0 &&
		grant.SourceMember != grant.TargetMember && grant.TargetNode != ([16]byte{})
}

func (grant Grant) Digest() [sha256.Size]byte {
	if !grant.Valid() {
		return [sha256.Size]byte{}
	}
	base := raftmember.MembershipTransitionDigest(
		grant.Group, grant.TransitionID, grant.MetadataEpoch,
		grant.CatalogGeneration, grant.SourceMember, grant.TargetMember,
	)
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/membership-grant/certified-rf3\x00"))
	_, _ = hash.Write(base[:])
	var encoded [8 + 3*8 + 2*sha256.Size + 16]byte
	binary.LittleEndian.PutUint64(encoded[:8], grant.InitialReplicaSetVersion)
	for index := range grant.InitialVoters {
		binary.LittleEndian.PutUint64(encoded[8+index*8:], grant.InitialVoters[index])
	}
	copy(encoded[8+3*8:], grant.InitialRosterDigest[:])
	copy(encoded[8+3*8+sha256.Size:], grant.InitialDescriptorDigest[:])
	copy(encoded[8+3*8+2*sha256.Size:], grant.TargetNode[:])
	_, _ = hash.Write(encoded[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

type RosterMember struct {
	Member uint64
	Node   [16]byte
}

// CertifiedRosterDigest is the shared catalog/runtime commitment to the exact
// stable identities of the initial RF3 voters. Callers supply ascending member
// IDs; the fixed representation is allocation-free and has one canonical form.
func CertifiedRosterDigest(
	group raftmember.GroupKey, version uint64, voters [3]RosterMember,
) [sha256.Size]byte {
	if !validRosterGroup(group) || version == 0 || !validInitialVoters(
		[3]uint64{voters[0].Member, voters[1].Member, voters[2].Member},
		voters[0].Member, 0,
	) || voters[0].Node == ([16]byte{}) || voters[1].Node == ([16]byte{}) ||
		voters[2].Node == ([16]byte{}) || voters[0].Node == voters[1].Node ||
		voters[0].Node == voters[2].Node || voters[1].Node == voters[2].Node {
		return [sha256.Size]byte{}
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/membership-grant/stable-initial-rf3\x00"))
	_, _ = hash.Write(group.ClusterID[:])
	_, _ = hash.Write(group.ClusterIncarnation[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], group.TopologyRecoveryEpoch)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(group.ShardIncarnation[:])
	_, _ = hash.Write(group.GroupID[:])
	binary.LittleEndian.PutUint64(scalar[:], version)
	_, _ = hash.Write(scalar[:])
	for _, voter := range voters {
		binary.LittleEndian.PutUint64(scalar[:], voter.Member)
		_, _ = hash.Write(scalar[:])
		_, _ = hash.Write(voter.Node[:])
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func validRosterGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func validInitialVoters(voters [3]uint64, source, target uint64) bool {
	return voters[0] != 0 && voters[0] < voters[1] && voters[1] < voters[2] &&
		(voters[0] == source || voters[1] == source || voters[2] == source) &&
		voters[0] != target && voters[1] != target && voters[2] != target
}

// Source returns a linearizable replicated-catalog grant lookup. Missing is a
// first-class state rather than a zero Grant so revocation can be proved.
type Source interface {
	ReadMembershipGrant(context.Context, raftmember.GroupKey) (Grant, bool, error)
}

// Sink is the exact CAS boundary owned by a runtime identity registry.
type Sink interface {
	CurrentTransitionGrant(raftmember.GroupKey) (Grant, bool, error)
	InstallTransitionGrant(Grant) error
	RevokeTransitionGrant(Grant) error
}
