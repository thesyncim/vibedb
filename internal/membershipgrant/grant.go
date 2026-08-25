// Package membershipgrant defines the sole bounded authority exchanged between
// the replicated catalog and one RF3 runtime during a replica replacement.
package membershipgrant

import (
	"context"
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// Grant authorizes exactly one adjacent membership lifecycle for one group.
// Stable node enrollment remains a separate prerequisite and cannot be added
// or widened by this value.
type Grant struct {
	Group             raftmember.GroupKey
	TransitionID      [16]byte
	MetadataEpoch     uint64
	CatalogGeneration uint64
	SourceMember      uint64
	TargetMember      uint64
}

func (grant Grant) Valid() bool {
	return grant.Group.ClusterID != ([16]byte{}) &&
		grant.Group.ClusterIncarnation != ([16]byte{}) &&
		grant.Group.TopologyRecoveryEpoch != 0 &&
		grant.Group.ShardIncarnation != ([16]byte{}) &&
		grant.Group.GroupID != ([16]byte{}) &&
		grant.TransitionID != ([16]byte{}) && grant.MetadataEpoch != 0 &&
		grant.CatalogGeneration != 0 && grant.SourceMember != 0 &&
		grant.TargetMember != 0 && grant.SourceMember != grant.TargetMember
}

func (grant Grant) Digest() [sha256.Size]byte {
	if !grant.Valid() {
		return [sha256.Size]byte{}
	}
	return raftmember.MembershipTransitionDigest(
		grant.Group, grant.TransitionID, grant.MetadataEpoch,
		grant.CatalogGeneration, grant.SourceMember, grant.TargetMember,
	)
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
