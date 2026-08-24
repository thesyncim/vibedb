package raftmember

import (
	"crypto/sha256"
	"encoding/binary"
)

const MembershipTransitionDigestBytes = sha256.Size

// MembershipTransitionDigest binds a Raft ConfChange to one exact metadata
// grant without retaining variable-width control-plane data in the log.
func MembershipTransitionDigest(
	group GroupKey,
	transitionID [16]byte,
	metadataEpoch, catalogGeneration, sourceMember, targetMember uint64,
) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/raft-membership-transition\x00"))
	var fixed [120]byte
	copy(fixed[0:16], group.ClusterID[:])
	copy(fixed[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(fixed[32:40], group.TopologyRecoveryEpoch)
	copy(fixed[40:56], group.ShardIncarnation[:])
	copy(fixed[56:72], group.GroupID[:])
	copy(fixed[72:88], transitionID[:])
	binary.BigEndian.PutUint64(fixed[88:96], metadataEpoch)
	binary.BigEndian.PutUint64(fixed[96:104], catalogGeneration)
	binary.BigEndian.PutUint64(fixed[104:112], sourceMember)
	binary.BigEndian.PutUint64(fixed[112:120], targetMember)
	_, _ = hash.Write(fixed[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}
