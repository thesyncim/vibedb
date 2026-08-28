package gateway

import (
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

const durableRequestGroupBytes = 72

func appendDurableRequestGroup(dst []byte, group raftmember.GroupKey) {
	copy(dst[:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}

func openDurableRequestGroup(raw []byte) raftmember.GroupKey {
	var group raftmember.GroupKey
	copy(group.ClusterID[:], raw[:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return group
}
