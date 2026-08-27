package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// rf3SnapshotGroupPath preserves the released singleton layout while making
// every multi-group source artifact namespace a deterministic function of the
// complete recovery and shard incarnation identity. No manifest-provided
// group byte is interpreted as a path component.
func rf3SnapshotGroupPath(root string, group raftmember.GroupKey, multiGroup bool) string {
	if !multiGroup {
		return root
	}
	var canonical [16 + 16 + 8 + 16 + 16]byte
	offset := copy(canonical[:], group.ClusterID[:])
	offset += copy(canonical[offset:], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(canonical[offset:offset+8], group.TopologyRecoveryEpoch)
	offset += 8
	offset += copy(canonical[offset:], group.ShardIncarnation[:])
	copy(canonical[offset:], group.GroupID[:])
	digest := sha256.Sum256(canonical[:])
	var scope [32]byte
	hex.Encode(scope[:], digest[:16])
	return filepath.Join(root, string(scope[:]))
}
