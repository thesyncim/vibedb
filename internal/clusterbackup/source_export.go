package clusterbackup

import (
	"crypto/sha256"
	"hash"
	"io"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type artifactHashWriter struct {
	destination io.Writer
	digest      hash.Hash
}

func (writer artifactHashWriter) Write(raw []byte) (int, error) {
	n, err := writer.destination.Write(raw)
	if n > 0 {
		_, _ = writer.digest.Write(raw[:n])
	}
	return n, err
}

// ExportLinearizableGroupCut streams one target-free backup artifact from an
// already ReadIndex-authorized immutable cut. Unlike learner transfer it does
// not invent a target member/store/incarnation. SourceMember must be a voter
// in the exact snapshot publication.
func ExportLinearizableGroupCut(destination io.Writer, snapshot *replicatedstate.ReadSnapshot,
	group raftmember.GroupKey, sourceMember uint64, chunkBytes int, payloadBuffer []byte,
) (GroupCut, replicatedstate.SnapshotArtifactManifest, error) {
	if destination == nil || snapshot == nil || !validGroup(group) || sourceMember == 0 {
		return GroupCut{}, replicatedstate.SnapshotArtifactManifest{}, ErrArtifactEvidence
	}
	fence := snapshot.Fence()
	binding := fence.Binding
	publication := snapshot.Publication()
	if binding.ClusterID != group.ClusterID || binding.ClusterIncarnation != group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != group.TopologyRecoveryEpoch ||
		binding.ShardIncarnation != group.ShardIncarnation || binding.GroupID != group.GroupID ||
		publication.ConfState == nil || publication.ReplicaSetVersion != fence.ReplicaSetVersion ||
		publication.Applied != fence.Applied || !slices.Contains(publication.ConfState.GetVoters(), sourceMember) {
		return GroupCut{}, replicatedstate.SnapshotArtifactManifest{}, ErrArtifactEvidence
	}
	options := replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: chunkBytes,
		PayloadBuffer: payloadBuffer[:0]}
	if err := replicatedstate.ValidateSnapshotArtifactOptions(options); err != nil {
		return GroupCut{}, replicatedstate.SnapshotArtifactManifest{}, err
	}
	digest := sha256.New()
	manifest, err := replicatedstate.WriteSnapshotArtifact(
		artifactHashWriter{destination: destination, digest: digest}, snapshot, options)
	if err != nil {
		return GroupCut{}, replicatedstate.SnapshotArtifactManifest{}, err
	}
	var artifactHash [sha256.Size]byte
	copy(artifactHash[:], digest.Sum(nil))
	cut, err := GroupCutFromVerifiedArtifact(sourceMember, manifest, artifactHash, manifest.EncodedBytes)
	if err != nil || cut.Group != group {
		return GroupCut{}, replicatedstate.SnapshotArtifactManifest{}, ErrArtifactEvidence
	}
	return cut, manifest, nil
}
