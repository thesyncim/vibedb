package clusterbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

type restoreArtifactSource struct {
	operation [32]byte
	group     raftmember.GroupKey
	raw       []byte
}

func (source restoreArtifactSource) OpenBackupArtifact(_ context.Context, operation [32]byte, group raftmember.GroupKey) (io.ReadCloser, error) {
	if operation != source.operation || group != source.group {
		return nil, ErrRestoreVerify
	}
	return io.NopCloser(bytes.NewReader(source.raw)), nil
}

func TestVerifyRestoreArtifactsStreamsExactCompleteVector(t *testing.T) {
	raw := []byte("certified-artifact")
	artifactHash := sha256.Sum256(raw)
	cut := backupCut(1)
	cut.ArtifactBytes = uint64(len(raw))
	cut.ArtifactHash = artifactHash
	manifest := replicatedstate.SnapshotArtifactManifest{EncodedBytes: uint64(len(raw)),
		RelationManifestDigest: cut.RelationManifestDigest, Digest: cut.ArtifactManifestDigest,
		State: replicatedstate.State{Binding: replicatedstate.Binding{
			ClusterID: replication.ID128(cut.Group.ClusterID), ClusterIncarnation: replication.ID128(cut.Group.ClusterIncarnation),
			TopologyRecoveryEpoch: cut.Group.TopologyRecoveryEpoch, ShardIncarnation: replication.ID128(cut.Group.ShardIncarnation),
			GroupID: replication.ID128(cut.Group.GroupID), SchemaGeneration: cut.SchemaGeneration},
			Applied: cut.SnapshotIndex, LastTerm: cut.SnapshotTerm, LastEntryDigest: cut.Lineage,
			ReplicaSetVersion: cut.ReplicaSetVersion}}
	certificate, err := Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9), PolicyGeneration: 8,
		Groups: []raftmember.GroupKey{cut.Group}}, []GroupCut{cut})
	if err != nil {
		t.Fatal(err)
	}
	previous := verifySnapshotArtifact
	verifySnapshotArtifact = func(reader io.Reader, _ replicatedstate.SnapshotArtifactCallbacks) (replicatedstate.SnapshotArtifactManifest, error) {
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return replicatedstate.SnapshotArtifactManifest{}, err
		}
		return manifest, nil
	}
	t.Cleanup(func() { verifySnapshotArtifact = previous })
	permit, err := VerifyRestoreArtifacts(t.Context(), certificate, filled32(20), filled16(21), filled16(22),
		RestoreVerifyOptions{Source: restoreArtifactSource{certificate.Operation, cut.Group, raw},
			MaxArtifactBytes: 1024, MaxTotalBytes: 1024, PayloadBuffer: make([]byte, 4096)})
	if err != nil || permit.CertificateDigest != certificate.Digest || permit.Groups != 1 {
		t.Fatalf("permit=%+v err=%v", permit, err)
	}
}

func TestVerifyRestoreArtifactsRejectsBoundsCorruptionAndPartialSource(t *testing.T) {
	cut := backupCut(1)
	certificate, err := Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9), PolicyGeneration: 8,
		Groups: []raftmember.GroupKey{cut.Group}}, []GroupCut{cut})
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []RestoreVerifyOptions{
		{},
		{Source: restoreArtifactSource{}, MaxArtifactBytes: AbsoluteMaxRestoreArtifactBytes + 1, MaxTotalBytes: AbsoluteMaxRestoreArtifactBytes + 1},
		{Source: restoreArtifactSource{}, MaxArtifactBytes: 1, MaxTotalBytes: 1},
	} {
		if _, err = VerifyRestoreArtifacts(t.Context(), certificate, filled32(20), filled16(21), filled16(22), options); !errors.Is(err, ErrRestoreVerify) {
			t.Fatalf("options=%+v err=%v", options, err)
		}
	}
	// The real verifier refuses arbitrary/truncated bytes before a permit exists.
	cut.ArtifactBytes = 3
	cut.ArtifactHash = sha256.Sum256([]byte("bad"))
	certificate, _ = Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9), PolicyGeneration: 8,
		Groups: []raftmember.GroupKey{cut.Group}}, []GroupCut{cut})
	if _, err = VerifyRestoreArtifacts(t.Context(), certificate, filled32(20), filled16(21), filled16(22),
		RestoreVerifyOptions{Source: restoreArtifactSource{certificate.Operation, cut.Group, []byte("bad")},
			MaxArtifactBytes: 1024, MaxTotalBytes: 1024}); !errors.Is(err, ErrRestoreVerify) {
		t.Fatalf("invalid artifact err=%v", err)
	}
}
