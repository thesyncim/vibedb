package clusterbackup

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

var ErrRestoreVerify = errors.New("clusterbackup: restore artifact verification failed")

const AbsoluteMaxRestoreArtifactBytes = uint64(1 << 40)

var verifySnapshotArtifact = replicatedstate.VerifySnapshotArtifact

// ArtifactSource opens the immutable artifact selected by operation and group.
// Implementations must not follow symlinks or substitute a newer artifact.
type ArtifactSource interface {
	OpenBackupArtifact(context.Context, [sha256.Size]byte, raftmember.GroupKey) (io.ReadCloser, error)
}

type RestoreVerifyOptions struct {
	Source           ArtifactSource
	MaxArtifactBytes uint64
	MaxTotalBytes    uint64
	PayloadBuffer    []byte
}

type countedHashReader struct {
	source  io.Reader
	hash    hash.Hash
	bytes   uint64
	maximum uint64
}

func (reader *countedHashReader) Read(dst []byte) (int, error) {
	if reader.bytes >= reader.maximum {
		var probe [1]byte
		count, err := reader.source.Read(probe[:])
		if count != 0 || err == nil {
			return 0, ErrRestoreVerify
		}
		return 0, err
	}
	if uint64(len(dst)) > reader.maximum-reader.bytes+1 {
		dst = dst[:reader.maximum-reader.bytes+1]
	}
	count, err := reader.source.Read(dst)
	if count > 0 {
		reader.bytes += uint64(count)
		_, _ = reader.hash.Write(dst[:count])
		if reader.bytes > reader.maximum {
			return count, ErrRestoreVerify
		}
	}
	return count, err
}

// VerifyRestoreArtifacts streams and verifies the complete certified vector,
// then returns only the non-serving staging permit. Memory is bounded by the
// caller-owned verifier buffer and one fixed evidence vector.
func VerifyRestoreArtifacts(ctx context.Context, certificate Certificate,
	restore [sha256.Size]byte, targetClusterID, targetClusterIncarnation [16]byte,
	options RestoreVerifyOptions,
) (RestoreStagingPermit, error) {
	if ctx == nil || options.Source == nil || options.MaxArtifactBytes == 0 ||
		options.MaxArtifactBytes > AbsoluteMaxRestoreArtifactBytes ||
		options.MaxTotalBytes < options.MaxArtifactBytes ||
		options.MaxTotalBytes > AbsoluteMaxRestoreArtifactBytes || len(certificate.Groups) == 0 {
		return RestoreStagingPermit{}, ErrRestoreVerify
	}
	evidence := make([]ArtifactEvidence, len(certificate.Groups))
	var total uint64
	for index, expected := range certificate.Groups {
		if cause := context.Cause(ctx); cause != nil {
			return RestoreStagingPermit{}, cause
		}
		if expected.ArtifactBytes > options.MaxArtifactBytes || total > options.MaxTotalBytes-expected.ArtifactBytes {
			return RestoreStagingPermit{}, ErrRestoreVerify
		}
		artifact, err := options.Source.OpenBackupArtifact(ctx, certificate.Operation, expected.Group)
		if err != nil || artifact == nil {
			return RestoreStagingPermit{}, errors.Join(ErrRestoreVerify, err)
		}
		digest := sha256.New()
		reader := &countedHashReader{source: artifact, hash: digest, maximum: options.MaxArtifactBytes}
		manifest, verifyErr := verifySnapshotArtifact(reader,
			replicatedstate.SnapshotArtifactCallbacks{PayloadBuffer: options.PayloadBuffer})
		closeErr := artifact.Close()
		if verifyErr != nil || closeErr != nil || reader.bytes != expected.ArtifactBytes {
			return RestoreStagingPermit{}, errors.Join(ErrRestoreVerify, verifyErr, closeErr)
		}
		var artifactHash [sha256.Size]byte
		copy(artifactHash[:], digest.Sum(nil))
		cut, err := GroupCutFromVerifiedArtifact(expected.SourceMember, manifest, artifactHash, reader.bytes)
		if err != nil || cut != expected {
			return RestoreStagingPermit{}, errors.Join(ErrRestoreVerify, err)
		}
		evidence[index] = ArtifactEvidence{Group: cut.Group, SnapshotIndex: cut.SnapshotIndex,
			SnapshotTerm: cut.SnapshotTerm, Lineage: cut.Lineage,
			RelationManifestDigest: cut.RelationManifestDigest, ArtifactHash: cut.ArtifactHash,
			ArtifactBytes: cut.ArtifactBytes, ArtifactManifestDigest: cut.ArtifactManifestDigest}
		total += reader.bytes
	}
	permit, err := AdmitRestore(certificate, evidence, restore, targetClusterID, targetClusterIncarnation)
	if err != nil {
		return RestoreStagingPermit{}, errors.Join(ErrRestoreVerify, err)
	}
	return permit, nil
}
