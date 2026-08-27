package clusterbackup

import (
	"bytes"
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

// ArtifactEvidence is produced only after the ordinary snapshot artifact
// verifier has authenticated the complete footer and logical image.
type ArtifactEvidence struct {
	Group                  raftmember.GroupKey
	SnapshotIndex          uint64
	SnapshotTerm           uint64
	Lineage                [sha256.Size]byte
	RelationManifestDigest [sha256.Size]byte
	ArtifactHash           [sha256.Size]byte
	ArtifactBytes          uint64
	ArtifactManifestDigest [sha256.Size]byte
}

// RestoreStagingPermit admits verified bytes only into non-serving candidate
// roots. It deliberately contains no member ID, store ID, node incarnation,
// membership grant, ownership epoch, or route generation.
type RestoreStagingPermit struct {
	Restore                  [sha256.Size]byte
	CertificateDigest        [sha256.Size]byte
	CatalogGeneration        uint64
	CatalogDigest            [sha256.Size]byte
	TargetClusterID          [16]byte
	TargetClusterIncarnation [16]byte
	Groups                   uint32
}

// AdmitRestore verifies the exact complete evidence vector and returns only a
// non-serving staging permit. A later replicated catalog/bootstrap operation
// must independently assign new stores, members, ownership, and routing.
func AdmitRestore(certificate Certificate, evidence []ArtifactEvidence,
	restore [sha256.Size]byte, targetClusterID, targetClusterIncarnation [16]byte,
) (RestoreStagingPermit, error) {
	if certificate.Digest == ([sha256.Size]byte{}) || len(certificate.Groups) == 0 ||
		len(evidence) != len(certificate.Groups) || restore == ([sha256.Size]byte{}) ||
		targetClusterID == ([16]byte{}) || targetClusterIncarnation == ([16]byte{}) {
		return RestoreStagingPermit{}, ErrArtifactEvidence
	}
	raw, err := AppendCertificate(nil, certificate)
	if err != nil || !bytes.Equal(raw[len(raw)-TrailerBytes:], certificate.Digest[:]) ||
		(targetClusterID == certificate.Groups[0].Group.ClusterID &&
			targetClusterIncarnation == certificate.Groups[0].Group.ClusterIncarnation) {
		return RestoreStagingPermit{}, ErrArtifactEvidence
	}
	for index, cut := range certificate.Groups {
		item := evidence[index]
		if item.Group != cut.Group || item.SnapshotIndex != cut.SnapshotIndex ||
			item.SnapshotTerm != cut.SnapshotTerm || item.Lineage != cut.Lineage ||
			item.RelationManifestDigest != cut.RelationManifestDigest ||
			item.ArtifactHash != cut.ArtifactHash || item.ArtifactBytes != cut.ArtifactBytes ||
			item.ArtifactManifestDigest != cut.ArtifactManifestDigest {
			return RestoreStagingPermit{}, ErrArtifactEvidence
		}
	}
	return RestoreStagingPermit{Restore: restore, CertificateDigest: certificate.Digest,
		CatalogGeneration: certificate.CatalogGeneration, CatalogDigest: certificate.CatalogDigest,
		TargetClusterID: targetClusterID, TargetClusterIncarnation: targetClusterIncarnation,
		Groups: uint32(len(certificate.Groups))}, nil
}
