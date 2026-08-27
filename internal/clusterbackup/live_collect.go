package clusterbackup

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"os"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

type LiveArtifactExporter interface {
	Export(context.Context, LiveRequest, io.Writer) (GroupCut, error)
}

type LiveArtifactSource struct {
	Group        raftmember.GroupKey
	SourceMember uint64
	Exporter     LiveArtifactExporter
}

type boundedArtifactWriter struct {
	destination io.Writer
	digest      hash.Hash
	written     uint64
	maximum     uint64
}

func (writer *boundedArtifactWriter) Write(raw []byte) (int, error) {
	if uint64(len(raw)) > writer.maximum-writer.written {
		return 0, ErrBound
	}
	n, err := writer.destination.Write(raw)
	if n > 0 {
		writer.written += uint64(n)
		_, _ = writer.digest.Write(raw[:n])
	}
	return n, err
}

// CollectLive streams a complete catalog inventory directly into repository
// drafts, avoiding a second staging copy. Certificate publication remains the
// only commit point. A crash before it leaves drafts that recover removes; a
// crash after it is handled by the ordinary exact artifact-set recovery.
func (r *BackupRepository) CollectLive(ctx context.Context, operation [sha256.Size]byte,
	authority CatalogCut, sources []LiveArtifactSource,
) (Certificate, error) {
	if r == nil || ctx == nil || operation == ([sha256.Size]byte{}) || len(sources) == 0 ||
		len(sources) != len(authority.Groups) || len(sources) > AbsoluteMaxGroupCuts {
		return Certificate{}, ErrCatalogCut
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed {
		return Certificate{}, ErrRepository
	}
	for _, record := range r.records {
		if record.certificate.Operation == operation {
			if record.certificate.CatalogGeneration == authority.Generation &&
				record.certificate.CatalogDigest == authority.Digest &&
				record.certificate.PolicyGeneration == authority.PolicyGeneration {
				return record.certificate, nil
			}
			return Certificate{}, ErrCatalogCut
		}
	}
	if len(r.records) >= r.limits.MaxBackups || len(sources) > r.limits.MaxArtifacts-r.artifacts {
		return Certificate{}, ErrBound
	}
	certificateBytes := uint64(HeaderBytes + len(sources)*GroupCutBytes + TrailerBytes)
	if certificateBytes > r.limits.MaxDiskBytes-r.diskBytes {
		return Certificate{}, ErrBound
	}
	cleanup := true
	var publicationDigest [sha256.Size]byte
	defer func() {
		if cleanup {
			for index := range sources {
				_ = r.root.Remove(liveDraftName(operation, index))
				if publicationDigest != ([sha256.Size]byte{}) {
					_ = r.root.Remove(artifactName(publicationDigest, index))
				}
			}
			_ = syncBackupRoot(r.root)
		}
	}()
	cuts := make([]GroupCut, len(sources))
	remainingDisk := r.limits.MaxDiskBytes - r.diskBytes - certificateBytes
	var artifactBytes uint64
	for index, source := range sources {
		if cause := context.Cause(ctx); cause != nil {
			return Certificate{}, cause
		}
		if source.Exporter == nil || source.SourceMember == 0 || source.Group != authority.Groups[index] {
			return Certificate{}, ErrCatalogCut
		}
		maximum := min(r.limits.MaxArtifactBytes, remainingDisk)
		if maximum == 0 {
			return Certificate{}, ErrBound
		}
		file, err := openBackupRegular(r.root, liveDraftName(operation, index),
			os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return Certificate{}, err
		}
		writer := boundedArtifactWriter{destination: file, digest: sha256.New(), maximum: maximum}
		cut, exportErr := source.Exporter.Export(ctx, LiveRequest{Operation: operation,
			Group: source.Group, SourceMember: source.SourceMember}, &writer)
		syncErr := file.Sync()
		closeErr := file.Close()
		var digest [sha256.Size]byte
		copy(digest[:], writer.digest.Sum(nil))
		if exportErr != nil || syncErr != nil || closeErr != nil || cut.Group != source.Group ||
			cut.SourceMember != source.SourceMember || cut.ArtifactBytes != writer.written ||
			cut.ArtifactHash != digest || !cut.Valid() {
			return Certificate{}, errors.Join(ErrArtifactEvidence, exportErr, syncErr, closeErr)
		}
		cuts[index] = cut
		artifactBytes += writer.written
		remainingDisk -= writer.written
	}
	certificate, err := Certify(operation, authority, cuts)
	if err != nil {
		return Certificate{}, err
	}
	publicationDigest = certificate.Digest
	for index := range sources {
		if err = replaceBackupEntry(r.root, liveDraftName(operation, index),
			artifactName(certificate.Digest, index)); err != nil {
			return Certificate{}, err
		}
	}
	if err = syncBackupRoot(r.root); err != nil {
		return Certificate{}, err
	}
	raw, err := AppendCertificate(nil, certificate)
	if err != nil {
		return Certificate{}, err
	}
	temporary := certificateTempName(certificate.Digest)
	file, err := openBackupRegular(r.root, temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Certificate{}, err
	}
	writeErr := writeBackupFull(file, raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return Certificate{}, errors.Join(writeErr, syncErr, closeErr)
	}
	if err = replaceBackupEntry(r.root, temporary, certificateName(certificate.Digest)); err != nil {
		return Certificate{}, err
	}
	cleanup = false
	if err = syncBackupRoot(r.root); err != nil {
		r.failed = true
		return Certificate{}, err
	}
	rawBytes := artifactBytes + uint64(len(raw))
	r.records[certificate.Digest] = backupRecord{certificate: certificate, rawBytes: rawBytes}
	r.artifacts += len(cuts)
	r.diskBytes += rawBytes
	return certificate, nil
}
