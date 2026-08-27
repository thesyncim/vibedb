package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

// BackupRepositoryCoordinator composes the replicated catalog lifecycle with
// the durable artifact repository. The repository certificate is committed
// before the catalog advances; a replacement coordinator settles an
// outcome-unknown CAS from the catalog record and never trusts local phase
// memory.
type BackupRepositoryCoordinator struct {
	lifecycle  *BackupOperationController
	repository *clusterbackup.BackupRepository
}

type RestoreStagingOptions struct {
	Path                     string
	RepositoryLimits         clusterbackup.RepositoryLimits
	Restore                  [sha256.Size]byte
	TargetClusterID          [16]byte
	TargetClusterIncarnation [16]byte
	MaxArtifactBytes         uint64
	MaxTotalBytes            uint64
	PayloadBuffer            []byte
}

type BackupLeaderResolver interface {
	ResolveBackupLeader(context.Context, raftmember.GroupKey) (uint64, clusterbackup.LiveArtifactExporter, error)
}

func NewBackupRepositoryCoordinator(lifecycle *BackupOperationController,
	repository *clusterbackup.BackupRepository,
) (*BackupRepositoryCoordinator, error) {
	if lifecycle == nil || repository == nil || !lifecycle.authorized() {
		return nil, ErrBackupOperation
	}
	return &BackupRepositoryCoordinator{lifecycle: lifecycle, repository: repository}, nil
}

// Publish commits the exact complete vector and advances it to exported. The
// readers are consumed only while collecting. Replaying a repository-committed
// call may pass nil readers because repository publication is idempotent.
func (coordinator *BackupRepositoryCoordinator) Publish(ctx context.Context,
	record ReplicatedOperationRecord, certificate clusterbackup.Certificate,
	artifacts ...clusterbackup.ArtifactInput,
) (ReplicatedOperationRecord, error) {
	if coordinator == nil || ctx == nil || coordinator.lifecycle == nil ||
		coordinator.repository == nil || !coordinator.lifecycle.authorized() {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	if validBackupRecord(record, backupStageCollecting) {
		if err := coordinator.repository.Publish(certificate, artifacts...); err != nil {
			return ReplicatedOperationRecord{}, err
		}
		published, err := coordinator.repository.Certificate(certificate.Digest)
		if err != nil || published.Digest != certificate.Digest ||
			!backupCertificateMatchesIntent(record, published) {
			return ReplicatedOperationRecord{}, errors.Join(ErrBackupOperation, err)
		}
		bytes := uint64(clusterbackup.HeaderBytes + len(published.Groups)*clusterbackup.GroupCutBytes + clusterbackup.TrailerBytes)
		next, advanceErr := coordinator.lifecycle.PublishCertified(ctx, record, published, bytes)
		if advanceErr != nil {
			next, advanceErr = coordinator.settle(ctx, record.ID, backupStageCertified, published.Digest, advanceErr)
		}
		if advanceErr != nil {
			return ReplicatedOperationRecord{}, advanceErr
		}
		record = next
	}
	if validBackupRecord(record, backupStageCertified) {
		published, err := coordinator.repository.Certificate(backupCertificateDigest(record.Cursor))
		if err != nil || published.Operation != record.ID || !backupCertificateMatchesIntent(record, published) {
			return ReplicatedOperationRecord{}, errors.Join(ErrBackupOperation, err)
		}
		exportDigest := backupExportDigest(published)
		next, advanceErr := coordinator.lifecycle.PublishExported(ctx, record, exportDigest)
		if advanceErr != nil {
			next, advanceErr = coordinator.settle(ctx, record.ID, backupStageExported, exportDigest, advanceErr)
		}
		if advanceErr != nil {
			return ReplicatedOperationRecord{}, advanceErr
		}
		record = next
	}
	if !validBackupRecord(record, backupStageExported) {
		return ReplicatedOperationRecord{}, ErrBackupOperation
	}
	return record, nil
}

// CollectLive drives the complete catalog inventory through the backup-only
// shard exporters directly into the central repository, then advances the
// replicated lifecycle. Repository drafts are operation-scoped and no second
// artifact copy is created.
func (coordinator *BackupRepositoryCoordinator) CollectLive(ctx context.Context,
	record ReplicatedOperationRecord, cut clusterbackup.CatalogCut,
	sources []clusterbackup.LiveArtifactSource,
) (ReplicatedOperationRecord, clusterbackup.Certificate, error) {
	if coordinator == nil || ctx == nil || !validBackupRecord(record, backupStageCollecting) ||
		len(sources) != int(record.Cursor[1]) {
		return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, ErrBackupOperation
	}
	certificate, err := coordinator.repository.CollectLive(ctx, record.ID, cut, sources)
	if err != nil {
		return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	// Existing publication is selected before readers are consumed; the exact
	// arity retains the repository's complete-vector grammar.
	artifacts := make([]clusterbackup.ArtifactInput, len(certificate.Groups))
	exported, err := coordinator.Publish(ctx, record, certificate, artifacts...)
	if err != nil {
		return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
	}
	return exported, certificate, nil
}

// CollectFromLeaders resolves every group in the immutable catalog inventory
// before starting publication. Resolution may perform bounded NotLeader
// retries, but the returned source member is authenticated again by the shard
// service and by repository evidence.
func (coordinator *BackupRepositoryCoordinator) CollectFromLeaders(ctx context.Context,
	record ReplicatedOperationRecord, cut clusterbackup.CatalogCut, resolver BackupLeaderResolver,
) (ReplicatedOperationRecord, clusterbackup.Certificate, error) {
	if ctx == nil || resolver == nil || len(cut.Groups) == 0 || len(cut.Groups) != int(record.Cursor[1]) {
		return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, ErrBackupOperation
	}
	sources := make([]clusterbackup.LiveArtifactSource, len(cut.Groups))
	for index, group := range cut.Groups {
		if cause := context.Cause(ctx); cause != nil {
			return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, cause
		}
		member, exporter, err := resolver.ResolveBackupLeader(ctx, group)
		if err != nil || member == 0 || exporter == nil {
			return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, errors.Join(ErrBackupOperation, err)
		}
		sources[index] = clusterbackup.LiveArtifactSource{Group: group, SourceMember: member, Exporter: exporter}
	}
	return coordinator.CollectLive(ctx, record, cut, sources)
}

// ResumeExport settles a certificate-last repository publication after a
// gateway restart without reading or copying any shard artifact again.
func (coordinator *BackupRepositoryCoordinator) ResumeExport(ctx context.Context,
	record ReplicatedOperationRecord,
) (ReplicatedOperationRecord, clusterbackup.Certificate, error) {
	if coordinator == nil || ctx == nil || coordinator.repository == nil ||
		!validBackupRecord(record, backupStageCertified) &&
			!validBackupRecord(record, backupStageExported) {
		return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, ErrBackupOperation
	}
	digest := backupCertificateDigest(record.Cursor)
	certificate, err := coordinator.repository.Certificate(digest)
	if err != nil || certificate.Operation != record.ID ||
		!backupCertificateMatchesIntent(record, certificate) {
		return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, errors.Join(ErrBackupOperation, err)
	}
	if validBackupRecord(record, backupStageCertified) {
		record, err = coordinator.Publish(ctx, record, certificate)
		if err != nil {
			return ReplicatedOperationRecord{}, clusterbackup.Certificate{}, err
		}
	}
	return record, certificate, nil
}

// StageRestore verifies every artifact from the coordinator-owned immutable
// export, builds a non-serving root, and only then advances the replicated
// lifecycle. It never creates databases, members, stores, routes, or grants.
func (coordinator *BackupRepositoryCoordinator) StageRestore(ctx context.Context,
	record ReplicatedOperationRecord, certificate clusterbackup.Certificate,
	options RestoreStagingOptions,
) (*clusterbackup.RestoreStagingRoot, ReplicatedOperationRecord, error) {
	if coordinator == nil || ctx == nil || coordinator.lifecycle == nil ||
		coordinator.repository == nil || !coordinator.lifecycle.authorized() ||
		backupCertificateDigest(record.Cursor) != certificate.Digest ||
		!backupCertificateMatchesIntent(record, certificate) {
		return nil, ReplicatedOperationRecord{}, ErrBackupOperation
	}
	if validBackupRecord(record, backupStageRestoreStaged) {
		staged, err := clusterbackup.OpenRestoreStagingRoot(options.Path, options.RepositoryLimits)
		if err != nil || staged.Permit.CertificateDigest != certificate.Digest ||
			staged.Permit.Restore != options.Restore ||
			staged.Permit.TargetClusterID != options.TargetClusterID ||
			staged.Permit.TargetClusterIncarnation != options.TargetClusterIncarnation ||
			record.Proof != backupRestoreProof(staged.Permit) {
			if staged != nil {
				_ = staged.Close()
			}
			return nil, ReplicatedOperationRecord{}, errors.Join(ErrBackupOperation, err)
		}
		return staged, record, nil
	}
	if !validBackupRecord(record, backupStageExported) || record.Proof != backupExportDigest(certificate) {
		return nil, ReplicatedOperationRecord{}, ErrBackupOperation
	}
	published, err := coordinator.repository.Certificate(certificate.Digest)
	if err != nil || published.Digest != certificate.Digest {
		return nil, ReplicatedOperationRecord{}, errors.Join(ErrBackupOperation, err)
	}
	permit, err := clusterbackup.VerifyRestoreArtifacts(ctx, published, options.Restore,
		options.TargetClusterID, options.TargetClusterIncarnation, clusterbackup.RestoreVerifyOptions{
			Source: coordinator.repository, MaxArtifactBytes: options.MaxArtifactBytes,
			MaxTotalBytes: options.MaxTotalBytes, PayloadBuffer: options.PayloadBuffer})
	if err != nil {
		return nil, ReplicatedOperationRecord{}, err
	}
	staged, err := clusterbackup.BuildRestoreStagingRoot(ctx, options.Path, options.RepositoryLimits,
		published, permit, coordinator.repository)
	if err != nil {
		return nil, ReplicatedOperationRecord{}, err
	}
	next, advanceErr := coordinator.lifecycle.PublishRestoreStaged(ctx, record, permit)
	if advanceErr != nil {
		next, advanceErr = coordinator.settle(ctx, record.ID, backupStageRestoreStaged,
			backupRestoreProof(permit), advanceErr)
	}
	if advanceErr != nil {
		_ = staged.Close()
		return nil, ReplicatedOperationRecord{}, advanceErr
	}
	return staged, next, nil
}

func (coordinator *BackupRepositoryCoordinator) settle(ctx context.Context, id [sha256.Size]byte,
	stage uint64, proof [sha256.Size]byte, cause error,
) (ReplicatedOperationRecord, error) {
	current, err := coordinator.lifecycle.authority.ReadOperation(ctx, id)
	if err == nil && validBackupRecord(current, stage) && current.Proof == proof {
		return current, nil
	}
	return ReplicatedOperationRecord{}, errors.Join(cause, err)
}

func backupExportDigest(certificate clusterbackup.Certificate) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb certified backup export\x00"))
	_, _ = hash.Write(certificate.Digest[:])
	var scalar [8]byte
	for _, cut := range certificate.Groups {
		_, _ = hash.Write(cut.ArtifactHash[:])
		binary.BigEndian.PutUint64(scalar[:], cut.ArtifactBytes)
		_, _ = hash.Write(scalar[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
