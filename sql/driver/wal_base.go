package driver

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var ErrWALBasePreparation = errors.New("vibedb: WAL base preparation is invalid")

var walBasePreparationDomain = []byte("vibedb/sql/wal-base-preparation/fixed\x00")

// WALBaseCaptureOptions controls only caller-owned bounded scan workspace.
// Artifact framing is fixed by CaptureWALBase and has no alternate grammar.
// Workspace is required. Its capacity must cover the exact no-growth payload
// bound derived from both collections' frozen key and document limits. Storage
// checkpointing and raw overflow-row traversal use separate bounded engine
// scratch.
type WALBaseCaptureOptions struct {
	Workspace []byte
}

// WALBasePreparation is a non-activating, bounded preparation. It owns no WAL
// generation and grants no rename, replacement, deletion, or replay authority.
// Its retention witness remains meaningful only while exact validation through
// the originating ReplicatedApply succeeds. An internal commitment binds the
// snapshot base and retention witness so callers cannot splice either part.
type WALBasePreparation struct {
	owner              *ReplicatedApply
	retention          durable.CheckpointRetentionWitness
	snapshotBase       *pb.Snapshot
	snapshotBaseDigest [sha256.Size]byte
	artifactDigest     [sha256.Size]byte
	imageDigest        [sha256.Size]byte
	binding            [sha256.Size]byte
	encodedBytes       uint64
	scanPasses         uint8
}

// SnapshotBase returns an owned copy of the compact base certificate. The
// streamed artifact itself is deliberately not retained locally.
func (p *WALBasePreparation) SnapshotBase() (*pb.Snapshot, error) {
	if p == nil || p.snapshotBase == nil || p.scanPasses != 1 {
		return nil, ErrWALBasePreparation
	}
	return proto.Clone(p.snapshotBase).(*pb.Snapshot), nil
}

// captureWALBaseScanHook is a test seam immediately before the sole bounded
// snapshot scan. Production leaves it nil.
var captureWALBaseScanHook func()

// CaptureWALBase seals the current SQL applied cut into both authenticated
// checkpoint slots, pins the matching coherent snapshot under the publication
// lock, and then releases that lock for exactly one bounded artifact scan into
// io.Discard. It neither creates a local artifact nor mutates any WAL file.
func (a *ReplicatedApply) CaptureWALBase(
	options WALBaseCaptureOptions,
) (preparation *WALBasePreparation, resultErr error) {
	if a == nil || a.database == nil {
		return nil, ErrReplicatedApplyClosed
	}
	artifactOptions := replicatedstate.SnapshotArtifactOptions{
		TargetChunkBytes: replicatedstate.DefaultSnapshotArtifactChunkBytes,
		PayloadBuffer:    options.Workspace,
	}
	if err := replicatedstate.ValidateSnapshotArtifactOptions(artifactOptions); err != nil {
		return nil, err
	}

	core := a.database
	core.mu.Lock()
	if err := a.checkLocked(); err != nil {
		core.mu.Unlock()
		return nil, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		core.mu.Unlock()
		return nil, err
	}
	if a.walBaseCaptureActive {
		core.mu.Unlock()
		return nil, ErrReplicatedApplyBusy
	}
	group := core.checkpointGroup
	base := core.catalog.ReplicatedShardStore
	if group == nil || base == nil || base.UserTable == "" {
		core.mu.Unlock()
		return nil, ErrReplicatedApplyMismatch
	}
	systemWorkspaceBytes, err := replicatedstate.RequiredSnapshotArtifactPayloadCapacity(
		replicatedstate.DefaultSnapshotArtifactChunkBytes,
		a.identity.SystemLimits.MaxKeyBytes,
		a.identity.SystemLimits.MaxDocumentBytes,
	)
	if err != nil {
		core.mu.Unlock()
		return nil, errors.Join(ErrReplicatedApplyMismatch, err)
	}
	userWorkspaceBytes, err := replicatedstate.RequiredSnapshotArtifactPayloadCapacity(
		replicatedstate.DefaultSnapshotArtifactChunkBytes,
		base.UserLimits.MaxKeyBytes,
		base.UserLimits.MaxDocumentBytes,
	)
	if err != nil {
		core.mu.Unlock()
		return nil, errors.Join(ErrReplicatedApplyMismatch, err)
	}
	requiredWorkspaceBytes := max(systemWorkspaceBytes, userWorkspaceBytes)
	if cap(options.Workspace) < requiredWorkspaceBytes {
		core.mu.Unlock()
		return nil, fmt.Errorf(
			"%w: WAL-base workspace capacity %d below no-growth requirement %d",
			replicatedstate.ErrSnapshotArtifactBound,
			cap(options.Workspace),
			requiredWorkspaceBytes,
		)
	}
	applied := a.machine.Applied()
	witness, err := group.SealRetentionFloor(applied)
	if err != nil {
		core.mu.Unlock()
		return nil, err
	}
	if !witness.BindsAppliedIndex(applied) {
		core.mu.Unlock()
		return nil, errors.Join(ErrWALBasePreparation, durable.ErrCheckpointRetentionWitness)
	}
	if err := group.ValidateRetentionWitness(witness); err != nil {
		core.mu.Unlock()
		return nil, errors.Join(ErrWALBasePreparation, err)
	}
	cut, err := a.machine.Snapshot(base.UserTable)
	if err != nil {
		core.mu.Unlock()
		return nil, err
	}
	if !witness.BindsAppliedIndex(cut.Fence().Applied) || cut.Fence().Applied != applied {
		closeErr := cut.Close()
		core.mu.Unlock()
		return nil, errors.Join(ErrWALBasePreparation, closeErr)
	}
	a.walBaseCaptureActive = true
	core.mu.Unlock()

	defer func() {
		closeErr := cut.Close()
		core.mu.Lock()
		a.walBaseCaptureActive = false
		core.mu.Unlock()
		if closeErr != nil {
			resultErr = errors.Join(resultErr, closeErr)
			preparation = nil
		}
	}()

	if captureWALBaseScanHook != nil {
		captureWALBaseScanHook()
	}
	manifest, err := replicatedstate.WriteSnapshotArtifact(
		io.Discard,
		cut,
		artifactOptions,
	)
	if err != nil {
		return nil, err
	}
	if !witness.BindsAppliedIndex(manifest.State.Applied) ||
		manifest.State.Applied != applied ||
		manifest.TargetChunkBytes != replicatedstate.DefaultSnapshotArtifactChunkBytes {
		return nil, ErrWALBasePreparation
	}
	snapshotBase, err := a.machine.BuildSnapshotBaseForManifest(manifest)
	if err != nil {
		return nil, err
	}
	certificate, err := replicatedstate.OpenSnapshotBase(snapshotBase)
	if err != nil {
		return nil, err
	}
	if !witness.BindsAppliedIndex(certificate.Manifest.State.Applied) ||
		certificate.Manifest.Digest != manifest.Digest ||
		certificate.Manifest.ImageDigest != manifest.ImageDigest ||
		certificate.Manifest.EncodedBytes != manifest.EncodedBytes {
		return nil, ErrWALBasePreparation
	}

	core.mu.RLock()
	validationErr := a.checkLocked()
	if validationErr == nil {
		validationErr = a.checkActivationBaseLocked()
	}
	if validationErr == nil && core.checkpointGroup != group {
		validationErr = ErrReplicatedApplyMismatch
	}
	if validationErr == nil {
		validationErr = group.ValidateRetentionWitness(witness)
	}
	core.mu.RUnlock()
	if validationErr != nil {
		return nil, errors.Join(ErrWALBasePreparation, validationErr)
	}

	result := &WALBasePreparation{
		owner: a, retention: witness, snapshotBase: snapshotBase,
		snapshotBaseDigest: certificate.Digest,
		artifactDigest:     manifest.Digest,
		imageDigest:        manifest.ImageDigest,
		encodedBytes:       manifest.EncodedBytes,
		scanPasses:         1,
	}
	result.binding = bindWALBasePreparation(result.snapshotBaseDigest, result.retention)
	return result, nil
}

// ValidateWALBasePreparation reopens the compact certificate and re-reads both
// authenticated checkpoint slots through their live owner. A newer
// uncertified SQL suffix and monotonic later checkpoints are allowed. A
// rollback below the retained floor or a lineage change makes the preparation
// stale. This still grants no WAL mutation authority.
func (a *ReplicatedApply) ValidateWALBasePreparation(
	preparation *WALBasePreparation,
) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	if preparation == nil || preparation.owner != a || preparation.snapshotBase == nil ||
		preparation.retention.IsZero() || preparation.scanPasses != 1 {
		return ErrWALBasePreparation
	}
	certificate, err := replicatedstate.OpenSnapshotBase(preparation.snapshotBase)
	if err != nil {
		return errors.Join(ErrWALBasePreparation, err)
	}
	if certificate.Digest != preparation.snapshotBaseDigest ||
		certificate.Manifest.Digest != preparation.artifactDigest ||
		certificate.Manifest.ImageDigest != preparation.imageDigest ||
		certificate.Manifest.EncodedBytes != preparation.encodedBytes ||
		!preparation.retention.BindsAppliedIndex(certificate.Manifest.State.Applied) ||
		preparation.binding != bindWALBasePreparation(
			certificate.Digest,
			preparation.retention,
		) {
		return ErrWALBasePreparation
	}

	core := a.database
	core.mu.RLock()
	defer core.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return err
	}
	base := core.catalog.ReplicatedShardStore
	if core.checkpointGroup == nil || base == nil ||
		!equalWALBaseCollection(certificate.Manifest.UserCollection, base.UserTable) {
		return ErrReplicatedApplyMismatch
	}
	if err := core.checkpointGroup.ValidateRetentionWitness(preparation.retention); err != nil {
		return errors.Join(ErrWALBasePreparation, err)
	}
	return nil
}

func bindWALBasePreparation(
	snapshotBaseDigest [sha256.Size]byte,
	witness durable.CheckpointRetentionWitness,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(walBasePreparationDomain)
	_, _ = h.Write(snapshotBaseDigest[:])
	witnessCommitment := witness.Commitment()
	_, _ = h.Write(witnessCommitment[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func equalWALBaseCollection(raw []byte, name string) bool {
	if len(raw) != len(name) {
		return false
	}
	for index := range raw {
		if raw[index] != name[index] {
			return false
		}
	}
	return true
}
