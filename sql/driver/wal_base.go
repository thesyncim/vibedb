package driver

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var ErrWALBasePreparation = errors.New("vibedb: WAL base preparation is invalid")

var walBasePreparationDomain = []byte("vibedb/sql/wal-base-preparation/fixed\x00")

// WALBaseCaptureOptions controls only caller-owned bounded scan workspace.
// Artifact framing is fixed by CaptureWALBase and has no alternate grammar.
// Workspace is required. The system, every dense relation, and capture
// collection's frozen key and document limits are validated before sealing,
// then its capacity must cover their shared fixed aggregate chunk target.
// Exceptional rows stream directly from borrowed engine bytes.
// Storage checkpointing and raw overflow-row traversal use separate bounded
// engine scratch.
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

var _ raftstore.GenerationActivationSettler = (*ReplicatedApply)(nil)

// SnapshotBase returns an owned copy of the compact base certificate. The
// streamed artifact itself is deliberately not retained locally.
func (p *WALBasePreparation) SnapshotBase() (*pb.Snapshot, error) {
	if p == nil || p.snapshotBase == nil || p.scanPasses != 1 {
		return nil, ErrWALBasePreparation
	}
	return proto.Clone(p.snapshotBase).(*pb.Snapshot), nil
}

// GenerationInput returns a detached, sealed input for raftstore generation
// construction. The retention commitment is not deletion authority by itself:
// the originating apply owner must still pass ValidateWALBasePreparation before
// selection, and SettleGenerationActivation re-derives the live witness before
// the old logical WAL leaf can be replaced.
func (p *WALBasePreparation) GenerationInput() (
	raftstore.GenerationInput,
	error,
) {
	if p == nil || p.snapshotBase == nil || p.retention.IsZero() ||
		p.scanPasses != 1 || p.binding == ([sha256.Size]byte{}) {
		return raftstore.GenerationInput{}, ErrWALBasePreparation
	}
	commitment := p.retention.Commitment()
	if commitment == ([sha256.Size]byte{}) {
		return raftstore.GenerationInput{}, ErrWALBasePreparation
	}
	return raftstore.GenerationInput{
		Snapshot:            proto.Clone(p.snapshotBase).(*pb.Snapshot),
		SnapshotBaseDigest:  p.snapshotBaseDigest,
		RetentionCommitment: commitment,
	}, nil
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
	if group == nil || base == nil || base.RelationCount == 0 {
		core.mu.Unlock()
		return nil, ErrReplicatedApplyMismatch
	}
	requiredWorkspaceBytes, err := walBaseRequiredWorkspaceBytes(*base, a.identity)
	if err != nil {
		core.mu.Unlock()
		return nil, err
	}
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
	cut, err := a.machine.Snapshot()
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
		manifest.TargetChunkBytes != replicatedstate.DefaultSnapshotArtifactChunkBytes ||
		!equalWALBaseManifest(
			manifest, *base, cut.Fence().RelationManifestDigest,
		) {
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
	certificate, err := a.openWALBasePreparation(preparation)
	if err != nil {
		return err
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
		!walBaseManifestMatchesMachine(a, certificate.Manifest, *base) {
		return ErrReplicatedApplyMismatch
	}
	if err := core.checkpointGroup.ValidateRetentionWitness(preparation.retention); err != nil {
		return errors.Join(ErrWALBasePreparation, err)
	}
	return nil
}

func (a *ReplicatedApply) openWALBasePreparation(
	preparation *WALBasePreparation,
) (replicatedstate.SnapshotBaseCertificate, error) {
	if preparation == nil || preparation.owner != a || preparation.snapshotBase == nil ||
		preparation.retention.IsZero() || preparation.scanPasses != 1 {
		return replicatedstate.SnapshotBaseCertificate{}, ErrWALBasePreparation
	}
	certificate, err := replicatedstate.OpenSnapshotBase(preparation.snapshotBase)
	if err != nil {
		return replicatedstate.SnapshotBaseCertificate{}, errors.Join(ErrWALBasePreparation, err)
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
		return replicatedstate.SnapshotBaseCertificate{}, ErrWALBasePreparation
	}
	return certificate, nil
}

// PublishWALGenerationSelection creates one short SQL/WAL quiescence lease.
// The exact preparation must be the one captured by builder, and SQL Applied
// must still equal that certificate while selection is published. Concurrent
// apply/snapshot/close calls fail busy rather than racing the durable cut.
func (a *ReplicatedApply) PublishWALGenerationSelection(
	preparation *WALBasePreparation,
	wal *raftstore.Store,
	builder *raftstore.GenerationBuilder,
) (resultErr error) {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	if wal == nil || builder == nil {
		return ErrWALBasePreparation
	}
	walIdentity := wal.Identity()
	walTopologyRecoveryEpoch := wal.TopologyRecoveryEpoch()
	certificate, err := a.openWALBasePreparation(preparation)
	if err != nil {
		return err
	}
	input, err := preparation.GenerationInput()
	if err != nil || !builder.BindsInput(input) {
		return errors.Join(ErrWALBasePreparation, err)
	}
	core := a.database
	core.mu.Lock()
	if err := a.checkLocked(); err != nil {
		core.mu.Unlock()
		return err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		core.mu.Unlock()
		return err
	}
	if a.walBaseCaptureActive || a.walBaseSelectActive || a.walBaseSelectPending {
		core.mu.Unlock()
		return ErrReplicatedApplyBusy
	}
	base := core.catalog.ReplicatedShardStore
	if core.checkpointGroup == nil || base == nil ||
		!walBaseManifestMatchesMachine(a, certificate.Manifest, *base) {
		core.mu.Unlock()
		return ErrReplicatedApplyMismatch
	}
	if !walMatchesReplicatedBinding(
		walIdentity, walTopologyRecoveryEpoch, base.Binding,
	) {
		core.mu.Unlock()
		return ErrReplicatedApplyMismatch
	}
	if err := core.checkpointGroup.ValidateRetentionWitness(preparation.retention); err != nil {
		core.mu.Unlock()
		return errors.Join(ErrWALBasePreparation, err)
	}
	if a.machine.Applied() != certificate.Manifest.State.Applied {
		core.mu.Unlock()
		return ErrWALBasePreparation
	}
	a.walBaseSelectActive = true
	core.mu.Unlock()
	selectionIdentity, selectionErr := wal.PublishGenerationSelection(builder)
	core.mu.Lock()
	a.walBaseSelectActive = false
	if selectionIdentity.Valid() {
		a.walBaseSelectPending = true
		a.walBasePending = selectionIdentity
	}
	core.mu.Unlock()
	return selectionErr
}

func walMatchesReplicatedBinding(
	identity raftstore.Identity,
	topologyRecoveryEpoch uint64,
	binding ReplicatedShardStoreBinding,
) bool {
	return identity.ClusterID == binding.ClusterID &&
		identity.ClusterIncarnation == binding.ClusterIncarnation &&
		topologyRecoveryEpoch == binding.TopologyRecoveryEpoch &&
		identity.Distribution == binding.Distribution && identity.Shard == binding.Shard &&
		identity.AllocationGeneration == binding.AllocationGeneration &&
		identity.ShardIncarnation == binding.ShardIncarnation &&
		identity.GroupID == binding.GroupID && identity.MemberID == binding.MemberID &&
		identity.StoreID == binding.StoreID
}

// SettleGenerationActivation implements raftstore's durable state-machine
// half of generation replacement. It reconstructs and authenticates the exact
// retained SQL floor, installs the selected compact snapshot base idempotently,
// and checkpoints the complete group before returning. raftstore keeps the old
// logical WAL leaf until this method succeeds.
func (a *ReplicatedApply) SettleGenerationActivation(
	activation raftstore.GenerationActivation,
) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	certificate, err := openGenerationActivation(activation)
	if err != nil {
		return err
	}

	core := a.database
	core.mu.Lock()
	defer core.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	if a.activationBasePending != ([sha256.Size]byte{}) {
		return ErrReplicatedApplyBasePending
	}
	if a.walBaseCaptureActive || a.walBaseSelectActive {
		return ErrReplicatedApplyBusy
	}
	group := core.checkpointGroup
	base := core.catalog.ReplicatedShardStore
	if group == nil || base == nil || base.RelationCount == 0 ||
		!walBaseManifestMatchesMachine(a, certificate.Manifest, *base) ||
		certificate.Manifest.State.Binding != replicatedStateBindingAt(*base, a.identity.Placement.Range) {
		return ErrReplicatedApplyMismatch
	}
	if !a.walBaseSelectPending {
		return ErrReplicatedApplyBusy
	}
	if !a.walBasePending.Matches(activation.Info) {
		return ErrReplicatedApplyMismatch
	}
	witness, err := group.SealRetentionFloor(certificate.Manifest.State.Applied)
	if err != nil {
		return errors.Join(ErrWALBasePreparation, err)
	}
	if witness.Commitment() != activation.Info.RetentionCommitment {
		return ErrWALBasePreparation
	}
	publication, err := a.machine.InstallSnapshot(activation.Snapshot)
	if err != nil || publication.Applied != certificate.Manifest.State.Applied {
		return errors.Join(ErrWALBasePreparation, err)
	}
	if err := group.Checkpoint(); err != nil {
		return err
	}
	if err := group.ValidateRetentionWitness(witness); err != nil {
		return errors.Join(ErrWALBasePreparation, err)
	}
	return nil
}

// LatchGenerationActivation fences a specially reopened SQL apply claim before
// it is returned to the activation coordinator. The fence remains through all
// WAL rename and family-publication barriers.
func (a *ReplicatedApply) LatchGenerationActivation(
	activation raftstore.GenerationActivation,
) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	certificate, err := openGenerationActivation(activation)
	if err != nil {
		return err
	}
	core := a.database
	core.mu.Lock()
	defer core.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return err
	}
	if a.walBaseCaptureActive || a.walBaseSelectActive {
		return ErrReplicatedApplyBusy
	}
	base := core.catalog.ReplicatedShardStore
	if core.checkpointGroup == nil || base == nil || base.RelationCount == 0 ||
		!walBaseManifestMatchesMachine(a, certificate.Manifest, *base) ||
		certificate.Manifest.State.Binding != replicatedStateBindingAt(*base, a.identity.Placement.Range) ||
		a.machine.Applied() != certificate.Manifest.State.Applied {
		return ErrReplicatedApplyMismatch
	}
	if a.walBaseSelectPending {
		if a.walBasePending.Matches(activation.Info) {
			return nil
		}
		return ErrReplicatedApplyMismatch
	}
	a.walBaseSelectPending = true
	a.walBasePending = raftstore.GenerationActivationIdentity{
		FamilyID:            activation.Info.FamilyID,
		Generation:          activation.Info.Generation,
		BindingDigest:       activation.Info.BindingDigest,
		SnapshotBaseDigest:  activation.Info.SnapshotBaseDigest,
		RetentionCommitment: activation.Info.RetentionCommitment,
	}
	return nil
}

func openGenerationActivation(
	activation raftstore.GenerationActivation,
) (replicatedstate.SnapshotBaseCertificate, error) {
	if activation.Snapshot == nil || activation.Info.Generation == 0 ||
		activation.Info.FamilyID == ([16]byte{}) ||
		activation.Info.BindingDigest == ([sha256.Size]byte{}) ||
		activation.Info.SnapshotBaseDigest == ([sha256.Size]byte{}) ||
		activation.Info.RetentionCommitment == ([sha256.Size]byte{}) ||
		activation.Info.BaseIndex != activation.Snapshot.GetMetadata().GetIndex() ||
		activation.Info.BaseTerm != activation.Snapshot.GetMetadata().GetTerm() ||
		activation.Info.HardCommit < activation.Info.BaseIndex {
		return replicatedstate.SnapshotBaseCertificate{}, ErrWALBasePreparation
	}
	certificate, err := replicatedstate.OpenSnapshotBase(activation.Snapshot)
	if err != nil || certificate.Digest != activation.Info.SnapshotBaseDigest ||
		certificate.Manifest.State.Applied != activation.Info.BaseIndex {
		return replicatedstate.SnapshotBaseCertificate{}, errors.Join(
			ErrWALBasePreparation, err,
		)
	}
	return certificate, nil
}

// CompleteGenerationActivation releases the in-memory SQL fence only after
// raftstore has durably published the exact selected generation as active.
// A zero, stale, foreign, or replayed capability cannot release the fence.
func (a *ReplicatedApply) CompleteGenerationActivation(
	completion raftstore.GenerationActivationCompletion,
) {
	if a == nil || a.database == nil {
		return
	}
	core := a.database
	core.mu.Lock()
	if a.checkLocked() == nil && a.walBaseSelectPending && completion.Matches(
		a.walBasePending.FamilyID,
		a.walBasePending.Generation,
		a.walBasePending.BindingDigest,
	) {
		a.walBaseSelectPending = false
		a.walBasePending = raftstore.GenerationActivationIdentity{}
	}
	core.mu.Unlock()
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

func walBaseRequiredWorkspaceBytes(
	base ReplicatedShardStoreIdentity,
	apply ReplicatedApplyIdentity,
) (int, error) {
	requiredFor := func(limits ReplicatedShardStoreLimits) (int, error) {
		capacity, err := replicatedstate.RequiredSnapshotArtifactPayloadCapacity(
			replicatedstate.DefaultSnapshotArtifactChunkBytes,
			limits.MaxKeyBytes,
			limits.MaxDocumentBytes,
		)
		if err != nil {
			return 0, errors.Join(ErrReplicatedApplyMismatch, err)
		}
		return capacity, nil
	}
	required, err := replicatedstate.RequiredSnapshotArtifactSystemPayloadCapacity(
		replicatedstate.DefaultSnapshotArtifactChunkBytes,
		apply.SystemLimits.MaxKeyBytes, apply.SystemLimits.MaxDocumentBytes)
	if err != nil {
		return 0, err
	}
	for ordinal := range base.Relations {
		capacity, err := requiredFor(base.Relations[ordinal].Limits)
		if err != nil {
			return 0, err
		}
		required = max(required, capacity)
	}
	// Capture rows have their own wider opaque-value ceiling and are streamed
	// directly when exceptional. They still share the same aggregate buffer;
	// validate their frozen bounds without incorrectly applying the user-value
	// ceiling used by RequiredSnapshotArtifactPayloadCapacity.
	if apply.CaptureLimits.MaxKeyBytes <= 0 ||
		apply.CaptureLimits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		apply.CaptureLimits.MaxDocumentBytes <= 0 ||
		apply.CaptureLimits.MaxDocumentBytes > replicatedstate.MaxTransitionCaptureRecordBytes {
		return 0, ErrReplicatedApplyMismatch
	}
	required = max(required, replicatedstate.DefaultSnapshotArtifactChunkBytes)
	return required, nil
}

// equalWALBaseManifestShape binds a streamed certificate to the catalog's
// complete logical relation shape. It deliberately does not compare digest
// fields: the replicated-state machine's relation-manifest digest has a
// different domain and field grammar from the SQL catalog/apply-profile
// digests. Callers which already own a Machine must additionally use
// equalWALBaseManifest with that Machine's exact digest. Snapshot staging
// performs the same exact check while constructing its replicated-state
// bundle candidate from these catalog-checked relations.
func equalWALBaseManifestShape(
	manifest replicatedstate.SnapshotArtifactManifest,
	base ReplicatedShardStoreIdentity,
) bool {
	if base.RelationCount == 0 || len(base.Relations) != int(base.RelationCount) ||
		!equalWALBaseCollection(manifest.UserCollection, base.UserTable) {
		return false
	}
	if base.RelationCount == 1 {
		return !manifest.Bundle && len(manifest.Relations) == 0 &&
			manifest.RelationManifestDigest == ([sha256.Size]byte{})
	}
	if !manifest.Bundle || len(manifest.Relations) != int(base.RelationCount) ||
		manifest.RelationManifestDigest == ([sha256.Size]byte{}) {
		return false
	}
	for ordinal := range base.Relations {
		stored := base.Relations[ordinal]
		artifact := manifest.Relations[ordinal]
		var kind replicatedstate.RelationKind
		switch stored.Kind {
		case ReplicatedShardRelationJSON:
			kind = replicatedstate.RelationJSON
		case ReplicatedShardRelationGlobalIndex:
			kind = replicatedstate.RelationGlobalIndex
		default:
			return false
		}
		if artifact.Relation != replication.RelationID(stored.Relation) ||
			artifact.Kind != kind ||
			!equalWALBaseCollection(artifact.Collection, stored.Table) {
			return false
		}
	}
	return true
}

// equalWALBaseManifest adds the exact replicated-state Machine schema digest
// to the catalog-shape proof. A singleton retains its original artifact
// grammar, which intentionally carries no relation-manifest digest.
func equalWALBaseManifest(
	manifest replicatedstate.SnapshotArtifactManifest,
	base ReplicatedShardStoreIdentity,
	machineDigest [sha256.Size]byte,
) bool {
	if !equalWALBaseManifestShape(manifest, base) || machineDigest == ([sha256.Size]byte{}) {
		return false
	}
	if base.RelationCount == 1 {
		return true
	}
	return manifest.RelationManifestDigest == machineDigest
}

func walBaseManifestMatchesMachine(
	apply *ReplicatedApply,
	manifest replicatedstate.SnapshotArtifactManifest,
	base ReplicatedShardStoreIdentity,
) bool {
	if apply == nil || apply.machine == nil {
		return false
	}
	digest, err := apply.machine.RelationManifestDigest()
	return err == nil && equalWALBaseManifest(manifest, base, digest)
}
