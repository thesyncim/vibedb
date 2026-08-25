package driver

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

var (
	ErrReplicatedChildStageBusy   = errors.New("vibedb: replicated child stage already has an owner")
	ErrReplicatedChildStageClosed = errors.New("vibedb: replicated child stage is closed")
	ErrReplicatedChildStageProof  = errors.New("vibedb: replicated child stage proof mismatch")
)

// ReplicatedChildStage is the exclusive, non-serving SQL ownership claim used
// while one split child is streamed directly into its final user collection.
// It exposes proof-carrying range-split operations but never the collection,
// transaction log, or underlying replicated-state machine.
type ReplicatedChildStage struct {
	mu sync.Mutex

	owner    *dbConnector
	database *database
	table    *table
	base     ReplicatedShardStoreIdentity
	options  ReplicatedApplyOptions
	stage    *rangesplit.ChildStage
	closed   bool
}

// ReplicatedChildActivation is the no-copy handoff from a sealed child stage
// to normal SQL replicated apply. SnapshotBase is the small immutable Raft
// prefix certificate that a new raftstore must use before the claim can serve.
type ReplicatedChildActivation struct {
	Apply            *ReplicatedApply
	ApplyIdentity    ReplicatedApplyIdentity
	SnapshotBase     *pb.Snapshot
	ArtifactManifest replicatedstate.SnapshotArtifactManifest
}

// OpenReplicatedChildStage exclusively claims the already-bound final user
// collection for one exact child artifact. A published apply descriptor is
// accepted only for recovery with an exact sealed cursor and matching options.
func (d *Database) OpenReplicatedChildStage(
	expected ReplicatedShardStoreIdentity,
	partitioner *rangesplit.Partitioner,
	artifact rangesplit.ChildArtifactManifest,
	persistedCursor []byte,
	applyOptions ReplicatedApplyOptions,
	stageOptions rangesplit.ChildStageOptions,
) (*ReplicatedChildStage, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, err
	}
	if err := validateReplicatedApplyOptions(expected, applyOptions); err != nil {
		return nil, err
	}
	if err := validateReplicatedChildArtifactProfile(
		expected, partitioner, artifact, applyOptions,
	); err != nil {
		return nil, err
	}
	expected = ownedReplicatedShardStoreIdentity(expected)
	applyOptions.Placement = ownedReplicatedPlacementProfile(applyOptions.Placement)
	if d == nil || d.connector == nil {
		return nil, ErrDatabaseClosed
	}

	connector := d.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil {
		return nil, ErrDatabaseClosed
	}
	if connector.refs != 0 || connector.exclusive {
		return nil, fmt.Errorf(
			"%w: %d live SQL/apply owner(s)", ErrReplicatedChildStageBusy, connector.refs,
		)
	}
	core := connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed {
		return nil, ErrDatabaseClosed
	}
	if core.replicatedChildStageClaim != nil || core.replicatedApplyClaim != nil {
		return nil, ErrReplicatedChildStageBusy
	}
	if core.catalog.ReplicatedShardStore == nil ||
		!core.catalog.ReplicatedShardStore.Equal(expected) {
		return nil, ErrReplicatedShardStoreIdentityMismatch
	}
	if err := core.settleCatalogLocked(); err != nil {
		return nil, fmt.Errorf(
			"vibedb: settle SQL catalog before child staging: %w", err,
		)
	}
	wantTxnOptions := expectedTxnLogOptions(expected)
	if core.txnLog == nil || core.txnLog.Options() != wantTxnOptions {
		return nil, fmt.Errorf(
			"%w: transaction-marker profile", ErrReplicatedApplyMismatch,
		)
	}
	var markerErr error
	if core.checkpointGroup != nil || core.replicatedSeedPending ||
		core.catalog.ReplicatedApply != nil {
		markerErr = core.txnLog.QualifyMinted()
	} else {
		markerErr = core.txnLog.EnsureMinted()
	}
	if markerErr != nil {
		return nil, fmt.Errorf(
			"vibedb: qualify replicated transaction marker: %w", markerErr,
		)
	}
	t := core.tables[expected.UserTable]
	if t == nil || t.collection == nil {
		return nil, fmt.Errorf(
			"%w: replicated user collection is unavailable", ErrReplicatedApplyMismatch,
		)
	}
	stage, err := rangesplit.NewChildStageWithOptions(
		partitioner, artifact, t.collection, persistedCursor, stageOptions,
	)
	if err != nil {
		return nil, errors.Join(ErrReplicatedChildStageProof, err)
	}
	if core.catalog.ReplicatedApply != nil {
		cursor, ok := stage.Cursor()
		if !ok || cursor.Phase() != rangesplit.ChildStageSealed ||
			!replicatedApplyMetaMatchesOptions(
				core.catalog.ReplicatedApply, expected, applyOptions,
			) || core.replicatedApplyCollection == nil || core.replicatedCaptureCollection == nil {
			return nil, ErrReplicatedChildStageProof
		}
	}
	claim := &ReplicatedChildStage{
		owner: connector, database: core, table: t, base: expected,
		options: applyOptions, stage: stage,
	}
	core.replicatedChildStageClaim = claim
	connector.exclusive = true
	connector.refs++
	return claim, nil
}

func validateReplicatedChildArtifactProfile(
	expected ReplicatedShardStoreIdentity,
	partitioner *rangesplit.Partitioner,
	artifact rangesplit.ChildArtifactManifest,
	options ReplicatedApplyOptions,
) error {
	if partitioner == nil || !artifact.Present || artifact.PlanDigest != partitioner.Digest() ||
		partitioner.CollectionName() != expected.UserTable ||
		string(partitioner.SourceDistribution()) != expected.Binding.Distribution ||
		string(artifact.Descriptor.Shard) != expected.Binding.Shard ||
		uint64(artifact.Descriptor.AllocationGeneration) != expected.Binding.AllocationGeneration ||
		uint64(artifact.Descriptor.OwnershipEpoch) != expected.Binding.Authority.OwnershipEpoch ||
		uint64(artifact.TargetRoutingVersion) != expected.Binding.Authority.RoutingVersion ||
		artifact.Descriptor.Range != options.Placement.Range {
		return ErrReplicatedChildStageProof
	}
	program, err := distribution.CompileDocumentPointProgram(
		[]string{options.Placement.ShardKey}, distribution.DefaultVirtualBucketBits,
	)
	if err != nil || program.Digest() != artifact.PlacementDigest {
		return errors.Join(ErrReplicatedChildStageProof, err)
	}
	return nil
}

func expectedTxnLogOptions(
	expected ReplicatedShardStoreIdentity,
) (result durable.TxnLogOptions) {
	result.Capacity = expected.Sidecars.TransactionMarkerBytes
	result.SealedCapacity = true
	return result
}

// Cursor returns the detached current durable stage cursor.
func (s *ReplicatedChildStage) Cursor() (rangesplit.ChildStageCursor, bool) {
	if s == nil {
		return rangesplit.ChildStageCursor{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stage == nil {
		return rangesplit.ChildStageCursor{}, false
	}
	return s.stage.Cursor()
}

// ArtifactComplete reports whether the complete artifact has entered tail
// catch-up. It grants no serving or apply authority.
func (s *ReplicatedChildStage) ArtifactComplete() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.stage != nil && s.stage.ArtifactComplete()
}

// ReceiveArtifact streams verified child chunks directly into the final user
// collection while the exclusive non-serving claim is held.
func (s *ReplicatedChildStage) ReceiveArtifact(
	r io.Reader,
	persist rangesplit.ChildStageCursorPersistence,
) (rangesplit.ChildArtifactManifest, error) {
	if s == nil {
		return rangesplit.ChildArtifactManifest{}, ErrReplicatedChildStageClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stage == nil {
		return rangesplit.ChildArtifactManifest{}, ErrReplicatedChildStageClosed
	}
	return s.stage.ReceiveArtifact(r, persist)
}

// ApplyTailBatch applies one verified consecutive translated batch directly to
// the final user collection and persists its caller-owned progress cursor.
func (s *ReplicatedChildStage) ApplyTailBatch(
	batch rangesplit.TailBatch,
	persist rangesplit.ChildStageCursorPersistence,
) error {
	if s == nil {
		return ErrReplicatedChildStageClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stage == nil {
		return ErrReplicatedChildStageClosed
	}
	return s.stage.ApplyTailBatch(batch, persist)
}

// Activate publishes or reuses the hidden apply participant, initializes its
// state row against the sealed child image, and atomically transfers this
// exclusive claim to normal ReplicatedApply. The returned snapshot base still
// must initialize the destination raftstore before serving.
func (s *ReplicatedChildStage) Activate(
	certificate rangesplit.CutoverCertificate,
	staticBootstrap *pb.Snapshot,
	artifactOptions replicatedstate.SnapshotArtifactOptions,
) (ReplicatedChildActivation, error) {
	return s.activate(certificate, staticBootstrap, artifactOptions, nil)
}

func (s *ReplicatedChildStage) activate(
	certificate rangesplit.CutoverCertificate,
	staticBootstrap *pb.Snapshot,
	artifactOptions replicatedstate.SnapshotArtifactOptions,
	persist func(*database) (bool, error),
) (ReplicatedChildActivation, error) {
	if s == nil {
		return ReplicatedChildActivation{}, ErrReplicatedChildStageClosed
	}
	if err := replicatedstate.ValidateSnapshotArtifactOptions(artifactOptions); err != nil {
		return ReplicatedChildActivation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stage == nil || s.owner == nil || s.database == nil {
		return ReplicatedChildActivation{}, ErrReplicatedChildStageClosed
	}
	if err := s.stage.CheckActivationCoordinates(
		certificate, replicatedStateBinding(s.base),
	); err != nil {
		return ReplicatedChildActivation{}, errors.Join(
			ErrReplicatedChildStageProof, err,
		)
	}

	connector := s.owner
	connector.mu.Lock()
	defer connector.mu.Unlock()
	core := s.database
	core.mu.Lock()
	defer core.mu.Unlock()
	if connector.closed || connector.db != core || !connector.exclusive ||
		connector.refs != 1 || core.closed || core.replicatedChildStageClaim != s ||
		core.replicatedApplyClaim != nil || core.catalog.ReplicatedShardStore == nil ||
		!core.catalog.ReplicatedShardStore.Equal(s.base) ||
		core.tables[s.base.UserTable] != s.table {
		return ReplicatedChildActivation{}, ErrReplicatedChildStageClosed
	}
	if err := core.settleCatalogLocked(); err != nil {
		return ReplicatedChildActivation{}, fmt.Errorf(
			"vibedb: settle SQL catalog before child activation: %w", err,
		)
	}
	if core.txnLog == nil || core.txnLog.Options() != expectedTxnLogOptions(s.base) {
		return ReplicatedChildActivation{}, ErrReplicatedApplyMismatch
	}
	var markerErr error
	if core.checkpointGroup != nil || core.replicatedSeedPending ||
		core.catalog.ReplicatedApply != nil {
		markerErr = core.txnLog.QualifyMinted()
	} else {
		markerErr = core.txnLog.EnsureMinted()
	}
	if markerErr != nil {
		return ReplicatedChildActivation{}, fmt.Errorf(
			"vibedb: qualify replicated transaction marker: %w", markerErr,
		)
	}
	identity, err := core.prepareReplicatedApplyStorageLocked(
		s.base, s.options, persist,
	)
	result := ReplicatedChildActivation{ApplyIdentity: identity}
	// Once the apply descriptor is durable or its publication outcome is
	// unknown, the staged user image has durable activation intent. Keep it
	// non-serving even if this stage is later closed after any activation error.
	// A zero identity means descriptor publication failed definitively and the
	// unpublished participant was discharged, so that case remains resumable as
	// an ordinary pre-activation stage.
	if identity != (ReplicatedApplyIdentity{}) {
		core.replicatedSeedPending = true
	}
	if err != nil {
		return result, err
	}
	claim := &ReplicatedApply{
		owner: connector, database: core, table: s.table, identity: identity,
		exclusiveConnector: true,
	}
	validator := newReplicatedSQLMutationValidator(
		s.base, s.table, identity.Placement,
	)
	groupMembers := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: core.replicatedApplyCollection},
		{Name: s.base.UserTable, Collection: s.table.collection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: core.replicatedCaptureCollection},
	}
	prepared, err := s.stage.PrepareReplicatedChild(
		certificate,
		rangesplit.ChildActivationTarget{
			Binding: replicatedStateBinding(s.base), StaticBootstrap: staticBootstrap,
			System: replicatedstate.CollectionTarget{
				Collection: core.replicatedApplyCollection,
				Validation: replicatedstate.ValidationOpaqueBinary,
				Limits:     replicatedStateCollectionLimits(identity.SystemLimits),
			},
			User: replicatedstate.UserCollection{
				Name: s.base.UserTable,
				Target: replicatedstate.CollectionTarget{
					Collection:       s.table.collection,
					Validation:       replicatedstate.ValidationProfile(identity.ValidationProfile),
					ValidationDigest: identity.ValidationDigest,
					Validator:        validator, ObserveMutationAttempt: claim.observeMutationAttempt,
					Limits: replicatedStateCollectionLimits(s.base.UserLimits),
				},
			},
			TxnLog: core.txnLog,
			MachineOptions: replicatedstate.Options{
				TxnLimits: identity.TxnLimits, MaxSessions: identity.MaxSessions,
				RetryWindow: identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
				TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
					Name:       replicatedstate.TransitionCaptureCollectionName,
					Collection: core.replicatedCaptureCollection,
				},
			},
			ArtifactOptions: artifactOptions,
		},
	)
	if err != nil {
		return result, fmt.Errorf("vibedb: activate replicated child: %w", err)
	}
	seedEnvelope := prepared.AppendSeedEnvelope(nil)
	seedKey := prepared.AppendSeedKey(nil)
	seed := durable.CheckpointGroupSeed{
		Applied: prepared.AppliedIndex(), Member: prepared.SeedMember(),
		Envelope: seedEnvelope,
		Images: []durable.CheckpointGroupSeedImage{{
			Collection: s.table.collection, Generation: prepared.UserGeneration(),
		}},
	}
	if core.checkpointGroup == nil {
		core.checkpointGroup, err = durable.NewSeededCheckpointGroup(
			core.txnLog, groupMembers, seed, durable.CheckpointGroupOptions{},
		)
		if err != nil {
			return result, fmt.Errorf(
				"vibedb: certify replicated child seed: %w", err,
			)
		}
	} else if !core.checkpointGroup.Owns(groupMembers) {
		return result, fmt.Errorf(
			"%w: checkpoint-group membership", ErrReplicatedApplyMismatch,
		)
	}
	if core.checkpointGroup.SeedActivationPending() {
		if err := core.checkpointGroup.Seed(
			seed, groupMembers[0], identity.TxnLimits, seedKey,
		); err != nil {
			return result, fmt.Errorf("vibedb: publish replicated child seed: %w", err)
		}
	}
	machine, base, manifest, err := prepared.Finish(core.checkpointGroup)
	if err != nil {
		return result, fmt.Errorf("vibedb: finish replicated child: %w", err)
	}
	baseCertificate, err := replicatedstate.OpenSnapshotBase(base)
	if err != nil {
		return result, fmt.Errorf("vibedb: authenticate replicated child base: %w", err)
	}
	claim.machine = machine
	claim.activationBasePending = baseCertificate.Digest
	core.replicatedChildStageClaim = nil
	core.replicatedApplyClaim = claim
	s.closed = true
	s.stage = nil
	result.Apply = claim
	result.SnapshotBase = base
	result.ArtifactManifest = manifest
	return result, nil
}

// Close releases an unactivated stage and its exclusive connector reference.
// It never removes staged rows or durable progress; an exact reopen resumes it.
func (s *ReplicatedChildStage) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	connector := s.owner
	core := s.database
	if connector == nil || core == nil {
		s.closed = true
		return nil
	}
	connector.mu.Lock()
	core.mu.Lock()
	if core.replicatedChildStageClaim != s || !connector.exclusive {
		core.mu.Unlock()
		connector.mu.Unlock()
		return ErrReplicatedChildStageClosed
	}
	core.replicatedChildStageClaim = nil
	connector.exclusive = false
	s.closed = true
	s.stage = nil
	core.mu.Unlock()
	connector.mu.Unlock()
	return connector.release()
}
