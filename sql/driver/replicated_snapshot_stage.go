package driver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrReplicatedSnapshotStageBusy   = errors.New("vibedb: replicated snapshot stage already has an owner")
	ErrReplicatedSnapshotStageClosed = errors.New("vibedb: replicated snapshot stage is closed")
	ErrReplicatedSnapshotStageProof  = errors.New("vibedb: replicated snapshot stage proof mismatch")
	replicatedSnapshotStageFaultHook func(replicatedSnapshotStageFaultPoint) error
)

type replicatedSnapshotStageFaultPoint uint8

const (
	replicatedSnapshotStageAfterGroupCreate replicatedSnapshotStageFaultPoint = iota + 1
	replicatedSnapshotStageAfterSeed
	replicatedSnapshotStageAfterMachineOpen
	replicatedSnapshotStageAfterSnapshotInstall
)

func replicatedSnapshotStageFault(point replicatedSnapshotStageFaultPoint) error {
	if replicatedSnapshotStageFaultHook == nil {
		return nil
	}
	return replicatedSnapshotStageFaultHook(point)
}

// ResumeReplicatedSnapshotActivation reclaims an already-certified snapshot
// activation after process restart. resumed is false only while no complete
// snapshot-base transition exists, in which case the caller must resume the
// artifact stage. A completed but non-matching transition fails closed.
func (d *Database) ResumeReplicatedSnapshotActivation(
	expected ReplicatedShardStoreIdentity,
	manifest replicatedstate.SnapshotArtifactManifest,
	staticBootstrap *pb.Snapshot,
	applyOptions ReplicatedApplyOptions,
) (activation ReplicatedChildActivation, resumed bool, err error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return activation, false, err
	}
	if err := validateReplicatedApplyOptions(expected, applyOptions); err != nil {
		return activation, false, err
	}
	if d == nil || d.connector == nil {
		return activation, false, ErrDatabaseClosed
	}
	connector := d.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil || connector.refs != 0 || connector.exclusive {
		return activation, false, ErrReplicatedSnapshotStageBusy
	}
	core := connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed || core.replicatedApplyClaim != nil || core.replicatedChildStageClaim != nil ||
		core.replicatedSnapshotStageClaim != nil {
		return activation, false, ErrReplicatedSnapshotStageBusy
	}
	if core.checkpointGroup == nil {
		return activation, false, nil
	}
	seedApplied, seeded := core.checkpointGroup.SeedAppliedIndex()
	if !seeded || seedApplied != manifest.State.Applied {
		return activation, false, fmt.Errorf(
			"%w: durable seed cut %d/%d seeded=%v",
			ErrReplicatedSnapshotStageProof, seedApplied, manifest.State.Applied, seeded,
		)
	}
	if core.checkpointGroup.SeedActivationPending() {
		return activation, false, nil
	}
	if core.catalog.ReplicatedShardStore == nil ||
		!core.catalog.ReplicatedShardStore.Equal(expected) ||
		core.catalog.ReplicatedApply == nil || manifest.State.Binding != replicatedStateBinding(expected) ||
		!bytes.Equal(manifest.UserCollection, []byte(expected.UserTable)) {
		return activation, false, fmt.Errorf("%w: durable activation identity", ErrReplicatedSnapshotStageProof)
	}
	if err := core.settleCatalogLocked(); err != nil {
		return activation, false, err
	}
	table := core.tables[expected.UserTable]
	if table == nil || table.collection == nil || core.replicatedApplyCollection == nil ||
		core.replicatedCaptureCollection == nil {
		return activation, false, fmt.Errorf("%w: durable activation collections", ErrReplicatedSnapshotStageProof)
	}
	identity, err := core.prepareReplicatedApplyStorageLocked(expected, applyOptions, nil)
	if err != nil {
		return activation, false, err
	}
	snapshotBase, err := replicatedstate.BuildSnapshotBase(manifest, staticBootstrap)
	if err != nil {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(snapshotBase)
	if err != nil {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	claim := &ReplicatedApply{owner: connector, database: core, table: table,
		identity: identity, exclusiveConnector: true}
	machine, err := replicatedstate.Open(
		replicatedStateBinding(expected), staticBootstrap,
		replicatedstate.CollectionTarget{Collection: core.replicatedApplyCollection,
			Validation: replicatedstate.ValidationOpaqueBinary,
			Limits:     replicatedStateCollectionLimits(identity.SystemLimits)},
		replicatedstate.UserCollection{Name: expected.UserTable, Target: replicatedstate.CollectionTarget{
			Collection: table.collection, Validation: replicatedstate.ValidationDeterministicMutation,
			ValidationDigest:       identity.ValidationDigest,
			Validator:              newReplicatedSQLMutationValidator(expected, table, identity.Placement),
			ObserveMutationAttempt: claim.observeMutationAttempt,
			Limits:                 replicatedStateCollectionLimits(expected.UserLimits),
		}}, core.txnLog, replicatedstate.Options{
			TxnLimits: identity.TxnLimits, MaxSessions: identity.MaxSessions,
			RetryWindow: identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
			TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
				Name:       replicatedstate.TransitionCaptureCollectionName,
				Collection: core.replicatedCaptureCollection,
			},
		},
	)
	if err != nil {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	cut, err := machine.Snapshot(expected.UserTable)
	if err != nil {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	current, writeErr := replicatedstate.WriteSnapshotArtifact(io.Discard, cut,
		replicatedstate.SnapshotArtifactOptions{TargetChunkBytes: int(manifest.TargetChunkBytes)})
	closeErr := cut.Close()
	currentState := current.State
	if currentState.SnapshotBaseDigest != certificate.Digest {
		return activation, false, errors.Join(
			fmt.Errorf("%w: durable snapshot-base digest", ErrReplicatedSnapshotStageProof),
			writeErr, closeErr,
		)
	}
	currentState.SnapshotBaseDigest = manifest.State.SnapshotBaseDigest
	currentEnvelope, currentErr := replicatedstate.AppendState(nil, currentState)
	expectedEnvelope, expectedErr := replicatedstate.AppendState(nil, manifest.State)
	if writeErr != nil || closeErr != nil || currentErr != nil || expectedErr != nil ||
		!bytes.Equal(currentEnvelope, expectedEnvelope) ||
		!bytes.Equal(current.UserCollection, manifest.UserCollection) ||
		current.ImageDigest != manifest.ImageDigest || current.SystemRows != manifest.SystemRows ||
		current.UserRows != manifest.UserRows || current.CaptureRows != manifest.CaptureRows ||
		current.CaptureImageDigest != manifest.CaptureImageDigest {
		return activation, false, errors.Join(
			fmt.Errorf("%w: durable activation image", ErrReplicatedSnapshotStageProof),
			writeErr, closeErr, currentErr, expectedErr,
		)
	}
	claim.machine = machine
	claim.activationBasePending = certificate.Digest
	core.replicatedApplyClaim = claim
	core.replicatedSeedPending = true
	connector.exclusive = true
	connector.refs++
	return ReplicatedChildActivation{Apply: claim, ApplyIdentity: identity,
		SnapshotBase: snapshotBase, ArtifactManifest: ownedReplicatedSnapshotManifest(manifest)}, true, nil
}

// ReplicatedSnapshotStage is the exclusive non-serving owner of an empty
// replica while one authenticated snapshot artifact is written directly into
// its final hidden and user collections. Artifact memory is bounded by the
// replicated-state verifier; no complete artifact copy is retained here.
type ReplicatedSnapshotStage struct {
	mu sync.Mutex

	owner             *dbConnector
	database          *database
	table             *table
	base              ReplicatedShardStoreIdentity
	identity          ReplicatedApplyIdentity
	expected          replicatedstate.SnapshotArtifactManifest
	stage             *replicatedstate.SnapshotArtifactStage
	candidateProved   bool
	seedEnvelope      []byte
	seedKey           []byte
	claim             *ReplicatedApply
	machine           *replicatedstate.Machine
	snapshotInstalled bool
	snapshotBase      *pb.Snapshot
	activation        ReplicatedChildActivation
	closed            bool
}

// OpenReplicatedSnapshotStage creates or resumes the sole non-serving snapshot
// receiver. A durable apply descriptor may already exist only through the
// explicit child/snapshot-stage recovery open policy; ordinary SQL/apply opens
// remain fail-closed until Activate installs the exact immutable snapshot base.
func (d *Database) OpenReplicatedSnapshotStage(
	expected ReplicatedShardStoreIdentity,
	manifest replicatedstate.SnapshotArtifactManifest,
	persistedCursor []byte,
	applyOptions ReplicatedApplyOptions,
	stageOptions replicatedstate.SnapshotArtifactStageOptions,
) (*ReplicatedSnapshotStage, ReplicatedApplyIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if err := validateReplicatedApplyOptions(expected, applyOptions); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if expected.RelationCount != 1 || expected.Relations[0].Kind != ReplicatedShardRelationJSON ||
		expected.Relations[0].Table != expected.UserTable ||
		!bytes.Equal(manifest.UserCollection, []byte(expected.UserTable)) ||
		manifest.State.Binding != replicatedStateBinding(expected) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedSnapshotStageProof
	}
	expected = ownedReplicatedShardStoreIdentity(expected)
	applyOptions.Placement = ownedReplicatedPlacementProfile(applyOptions.Placement)
	if d == nil || d.connector == nil {
		return nil, ReplicatedApplyIdentity{}, ErrDatabaseClosed
	}
	connector := d.connector
	connector.mu.Lock()
	defer connector.mu.Unlock()
	if connector.closed || connector.db == nil {
		return nil, ReplicatedApplyIdentity{}, ErrDatabaseClosed
	}
	if connector.refs != 0 || connector.exclusive {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedSnapshotStageBusy
	}
	core := connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed || core.replicatedApplyClaim != nil ||
		core.replicatedChildStageClaim != nil || core.replicatedSnapshotStageClaim != nil {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedSnapshotStageBusy
	}
	if core.catalog.ReplicatedShardStore == nil ||
		!core.catalog.ReplicatedShardStore.Equal(expected) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedShardStoreIdentityMismatch
	}
	if err := core.settleCatalogLocked(); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if core.txnLog == nil || core.txnLog.Options() != expectedTxnLogOptions(expected) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
	}
	t := core.tables[expected.UserTable]
	if t == nil || t.collection == nil {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
	}
	if core.catalog.ReplicatedApply == nil && (t.collection.Len() != 0 || len(persistedCursor) != 0) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedSnapshotStageProof
	}
	if core.checkpointGroup != nil {
		cursor, cursorErr := replicatedstate.OpenSnapshotArtifactCursor(persistedCursor)
		members := []durable.NamedCollection{
			{Name: replicatedstate.SystemCollectionName, Collection: core.replicatedApplyCollection},
			{Name: expected.UserTable, Collection: t.collection},
			{Name: replicatedstate.TransitionCaptureCollectionName, Collection: core.replicatedCaptureCollection},
		}
		offset := cursor.Offset()
		owns := core.checkpointGroup.Owns(members)
		footerOffset, footerBound := uint64(0), manifest.EncodedBytes >= replicatedstate.SnapshotArtifactFooterBytes
		if footerBound {
			footerOffset = manifest.EncodedBytes - replicatedstate.SnapshotArtifactFooterBytes
		}
		if !footerBound || cursorErr != nil || offset != footerOffset || !owns {
			return nil, ReplicatedApplyIdentity{}, errors.Join(
				fmt.Errorf("%w: resumed cursor %d/%d group-owned=%v",
					ErrReplicatedSnapshotStageProof, offset, manifest.EncodedBytes, owns),
				cursorErr,
			)
		}
	}
	identity, err := core.prepareReplicatedApplyStorageLocked(expected, applyOptions, nil)
	if err != nil {
		return nil, identity, err
	}
	if core.replicatedApplyCollection == nil || core.replicatedCaptureCollection == nil {
		return nil, identity, ErrReplicatedApplyMismatch
	}
	validatorClaim := &ReplicatedApply{owner: connector, database: core, table: t, identity: identity}
	stageOptions.Capture = replicatedstate.CollectionTarget{
		Collection: core.replicatedCaptureCollection,
		Validation: replicatedstate.ValidationOpaqueBinary,
		Limits: replicatedstate.CollectionLimits{
			MaxKeyBytes:          core.replicatedCaptureCollection.MaxKeyBytes(),
			MaxDocumentBytes:     core.replicatedCaptureCollection.MaxDocumentBytes(),
			MaxDistinctMutations: core.replicatedCaptureCollection.MaxBatchDocuments(),
			MaxBatchBytes:        core.replicatedCaptureCollection.MaxBatchBytes(),
		},
	}
	stage, err := replicatedstate.NewSnapshotArtifactStageWithOptions(
		manifest,
		replicatedstate.CollectionTarget{
			Collection: core.replicatedApplyCollection,
			Validation: replicatedstate.ValidationOpaqueBinary,
			Limits:     replicatedStateCollectionLimits(identity.SystemLimits),
		},
		replicatedstate.CollectionTarget{
			Collection:             t.collection,
			Validation:             replicatedstate.ValidationDeterministicMutation,
			ValidationDigest:       identity.ValidationDigest,
			Validator:              newReplicatedSQLMutationValidator(expected, t, identity.Placement),
			ObserveMutationAttempt: validatorClaim.observeMutationAttempt,
			Limits:                 replicatedStateCollectionLimits(expected.UserLimits),
		},
		persistedCursor, stageOptions,
	)
	if err != nil {
		return nil, identity, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	manifest = ownedReplicatedSnapshotManifest(manifest)
	claim := &ReplicatedSnapshotStage{owner: connector, database: core, table: t,
		base: expected, identity: identity,
		expected: manifest, stage: stage}
	core.replicatedSnapshotStageClaim = claim
	core.replicatedSeedPending = true
	connector.exclusive = true
	connector.refs++
	return claim, identity, nil
}

func ownedReplicatedSnapshotManifest(
	manifest replicatedstate.SnapshotArtifactManifest,
) replicatedstate.SnapshotArtifactManifest {
	manifest.UserCollection = bytes.Clone(manifest.UserCollection)
	manifest.State.Binding.Distribution = strings.Clone(manifest.State.Binding.Distribution)
	manifest.State.Binding.Shard = strings.Clone(manifest.State.Binding.Shard)
	if manifest.State.ConfState != nil {
		manifest.State.ConfState = proto.Clone(manifest.State.ConfState).(*pb.ConfState)
	}
	return manifest
}

func (s *ReplicatedSnapshotStage) Offset() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stage == nil {
		return 0
	}
	return s.stage.Offset()
}

func (s *ReplicatedSnapshotStage) Receive(
	r io.Reader,
	persist replicatedstate.SnapshotCursorPersistence,
) (replicatedstate.SnapshotArtifactManifest, error) {
	if s == nil {
		return replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedSnapshotStageClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.stage == nil {
		return replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedSnapshotStageClosed
	}
	return s.stage.Receive(r, persist)
}

// Activate authenticates both final collection images, installs the exact
// snapshot base, and transfers the exclusive stage to ReplicatedApply. It does
// not create a WAL, mint a node incarnation, join multiraft, promote the
// learner, or grant serving authority.
func (s *ReplicatedSnapshotStage) Activate(
	staticBootstrap *pb.Snapshot,
) (ReplicatedChildActivation, error) {
	if s == nil {
		return ReplicatedChildActivation{}, ErrReplicatedSnapshotStageClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.activation.Apply != nil {
		return ownedReplicatedSnapshotActivation(s.activation), nil
	}
	if s.closed || s.stage == nil || s.owner == nil || s.database == nil {
		return ReplicatedChildActivation{}, ErrReplicatedSnapshotStageClosed
	}
	connector, core := s.owner, s.database
	connector.mu.Lock()
	defer connector.mu.Unlock()
	core.mu.Lock()
	defer core.mu.Unlock()
	if connector.closed || connector.db != core || !connector.exclusive || connector.refs != 1 ||
		core.closed || core.replicatedSnapshotStageClaim != s || core.replicatedApplyClaim != nil {
		return ReplicatedChildActivation{}, ErrReplicatedSnapshotStageClosed
	}
	members := []durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: core.replicatedApplyCollection},
		{Name: s.base.UserTable, Collection: s.table.collection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: core.replicatedCaptureCollection},
	}
	var err error
	if s.snapshotBase == nil {
		s.snapshotBase, err = replicatedstate.BuildSnapshotBase(s.expected, staticBootstrap)
		if err != nil {
			return ReplicatedChildActivation{}, err
		}
	} else {
		candidate, buildErr := replicatedstate.BuildSnapshotBase(s.expected, staticBootstrap)
		if buildErr != nil || !proto.Equal(candidate, s.snapshotBase) {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, buildErr)
		}
	}
	if !s.candidateProved && core.checkpointGroup == nil {
		_, err = s.stage.OpenCandidate(staticBootstrap, core.txnLog, replicatedstate.Options{
			TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
			RetryWindow: s.identity.RetryWindow,
			TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
				Name:       replicatedstate.TransitionCaptureCollectionName,
				Collection: core.replicatedCaptureCollection,
			},
		})
		if err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
		s.seedEnvelope = s.stage.AppendSeedEnvelope(s.seedEnvelope[:0])
		s.seedKey = s.stage.AppendSeedKey(s.seedKey[:0])
		s.candidateProved = true
	} else if !s.candidateProved {
		s.seedKey, s.seedEnvelope, err = s.stage.AppendRecoveredSeed(
			nil, nil, core.checkpointGroup, replicatedstate.SystemCollectionName,
		)
		if err != nil || !core.checkpointGroup.Owns(members) {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
		s.candidateProved = true
	}
	if len(s.seedEnvelope) == 0 || len(s.seedKey) == 0 {
		return ReplicatedChildActivation{}, ErrReplicatedSnapshotStageProof
	}
	seed := durable.CheckpointGroupSeed{
		Applied: s.expected.State.Applied, Member: replicatedstate.SystemCollectionName,
		Envelope: s.seedEnvelope,
		Images: []durable.CheckpointGroupSeedImage{
			{Collection: core.replicatedApplyCollection, Generation: core.replicatedApplyCollection.Generation()},
			{Collection: s.table.collection, Generation: s.table.collection.Generation()},
			{Collection: core.replicatedCaptureCollection, Generation: core.replicatedCaptureCollection.Generation()},
		},
	}
	if core.checkpointGroup == nil {
		core.checkpointGroup, err = durable.NewSeededCheckpointGroup(
			core.txnLog, members, seed, durable.CheckpointGroupOptions{},
		)
		if err != nil || !core.checkpointGroup.Owns(members) {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
		if err = replicatedSnapshotStageFault(replicatedSnapshotStageAfterGroupCreate); err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
	}
	if !s.snapshotInstalled && core.checkpointGroup.SeedActivationPending() {
		if err = core.checkpointGroup.Seed(seed, members[0], s.identity.TxnLimits, s.seedKey); err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
		if err = replicatedSnapshotStageFault(replicatedSnapshotStageAfterSeed); err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
	}
	if s.claim == nil {
		s.claim = &ReplicatedApply{owner: connector, database: core, table: s.table,
			identity: s.identity, exclusiveConnector: true}
	}
	if s.machine == nil {
		s.machine, err = replicatedstate.Open(
			replicatedStateBinding(s.base), staticBootstrap,
			replicatedstate.CollectionTarget{
				Collection: core.replicatedApplyCollection,
				Validation: replicatedstate.ValidationOpaqueBinary,
				Limits:     replicatedStateCollectionLimits(s.identity.SystemLimits),
			},
			replicatedstate.UserCollection{Name: s.base.UserTable, Target: replicatedstate.CollectionTarget{
				Collection:             s.table.collection,
				Validation:             replicatedstate.ValidationDeterministicMutation,
				ValidationDigest:       s.identity.ValidationDigest,
				Validator:              newReplicatedSQLMutationValidator(s.base, s.table, s.identity.Placement),
				ObserveMutationAttempt: s.claim.observeMutationAttempt,
				Limits:                 replicatedStateCollectionLimits(s.base.UserLimits),
			}},
			core.txnLog, replicatedstate.Options{
				TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
				RetryWindow: s.identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
				TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
					Name:       replicatedstate.TransitionCaptureCollectionName,
					Collection: core.replicatedCaptureCollection,
				},
			},
		)
		if err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
		if err = replicatedSnapshotStageFault(replicatedSnapshotStageAfterMachineOpen); err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
	}
	publication, err := s.machine.InstallSnapshot(s.snapshotBase)
	if err != nil || publication.Applied != s.expected.State.Applied ||
		publication.ReplicaSetVersion != s.expected.State.ReplicaSetVersion {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	s.snapshotInstalled = true
	if err = replicatedSnapshotStageFault(replicatedSnapshotStageAfterSnapshotInstall); err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(s.snapshotBase)
	if err != nil {
		return ReplicatedChildActivation{}, err
	}
	s.claim.machine = s.machine
	s.claim.activationBasePending = certificate.Digest
	core.replicatedSnapshotStageClaim = nil
	core.replicatedApplyClaim = s.claim
	core.replicatedSeedPending = true
	result := ReplicatedChildActivation{Apply: s.claim, ApplyIdentity: s.identity,
		SnapshotBase: s.snapshotBase, ArtifactManifest: s.expected}
	s.activation = ownedReplicatedSnapshotActivation(result)
	s.closed, s.stage = true, nil
	return ownedReplicatedSnapshotActivation(result), nil
}

func ownedReplicatedSnapshotActivation(
	activation ReplicatedChildActivation,
) ReplicatedChildActivation {
	activation.ArtifactManifest = ownedReplicatedSnapshotManifest(activation.ArtifactManifest)
	if activation.SnapshotBase != nil {
		activation.SnapshotBase = proto.Clone(activation.SnapshotBase).(*pb.Snapshot)
	}
	return activation
}

func (s *ReplicatedSnapshotStage) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	connector, core := s.owner, s.database
	if connector == nil || core == nil {
		s.closed = true
		return nil
	}
	connector.mu.Lock()
	core.mu.Lock()
	if core.replicatedSnapshotStageClaim != s || !connector.exclusive {
		core.mu.Unlock()
		connector.mu.Unlock()
		return ErrReplicatedSnapshotStageClosed
	}
	core.replicatedSnapshotStageClaim = nil
	connector.exclusive = false
	s.closed, s.stage = true, nil
	core.mu.Unlock()
	connector.mu.Unlock()
	return connector.release()
}
