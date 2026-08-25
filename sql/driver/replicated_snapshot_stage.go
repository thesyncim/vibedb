package driver

import (
	"bytes"
	"errors"
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
)

// ReplicatedSnapshotStage is the exclusive non-serving owner of an empty
// replica while one authenticated snapshot artifact is written directly into
// its final hidden and user collections. Artifact memory is bounded by the
// replicated-state verifier; no complete artifact copy is retained here.
type ReplicatedSnapshotStage struct {
	mu sync.Mutex

	owner      *dbConnector
	database   *database
	table      *table
	base       ReplicatedShardStoreIdentity
	identity   ReplicatedApplyIdentity
	expected   replicatedstate.SnapshotArtifactManifest
	stage      *replicatedstate.SnapshotArtifactStage
	activation ReplicatedChildActivation
	closed     bool
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
		}
		if cursorErr != nil || cursor.Offset() != manifest.EncodedBytes ||
			!core.checkpointGroup.Owns(members) {
			return nil, ReplicatedApplyIdentity{}, errors.Join(
				ErrReplicatedSnapshotStageProof, cursorErr,
			)
		}
	}
	identity, err := core.prepareReplicatedApplyStorageLocked(expected, applyOptions, nil)
	if err != nil {
		return nil, identity, err
	}
	if core.replicatedApplyCollection == nil {
		return nil, identity, ErrReplicatedApplyMismatch
	}
	validatorClaim := &ReplicatedApply{owner: connector, database: core, table: t, identity: identity}
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
	}
	var seedEnvelope, seedKey []byte
	var err error
	if core.checkpointGroup == nil {
		_, err = s.stage.OpenCandidate(staticBootstrap, core.txnLog, replicatedstate.Options{
			TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
			RetryWindow: s.identity.RetryWindow,
		})
		if err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
		seedEnvelope = s.stage.AppendSeedEnvelope(nil)
		seedKey = s.stage.AppendSeedKey(nil)
	} else {
		seedKey, seedEnvelope, err = s.stage.AppendRecoveredSeed(
			nil, nil, core.checkpointGroup, replicatedstate.SystemCollectionName,
		)
		if err != nil || !core.checkpointGroup.Owns(members) {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
	}
	if len(seedEnvelope) == 0 || len(seedKey) == 0 {
		return ReplicatedChildActivation{}, ErrReplicatedSnapshotStageProof
	}
	seed := durable.CheckpointGroupSeed{
		Applied: s.expected.State.Applied, Member: replicatedstate.SystemCollectionName,
		Envelope: seedEnvelope,
		Images: []durable.CheckpointGroupSeedImage{
			{Collection: core.replicatedApplyCollection, Generation: core.replicatedApplyCollection.Generation()},
			{Collection: s.table.collection, Generation: s.table.collection.Generation()},
		},
	}
	if core.checkpointGroup == nil {
		core.checkpointGroup, err = durable.NewSeededCheckpointGroup(
			core.txnLog, members, seed, durable.CheckpointGroupOptions{},
		)
		if err != nil || !core.checkpointGroup.Owns(members) {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
		}
	}
	if err = core.checkpointGroup.Seed(seed, members[0], s.identity.TxnLimits, seedKey); err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	claim := &ReplicatedApply{owner: connector, database: core, table: s.table,
		identity: s.identity, exclusiveConnector: true}
	machine, err := replicatedstate.Open(
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
			ObserveMutationAttempt: claim.observeMutationAttempt,
			Limits:                 replicatedStateCollectionLimits(s.base.UserLimits),
		}},
		core.txnLog, replicatedstate.Options{
			TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
			RetryWindow: s.identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
		},
	)
	if err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	base, err := replicatedstate.BuildSnapshotBase(s.expected, staticBootstrap)
	if err != nil {
		return ReplicatedChildActivation{}, err
	}
	publication, err := machine.InstallSnapshot(base)
	if err != nil || publication.Applied != s.expected.State.Applied ||
		publication.ReplicaSetVersion != s.expected.State.ReplicaSetVersion {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(base)
	if err != nil {
		return ReplicatedChildActivation{}, err
	}
	claim.machine = machine
	claim.activationBasePending = certificate.Digest
	core.replicatedSnapshotStageClaim = nil
	core.replicatedApplyClaim = claim
	core.replicatedSeedPending = true
	result := ReplicatedChildActivation{Apply: claim, ApplyIdentity: s.identity,
		SnapshotBase: base, ArtifactManifest: s.expected}
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
