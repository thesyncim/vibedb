package driver

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
		core.catalog.ReplicatedApply == nil ||
		manifest.State.Binding != replicatedStateBindingAt(expected, applyOptions.Placement.Range) ||
		!equalWALBaseManifestShape(manifest, expected) {
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
	members, err := replicatedApplyCheckpointMembers(expected, core)
	if err != nil || !core.checkpointGroup.Owns(members) {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
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
	relations, err := replicatedApplyRelations(expected, identity, core, claim)
	if err != nil {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	machine, err := replicatedstate.OpenBundle(
		replicatedStateBindingAt(expected, applyOptions.Placement.Range), staticBootstrap,
		replicatedstate.CollectionTarget{Collection: core.replicatedApplyCollection,
			Validation: replicatedstate.ValidationOpaqueBinary,
			Limits:     replicatedStateCollectionLimits(identity.SystemLimits)},
		relations, core.txnLog, replicatedSnapshotLedgerOptions(identity, replicatedstate.Options{
			TxnLimits: identity.TxnLimits, MaxSessions: identity.MaxSessions,
			RetryWindow: identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
			TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
				Name:       replicatedstate.TransitionCaptureCollectionName,
				Collection: core.replicatedCaptureCollection,
			},
		}),
	)
	if err != nil {
		return activation, false, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
	cut, err := machine.Snapshot()
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
		!equalReplicatedActivatedSnapshotImage(current, manifest) {
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
// its final hidden and dense relation collections. Artifact memory is bounded by the
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
	if !equalWALBaseManifestShape(manifest, expected) ||
		manifest.State.Binding != replicatedStateBindingAt(expected, applyOptions.Placement.Range) {
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
	if core.catalog.ReplicatedApply == nil {
		for ordinal := range expected.Relations {
			table := core.tables[expected.Relations[ordinal].Table]
			if table == nil || table.collection == nil || table.collection.Len() != 0 {
				return nil, ReplicatedApplyIdentity{}, ErrReplicatedSnapshotStageProof
			}
		}
		if len(persistedCursor) != 0 {
			return nil, ReplicatedApplyIdentity{}, ErrReplicatedSnapshotStageProof
		}
	}
	if core.checkpointGroup != nil {
		cursor, cursorErr := replicatedstate.OpenSnapshotArtifactCursor(persistedCursor)
		members, membersErr := replicatedApplyCheckpointMembers(expected, core)
		offset := cursor.Offset()
		owns := membersErr == nil && core.checkpointGroup.Owns(members)
		footerOffset, footerBound := uint64(0), manifest.EncodedBytes >= replicatedstate.SnapshotArtifactFooterBytes
		if footerBound {
			footerOffset = manifest.EncodedBytes - replicatedstate.SnapshotArtifactFooterBytes
		}
		if !footerBound || cursorErr != nil || membersErr != nil || offset != footerOffset || !owns {
			return nil, ReplicatedApplyIdentity{}, errors.Join(
				fmt.Errorf("%w: resumed cursor %d/%d group-owned=%v",
					ErrReplicatedSnapshotStageProof, offset, manifest.EncodedBytes, owns),
				cursorErr, membersErr,
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
	relations, err := replicatedApplyRelations(expected, identity, core, validatorClaim)
	if err != nil {
		return nil, identity, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
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
	systemTarget := replicatedstate.CollectionTarget{
		Collection: core.replicatedApplyCollection,
		Validation: replicatedstate.ValidationOpaqueBinary,
		Limits:     replicatedStateCollectionLimits(identity.SystemLimits),
	}
	var stage *replicatedstate.SnapshotArtifactStage
	if expected.RelationCount == 1 {
		stage, err = replicatedstate.NewSnapshotArtifactStageWithOptions(
			manifest, systemTarget, relations[0].Target, persistedCursor, stageOptions,
		)
	} else {
		stage, err = replicatedstate.NewBundleSnapshotArtifactStageWithOptions(
			manifest, systemTarget, relations, persistedCursor, stageOptions,
		)
	}
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
	return manifest.Clone()
}

func equalReplicatedSnapshotImage(
	left, right replicatedstate.SnapshotArtifactManifest,
) bool {
	if left.Bundle != right.Bundle ||
		left.RelationManifestDigest != right.RelationManifestDigest ||
		!bytes.Equal(left.UserCollection, right.UserCollection) ||
		left.SystemRows != right.SystemRows || left.UserRows != right.UserRows ||
		left.CaptureRows != right.CaptureRows || left.ImageDigest != right.ImageDigest ||
		left.CaptureImageDigest != right.CaptureImageDigest ||
		len(left.Relations) != len(right.Relations) {
		return false
	}
	for ordinal := range left.Relations {
		a, b := left.Relations[ordinal], right.Relations[ordinal]
		if a.Relation != b.Relation || a.Kind != b.Kind || a.Rows != b.Rows ||
			a.ImageDigest != b.ImageDigest || !bytes.Equal(a.Collection, b.Collection) {
			return false
		}
	}
	return true
}

// A compact singleton seed certifies the fresh user image before its one
// imported State row is materialized. Its authenticated grammar requires
// SystemRows=0; a streamed export of the activated image must contain exactly
// that one row. All other image fields remain exact, and the caller separately
// compares the canonical State against the authenticated snapshot base.
func equalReplicatedActivatedSnapshotImage(
	current, certified replicatedstate.SnapshotArtifactManifest,
) bool {
	if certified.Seeded {
		if certified.Bundle || current.Seeded || current.Bundle ||
			certified.SystemRows != 0 || current.SystemRows != 1 {
			return false
		}
		certified.SystemRows = 1
	}
	return equalReplicatedSnapshotImage(current, certified)
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

// Activate authenticates every final collection image, installs the exact
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
	members, err := replicatedApplyCheckpointMembers(s.base, core)
	if err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, err)
	}
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
		candidateOptions := replicatedSnapshotLedgerOptions(s.identity, replicatedstate.Options{
			TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
			RetryWindow: s.identity.RetryWindow,
			TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
				Name:       replicatedstate.TransitionCaptureCollectionName,
				Collection: core.replicatedCaptureCollection,
			},
		})
		if s.base.RelationCount == 1 {
			_, err = s.stage.OpenCandidate(staticBootstrap, core.txnLog, candidateOptions)
		} else {
			_, err = s.stage.OpenCandidateBundle(staticBootstrap, core.txnLog, candidateOptions)
		}
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
	seedImages := make([]durable.CheckpointGroupSeedImage, 0, len(members))
	for i := range members {
		seedImages = append(seedImages, durable.CheckpointGroupSeedImage{
			Collection: members[i].Collection, Generation: members[i].Collection.Generation(),
		})
	}
	seed := durable.CheckpointGroupSeed{
		Applied: s.expected.State.Applied, Member: replicatedstate.SystemCollectionName,
		Envelope: s.seedEnvelope,
		Images:   seedImages,
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
		relations, relationErr := replicatedApplyRelations(s.base, s.identity, core, s.claim)
		if relationErr != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedSnapshotStageProof, relationErr)
		}
		s.machine, err = replicatedstate.OpenBundle(
			replicatedStateBindingAt(s.base, s.identity.Placement.Range), staticBootstrap,
			replicatedstate.CollectionTarget{
				Collection: core.replicatedApplyCollection,
				Validation: replicatedstate.ValidationOpaqueBinary,
				Limits:     replicatedStateCollectionLimits(s.identity.SystemLimits),
			},
			relations,
			core.txnLog, replicatedSnapshotLedgerOptions(s.identity, replicatedstate.Options{
				TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
				RetryWindow: s.identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
				TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
					Name:       replicatedstate.TransitionCaptureCollectionName,
					Collection: core.replicatedCaptureCollection,
				},
			}),
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
