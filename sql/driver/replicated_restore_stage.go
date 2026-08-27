package driver

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrReplicatedRestoreStageBusy   = errors.New("vibedb: replicated restore stage already has an owner")
	ErrReplicatedRestoreStageClosed = errors.New("vibedb: replicated restore stage is closed")
	ErrReplicatedRestoreStageProof  = errors.New("vibedb: replicated restore stage proof mismatch")
)

// ReplicatedRestoreStage is a non-serving import owner. It authenticates the
// complete source artifact but materializes only user relation chunks. Source
// state, sessions, ledgers, route gates, transaction intents, and transition
// capture are never written into the destination store.
type ReplicatedRestoreStage struct {
	mu sync.Mutex

	owner    *dbConnector
	database *database
	table    *table
	base     ReplicatedShardStoreIdentity
	identity ReplicatedApplyIdentity
	source   replicatedstate.SnapshotArtifactManifest
	cursor   *replicatedstate.SnapshotArtifactCursor

	payload      []byte
	cursorBuffer []byte
	complete     bool
	closed       bool
	activation   ReplicatedChildActivation
	projection   []replicatedstate.ProjectionRow
}

// OpenReplicatedRestoreStage opens or resumes a fresh destination import.
// source may describe another cluster/group/member, but its logical dense
// relation schema must match expected. persistedCursor is the exact verifier
// cursor durably ordered after previously materialized relation chunks.
func (d *Database) OpenReplicatedRestoreStage(
	expected ReplicatedShardStoreIdentity,
	source replicatedstate.SnapshotArtifactManifest,
	persistedCursor []byte,
	applyOptions ReplicatedApplyOptions,
) (*ReplicatedRestoreStage, ReplicatedApplyIdentity, error) {
	return d.openReplicatedRestoreStage(expected, source, persistedCursor, applyOptions, nil)
}

// OpenReplicatedRestoreProjection authenticates but discards every source row
// and installs only the caller's operation-bound, ordered singleton projection.
// The caller must authenticate the projection before acquiring this owner.
func (d *Database) OpenReplicatedRestoreProjection(expected ReplicatedShardStoreIdentity,
	source replicatedstate.SnapshotArtifactManifest, persistedCursor []byte,
	applyOptions ReplicatedApplyOptions, rows []replicatedstate.ProjectionRow,
) (*ReplicatedRestoreStage, ReplicatedApplyIdentity, error) {
	if expected.RelationCount != 1 || len(rows) == 0 || applyOptions.TxnLimits.MaxDocuments <= 0 || applyOptions.TxnLimits.MaxBytes <= 0 || uint64(len(rows)) > uint64(applyOptions.TxnLimits.MaxDocuments) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageProof
	}
	var total uint64
	for i, row := range rows {
		if len(row.Key) == 0 || len(row.Key) > replication.MaxMutationKeyBytes || len(row.Value) == 0 || len(row.Value) > replication.MaxMutationValueBytes || i > 0 && bytes.Compare(rows[i-1].Key, row.Key) >= 0 {
			return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageProof
		}
		width := uint64(len(row.Key)) + uint64(len(row.Value))
		if width > uint64(applyOptions.TxnLimits.MaxBytes)-total {
			return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageProof
		}
		total += width
	}
	owned := make([]replicatedstate.ProjectionRow, len(rows))
	for i, row := range rows {
		owned[i] = replicatedstate.ProjectionRow{Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)}
	}
	return d.openReplicatedRestoreStage(expected, source, persistedCursor, applyOptions, owned)
}

func (d *Database) openReplicatedRestoreStage(expected ReplicatedShardStoreIdentity,
	source replicatedstate.SnapshotArtifactManifest, persistedCursor []byte,
	applyOptions ReplicatedApplyOptions, projection []replicatedstate.ProjectionRow,
) (*ReplicatedRestoreStage, ReplicatedApplyIdentity, error) {
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if err := validateReplicatedApplyOptions(expected, applyOptions); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if !equalWALBaseManifestShape(source, expected) || source.Seeded ||
		source.Digest == ([32]byte{}) || source.State.Binding == replicatedStateBindingAt(expected, applyOptions.Placement.Range) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageProof
	}
	if expected.RelationCount > 1 {
		manifestDigest, err := ReplicatedRelationManifestDigest(expected)
		if err != nil || manifestDigest != source.RelationManifestDigest {
			return nil, ReplicatedApplyIdentity{}, errors.Join(ErrReplicatedRestoreStageProof, err)
		}
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
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageBusy
	}
	core := connector.db
	core.mu.Lock()
	defer core.mu.Unlock()
	if core.closed || core.replicatedApplyClaim != nil || core.replicatedChildStageClaim != nil ||
		core.replicatedSnapshotStageClaim != nil || core.replicatedRestoreStageClaim != nil {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageBusy
	}
	if core.catalog.ReplicatedShardStore == nil || !core.catalog.ReplicatedShardStore.Equal(expected) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedShardStoreIdentityMismatch
	}
	if err := core.settleCatalogLocked(); err != nil {
		return nil, ReplicatedApplyIdentity{}, err
	}
	if core.txnLog == nil || core.txnLog.Options() != expectedTxnLogOptions(expected) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedApplyMismatch
	}
	var cursor *replicatedstate.SnapshotArtifactCursor
	var err error
	if len(persistedCursor) != 0 {
		cursor, err = replicatedstate.OpenSnapshotArtifactCursor(persistedCursor)
		if err != nil || !restorePrefixMatchesSource(cursor, source) {
			return nil, ReplicatedApplyIdentity{}, errors.Join(ErrReplicatedRestoreStageProof, err)
		}
	}
	if core.catalog.ReplicatedApply == nil {
		for i := range expected.Relations {
			t := core.tables[expected.Relations[i].Table]
			if t == nil || t.collection == nil || t.collection.Len() != 0 {
				return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageProof
			}
		}
		if cursor != nil {
			return nil, ReplicatedApplyIdentity{}, ErrReplicatedRestoreStageProof
		}
	}
	identity, err := core.prepareReplicatedApplyStorageLocked(expected, applyOptions, nil)
	if err != nil {
		return nil, identity, err
	}
	if core.replicatedApplyCollection == nil || core.replicatedCaptureCollection == nil ||
		core.replicatedCaptureCollection.Len() != 0 {
		return nil, identity, ErrReplicatedRestoreStageProof
	}
	if core.checkpointGroup == nil && core.replicatedApplyCollection.Len() != 0 {
		return nil, identity, ErrReplicatedRestoreStageProof
	}
	if projection == nil && cursor != nil && !restoreRowsCoverPrefix(core, expected, cursor.PrefixManifest()) {
		return nil, identity, ErrReplicatedRestoreStageProof
	}
	table := core.tables[expected.UserTable]
	if table == nil || table.collection == nil {
		return nil, identity, ErrReplicatedApplyMismatch
	}
	if projection != nil && !restoreProjectionPrefix(table.collection, projection) {
		return nil, identity, ErrReplicatedRestoreStageProof
	}
	if projection != nil {
		validator := newReplicatedSQLMutationValidator(expected, table, identity.Placement)
		for _, row := range projection {
			if validator.ValidatePut(row.Key, row.Value) != replicatedstate.MutationValidationAccept {
				return nil, identity, ErrReplicatedRestoreStageProof
			}
		}
	}
	stage := &ReplicatedRestoreStage{
		owner: connector, database: core, table: table, base: expected, identity: identity,
		source: source.Clone(), cursor: cursor,
		projection: projection,
		payload:    make([]byte, 0, replicatedstate.DefaultSnapshotArtifactChunkBytes),
	}
	core.replicatedRestoreStageClaim = stage
	core.replicatedSeedPending = true
	connector.exclusive = true
	connector.refs++
	return stage, identity, nil
}

func restorePrefixMatchesSource(cursor *replicatedstate.SnapshotArtifactCursor, source replicatedstate.SnapshotArtifactManifest) bool {
	if cursor == nil {
		return false
	}
	prefix := cursor.PrefixManifest()
	if prefix.HeaderDigest != source.HeaderDigest || prefix.Bundle != source.Bundle ||
		prefix.RelationManifestDigest != source.RelationManifestDigest ||
		!bytes.Equal(prefix.UserCollection, source.UserCollection) || len(prefix.Relations) != len(source.Relations) {
		return false
	}
	for i := range prefix.Relations {
		if prefix.Relations[i].Relation != source.Relations[i].Relation ||
			prefix.Relations[i].Kind != source.Relations[i].Kind ||
			!bytes.Equal(prefix.Relations[i].Collection, source.Relations[i].Collection) {
			return false
		}
	}
	return true
}

func restoreRowsCoverPrefix(core *database, base ReplicatedShardStoreIdentity, prefix replicatedstate.SnapshotArtifactManifest) bool {
	if core == nil {
		return false
	}
	if base.RelationCount == 1 {
		t := core.tables[base.UserTable]
		return t != nil && t.collection != nil && t.collection.Len() >= prefix.UserRows
	}
	if len(prefix.Relations) != len(base.Relations) {
		return false
	}
	for i := range base.Relations {
		t := core.tables[base.Relations[i].Table]
		if t == nil || t.collection == nil || t.collection.Len() < prefix.Relations[i].Rows {
			return false
		}
	}
	return true
}

// Offset returns the next exact source byte required by Receive.
func (s *ReplicatedRestoreStage) Offset() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursor == nil {
		return 0
	}
	return s.cursor.Offset()
}

// Receive authenticates the source range at Offset, applies only user
// relation rows, and persists a cursor after each fully durable chunk.
func (s *ReplicatedRestoreStage) Receive(r io.Reader, persist replicatedstate.SnapshotCursorPersistence) (
	replicatedstate.SnapshotArtifactManifest, error,
) {
	if s == nil || r == nil || persist == nil {
		return replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedRestoreStageClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedRestoreStageClosed
	}
	if s.complete {
		return s.source.Clone(), nil
	}
	manifest, cursor, err := replicatedstate.ContinueSnapshotArtifact(
		r, s.cursor, replicatedstate.SnapshotArtifactCallbacks{
			PayloadBuffer: s.payload,
			Rows: func(_ replicatedstate.SnapshotArtifactCheckpoint, rows replicatedstate.SnapshotArtifactRows) error {
				if s.projection != nil || rows.Collection() != replicatedstate.SnapshotArtifactUser {
					return nil
				}
				collection, lookupErr := s.relationCollection(int(rows.Relation()))
				if lookupErr != nil {
					return lookupErr
				}
				return restoreApplyRows(collection, rows)
			},
			Chunk: func(_ replicatedstate.SnapshotArtifactCheckpoint, next *replicatedstate.SnapshotArtifactCursor) error {
				encoded, encodeErr := replicatedstate.AppendSnapshotArtifactCursor(s.cursorBuffer[:0], next)
				if encodeErr != nil {
					return encodeErr
				}
				s.cursorBuffer = encoded
				return persist(encoded)
			},
		},
	)
	if cursor != nil {
		s.cursor = cursor
	}
	if err != nil {
		return replicatedstate.SnapshotArtifactManifest{}, err
	}
	if manifest.Digest != s.source.Digest || !equalReplicatedSnapshotImage(manifest, s.source) {
		return replicatedstate.SnapshotArtifactManifest{}, ErrReplicatedRestoreStageProof
	}
	s.complete = true
	return manifest.Clone(), nil
}

func (s *ReplicatedRestoreStage) relationCollection(relation int) (*durable.Collection, error) {
	ordinal := 0
	if s.base.RelationCount > 1 {
		if relation == 0 {
			return nil, ErrReplicatedRestoreStageProof
		}
		ordinal = relation - 1
	} else if relation != 0 {
		return nil, ErrReplicatedRestoreStageProof
	}
	if ordinal < 0 || ordinal >= len(s.base.Relations) {
		return nil, ErrReplicatedRestoreStageProof
	}
	t := s.database.tables[s.base.Relations[ordinal].Table]
	if t == nil || t.collection == nil {
		return nil, ErrReplicatedRestoreStageProof
	}
	return t.collection, nil
}

func restoreApplyRows(collection *durable.Collection, rows replicatedstate.SnapshotArtifactRows) error {
	iterator := rows.Iterator()
	remaining := rows.Len()
	var key, value []byte
	pending := false
	for pending || remaining != 0 {
		added := 0
		err := collection.Update(func(batch *durable.WriteBatch) error {
			for pending || remaining != 0 {
				if !pending {
					var ok bool
					key, value, ok = iterator.Next()
					if !ok {
						break
					}
					pending = true
					remaining--
				}
				if err := batch.Put(key, value); err != nil {
					if errors.Is(err, durable.ErrBatchTooLarge) && added != 0 {
						return nil
					}
					return err
				}
				pending = false
				added++
			}
			return nil
		})
		if err != nil {
			return err
		}
		if added == 0 {
			return ErrReplicatedRestoreStageProof
		}
	}
	return nil
}

func restoreProjectionPrefix(collection *durable.Collection, rows []replicatedstate.ProjectionRow) bool {
	var foundRows uint64
	for _, row := range rows {
		value, found, err := collection.AppendRaw(nil, row.Key)
		if err != nil || found && !bytes.Equal(value, row.Value) {
			return false
		}
		if found {
			foundRows++
		}
	}
	return collection.Len() == foundRows
}

// Activate audits the imported relation image, synthesizes destination-owned
// state at cut, certifies it with a fresh checkpoint group, and returns a
// non-serving apply claim plus snapshot base. The caller must still initialize
// Raft storage and adopt the claim into an authorized runtime.
func (s *ReplicatedRestoreStage) Activate(
	staticBootstrap *pb.Snapshot,
	cut replicatedstate.StagedSnapshotCut,
	artifactOptions replicatedstate.SnapshotArtifactOptions,
) (ReplicatedChildActivation, error) {
	if s == nil {
		return ReplicatedChildActivation{}, ErrReplicatedRestoreStageClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed && s.activation.Apply != nil {
		return ownedReplicatedSnapshotActivation(s.activation), nil
	}
	if s.closed || !s.complete || s.owner == nil || s.database == nil {
		return ReplicatedChildActivation{}, ErrReplicatedRestoreStageClosed
	}
	connector, core := s.owner, s.database
	connector.mu.Lock()
	defer connector.mu.Unlock()
	core.mu.Lock()
	defer core.mu.Unlock()
	if connector.closed || connector.db != core || !connector.exclusive || connector.refs != 1 ||
		core.closed || core.replicatedRestoreStageClaim != s || core.replicatedApplyClaim != nil {
		return ReplicatedChildActivation{}, ErrReplicatedRestoreStageClosed
	}
	claim := &ReplicatedApply{
		owner: connector, database: core, table: s.table,
		identity: s.identity, exclusiveConnector: true,
	}
	relations, err := replicatedApplyRelations(s.base, s.identity, core, claim)
	if err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
	}
	if s.projection != nil {
		if !restoreProjectionPrefix(s.table.collection, s.projection) {
			return ReplicatedChildActivation{}, ErrReplicatedRestoreStageProof
		}
		for _, row := range s.projection {
			if relations[0].Target.Validator.ValidatePut(row.Key, row.Value) != replicatedstate.MutationValidationAccept {
				return ReplicatedChildActivation{}, ErrReplicatedRestoreStageProof
			}
			if err := s.table.collection.Update(func(batch *durable.WriteBatch) error { return batch.Put(row.Key, row.Value) }); err != nil {
				return ReplicatedChildActivation{}, err
			}
		}
	}
	members, err := replicatedApplyCheckpointMembers(s.base, core)
	if err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
	}
	machineOptions := replicatedSnapshotLedgerOptions(s.identity, replicatedstate.Options{
		TxnLimits: s.identity.TxnLimits, MaxSessions: s.identity.MaxSessions,
		RetryWindow: s.identity.RetryWindow, CheckpointGroup: core.checkpointGroup,
		TransitionCaptureTarget: replicatedstate.TransitionCaptureTarget{
			Name:       replicatedstate.TransitionCaptureCollectionName,
			Collection: core.replicatedCaptureCollection,
		},
	})
	systemTarget := replicatedstate.CollectionTarget{
		Collection: core.replicatedApplyCollection,
		Validation: replicatedstate.ValidationOpaqueBinary,
		Limits:     replicatedStateCollectionLimits(s.identity.SystemLimits),
	}
	var prepared *replicatedstate.StagedSnapshotPreparation
	if len(relations) == 1 {
		prepared, err = replicatedstate.PrepareStagedSnapshot(
			replicatedStateBindingAt(s.base, s.identity.Placement.Range), staticBootstrap,
			systemTarget,
			replicatedstate.UserCollection{
				Name: relations[0].Name, Target: relations[0].Target,
				LocalIndexes: relations[0].LocalIndexes,
			},
			core.txnLog, machineOptions, cut, artifactOptions,
		)
	} else {
		prepared, err = replicatedstate.PrepareStagedSnapshotBundle(
			replicatedStateBindingAt(s.base, s.identity.Placement.Range), staticBootstrap,
			systemTarget, relations, core.txnLog, machineOptions, cut, artifactOptions,
		)
	}
	if err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
	}
	seedEnvelope := prepared.AppendSeedEnvelope(nil)
	seedKey := prepared.AppendSeedKey(nil)
	seedImages := make([]durable.CheckpointGroupSeedImage, len(members))
	for i := range members {
		seedImages[i] = durable.CheckpointGroupSeedImage{
			Collection: members[i].Collection,
			Generation: members[i].Collection.Generation(),
		}
	}
	seed := durable.CheckpointGroupSeed{
		Applied: prepared.AppliedIndex(), Member: prepared.SeedMember(),
		Envelope: seedEnvelope, Images: seedImages,
	}
	if core.checkpointGroup == nil {
		core.checkpointGroup, err = durable.NewSeededCheckpointGroup(
			core.txnLog, members, seed, durable.CheckpointGroupOptions{},
		)
		if err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
		}
	}
	if !core.checkpointGroup.Owns(members) {
		return ReplicatedChildActivation{}, ErrReplicatedRestoreStageProof
	}
	if core.checkpointGroup.SeedActivationPending() {
		if err := core.checkpointGroup.Seed(seed, members[0], s.identity.TxnLimits, seedKey); err != nil {
			return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
		}
	}
	machineOptions.CheckpointGroup = core.checkpointGroup
	machine, snapshotBase, manifest, err := prepared.Finish(core.checkpointGroup)
	if err != nil {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
	}
	publication, err := machine.InstallSnapshot(snapshotBase)
	if err != nil || publication.Applied != cut.Applied {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
	}
	certificate, err := replicatedstate.OpenSnapshotBase(snapshotBase)
	if err != nil || !proto.Equal(certificate.StaticBootstrap, staticBootstrap) {
		return ReplicatedChildActivation{}, errors.Join(ErrReplicatedRestoreStageProof, err)
	}
	claim.machine = machine
	claim.activationBasePending = certificate.Digest
	core.replicatedRestoreStageClaim = nil
	core.replicatedApplyClaim = claim
	core.replicatedSeedPending = true
	result := ReplicatedChildActivation{
		Apply: claim, ApplyIdentity: s.identity, SnapshotBase: snapshotBase,
		ArtifactManifest: manifest,
	}
	s.activation = ownedReplicatedSnapshotActivation(result)
	s.closed = true
	return ownedReplicatedSnapshotActivation(result), nil
}

// Close releases an unactivated restore owner. Durable relation rows and the
// verifier cursor remain caller-owned recovery state and are never deleted.
func (s *ReplicatedRestoreStage) Close() error {
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
	if core.replicatedRestoreStageClaim != s || !connector.exclusive {
		core.mu.Unlock()
		connector.mu.Unlock()
		return ErrReplicatedRestoreStageClosed
	}
	core.replicatedRestoreStageClaim = nil
	connector.exclusive = false
	s.closed = true
	core.mu.Unlock()
	connector.mu.Unlock()
	return connector.release()
}
