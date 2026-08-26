package replicatedstate

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

// SnapshotCursorPersistence must durably replace the receiver's cursor before
// returning. raw is borrowed only for the call. The stager orders every named
// collection update before invoking this function, so a crash can replay at
// most one uncheckpointed artifact chunk.
type SnapshotCursorPersistence func(raw []byte) error

const DefaultSnapshotStageCheckpointBytes = 64 << 20

// SnapshotArtifactStageOptions bounds how much already-durable data a crash
// may replay. Zero selects DefaultSnapshotStageCheckpointBytes. The cursor is
// also forced at every Receive return, so short ranges never wait for the
// threshold.
type SnapshotArtifactStageOptions struct {
	CheckpointBytes uint64
	// Capture is the private opaque transition-capture destination authenticated
	// by the third artifact collection.
	Capture CollectionTarget
}

// SnapshotArtifactStage applies verified chunks only to caller-owned,
// non-serving durable collections. It never publishes routing, ownership, Raft
// storage, or a serving Machine. One stage is serial; concurrent Receive or
// OpenCandidate calls are rejected by its mutex and terminal state.
type SnapshotArtifactStage struct {
	mu sync.Mutex

	expected  SnapshotArtifactManifest
	system    CollectionTarget
	user      CollectionTarget
	relations []relationCollection
	capture   CollectionTarget
	cursor    *SnapshotArtifactCursor

	payloadBuffer   []byte
	cursorBuffer    []byte
	checkpointBytes uint64
	persistedOffset uint64
	complete        bool
	opened          bool
}

// NewSnapshotArtifactStage validates a destination and optional persisted
// cursor. Existing rows are allowed only within the expected final counts:
// they can be the idempotent result of a crash after data sync but before cursor
// replacement. Final Machine open validates their exact contents.
func NewSnapshotArtifactStage(
	expected SnapshotArtifactManifest,
	system CollectionTarget,
	user CollectionTarget,
	persistedCursor []byte,
) (*SnapshotArtifactStage, error) {
	return NewSnapshotArtifactStageWithOptions(
		expected, system, user, persistedCursor, SnapshotArtifactStageOptions{},
	)
}

// NewSnapshotArtifactStageWithOptions is NewSnapshotArtifactStage with an
// explicit replay/checkpoint budget.
func NewSnapshotArtifactStageWithOptions(
	expected SnapshotArtifactManifest,
	system CollectionTarget,
	user CollectionTarget,
	persistedCursor []byte,
	options SnapshotArtifactStageOptions,
) (*SnapshotArtifactStage, error) {
	if expected.Bundle {
		return nil, fmt.Errorf("%w: bundle artifact requires bundle stage", ErrSnapshotStage)
	}
	return newSnapshotArtifactStage(
		expected, system, user, nil, persistedCursor, options,
	)
}

// NewBundleSnapshotArtifactStage constructs a bounded receiver for one
// coherent dense relation bundle. Relation IDs are the only hot chunk-routing
// identity; names remain cold authenticated open metadata.
func NewBundleSnapshotArtifactStage(
	expected SnapshotArtifactManifest,
	system CollectionTarget,
	relations []RelationCollection,
	persistedCursor []byte,
) (*SnapshotArtifactStage, error) {
	return NewBundleSnapshotArtifactStageWithOptions(
		expected, system, relations, persistedCursor, SnapshotArtifactStageOptions{},
	)
}

// NewBundleSnapshotArtifactStageWithOptions is
// NewBundleSnapshotArtifactStage with an explicit replay/checkpoint budget and
// optional proven transition-capture destination.
func NewBundleSnapshotArtifactStageWithOptions(
	expected SnapshotArtifactManifest,
	system CollectionTarget,
	relations []RelationCollection,
	persistedCursor []byte,
	options SnapshotArtifactStageOptions,
) (*SnapshotArtifactStage, error) {
	if !expected.Bundle {
		return nil, fmt.Errorf("%w: singleton artifact requires singleton stage", ErrSnapshotStage)
	}
	prepared, manifestDigest, err := prepareRelationCollections(expected.State.Binding, relations)
	if err != nil || manifestDigest != expected.RelationManifestDigest ||
		len(prepared) != len(expected.Relations) {
		return nil, errors.Join(
			fmt.Errorf("%w: relation manifest", ErrSnapshotStage), err,
		)
	}
	for index := range prepared {
		relation, certificate := &prepared[index], &expected.Relations[index]
		if relation.id != certificate.Relation || relation.kind != certificate.Kind ||
			!bytes.Equal([]byte(relation.name), certificate.Collection) ||
			certificate.ImageDigest == ([32]byte{}) {
			return nil, fmt.Errorf("%w: relation %d certificate", ErrSnapshotStage, index+1)
		}
		for prior := 0; prior < index; prior++ {
			if relation.target.Collection == prepared[prior].target.Collection {
				return nil, fmt.Errorf("%w: duplicate relation target", ErrSnapshotStage)
			}
		}
	}
	return newSnapshotArtifactStage(
		expected, system, CollectionTarget{}, prepared, persistedCursor, options,
	)
}

func newSnapshotArtifactStage(
	expected SnapshotArtifactManifest,
	system CollectionTarget,
	user CollectionTarget,
	relations []relationCollection,
	persistedCursor []byte,
	options SnapshotArtifactStageOptions,
) (*SnapshotArtifactStage, error) {
	checkpointBytes := options.CheckpointBytes
	if checkpointBytes == 0 {
		checkpointBytes = DefaultSnapshotStageCheckpointBytes
	}
	if checkpointBytes < MaxSnapshotArtifactChunkBytes {
		return nil, fmt.Errorf("%w: checkpoint bytes %d", ErrSnapshotStage, checkpointBytes)
	}
	if err := validateExpectedSnapshotArtifact(expected); err != nil {
		return nil, err
	}
	if err := system.validate(); err != nil ||
		system.Validation != ValidationOpaqueBinary ||
		system.ValidationDigest != ([32]byte{}) || system.Validator != nil ||
		system.ObserveMutationAttempt != nil {
		return nil, fmt.Errorf("%w: system target: %v", ErrSnapshotStage, err)
	}
	if len(relations) == 0 {
		if err := user.validate(); err != nil ||
			user.Validation != ValidationDeterministicMutation ||
			user.ValidationDigest == ([32]byte{}) || user.Validator == nil {
			return nil, fmt.Errorf("%w: user target: %v", ErrSnapshotStage, err)
		}
	}
	capture := options.Capture
	captureOptional := capture.Collection == nil && expected.CaptureRows == 0 &&
		expected.CaptureImageDigest == snapshotArtifactEmptyCaptureImageDigest()
	if err := capture.validate(); !captureOptional && (err != nil || capture.Validation != ValidationOpaqueBinary ||
		capture.ValidationDigest != ([32]byte{}) || capture.Validator != nil ||
		capture.ObserveMutationAttempt != nil ||
		capture.Limits.MaxDocumentBytes > MaxTransitionCaptureRecordBytes) {
		return nil, fmt.Errorf("%w: capture target: %v", ErrSnapshotStage, err)
	}
	if len(relations) == 0 && (system.Collection == user.Collection ||
		user.Collection == capture.Collection ||
		user.Limits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		user.Limits.MaxDocumentBytes > replication.MaxMutationValueBytes) ||
		!captureOptional && system.Collection == capture.Collection {
		return nil, fmt.Errorf("%w: collection targets", ErrSnapshotStage)
	}
	for index := range relations {
		target := relations[index].target.Collection
		if target == system.Collection || !captureOptional && target == capture.Collection {
			return nil, fmt.Errorf("%w: relation target aliases private target", ErrSnapshotStage)
		}
	}
	var cursor *SnapshotArtifactCursor
	var err error
	if len(persistedCursor) != 0 {
		cursor, err = OpenSnapshotArtifactCursor(persistedCursor)
		if err != nil {
			return nil, err
		}
		if err := snapshotArtifactPrefixMatchesExpected(cursor.PrefixManifest(), expected); err != nil {
			return nil, err
		}
	}
	systemRows, userRows, captureRows := system.Collection.Len(), uint64(0), uint64(0)
	if len(relations) == 0 {
		userRows = user.Collection.Len()
	} else {
		for index := range relations {
			rows := relations[index].target.Collection.Len()
			if rows > expected.Relations[index].Rows ||
				userRows > ^uint64(0)-rows {
				return nil, fmt.Errorf("%w: destination relation row counts exceed artifact", ErrSnapshotStage)
			}
			userRows += rows
		}
	}
	if capture.Collection != nil {
		captureRows = capture.Collection.Len()
	}
	if systemRows > expected.SystemRows || userRows > expected.UserRows || captureRows > expected.CaptureRows {
		return nil, fmt.Errorf("%w: destination row counts exceed artifact", ErrSnapshotStage)
	}
	if cursor != nil {
		prefix := cursor.PrefixManifest()
		if systemRows < prefix.SystemRows || userRows < prefix.UserRows ||
			captureRows < prefix.CaptureRows {
			return nil, fmt.Errorf("%w: cursor advances beyond durable rows", ErrSnapshotStage)
		}
		if len(relations) != 0 {
			if len(prefix.Relations) != len(relations) {
				return nil, fmt.Errorf("%w: cursor relation geometry", ErrSnapshotStage)
			}
			for index := range relations {
				if relations[index].target.Collection.Len() < prefix.Relations[index].Rows {
					return nil, fmt.Errorf("%w: cursor advances beyond durable relation rows", ErrSnapshotStage)
				}
			}
		}
	}
	stage := &SnapshotArtifactStage{
		expected: cloneSnapshotArtifactManifest(expected), system: system, user: user, capture: capture,
		relations: relations,
		cursor:    cursor, payloadBuffer: make([]byte, 0, DefaultSnapshotArtifactChunkBytes),
		checkpointBytes: checkpointBytes,
	}
	if cursor != nil {
		stage.persistedOffset = cursor.Offset()
	}
	return stage, nil
}

// Offset returns the next required source byte. Zero means the receiver needs
// the artifact header; a nonzero value is an exact resumable range boundary.
func (s *SnapshotArtifactStage) Offset() uint64 {
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

// Receive verifies and applies the artifact range beginning at Offset. Each
// chunk may use several bounded durable batches, but persist is called only
// after all of them acknowledge. On any failure, the returned offset remains a
// safe replay point and duplicate puts are exact/idempotent.
func (s *SnapshotArtifactStage) Receive(
	r io.Reader,
	persist SnapshotCursorPersistence,
) (SnapshotArtifactManifest, error) {
	if s == nil || r == nil || persist == nil {
		return SnapshotArtifactManifest{}, fmt.Errorf("%w: nil receive input", ErrSnapshotStage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened {
		return SnapshotArtifactManifest{}, fmt.Errorf("%w: candidate already opened", ErrSnapshotStage)
	}
	if s.complete {
		return cloneSnapshotArtifactManifest(s.expected), nil
	}
	persistCursor := func(next *SnapshotArtifactCursor) error {
		encoded, encodeErr := AppendSnapshotArtifactCursor(s.cursorBuffer[:0], next)
		if encodeErr != nil {
			return encodeErr
		}
		s.cursorBuffer = encoded
		if persistErr := persist(encoded); persistErr != nil {
			return persistErr
		}
		s.persistedOffset = next.Offset()
		return nil
	}
	manifest, cursor, err := ContinueSnapshotArtifact(r, s.cursor, SnapshotArtifactCallbacks{
		PayloadBuffer: s.payloadBuffer,
		Rows: func(_ SnapshotArtifactCheckpoint, rows SnapshotArtifactRows) error {
			return s.applyRows(rows)
		},
		Chunk: func(_ SnapshotArtifactCheckpoint, next *SnapshotArtifactCursor) error {
			if next.Offset()-s.persistedOffset >= s.checkpointBytes {
				return persistCursor(next)
			}
			return nil
		},
	})
	if cursor != nil {
		s.cursor = cursor
		if cursor.Offset() > s.persistedOffset {
			if persistErr := persistCursor(cursor); persistErr != nil {
				if err != nil {
					return SnapshotArtifactManifest{}, errors.Join(err, persistErr)
				}
				return SnapshotArtifactManifest{}, persistErr
			}
		}
	}
	if err != nil {
		return SnapshotArtifactManifest{}, err
	}
	if !equalSnapshotArtifactManifest(manifest, s.expected) {
		return SnapshotArtifactManifest{}, fmt.Errorf("%w: completed artifact differs from expectation", ErrSnapshotStage)
	}
	s.complete = true
	return cloneSnapshotArtifactManifest(manifest), nil
}

func (s *SnapshotArtifactStage) applyRows(rows SnapshotArtifactRows) error {
	var collection *durable.Collection
	relation := rows.Relation()
	switch rows.Collection() {
	case SnapshotArtifactSystem:
		if relation != 0 {
			return fmt.Errorf("%w: private chunk relation", ErrSnapshotStage)
		}
		collection = s.system.Collection
	case SnapshotArtifactUser:
		if len(s.relations) == 0 {
			if relation != 0 {
				return fmt.Errorf("%w: singleton chunk relation", ErrSnapshotStage)
			}
			collection = s.user.Collection
		} else {
			ordinal := int(relation) - 1
			if relation == 0 || ordinal < 0 || ordinal >= len(s.relations) ||
				s.relations[ordinal].id != relation {
				return fmt.Errorf("%w: bundle chunk relation", ErrSnapshotStage)
			}
			collection = s.relations[ordinal].target.Collection
		}
	case SnapshotArtifactCapture:
		if relation != 0 {
			return fmt.Errorf("%w: private chunk relation", ErrSnapshotStage)
		}
		if s.capture.Collection == nil {
			return fmt.Errorf("%w: absent capture target", ErrSnapshotStage)
		}
		collection = s.capture.Collection
	default:
		return fmt.Errorf("%w: chunk collection", ErrSnapshotStage)
	}
	iterator := rows.Iterator()
	var pendingKey, pendingValue []byte
	pending := false
	for pending || iterator.remaining != 0 {
		added := 0
		err := collection.Update(func(batch *durable.WriteBatch) error {
			for pending || iterator.remaining != 0 {
				if !pending {
					var ok bool
					pendingKey, pendingValue, ok = iterator.Next()
					if !ok {
						break
					}
					pending = true
				}
				if err := batch.Put(pendingKey, pendingValue); err != nil {
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
			return fmt.Errorf("%w: destination batch made no progress", ErrSnapshotStage)
		}
	}
	return nil
}

// OpenCandidate performs the expensive final proof over all three completed
// files: system records, retained completions, user placement, canonical user
// image, and the exact opaque transition-capture image. Success returns a non-serving
// Machine at the exact expected publication. The caller still needs learner
// membership, log-tail catch-up, and topology cutover before serving it.
func (s *SnapshotArtifactStage) OpenCandidate(
	bootstrap *pb.Snapshot,
	txnLog *durable.TxnLog,
	options Options,
) (*Machine, error) {
	return s.openCandidate(bootstrap, txnLog, options, false)
}

// OpenCandidateBundle performs the final cross-relation image and publication
// proof through OpenBundle. The returned machine remains non-serving until the
// caller completes learner catch-up and the topology cutover.
func (s *SnapshotArtifactStage) OpenCandidateBundle(
	bootstrap *pb.Snapshot,
	txnLog *durable.TxnLog,
	options Options,
) (*Machine, error) {
	return s.openCandidate(bootstrap, txnLog, options, true)
}

func (s *SnapshotArtifactStage) openCandidate(
	bootstrap *pb.Snapshot,
	txnLog *durable.TxnLog,
	options Options,
	bundle bool,
) (*Machine, error) {
	if s == nil {
		return nil, fmt.Errorf("%w: nil stage", ErrSnapshotStage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.complete {
		return nil, ErrSnapshotStageIncomplete
	}
	if s.opened {
		return nil, fmt.Errorf("%w: candidate already opened", ErrSnapshotStage)
	}
	if bundle != (len(s.relations) != 0) || bundle != s.expected.Bundle {
		return nil, fmt.Errorf("%w: candidate artifact shape", ErrSnapshotStage)
	}
	// The stage's capture target is the independently verified destination.
	// Never let caller options redirect the opened Machine to another opaque
	// collection or bind a different member name after image proof.
	if s.capture.Collection == nil {
		if options.TransitionCaptureTarget.Collection != nil ||
			options.TransitionCaptureTarget.Name != "" {
			return nil, fmt.Errorf("%w: candidate capture target mismatch", ErrSnapshotStage)
		}
		options.TransitionCaptureTarget = TransitionCaptureTarget{}
	} else {
		want := TransitionCaptureTarget{
			Name: TransitionCaptureCollectionName, Collection: s.capture.Collection,
		}
		if supplied := options.TransitionCaptureTarget; (supplied.Collection != nil || supplied.Name != "") && supplied != want {
			return nil, fmt.Errorf("%w: candidate capture target mismatch", ErrSnapshotStage)
		}
		options.TransitionCaptureTarget = want
	}
	var machine *Machine
	var err error
	if bundle {
		specs := make([]RelationCollection, len(s.relations))
		for index := range s.relations {
			relation := &s.relations[index]
			specs[index] = RelationCollection{
				Relation: relation.id, Kind: relation.kind, Name: relation.name,
				Target: relation.target, LocalIndexes: relation.localIndexes,
				GlobalIndex: relation.globalIndex,
			}
		}
		machine, err = OpenBundle(
			s.expected.State.Binding, bootstrap, s.system, specs, txnLog, options,
		)
	} else {
		machine, err = Open(
			s.expected.State.Binding, bootstrap, s.system,
			UserCollection{Name: string(s.expected.UserCollection), Target: s.user},
			txnLog, options,
		)
	}
	if err != nil {
		return nil, err
	}
	if s.capture.Collection != nil {
		captureSnapshot, err := s.capture.Collection.Snapshot()
		if err != nil {
			return nil, err
		}
		captureDigest, digestErr := snapshotArtifactOpaqueImageDigest(captureSnapshot)
		closeErr := captureSnapshot.Close()
		if digestErr != nil || closeErr != nil || s.capture.Collection.Len() != s.expected.CaptureRows ||
			captureDigest != s.expected.CaptureImageDigest {
			return nil, errors.Join(fmt.Errorf("%w: candidate capture image", ErrSnapshotStage), digestErr, closeErr)
		}
	}
	if bundle {
		if machine.manifestDigest != s.expected.RelationManifestDigest ||
			len(machine.relations) != len(s.expected.Relations) {
			return nil, fmt.Errorf("%w: candidate relation manifest", ErrSnapshotStage)
		}
		for index := range machine.relations {
			relation, certificate := &machine.relations[index], &s.expected.Relations[index]
			if relation.id != certificate.Relation || relation.kind != certificate.Kind ||
				!bytes.Equal([]byte(relation.name), certificate.Collection) ||
				relation.target.Collection.Len() != certificate.Rows ||
				relation.openedImage != certificate.ImageDigest {
				return nil, fmt.Errorf("%w: candidate relation %d image", ErrSnapshotStage, index+1)
			}
		}
	}
	if machine.openedImageApplied != s.expected.State.Applied ||
		machine.openedImageDigest != s.expected.ImageDigest {
		return nil, fmt.Errorf("%w: candidate image digest", ErrSnapshotStage)
	}
	publication := machine.Published()
	if !equalStatePublication(
		s.expected.State, publication.Applied, publication.DataChainDigest,
		publication.ConfState, publication.ReplicaSetVersion,
	) {
		return nil, fmt.Errorf("%w: candidate publication", ErrSnapshotStage)
	}
	s.opened = true
	return machine, nil
}

// AppendSeedEnvelope appends the exact authenticated State row carried by the
// completed artifact. It is available only after OpenCandidate has proved the
// final system, user, and capture images.
func (s *SnapshotArtifactStage) AppendSeedEnvelope(dst []byte) []byte {
	if s == nil {
		return dst
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.complete || !s.opened {
		return dst
	}
	envelope, err := AppendState(nil, s.expected.State)
	if err != nil {
		return dst
	}
	return append(dst, envelope...)
}

// AppendSeedKey appends the fixed hidden State key after final image proof.
func (s *SnapshotArtifactStage) AppendSeedKey(dst []byte) []byte {
	if s == nil {
		return dst
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.complete || !s.opened {
		return dst
	}
	return append(dst, stateKey...)
}

// AppendRecoveredSeed returns the exact State key/envelope only when an
// already-published checkpoint-group certificate independently authenticates
// them. This is the crash-resume counterpart to OpenCandidate: artifact receipt
// alone never exposes activation material.
func (s *SnapshotArtifactStage) AppendRecoveredSeed(
	keyDst, envelopeDst []byte,
	group *durable.CheckpointGroup,
	member string,
) (key, envelope []byte, err error) {
	if s == nil || group == nil {
		return keyDst, envelopeDst, ErrSnapshotStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.complete {
		return keyDst, envelopeDst, ErrSnapshotStageIncomplete
	}
	encoded, encodeErr := AppendState(nil, s.expected.State)
	seeded, proofErr := group.ValidateSeedState(s.expected.State.Applied, member, encoded)
	if encodeErr != nil || proofErr != nil || !seeded {
		return keyDst, envelopeDst, errors.Join(ErrSnapshotStage, encodeErr, proofErr)
	}
	return append(keyDst, stateKey...), append(envelopeDst, encoded...), nil
}

func validateExpectedSnapshotArtifact(expected SnapshotArtifactManifest) error {
	wantSystemRows, systemRowsOK := stateSystemRowCount(expected.State)
	if err := validateState(expected.State); err != nil ||
		expected.Seeded ||
		len(expected.UserCollection) == 0 ||
		len(expected.UserCollection) > replication.MaxCollectionBytes ||
		!utf8.Valid(expected.UserCollection) || bytes.IndexByte(expected.UserCollection, 0) >= 0 ||
		bytes.Equal(expected.UserCollection, []byte(systemCollectionName)) ||
		expected.TargetChunkBytes < MinSnapshotArtifactChunkBytes ||
		expected.TargetChunkBytes > MaxSnapshotArtifactChunkBytes ||
		expected.Chunks == 0 || !systemRowsOK || expected.SystemRows != wantSystemRows ||
		expected.PayloadBytes == 0 || expected.EncodedBytes == 0 ||
		expected.HeaderDigest == ([32]byte{}) ||
		expected.LastChunkDigest == ([32]byte{}) || expected.ImageDigest == ([32]byte{}) ||
		expected.CaptureImageDigest == ([32]byte{}) ||
		expected.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: expected artifact", ErrSnapshotStage)
	}
	if expected.Bundle {
		if len(expected.Relations) < 2 ||
			len(expected.Relations) > replication.MaxRelationsPerBundle ||
			expected.RelationManifestDigest == ([32]byte{}) {
			return fmt.Errorf("%w: expected relation manifest", ErrSnapshotStage)
		}
		var rows uint64
		for index := range expected.Relations {
			relation := &expected.Relations[index]
			if relation.Relation != replication.RelationID(index+1) ||
				(relation.Kind != RelationJSON && relation.Kind != RelationGlobalIndex) ||
				len(relation.Collection) == 0 ||
				len(relation.Collection) > replication.MaxIdentityBytes ||
				!utf8.Valid(relation.Collection) ||
				bytes.IndexByte(relation.Collection, 0) >= 0 ||
				relation.ImageDigest == ([32]byte{}) || rows > ^uint64(0)-relation.Rows {
				return fmt.Errorf("%w: expected relation %d", ErrSnapshotStage, index+1)
			}
			for prior := 0; prior < index; prior++ {
				if bytes.Equal(relation.Collection, expected.Relations[prior].Collection) {
					return fmt.Errorf("%w: duplicate expected relation", ErrSnapshotStage)
				}
			}
			rows += relation.Rows
		}
		if expected.Relations[0].Kind != RelationJSON ||
			!bytes.Equal(expected.Relations[0].Collection, expected.UserCollection) ||
			rows != expected.UserRows {
			return fmt.Errorf("%w: expected relation geometry", ErrSnapshotStage)
		}
	} else if len(expected.Relations) != 0 ||
		expected.RelationManifestDigest != ([32]byte{}) {
		return fmt.Errorf("%w: singleton relation manifest", ErrSnapshotStage)
	}
	stateEnvelope, err := AppendState(nil, expected.State)
	if err != nil {
		return fmt.Errorf("%w: expected state", ErrSnapshotStage)
	}
	headerRelations := make([]SnapshotArtifactRelation, len(expected.Relations))
	for index := range expected.Relations {
		headerRelations[index] = SnapshotArtifactRelation{
			Relation:   expected.Relations[index].Relation,
			Kind:       expected.Relations[index].Kind,
			Collection: expected.Relations[index].Collection,
		}
	}
	header, headerDigest, err := makeSnapshotArtifactHeaderForRelations(
		stateEnvelope, string(expected.UserCollection), int(expected.TargetChunkBytes),
		expected.RelationManifestDigest, headerRelations, expected.Bundle,
	)
	wantEncodedBytes, encodedBytesOK := snapshotArtifactEncodedBytes(
		uint64(len(header)), expected.Chunks, expected.PayloadBytes, true,
	)
	if expected.Bundle {
		certificates := uint64(len(expected.Relations)) * snapshotArtifactRelationBytes
		if !encodedBytesOK || wantEncodedBytes > ^uint64(0)-certificates {
			encodedBytesOK = false
		} else {
			wantEncodedBytes += certificates
		}
	}
	_, wantFooterDigest := makeSnapshotArtifactFooter(
		expected.Chunks, expected.SystemRows, expected.UserRows, expected.CaptureRows,
		expected.PayloadBytes, expected.EncodedBytes,
		expected.LastChunkDigest, expected.HeaderDigest, expected.ImageDigest,
		expected.CaptureImageDigest,
	)
	if err != nil || headerDigest != expected.HeaderDigest ||
		!encodedBytesOK || expected.EncodedBytes != wantEncodedBytes ||
		expected.Digest != wantFooterDigest {
		return fmt.Errorf("%w: expected artifact identity", ErrSnapshotStage)
	}
	return nil
}

func snapshotArtifactPrefixMatchesExpected(
	prefix SnapshotArtifactManifest,
	expected SnapshotArtifactManifest,
) error {
	if !equalState(prefix.State, expected.State) ||
		!bytes.Equal(prefix.UserCollection, expected.UserCollection) ||
		prefix.Bundle != expected.Bundle ||
		prefix.RelationManifestDigest != expected.RelationManifestDigest ||
		len(prefix.Relations) != len(expected.Relations) ||
		prefix.TargetChunkBytes != expected.TargetChunkBytes ||
		prefix.HeaderDigest != expected.HeaderDigest ||
		prefix.Chunks > expected.Chunks || prefix.SystemRows > expected.SystemRows ||
		prefix.UserRows > expected.UserRows || prefix.CaptureRows > expected.CaptureRows ||
		prefix.PayloadBytes > expected.PayloadBytes {
		return fmt.Errorf("%w: cursor belongs to another artifact", ErrSnapshotStage)
	}
	for index := range prefix.Relations {
		got, want := &prefix.Relations[index], &expected.Relations[index]
		if got.Relation != want.Relation || got.Kind != want.Kind ||
			!bytes.Equal(got.Collection, want.Collection) || got.Rows > want.Rows ||
			(got.ImageDigest != ([32]byte{}) && got.ImageDigest != want.ImageDigest) {
			return fmt.Errorf("%w: cursor relation belongs to another artifact", ErrSnapshotStage)
		}
	}
	return nil
}

func equalSnapshotArtifactManifest(left, right SnapshotArtifactManifest) bool {
	if !equalState(left.State, right.State) ||
		!bytes.Equal(left.UserCollection, right.UserCollection) ||
		left.Bundle != right.Bundle ||
		left.RelationManifestDigest != right.RelationManifestDigest ||
		len(left.Relations) != len(right.Relations) {
		return false
	}
	for i := range left.Relations {
		if left.Relations[i].Relation != right.Relations[i].Relation ||
			left.Relations[i].Kind != right.Relations[i].Kind ||
			!bytes.Equal(left.Relations[i].Collection, right.Relations[i].Collection) ||
			left.Relations[i].Rows != right.Relations[i].Rows ||
			left.Relations[i].ImageDigest != right.Relations[i].ImageDigest {
			return false
		}
	}
	return left.Seeded == right.Seeded &&
		left.TargetChunkBytes == right.TargetChunkBytes &&
		left.Chunks == right.Chunks && left.SystemRows == right.SystemRows &&
		left.UserRows == right.UserRows && left.CaptureRows == right.CaptureRows &&
		left.PayloadBytes == right.PayloadBytes &&
		left.EncodedBytes == right.EncodedBytes &&
		left.HeaderDigest == right.HeaderDigest &&
		left.LastChunkDigest == right.LastChunkDigest &&
		left.ImageDigest == right.ImageDigest &&
		left.CaptureImageDigest == right.CaptureImageDigest && left.Digest == right.Digest
}
