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
}

// SnapshotArtifactStage applies verified chunks only to caller-owned,
// non-serving durable collections. It never publishes routing, ownership, Raft
// storage, or a serving Machine. One stage is serial; concurrent Receive or
// OpenCandidate calls are rejected by its mutex and terminal state.
type SnapshotArtifactStage struct {
	mu sync.Mutex

	expected SnapshotArtifactManifest
	system   CollectionTarget
	user     CollectionTarget
	cursor   *SnapshotArtifactCursor

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
		system.Validation != ValidationSchemaFreeJSON ||
		system.ValidationDigest != ([32]byte{}) || system.Validator != nil ||
		system.ObserveMutationAttempt != nil {
		return nil, fmt.Errorf("%w: system target: %v", ErrSnapshotStage, err)
	}
	if err := user.validate(); err != nil ||
		user.Validation != ValidationDeterministicMutation ||
		user.ValidationDigest == ([32]byte{}) || user.Validator == nil {
		return nil, fmt.Errorf("%w: user target: %v", ErrSnapshotStage, err)
	}
	if system.Collection == user.Collection ||
		user.Limits.MaxKeyBytes > replication.MaxMutationKeyBytes ||
		user.Limits.MaxDocumentBytes > replication.MaxMutationValueBytes {
		return nil, fmt.Errorf("%w: collection targets", ErrSnapshotStage)
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
	systemRows, userRows := system.Collection.Len(), user.Collection.Len()
	if systemRows > expected.SystemRows || userRows > expected.UserRows {
		return nil, fmt.Errorf("%w: destination row counts exceed artifact", ErrSnapshotStage)
	}
	if cursor != nil {
		prefix := cursor.PrefixManifest()
		if systemRows < prefix.SystemRows || userRows < prefix.UserRows {
			return nil, fmt.Errorf("%w: cursor advances beyond durable rows", ErrSnapshotStage)
		}
	}
	stage := &SnapshotArtifactStage{
		expected: cloneSnapshotArtifactManifest(expected), system: system, user: user,
		cursor: cursor, payloadBuffer: make([]byte, 0, MaxSnapshotArtifactChunkBytes),
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
	switch rows.Collection() {
	case SnapshotArtifactSystem:
		collection = s.system.Collection
	case SnapshotArtifactUser:
		collection = s.user.Collection
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

// OpenCandidate performs the expensive final proof over both completed files:
// system-record validation, retained-completion validation, user placement
// validation, and canonical-image verification. Success returns a non-serving
// Machine at the exact expected publication. The caller still needs learner
// membership, log-tail catch-up, and topology cutover before serving it.
func (s *SnapshotArtifactStage) OpenCandidate(
	bootstrap *pb.Snapshot,
	txnLog *durable.TxnLog,
	options Options,
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
	machine, err := Open(
		s.expected.State.Binding, bootstrap, s.system,
		UserCollection{Name: string(s.expected.UserCollection), Target: s.user},
		txnLog, options,
	)
	if err != nil {
		return nil, err
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

func validateExpectedSnapshotArtifact(expected SnapshotArtifactManifest) error {
	if err := validateState(expected.State); err != nil ||
		len(expected.UserCollection) == 0 ||
		len(expected.UserCollection) > replication.MaxCollectionBytes ||
		!utf8.Valid(expected.UserCollection) || bytes.IndexByte(expected.UserCollection, 0) >= 0 ||
		bytes.Equal(expected.UserCollection, []byte(systemCollectionName)) ||
		expected.TargetChunkBytes < MinSnapshotArtifactChunkBytes ||
		expected.TargetChunkBytes > MaxSnapshotArtifactChunkBytes ||
		expected.Chunks == 0 || expected.SystemRows != expected.State.CompletionCount+1 ||
		expected.PayloadBytes == 0 || expected.EncodedBytes == 0 ||
		expected.HeaderDigest == ([32]byte{}) ||
		expected.LastChunkDigest == ([32]byte{}) || expected.ImageDigest == ([32]byte{}) ||
		expected.Digest == ([32]byte{}) {
		return fmt.Errorf("%w: expected artifact", ErrSnapshotStage)
	}
	stateEnvelope, err := AppendState(nil, expected.State)
	if err != nil {
		return fmt.Errorf("%w: expected state", ErrSnapshotStage)
	}
	header, headerDigest, err := makeSnapshotArtifactHeader(
		stateEnvelope, string(expected.UserCollection), int(expected.TargetChunkBytes),
	)
	wantEncodedBytes, encodedBytesOK := snapshotArtifactEncodedBytes(
		uint64(len(header)), expected.Chunks, expected.PayloadBytes, true,
	)
	_, wantFooterDigest := makeSnapshotArtifactFooter(
		expected.Chunks, expected.SystemRows, expected.UserRows,
		expected.PayloadBytes, expected.EncodedBytes,
		expected.LastChunkDigest, expected.HeaderDigest, expected.ImageDigest,
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
		prefix.TargetChunkBytes != expected.TargetChunkBytes ||
		prefix.HeaderDigest != expected.HeaderDigest ||
		prefix.Chunks > expected.Chunks || prefix.SystemRows > expected.SystemRows ||
		prefix.UserRows > expected.UserRows || prefix.PayloadBytes > expected.PayloadBytes {
		return fmt.Errorf("%w: cursor belongs to another artifact", ErrSnapshotStage)
	}
	return nil
}

func equalSnapshotArtifactManifest(left, right SnapshotArtifactManifest) bool {
	return equalState(left.State, right.State) &&
		bytes.Equal(left.UserCollection, right.UserCollection) &&
		left.TargetChunkBytes == right.TargetChunkBytes &&
		left.Chunks == right.Chunks && left.SystemRows == right.SystemRows &&
		left.UserRows == right.UserRows && left.PayloadBytes == right.PayloadBytes &&
		left.EncodedBytes == right.EncodedBytes &&
		left.HeaderDigest == right.HeaderDigest &&
		left.LastChunkDigest == right.LastChunkDigest &&
		left.ImageDigest == right.ImageDigest && left.Digest == right.Digest
}
