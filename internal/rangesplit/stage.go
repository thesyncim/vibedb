package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

var (
	ErrChildStage = errors.New("rangesplit: invalid child stage")
	// ErrChildStageOutcomeUnknown means that destination rows are durable but
	// cursor replacement did not return a definite outcome. Reopen the cursor
	// store or retry the exact artifact range or tail batch.
	ErrChildStageOutcomeUnknown = errors.New("rangesplit: child stage cursor outcome unknown")
)

const DefaultChildStageCheckpointBytes = 64 << 20

// ChildStageCursorPersistence must durably replace the cursor before it
// returns. raw is borrowed for the call. The stage orders destination updates
// before cursor persistence, so recovery can replay an uncheckpointed prefix.
type ChildStageCursorPersistence func(raw []byte) error

// ChildStageOptions bounds artifact bytes that a crash can replay. Zero uses
// DefaultChildStageCheckpointBytes. ReceiveArtifact also persists its latest
// applied prefix before every return.
type ChildStageOptions struct {
	CheckpointBytes uint64
}

// ChildStage owns one serial, non-serving durable child collection. It applies
// verified artifact rows, validates the complete collection against the exact
// artifact digest, and then accepts consecutive translated tail batches. It
// does not publish routing or ownership.
type ChildStage struct {
	mu sync.Mutex

	partitioner *Partitioner
	expected    ChildArtifactManifest
	collection  *durable.Collection
	cursor      *ChildStageCursor

	checkpointBytes uint64
	persistedOffset uint64
	cursorBuffer    []byte
	cursorCodec     ChildStageCursorWorkspace
	scanBuffer      []byte
	verify          ChildArtifactVerifyWorkspace
	validator       childArtifactWriter
	tailVerify      TailBatchVerifyWorkspace
}

// NewChildStage creates one stage with the default artifact replay bound.
func NewChildStage(
	partitioner *Partitioner,
	expected ChildArtifactManifest,
	collection *durable.Collection,
	persistedCursor []byte,
) (*ChildStage, error) {
	return NewChildStageWithOptions(
		partitioner, expected, collection, persistedCursor, ChildStageOptions{},
	)
}

// NewChildStageWithOptions creates one stage with an explicit replay bound.
func NewChildStageWithOptions(
	partitioner *Partitioner,
	expected ChildArtifactManifest,
	collection *durable.Collection,
	persistedCursor []byte,
	options ChildStageOptions,
) (*ChildStage, error) {
	checkpointBytes := options.CheckpointBytes
	if checkpointBytes == 0 {
		checkpointBytes = DefaultChildStageCheckpointBytes
	}
	if checkpointBytes < MaxChildArtifactChunkBytes ||
		validateExpectedChildArtifact(partitioner, expected) != nil ||
		collection == nil || !collection.SupportsUpdate() ||
		!collection.HasSynchronousDurability() {
		return nil, ErrChildStage
	}
	var cursor *ChildStageCursor
	var err error
	if len(persistedCursor) != 0 {
		cursor, err = OpenChildStageCursor(persistedCursor)
		if err != nil || !childStageCursorMatchesExpected(cursor, expected) {
			return nil, ErrChildStage
		}
	}
	rows := collection.Len()
	if cursor == nil || cursor.phase == ChildStageArtifact {
		if rows > expected.Rows || cursor != nil && rows < cursor.artifactRows {
			return nil, ErrChildStage
		}
	}
	stage := &ChildStage{
		partitioner: partitioner, expected: expected, collection: collection,
		cursor: cursor, checkpointBytes: checkpointBytes,
	}
	if cursor != nil {
		stage.persistedOffset = cursor.artifactOffset
	}
	return stage, nil
}

// Cursor returns a detached current durable cursor.
func (s *ChildStage) Cursor() (ChildStageCursor, bool) {
	if s == nil {
		return ChildStageCursor{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursor == nil {
		return ChildStageCursor{}, false
	}
	return *s.cursor, true
}

// ArtifactComplete reports whether the exact child image was validated and
// the durable cursor entered tail catch-up.
func (s *ChildStage) ArtifactComplete() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor != nil && s.cursor.phase == ChildStageTail
}

// ReceiveArtifact verifies one complete artifact stream. It skips callbacks
// for an already durable prefix only after the verifier proves that prefix
// again. It forces a cursor at return, even below CheckpointBytes.
func (s *ChildStage) ReceiveArtifact(
	r io.Reader,
	persist ChildStageCursorPersistence,
) (ChildArtifactManifest, error) {
	if s == nil || r == nil || persist == nil {
		return ChildArtifactManifest{}, ErrChildStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursor != nil && s.cursor.phase == ChildStageTail {
		return s.expected, nil
	}
	working := s.initialCursor()
	if s.cursor != nil {
		working = *s.cursor
	}
	persistedChunks := working.artifactChunks
	verifiedRows, verifiedPayload := uint64(0), uint64(0)
	manifest, verifyErr := s.partitioner.VerifyChildArtifact(
		r, s.expected.Child,
		ChildArtifactCallbacks{Rows: func(
			checkpoint ChildArtifactCheckpoint,
			rows ChildArtifactRows,
		) error {
			if checkpoint.Sequence >= s.expected.Chunks ||
				verifiedRows > math.MaxUint64-checkpoint.Rows ||
				verifiedPayload > math.MaxUint64-checkpoint.PayloadBytes {
				return ErrChildStage
			}
			verifiedRows += checkpoint.Rows
			verifiedPayload += checkpoint.PayloadBytes
			if verifiedRows > s.expected.Rows || verifiedPayload > s.expected.PayloadBytes ||
				checkpoint.EndOffset >= s.expected.EncodedBytes {
				return ErrChildStage
			}
			if checkpoint.Sequence < persistedChunks {
				if checkpoint.Sequence+1 == persistedChunks &&
					(checkpoint.Digest != working.lastChunkDigest ||
						checkpoint.EndOffset != working.artifactOffset ||
						verifiedRows != working.artifactRows ||
						verifiedPayload != working.artifactPayload) {
					return ErrChildStage
				}
				return nil
			}
			if checkpoint.Sequence != working.artifactChunks {
				return ErrChildStage
			}
			if err := s.applyArtifactRows(rows); err != nil {
				return err
			}
			working.artifactChunks++
			working.artifactRows = verifiedRows
			working.artifactPayload = verifiedPayload
			working.artifactOffset = checkpoint.EndOffset
			working.lastChunkDigest = checkpoint.Digest
			if working.artifactOffset-s.persistedOffset >= s.checkpointBytes {
				return s.persistCursor(&working, persist)
			}
			return nil
		}},
		&s.verify,
	)
	if working.artifactChunks > persistedChunks &&
		(s.cursor == nil || working.artifactChunks > s.cursor.artifactChunks) {
		if persistErr := s.persistCursor(&working, persist); persistErr != nil {
			if verifyErr != nil {
				return ChildArtifactManifest{}, errors.Join(verifyErr, persistErr)
			}
			return ChildArtifactManifest{}, persistErr
		}
	}
	if verifyErr != nil {
		return ChildArtifactManifest{}, verifyErr
	}
	if !equalChildArtifactManifest(manifest, s.expected) {
		return ChildArtifactManifest{}, ErrChildStage
	}
	if err := s.validateDestinationArtifact(); err != nil {
		return ChildArtifactManifest{}, err
	}
	tail := s.initialCursor()
	tail.phase = ChildStageTail
	tail.artifactChunks = s.expected.Chunks
	tail.artifactRows = s.expected.Rows
	tail.artifactPayload = s.expected.PayloadBytes
	tail.artifactOffset = s.expected.EncodedBytes
	tail.lastChunkDigest = s.expected.LastChunkDigest
	if err := s.persistCursor(&tail, persist); err != nil {
		return ChildArtifactManifest{}, err
	}
	return manifest, nil
}

// ApplyTailBatch applies one exact child batch and then advances the durable
// cursor. An exact retry is a no-op. A failed cursor persistence leaves a safe
// idempotent replay because the collection remains non-serving and serial.
func (s *ChildStage) ApplyTailBatch(
	batch TailBatch,
	persist ChildStageCursorPersistence,
) error {
	if s == nil || persist == nil {
		return ErrChildStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cursor == nil || s.cursor.phase != ChildStageTail ||
		s.partitioner.VerifyTailBatch(batch, &s.tailVerify) != nil ||
		batch.Child != s.expected.Child ||
		batch.SourceBaseDigest != s.expected.Source.BaseDigest ||
		batch.ChildBaseDigest != s.expected.Digest {
		return ErrChildStage
	}
	current := s.cursor
	if batch.Applied == current.applied {
		if current.lastBatchDigest != ([sha256.Size]byte{}) &&
			batch.Digest == current.lastBatchDigest && batch.Term == current.term &&
			batch.EntryDigest == current.entryDigest &&
			batch.AfterLogicalDigest == current.logicalDigest &&
			batch.RouteGeneration == current.routeGeneration {
			return nil
		}
		return ErrChildStage
	}
	if current.applied == math.MaxUint64 || batch.Applied != current.applied+1 ||
		batch.Term < current.term || batch.RouteGeneration != current.routeGeneration ||
		batch.PreviousEntryDigest != current.entryDigest ||
		batch.BeforeLogicalDigest != current.logicalDigest {
		return ErrChildStage
	}
	if err := s.applyTailOperations(batch); err != nil {
		return err
	}
	next := *current
	next.applied = batch.Applied
	next.term = batch.Term
	next.entryDigest = batch.EntryDigest
	next.logicalDigest = batch.AfterLogicalDigest
	next.lastBatchDigest = batch.Digest
	return s.persistCursor(&next, persist)
}

func (s *ChildStage) initialCursor() ChildStageCursor {
	return ChildStageCursor{
		phase: ChildStageArtifact, child: s.expected.Child,
		planDigest: s.expected.PlanDigest, placementDigest: s.expected.PlacementDigest,
		artifactDigest: s.expected.Digest, headerDigest: s.expected.HeaderDigest,
		lastChunkDigest: s.expected.HeaderDigest,
		logicalDigest:   s.expected.Source.LogicalDigest,
		baseDigest:      s.expected.Source.BaseDigest,
		entryDigest:     s.expected.Source.EntryDigest,
		applied:         s.expected.Source.Applied, term: s.expected.Source.Term,
		routeGeneration: s.expected.Source.RouteGeneration,
	}
}

func (s *ChildStage) persistCursor(
	next *ChildStageCursor,
	persist ChildStageCursorPersistence,
) error {
	encoded, err := AppendChildStageCursorWithWorkspace(
		s.cursorBuffer[:0], next, &s.cursorCodec,
	)
	if err != nil {
		return err
	}
	s.cursorBuffer = encoded
	if err := persist(encoded); err != nil {
		return errors.Join(ErrChildStageOutcomeUnknown, err)
	}
	copy := *next
	s.cursor = &copy
	s.persistedOffset = next.artifactOffset
	return nil
}

func (s *ChildStage) applyArtifactRows(rows ChildArtifactRows) error {
	iterator := rows.Iterator()
	var pendingKey, pendingValue []byte
	pending := false
	for pending || iterator.remaining != 0 {
		added := 0
		err := s.collection.Update(func(batch *durable.WriteBatch) error {
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
			return ErrChildStage
		}
	}
	return nil
}

func (s *ChildStage) applyTailOperations(batch TailBatch) error {
	iterator := batch.Iterator()
	var pending TailOperation
	hasPending := false
	for {
		if !hasPending {
			if !iterator.Next() {
				return nil
			}
			pending = iterator.Operation()
			hasPending = true
		}
		added := 0
		err := s.collection.Update(func(write *durable.WriteBatch) error {
			for hasPending {
				var err error
				switch pending.Kind {
				case replication.MutationPut:
					err = write.Put(pending.Key, pending.Value)
				case replication.MutationDelete:
					err = write.Delete(pending.Key)
				default:
					return ErrChildStage
				}
				if err != nil {
					if errors.Is(err, durable.ErrBatchTooLarge) && added != 0 {
						return nil
					}
					return err
				}
				hasPending = false
				added++
				if iterator.Next() {
					pending = iterator.Operation()
					hasPending = true
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if added == 0 {
			return ErrChildStage
		}
	}
}

func (s *ChildStage) validateDestinationArtifact() error {
	writer := &s.validator
	if err := writer.prepare(
		s.partitioner, s.expected.Child, s.expected.Source,
		int(s.expected.TargetChunkBytes), io.Discard, nil,
	); err != nil {
		return err
	}
	if err := writer.writeHeader(); err != nil {
		return err
	}
	buffer, err := s.collection.RangeRawCurrentBuffer(s.scanBuffer, writer.accept)
	s.scanBuffer = buffer
	if err != nil {
		return err
	}
	manifest, err := writer.finish(s.partitioner, s.expected.Source)
	if err != nil {
		return err
	}
	if !equalChildArtifactManifest(manifest, s.expected) {
		return ErrChildStage
	}
	return nil
}

func validateExpectedChildArtifact(
	p *Partitioner,
	expected ChildArtifactManifest,
) error {
	if p == nil || !expected.Present || int(expected.Child) >= int(p.childCount) ||
		expected.Child == p.retained || expected.PlanDigest != p.digest ||
		expected.PlacementDigest != p.program.Digest() ||
		expected.TargetRoutingVersion != p.target ||
		expected.Descriptor != p.artifactDescriptor(expected.Child) ||
		expected.Digest == ([sha256.Size]byte{}) {
		return ErrChildStage
	}
	target, err := normalizeChildArtifactTarget(int(expected.TargetChunkBytes))
	if err != nil || target != int(expected.TargetChunkBytes) {
		return ErrChildStage
	}
	hasher := sha256.New()
	var computed [sha256.Size]byte
	header, headerDigest, err := makeChildArtifactHeader(
		p, expected.Child, expected.Source, target, nil, hasher, &computed,
	)
	if err != nil || headerDigest != expected.HeaderDigest {
		return ErrChildStage
	}
	if expected.Rows > math.MaxUint64/childArtifactRowHeaderBytes ||
		expected.RowBytes > math.MaxUint64-expected.Rows*childArtifactRowHeaderBytes ||
		expected.PayloadBytes != expected.RowBytes+expected.Rows*childArtifactRowHeaderBytes ||
		expected.Chunks > expected.Rows ||
		expected.Chunks > (math.MaxUint64-uint64(len(header))-childArtifactFooterBytes)/
			(childArtifactChunkHeaderBytes+sha256.Size) {
		return ErrChildStage
	}
	overhead := expected.Chunks * (childArtifactChunkHeaderBytes + sha256.Size)
	if expected.PayloadBytes > math.MaxUint64-uint64(len(header))-overhead-childArtifactFooterBytes ||
		expected.EncodedBytes != uint64(len(header))+overhead+
			expected.PayloadBytes+childArtifactFooterBytes ||
		expected.PayloadBytes >= expected.EncodedBytes {
		return ErrChildStage
	}
	if expected.Chunks == 0 {
		if expected.Rows != 0 || expected.RowBytes != 0 || expected.PayloadBytes != 0 ||
			expected.LastChunkDigest != expected.HeaderDigest {
			return ErrChildStage
		}
	} else if expected.Rows == 0 || expected.PayloadBytes == 0 ||
		expected.LastChunkDigest == ([sha256.Size]byte{}) {
		return ErrChildStage
	}
	var footer [childArtifactFooterBytes]byte
	copy(footer[0:8], childArtifactFooterMagic[:])
	binary.LittleEndian.PutUint16(footer[8:10], childArtifactFormat)
	binary.LittleEndian.PutUint16(footer[10:12], childArtifactFooterBytes)
	binary.LittleEndian.PutUint32(footer[12:16], childArtifactFooterBytes)
	binary.LittleEndian.PutUint64(footer[16:24], expected.Chunks)
	binary.LittleEndian.PutUint64(footer[24:32], expected.Rows)
	binary.LittleEndian.PutUint64(footer[32:40], expected.RowBytes)
	binary.LittleEndian.PutUint64(footer[40:48], expected.PayloadBytes)
	binary.LittleEndian.PutUint64(footer[48:56], expected.EncodedBytes)
	binary.LittleEndian.PutUint64(footer[56:64], uint64(expected.Child))
	copy(footer[64:96], expected.LastChunkDigest[:])
	copy(footer[96:128], expected.HeaderDigest[:])
	if childArtifactDigest(childArtifactFooterDomain, footer[:128]) != expected.Digest {
		return ErrChildStage
	}
	return nil
}

func childStageCursorMatchesExpected(
	cursor *ChildStageCursor,
	expected ChildArtifactManifest,
) bool {
	if cursor == nil || cursor.child != expected.Child ||
		cursor.planDigest != expected.PlanDigest ||
		cursor.placementDigest != expected.PlacementDigest ||
		cursor.artifactDigest != expected.Digest ||
		cursor.headerDigest != expected.HeaderDigest ||
		cursor.baseDigest != expected.Source.BaseDigest ||
		cursor.routeGeneration != expected.Source.RouteGeneration ||
		cursor.artifactChunks > expected.Chunks || cursor.artifactRows > expected.Rows ||
		cursor.artifactPayload > expected.PayloadBytes ||
		cursor.artifactOffset > expected.EncodedBytes {
		return false
	}
	if cursor.phase == ChildStageArtifact {
		return cursor.applied == expected.Source.Applied &&
			cursor.term == expected.Source.Term &&
			cursor.logicalDigest == expected.Source.LogicalDigest &&
			cursor.entryDigest == expected.Source.EntryDigest &&
			cursor.artifactOffset < expected.EncodedBytes &&
			(cursor.artifactChunks != expected.Chunks ||
				cursor.lastChunkDigest == expected.LastChunkDigest)
	}
	if cursor.phase != ChildStageTail ||
		cursor.artifactChunks != expected.Chunks ||
		cursor.artifactRows != expected.Rows ||
		cursor.artifactPayload != expected.PayloadBytes ||
		cursor.artifactOffset != expected.EncodedBytes ||
		cursor.lastChunkDigest != expected.LastChunkDigest ||
		cursor.applied < expected.Source.Applied || cursor.term < expected.Source.Term {
		return false
	}
	if cursor.applied == expected.Source.Applied {
		return cursor.term == expected.Source.Term &&
			cursor.logicalDigest == expected.Source.LogicalDigest &&
			cursor.entryDigest == expected.Source.EntryDigest &&
			cursor.lastBatchDigest == ([sha256.Size]byte{})
	}
	return cursor.lastBatchDigest != ([sha256.Size]byte{})
}

func equalChildArtifactManifest(left, right ChildArtifactManifest) bool {
	return left.Present == right.Present && left.Child == right.Child &&
		left.PlanDigest == right.PlanDigest &&
		left.PlacementDigest == right.PlacementDigest && left.Source == right.Source &&
		left.TargetRoutingVersion == right.TargetRoutingVersion &&
		left.Descriptor == right.Descriptor &&
		left.TargetChunkBytes == right.TargetChunkBytes &&
		left.Chunks == right.Chunks && left.Rows == right.Rows &&
		left.RowBytes == right.RowBytes && left.PayloadBytes == right.PayloadBytes &&
		left.EncodedBytes == right.EncodedBytes &&
		left.HeaderDigest == right.HeaderDigest &&
		left.LastChunkDigest == right.LastChunkDigest && left.Digest == right.Digest
}
