package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"math/bits"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/store/durable"
)

var (
	childStageImageDomain = []byte("vibedb/range-split/child-stage-image\x00")
	childStageRowDomain   = []byte("vibedb/range-split/child-stage-row\x00")
)

// childStageImageAccumulator is a constant-space authenticated multiset of
// rows. Each row contributes SHA-256(domain, key, value length, value digest)
// to a 256-bit modular sum. Keys are unique in the collection, so a logical
// replace subtracts the authenticated before witness and adds the after row.
// The construction retains SHA-256's 128-bit collision target while allowing
// O(1) insert, replace, and delete updates without an auxiliary index.
type childStageImageAccumulator struct {
	rows  uint64
	bytes uint64
	root  [sha256.Size]byte
}

type childStageImageWorkspace struct {
	snapshot durable.Snapshot
	document distribution.DocumentPointWorkspace
	scan     childStageImageScan
	visit    func(key, document []byte) error
	bound    *childStageImageScan
	scratch  []byte
	hasher   hash.Hash
	digest   [sha256.Size]byte
	value    [sha256.Size]byte
	size     [8]byte
	fixed    [56]byte
}

type childStageImageScan struct {
	stage       *ChildStage
	workspace   *childStageImageWorkspace
	cursor      *ChildStageCursor
	accumulator childStageImageAccumulator
}

type childStageSealedImageAudit struct {
	stage  *ChildStage
	cursor *ChildStageCursor
	active bool
}

func (s *ChildStage) accumulateArtifactRows(cursor *ChildStageCursor, rows ChildArtifactRows) error {
	if s == nil || cursor == nil || cursor.phase != ChildStageArtifact {
		return ErrChildStage
	}
	accumulator := childStageImageAccumulator{
		rows: cursor.imageRows, bytes: cursor.imageBytes, root: cursor.imageDigest,
	}
	iterator := rows.Iterator()
	for iterator.remaining != 0 {
		key, value, ok := iterator.Next()
		if !ok || accumulator.addRow(&s.image, key, value) != nil {
			return ErrChildStage
		}
	}
	cursor.imageRows, cursor.imageBytes = accumulator.rows, accumulator.bytes
	cursor.imageDigest = accumulator.root
	return nil
}

func (s *ChildStage) accumulateTailBatch(cursor *ChildStageCursor, batch TailBatch) error {
	if s == nil || cursor == nil || cursor.phase != ChildStageTail ||
		len(batch.transitions) != len(batch.routes) {
		return ErrChildStage
	}
	accumulator := childStageImageAccumulator{
		rows: cursor.imageRows, bytes: cursor.imageBytes, root: cursor.imageDigest,
	}
	for ordinal := range batch.transitions {
		transition, route := &batch.transitions[ordinal], batch.routes[ordinal]
		if route.before == batch.Child {
			before, err := s.partitioner.tailBeforeWitness(transition, &s.image.document)
			if err != nil || !before.Present || accumulator.removeWitness(
				&s.image, transition.Key, before.DocumentBytes, before.Digest,
			) != nil {
				return errors.Join(ErrChildStage, err)
			}
		}
		if route.after == batch.Child {
			if accumulator.addRow(&s.image, transition.Key, transition.After) != nil {
				return ErrChildStage
			}
		}
	}
	cursor.imageRows, cursor.imageBytes = accumulator.rows, accumulator.bytes
	cursor.imageDigest = accumulator.root
	return nil
}

func (a *childStageImageAccumulator) addRow(workspace *childStageImageWorkspace, key, value []byte) error {
	if workspace == nil || len(value) == 0 {
		return ErrChildStage
	}
	workspace.value = sha256.Sum256(value)
	return a.addWitness(workspace, key, uint32(len(value)), workspace.value)
}

func (a *childStageImageAccumulator) addWitness(
	workspace *childStageImageWorkspace, key []byte, valueBytes uint32,
	valueDigest [sha256.Size]byte,
) error {
	rowBytes := uint64(len(key)) + uint64(valueBytes)
	if a == nil || len(key) == 0 || valueBytes == 0 ||
		a.rows == math.MaxUint64 || a.bytes > math.MaxUint64-rowBytes {
		return ErrChildStage
	}
	digest := childStageRowDigest(workspace, key, valueBytes, valueDigest)
	addChildStageDigest(&a.root, digest)
	a.rows++
	a.bytes += rowBytes
	return nil
}

func (a *childStageImageAccumulator) removeWitness(
	workspace *childStageImageWorkspace, key []byte, valueBytes uint32,
	valueDigest [sha256.Size]byte,
) error {
	rowBytes := uint64(len(key)) + uint64(valueBytes)
	if a == nil || len(key) == 0 || valueBytes == 0 || a.rows == 0 || a.bytes < rowBytes {
		return ErrChildStage
	}
	digest := childStageRowDigest(workspace, key, valueBytes, valueDigest)
	subtractChildStageDigest(&a.root, digest)
	a.rows--
	a.bytes -= rowBytes
	return nil
}

func childStageRowDigest(
	workspace *childStageImageWorkspace, key []byte, valueBytes uint32,
	valueDigest [sha256.Size]byte,
) [sha256.Size]byte {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	h := workspace.hasher
	h.Reset()
	_, _ = h.Write(childStageRowDomain)
	hashTailFrame(h, &workspace.size, key)
	binary.LittleEndian.PutUint64(workspace.size[:], uint64(valueBytes))
	_, _ = h.Write(workspace.size[:])
	_, _ = h.Write(valueDigest[:])
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

func addChildStageDigest(target *[sha256.Size]byte, value [sha256.Size]byte) {
	carry := uint64(0)
	for offset := 0; offset < sha256.Size; offset += 8 {
		next, nextCarry := bits.Add64(
			binary.LittleEndian.Uint64(target[offset:offset+8]),
			binary.LittleEndian.Uint64(value[offset:offset+8]), carry,
		)
		binary.LittleEndian.PutUint64(target[offset:offset+8], next)
		carry = nextCarry
	}
}

func subtractChildStageDigest(target *[sha256.Size]byte, value [sha256.Size]byte) {
	borrow := uint64(0)
	for offset := 0; offset < sha256.Size; offset += 8 {
		next, nextBorrow := bits.Sub64(
			binary.LittleEndian.Uint64(target[offset:offset+8]),
			binary.LittleEndian.Uint64(value[offset:offset+8]), borrow,
		)
		binary.LittleEndian.PutUint64(target[offset:offset+8], next)
		borrow = nextBorrow
	}
}

// sealAccumulatedImage is O(1): artifact receipt and every tail apply already
// advanced the authenticated root stored in the durable cursor.
func (s *ChildStage) sealAccumulatedImage(cursor *ChildStageCursor) error {
	if s == nil || cursor == nil || cursor.phase != ChildStageSealed ||
		cursor.lastBatchDigest == ([sha256.Size]byte{}) ||
		(cursor.imageRows == 0) != (cursor.imageBytes == 0) {
		return ErrChildStage
	}
	root := cursor.imageDigest
	cursor.imageDigest = s.terminalImageDigest(cursor, root)
	if cursor.imageDigest == ([sha256.Size]byte{}) {
		return ErrChildStage
	}
	return nil
}

func (s *ChildStage) terminalImageDigest(
	cursor *ChildStageCursor, root [sha256.Size]byte,
) [sha256.Size]byte {
	workspace := &s.image
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	h := workspace.hasher
	h.Reset()
	_, _ = h.Write(childStageImageDomain)
	_, _ = h.Write(cursor.planDigest[:])
	_, _ = h.Write(cursor.placementDigest[:])
	_, _ = h.Write(cursor.artifactDigest[:])
	_, _ = h.Write(cursor.lastBatchDigest[:])
	_, _ = h.Write(cursor.dataChainDigest[:])
	_, _ = h.Write(cursor.baseDigest[:])
	_, _ = h.Write(cursor.entryDigest[:])
	clear(workspace.fixed[:])
	workspace.fixed[0] = cursor.child
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], cursor.applied)
	binary.LittleEndian.PutUint64(workspace.fixed[16:24], cursor.term)
	binary.LittleEndian.PutUint64(workspace.fixed[24:32], cursor.routeGeneration)
	binary.LittleEndian.PutUint64(
		workspace.fixed[32:40], uint64(s.partitioner.children[cursor.child].OwnershipEpoch),
	)
	binary.LittleEndian.PutUint64(workspace.fixed[40:48], uint64(s.partitioner.target))
	_, _ = h.Write(workspace.fixed[:48])
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], cursor.imageRows)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], cursor.imageBytes)
	_, _ = h.Write(workspace.fixed[:16])
	_, _ = h.Write(root[:])
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

// A sealed cursor recovered after a crash is audited against the physical
// collection once. This is recovery work, not cutover work, and detects store
// corruption or out-of-band mutation without putting an O(rows) scan on the
// normal split seal path.
func (s *ChildStage) verifySealedImage(cursor *ChildStageCursor) (resultErr error) {
	if s == nil || s.collection == nil {
		return ErrChildStage
	}
	workspace := &s.image
	if err := s.collection.SnapshotInto(&workspace.snapshot); err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, workspace.snapshot.Close()) }()
	if err := s.beginImageProof(cursor); err != nil {
		return err
	}
	defer s.cancelImageProof()
	buffer, err := workspace.snapshot.RangeRawBuffer(workspace.scratch, workspace.visit)
	workspace.scratch = buffer
	if err != nil {
		return err
	}
	return s.finishImageProof()
}

func (s *ChildStage) beginImageProof(cursor *ChildStageCursor) error {
	if s == nil || cursor == nil || cursor.phase != ChildStageSealed ||
		cursor.lastBatchDigest == ([sha256.Size]byte{}) {
		return ErrChildStage
	}
	workspace := &s.image
	workspace.scan = childStageImageScan{stage: s, workspace: workspace, cursor: cursor}
	if workspace.visit == nil || workspace.bound != &workspace.scan {
		workspace.visit = workspace.scan.visitRow
		workspace.bound = &workspace.scan
	}
	return nil
}

func (s *ChildStage) finishImageProof() error {
	if s == nil {
		return ErrChildStage
	}
	workspace := &s.image
	scan := &workspace.scan
	if scan.stage != s || scan.workspace != workspace || scan.cursor == nil {
		return ErrChildStage
	}
	cursor, accumulator := scan.cursor, scan.accumulator
	scan.stage, scan.workspace, scan.cursor = nil, nil, nil
	got := s.terminalImageDigest(cursor, accumulator.root)
	if accumulator.rows != cursor.imageRows || accumulator.bytes != cursor.imageBytes ||
		got != cursor.imageDigest {
		return ErrChildStage
	}
	return nil
}

func (s *ChildStage) cancelImageProof() {
	if s == nil {
		return
	}
	s.image.scan.stage, s.image.scan.workspace, s.image.scan.cursor = nil, nil, nil
}

func (a *childStageSealedImageAudit) begin(stage *ChildStage, cursor *ChildStageCursor) error {
	if a == nil || stage == nil || cursor == nil || a.active {
		return ErrChildStage
	}
	if err := stage.beginImageProof(cursor); err != nil {
		return err
	}
	a.stage, a.cursor, a.active = stage, cursor, true
	return nil
}

func (a *childStageSealedImageAudit) visit(key, value []byte) error {
	if a == nil || !a.active || a.stage == nil || a.cursor == nil {
		return ErrChildStage
	}
	return a.stage.image.scan.visitRow(key, value)
}

func (a *childStageSealedImageAudit) finish() error {
	if a == nil || !a.active || a.stage == nil || a.cursor == nil {
		return ErrChildStage
	}
	stage := a.stage
	err := stage.finishImageProof()
	a.stage, a.cursor, a.active = nil, nil, false
	return err
}

func (a *childStageSealedImageAudit) close() {
	if a == nil {
		return
	}
	if a.active && a.stage != nil {
		a.stage.cancelImageProof()
	}
	a.stage, a.cursor, a.active = nil, nil, false
}

func (s *childStageImageScan) visitRow(key, document []byte) error {
	if s.stage == nil || s.workspace == nil || s.cursor == nil {
		return ErrChildStage
	}
	point, err := s.stage.partitioner.program.Point(document, &s.workspace.document)
	if err != nil || s.stage.partitioner.childFor(point) != int(s.stage.expected.Child) {
		return errors.Join(ErrChildStage, err)
	}
	return s.accumulator.addRow(s.workspace, key, document)
}
