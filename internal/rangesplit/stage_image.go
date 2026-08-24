package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/store/durable"
)

var childStageImageDomain = []byte("vibedb/range-split/child-stage-image\x00")

type childStageImageWorkspace struct {
	snapshot durable.Snapshot
	document distribution.DocumentPointWorkspace
	scan     childStageImageScan
	visit    func(key, document []byte) error
	bound    *childStageImageScan
	scratch  []byte
	hasher   hash.Hash
	digest   [sha256.Size]byte
	size     [8]byte
	fixed    [56]byte
}

type childStageImageScan struct {
	stage     *ChildStage
	workspace *childStageImageWorkspace
	rows      uint64
	bytes     uint64
}

type childStageSealedImageAudit struct {
	stage  *ChildStage
	cursor *ChildStageCursor
	active bool
}

func (s *ChildStage) certifySealedImage(cursor *ChildStageCursor) (resultErr error) {
	if s == nil || s.collection == nil {
		return ErrChildStage
	}
	workspace := &s.image
	if err := s.collection.SnapshotInto(&workspace.snapshot); err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, workspace.snapshot.Close())
	}()
	if err := s.beginImageProof(cursor); err != nil {
		return err
	}
	defer s.cancelImageProof()
	buffer, err := workspace.snapshot.RangeRawBuffer(workspace.scratch, workspace.visit)
	workspace.scratch = buffer
	if err != nil {
		return err
	}
	rows, bytesCount, digest, err := s.finishImageProof()
	if err != nil {
		return err
	}
	cursor.imageRows, cursor.imageBytes = rows, bytesCount
	cursor.imageDigest = digest
	return nil
}

func (s *ChildStage) beginImageProof(cursor *ChildStageCursor) error {
	if s == nil || cursor == nil || cursor.phase != ChildStageSealed ||
		cursor.lastBatchDigest == ([sha256.Size]byte{}) {
		return ErrChildStage
	}
	workspace := &s.image
	h := workspace.hasher
	if h == nil {
		h = sha256.New()
		workspace.hasher = h
	}
	h.Reset()
	_, _ = h.Write(childStageImageDomain)
	_, _ = h.Write(cursor.planDigest[:])
	_, _ = h.Write(cursor.placementDigest[:])
	_, _ = h.Write(cursor.artifactDigest[:])
	_, _ = h.Write(cursor.lastBatchDigest[:])
	_, _ = h.Write(cursor.dataChainDigest[:])
	_, _ = h.Write(cursor.baseDigest[:])
	_, _ = h.Write(cursor.entryDigest[:])
	workspace.fixed = [56]byte{}
	workspace.fixed[0] = cursor.child
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], cursor.applied)
	binary.LittleEndian.PutUint64(workspace.fixed[16:24], cursor.term)
	binary.LittleEndian.PutUint64(workspace.fixed[24:32], cursor.routeGeneration)
	binary.LittleEndian.PutUint64(
		workspace.fixed[32:40], uint64(s.partitioner.children[cursor.child].OwnershipEpoch),
	)
	binary.LittleEndian.PutUint64(workspace.fixed[40:48], uint64(s.partitioner.target))
	_, _ = h.Write(workspace.fixed[:48])
	workspace.scan = childStageImageScan{stage: s, workspace: workspace}
	if workspace.visit == nil || workspace.bound != &workspace.scan {
		workspace.visit = workspace.scan.visitRow
		workspace.bound = &workspace.scan
	}
	return nil
}

func (s *ChildStage) finishImageProof() (
	rows uint64,
	bytesCount uint64,
	digest [sha256.Size]byte,
	err error,
) {
	if s == nil {
		return 0, 0, digest, ErrChildStage
	}
	workspace := &s.image
	if workspace.scan.stage != s || workspace.scan.workspace != workspace ||
		workspace.hasher == nil {
		return 0, 0, digest, ErrChildStage
	}
	rows, bytesCount = workspace.scan.rows, workspace.scan.bytes
	workspace.scan.stage, workspace.scan.workspace = nil, nil
	workspace.fixed = [56]byte{}
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], rows)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], bytesCount)
	_, _ = workspace.hasher.Write(workspace.fixed[:16])
	_ = workspace.hasher.Sum(workspace.digest[:0])
	return rows, bytesCount, workspace.digest, nil
}

func (s *ChildStage) cancelImageProof() {
	if s == nil {
		return
	}
	s.image.scan.stage, s.image.scan.workspace = nil, nil
}

func (s *ChildStage) verifySealedImage(cursor *ChildStageCursor) error {
	wantRows, wantBytes, wantDigest := cursor.imageRows, cursor.imageBytes, cursor.imageDigest
	computed := *cursor
	computed.imageRows, computed.imageBytes = 0, 0
	computed.imageDigest = [sha256.Size]byte{}
	if err := s.certifySealedImage(&computed); err != nil ||
		computed.imageRows != wantRows || computed.imageBytes != wantBytes ||
		computed.imageDigest != wantDigest {
		return errors.Join(ErrChildStage, err)
	}
	return nil
}

func (a *childStageSealedImageAudit) begin(
	stage *ChildStage,
	cursor *ChildStageCursor,
) error {
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
	stage, cursor := a.stage, a.cursor
	rows, bytesCount, digest, err := stage.finishImageProof()
	a.stage, a.cursor, a.active = nil, nil, false
	if err != nil || rows != cursor.imageRows || bytesCount != cursor.imageBytes ||
		digest != cursor.imageDigest {
		return errors.Join(ErrChildStage, err)
	}
	return nil
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
	if s.stage == nil || s.workspace == nil {
		return ErrChildStage
	}
	point, err := s.stage.partitioner.program.Point(document, &s.workspace.document)
	if err != nil || s.stage.partitioner.childFor(point) != int(s.stage.expected.Child) {
		return errors.Join(ErrChildStage, err)
	}
	rowBytes := uint64(len(key)) + uint64(len(document))
	if s.rows == math.MaxUint64 || s.bytes > math.MaxUint64-rowBytes {
		return ErrChildStage
	}
	s.rows++
	s.bytes += rowBytes
	hashTailFrame(s.workspace.hasher, &s.workspace.size, key)
	hashTailFrame(s.workspace.hasher, &s.workspace.size, document)
	return nil
}
