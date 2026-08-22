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

func (s *ChildStage) certifySealedImage(cursor *ChildStageCursor) (resultErr error) {
	if s == nil || cursor == nil || cursor.phase != ChildStageSealed ||
		cursor.lastBatchDigest == ([sha256.Size]byte{}) {
		return ErrChildStage
	}
	workspace := &s.image
	if err := s.collection.SnapshotInto(&workspace.snapshot); err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, workspace.snapshot.Close())
	}()
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
	buffer, err := workspace.snapshot.RangeRawBuffer(workspace.scratch, workspace.visit)
	workspace.scratch = buffer
	rows, bytesCount := workspace.scan.rows, workspace.scan.bytes
	workspace.scan.stage, workspace.scan.workspace = nil, nil
	if err != nil {
		return err
	}
	workspace.fixed = [56]byte{}
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], rows)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], bytesCount)
	_, _ = h.Write(workspace.fixed[:16])
	_ = h.Sum(workspace.digest[:0])
	cursor.imageRows, cursor.imageBytes = rows, bytesCount
	cursor.imageDigest = workspace.digest
	return nil
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
