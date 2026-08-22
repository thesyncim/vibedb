package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	ErrRetainedPrune               = errors.New("rangesplit: invalid retained-range prune")
	ErrRetainedPruneOutcomeUnknown = errors.New(
		"rangesplit: retained-range prune cursor outcome unknown",
	)
)

var (
	retainedPruneBatchDomain = []byte("vibedb/range-split/retained-prune-batch\x00")
	retainedPruneProofDomain = []byte("vibedb/range-split/retained-prune-proof\x00")
)

const (
	DefaultRetainedPruneKeys     = 256
	DefaultRetainedPruneKeyBytes = 1 << 20
	DefaultRetainedPruneScanRows = 64 << 10
)

type RetainedPruneLimits struct {
	MaxKeys     int
	MaxKeyBytes int
	MaxScanRows uint64
}

type RetainedPruneCursorPersistence func(raw []byte) error

// RetainedPruneBatch is one exact ordered set of out-of-range source keys. The
// controller must encode these keys as deletes in one normal replicated
// command at the certificate's post-seal serving fences. Keys are borrowed
// from the workspace until its next use.
type RetainedPruneBatch struct {
	Digest   [sha256.Size]byte
	Count    uint64
	KeyBytes uint64
	keys     [][]byte
}

func (b RetainedPruneBatch) Iterator() RetainedPruneKeyIterator {
	return RetainedPruneKeyIterator{keys: b.keys}
}

type RetainedPruneKeyIterator struct {
	keys [][]byte
	next int
}

func (i *RetainedPruneKeyIterator) Next() bool {
	if i == nil || i.next >= len(i.keys) {
		return false
	}
	i.next++
	return true
}

func (i *RetainedPruneKeyIterator) Key() []byte {
	if i == nil || i.next == 0 || i.next > len(i.keys) {
		return nil
	}
	return i.keys[i.next-1]
}

// RetainedPruner plans bounded replicated deletes and confirms their exact
// atomically captured transitions. It never mutates the user collection
// directly, so every intermediate source data-chain digest remains reopen-safe.
type RetainedPruner struct {
	mu sync.Mutex

	partitioner *Partitioner
	certificate CutoverCertificate
	cursor      RetainedPruneCursor
	scanAfter   []byte
	resumeAfter []byte
	cursorRaw   []byte
	cursorCodec RetainedPruneCursorWorkspace
}

type RetainedPruneWorkspace struct {
	document distribution.DocumentPointWorkspace
	capture  SourceCaptureWorkspace
	keys     [][]byte
	arena    []byte
	resume   []byte
	scanRaw  []byte
	hasher   hash.Hash
	digest   [sha256.Size]byte
	size     [8]byte
	fixed    [40]byte
	scan     retainedPruneScan
	visit    func(key, document []byte) error
	bound    *retainedPruneScan
}

func NewRetainedPruner(
	partitioner *Partitioner,
	certificate CutoverCertificate,
	persistedCursor []byte,
) (*RetainedPruner, error) {
	if partitioner == nil || partitioner.VerifyCutoverCertificate(certificate) != nil {
		return nil, ErrRetainedPrune
	}
	pruner := &RetainedPruner{partitioner: partitioner, certificate: certificate}
	if len(persistedCursor) == 0 {
		cut := certificate.SourceCut()
		coordinates := certificate.SourceCoordinates()
		pruner.cursor = RetainedPruneCursor{
			phase: RetainedPruneScan, retained: partitioner.retained,
			applied: cut.Applied, term: cut.Term,
			ownershipEpoch:  coordinates.OwnershipEpoch,
			routingVersion:  coordinates.RoutingVersion,
			routeGeneration: coordinates.RouteGeneration,
			plan:            certificate.plan, placement: certificate.placement,
			cutover: certificate.digest, dataChain: cut.DataChainDigest,
			base: cut.BaseDigest, entry: cut.EntryDigest,
		}
		return pruner, nil
	}
	cursor, err := OpenRetainedPruneCursor(persistedCursor)
	if err != nil || !retainedPruneCursorMatches(partitioner, certificate, cursor) {
		return nil, ErrRetainedPrune
	}
	pruner.installCursor(*cursor)
	return pruner, nil
}

func (p *RetainedPruner) Cursor() RetainedPruneCursor {
	if p == nil {
		return RetainedPruneCursor{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.cursor
	result.scanAfter = bytes.Clone(p.cursor.scanAfter)
	result.resumeAfter = bytes.Clone(p.cursor.resumeAfter)
	return result
}

// VerifyRetainedPruneCompletion proves that cursor is the terminal retained
// image produced from certificate by this exact split plan. The cursor remains
// evidence only; catalog publication still requires its own generation CAS.
func (p *Partitioner) VerifyRetainedPruneCompletion(
	certificate CutoverCertificate,
	cursor RetainedPruneCursor,
) error {
	if p == nil || p.VerifyCutoverCertificate(certificate) != nil ||
		!validRetainedPruneCursor(&cursor) || cursor.phase != RetainedPruneComplete ||
		!retainedPruneCursorMatches(p, certificate, &cursor) {
		return ErrRetainedPrune
	}
	return nil
}

// Advance confirms an applied pending batch, retries a not-yet-applied pending
// batch, or plans and checkpoints the next batch. hasBatch means the caller
// should replicate the returned deletes before calling Advance again.
func (p *RetainedPruner) Advance(
	snapshot *replicatedstate.ReadSnapshot,
	capture *SourceCapture,
	limits RetainedPruneLimits,
	persist RetainedPruneCursorPersistence,
	workspace *RetainedPruneWorkspace,
) (batch RetainedPruneBatch, hasBatch bool, err error) {
	if p == nil || snapshot == nil || capture == nil || persist == nil || workspace == nil {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	limits, err = normalizeRetainedPruneLimits(limits)
	if err != nil {
		return RetainedPruneBatch{}, false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cursor.phase == RetainedPruneComplete {
		return RetainedPruneBatch{}, false, nil
	}
	if p.cursor.phase == RetainedPruneAwaitingApply {
		switch head := capture.Head(); {
		case head < p.cursor.applied:
			return RetainedPruneBatch{}, false, ErrRetainedPrune
		case head > p.cursor.applied:
			if err := p.confirmPending(capture, persist, workspace); err != nil {
				return RetainedPruneBatch{}, false, err
			}
			return RetainedPruneBatch{}, false, nil
		default:
			if !fenceMatchesRetainedCursor(snapshot.Fence(), &p.cursor) {
				return RetainedPruneBatch{}, false, ErrRetainedPrune
			}
			planned, _, _, planErr := p.scan(snapshot, limits, workspace)
			if planErr != nil || planned.Digest != p.cursor.pending ||
				planned.Count != p.cursor.pendingCount ||
				planned.KeyBytes != p.cursor.pendingKeyBytes ||
				!bytes.Equal(workspace.resume, p.cursor.resumeAfter) {
				return RetainedPruneBatch{}, false, errors.Join(ErrRetainedPrune, planErr)
			}
			return planned, true, nil
		}
	}
	if p.cursor.phase != RetainedPruneScan ||
		!fenceMatchesRetainedCursor(snapshot.Fence(), &p.cursor) ||
		!captureMatchesRetainedCursor(capture, &p.cursor) {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	planned, scanned, eof, err := p.scan(snapshot, limits, workspace)
	if err != nil {
		return RetainedPruneBatch{}, false, err
	}
	next := p.cursor
	if !addRetainedProgress(&next.scannedRows, scanned.rows) ||
		!addRetainedProgress(&next.scannedBytes, scanned.bytes) {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	if planned.Count != 0 {
		next.phase = RetainedPruneAwaitingApply
		next.pending = planned.Digest
		next.pendingCount = planned.Count
		next.pendingKeyBytes = planned.KeyBytes
		next.resumeAfter = workspace.resume
		if err := p.persistCursor(next, persist); err != nil {
			return RetainedPruneBatch{}, false, err
		}
		return planned, true, nil
	}
	next.scanAfter = workspace.resume
	if !eof {
		if err := p.persistCursor(next, persist); err != nil {
			return RetainedPruneBatch{}, false, err
		}
		return RetainedPruneBatch{}, false, nil
	}
	rows, bytesCount, generation, digest, err := p.verifyRetained(snapshot, capture, workspace)
	if err != nil {
		return RetainedPruneBatch{}, false, err
	}
	next.phase = RetainedPruneComplete
	next.remainingRows, next.remainingBytes = rows, bytesCount
	next.snapshotGeneration, next.retainedDigest = generation, digest
	if err := p.persistCursor(next, persist); err != nil {
		return RetainedPruneBatch{}, false, err
	}
	return RetainedPruneBatch{}, false, nil
}

type retainedPruneScanned struct {
	rows  uint64
	bytes uint64
}

type retainedPruneScan struct {
	partitioner *Partitioner
	workspace   *RetainedPruneWorkspace
	limits      RetainedPruneLimits
	scanned     retainedPruneScanned
	keyBytes    uint64
	stopped     bool
}

func (p *RetainedPruner) scan(
	snapshot *replicatedstate.ReadSnapshot,
	limits RetainedPruneLimits,
	workspace *RetainedPruneWorkspace,
) (RetainedPruneBatch, retainedPruneScanned, bool, error) {
	collection, ok := snapshot.Collection(p.partitioner.collection)
	if !ok || collection == nil {
		return RetainedPruneBatch{}, retainedPruneScanned{}, false, ErrRetainedPrune
	}
	if cap(workspace.keys) < limits.MaxKeys {
		workspace.keys = make([][]byte, 0, limits.MaxKeys)
	} else {
		workspace.keys = workspace.keys[:0]
	}
	if cap(workspace.arena) < limits.MaxKeyBytes {
		workspace.arena = make([]byte, 0, limits.MaxKeyBytes)
	} else {
		workspace.arena = workspace.arena[:0]
	}
	workspace.resume = append(workspace.resume[:0], p.cursor.scanAfter...)
	workspace.scan = retainedPruneScan{
		partitioner: p.partitioner, workspace: workspace, limits: limits,
	}
	if workspace.visit == nil || workspace.bound != &workspace.scan {
		workspace.visit = workspace.scan.visitRow
		workspace.bound = &workspace.scan
	}
	buffer, scanErr := collection.RangeAfterRawBuffer(
		p.cursor.scanAfter, workspace.scanRaw, workspace.visit,
	)
	workspace.scanRaw = buffer
	scanned, keyBytes, stopped := workspace.scan.scanned,
		workspace.scan.keyBytes, workspace.scan.stopped
	workspace.scan.partitioner = nil
	workspace.scan.workspace = nil
	if scanErr != nil && !errors.Is(scanErr, errRetainedPruneScanStop) {
		return RetainedPruneBatch{}, retainedPruneScanned{}, false, scanErr
	}
	batch := RetainedPruneBatch{
		Count: uint64(len(workspace.keys)), KeyBytes: keyBytes,
		keys: workspace.keys,
	}
	if batch.Count != 0 {
		batch.Digest = p.hashPruneBatch(
			&p.cursor, workspace.resume, workspace.keys, workspace,
		)
	}
	return batch, scanned, !stopped && scanErr == nil, nil
}

func (s *retainedPruneScan) visitRow(key, document []byte) error {
	workspace := s.workspace
	if s.partitioner == nil || workspace == nil {
		return ErrRetainedPrune
	}
	if s.scanned.rows >= s.limits.MaxScanRows {
		s.stopped = true
		return errRetainedPruneScanStop
	}
	point, err := s.partitioner.program.Point(document, &workspace.document)
	if err != nil {
		return errors.Join(ErrRetainedPrune, err)
	}
	child := s.partitioner.childFor(point)
	if child < 0 {
		return ErrRetainedPrune
	}
	if child != int(s.partitioner.retained) &&
		(len(workspace.keys) >= s.limits.MaxKeys ||
			len(key) > s.limits.MaxKeyBytes-len(workspace.arena)) {
		if len(workspace.keys) == 0 {
			return ErrRetainedPrune
		}
		s.stopped = true
		return errRetainedPruneScanStop
	}
	rowBytes := uint64(len(key)) + uint64(len(document))
	if s.scanned.rows == math.MaxUint64 || s.scanned.bytes > math.MaxUint64-rowBytes {
		return ErrRetainedPrune
	}
	s.scanned.rows++
	s.scanned.bytes += rowBytes
	workspace.resume = append(workspace.resume[:0], key...)
	if child == int(s.partitioner.retained) {
		return nil
	}
	start := len(workspace.arena)
	workspace.arena = append(workspace.arena, key...)
	workspace.keys = append(workspace.keys, workspace.arena[start:len(workspace.arena)])
	s.keyBytes += uint64(len(key))
	if len(workspace.keys) == s.limits.MaxKeys ||
		len(workspace.arena) == s.limits.MaxKeyBytes {
		s.stopped = true
		return errRetainedPruneScanStop
	}
	return nil
}

func (p *RetainedPruner) confirmPending(
	capture *SourceCapture,
	persist RetainedPruneCursorPersistence,
	workspace *RetainedPruneWorkspace,
) error {
	tail := TailCursor{
		planDigest: p.cursor.plan, placementDigest: p.cursor.placement,
		dataChainDigest: p.cursor.dataChain, baseDigest: p.cursor.base,
		entryDigest: p.cursor.entry, applied: p.cursor.applied, term: p.cursor.term,
		ownershipEpoch: p.cursor.ownershipEpoch, routingVersion: p.cursor.routingVersion,
		routeGeneration: p.cursor.routeGeneration, sealed: true,
	}
	entry, ok, err := capture.NextTailEntry(tail, &workspace.capture)
	if err != nil || !ok || entry.beforeCoordinates() != tail.coordinates() ||
		entry.afterCoordinates() != tail.coordinates() ||
		uint64(len(entry.Transitions)) != p.cursor.pendingCount {
		return errors.Join(ErrRetainedPrune, err)
	}
	workspace.keys = workspace.keys[:0]
	keyBytes := uint64(0)
	for index := range entry.Transitions {
		transition := &entry.Transitions[index]
		if transition.Before == nil || transition.After != nil {
			return ErrRetainedPrune
		}
		point, pointErr := p.partitioner.program.Point(transition.Before, &workspace.document)
		if pointErr != nil || p.partitioner.childFor(point) == int(p.partitioner.retained) {
			return errors.Join(ErrRetainedPrune, pointErr)
		}
		workspace.keys = append(workspace.keys, transition.Key)
		keyBytes += uint64(len(transition.Key))
	}
	if keyBytes != p.cursor.pendingKeyBytes ||
		p.hashPruneBatch(&p.cursor, p.cursor.resumeAfter, workspace.keys, workspace) != p.cursor.pending {
		return ErrRetainedPrune
	}
	next := p.cursor
	next.phase = RetainedPruneScan
	next.applied, next.term = entry.Applied, entry.Term
	next.dataChain, next.entry = entry.AfterDataChainDigest, entry.EntryDigest
	next.scanAfter = p.cursor.resumeAfter
	next.resumeAfter = nil
	next.pending = [sha256.Size]byte{}
	next.pendingCount, next.pendingKeyBytes = 0, 0
	if !addRetainedProgress(&next.deletedRows, uint64(len(entry.Transitions))) ||
		!addRetainedProgress(&next.deletedKeyBytes, keyBytes) {
		return ErrRetainedPrune
	}
	return p.persistCursor(next, persist)
}

func (p *RetainedPruner) verifyRetained(
	snapshot *replicatedstate.ReadSnapshot,
	capture *SourceCapture,
	workspace *RetainedPruneWorkspace,
) (uint64, uint64, uint64, [sha256.Size]byte, error) {
	collection, ok := snapshot.Collection(p.partitioner.collection)
	if !ok || collection == nil || !fenceMatchesRetainedCursor(snapshot.Fence(), &p.cursor) {
		return 0, 0, 0, [sha256.Size]byte{}, ErrRetainedPrune
	}
	h := retainedPruneHasher(workspace)
	_, _ = h.Write(retainedPruneProofDomain)
	_, _ = h.Write(p.certificate.digest[:])
	var rows, bytesCount uint64
	buffer, err := collection.RangeRawBuffer(workspace.scanRaw, func(key, document []byte) error {
		point, pointErr := p.partitioner.program.Point(document, &workspace.document)
		if pointErr != nil || p.partitioner.childFor(point) != int(p.partitioner.retained) {
			return errors.Join(ErrRetainedPrune, pointErr)
		}
		rowBytes := uint64(len(key)) + uint64(len(document))
		if rows == math.MaxUint64 || bytesCount > math.MaxUint64-rowBytes {
			return ErrRetainedPrune
		}
		rows++
		bytesCount += rowBytes
		hashTailFrame(h, &workspace.size, key)
		hashTailFrame(h, &workspace.size, document)
		return nil
	})
	workspace.scanRaw = buffer
	if err != nil || !captureMatchesRetainedCursor(capture, &p.cursor) {
		return 0, 0, 0, [sha256.Size]byte{}, errors.Join(ErrRetainedPrune, err)
	}
	workspace.fixed = [40]byte{}
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], rows)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], bytesCount)
	binary.LittleEndian.PutUint64(workspace.fixed[16:24], collection.Generation())
	_, _ = h.Write(workspace.fixed[:24])
	_ = h.Sum(workspace.digest[:0])
	return rows, bytesCount, collection.Generation(), workspace.digest, nil
}

func (p *RetainedPruner) hashPruneBatch(
	cursor *RetainedPruneCursor,
	resumeAfter []byte,
	keys [][]byte,
	workspace *RetainedPruneWorkspace,
) [sha256.Size]byte {
	h := retainedPruneHasher(workspace)
	_, _ = h.Write(retainedPruneBatchDomain)
	_, _ = h.Write(cursor.plan[:])
	_, _ = h.Write(cursor.cutover[:])
	workspace.fixed = [40]byte{}
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], cursor.applied)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], uint64(len(keys)))
	_, _ = h.Write(workspace.fixed[:16])
	_, _ = h.Write(cursor.entry[:])
	hashTailFrame(h, &workspace.size, resumeAfter)
	for _, key := range keys {
		hashTailFrame(h, &workspace.size, key)
	}
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

func retainedPruneHasher(workspace *RetainedPruneWorkspace) hash.Hash {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	workspace.hasher.Reset()
	return workspace.hasher
}

func (p *RetainedPruner) persistCursor(
	next RetainedPruneCursor,
	persist RetainedPruneCursorPersistence,
) error {
	raw, err := AppendRetainedPruneCursorWithWorkspace(
		p.cursorRaw[:0], &next, &p.cursorCodec,
	)
	if err != nil {
		return err
	}
	p.cursorRaw = raw
	if err := persist(raw); err != nil {
		return errors.Join(ErrRetainedPruneOutcomeUnknown, err)
	}
	p.installCursor(next)
	return nil
}

func (p *RetainedPruner) installCursor(next RetainedPruneCursor) {
	p.scanAfter = append(p.scanAfter[:0], next.scanAfter...)
	p.resumeAfter = append(p.resumeAfter[:0], next.resumeAfter...)
	next.scanAfter = p.scanAfter
	next.resumeAfter = p.resumeAfter
	p.cursor = next
}

func normalizeRetainedPruneLimits(limits RetainedPruneLimits) (RetainedPruneLimits, error) {
	if limits.MaxKeys == 0 {
		limits.MaxKeys = DefaultRetainedPruneKeys
	}
	if limits.MaxKeyBytes == 0 {
		limits.MaxKeyBytes = DefaultRetainedPruneKeyBytes
	}
	if limits.MaxScanRows == 0 {
		limits.MaxScanRows = DefaultRetainedPruneScanRows
	}
	if limits.MaxKeys < 1 || limits.MaxKeys > replication.MaxMutations ||
		limits.MaxKeyBytes < 1 || limits.MaxKeyBytes > replication.MaxCommandBytes ||
		limits.MaxScanRows == 0 {
		return RetainedPruneLimits{}, ErrRetainedPrune
	}
	return limits, nil
}

func fenceMatchesRetainedCursor(
	fence replicatedstate.SnapshotFence,
	cursor *RetainedPruneCursor,
) bool {
	return cursor != nil && fence.Applied == cursor.applied && fence.LastTerm == cursor.term &&
		fence.DataChainDigest == cursor.dataChain && fence.SnapshotBaseDigest == cursor.base &&
		fence.LastEntryDigest == cursor.entry &&
		fence.Binding.OwnershipEpoch == cursor.ownershipEpoch &&
		fence.Binding.RoutingVersion == cursor.routingVersion &&
		fence.Binding.RouteGeneration == cursor.routeGeneration
}

func captureMatchesRetainedCursor(capture *SourceCapture, cursor *RetainedPruneCursor) bool {
	if capture == nil || cursor == nil {
		return false
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return capture.begun.Load() && capture.pending == 0 &&
		capture.head.Load() == cursor.applied &&
		capture.current == sourceCapturePublication{
			applied: cursor.applied, term: cursor.term,
			ownershipEpoch: cursor.ownershipEpoch, routingVersion: cursor.routingVersion,
			routeGeneration: cursor.routeGeneration,
			entryDigest:     cursor.entry, dataChainDigest: cursor.dataChain,
		}
}

func retainedPruneCursorMatches(
	partitioner *Partitioner,
	certificate CutoverCertificate,
	cursor *RetainedPruneCursor,
) bool {
	cut := certificate.SourceCut()
	coordinates := certificate.SourceCoordinates()
	return cursor != nil && cursor.retained == partitioner.retained &&
		cursor.plan == certificate.plan && cursor.placement == certificate.placement &&
		cursor.cutover == certificate.digest && cursor.base == cut.BaseDigest &&
		cursor.applied >= cut.Applied && cursor.ownershipEpoch == coordinates.OwnershipEpoch &&
		cursor.routingVersion == coordinates.RoutingVersion &&
		cursor.routeGeneration == coordinates.RouteGeneration
}

func addRetainedProgress(target *uint64, value uint64) bool {
	if *target > math.MaxUint64-value {
		return false
	}
	*target += value
	return true
}
