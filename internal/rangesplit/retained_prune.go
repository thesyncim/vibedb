package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"
	"reflect"
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
	// MaxRetainedPruneScanRows is the hard per-advance work ceiling. Callers
	// may lower it, but cannot turn one controller step into an unbounded scan.
	MaxRetainedPruneScanRows = 1 << 20
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
// Concurrent published writes are advanced one captured entry per call and are
// accepted only when every changed document remains inside the retained child.
type RetainedPruner struct {
	mu sync.Mutex

	partitioner *Partitioner
	certificate CutoverCertificate
	cursor      RetainedPruneCursor
	scanAfter   []byte
	resumeAfter []byte
	pendingRaw  []byte
	cursorRaw   []byte
	cursorCodec RetainedPruneCursorWorkspace
}

type RetainedPruneWorkspace struct {
	document   distribution.DocumentPointWorkspace
	capture    SourceCaptureWorkspace
	keys       [][]byte
	arena      []byte
	resume     []byte
	pendingRaw []byte
	scanRaw    []byte
	hasher     hash.Hash
	digest     [sha256.Size]byte
	size       [8]byte
	fixed      [40]byte
	scan       retainedPruneScan
	visit      func(key, document []byte) error
	bound      *retainedPruneScan
}

func NewRetainedPruner(
	partitioner *Partitioner,
	certificate CutoverCertificate,
	authority any,
	persistedCursor []byte,
) (*RetainedPruner, error) {
	view, ok := sealedRetainedPruneAuthority(authority)
	if !ok || partitioner == nil || partitioner.ValidateRetainedPruneAuthority(
		view.Manifest(), view.Generation(), certificate,
	) != nil || view.Operation() == ([sha256.Size]byte{}) ||
		view.Certificate() != certificate.Digest() {
		return nil, ErrRetainedPrune
	}
	return newRetainedPruner(partitioner, certificate, view.Operation(), persistedCursor)
}

type retainedPruneAuthorityView interface {
	Manifest() *distribution.Manifest
	Generation() uint64
	Operation() [sha256.Size]byte
	Certificate() [sha256.Size]byte
}

func sealedRetainedPruneAuthority(authority any) (retainedPruneAuthorityView, bool) {
	view, ok := authority.(retainedPruneAuthorityView)
	if !ok || view == nil {
		return nil, false
	}
	typeOf := reflect.TypeOf(authority)
	return view, typeOf.Kind() == reflect.Struct &&
		typeOf.PkgPath() == "github.com/thesyncim/vibedb/gateway" &&
		typeOf.Name() == "retainedPruneAuthority"
}

func newRetainedPruner(
	partitioner *Partitioner,
	certificate CutoverCertificate,
	operation [sha256.Size]byte,
	persistedCursor []byte,
) (*RetainedPruner, error) {
	if partitioner == nil || operation == ([sha256.Size]byte{}) {
		return nil, ErrRetainedPrune
	}
	pruner := &RetainedPruner{partitioner: partitioner, certificate: certificate}
	if len(persistedCursor) == 0 {
		cut := certificate.SourceCut()
		coordinates := certificate.SourceCoordinates()
		pruner.cursor = RetainedPruneCursor{
			phase: RetainedPruneScan, retained: partitioner.retained, operation: operation,
			applied: cut.Applied, term: cut.Term, ownershipEpoch: coordinates.OwnershipEpoch,
			routingVersion: coordinates.RoutingVersion, routeGeneration: coordinates.RouteGeneration,
			plan: certificate.plan, placement: certificate.placement, cutover: certificate.digest,
			dataChain: cut.DataChainDigest, base: cut.BaseDigest, entry: cut.EntryDigest,
		}
		return pruner, nil
	}
	cursor, err := OpenRetainedPruneCursor(persistedCursor)
	if err != nil || cursor.operation != operation || !retainedPruneCursorMatches(partitioner, certificate, cursor) {
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
	result.pendingKeys = bytes.Clone(p.cursor.pendingKeys)
	return result
}

// VerifyRetainedPruneCompletion proves that cursor is the terminal retained
// image produced from certificate by this exact already-published split plan.
func (p *Partitioner) VerifyRetainedPruneCompletion(
	certificate CutoverCertificate,
	operation [sha256.Size]byte,
	cursor RetainedPruneCursor,
) error {
	if p == nil || p.VerifyCutoverCertificate(certificate) != nil ||
		operation == ([sha256.Size]byte{}) || cursor.operation != operation ||
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
	head := capture.Head()
	if head < p.cursor.applied {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	if p.cursor.phase == RetainedPruneComplete {
		if head == p.cursor.applied {
			return RetainedPruneBatch{}, false, nil
		}
		if err := p.advanceCompletedRetainedEntry(snapshot, capture, persist, workspace); err != nil {
			return RetainedPruneBatch{}, false, err
		}
		return RetainedPruneBatch{}, false, nil
	}
	if head > p.cursor.applied {
		if p.cursor.phase == RetainedPruneAwaitingApply {
			if err := p.confirmOrAdvancePending(capture, persist, workspace); err != nil {
				return RetainedPruneBatch{}, false, err
			}
			return RetainedPruneBatch{}, false, nil
		}
		if p.cursor.phase != RetainedPruneScan && p.cursor.phase != RetainedPruneVerify {
			return RetainedPruneBatch{}, false, ErrRetainedPrune
		}
		if err := p.advanceRetainedEntry(capture, persist, workspace); err != nil {
			return RetainedPruneBatch{}, false, err
		}
		return RetainedPruneBatch{}, false, nil
	}
	if p.cursor.phase == RetainedPruneVerify {
		return p.advanceRetainedVerification(snapshot, capture, limits, persist, workspace)
	}
	if p.cursor.phase == RetainedPruneAwaitingApply {
		if !fenceMatchesRetainedCursor(snapshot.Fence(), &p.cursor) {
			return RetainedPruneBatch{}, false, ErrRetainedPrune
		}
		planned, planErr := p.openPendingBatch(workspace)
		if planErr != nil || planned.Digest != p.cursor.pending ||
			planned.Count != p.cursor.pendingCount ||
			planned.KeyBytes != p.cursor.pendingKeyBytes {
			return RetainedPruneBatch{}, false, errors.Join(ErrRetainedPrune, planErr)
		}
		return planned, true, nil
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
		next.pendingApplied = p.cursor.applied
		next.pendingEntry = p.cursor.entry
		next.resumeAfter = workspace.resume
		next.pendingKeys = appendPendingKeys(workspace.pendingRaw[:0], workspace.keys)
		if len(next.pendingKeys) > replication.MaxCommandBytes {
			return RetainedPruneBatch{}, false, ErrRetainedPrune
		}
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
	collection, ok := snapshot.Collection(p.partitioner.collection)
	if !ok || collection == nil || !captureMatchesRetainedCursor(capture, &p.cursor) {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	next.phase = RetainedPruneVerify
	next.scanAfter = nil
	next.snapshotGeneration = collection.Generation()
	h := retainedPruneHasher(workspace)
	_, _ = h.Write(retainedPruneProofDomain)
	_, _ = h.Write(p.certificate.digest[:])
	_ = h.Sum(next.retainedDigest[:0])
	if err := p.persistCursor(next, persist); err != nil {
		return RetainedPruneBatch{}, false, err
	}
	return RetainedPruneBatch{}, false, nil
}

func (p *RetainedPruner) advanceRetainedVerification(
	snapshot *replicatedstate.ReadSnapshot,
	capture *SourceCapture,
	limits RetainedPruneLimits,
	persist RetainedPruneCursorPersistence,
	workspace *RetainedPruneWorkspace,
) (RetainedPruneBatch, bool, error) {
	collection, ok := snapshot.Collection(p.partitioner.collection)
	if !ok || collection == nil || !fenceMatchesRetainedCursor(snapshot.Fence(), &p.cursor) ||
		!captureMatchesRetainedCursor(capture, &p.cursor) {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	h := retainedPruneHasher(workspace)
	_, _ = h.Write(retainedPruneProofDomain)
	_, _ = h.Write(p.cursor.retainedDigest[:])
	workspace.resume = append(workspace.resume[:0], p.cursor.scanAfter...)
	var rows, byteCount uint64
	stopped := false
	buffer, scanErr := collection.RangeAfterRawBuffer(
		p.cursor.scanAfter, workspace.scanRaw, func(key, document []byte) error {
			if rows >= limits.MaxScanRows {
				stopped = true
				return errRetainedPruneScanStop
			}
			point, pointErr := p.partitioner.program.Point(document, &workspace.document)
			if pointErr != nil || p.partitioner.childFor(point) != int(p.partitioner.retained) {
				return errors.Join(ErrRetainedPrune, pointErr)
			}
			rowBytes := uint64(len(key)) + uint64(len(document))
			if rows == math.MaxUint64 || byteCount > math.MaxUint64-rowBytes {
				return ErrRetainedPrune
			}
			rows++
			byteCount += rowBytes
			workspace.resume = append(workspace.resume[:0], key...)
			hashTailFrame(h, &workspace.size, key)
			hashTailFrame(h, &workspace.size, document)
			return nil
		},
	)
	workspace.scanRaw = buffer
	if scanErr != nil && !errors.Is(scanErr, errRetainedPruneScanStop) {
		return RetainedPruneBatch{}, false, scanErr
	}
	next := p.cursor
	if !addRetainedProgress(&next.remainingRows, rows) ||
		!addRetainedProgress(&next.remainingBytes, byteCount) {
		return RetainedPruneBatch{}, false, ErrRetainedPrune
	}
	next.scanAfter = workspace.resume
	next.snapshotGeneration = collection.Generation()
	_ = h.Sum(next.retainedDigest[:0])
	if !stopped && scanErr == nil {
		next.phase = RetainedPruneComplete
	}
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

func (p *RetainedPruner) confirmOrAdvancePending(
	capture *SourceCapture,
	persist RetainedPruneCursorPersistence,
	workspace *RetainedPruneWorkspace,
) error {
	entry, ok, err := p.nextCapturedEntry(capture, workspace)
	if err != nil || !ok {
		return errors.Join(ErrRetainedPrune, err)
	}
	workspace.keys = workspace.keys[:0]
	keyBytes := uint64(0)
	pruneEntry := uint64(len(entry.Transitions)) == p.cursor.pendingCount
	if pruneEntry {
		for index := range entry.Transitions {
			transition := &entry.Transitions[index]
			if transition.Before == nil || transition.After != nil {
				pruneEntry = false
				break
			}
			point, pointErr := p.partitioner.program.Point(transition.Before, &workspace.document)
			if pointErr != nil {
				return errors.Join(ErrRetainedPrune, pointErr)
			}
			if p.partitioner.childFor(point) == int(p.partitioner.retained) {
				pruneEntry = false
				break
			}
			workspace.keys = append(workspace.keys, transition.Key)
			keyBytes += uint64(len(transition.Key))
		}
		if pruneEntry && (keyBytes != p.cursor.pendingKeyBytes ||
			p.hashPruneBatchAt(
				p.cursor.pendingApplied, p.cursor.pendingEntry,
				p.cursor.resumeAfter, workspace.keys, workspace,
			) != p.cursor.pending) {
			pruneEntry = false
		}
	}
	if !pruneEntry {
		if !p.retainedEntry(entry, workspace) {
			return ErrRetainedPrune
		}
		next := p.cursor
		advanceRetainedCursorPublication(&next, entry)
		return p.persistCursor(next, persist)
	}
	next := p.cursor
	next.phase = RetainedPruneScan
	advanceRetainedCursorPublication(&next, entry)
	next.scanAfter = p.cursor.resumeAfter
	next.resumeAfter = nil
	next.pendingKeys = nil
	next.pending = [sha256.Size]byte{}
	next.pendingEntry = [sha256.Size]byte{}
	next.pendingApplied = 0
	next.pendingCount, next.pendingKeyBytes = 0, 0
	if !addRetainedProgress(&next.deletedRows, uint64(len(entry.Transitions))) ||
		!addRetainedProgress(&next.deletedKeyBytes, keyBytes) {
		return ErrRetainedPrune
	}
	return p.persistCursor(next, persist)
}

func appendPendingKeys(dst []byte, keys [][]byte) []byte {
	var size [4]byte
	for _, key := range keys {
		binary.LittleEndian.PutUint32(size[:], uint32(len(key)))
		dst = append(dst, size[:]...)
		dst = append(dst, key...)
	}
	return dst
}

func (p *RetainedPruner) openPendingBatch(workspace *RetainedPruneWorkspace) (RetainedPruneBatch, error) {
	workspace.keys = workspace.keys[:0]
	raw := p.cursor.pendingKeys
	var keyBytes uint64
	for len(raw) != 0 {
		if len(raw) < 4 {
			return RetainedPruneBatch{}, ErrRetainedPrune
		}
		size := int(binary.LittleEndian.Uint32(raw[:4]))
		raw = raw[4:]
		if size == 0 || size > len(raw) {
			return RetainedPruneBatch{}, ErrRetainedPrune
		}
		workspace.keys = append(workspace.keys, raw[:size])
		keyBytes += uint64(size)
		raw = raw[size:]
	}
	batch := RetainedPruneBatch{Count: uint64(len(workspace.keys)), KeyBytes: keyBytes, keys: workspace.keys}
	batch.Digest = p.hashPruneBatchAt(
		p.cursor.pendingApplied, p.cursor.pendingEntry,
		p.cursor.resumeAfter, workspace.keys, workspace,
	)
	return batch, nil
}

func (p *RetainedPruner) advanceRetainedEntry(
	capture *SourceCapture,
	persist RetainedPruneCursorPersistence,
	workspace *RetainedPruneWorkspace,
) error {
	entry, ok, err := p.nextCapturedEntry(capture, workspace)
	if err != nil || !ok || !p.retainedEntry(entry, workspace) {
		return errors.Join(ErrRetainedPrune, err)
	}
	next := p.cursor
	advanceRetainedCursorPublication(&next, entry)
	return p.persistCursor(next, persist)
}

func (p *RetainedPruner) advanceCompletedRetainedEntry(
	snapshot *replicatedstate.ReadSnapshot,
	capture *SourceCapture,
	persist RetainedPruneCursorPersistence,
	workspace *RetainedPruneWorkspace,
) error {
	entry, ok, err := p.nextCapturedEntry(capture, workspace)
	collection, collectionOK := snapshot.Collection(p.partitioner.collection)
	if err != nil || !ok || !collectionOK || collection == nil ||
		!p.retainedEntry(entry, workspace) {
		return errors.Join(ErrRetainedPrune, err)
	}
	next := p.cursor
	advanceRetainedCursorPublication(&next, entry)
	if !advanceCompletedRetainedCounts(&next, entry) {
		return ErrRetainedPrune
	}
	next.snapshotGeneration = collection.Generation()
	h := retainedPruneHasher(workspace)
	_, _ = h.Write(retainedPruneProofDomain)
	_, _ = h.Write(p.cursor.retainedDigest[:])
	_, _ = h.Write(entry.EntryDigest[:])
	_, _ = h.Write(entry.AfterDataChainDigest[:])
	workspace.fixed = [40]byte{}
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], entry.Applied)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], entry.Term)
	_, _ = h.Write(workspace.fixed[:16])
	_ = h.Sum(next.retainedDigest[:0])
	return p.persistCursor(next, persist)
}

func advanceCompletedRetainedCounts(next *RetainedPruneCursor, entry TailEntry) bool {
	for index := range entry.Transitions {
		transition := &entry.Transitions[index]
		beforeBytes, afterBytes := uint64(0), uint64(0)
		if transition.Before != nil {
			beforeBytes = uint64(len(transition.Key) + len(transition.Before))
		}
		if transition.After != nil {
			afterBytes = uint64(len(transition.Key) + len(transition.After))
		}
		switch {
		case transition.Before == nil:
			if next.remainingRows == math.MaxUint64 || next.remainingBytes > math.MaxUint64-afterBytes {
				return false
			}
			next.remainingRows++
			next.remainingBytes += afterBytes
		case transition.After == nil:
			if next.remainingRows == 0 || next.remainingBytes < beforeBytes {
				return false
			}
			next.remainingRows--
			next.remainingBytes -= beforeBytes
		case afterBytes >= beforeBytes:
			delta := afterBytes - beforeBytes
			if next.remainingBytes > math.MaxUint64-delta {
				return false
			}
			next.remainingBytes += delta
		default:
			delta := beforeBytes - afterBytes
			if next.remainingBytes < delta {
				return false
			}
			next.remainingBytes -= delta
		}
	}
	return true
}

func (p *RetainedPruner) nextCapturedEntry(
	capture *SourceCapture,
	workspace *RetainedPruneWorkspace,
) (TailEntry, bool, error) {
	tail := TailCursor{
		planDigest: p.cursor.plan, placementDigest: p.cursor.placement,
		dataChainDigest: p.cursor.dataChain, baseDigest: p.cursor.base,
		entryDigest: p.cursor.entry, applied: p.cursor.applied, term: p.cursor.term,
		ownershipEpoch: p.cursor.ownershipEpoch, routingVersion: p.cursor.routingVersion,
		routeGeneration: p.cursor.routeGeneration, sealed: true,
	}
	entry, ok, err := capture.NextTailEntry(tail, &workspace.capture)
	if err != nil || !ok || entry.beforeCoordinates() != tail.coordinates() ||
		entry.afterCoordinates() != tail.coordinates() {
		return TailEntry{}, false, errors.Join(ErrRetainedPrune, err)
	}
	return entry, true, nil
}

func (p *RetainedPruner) retainedEntry(
	entry TailEntry,
	workspace *RetainedPruneWorkspace,
) bool {
	for index := range entry.Transitions {
		transition := &entry.Transitions[index]
		if transition.Before == nil && transition.After == nil {
			return false
		}
		if !p.retainedDocument(transition.Before, workspace) ||
			!p.retainedDocument(transition.After, workspace) {
			return false
		}
	}
	return true
}

func (p *RetainedPruner) retainedDocument(
	document []byte,
	workspace *RetainedPruneWorkspace,
) bool {
	if document == nil {
		return true
	}
	point, err := p.partitioner.program.Point(document, &workspace.document)
	return err == nil && p.partitioner.childFor(point) == int(p.partitioner.retained)
}

func advanceRetainedCursorPublication(next *RetainedPruneCursor, entry TailEntry) {
	next.applied, next.term = entry.Applied, entry.Term
	next.dataChain, next.entry = entry.AfterDataChainDigest, entry.EntryDigest
}

func (p *RetainedPruner) hashPruneBatch(
	cursor *RetainedPruneCursor,
	resumeAfter []byte,
	keys [][]byte,
	workspace *RetainedPruneWorkspace,
) [sha256.Size]byte {
	return p.hashPruneBatchAt(
		cursor.applied, cursor.entry, resumeAfter, keys, workspace,
	)
}

func (p *RetainedPruner) hashPruneBatchAt(
	applied uint64,
	entry [sha256.Size]byte,
	resumeAfter []byte,
	keys [][]byte,
	workspace *RetainedPruneWorkspace,
) [sha256.Size]byte {
	h := retainedPruneHasher(workspace)
	_, _ = h.Write(retainedPruneBatchDomain)
	_, _ = h.Write(p.cursor.plan[:])
	_, _ = h.Write(p.cursor.cutover[:])
	workspace.fixed = [40]byte{}
	binary.LittleEndian.PutUint64(workspace.fixed[0:8], applied)
	binary.LittleEndian.PutUint64(workspace.fixed[8:16], uint64(len(keys)))
	_, _ = h.Write(workspace.fixed[:16])
	_, _ = h.Write(entry[:])
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
	p.pendingRaw = append(p.pendingRaw[:0], next.pendingKeys...)
	next.scanAfter = p.scanAfter
	next.resumeAfter = p.resumeAfter
	next.pendingKeys = p.pendingRaw
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
		limits.MaxKeyBytes > replication.MaxCommandBytes-4*limits.MaxKeys ||
		limits.MaxScanRows == 0 || limits.MaxScanRows > MaxRetainedPruneScanRows {
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
