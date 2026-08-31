package query

import (
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// fileCandidateMasks and fileExactCandidateMasks are the durable entry
// points into the shared generic planner (candidates_mask.go). They stay
// concretely typed on *durable.Snapshot/*durable.IndexSession. The session
// owns reusable probe scratch and exposes only detached metrics, so query
// retains capacity without wiring workspace or counter pointers through the
// public API.

func (p *plan) fileCandidateMasks(snapshot *durable.Snapshot, index *durable.IndexSession, w *Workspace) ([]store.Mask, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, err
	}
	index.Reset(snapshot)
	// Durable offers ordered scalar range probes through its persisted exact
	// term leaves. It still offers no cheap live-row universe: constructing one
	// would read every primary page, so durable declines NOT rather than hiding
	// a full scan behind a metadata capability.
	masks, _, err := snapshotCandidateMasks(
		p, index, sourceCaps{ranges: index}, w, false,
	)
	if err == nil {
		err = w.checkCanceled()
	}
	return masks, err
}

// fileCandidateMasksBounded admits the complete durable planning high-water
// before AppendIndexes, a mask append, or an IndexSession probe can grow.
// Declining returns the ordinary nil/full-scan plan rather than a resource
// error: an index is an optimization, and the batched scan remains exact.
func (p *plan) fileCandidateMasksBounded(
	snapshot *durable.Snapshot,
	index *durable.IndexSession,
	w *Workspace,
	memoryBytes int64,
) ([]store.Mask, int64, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, 0, err
	}
	if p.where == nil || p.requiresSQLDomainScan() {
		return nil, 0, nil
	}
	w.storeMaskUsed = 0
	w.storeIndexProbes = 0
	bound, err := snapshot.IndexProbeMemoryBound()
	if err != nil {
		return nil, 0, err
	}
	if err := w.checkCanceled(); err != nil {
		return nil, 0, err
	}
	workspaceBytes := bound.CandidateWorkspaceBytes
	if p.where.hasRangeComparison() {
		workspaceBytes = max(workspaceBytes, bound.RangeWorkspaceBytes)
	}
	planned := p.fileIndexPlanMemoryBytes(bound, workspaceBytes)
	if memoryBytes < 0 || planned > memoryBytes {
		return nil, 0, nil
	}
	masks, err := p.fileCandidateMasks(snapshot, index, w)
	return masks, planned, err
}

// fileExactCandidateMasksBounded is the direct-answer admission lane. Catalog
// capacity is admitted before the catalog is copied; after that no-I/O catalog
// proof identifies whether any selected probe is compound, so the much larger
// document-tape bound is charged only when a compound verifier can actually
// run. A refusal is a cost-based decline, never a query error.
func (p *plan) fileExactCandidateMasksBounded(
	snapshot *durable.Snapshot,
	index *durable.IndexSession,
	w *Workspace,
	memoryBytes int64,
) ([]store.Mask, uint64, uint64, int, bool, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, 0, 0, 0, true, err
	}
	if p.requiresSQLDomainScan() {
		return nil, 0, 0, 0, false, nil
	}
	index.Reset(snapshot)
	if p.where == nil {
		return nil, 0, 0, 0, false, nil
	}
	w.storeMaskUsed = 0
	w.storeIndexProbes = 0
	bound, err := snapshot.IndexProbeMemoryBound()
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	if err := w.checkCanceled(); err != nil {
		return nil, 0, 0, 0, true, err
	}
	if memoryBytes < 0 || bound.CatalogBytes > memoryBytes {
		return nil, 0, 0, 0, false, nil
	}
	w.storeIndexes = snapshot.AppendIndexes(w.storeIndexes[:0])
	if err := w.checkCanceled(); err != nil {
		return nil, 0, 0, 0, true, err
	}
	if !p.where.canAnswerExactly(p.valuePaths, w.storeIndexes, w) {
		return nil, 0, 0, 0, false, nil
	}
	columns := p.where.maxExactProbeColumns(
		p.valuePaths, w.storeIndexes, w,
	)
	workspaceBytes := int64(0)
	switch {
	case columns > 1:
		workspaceBytes = bound.ExactCompoundWorkspaceBytes
	case columns == 1:
		workspaceBytes = bound.ExactSingleWorkspaceBytes
	}
	if p.fileIndexPlanMemoryBytes(bound, workspaceBytes) > memoryBytes {
		return nil, 0, 0, 0, false, nil
	}
	return p.fileExactCandidateMasksLoaded(index, w)
}

func (p *plan) fileExactCandidateMasksLoaded(
	index *durable.IndexSession,
	w *Workspace,
) ([]store.Mask, uint64, uint64, int, bool, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, 0, 0, 0, true, err
	}
	// This lane consumes masks as final answers, and durable has no live-mask
	// source, so no capabilities are offered here at all.
	masks, bounded, exact, err := candidatesFor(p.where, index, sourceCaps{}, p.valuePaths, w.storeIndexes, w, true)
	metrics := index.Metrics()
	rechecks := metrics.DocumentRecheckRows
	certificates := metrics.CertificateRows
	postingPages := int(min(metrics.PostingPages, uint64(^uint(0)>>1)))
	if err != nil {
		return nil, rechecks, certificates, postingPages, true, err
	}
	if err := w.checkCanceled(); err != nil {
		return nil, rechecks, certificates, postingPages, true, err
	}
	if !bounded || !exact {
		return nil, rechecks, certificates, postingPages, false, nil
	}
	if masks == nil {
		masks = w.emptyStoreMask[:0]
	}
	return masks, rechecks, certificates, postingPages, true, nil
}

// fileIndexPlanMemoryBytes combines durable's one-probe workspace bound with
// the independent mask buffers a boolean predicate can retain. Each slice is
// charged at twice its logical width plus small-slice allocator slack, so the
// decision is made against capacity growth rather than merely len.
func (p *plan) fileIndexPlanMemoryBytes(
	bound durable.IndexProbeMemoryBound,
	workspaceBytes int64,
) int64 {
	buffers := heapPredicateBufferUpperBound(p.where)
	if buffers <= 0 {
		return 0
	}
	maskBuffer := filePlannerSliceBytes(
		bound.MaskCount, uint64(unsafe.Sizeof(store.Mask{})),
	)
	masks := saturatedProduct(buffers, maskBuffer)
	headers := filePlannerSliceBytes(
		uint64(buffers), uint64(unsafe.Sizeof([]store.Mask{})),
	)
	return saturatedBytes(
		bound.CatalogBytes,
		saturatedBytes(workspaceBytes, saturatedBytes(masks, headers)),
	)
}

func filePlannerSliceBytes(count, width uint64) int64 {
	if count == 0 || width == 0 {
		return 0
	}
	if count > uint64(math.MaxInt64)/width {
		return math.MaxInt64
	}
	bytes := int64(count * width)
	bytes = saturatedBytes(bytes, bytes)
	return saturatedBytes(bytes, 64)
}
