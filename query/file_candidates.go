package query

import (
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// fileCandidateMasks and fileExactCandidateMasks are the durable entry
// points into the shared generic planner (candidates_mask.go). They stay
// concretely typed on *durable.Snapshot/*durable.IndexWorkspace, wrapping
// them in a durable.QuerySnapshot — durable's probes carry extra I/O-
// workspace and stats-accumulator parameters that store.IndexSource's plain
// method set can't express, so this adapter (not the plan-level function
// signatures) is where that gap is bridged.

func (p *plan) fileCandidateMasks(snapshot *durable.Snapshot, index *durable.IndexWorkspace, w *Workspace) ([]store.Mask, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, err
	}
	// Durable has one of the two optional capabilities. Its chunks carry
	// per-chunk summaries in the chunk-directory leaves
	// (store/durable/store_file_zone.go), so the block-pruning tier is offered.
	// A live-row universe is not: materializing it would mean reading every
	// document page, which is real I/O rather than the metadata read a NOT
	// needs it to be, so durable still declines NOT.
	//
	// The summary capability is stated as the bare *durable.Snapshot rather
	// than the QuerySnapshot below it, and that is load-bearing rather than
	// tidy. sourceCaps.zone is an interface field, so whatever goes in it is
	// converted; a single pointer converts in place, while the five-pointer
	// QuerySnapshot has to be copied to the heap to be boxed — the same
	// 48-byte allocation per execution that removing the `any(snapshot)`
	// assertion from the generic planner was meant to eliminate. Probing a
	// summary needs neither the I/O workspace nor the stats accumulators
	// QuerySnapshot exists to carry, so nothing is lost by handing over the
	// narrower type. See sourceCaps.
	masks, _, err := snapshotCandidateMasks(
		p, durable.QuerySnapshot{Snapshot: snapshot, Workspace: index},
		sourceCaps{zone: snapshot}, w, false)
	if err == nil {
		err = w.checkCanceled()
	}
	return masks, err
}

// fileCandidateMasksBounded admits the complete durable planning high-water
// before AppendIndexes, a mask append, or an IndexWorkspace probe can grow.
// Declining returns the ordinary nil/full-scan plan rather than a resource
// error: an index is an optimization, and the batched scan remains exact.
func (p *plan) fileCandidateMasksBounded(
	snapshot *durable.Snapshot,
	index *durable.IndexWorkspace,
	w *Workspace,
	memoryBytes int64,
) ([]store.Mask, int64, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, 0, err
	}
	if p.where == nil {
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
	planned := p.fileIndexPlanMemoryBytes(
		bound, bound.CandidateWorkspaceBytes,
	)
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
	index *durable.IndexWorkspace,
	w *Workspace,
	memoryBytes int64,
) ([]store.Mask, uint64, uint64, int, bool, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, 0, 0, 0, true, err
	}
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
	return p.fileExactCandidateMasksLoaded(snapshot, index, w)
}

func (p *plan) fileExactCandidateMasksLoaded(
	snapshot *durable.Snapshot,
	index *durable.IndexWorkspace,
	w *Workspace,
) ([]store.Mask, uint64, uint64, int, bool, error) {
	if err := w.checkCanceled(); err != nil {
		return nil, 0, 0, 0, true, err
	}
	var rechecks, certificates uint64
	var postingPages int
	qs := durable.QuerySnapshot{
		Snapshot: snapshot, Workspace: index,
		Rechecks: &rechecks, Certificates: &certificates, PostingPages: &postingPages,
	}
	// requireExact rules the chunk-summary tier out anyway (a zone mask is a
	// superset, and this lane consumes masks as final answers), and durable has
	// no live-mask source either, so no capabilities are offered here at all.
	masks, bounded, exact, err := candidatesFor(p.where, qs, sourceCaps{}, p.valuePaths, w.storeIndexes, w, true)
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
