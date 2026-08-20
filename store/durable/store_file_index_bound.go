package durable

import (
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// IndexProbeMemoryBound is the non-allocating geometry a query planner needs
// to admit durable exact-index work before growing either its mask buffers or
// an [IndexWorkspace].
//
// CatalogBytes bounds the retained []store.IndexInfo backing array copied by
// [Snapshot.AppendIndexes]. The Name and Columns string payloads remain owned
// by the snapshot's immutable collection catalog; AppendIndexes borrows those
// strings rather than copying their bytes into query workspace. MaskCount is
// the maximum number of stable-slot masks one probe can return. The workspace
// fields bound storage retained by one candidate, ordered range, exact
// single-column, or exact compound probe respectively; query-owned output masks
// are separate because a boolean predicate may retain several independent mask
// buffers at once.
type IndexProbeMemoryBound struct {
	CatalogBytes                int64
	MaskCount                   uint64
	CandidateWorkspaceBytes     int64
	RangeWorkspaceBytes         int64
	ExactSingleWorkspaceBytes   int64
	ExactCompoundWorkspaceBytes int64
}

// IndexProbeMemoryBound returns a conservative retained-capacity bound without
// reading an index page or growing caller storage. It uses this snapshot's
// immutable catalog geometry, not observed selectivity: a tuple hash may
// legally occur in every live chunk, and every copied directory entry may carry
// a distinct maximum certificate.
func (s *Snapshot) IndexProbeMemoryBound() (IndexProbeMemoryBound, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return IndexProbeMemoryBound{}, ErrClosed
	}
	state := s.state
	bound := IndexProbeMemoryBound{
		CatalogBytes: retainedIndexSliceBytes(
			uint64(len(s.indexDefinitions)),
			uint64(unsafe.Sizeof(store.IndexInfo{})),
		),
	}

	// Ordered-primary postings are resident and append directly to the
	// caller's masks. They do not populate IndexWorkspace's directory,
	// certificate, posting-decision, document, or tape buffers.
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		// Each iterator result is one live posting tile. The epoch bound is
		// the fold base's occupied tiles plus every tile the overlay window
		// could have added — conservative, which is this function's contract.
		bound.MaskCount = s.epoch.liveTileBound()
		maskBytes := retainedIndexSliceBytes(
			bound.MaskCount, uint64(unsafe.Sizeof(store.Mask{})),
		)
		termBytes := retainedIndexSliceBytes(
			storeio.IndexTermMaxKeyBytes+1, 1,
		)
		bound.RangeWorkspaceBytes = indexBoundSum(
			indexBoundProduct(3, uint64(maskBytes)),
			indexBoundProduct(3, uint64(termBytes)),
		)
		return bound, nil
	}

	// An ordered primary without a secondary exact index carries no posting
	// live map, so no exact probe can run and the reusable exact-index workspaces
	// remain empty. The primary graph still exposes its live stable-slot tiles to the
	// executor's full scan, and MaskCount must bound the query-owned masks that
	// scan appends: one per live tile. The ordered graph does not maintain the
	// document, so DocumentCount is the conservative resident upper bound on the
	// live-tile count without reading a page.
	bound.MaskCount = state.root.DocumentCount
	return bound, nil
}

// retainedIndexSliceBytes covers geometric slice growth and allocator rounding.
// The fixed slack handles the small-slice classes where cap can exceed twice
// the logical byte count (notably []byte); larger slices remain below the
// doubled logical width.
func retainedIndexSliceBytes(count, width uint64) int64 {
	if count == 0 || width == 0 {
		return 0
	}
	return retainedIndexLogicalBytes(indexBoundProduct(count, width))
}

func retainedIndexLogicalBytes(bytes int64) int64 {
	if bytes <= 0 {
		return 0
	}
	bytes = indexBoundSum(bytes, bytes)
	return indexBoundSum(bytes, 64)
}

func indexBoundProduct(a, b uint64) int64 {
	if a == 0 || b == 0 {
		return 0
	}
	if a > uint64(math.MaxInt64)/b {
		return math.MaxInt64
	}
	return int64(a * b)
}

func indexBoundSum(a, b int64) int64 {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return math.MaxInt64
	}
	return a + b
}
