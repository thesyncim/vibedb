package durable

import (
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
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
// fields bound the storage retained by one candidate, exact single-column, or
// exact compound probe respectively; query-owned output masks are intentionally
// separate because a boolean predicate may retain several independent mask
// buffers at once.
type IndexProbeMemoryBound struct {
	CatalogBytes                int64
	MaskCount                   uint64
	CandidateWorkspaceBytes     int64
	ExactSingleWorkspaceBytes   int64
	ExactCompoundWorkspaceBytes int64
}

// IndexProbeMemoryBound returns a conservative retained-capacity bound without
// reading an index page or growing caller storage. It uses the frozen catalog
// geometry, not observed selectivity: a tuple hash may legally occur in every
// live chunk, and every copied directory entry may carry a distinct maximum
// certificate.
func (s *Snapshot) IndexProbeMemoryBound() (IndexProbeMemoryBound, error) {
	if s == nil || s.collection == nil || s.state == nil {
		return IndexProbeMemoryBound{}, ErrClosed
	}
	state := s.state
	bound := IndexProbeMemoryBound{
		CatalogBytes: retainedIndexSliceBytes(
			uint64(len(s.collection.options.Indexes)),
			uint64(unsafe.Sizeof(store.IndexInfo{})),
		),
	}

	// Ordered-primary postings are resident and append directly to the
	// caller's masks. They do not populate IndexWorkspace's directory,
	// certificate, posting-decision, document, or tape buffers.
	if state.root.ExactIndexRoot != (storeio.PageRef{}) {
		// Each iterator result is one live posting tile, and s.live contains
		// exactly those occupied tiles for this frozen generation.
		bound.MaskCount = uint64(len(s.live))
		return bound, nil
	}

	chunks := uint64(state.root.LiveChunks)
	bound.MaskCount = chunks
	if chunks == 0 {
		return bound, nil
	}
	// A catalog-free snapshot can still use chunk summaries, whose query-owned
	// masks are covered by MaskCount. No exact-index probe can run, so charging
	// directory/certificate workspace here would needlessly suppress that
	// independent pruning tier.
	if len(s.collection.options.Indexes) == 0 {
		return bound, nil
	}
	entries := retainedIndexSliceBytes(
		chunks, uint64(unsafe.Sizeof(storeio.IndexDirectoryEntry{})),
	)
	certificates := retainedIndexLogicalBytes(
		indexBoundProduct(
			chunks,
			uint64(storeio.IndexDirectoryMaxCertificate(state.root.PageSize)),
		),
	)
	postings := retainedIndexSliceBytes(
		chunks, uint64(unsafe.Sizeof(fileIndexProbePosting{})),
	)
	bound.CandidateWorkspaceBytes =
		indexBoundSum(indexBoundSum(entries, certificates), postings)

	document := retainedIndexSliceBytes(
		uint64(state.root.MaxDocumentBytes), 1,
	)
	bound.ExactSingleWorkspaceBytes =
		indexBoundSum(bound.CandidateWorkspaceBytes, document)

	tapeEntries := uint64(state.root.MaxDocumentBytes) + 2
	tape := retainedIndexSliceBytes(
		tapeEntries, uint64(unsafe.Sizeof(vibejson.IndexEntry{})),
	)
	bound.ExactCompoundWorkspaceBytes =
		indexBoundSum(bound.ExactSingleWorkspaceBytes, tape)
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
