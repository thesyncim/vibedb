package storeio

import (
	"cmp"
	"fmt"
	"slices"
)

// Geometry of the durable free log. These are policy rather than encoding: the
// writer never exceeds them and the reader rejects anything that does, so a
// corrupt prev or next link cannot turn recovery into an unbounded walk, and a
// reopen's free-set cost is a fixed number of page reads instead of a function
// of how long the store has been running.
const (
	// FreeLogMaxChainPages is the delta-chain length at which the writer folds
	// the chain back into a fresh image. It is a straight trade: every open
	// reads the whole chain, and every fold rewrites the whole image. At 32 the
	// worst reopen reads 32 delta pages plus at most 16 image pages, 192 KiB at
	// the 4 KiB page size, while a fold recurs at most once per 32 commits and
	// so amortises to well under one image page written per commit. Lowering it
	// makes opens cheaper and steady-state commits more expensive; raising it
	// does the reverse.
	FreeLogMaxChainPages = 32
	// FreeLogMaxFoldSegments is the single-document baseline for how many image
	// segments one fold rewrites, and so how many fold pages an ordinary commit
	// reserves. A durable writer that admits wider atomic batches may raise its
	// in-memory fold limit and transaction reservation up to the complete
	// segment-index capacity; the on-disk format does not encode this policy.
	//
	// It replaced FreeLogMaxImagePages, which bounded the image itself at
	// sixteen pages and therefore bounded the entire free set at about 2,700
	// extents. Bounding the whole image was the only way to bound a fold while
	// the image was a linked list, because changing one page changed the
	// reference in the page before it and a fold rewrote everything. With the
	// segment index a fold rewrites only the segments whose extents changed, so
	// the bound moved off the image and onto the fold, which is where it belongs:
	// the free set may now be as large as the index can name, and it is the
	// per-commit write that stays fixed.
	//
	// Sixteen is chosen against the reclaim batch. Reclamation is what scatters
	// changes across segments, so the store caps one commit's batch by how many
	// segments it would dirty rather than by extents alone, and extents that do
	// not fit stay pending and are offered again next commit. Allocation cannot
	// scatter: it takes from the tail of the lowest-offset extent that fits, so a
	// transaction's page allocations land in a short run of consecutive extents
	// and therefore in one or two segments. Sixteen leaves room for both plus the
	// splits a grown segment causes.
	FreeLogMaxFoldSegments = 16
	// FreeLogMaxDeltaPages bounds one commit's diff. A commit that needs more
	// folds instead, which is always available because a fold's own diff
	// describes only the segment, index, and delta pages it just allocated.
	FreeLogMaxDeltaPages = 4
)

// FreeLogBounds are the physical and logical high-water marks the selected
// superblock and state root publish. Every page in the chain is validated
// against them, so a stale or grafted page cannot describe space past the end
// of the file the caller is actually recovering.
type FreeLogBounds struct {
	FileEnd       uint64
	NextLogicalID uint64
	// ProtectedExtent is one immutable physical run that replay must never
	// advertise as reusable. Mutable Store recovery sets it to the exact
	// canonical catalog run. Keeping the fence in replay, immediately before
	// insertion into dst, prevents a checksummed external image or delta from
	// resurrecting catalog pages even if an older writer retired them.
	//
	// The zero value means no protected run, preserving the standalone
	// external-free-log API.
	ProtectedExtent FreeExtent
}

// ExtentOverlapsProtected reports whether extent reaches the immutable run in
// these bounds. It is shared by replay and by writers assembling a new folded
// image so neither direction can publish protected catalog bytes as free.
func (b FreeLogBounds) ExtentOverlapsProtected(
	extent FreeExtent,
) bool {
	return freeLogExtentOverlapsProtected(extent, b.ProtectedExtent)
}

// FreeLogPages are the pages that carry the current durable free set, and the
// segment descriptors the index published.
//
// The writer keeps the page lists so a fold can retire exactly what it
// supersedes; without them the old chain would be unreachable but never
// reclaimed, which is the same unbounded growth this representation exists to
// remove. It keeps Segments because a fold now rewrites only the segments that
// changed and must carry the rest forward by reference: a fold that rebuilt the
// index from scratch would have to rewrite every segment to learn where it was,
// which is the whole-image rewrite this design removed.
//
// Resident marks, per segment, whether its extents were read. An open reads
// only the segments it must — the ones the delta chain has records for, which
// cannot be applied otherwise, and then the largest-free ones until the budget
// is spent — because reading all of them makes open time proportional to the
// free set rather than to the part of it the store is about to use. A
// non-resident segment contributes nothing to the returned extents; its page
// remains the whole truth about its own offset range.
//
// Index and Delta run oldest first. Segments run in ascending FirstOffset, and
// Resident is parallel to Segments.
type FreeLogPages struct {
	Index    []PageRef
	Delta    []PageRef
	Segments []FreeSegment
	Resident []bool
}

// replayExternalFreeLog rebuilds the external portion of the durable free set
// from head, the newest delta page.
// It walks Prev to the end of the chain, takes the segment index the newest
// page names, reads the segments it needs, and applies the collected deltas
// oldest-to-newest.
//
// segmentBudget caps how many segments are read. It is what keeps an open
// proportional to the working set instead of to the free set: at 165 extents per
// segment, a store with ninety thousand free extents has five hundred and
// forty-six of them, and reading all of them costs half a megabyte before the
// first write. The store does not need them — it needs somewhere to allocate
// from, and the index publishes each segment's largest free extent so the
// biggest can be picked without reading any.
//
// Two classes are read regardless of the budget. A segment the delta chain has
// records for must be read, because a record supersedes what its segment says
// and applying one to nothing would invent an extent. And the budget is a floor
// of one, so a store always has somewhere to look.
//
// dst is appended to and must have room for limit extents; exceeding limit is
// reported rather than grown, because the caller's arena is fixed and reusing a
// larger Go-heap copy would silently move the free set onto the heap.
func replayExternalFreeLog(
	cache *PageCache, head PageRef, bounds FreeLogBounds, dst []FreeExtent, limit, segmentBudget int,
) ([]FreeExtent, FreeLogPages, error) {
	var pages FreeLogPages
	if err := validateFreeLogReplayBounds(bounds, dst); err != nil {
		return dst, pages, err
	}
	if head == (PageRef{}) {
		return dst, pages, nil
	}
	if cache == nil || limit < len(dst) {
		return dst, pages, fmt.Errorf("%w: free log replay", ErrInvalidWrite)
	}
	records, indexHead, err := collectFreeLogDeltas(cache, head, bounds, &pages)
	if err != nil {
		return dst, pages, err
	}
	return replayCollectedFreeLog(
		cache, indexHead, bounds, dst, limit, segmentBudget, records, pages,
	)
}

// ReplayInlineFreeLog rebuilds the durable free set published by an inline
// superblock. ExternalPrev is the newest spilled delta page; the records in
// inline are the cumulative changes after it and therefore win over every
// external record for the same offset. IndexHead is also accepted without an
// external delta page, which is the steady state immediately after a fold.
func ReplayInlineFreeLog(
	cache *PageCache, inline *InlineFreeDelta, bounds FreeLogBounds,
	dst []FreeExtent, limit, segmentBudget int,
) ([]FreeExtent, FreeLogPages, error) {
	var pages FreeLogPages
	if err := validateFreeLogReplayBounds(bounds, dst); err != nil {
		return dst, pages, err
	}
	if inline == nil ||
		inline.ExternalPrev() == (PageRef{}) &&
			inline.IndexHead() == (PageRef{}) &&
			inline.Len() == 0 {
		return dst, pages, nil
	}
	if cache == nil || limit < len(dst) {
		return dst, pages, fmt.Errorf("%w: inline free log replay", ErrInvalidWrite)
	}

	indexHead := inline.IndexHead()
	var records []freeLogRecord
	if externalPrev := inline.ExternalPrev(); externalPrev != (PageRef{}) {
		var externalIndex PageRef
		var err error
		records, externalIndex, err = collectFreeLogDeltas(
			cache, externalPrev, bounds, &pages,
		)
		if err != nil {
			return dst, pages, err
		}
		// The inline root and every page in its external predecessor chain must
		// name one index image. Otherwise recovery would combine changes from
		// one free-set generation with segments from another.
		if externalIndex != indexHead {
			return dst, pages, fmt.Errorf(
				"%w: inline and external chain span two images", ErrFreeLogCorrupt,
			)
		}
	}
	var err error
	records, err = collectInlineFreeLogDeltas(inline, records)
	if err != nil {
		return dst, pages, err
	}
	return replayCollectedFreeLog(
		cache, indexHead, bounds, dst, limit, segmentBudget, records, pages,
	)
}

func replayCollectedFreeLog(
	cache *PageCache, indexHead PageRef, bounds FreeLogBounds,
	dst []FreeExtent, limit, segmentBudget int,
	records []freeLogRecord, pages FreeLogPages,
) ([]FreeExtent, FreeLogPages, error) {
	if err := collectFreeLogIndex(cache, indexHead, bounds, &pages); err != nil {
		return dst, pages, err
	}
	pages.Resident = chooseFreeLogResidency(pages.Segments, records, segmentBudget)
	image, err := collectFreeLogSegments(cache, bounds, pages.Segments, pages.Resident)
	if err != nil {
		return dst, pages, err
	}
	dst, err = applyFreeLogRecords(dst, image, records, bounds, limit)
	if err != nil {
		return dst, pages, err
	}
	return dst, pages, nil
}

// freeLogRecord pairs one delta with how recent it is. The chain is walked
// newest-first and each page's records are read back to front, so rank
// ascending is exactly reverse chronological order and the smallest rank for an
// offset is the record that survives. Ranking avoids a second traversal purely
// to reverse the chain, which would double the reads a reopen performs.
type freeLogRecord struct {
	rank  int
	delta FreeDelta
}

func collectFreeLogDeltas(
	cache *PageCache, head PageRef, bounds FreeLogBounds, pages *FreeLogPages,
) ([]freeLogRecord, PageRef, error) {
	records := make([]freeLogRecord, 0, FreeLogMaxChainPages*FreeDeltaRecordCapacity(head.Length))
	var indexHead PageRef
	for ref := head; ref != (PageRef{}); {
		if len(pages.Delta) == FreeLogMaxChainPages {
			return nil, PageRef{}, fmt.Errorf(
				"%w: delta chain exceeds %d pages", ErrFreeLogCorrupt, FreeLogMaxChainPages)
		}
		lease, err := cache.Acquire(ref)
		if err != nil {
			return nil, PageRef{}, err
		}
		view, err := OpenFreeDeltaPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
		if err != nil {
			lease.Release()
			return nil, PageRef{}, err
		}
		// Every page repeats the index it is relative to. Disagreement means two
		// chains have been spliced, and applying one chain's diff to another
		// chain's image would advertise space that is live.
		if len(pages.Delta) == 0 {
			indexHead = view.IndexHead()
		} else if view.IndexHead() != indexHead {
			lease.Release()
			return nil, PageRef{}, fmt.Errorf("%w: chain spans two images", ErrFreeLogCorrupt)
		}
		for i := view.Len() - 1; i >= 0; i-- {
			delta, ok := view.DeltaAt(i)
			if !ok {
				lease.Release()
				return nil, PageRef{}, ErrFreeLogCorrupt
			}
			records = append(records, freeLogRecord{rank: len(records), delta: delta})
		}
		pages.Delta = append(pages.Delta, ref)
		prev := view.Prev()
		lease.Release()
		ref = prev
	}
	slices.Reverse(pages.Delta)
	return canonicalFreeLogRecords(records), indexHead, nil
}

// collectInlineFreeLogDeltas gives every inline record a lower (newer) rank
// than every external record, then performs the same offset canonicalization as
// external replay. InlineFreeDelta's production codec already validates record
// bounds; this seam still checks decode and canonical ordering so a malformed
// in-memory value cannot silently change which record wins.
func collectInlineFreeLogDeltas(
	inline *InlineFreeDelta, external []freeLogRecord,
) ([]freeLogRecord, error) {
	records := make([]freeLogRecord, 0, inline.Len()+len(external))
	var previousOffset uint64
	for rank := 0; rank < inline.Len(); rank++ {
		delta, ok := inline.DeltaAt(rank)
		if !ok || rank != 0 && delta.Extent.Offset <= previousOffset {
			return nil, fmt.Errorf("%w: malformed inline free records", ErrFreeLogCorrupt)
		}
		previousOffset = delta.Extent.Offset
		records = append(records, freeLogRecord{rank: len(records), delta: delta})
	}
	for _, record := range external {
		records = append(records, freeLogRecord{rank: len(records), delta: record.delta})
	}
	return canonicalFreeLogRecords(records), nil
}

func canonicalFreeLogRecords(records []freeLogRecord) []freeLogRecord {
	slices.SortFunc(records, func(a, b freeLogRecord) int {
		if c := cmp.Compare(a.delta.Extent.Offset, b.delta.Extent.Offset); c != 0 {
			return c
		}
		return cmp.Compare(a.rank, b.rank)
	})
	kept := records[:0]
	var previous uint64
	for i, record := range records {
		if i == 0 || record.delta.Extent.Offset != previous {
			kept = append(kept, record)
		}
		previous = record.delta.Extent.Offset
	}
	return kept
}

// collectFreeLogIndex walks the index chain from its newest page and leaves
// pages.Segments in ascending FirstOffset. The chain is walked high-to-low
// because that is the only direction a copy-on-write chain can be written in,
// and reversed once at the end rather than traversed twice.
func collectFreeLogIndex(
	cache *PageCache, indexHead PageRef, bounds FreeLogBounds, pages *FreeLogPages,
) error {
	if indexHead == (PageRef{}) {
		return nil
	}
	pages.Segments = make([]FreeSegment, 0,
		FreeLogMaxIndexPages*FreeIndexRecordCapacity(indexHead.Length))
	for ref := indexHead; ref != (PageRef{}); {
		if len(pages.Index) == FreeLogMaxIndexPages {
			return fmt.Errorf(
				"%w: index exceeds %d pages", ErrFreeLogCorrupt, FreeLogMaxIndexPages)
		}
		lease, err := cache.Acquire(ref)
		if err != nil {
			return err
		}
		view, err := OpenFreeIndexPage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
		if err != nil {
			lease.Release()
			return err
		}
		// Read this page's segments back to front so that reversing the whole
		// accumulated slice once, below, restores ascending order across pages.
		for i := view.Len() - 1; i >= 0; i-- {
			segment, ok := view.SegmentAt(i)
			if !ok {
				lease.Release()
				return ErrFreeLogCorrupt
			}
			pages.Segments = append(pages.Segments, segment)
		}
		pages.Index = append(pages.Index, ref)
		prev := view.Prev()
		lease.Release()
		ref = prev
	}
	slices.Reverse(pages.Index)
	slices.Reverse(pages.Segments)
	// Page-local order is checked by the opener; the join between pages is
	// checked here, because a merge that assumes a partitioned offset space
	// would otherwise give one offset two owners from a plausible-looking chain.
	for i := 1; i < len(pages.Segments); i++ {
		if pages.Segments[i-1].FirstOffset >= pages.Segments[i].FirstOffset {
			return fmt.Errorf("%w: index segment order across pages", ErrFreeLogCorrupt)
		}
	}
	return nil
}

// chooseFreeLogResidency decides which segments an open reads.
//
// Every segment a record names is mandatory: records win over segments, so
// applying one to a segment that was not read would merge a diff into nothing
// and invent an extent at that offset. Beyond those, segments are taken by
// descending largest-free-extent, because the only thing the store needs the
// free set for is somewhere to put the next page, and a segment whose biggest
// extent is one page can serve strictly less than one whose biggest is eight.
//
// The index publishes LargestFree for exactly this: choosing without reading.
func chooseFreeLogResidency(segments []FreeSegment, records []freeLogRecord, budget int) []bool {
	resident := make([]bool, len(segments))
	if len(segments) == 0 {
		return resident
	}
	budget = max(budget, 1)
	loaded := 0
	admit := func(i int) {
		if !resident[i] {
			resident[i] = true
			loaded++
		}
	}
	for _, record := range records {
		admit(freeSegmentOwning(segments, record.delta.Extent.Offset))
	}
	if loaded >= budget {
		return resident
	}
	// Partial selection by largest free extent. The set is small — one entry per
	// 165 extents — so a linear scan per pick is cheaper than sorting a copy, and
	// it allocates nothing.
	for loaded < budget && loaded < len(segments) {
		best, bestFree := -1, uint64(0)
		for i, segment := range segments {
			if resident[i] {
				continue
			}
			if best == -1 || segment.LargestFree > bestFree {
				best, bestFree = i, segment.LargestFree
			}
		}
		if best == -1 {
			break
		}
		admit(best)
	}
	return resident
}

// freeSegmentOwning returns the segment that owns offset. The first segment owns
// everything below its own first offset and the last owns everything above, so
// every offset in the file has exactly one owner however far the free set has
// drifted from the index.
func freeSegmentOwning(segments []FreeSegment, offset uint64) int {
	lo, hi := 0, len(segments)
	for lo < hi {
		middle := int(uint(lo+hi) >> 1)
		if segments[middle].FirstOffset <= offset {
			lo = middle + 1
		} else {
			hi = middle
		}
	}
	if lo == 0 {
		return 0
	}
	return lo - 1
}

// collectFreeLogSegments reads the segments residency selected. A segment left
// unread is not missing: its page still describes its own offset range in full,
// and nothing may change that range until the store reads it.
func collectFreeLogSegments(
	cache *PageCache, bounds FreeLogBounds, segments []FreeSegment, resident []bool,
) ([]FreeExtent, error) {
	if len(segments) == 0 {
		return nil, nil
	}
	total := 0
	for i, segment := range segments {
		if resident[i] {
			total += int(segment.Count)
		}
	}
	extents := make([]FreeExtent, 0, total)
	for i, segment := range segments {
		if !resident[i] {
			continue
		}
		lease, err := cache.Acquire(segment.Ref)
		if err != nil {
			return nil, err
		}
		view, err := OpenFreeImagePage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
		if err != nil {
			lease.Release()
			return nil, err
		}
		// The index publishes each segment's extent count and first offset. A
		// segment whose page disagrees means the index and the image were
		// published from different states, and merging them would advertise
		// space some other segment still owns.
		if view.Len() != int(segment.Count) {
			lease.Release()
			return nil, fmt.Errorf("%w: segment count disagrees with index", ErrFreeLogCorrupt)
		}
		for i := range view.Len() {
			extent, ok := view.ExtentAt(i)
			if !ok || i == 0 && extent.Offset != segment.FirstOffset ||
				len(extents) != 0 && extents[len(extents)-1].Offset+
					extents[len(extents)-1].Length > extent.Offset {
				lease.Release()
				return nil, fmt.Errorf("%w: segment extent order", ErrFreeLogCorrupt)
			}
			extents = append(extents, extent)
		}
		lease.Release()
	}
	return extents, nil
}

// applyFreeLogRecords merges the offset-sorted surviving records into the
// offset-sorted image. Both inputs are keyed by offset and an extent's offset
// never moves once chosen — allocation always takes from an extent's tail —
// so one linear pass is the whole of "apply the deltas".
func applyFreeLogRecords(
	dst []FreeExtent,
	image []FreeExtent,
	records []freeLogRecord,
	bounds FreeLogBounds,
	limit int,
) ([]FreeExtent, error) {
	start := len(dst)
	appendExtent := func(extent FreeExtent) error {
		if bounds.ExtentOverlapsProtected(extent) {
			return fmt.Errorf(
				"%w: free extent overlaps immutable run",
				ErrFreeLogCorrupt,
			)
		}
		if len(dst) == limit {
			return ErrRetiredExtentCapacity
		}
		if len(dst) != start {
			previous := dst[len(dst)-1]
			// Overlap is the one failure that cannot be tolerated: an
			// over-reported free set hands out space a live page occupies.
			if previous.Offset+previous.Length > extent.Offset {
				return fmt.Errorf("%w: overlapping free extents", ErrFreeLogCorrupt)
			}
		}
		dst = append(dst, extent)
		return nil
	}
	i, j := 0, 0
	for i < len(image) || j < len(records) {
		var extent FreeExtent
		switch {
		case j == len(records) || i < len(image) && image[i].Offset < records[j].delta.Extent.Offset:
			extent = image[i]
			i++
		default:
			record := records[j]
			j++
			if i < len(image) && image[i].Offset == record.delta.Extent.Offset {
				i++
			}
			if record.delta.Op == FreeOpDelete {
				continue
			}
			extent = record.delta.Extent
		}
		if err := appendExtent(extent); err != nil {
			return dst[:start], err
		}
	}
	return dst, nil
}

func validateFreeLogReplayBounds(
	bounds FreeLogBounds,
	dst []FreeExtent,
) error {
	protected := bounds.ProtectedExtent
	if protected == (FreeExtent{}) {
		return nil
	}
	protectedEnd := protected.Offset + protected.Length
	if protected.Length == 0 ||
		protectedEnd < protected.Offset ||
		protectedEnd > bounds.FileEnd ||
		protected.RetiredGeneration != 0 {
		return fmt.Errorf(
			"%w: invalid immutable free-log fence",
			ErrInvalidWrite,
		)
	}
	for _, extent := range dst {
		if freeLogExtentOverlapsProtected(extent, protected) {
			return fmt.Errorf(
				"%w: existing reusable extent overlaps immutable run",
				ErrFreeLogCorrupt,
			)
		}
	}
	return nil
}

func freeLogExtentOverlapsProtected(
	extent FreeExtent,
	protected FreeExtent,
) bool {
	if protected == (FreeExtent{}) {
		return false
	}
	extentEnd := extent.Offset + extent.Length
	protectedEnd := protected.Offset + protected.Length
	// A malformed destination extent is not allowed to bypass the immutable
	// fence through unsigned wraparound. Source image/delta extents have
	// already passed their typed decoders; this also hardens caller-supplied
	// destination slices.
	if extent.Length == 0 || extentEnd < extent.Offset ||
		protectedEnd < protected.Offset {
		return true
	}
	return extent.Offset < protectedEnd &&
		protected.Offset < extentEnd
}
