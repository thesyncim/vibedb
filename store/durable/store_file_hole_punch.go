package durable

import (
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// fileStoreHolePunchMaxCalls caps filesystem work added to one explicit
// physical durability boundary. One call keeps the global writer stall bounded
// by a single filesystem operation even for an adversarially fragmented free
// set. Later foreground boundaries continue the exact monotone sweep; there is
// no background or close-time maintenance pass.
const fileStoreHolePunchMaxCalls = 1

// Interrupted platform calls are retried only a fixed number of times. EINTR
// means no deallocation completed, but an unbounded retry loop would still turn
// one foreground boundary into unbounded writer-held work.
const fileStoreHolePunchMaxAttempts = 4

// Candidate discovery uses the same one-extent bound. Advancing the exact
// source cursor after each success makes progress independent of the optional
// completion cache, so hash collisions can cause only a redundant future call,
// never a livelock or an unbounded scan under the reader fence.
const fileStoreHolePunchCandidateWindow = fileStoreHolePunchMaxCalls

// Physical offsets are bounded by MaxInt64, so MaxUint64 is an unambiguous
// process-local marker that the current reusable offset sweep reached its end.
const fileStoreHolePunchOffsetSweepDone = ^uint64(0)

const (
	// Four-way set-associative completion tracking avoids duplicate work during
	// a retry or process-local source handoff without growing with file history.
	// It is an optimization, not sweep authority: exact monotone source cursors
	// guarantee stable convergence even after an entry is evicted. Exact
	// (offset,length,retired-generation) identity is essential: reusing and
	// later re-retiring the same physical extent gives it a newer generation and
	// must make it eligible again. Collisions can only cause a redundant future
	// punch, never suppress a required one.
	fileStoreHolePunchCompletionWays  = 4
	fileStoreHolePunchCompletionSlots = 256
)

func fileStoreHolePunchCompletionSet(extent storeio.FreeExtent) int {
	hash := extent.Offset ^ extent.Length<<17 ^ extent.Length>>47 ^
		extent.RetiredGeneration*0x9e3779b97f4a7c15
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	return int(hash&(fileStoreHolePunchCompletionSlots/fileStoreHolePunchCompletionWays-1)) *
		fileStoreHolePunchCompletionWays
}

func (c *Collection) holePunchCompleted(extent storeio.FreeExtent) bool {
	base := fileStoreHolePunchCompletionSet(extent)
	for way := range fileStoreHolePunchCompletionWays {
		if c.holePunchCompletions[base+way] == extent {
			return true
		}
	}
	return false
}

func (c *Collection) rememberHolePunchCompletion(extent storeio.FreeExtent) {
	base := fileStoreHolePunchCompletionSet(extent)
	for way := range fileStoreHolePunchCompletionWays {
		entry := &c.holePunchCompletions[base+way]
		if *entry == extent || entry.RetiredGeneration == 0 ||
			entry.Offset == extent.Offset && entry.Length == extent.Length {
			*entry = extent
			return
		}
	}
	way := int(c.holePunchCompletionVictim % fileStoreHolePunchCompletionWays)
	c.holePunchCompletionVictim++
	c.holePunchCompletions[base+way] = extent
}

// appendHolePunchOffsetWindow copies one non-circular fixed window from an
// offset-sorted source. The binary search and bounded copy replace the old
// whole-free-set copy performed while readers were fenced. New free ranges
// inserted below cursor explicitly rewind it through rewindHolePunchReusable.
func appendHolePunchOffsetWindow(
	dst, src []storeio.FreeExtent, cursor uint64, limit int,
) ([]storeio.FreeExtent, uint64) {
	if limit <= 0 || len(dst) == cap(dst) ||
		cursor == fileStoreHolePunchOffsetSweepDone {
		return dst, cursor
	}
	if len(src) == 0 {
		return dst, fileStoreHolePunchOffsetSweepDone
	}
	start := 0
	if cursor != 0 {
		low, high := 0, len(src)
		for low < high {
			middle := int(uint(low+high) >> 1)
			if src[middle].Offset < cursor {
				low = middle + 1
			} else {
				high = middle
			}
		}
		if low == len(src) {
			return dst, fileStoreHolePunchOffsetSweepDone
		}
		start = low
	}
	copied := min(len(src)-start, limit, cap(dst)-len(dst))
	dst = append(dst, src[start:start+copied]...)
	if start+copied == len(src) {
		return dst, fileStoreHolePunchOffsetSweepDone
	}
	return dst, src[start+copied].Offset
}

// appendHolePunchIndexWindow is the corresponding bounded linear copy for the
// small append-only absorbed-retirement source. Newly absorbed entries extend
// the slice after the cursor; merging the slice into reusable resets the cursor.
func appendHolePunchIndexWindow(
	dst, src []storeio.FreeExtent, cursor uint64, limit int,
) ([]storeio.FreeExtent, uint64) {
	if len(src) == 0 || limit <= 0 || len(dst) == cap(dst) {
		return dst, cursor
	}
	if cursor >= uint64(len(src)) {
		return dst, uint64(len(src))
	}
	start := int(cursor)
	copied := min(len(src)-start, limit, cap(dst)-len(dst))
	dst = append(dst, src[start:start+copied]...)
	return dst, uint64(start + copied)
}

// rewindHolePunchReusable makes a newly added/coalesced physical range visible
// to the offset sweep without throwing away progress below the affected point.
// Reuse and removal do not call it: a remaining subset of an already-punched
// hole is still punched, while an unvisited subset remains at or after cursor.
func (c *Collection) rewindHolePunchReusable(offset uint64) {
	if c == nil || offset == 0 || c.holePunchReusableCursor == 0 {
		return
	}
	if c.holePunchReusableCursor == fileStoreHolePunchOffsetSweepDone ||
		offset < c.holePunchReusableCursor {
		c.holePunchReusableCursor = offset
	}
}

// punchDurableFreeExtentsLocked releases physical blocks for free extents only
// after the corresponding root is durable and the recovery journal has been
// recycled. The caller holds writer. Hole punching never edits reusable,
// retirementAbsorbed, the reclaimer, or the durable free log: it changes only
// filesystem allocation beneath byte ranges every possible reader/root has
// already proved dead.
func (c *Collection) punchDurableFreeExtentsLocked() error {
	if c == nil || c.file == nil || c.committer == nil || c.reclaimer == nil ||
		c.holePunchDisabled || cap(c.freeImageScratch) == 0 {
		return nil
	}
	if c.freeImageScratchInUse {
		return fmt.Errorf(
			"%w: hole punch raced free-image planner",
			storeio.ErrFreeLogCorrupt,
		)
	}
	state := c.durableState.Load()
	if state == nil || state.root.Generation == 0 {
		return nil
	}
	current := c.committer.DurableGeneration()
	fallback := c.committer.FallbackGeneration()
	if current == 0 || fallback == 0 || state.root.Generation < current {
		return nil
	}
	// recycleRecoveryJournalLocked can deliberately retain a newer carried
	// suffix while an older physical cut drains. A nil return in that case is a
	// safe recycle no-op, not authority to deallocate beneath replay. Wait for a
	// later physical root whose successful recycle covers this exact watermark.
	if c.journalEnabled() && c.journal.BaseGeneration() < current {
		return nil
	}

	c.snapshotGate.Lock()
	c.beginReaderFence()
	readerFenceHeld := true
	releaseReaderFence := func() {
		if !readerFenceHeld {
			return
		}
		readerFenceHeld = false
		c.endReaderFence()
		c.snapshotGate.Unlock()
	}
	defer releaseReaderFence()

	ranges := c.freeImageScratch[:0]
	defer func() {
		clear(ranges)
		c.freeImageScratch = c.freeImageScratch[:0]
	}()
	limit := min(cap(ranges), fileStoreHolePunchCandidateWindow)
	reusableCursor := c.holePunchReusableCursor
	pendingCursor := c.holePunchPendingCursor
	absorbedCursor := c.holePunchAbsorbedCursor
	source := int(c.holePunchCandidateSource % 3)
	selectedSource := -1
	for pass := 0; pass < 3 && len(ranges) < limit; pass++ {
		candidateSource := (source + pass) % 3
		before := len(ranges)
		remaining := limit - len(ranges)
		switch candidateSource {
		case 0:
			ranges, reusableCursor = appendHolePunchOffsetWindow(
				ranges, c.reusable, reusableCursor, remaining,
			)
		case 1:
			ranges, pendingCursor, _ = c.reclaimer.AppendPunchableAfter(
				ranges, current, fallback, pendingCursor, remaining,
			)
		case 2:
			ranges, absorbedCursor = appendHolePunchIndexWindow(
				ranges, c.retirementAbsorbed, absorbedCursor, remaining,
			)
		}
		if len(ranges) != before {
			selectedSource = candidateSource
		}
	}
	if c.holePunchCandidateFenced != nil {
		c.holePunchCandidateFenced()
	}
	// The exact reader/recovery floors have now been sampled and the bounded
	// candidate values copied while new readers were diverted. The caller still
	// holds writer, so no allocator can reuse those extents; a reader admitted
	// after this point can select only the unchanged current root and therefore
	// cannot address them. Release the reader fence before validation and, most
	// importantly, before any filesystem deallocation syscall.
	releaseReaderFence()
	if len(ranges) == 0 {
		c.holePunchReusableCursor = reusableCursor
		c.holePunchPendingCursor = pendingCursor
		c.holePunchAbsorbedCursor = absorbedCursor
		c.holePunchCandidateSource = uint8((source + 1) % 3)
		return nil
	}

	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		return err
	}
	pageSize := uint64(state.root.PageSize)
	extent := ranges[0]
	if extent.Offset < layout.DataStart || extent.Length == 0 ||
		extent.RetiredGeneration == 0 ||
		extent.Offset%pageSize != 0 || extent.Length%pageSize != 0 ||
		extent.Offset > math.MaxInt64 || extent.Length > math.MaxInt64 ||
		extent.Offset > ^uint64(0)-extent.Length {
		return fmt.Errorf(
			"%w: invalid hole-punch extent %+v",
			storeio.ErrFreeLogCorrupt, extent,
		)
	}
	end := extent.Offset + extent.Length
	if end > state.fileEnd || end > math.MaxInt64 {
		return fmt.Errorf(
			"%w: out-of-bounds hole-punch extent %+v fileEnd=%d",
			storeio.ErrFreeLogCorrupt, extent, state.fileEnd,
		)
	}
	if !c.punchFileStoreExtent(extent) {
		return nil
	}
	c.holePunchReusableCursor = reusableCursor
	c.holePunchPendingCursor = pendingCursor
	c.holePunchAbsorbedCursor = absorbedCursor
	c.holePunchCandidateSource = uint8((selectedSource + 1) % 3)
	return nil
}

// punchFileStoreExtent performs at most one successful deallocation operation.
// Completion tracking is only an idempotence optimization: the exact source
// cursor advances after either a cache hit or a success, so eviction cannot
// affect convergence. Any optional platform failure disables this optimization
// for the process instead of repeatedly taxing later foreground boundaries.
func (c *Collection) punchFileStoreExtent(extent storeio.FreeExtent) bool {
	if c == nil || c.holePunchDisabled {
		return false
	}
	if c.holePunchCompleted(extent) {
		return true
	}
	punch := c.holePunch
	if punch == nil {
		punch = punchFileStoreHole
	}
	supported, punchErr := punch(c.file, extent.Offset, extent.Length)
	if !supported {
		c.holePunchDisabled = true
		c.holePunchUnsupported.Add(1)
		c.holePunchSkippedRanges.Add(1)
		return false
	}
	if punchErr != nil {
		c.holePunchErrors.Add(1)
		c.holePunchSkippedRanges.Add(1)
		c.holePunchDisabled = true
		return false
	}
	c.holePunchRanges.Add(1)
	c.holePunchBytes.Add(extent.Length)
	c.rememberHolePunchCompletion(extent)
	return true
}
