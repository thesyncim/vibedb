package durable

import (
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// fileStoreHolePunchCandidateRuns bounds the fragmented physical runs inspected
// at one explicit durability boundary. Discovery divides this budget across all
// three sources, so sustained churn in one source cannot hide another source.
const fileStoreHolePunchCandidateRuns = 64

// The scheduler snapshots up to CandidateRuns physical runs but spends only
// the largest fixed prefix at this boundary.
const fileStoreHolePunchSelectedCalls = 6

// Selection is byte-bounded independently of fragmentation. Twenty MiB is
// enough to reclaim the complete mature class-5 image measured by the
// qualification harness while keeping one physical checkpoint predictable.
const fileStoreHolePunchSelectedBytes = uint64(20 << 20)

// Interrupted platform calls are retried only a fixed number of times. EINTR
// means no deallocation completed, but an unbounded retry loop would still turn
// one foreground boundary into unbounded writer-held work.
const fileStoreHolePunchMaxAttempts = 4

// Candidate discovery may collapse this many adjacent free identities into one
// physical range. This is an identity-scan bound, independent of the syscall
// bound above: mature class-5 churn commonly leaves hundreds of adjacent 32-KiB
// identities whose union is safe to release with one F_PUNCHHOLE/fallocate.
// The fixed window keeps reader-fenced work deterministic even for a corrupt or
// adversarially fragmented free set.
const fileStoreHolePunchCandidateWindow = 1024

const fileStoreHolePunchSourceCount = 3

// Active sources split the byte budget equally before spare calls and bytes are
// redistributed. One third remains the coalescing/chunk ceiling even when a
// peer source is idle, preventing one syscall from monopolizing the boundary.
const fileStoreHolePunchFairBytes = fileStoreHolePunchSelectedBytes /
	fileStoreHolePunchSourceCount

// Physical offsets are bounded by MaxInt64, so MaxUint64 is an unambiguous
// process-local marker that the current reusable offset sweep reached its end.
const fileStoreHolePunchOffsetSweepDone = ^uint64(0)

const (
	// One inspection window can contain 1,024 exact identities. Keeping twice
	// that many open-addressed slots lets the scheduler retain every successful
	// constituent while it drains the window over several physical roots. The
	// table is cleared when the window advances, so it never grows with history.
	fileStoreHolePunchCompletionSlots = 2048
)

// fileStoreHolePunchPartial retains progress through one oversized identity per
// source. Losing it at process exit is safe (punching an already sparse prefix
// is idempotent); retaining it across foreground boundaries prevents a stable
// large free extent from restarting at its first chunk forever.
type fileStoreHolePunchPartial struct {
	extent    storeio.FreeExtent
	completed uint64
}

func fileStoreHolePunchCompletionSet(extent storeio.FreeExtent) int {
	hash := extent.Offset ^ extent.Length<<17 ^ extent.Length>>47 ^
		extent.RetiredGeneration*0x9e3779b97f4a7c15
	hash ^= hash >> 30
	hash *= 0xbf58476d1ce4e5b9
	hash ^= hash >> 27
	return int(hash & (fileStoreHolePunchCompletionSlots - 1))
}

func (c *Collection) holePunchCompleted(extent storeio.FreeExtent) bool {
	base := fileStoreHolePunchCompletionSet(extent)
	for probe := range fileStoreHolePunchCompletionSlots {
		entry := c.holePunchCompletions[(base+probe)&
			(fileStoreHolePunchCompletionSlots-1)]
		if entry.RetiredGeneration == 0 {
			return false
		}
		if entry == extent {
			return true
		}
	}
	return false
}

func (c *Collection) rememberHolePunchCompletion(extent storeio.FreeExtent) {
	base := fileStoreHolePunchCompletionSet(extent)
	for probe := range fileStoreHolePunchCompletionSlots {
		entry := &c.holePunchCompletions[(base+probe)&
			(fileStoreHolePunchCompletionSlots-1)]
		if *entry == extent || entry.RetiredGeneration == 0 {
			*entry = extent
			return
		}
	}
	// This cannot occur while draining one bounded inspection window (the table
	// has twice its maximum population). Retain a safe degradation for direct
	// package use: eviction can only cause a redundant future punch.
	way := int(c.holePunchCompletionVictim % fileStoreHolePunchCompletionSlots)
	c.holePunchCompletionVictim++
	c.holePunchCompletions[way] = extent
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

func fileStoreHolePunchAdjacent(
	left, right storeio.FreeExtent,
) bool {
	return left.Length != 0 &&
		left.Offset <= ^uint64(0)-left.Length &&
		left.Offset+left.Length == right.Offset
}

func fileStoreHolePunchRunContinues(
	left, right storeio.FreeExtent, runBytes uint64,
) bool {
	return fileStoreHolePunchAdjacent(left, right) &&
		runBytes <= fileStoreHolePunchFairBytes &&
		right.Length <= fileStoreHolePunchFairBytes-runBytes
}

// fileStoreHolePunchCandidatePrefix is the completion-aware form used by the
// physical-root scheduler. Completed identities remain part of the inspected
// source prefix (and therefore advance its local cursor once the whole window
// drains), but they neither consume a run nor bridge two uncompleted runs.
func fileStoreHolePunchCandidatePrefix(
	src []storeio.FreeExtent, completed []bool, rangeLimit int,
) (identities, ranges int) {
	if len(src) == 0 || rangeLimit <= 0 {
		return 0, 0
	}
	open := false
	runBytes := uint64(0)
	var previous storeio.FreeExtent
	for rank, extent := range src {
		if rank < len(completed) && completed[rank] {
			open = false
			runBytes = 0
			continue
		}
		if open && fileStoreHolePunchRunContinues(previous, extent, runBytes) {
			previous = extent
			runBytes += extent.Length
			continue
		}
		if ranges == rangeLimit {
			return rank, ranges
		}
		ranges++
		open = true
		previous = extent
		runBytes = extent.Length
	}
	return len(src), ranges
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
	if c == nil || offset == 0 {
		return
	}
	if c.holePunchReusableCursor == 0 {
		return
	}
	if c.holePunchReusableCursor == fileStoreHolePunchOffsetSweepDone ||
		offset < c.holePunchReusableCursor {
		c.holePunchReusableCursor = offset
	}
}

// punchNewPhysicalGenerationLocked grants exactly one bounded scheduler pass
// to each newly authoritative physical generation. Journal-only Flushes never
// call it; repeated Wait/Flush completion for the same root is a scalar no-op.
func (c *Collection) punchNewPhysicalGenerationLocked(generation uint64) error {
	if c == nil || generation == 0 || c.holePunchDisabled ||
		generation <= c.holePunchGeneration {
		return nil
	}
	authority, err := c.punchDurableFreeExtentsBatchLocked()
	if err == nil && authority > c.holePunchGeneration {
		// Record the exact coherent generation the planner sampled, not a prior or
		// subsequent atomic observation. The durability callback does not take
		// writer and may otherwise advance between those two observations.
		c.holePunchGeneration = authority
	}
	return err
}

// punchDurableFreeExtentsLocked releases physical blocks for free extents only
// after the corresponding root is durable and the recovery journal has been
// recycled. The caller holds writer. Hole punching never edits reusable,
// retirementAbsorbed, the reclaimer, or the durable free log: it changes only
// filesystem allocation beneath byte ranges every possible reader/root has
// already proved dead.
func (c *Collection) punchDurableFreeExtentsLocked() error {
	_, err := c.punchDurableFreeExtentsBatchLocked()
	return err
}

func (c *Collection) punchDurableFreeExtentsBatchLocked() (uint64, error) {
	if c == nil || c.file == nil || c.committer == nil || c.reclaimer == nil ||
		c.holePunchDisabled || cap(c.freeImageScratch) == 0 {
		return 0, nil
	}
	if c.freeImageScratchInUse {
		return 0, fmt.Errorf(
			"%w: hole punch raced free-image planner",
			storeio.ErrFreeLogCorrupt,
		)
	}

	// durableState is promoted by the private persistence worker without taking
	// writer. Sample it, the committer roots, and journal authority under the same
	// gate used by that callback; the generation returned from this function is
	// therefore exactly the generation whose safety floor the planner used.
	c.snapshotGate.Lock()
	state := c.durableState.Load()
	if state == nil || state.root.Generation == 0 {
		c.snapshotGate.Unlock()
		return 0, nil
	}
	current := c.committer.DurableGeneration()
	fallback := c.committer.FallbackGeneration()
	if current == 0 || fallback == 0 || state.root.Generation < current {
		c.snapshotGate.Unlock()
		return 0, nil
	}
	// recycleRecoveryJournalLocked can deliberately retain a newer carried
	// suffix while an older physical cut drains. A nil return in that case is a
	// safe recycle no-op, not authority to deallocate beneath replay. Wait for a
	// later physical root whose successful recycle covers this exact watermark.
	if c.journalEnabled() && c.journal.BaseGeneration() < current {
		c.snapshotGate.Unlock()
		return 0, nil
	}

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
	source := int(c.holePunchCandidateSource % fileStoreHolePunchSourceCount)
	var candidateRanges [fileStoreHolePunchCandidateRuns][2]int
	var candidateSources [fileStoreHolePunchCandidateRuns]uint8
	var sourceCandidates [fileStoreHolePunchSourceCount]int
	var completed [fileStoreHolePunchCandidateWindow]bool
	candidateCount := 0

	// Probe one identity per source before partitioning the fixed discovery
	// budget. Equal fixed thirds strand two thirds of the work whenever pending
	// and absorbed are empty, which lets the reusable sweep fall behind sustained
	// churn even though the foreground bound has ample room. Three fixed probes
	// keep the decision bounded; the full 1,024-identity/64-run budget is then
	// divided only among sources that currently have work.
	var sourceActive [fileStoreHolePunchSourceCount]bool
	activeCount := 0
	for turn := 0; turn < fileStoreHolePunchSourceCount; turn++ {
		candidateSource := (source + turn) % fileStoreHolePunchSourceCount
		var probe [1]storeio.FreeExtent
		switch candidateSource {
		case 0:
			copied, next := appendHolePunchOffsetWindow(
				probe[:0], c.reusable, reusableCursor, 1,
			)
			sourceActive[candidateSource] = len(copied) != 0
			if !sourceActive[candidateSource] {
				reusableCursor = next
			}
		case 1:
			copied, next, _ := c.reclaimer.AppendPunchableAfter(
				probe[:0], current, fallback, pendingCursor, 1,
			)
			sourceActive[candidateSource] = len(copied) != 0
			if !sourceActive[candidateSource] {
				pendingCursor = next
			}
		case 2:
			copied, next := appendHolePunchIndexWindow(
				probe[:0], c.retirementAbsorbed, absorbedCursor, 1,
			)
			sourceActive[candidateSource] = len(copied) != 0
			if !sourceActive[candidateSource] {
				absorbedCursor = next
			}
		}
		if sourceActive[candidateSource] {
			activeCount++
		}
	}

	appendSource := func(
		candidateSource, identityLimit, rangeLimit int,
	) (identities, candidateRuns int, done bool) {
		if identityLimit <= 0 || rangeLimit <= 0 {
			return 0, 0, false
		}
		before := len(ranges)
		switch candidateSource {
		case 0:
			beforeCursor := reusableCursor
			ranges, reusableCursor = appendHolePunchOffsetWindow(
				ranges, c.reusable, reusableCursor, identityLimit,
			)
			done = reusableCursor == fileStoreHolePunchOffsetSweepDone
			for rank := before; rank < len(ranges); rank++ {
				completed[rank] = c.holePunchCompleted(ranges[rank])
			}
			kept, _ := fileStoreHolePunchCandidatePrefix(
				ranges[before:], completed[before:len(ranges)], rangeLimit,
			)
			if kept < len(ranges)-before {
				ranges = ranges[:before]
				reusableCursor = beforeCursor
				ranges, reusableCursor = appendHolePunchOffsetWindow(
					ranges, c.reusable, reusableCursor, kept,
				)
				for rank := before; rank < len(ranges); rank++ {
					completed[rank] = c.holePunchCompleted(ranges[rank])
				}
				done = false
			}
		case 1:
			beforeCursor := pendingCursor
			ranges, pendingCursor, done = c.reclaimer.AppendPunchableAfter(
				ranges, current, fallback, pendingCursor, identityLimit,
			)
			for rank := before; rank < len(ranges); rank++ {
				completed[rank] = c.holePunchCompleted(ranges[rank])
			}
			kept, _ := fileStoreHolePunchCandidatePrefix(
				ranges[before:], completed[before:len(ranges)], rangeLimit,
			)
			if kept < len(ranges)-before {
				ranges = ranges[:before]
				pendingCursor = beforeCursor
				ranges, pendingCursor, _ = c.reclaimer.AppendPunchableAfter(
					ranges, current, fallback, pendingCursor, kept,
				)
				for rank := before; rank < len(ranges); rank++ {
					completed[rank] = c.holePunchCompleted(ranges[rank])
				}
				done = false
			}
		case 2:
			beforeCursor := absorbedCursor
			ranges, absorbedCursor = appendHolePunchIndexWindow(
				ranges, c.retirementAbsorbed, absorbedCursor, identityLimit,
			)
			done = absorbedCursor >= uint64(len(c.retirementAbsorbed))
			for rank := before; rank < len(ranges); rank++ {
				completed[rank] = c.holePunchCompleted(ranges[rank])
			}
			kept, _ := fileStoreHolePunchCandidatePrefix(
				ranges[before:], completed[before:len(ranges)], rangeLimit,
			)
			if kept < len(ranges)-before {
				ranges = ranges[:before+kept]
				absorbedCursor = beforeCursor + uint64(kept)
				done = false
			}
		}
		identities = len(ranges) - before
		first, previous := -1, -1
		runBytes := uint64(0)
		for rank := before; rank <= len(ranges); rank++ {
			if rank != len(ranges) && completed[rank] {
				if first >= 0 {
					candidateRanges[candidateCount] = [2]int{first, rank}
					candidateSources[candidateCount] = uint8(candidateSource)
					candidateCount++
					candidateRuns++
					sourceCandidates[candidateSource]++
					first = -1
				}
				previous = -1
				runBytes = 0
				continue
			}
			if rank != len(ranges) && first < 0 {
				first, previous = rank, rank
				runBytes = ranges[rank].Length
				continue
			}
			if rank != len(ranges) && previous >= 0 &&
				fileStoreHolePunchRunContinues(
					ranges[previous], ranges[rank], runBytes,
				) {
				previous = rank
				runBytes += ranges[rank].Length
				continue
			}
			if first >= 0 {
				candidateRanges[candidateCount] = [2]int{first, rank}
				candidateSources[candidateCount] = uint8(candidateSource)
				candidateCount++
				candidateRuns++
				sourceCandidates[candidateSource]++
			}
			if rank != len(ranges) {
				first, previous = rank, rank
				runBytes = ranges[rank].Length
			}
		}
		return identities, candidateRuns, done
	}

	// Give every active source an equal first share. If one source exhausts its
	// window early, repeat with the unused identities/runs divided among the
	// survivors. The fixed three-round cap keeps mixed contiguous/fragmented
	// source shapes from turning quota redistribution into a long control loop;
	// any residual budget waits for the next physical generation. Global copy and
	// run totals never exceed the original fixed bounds.
	remainingIdentities := limit
	remainingRuns := fileStoreHolePunchCandidateRuns
	for round := 0; round < fileStoreHolePunchSourceCount &&
		activeCount != 0 && remainingIdentities != 0 && remainingRuns != 0; round++ {
		roundActive := activeCount
		roundIdentities := remainingIdentities
		roundRuns := remainingRuns
		activeRank := 0
		progress := false
		for turn := 0; turn < fileStoreHolePunchSourceCount; turn++ {
			candidateSource := (source + turn) % fileStoreHolePunchSourceCount
			if !sourceActive[candidateSource] {
				continue
			}
			identityShare := roundIdentities / roundActive
			if activeRank < roundIdentities%roundActive {
				identityShare++
			}
			runShare := roundRuns / roundActive
			if activeRank < roundRuns%roundActive {
				runShare++
			}
			activeRank++
			identities, runs, done := appendSource(
				candidateSource, identityShare, runShare,
			)
			remainingIdentities -= identities
			remainingRuns -= runs
			progress = progress || identities != 0 || runs != 0
			if done {
				sourceActive[candidateSource] = false
				activeCount--
			}
		}
		if !progress {
			break
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
	var retained [fileStoreHolePunchCandidateWindow]storeio.FreeExtent
	retainedCount := 0
	for rank := range len(ranges) {
		if completed[rank] {
			retained[retainedCount] = ranges[rank]
			retainedCount++
		}
	}
	clear(c.holePunchCompletions[:])
	c.holePunchCompletionVictim = 0
	for rank := range retainedCount {
		c.rememberHolePunchCompletion(retained[rank])
	}
	if len(ranges) == 0 {
		c.holePunchReusableCursor = reusableCursor
		c.holePunchPendingCursor = pendingCursor
		c.holePunchAbsorbedCursor = absorbedCursor
		c.holePunchCandidateSource = uint8(
			(source + 1) % fileStoreHolePunchSourceCount,
		)
		return current, nil
	}
	if candidateCount == 0 {
		c.holePunchReusableCursor = reusableCursor
		c.holePunchPendingCursor = pendingCursor
		c.holePunchAbsorbedCursor = absorbedCursor
		c.holePunchCandidateSource = uint8(
			(source + 1) % fileStoreHolePunchSourceCount,
		)
		clear(c.holePunchCompletions[:])
		return current, nil
	}

	layout, err := storeio.MutableStoreLayout(state.root.PageSize)
	if err != nil {
		return current, err
	}
	pageSize := uint64(state.root.PageSize)
	var sourceByteBudget [fileStoreHolePunchSourceCount]uint64
	var physical [fileStoreHolePunchCandidateRuns]storeio.FreeExtent
	var deferred [fileStoreHolePunchCandidateRuns]bool
	logicalFileEnd := state.fileEnd
	if logical := c.state.Load(); logical != nil &&
		logical.root.Generation >= state.root.Generation {
		logicalFileEnd = max(logicalFileEnd, logical.fileEnd)
	}
	for candidate := 0; candidate < candidateCount; candidate++ {
		first, last := candidateRanges[candidate][0], candidateRanges[candidate][1]
		for rank := first; rank < last; rank++ {
			validationFileEnd := state.fileEnd
			if candidateSources[candidate] != 1 {
				validationFileEnd = logicalFileEnd
			}
			if err := validateFileStoreHolePunchExtent(
				ranges[rank], layout.DataStart, pageSize, validationFileEnd,
			); err != nil {
				return current, err
			}
			if ranges[rank].Offset+ranges[rank].Length > state.fileEnd {
				// Reusable and exact-absorbed sources can legitimately contain
				// logical future-root debt. Keep their source window pinned and
				// continue reclaiming independent in-root sources. Pending reached
				// here only through the strict physical/fallback floor above, so its
				// out-of-root form was rejected by validation.
				deferred[candidate] = true
			}
		}
		extent := ranges[first]
		end := extent.Offset + extent.Length
		for rank := first + 1; rank < last; rank++ {
			next := ranges[rank]
			if next.Offset != end {
				return current, fmt.Errorf(
					"%w: non-adjacent hole-punch range %+v after end %d",
					storeio.ErrFreeLogCorrupt, next, end,
				)
			}
			end += next.Length
			extent.RetiredGeneration = max(
				extent.RetiredGeneration, next.RetiredGeneration,
			)
		}
		extent.Length = end - extent.Offset
		physical[candidate] = extent
	}
	// The three candidate sources are disjoint by allocator construction. Check
	// that invariant before the first destructive syscall rather than letting a
	// bookkeeping bug turn the fixed budget into overlapping duplicate work.
	for i := 0; i < candidateCount; i++ {
		iEnd := physical[i].Offset + physical[i].Length
		for j := 0; j < i; j++ {
			jEnd := physical[j].Offset + physical[j].Length
			if physical[i].Offset < jEnd && physical[j].Offset < iEnd {
				return current, fmt.Errorf(
					"%w: overlapping hole-punch candidates %+v and %+v",
					storeio.ErrFreeLogCorrupt, physical[j], physical[i],
				)
			}
		}
	}
	var activeSources [fileStoreHolePunchSourceCount]bool
	spendActiveCount := 0
	for candidate := 0; candidate < candidateCount; candidate++ {
		sourceIndex := int(candidateSources[candidate])
		if !deferred[candidate] && !activeSources[sourceIndex] {
			activeSources[sourceIndex] = true
			spendActiveCount++
		}
	}
	if spendActiveCount != 0 {
		budgetPages := fileStoreHolePunchSelectedBytes / pageSize
		activeRank := 0
		for turn := 0; turn < fileStoreHolePunchSourceCount; turn++ {
			candidateSource := (source + turn) % fileStoreHolePunchSourceCount
			if !activeSources[candidateSource] {
				continue
			}
			pages := budgetPages / uint64(spendActiveCount)
			if uint64(activeRank) < budgetPages%uint64(spendActiveCount) {
				pages++
			}
			sourceByteBudget[candidateSource] = pages * pageSize
			activeRank++
		}
	}

	// A source can expose more than one oversized identity in its fair window,
	// but one retained partial is enough: advance the existing partial when it is
	// present, otherwise the first oversized identity. Other identities stay
	// unresolved at the same exact source cursor.
	activeOversize := [fileStoreHolePunchSourceCount]int{-1, -1, -1}
	firstOversize := [fileStoreHolePunchSourceCount]int{-1, -1, -1}
	for candidate := 0; candidate < candidateCount; candidate++ {
		sourceIndex := int(candidateSources[candidate])
		if deferred[candidate] ||
			physical[candidate].Length <= sourceByteBudget[sourceIndex] {
			continue
		}
		first, last := candidateRanges[candidate][0], candidateRanges[candidate][1]
		if last != first+1 {
			return current, fmt.Errorf(
				"%w: oversized coalesced hole-punch candidate %+v",
				storeio.ErrFreeLogCorrupt, physical[candidate],
			)
		}
		if firstOversize[sourceIndex] < 0 {
			firstOversize[sourceIndex] = candidate
		}
		partial := c.holePunchPartials[sourceIndex]
		if partial.completed != 0 && partial.extent == ranges[first] {
			activeOversize[sourceIndex] = candidate
		}
	}
	for sourceIndex := range fileStoreHolePunchSourceCount {
		if activeOversize[sourceIndex] < 0 {
			activeOversize[sourceIndex] = firstOversize[sourceIndex]
		}
	}
	var partialCandidate [fileStoreHolePunchCandidateRuns]bool
	var partialFinishes [fileStoreHolePunchCandidateRuns]bool
	var partialProgress [fileStoreHolePunchCandidateRuns]uint64
	for candidate := 0; candidate < candidateCount; candidate++ {
		sourceIndex := int(candidateSources[candidate])
		if physical[candidate].Length <= sourceByteBudget[sourceIndex] {
			continue
		}
		if candidate != activeOversize[sourceIndex] {
			deferred[candidate] = true
			continue
		}
		first := candidateRanges[candidate][0]
		parent := ranges[first]
		progress := uint64(0)
		partial := c.holePunchPartials[sourceIndex]
		if partial.extent == parent && partial.completed < parent.Length {
			progress = partial.completed
		}
		chunk := min(parent.Length-progress, sourceByteBudget[sourceIndex])
		physical[candidate] = storeio.FreeExtent{
			Offset:            parent.Offset + progress,
			Length:            chunk,
			RetiredGeneration: parent.RetiredGeneration,
		}
		partialCandidate[candidate] = true
		partialProgress[candidate] = progress
		partialFinishes[candidate] = progress+chunk == parent.Length
	}
	// Put the largest ranges first without allocating. CandidateCount is fixed
	// above, so this bounded selection sort is cheaper than maintaining another
	// heap in the reader-fenced discovery pass.
	for left := 0; left < candidateCount; left++ {
		largest := left
		for right := left + 1; right < candidateCount; right++ {
			if physical[right].Length > physical[largest].Length {
				largest = right
			}
		}
		physical[left], physical[largest] = physical[largest], physical[left]
		candidateRanges[left], candidateRanges[largest] =
			candidateRanges[largest], candidateRanges[left]
		candidateSources[left], candidateSources[largest] =
			candidateSources[largest], candidateSources[left]
		deferred[left], deferred[largest] = deferred[largest], deferred[left]
		partialCandidate[left], partialCandidate[largest] =
			partialCandidate[largest], partialCandidate[left]
		partialFinishes[left], partialFinishes[largest] =
			partialFinishes[largest], partialFinishes[left]
		partialProgress[left], partialProgress[largest] =
			partialProgress[largest], partialProgress[left]
	}
	selected := 0
	selectedBytes := uint64(0)
	var attempted [fileStoreHolePunchCandidateRuns]bool
	var sourceResolved [fileStoreHolePunchSourceCount]int
	selectCandidate := func(candidate int) bool {
		extent := physical[candidate]
		first, last := candidateRanges[candidate][0], candidateRanges[candidate][1]
		if attempted[candidate] || deferred[candidate] ||
			selected == fileStoreHolePunchSelectedCalls ||
			extent.Length > fileStoreHolePunchSelectedBytes-selectedBytes {
			return true
		}
		attempted[candidate] = true
		constituents := ranges[first:last]
		if partialCandidate[candidate] {
			constituents = nil
		}
		if !c.punchFileStoreConstituents(extent, constituents) {
			return false
		}
		selected++
		selectedBytes += extent.Length
		sourceIndex := int(candidateSources[candidate])
		if !partialCandidate[candidate] {
			sourceResolved[sourceIndex]++
			return true
		}
		parent := ranges[first]
		if partialFinishes[candidate] {
			c.rememberHolePunchCompletion(parent)
			c.holePunchPartials[sourceIndex] = fileStoreHolePunchPartial{}
			sourceResolved[sourceIndex]++
			return true
		}
		c.holePunchPartials[sourceIndex] = fileStoreHolePunchPartial{
			extent: parent, completed: partialProgress[candidate] + extent.Length,
		}
		return true
	}

	// Reserve one call and the fair byte share for every active source before
	// redistributing spare work by descending range size.
	for turn := 0; turn < fileStoreHolePunchSourceCount; turn++ {
		candidateSource := (source + turn) % fileStoreHolePunchSourceCount
		for candidate := 0; candidate < candidateCount; candidate++ {
			if int(candidateSources[candidate]) != candidateSource ||
				deferred[candidate] ||
				physical[candidate].Length > sourceByteBudget[candidateSource] {
				continue
			}
			if !selectCandidate(candidate) {
				return current, nil
			}
			break
		}
	}
	for candidate := 0; candidate < candidateCount; candidate++ {
		if !selectCandidate(candidate) {
			return current, nil
		}
	}

	allResolved := true
	for sourceIndex := range fileStoreHolePunchSourceCount {
		if sourceResolved[sourceIndex] != sourceCandidates[sourceIndex] {
			allResolved = false
			continue
		}
		switch sourceIndex {
		case 0:
			c.holePunchReusableCursor = reusableCursor
		case 1:
			c.holePunchPendingCursor = pendingCursor
		case 2:
			c.holePunchAbsorbedCursor = absorbedCursor
		}
	}
	c.holePunchCandidateSource = uint8(
		(source + 1) % fileStoreHolePunchSourceCount,
	)
	if allResolved {
		clear(c.holePunchCompletions[:])
		c.holePunchCompletionVictim = 0
	}
	return current, nil
}

func validateFileStoreHolePunchExtent(
	extent storeio.FreeExtent,
	dataStart, pageSize, fileEnd uint64,
) error {
	if extent.Offset < dataStart || extent.Length == 0 ||
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
	if end > fileEnd || end > math.MaxInt64 {
		return fmt.Errorf(
			"%w: out-of-bounds hole-punch extent %+v fileEnd=%d",
			storeio.ErrFreeLogCorrupt, extent, fileEnd,
		)
	}
	return nil
}

// punchFileStoreExtent performs at most one successful deallocation operation
// for one durable free identity.
// Completion tracking is only an idempotence optimization: the exact source
// cursor advances after either a cache hit or a success, so eviction cannot
// affect convergence. Any optional platform failure disables this optimization
// for the process instead of repeatedly taxing later foreground boundaries.
func (c *Collection) punchFileStoreExtent(extent storeio.FreeExtent) bool {
	if c != nil && c.holePunchCompleted(extent) {
		return true
	}
	constituents := [1]storeio.FreeExtent{extent}
	return c.punchFileStoreConstituents(extent, constituents[:])
}

// punchFileStoreConstituents deallocates one adjacent physical union, then and
// only then records every exact source identity it covered. Recording the
// synthetic union would be wrong: reusing one constituent and re-retiring it at
// a newer generation must make that constituent visible again.
func (c *Collection) punchFileStoreConstituents(
	extent storeio.FreeExtent, constituents []storeio.FreeExtent,
) bool {
	if c == nil || c.holePunchDisabled {
		return false
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
	for _, constituent := range constituents {
		c.rememberHolePunchCompletion(constituent)
	}
	return true
}
