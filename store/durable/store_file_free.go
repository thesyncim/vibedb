package durable

import (
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// The durable free set, and why it is a log rather than a tree.
//
// Reclaimed space is only free once it is written down. The B+tree that used to
// hold it could persist exactly one edit per commit, because mutating a tree
// costs pages, allocating those pages changes the free set, and a changed free
// set changes the tree's shape. Everything a commit reclaimed past the first
// extent stayed in memory and was abandoned at the next restart, clean or not,
// so identical write volume consumed progressively more space across restarts.
// The measured reproduction lives in docs/performance.md.
//
// A flat log breaks that cycle because a record can describe its own
// allocation. Content is fixed once an extent is chosen, so d records need
// p = ceil(d/capacity) pages, allocating those adds at most p more records, and
// the recomputation converges. A commit therefore writes its complete diff, and
// the in-memory set and the durable set are the same set — which is the
// property the tests assert after every commit, because its violation is what
// corrupts.
//
// Two failure directions are not symmetric. Under-reporting free space leaks
// and is recoverable; over-reporting hands out space a live page occupies and
// is not. Every bound here therefore refuses to reclaim rather than refusing to
// be exact: a full arena stops taking extents, an oversized diff folds instead
// of truncating, and a replay that cannot prove its result is disjoint fails.
//
// What the log costs per commit is a second question, and it is the one that
// decides whether this scales. Folding used to rewrite the whole image, so the
// image had to be small enough to rewrite — sixteen pages, about 2,700 extents,
// roughly 11 MiB of trackable free space at the 4 KiB page size — and a store
// that fragmented past that could no longer write down space it had reclaimed.
// The image is now a set of independently addressed segments named by an index,
// a fold rewrites only the segments a commit changed, and what must fit inside
// one commit is a directory of the free set rather than the free set. See
// foldFreeLog for the mechanics and internal/storeio/free_index.go for the
// durable shape.

const (
	// freeReclaimBatch bounds how many retired extents one commit folds into the
	// free set. A maximum-width primary checkpoint retires about 509 extents, so
	// 512 lets the next foreground checkpoint service that complete fixed window
	// instead of accumulating roughly half of it on every cycle. The merge
	// scratch remains a fixed allocation. A batch whose exact delta does not fit
	// FreeLogMaxDeltaPages takes the already-bounded segmented fold path; the
	// reclaimer keeps whatever does not fit and offers it again next commit.
	freeReclaimBatch = 512
	// One reclaimed extent can coalesce with the durable neighbours on both
	// sides. Retain that complete worst-case representation until syncFreeLogFor
	// selects its bounded segmented fold. A second batch after an aborted
	// transaction still hits appendFreePending's conservative whole-image
	// fallback, keeping repeated-failure memory fixed.
	freePendingCapacity = 2*freeReclaimBatch + 1
	// freeLogMinFoldChain is the shortest chain worth folding. The fold point is
	// otherwise the size of the image, and an image of one page would then force
	// a fold every commit, doubling metadata writes to save four pages.
	freeLogMinFoldChain = 4
	// freeLogAllocationRounds bounds the delta-page fixed point before the
	// writer stops reusing space and appends the remainder instead. Two rounds
	// suffice in every case the arithmetic admits; the third exists so that
	// termination does not depend on that argument being right.
	freeLogAllocationRounds = 3
)

// freeLogCommit is what one commit decided to do to the durable free log. It is
// applied to in-memory bookkeeping only after Publish succeeds: an aborted
// transaction leaves the previously published chain on disk, and the store must
// go on describing the free set relative to that chain rather than to pages
// that were never written.
type freeLogCommit struct {
	head   storeio.PageRef
	inline *storeio.InlineFreeDelta
	// changed distinguishes "wrote a new chain head" from "the free set did not
	// move, so the published head still describes it".
	changed bool
	folded  bool
}

// restoreFencedExtents splits the replayed free set into what the allocator may
// hand out now and what is still fenced, and gives the fenced part back to the
// reclaimer.
//
// This is the other half of writing retirements down at the commit that makes
// them. The durable free set is the whole free set, including extents a reader
// or the alternate recovery root can still reach; only the generation stamp
// distinguishes them, and only the store applies it. Putting a fenced extent
// into c.reusable would hand it out — c.reusable is exactly the array the
// allocator scans and it carries no floor — so the split has to happen here,
// once, before anything can allocate.
//
// The floor is the same one refreshReusable uses every commit: an extent is
// reusable when no active reader and no recovery root can still name the
// generation that retired it. At open there are no readers, so the floor is the
// alternate superblock's generation, and everything the last session retired in
// its final two commits comes back fenced rather than free — which is exactly
// what it is.
func (c *Collection) restoreFencedExtents(state *fileStoreState, before int) error {
	floor := c.effectiveRecoveryFloor(state.root.Generation)
	fenced := c.freeReclaimed[:0]
	kept := c.reusable[:before]
	for _, extent := range c.reusable[before:] {
		if extent.RetiredGeneration >= floor {
			fenced = append(fenced, extent)
			continue
		}
		kept = append(kept, extent)
	}
	c.freeReclaimed = fenced
	if err := c.reclaimer.Restore(fenced); err != nil {
		return err
	}
	clear(c.reusable[len(kept):])
	c.reusable = kept
	return nil
}

// refreshReusable brings the in-memory free set up to date before a commit
// begins: it replays the durable log once per open, then folds in whatever the
// reclaimer can now prove no reader and no recovery root can still reach.
func (c *Collection) refreshReusable(state *fileStoreState) error {
	return c.refreshReusableFor(
		state, c.options.maxTransactionPages, c.freeFoldLimit,
	)
}

// refreshReusableFor applies the reservation geometry of the operation about
// to begin. Point mutations neither reserve the batch retirement headroom nor
// let reclamation dirty more free-image segments than their smaller fold can
// rewrite.
func (c *Collection) refreshReusableFor(
	state *fileStoreState, transactionPages, foldLimit int,
) error {
	if !c.freeLoaded {
		before := len(c.reusable)
		reusable, pages, err := storeio.ReplayInlineFreeLog(
			c.cache, &c.inlineFree,
			fileStoreFreeLogBounds(state),
			c.reusable, c.freeSetLimit, c.freeResidentBudget,
		)
		if err != nil {
			clear(c.reusable[before:])
			c.reusable = c.reusable[:before]
			return err
		}
		c.reusable = reusable
		if err := c.restoreFencedExtents(state, before); err != nil {
			clear(c.reusable[before:])
			c.reusable = c.reusable[:before]
			return err
		}
		if len(c.reusable) > before {
			// A no-op Flush may have completed an empty process-local sweep before
			// the durable free log is lazily replayed. Make the first replayed
			// reusable identity visible without discarding any earlier progress.
			c.rewindHolePunchReusable(c.reusable[before].Offset)
		}
		c.freeSegments = append(c.freeSegments[:0], pages.Segments...)
		c.freeResident = append(c.freeResident[:0], pages.Resident...)
		c.freeIndexPages = append(c.freeIndexPages[:0], pages.Index...)
		c.freeDeltaPages = append(c.freeDeltaPages[:0], pages.Delta...)
		c.resetFreeNonResident()
		// Every segment the replayed chain had records for is already stale on
		// disk: the records won, so the page they overrode no longer describes
		// what the store now holds. Marking them here is what lets the first fold
		// after an open rewrite exactly those and carry the rest by reference.
		c.resetFreeDirty()
		for _, record := range c.freePending {
			c.markFreeDirty(record.Extent.Offset)
		}
		if len(pages.Delta) != 0 {
			c.freeDirtyAll = true
		}
		if err := c.rebuildReusableIndex(); err != nil {
			return err
		}
		c.freeLoaded = true
	}
	if len(c.retirementAbsorbed) != 0 {
		slices.SortFunc(c.retirementAbsorbed, func(a, b storeio.FreeExtent) int {
			switch {
			case a.Offset < b.Offset:
				return -1
			case a.Offset > b.Offset:
				return 1
			default:
				return 0
			}
		})
		if err := c.mergeReusable(c.retirementAbsorbed); err != nil {
			return err
		}
		clear(c.retirementAbsorbed)
		c.retirementAbsorbed = c.retirementAbsorbed[:0]
		c.holePunchAbsorbedCursor = 0
	}
	durable := c.committer.DurableGeneration()
	c.cache.MarkDurable(durable)
	// Async publication can fill the retirement table behind the physical
	// committer even after the reader that caused the pressure has released its
	// lease. Waiting only when the remaining slots cannot hold one worst-case
	// transaction advances the recovery-root fence before this transaction
	// starts, so the ordinary drain below can make room without mutating an
	// in-flight allocator. This is pressure relief, not a steady-state fence:
	// the normal async path remains wait-free while it has configured headroom.
	//
	// A manual committer must not take this wait. Its worker deliberately stays
	// asleep until Flush authorizes a checkpoint, so waiting for a merely
	// published generation here would deadlock Snapshot while holding writer.
	// Manual publication also has a stronger no-reader proof available later:
	// PublishInlineRetiring can supersede and extract exact never-durable writes.
	// Let the exact RetireBatch admission at the end of preparation enforce the
	// bound on that lane; it fails before publication if the actual retirement
	// set (rather than this worst-case estimate) cannot fit.
	retired := c.reclaimer.Stats()
	leases := c.leases.Stats(state.root.Generation)
	reserve := uint64(min(transactionPages, int(retired.Capacity)))
	if c.options.Durability == DurabilityAsyncVisible &&
		leases.Active == 0 && retired.Pending > retired.Capacity-reserve &&
		durable < state.root.Generation {
		if err := c.waitPublished(state.root.Generation); err != nil {
			return err
		}
	}
	// Reclaim as many extents as the arena has room for, rather than declining
	// the batch whenever the pending set is larger than the room left. That
	// guard protected a real invariant — c.reusable is backed by a fixed
	// off-heap block and must never be reallocated onto the Go heap — but it
	// enforced it by doing nothing at all, which is self-defeating: this call
	// is the only drain of the pending set. Once it declined, the pending set
	// could only grow, so it never fell back under the bound, so the call
	// never resumed. It reached its own capacity a few commits later and
	// RetireBatch began failing every write, and releasing the snapshot that
	// started it did not recover the store — only restarting the process did,
	// abandoning every pending extent. The bound now limits how much moves
	// instead of whether anything moves.
	oldestRecovery := c.effectiveRecoveryFloor(state.root.Generation)
	batch, err := c.reclaimer.AppendReusable(
		c.freeReclaimed[:0], state.root.Generation, oldestRecovery,
		min(c.freeSetLimit-len(c.reusable), freeReclaimBatch),
	)
	if err != nil {
		return err
	}
	// Reclamation is the only thing that scatters changes across segments, and a
	// fold has to rewrite every segment one commit dirtied. Trimming the batch by
	// the segments it would dirty is therefore what keeps a fold bounded, and it
	// costs nothing: what does not fit stays pending and is offered again at the
	// next commit, exactly as an over-large batch already was.
	batch, err = c.trimBatchToFoldReserve(batch, foldLimit)
	if err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}
	slices.SortFunc(batch, func(a, b storeio.FreeExtent) int {
		if a.Offset < b.Offset {
			return -1
		}
		if a.Offset > b.Offset {
			return 1
		}
		return 0
	})
	c.freeReclaimed = batch
	return c.mergeReusable(batch)
}

func fileStoreFreeLogBounds(
	state *fileStoreState,
) storeio.FreeLogBounds {
	bounds := storeio.FreeLogBounds{
		FileEnd:       state.fileEnd,
		NextLogicalID: state.root.NextLogicalID,
	}
	if extent, ok := storeio.StateRootPageCatalogExtent(state.root); ok {
		bounds.ProtectedExtent = extent
	}
	return bounds
}

// trimBatchToFoldReserve drops the tail of an offset-sorted batch that would
// push the number of dirty segments past what one fold may rewrite.
//
// It counts segments rather than extents because the fold reserve is a page
// budget: a hundred reclaimed extents inside one segment cost one page, and
// two extents in two segments cost two. Dropping the tail rather than the whole
// batch matters for the same reason refreshReusable stopped declining batches
// outright — this call is the only drain of the pending set, and a drain that
// refuses to move anything is a drain that never resumes.
func (c *Collection) trimBatchToFoldReserve(
	batch []storeio.FreeExtent,
	foldLimit int,
) ([]storeio.FreeExtent, error) {
	if c.freeDirtyAll || len(c.freeSegments) == 0 {
		return batch, nil
	}
	// Half the reserve is left for the segments this commit's own allocations
	// dirty and for the splits a grown segment causes.
	budget := c.freeFoldPageLimitFor(foldLimit)/2 - c.freeDirtyCount
	if budget <= 0 {
		budget = 1
	}
	seen, last := 0, -1
	for i, extent := range batch {
		index := c.segmentOfFreeOffset(extent.Offset)
		if index == last || index >= 0 && index < len(c.freeDirty) && c.freeDirty[index] {
			continue
		}
		last = index
		seen++
		if seen > budget {
			// AppendReusable has already taken these out of the reclaimer, so the
			// tail has to be handed back rather than simply not used. Dropping it
			// would lose the extents outright: nothing else remembers them, which
			// is the same shape as the restart leak this subsystem just closed.
			if err := c.reclaimer.RetireBatch(batch[i:]); err != nil {
				return nil, err
			}
			return batch[:i], nil
		}
	}
	return batch, nil
}

// mergeReusable folds an offset-sorted batch of newly reclaimed extents into
// the offset-sorted free set and records the resulting durable diff.
//
// It coalesces across the whole set, not merely within the batch. The tree
// coalesced only within a batch because it could not afford to edit a neighbour
// it had not already touched; a free set that never re-merges fragments until
// no single extent can serve a document page, at which point a store with
// megabytes free grows the file anyway.
func (c *Collection) mergeReusable(batch []storeio.FreeExtent) error {
	head, count := len(c.reusable), len(batch)
	if count > cap(c.reusable)-head || head+count > c.freeSetLimit {
		return storeio.ErrRetiredExtentCapacity
	}
	// Everything strictly below, and not adjacent to, the batch's lowest extent
	// is untouched: the set is kept coalesced, so nothing before this point can
	// absorb or be absorbed by anything in the batch. Skipping that prefix makes
	// the cost proportional to the affected region instead of to the whole set.
	low, high := 0, head
	for low < high {
		middle := int(uint(low+high) >> 1)
		if c.reusable[middle].Offset+c.reusable[middle].Length < batch[0].Offset {
			low = middle + 1
		} else {
			high = middle
		}
	}
	base := low
	// A reclaimed range can merge with the existing predecessor selected at
	// base, producing one new identity whose offset is below batch[0]. Rewind
	// only to that first affected identity so the monotone foreground punch
	// sweep sees the new bytes without rescanning the stable prefix.
	affectedOffset := batch[0].Offset
	if base < head {
		affectedOffset = min(affectedOffset, c.reusable[base].Offset)
	}
	c.rewindHolePunchReusable(affectedOffset)
	c.reusable = c.reusable[:head+count]
	// Merge right to left, in place. The write cursor stays strictly above the
	// unread set-side index — w = i + j + 2 (+1 while an extent is held) with
	// j >= -1 — so output can overwrite the region the merge has already
	// consumed. That is what the batch's separate scratch buys: two adjacent
	// runs inside one array cannot be merged in place in either direction.
	i, j, w := head-1, count-1, head+count
	var held storeio.FreeExtent
	holding, durable, changed := false, false, false
	flush := func() {
		w--
		c.reusable[w] = held
		// A record is needed when the offset is new to the durable set or when
		// what lives at that offset has changed length. An extent that passed
		// through untouched is already on disk.
		if !durable || changed {
			c.appendFreePending(storeio.FreeDelta{Op: storeio.FreeOpSet, Extent: held})
		}
	}
	for i >= base || j >= 0 {
		var extent storeio.FreeExtent
		fromSet := j < 0 || i >= base && c.reusable[i].Offset > batch[j].Offset
		if fromSet {
			extent = c.reusable[i]
			i--
		} else {
			extent = batch[j]
			j--
		}
		if holding {
			end := extent.Offset + extent.Length
			// Overlap means the same range was reclaimed twice or was never
			// retired. Publishing it would advertise live space; refusing the
			// commit only stalls reclamation.
			if end > held.Offset {
				return fmt.Errorf("%w: overlapping reclaimed extents", storeio.ErrFreeLogCorrupt)
			}
			if end == held.Offset {
				if durable {
					// The absorbed extent's offset stops naming anything. Its
					// space is not lost: it is now the tail of the extent below.
					c.appendFreePending(storeio.FreeDelta{
						Op: storeio.FreeOpDelete, Extent: storeio.FreeExtent{Offset: held.Offset},
					})
				}
				held = storeio.FreeExtent{
					Offset: extent.Offset, Length: extent.Length + held.Length,
					RetiredGeneration: max(extent.RetiredGeneration, held.RetiredGeneration),
				}
				durable, changed = true, true
				continue
			}
			flush()
		}
		held, holding, durable, changed = extent, true, true, false
	}
	if holding {
		flush()
	}
	moved := copy(c.reusable[base:], c.reusable[w:head+count])
	clear(c.reusable[base+moved:])
	c.reusable = c.reusable[:base+moved]
	return c.rebuildReusableIndex()
}

// appendFreePending records a change the durable log does not yet carry. The
// list survives an aborted transaction, because refreshReusable's edits to the
// in-memory set are not rolled back and would otherwise never reach disk.
func (c *Collection) appendFreePending(delta storeio.FreeDelta) {
	// Once a fold is required the list is dead weight: the fold rebuilds every
	// segment straight from memory, so nothing accumulated here can still matter.
	// Overflowing into a fold is also what keeps the list bounded across a run
	// of consecutive aborts.
	//
	// Requiring a fold therefore has to dirty everything. The list is what told
	// the next fold which segments had moved, and a fold that dropped the list
	// without dirtying every segment would carry stale segment pages forward by
	// reference — publishing, as free, space the store had already taken back.
	if c.freeFoldRequired {
		return
	}
	c.markFreeDirty(delta.Extent.Offset)
	if len(c.freePending) == cap(c.freePending) {
		c.freeFoldRequired = true
		c.freeDirtyAll = true
		c.freePending = c.freePending[:0]
		return
	}
	c.freePending = append(c.freePending, delta)
}

func (c *Collection) syncFreeLogFor(
	tx *storeio.WriteTransaction, state *fileStoreState, foldLimit int,
) (freeLogCommit, error) {
	// The fold planner retains ranges into c.reusable and uses
	// freeImageScratch until every free-log page is staged. Lazy promotion may
	// reorder the resident slice and uses the same bounded scratch, so freeze
	// promotion here. Allocation from extents already made resident remains
	// indexed and enabled.
	tx.DisableReusablePromotion()
	c.freeNewSegments = c.freeNewSegments[:0]
	c.freeNewIndex = c.freeNewIndex[:0]
	c.freeNewDelta = c.freeNewDelta[:0]
	// The extents this commit retires are free space the moment it publishes,
	// fenced only by their generation, so they belong in the durable set now.
	// They used to live in the reclaimer's memory until the fence lifted, which
	// meant the last two commits of every session were never written down: an
	// open-write-close cycle abandoned a fixed three pages and the file grew
	// linearly with restart count while the content did not.
	c.markRetirementsDirty()
	deltas, err := c.appendFreeAllocationDeltas(
		append(c.freeDeltas[:0], c.freePending...), tx.ReuseEdits(), 0)
	if err != nil {
		return freeLogCommit{}, err
	}
	c.freeDeltas = c.appendRetirementDeltas(deltas)
	c.nextInlineFree = c.inlineFree
	// A commit that neither consumed nor reclaimed free space leaves the durable
	// set exactly as the published chain already describes it. Keeping the old
	// head is not a micro-optimisation: writing an empty delta every commit
	// would drive the chain to its fold threshold, and rewrite the whole image,
	// for no change at all.
	if !c.freeFoldRequired && len(c.freeDeltas) == 0 {
		return freeLogCommit{
			head: state.freeHead, inline: &c.nextInlineFree,
		}, nil
	}
	live := c.liveReusable()
	// A fold must rewrite every dirty segment in one commit, because the delta
	// chain it truncates is what described those segments' changes. So the
	// number of dirty segments is also a fold trigger: letting it drift past the
	// fold reserve would leave a commit that must fold and cannot.
	if c.freeFoldRequired ||
		c.freeDirtySegments() >= c.freeFoldPageLimitFor(foldLimit)/2 {
		return c.foldFreeLog(tx, state, live, foldLimit)
	}
	if err := c.nextInlineFree.Append(
		c.freeDeltas, uint32(c.options.PageSize), tx.FileEnd(),
	); err == nil {
		return freeLogCommit{
			head:   c.nextInlineFree.ExternalPrev(),
			inline: &c.nextInlineFree, changed: true,
		}, nil
	} else if !errors.Is(err, storeio.ErrInlineFreeDeltaFull) {
		return freeLogCommit{}, err
	}

	// Size a spill against the worst case before allocating anything. A commit
	// that discovered halfway through that it needed to fold would already own
	// unstaged delta pages and could not publish.
	room := storeio.FreeLogMaxChainPages - len(c.freeDeltaPages)
	need := freeLogPageCount(
		c.inlineFree.Len()+len(c.freeDeltas)+storeio.FreeLogMaxDeltaPages,
		c.freeDeltaPerPage,
	)
	if need > min(room, storeio.FreeLogMaxDeltaPages) ||
		len(c.freeDeltaPages)+need > c.freeFoldThreshold() {
		return c.foldFreeLog(tx, state, live, foldLimit)
	}
	c.freeSpill = c.freeSpill[:0]
	for rank := 0; rank < c.inlineFree.Len(); rank++ {
		delta, ok := c.inlineFree.DeltaAt(rank)
		if !ok {
			return freeLogCommit{}, storeio.ErrFreeLogCorrupt
		}
		c.freeSpill = append(c.freeSpill, delta)
	}
	return c.writeFreeDeltaChain(
		tx, c.inlineFree.ExternalPrev(), c.inlineFree.IndexHead(), 0, false,
		c.freeSpill,
	)
}

// foldFreeLog rewrites the segments this store has changed since the last fold,
// republishes the segment index, and starts a fresh one-link delta chain.
//
// It is the whole point of the segment index. The flat image was a linked list,
// so changing one extent changed the page holding it, which changed that page's
// offset, which changed the reference in the page before it: a fold rewrote
// every image page no matter how little had moved. That is why the image had to
// stay inside sixteen pages, and why the free set could hold only about 2,700
// extents — roughly 11 MiB of trackable free space at the 4 KiB page size,
// against the millions of extents a multi-terabyte store fragments into.
//
// Here a fold writes only the segments whose extents changed plus the index,
// and the index is smaller than the set it describes by the segment fan-out. So
// the per-commit cost stopped being a function of how much free space exists
// and became a function of how much of it this commit touched, which is what
// "O(1)-ish per commit" has to mean for a structure that must also be complete.
//
// Segment contents are dumped straight from c.reusable rather than replayed
// forward from the old chain. c.reusable is already the complete authoritative
// set; an incremental rebuild would only add a second, less direct way to be
// wrong, and it is the disagreement between the two that corrupts.
func (c *Collection) foldFreeLog(
	tx *storeio.WriteTransaction,
	state *fileStoreState,
	liveCount, foldLimit int,
) (freeLogCommit, error) {
	if liveCount == 0 && len(c.retireScratch) == 0 && c.reclaimer.PendingCount() == 0 {
		// An empty free set is fully described by publishing no free reference
		// at all: a replay of nothing is the empty set, which is the right
		// answer rather than a missing one.
		if err := c.retireFreeLogPages(state, true); err != nil {
			return freeLogCommit{}, err
		}
		c.nextInlineFree = storeio.NewInlineFreeDelta(storeio.PageRef{}, storeio.PageRef{})
		return freeLogCommit{
			inline: &c.nextInlineFree, changed: true, folded: true,
		}, nil
	}
	if c.freeImageScratchInUse {
		return freeLogCommit{}, fmt.Errorf(
			"%w: nested free-image planner", storeio.ErrFreeLogCorrupt,
		)
	}
	c.freeImageScratchInUse = true
	defer func() { c.freeImageScratchInUse = false }()
	// Retire the superseded pages before the image is built, not after. They are
	// free space the instant this commit publishes, so the segments this fold is
	// about to write have to contain them — a fold that retired afterwards left
	// its own predecessor undescribed until some later commit noticed, and at
	// Close there is no later commit.
	if err := c.retireFreeLogPages(state, true); err != nil {
		return freeLogCommit{}, err
	}
	bounds := fileStoreFreeLogBounds(state)
	live, err := c.buildFoldImage(bounds)
	if err != nil {
		return freeLogCommit{}, err
	}
	plan, live, err := c.planFreeFoldWithRepackFor(
		live, bounds, state, foldLimit,
	)
	if err != nil {
		return freeLogCommit{}, err
	}
	// The segment contents are the free set as it stood after every other
	// allocation this commit made and before any the fold makes. Everything the
	// fold itself consumes from here — segment pages, index pages, delta pages —
	// is therefore recorded in the fresh chain rather than being inside the
	// segments, which is the same self-describing trick the chain has always
	// used and the reason a fold needs no second commit to describe its own
	// allocation. Taking the mark before the first allocation rather than after
	// is what makes that true; taking it later would leave the segment pages'
	// own extents advertised as free by the very segments they were cut from.
	editStart := len(tx.ReuseEdits())
	if err := c.writeFreeSegments(tx, plan, live); err != nil {
		return freeLogCommit{}, err
	}
	indexHead, err := c.writeFreeIndex(tx)
	if err != nil {
		return freeLogCommit{}, err
	}
	return c.writeFreeDeltaChain(tx, storeio.PageRef{}, indexHead, editStart, true, nil)
}

// buildFoldImage assembles the complete durable free set: what the allocator may
// hand out, what is still fenced in the reclaimer, and what this commit has just
// retired.
//
// All three are free space; only the generation stamp distinguishes them, and a
// replay applies that stamp to decide which of the three a reopened store puts
// each extent back into. Leaving the fenced ones out of the image is exactly the
// restart leak: they existed only in the reclaimer's memory, so every Close threw
// away whatever the last two commits had retired.
//
// Entries an in-flight transaction consumed whole are zeroed rather than removed
// — finalizeReusable only compacts after Publish — and a zeroed entry has no
// offset, so they are dropped here rather than encoded as an extent at zero.
func (c *Collection) buildFoldImage(bounds storeio.FreeLogBounds) ([]storeio.FreeExtent, error) {
	fenced := c.reclaimer.AppendPending(c.freeFenced[:0])
	fenced = append(fenced, c.retireScratch...)
	fenced, err := c.loadDirtySegments(fenced, bounds)
	if err != nil {
		return nil, err
	}
	c.freeFenced = fenced
	slices.SortFunc(fenced, compareFreeExtentOffset)
	live := c.freeImageScratch[:0]
	i, j := 0, 0
	for i < len(c.reusable) || j < len(fenced) {
		var extent storeio.FreeExtent
		switch {
		case i < len(c.reusable) && c.reusable[i].Length == 0:
			i++
			continue
		case j == len(fenced) || i < len(c.reusable) && c.reusable[i].Offset < fenced[j].Offset:
			extent = c.reusable[i]
			i++
		default:
			extent = fenced[j]
			j++
		}
		if bounds.ExtentOverlapsProtected(extent) {
			return nil, fmt.Errorf(
				"%w: free-set fold overlaps canonical catalog",
				storeio.ErrFreeLogCorrupt,
			)
		}
		// Overlap between the three sources means the same range was retired
		// twice or was never live. Refusing the commit only stalls reclamation;
		// publishing it would advertise a live page as free.
		if len(live) != 0 {
			previous := live[len(live)-1]
			if previous.Offset+previous.Length > extent.Offset {
				return nil, fmt.Errorf("%w: overlapping free and retired extents prev=[%d+%d g%d] next=[%d+%d g%d]",
					storeio.ErrFreeLogCorrupt, previous.Offset, previous.Length, uint64(0), extent.Offset, extent.Length, uint64(0))
			}
		}
		live = append(live, extent)
	}
	c.freeImageScratch = live
	return live, nil
}

// loadDirtySegments reads the segments a fold must rewrite but the store has
// never looked inside.
//
// A segment becomes dirty without being resident in exactly one way: a
// retirement landed in the range it owns. The retired extent is in memory, but
// everything else the segment holds is only on disk, and a fold rebuilds a
// segment from the in-memory set — so rebuilding this one without reading it
// first would publish a segment holding one extent and silently drop the rest.
// Allocation and coalescing cannot create this case, because both require
// having read the segment already.
//
// It is bounded by the fold reserve, since that is what bounds how many segments
// one fold rewrites, and the pages are read through the ordinary cache.
func (c *Collection) loadDirtySegments(
	dst []storeio.FreeExtent, bounds storeio.FreeLogBounds,
) ([]storeio.FreeExtent, error) {
	c.freeReadBack = append(c.freeReadBack[:0], make([]bool, len(c.freeSegments))...)
	if c.freeDirtyAll {
		// Rebuilding everything from memory is only correct when memory holds
		// everything. Any segment the store never read has to be read now.
		for i := range c.freeSegments {
			if !c.freeResident[i] {
				c.freeDirty[i] = true
			}
		}
	}
	for i, segment := range c.freeSegments {
		if c.freeResident[i] || !c.freeDirtyAll && !c.freeDirty[i] {
			continue
		}
		lease, err := c.cache.Acquire(segment.Ref)
		if err != nil {
			return nil, err
		}
		view, err := storeio.OpenFreeImagePage(lease.Page(), bounds.FileEnd, bounds.NextLogicalID)
		if err != nil {
			lease.Release()
			return nil, err
		}
		for rank := range view.Len() {
			extent, ok := view.ExtentAt(rank)
			if !ok {
				lease.Release()
				return nil, storeio.ErrFreeLogCorrupt
			}
			dst = append(dst, extent)
		}
		lease.Release()
		c.freeReadBack[i] = true
	}
	return dst, nil
}

// anyFreeSegmentLoaded reports whether this fold had to read a segment it did
// not already hold, which decides whether a whole-set rebuild may claim to be
// resident afterwards.
func (c *Collection) anyFreeSegmentLoaded() bool {
	for _, loaded := range c.freeReadBack {
		if loaded {
			return true
		}
	}
	return false
}

func compareFreeExtentOffset(a, b storeio.FreeExtent) int {
	switch {
	case a.Offset < b.Offset:
		return -1
	case a.Offset > b.Offset:
		return 1
	default:
		return 0
	}
}

// freeFoldPlan is the segment layout one fold will publish, expressed as ranges
// of the in-memory free set. It is computed before anything is allocated
// because a fold that discovered halfway through that it needed one more page
// would already have staged pages it cannot now describe.
type freeFoldPlan struct {
	// rebuilt are half-open [lo,hi) ranges of c.reusable, one per new segment
	// page. carried names, for each entry, the published segment it replaces, or
	// -1 for a segment that is new.
	rebuilt [][2]int
	// slots maps each new segment page to its rank in the published order, so
	// clean segments can be spliced back in without being read.
	order []freeFoldSlot
}

// freeFoldSlot is one entry in the folded index: either a segment carried
// forward untouched, or one of the pages this fold is about to write.
type freeFoldSlot struct {
	carried  storeio.FreeSegment
	rebuilt  int
	fresh    bool
	resident bool
}

// planFreeFold decides which segments this fold rewrites and how the rewritten
// extents divide into pages.
//
// A dirty segment is rebuilt from whatever c.reusable now holds inside the
// offset range that segment owns, which may be more extents than one page holds
// — so it splits — or none at all — so it disappears. Neither case propagates:
// the index is an array that is rewritten whole at every fold, so adding or
// removing an entry costs nothing beyond the index rewrite already being paid.
// That is the property a B+tree could not have here, where an insert splits a
// node, the split allocates a page, the allocation changes the free set, and
// the changed free set may split again.
func (c *Collection) planFreeFold(live []storeio.FreeExtent) (freeFoldPlan, error) {
	return c.planFreeFoldFor(live, c.freeFoldLimit)
}

func (c *Collection) planFreeFoldFor(
	live []storeio.FreeExtent, foldLimit int,
) (freeFoldPlan, error) {
	plan := freeFoldPlan{rebuilt: c.freeFoldRanges[:0], order: c.freeFoldOrder[:0]}
	appendRebuilt := func(lo, hi int, fromDisk bool) error {
		// An empty range emits nothing: a dirty segment whose extents have all
		// been consumed simply stops existing, and allocating a page for it
		// would leave an unstaged page that fails publication.
		for start := lo; start < hi; start += c.freeImagePerPage {
			end := min(start+c.freeImagePerPage, hi)
			if len(plan.rebuilt) == c.freeFoldPageLimitFor(foldLimit) {
				return storeio.ErrRetiredExtentCapacity
			}
			plan.order = append(plan.order, freeFoldSlot{
				rebuilt: len(plan.rebuilt), fresh: true,
				// A segment rebuilt out of pages this fold had to read is not
				// resident afterwards: the extents went into the fold's scratch,
				// not into the allocator's array, so the next fold must read it
				// again rather than trust a span that does not describe it.
				resident: !fromDisk,
			})
			plan.rebuilt = append(plan.rebuilt, [2]int{start, end})
		}
		return nil
	}
	if c.freeDirtyAll || len(c.freeSegments) == 0 {
		if err := appendRebuilt(0, len(live), c.anyFreeSegmentLoaded()); err != nil {
			return freeFoldPlan{}, err
		}
		c.freeFoldRanges, c.freeFoldOrder = plan.rebuilt, plan.order
		return plan, nil
	}
	for i, segment := range c.freeSegments {
		lo, hi := c.segmentSpan(live, i)
		// A segment may be carried forward only if its durable page still says
		// what the in-memory set says. The dirty marks are the primary answer,
		// but they are maintained by every path that touches an extent, and a
		// path that forgets to mark publishes a stale segment — free space
		// overlapping a live page, discovered commits later. Comparing the
		// published extent count against the range the segment owns is a cheap
		// independent check that cannot be forgotten, because it is derived from
		// the set itself rather than maintained alongside it.
		// A segment the store has never read is carried forward unconditionally.
		// Its page is the whole truth about its own offset range and memory holds
		// none of it, so the count check below would compare the published count
		// against an empty span and rebuild the segment out of nothing — dropping
		// every extent it holds. Nothing can have changed inside it either:
		// allocating from an extent and coalescing one both require having read
		// it, and a retirement landing in its range marks it dirty, which is the
		// branch below.
		if !c.freeResident[i] && !c.freeDirty[i] {
			plan.order = append(plan.order, freeFoldSlot{carried: segment, resident: false})
			continue
		}
		if !c.freeDirty[i] && hi-lo == int(segment.Count) {
			plan.order = append(plan.order, freeFoldSlot{carried: segment, resident: true})
			continue
		}
		c.freeDirty[i] = true
		// Segment i owns [FirstOffset(i), FirstOffset(i+1)); the first segment
		// also owns everything below its own first offset, and the last owns
		// everything above, so the partition covers the whole file however far
		// the free set has moved since the index was written.
		if err := appendRebuilt(lo, hi, i < len(c.freeReadBack) && c.freeReadBack[i]); err != nil {
			return freeFoldPlan{}, err
		}
	}
	if len(plan.order) > storeio.FreeLogMaxIndexPages*c.freeIndexPerPage {
		return freeFoldPlan{}, storeio.ErrRetiredExtentCapacity
	}
	c.freeFoldRanges, c.freeFoldOrder = plan.rebuilt, plan.order
	return plan, nil
}

// planFreeFoldWithRepack preserves the cheap incremental plan when its carried
// and rebuilt slots fit the segment index, and falls back to one compact
// whole-image repartition when skew makes that layout exceed the format bound.
//
// A half-capacity free set can still occupy nearly every index slot: some
// segments may be full while many others contain one extent. If the full
// segments split, carrying the tiny segments verbatim can overflow the index
// even though repacking the same complete set would use roughly half as many
// pages. The fallback sets dirty-all before rebuilding the image, which makes
// loadDirtySegments read every clean nonresident segment. buildFoldImage also
// re-adds reusable, fenced, and newly retired extents, so compaction cannot
// silently omit space merely because the incremental plan had carried it.
func (c *Collection) planFreeFoldWithRepack(
	live []storeio.FreeExtent, bounds storeio.FreeLogBounds,
	state *fileStoreState,
) (freeFoldPlan, []storeio.FreeExtent, error) {
	return c.planFreeFoldWithRepackFor(
		live, bounds, state, c.freeFoldLimit,
	)
}

func (c *Collection) planFreeFoldWithRepackFor(
	live []storeio.FreeExtent, bounds storeio.FreeLogBounds,
	state *fileStoreState,
	foldLimit int,
) (freeFoldPlan, []storeio.FreeExtent, error) {
	plan, err := c.planFreeFoldFor(live, foldLimit)
	if !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		return plan, live, err
	}
	c.freeDirtyAll = true
	// The incremental plan would have carried clean segment pages by
	// reference. A whole-image repack replaces them too, so they become free
	// space and must enter both retireScratch and the image being rebuilt.
	if err := c.retireDirtyFreeSegments(state); err != nil {
		return freeFoldPlan{}, nil, err
	}
	live, err = c.buildFoldImage(bounds)
	if err != nil {
		return freeFoldPlan{}, nil, err
	}
	plan, err = c.planFreeFoldFor(live, foldLimit)
	return plan, live, err
}

func (c *Collection) freeFoldPageLimitFor(limit int) int {
	if limit < storeio.FreeLogMaxFoldSegments {
		return storeio.FreeLogMaxFoldSegments
	}
	return limit
}

// segmentSpan returns the half-open range of an offset-sorted set that published
// segment i owns. The first segment also owns everything below its own first
// offset and the last owns everything above, so the spans partition the set
// however far it has drifted from the index.
func (c *Collection) segmentSpan(set []storeio.FreeExtent, i int) (int, int) {
	lo := 0
	if i != 0 {
		lo = freeLowerBound(set, c.freeSegments[i].FirstOffset)
	}
	hi := len(set)
	if i+1 < len(c.freeSegments) {
		hi = freeLowerBound(set, c.freeSegments[i+1].FirstOffset)
	}
	return lo, hi
}

// freeLowerBound returns the first index in an offset-sorted set whose extent
// starts at or after offset.
func freeLowerBound(set []storeio.FreeExtent, offset uint64) int {
	lo, hi := 0, len(set)
	for lo < hi {
		middle := int(uint(lo+hi) >> 1)
		if set[middle].Offset < offset {
			lo = middle + 1
		} else {
			hi = middle
		}
	}
	return lo
}

func (c *Collection) rebuildReusableIndex() error {
	if c == nil ||
		!c.freeExtentIndex.Rebuild(c.reusable, c.freeExtentMaxima) {
		return fmt.Errorf("%w: reusable extent index capacity", storeio.ErrInvalidWrite)
	}
	return nil
}

func (c *Collection) resetFreeNonResident() {
	c.freeNonResident = 0
	for _, resident := range c.freeResident {
		if !resident {
			c.freeNonResident++
		}
	}
}

// fileReusablePromoter keeps the internal transaction hook off Collection's
// public method set without adding a wrapper allocation to each transaction.
type fileReusablePromoter Collection

func (c *Collection) reusableExtentPromoter() storeio.ReusableExtentPromoter {
	if c == nil || c.freeNonResident == 0 {
		return nil
	}
	return (*fileReusablePromoter)(c)
}

func (p *fileReusablePromoter) PromoteReusable(
	want, beforeOffset uint64, edits []storeio.ReuseEdit,
) ([]storeio.FreeExtent, bool, error) {
	return (*Collection)(p).promoteReusable(want, beforeOffset, edits)
}

// promoteReusable lazily reads the earliest nonresident free-image segment
// that can change exact lowest-offset first-fit for want. Clean resident
// segments may be evicted to make room; their immutable page remains the
// authoritative copy, so the arena stays bounded and the segment can be
// promoted again later. Dirty, partially fenced, and transaction-touched
// segments are never evicted.
func (c *Collection) promoteReusable(
	want, beforeOffset uint64, edits []storeio.ReuseEdit,
) ([]storeio.FreeExtent, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf(
			"%w: reusable promotion request", storeio.ErrInvalidWrite,
		)
	}
	if want == 0 {
		return c.reusable, false, fmt.Errorf(
			"%w: reusable promotion request", storeio.ErrInvalidWrite,
		)
	}
	if c.freeNonResident == 0 {
		return c.reusable, false, nil
	}
	if len(c.freeResident) != len(c.freeSegments) {
		return c.reusable, false, fmt.Errorf(
			"%w: reusable segment residency", storeio.ErrFreeLogCorrupt,
		)
	}
	changed := false
	for {
		target := -1
		for i, segment := range c.freeSegments {
			if segment.FirstOffset >= beforeOffset {
				break
			}
			if !c.freeResident[i] && segment.LargestFree >= want {
				target = i
				break
			}
		}
		if target < 0 {
			return c.reusable, changed, nil
		}
		// An attempted promotion may evict clean resident segments before a
		// later read/reclaimer error. Report the possible shape change so the
		// transaction remaps its rollback journal even on failure.
		changed = true
		if err := c.promoteFreeSegment(target, edits); err != nil {
			return c.reusable, changed, err
		}
		if rank, ok := c.freeExtentIndex.FirstFit(c.reusable, want); ok &&
			c.reusable[rank].Offset < beforeOffset {
			return c.reusable, changed, nil
		}
		// The index descriptor can name an extent that is still fenced by the
		// alternate recovery root. Once loaded it lives in the reclaimer, and
		// this loop advances to the next eligible segment without rereading it.
	}
}

func (c *Collection) promoteFreeSegment(
	segmentRank int, edits []storeio.ReuseEdit,
) error {
	if segmentRank < 0 || segmentRank >= len(c.freeSegments) ||
		segmentRank >= len(c.freeResident) || c.freeResident[segmentRank] {
		return fmt.Errorf("%w: reusable segment rank", storeio.ErrInvalidWrite)
	}
	state := c.state.Load()
	if state == nil {
		return storeio.ErrInvalidWrite
	}
	segment := c.freeSegments[segmentRank]
	lease, err := c.cache.Acquire(segment.Ref)
	if err != nil {
		return err
	}
	view, err := storeio.OpenFreeImagePage(
		lease.Page(), state.fileEnd, state.root.NextLogicalID,
	)
	if err != nil {
		lease.Release()
		return err
	}
	if view.Len() != int(segment.Count) {
		lease.Release()
		return fmt.Errorf(
			"%w: promoted segment count disagrees with index",
			storeio.ErrFreeLogCorrupt,
		)
	}
	loaded := c.freeImageScratch[:0]
	var previousEnd uint64
	nextOffset := ^uint64(0)
	if segmentRank+1 < len(c.freeSegments) {
		nextOffset = c.freeSegments[segmentRank+1].FirstOffset
	}
	for rank := range view.Len() {
		extent, ok := view.ExtentAt(rank)
		end := extent.Offset + extent.Length
		if !ok || rank == 0 && extent.Offset != segment.FirstOffset ||
			rank != 0 && previousEnd > extent.Offset ||
			end < extent.Offset || end > nextOffset {
			lease.Release()
			return fmt.Errorf(
				"%w: promoted segment extent order", storeio.ErrFreeLogCorrupt,
			)
		}
		loaded = append(loaded, extent)
		previousEnd = end
	}
	lease.Release()
	c.freeImageScratch = loaded
	defer func() {
		clear(c.freeImageScratch)
		c.freeImageScratch = c.freeImageScratch[:0]
	}()

	floor := c.effectiveRecoveryFloor(state.root.Generation)
	safe := loaded[:0]
	fenced := c.freeReclaimed[:0]
	for _, extent := range loaded {
		if extent.RetiredGeneration >= floor {
			fenced = append(fenced, extent)
		} else {
			safe = append(safe, extent)
		}
	}
	c.freeReclaimed = fenced
	if err := c.makeReusableRoom(len(safe), edits); err != nil {
		return err
	}
	for _, extent := range safe {
		at := freeLowerBound(c.reusable, extent.Offset)
		if at != 0 {
			previous := c.reusable[at-1]
			if previous.Length != 0 &&
				previous.Offset+previous.Length > extent.Offset {
				return fmt.Errorf(
					"%w: promoted reusable overlap", storeio.ErrFreeLogCorrupt,
				)
			}
		}
		if at != len(c.reusable) && c.reusable[at].Length != 0 &&
			extent.Offset+extent.Length > c.reusable[at].Offset {
			return fmt.Errorf(
				"%w: promoted reusable overlap", storeio.ErrFreeLogCorrupt,
			)
		}
	}
	if err := c.reclaimer.RetireBatch(fenced); err != nil {
		return err
	}
	for _, extent := range safe {
		at := freeLowerBound(c.reusable, extent.Offset)
		c.reusable = c.reusable[:len(c.reusable)+1]
		copy(c.reusable[at+1:], c.reusable[at:])
		c.reusable[at] = extent
	}
	c.freeResident[segmentRank] = true
	c.freeNonResident--
	return c.rebuildReusableIndex()
}

func (c *Collection) makeReusableRoom(
	count int, edits []storeio.ReuseEdit,
) error {
	if count < 0 || count > cap(c.reusable) {
		return storeio.ErrRetiredExtentCapacity
	}
	changed := false
	for cap(c.reusable)-len(c.reusable) < count {
		evict := -1
		evictLo, evictHi := 0, 0
		for i := len(c.freeSegments) - 1; i >= 0; i-- {
			if !c.freeResident[i] || i < len(c.freeDirty) && c.freeDirty[i] ||
				c.freeSegmentProtected(i, edits) {
				continue
			}
			lo, hi := c.segmentSpan(c.reusable, i)
			// A clean page can replace memory only when every extent it names
			// is in the resident set. A smaller span means some are fenced in
			// the reclaimer and rereading would duplicate them.
			if hi-lo != int(c.freeSegments[i].Count) {
				continue
			}
			evict, evictLo, evictHi = i, lo, hi
			break
		}
		if evict < 0 {
			if changed {
				_ = c.rebuildReusableIndex()
			}
			return storeio.ErrRetiredExtentCapacity
		}
		copy(c.reusable[evictLo:], c.reusable[evictHi:])
		clear(c.reusable[len(c.reusable)-(evictHi-evictLo):])
		c.reusable = c.reusable[:len(c.reusable)-(evictHi-evictLo)]
		c.freeResident[evict] = false
		c.freeNonResident++
		changed = true
	}
	if changed {
		return c.rebuildReusableIndex()
	}
	return nil
}

func (c *Collection) freeSegmentProtected(
	segmentRank int, edits []storeio.ReuseEdit,
) bool {
	for _, edit := range edits {
		if c.segmentOfFreeOffset(edit.Before.Offset) == segmentRank {
			return true
		}
	}
	return false
}

// writeFreeSegments allocates and encodes the segment pages the plan calls for,
// and leaves c.freeNewSegments holding the complete published order: carried
// descriptors unchanged, rewritten ones pointing at the pages just staged.
func (c *Collection) writeFreeSegments(
	tx *storeio.WriteTransaction, plan freeFoldPlan, live []storeio.FreeExtent,
) error {
	pages := c.freeFoldPages[:0]
	defer func() {
		clear(pages)
		c.freeFoldPages = pages[:0]
	}()
	for range plan.rebuilt {
		page, err := tx.Allocate(storeio.PageFreeImage, uint32(c.options.PageSize), 0)
		if err != nil {
			return err
		}
		pages = append(pages, page)
	}
	c.freeNewSegments = c.freeNewSegments[:0]
	c.freeNewResident = c.freeNewResident[:0]
	for orderRank, slot := range plan.order {
		if !slot.fresh {
			c.freeNewSegments = append(c.freeNewSegments, slot.carried)
			// A carried segment keeps whatever residency it had. Carrying is
			// precisely the case where the store did not need to look inside it.
			c.freeNewResident = append(c.freeNewResident, slot.resident)
			continue
		}
		span := plan.rebuilt[slot.rebuilt]
		extents := live[span[0]:span[1]]
		page := pages[slot.rebuilt]
		if _, err := storeio.EncodeFreeImagePage(page.Bytes(), storeio.FreeLogHeader{
			StoreID: c.storeID, Generation: tx.Generation(),
			LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length,
		}, extents, tx.FileEnd(), tx.NextLogicalID()); err != nil {
			return err
		}
		if err := page.Stage(); err != nil {
			return err
		}
		largest := uint64(0)
		for _, extent := range extents {
			largest = max(largest, extent.Length)
		}
		c.freeNewSegments = append(c.freeNewSegments, storeio.FreeSegment{
			Ref: page.Ref(), FirstOffset: extents[0].Offset,
			LargestFree: largest, Count: uint32(len(extents)),
		})
		c.freeNewResident = append(c.freeNewResident, plan.order[orderRank].resident)
	}
	return nil
}

// writeFreeIndex publishes the whole segment index and returns its newest page.
//
// The index is rewritten in full even though only some segments moved. That is
// the deliberate stopping point of this design: at 4 KiB the index is smaller
// than the free set by the segment fan-out — 165 extents per segment and 70
// segments per index page — so rewriting all of it costs a small fraction of
// rewriting the image, and stopping here avoids a third level whose only job
// would be to make this rewrite partial too.
func (c *Collection) writeFreeIndex(tx *storeio.WriteTransaction) (storeio.PageRef, error) {
	segments := c.freeNewSegments
	if len(segments) == 0 {
		return storeio.PageRef{}, nil
	}
	pageCount := freeLogPageCount(len(segments), c.freeIndexPerPage)
	if pageCount > storeio.FreeLogMaxIndexPages {
		return storeio.PageRef{}, storeio.ErrRetiredExtentCapacity
	}
	c.freeNewIndex = c.freeNewIndex[:0]
	var prev storeio.PageRef
	for i := range pageCount {
		lower := i * c.freeIndexPerPage
		upper := min(lower+c.freeIndexPerPage, len(segments))
		page, err := tx.Allocate(storeio.PageFreeIndex, uint32(c.options.PageSize), 0)
		if err != nil {
			return storeio.PageRef{}, err
		}
		if _, err := storeio.EncodeFreeIndexPage(page.Bytes(), storeio.FreeLogHeader{
			StoreID: c.storeID, Generation: tx.Generation(),
			LogicalID: page.Ref().LogicalID, PageSize: page.Ref().Length,
		}, segments[lower:upper], prev, tx.FileEnd(), tx.NextLogicalID()); err != nil {
			return storeio.PageRef{}, err
		}
		if err := page.Stage(); err != nil {
			return storeio.PageRef{}, err
		}
		c.freeNewIndex = append(c.freeNewIndex, page.Ref())
		prev = page.Ref()
	}
	return prev, nil
}

// writeFreeDeltaChain allocates the diff's pages and then encodes them, in that
// order. That is the whole trick: a delta page allocated before its content is
// fixed can record its own allocation, so the diff is complete without a second
// commit to describe the first one. p pages hold d records, allocating them
// adds at most p records, and recomputing converges — a shape a tree cannot
// have, because writing a tree may split it, splitting allocates, and
// allocating changes the shape again.
func (c *Collection) writeFreeDeltaChain(
	tx *storeio.WriteTransaction, prev, indexHead storeio.PageRef, editStart int, folded bool,
	prefix []storeio.FreeDelta,
) (freeLogCommit, error) {
	var pages [storeio.FreeLogMaxDeltaPages]storeio.TransactionPage
	allocated, rounds := 0, 0
	for {
		deltas := append(c.freeDeltas[:0], prefix...)
		// A fold's segments already carry everything the pending list and this
		// commit's retirements described, so replaying them on top would restate
		// settled facts and could only disagree with them.
		if !folded {
			deltas = append(deltas, c.freePending...)
		}
		deltas, err := c.appendFreeAllocationDeltas(deltas, tx.ReuseEdits(), editStart)
		if err != nil {
			return freeLogCommit{}, err
		}
		// Retirements go last so that they win. An offset can appear both as an
		// extent this commit allocated from — recorded as a delete or a shortened
		// set — and as a page this commit retired, and the retirement is the
		// truth at commit end: the page is dead and its bytes are free. Ordering
		// them earlier let an allocation record for the same offset supersede the
		// retirement, which lost the extent silently and permanently, since a
		// later fold rebuilds segments from a set that no longer mentions it.
		if !folded {
			deltas = c.appendRetirementDeltas(deltas)
		}
		c.freeDeltas = deltas
		need := max(1, freeLogPageCount(len(deltas), c.freeDeltaPerPage))
		if need <= allocated {
			break
		}
		if need > storeio.FreeLogMaxDeltaPages {
			return freeLogCommit{}, storeio.ErrRetiredExtentCapacity
		}
		for allocated < need {
			page, allocErr := tx.Allocate(storeio.PageFreeDelta, uint32(c.options.PageSize), 0)
			if allocErr != nil {
				return freeLogCommit{}, allocErr
			}
			pages[allocated] = page
			allocated++
		}
		rounds++
		if rounds == freeLogAllocationRounds {
			// Appending consumes no free extent, so it adds no record and the
			// next round is guaranteed to be the last. Termination stops
			// depending on the convergence argument being right.
			tx.DisableReuse()
		}
	}
	for i := range allocated {
		lower := min(i*c.freeDeltaPerPage, len(c.freeDeltas))
		upper := min(lower+c.freeDeltaPerPage, len(c.freeDeltas))
		link := prev
		if i != 0 {
			link = pages[i-1].Ref()
		}
		if _, err := storeio.EncodeFreeDeltaPage(pages[i].Bytes(), storeio.FreeLogHeader{
			StoreID: c.storeID, Generation: tx.Generation(),
			LogicalID: pages[i].Ref().LogicalID, PageSize: pages[i].Ref().Length,
		}, c.freeDeltas[lower:upper], link, indexHead, tx.FileEnd(), tx.NextLogicalID()); err != nil {
			return freeLogCommit{}, err
		}
		if err := pages[i].Stage(); err != nil {
			return freeLogCommit{}, err
		}
		c.freeNewDelta = append(c.freeNewDelta, pages[i].Ref())
	}
	head := pages[allocated-1]
	c.nextInlineFree = storeio.NewInlineFreeDelta(head.Ref(), indexHead)
	return freeLogCommit{
		head:    head.Ref(),
		inline:  &c.nextInlineFree,
		changed: true, folded: folded,
	}, nil
}

// appendRetirementDeltas states this commit's retirements as free-set records.
//
// They are ordinary FreeOpSet records carrying the outgoing generation, which is
// the fence: a replay puts an extent whose retired generation is at or above the
// recovery floor back into the reclaimer rather than into the allocator's array.
// Nothing else distinguishes them, and nothing else needs to — an extent is free
// space from the instant the commit that retires it publishes, and the only
// question is who may take it.
func (c *Collection) appendRetirementDeltas(dst []storeio.FreeDelta) []storeio.FreeDelta {
	for _, extent := range c.retireScratch {
		dst = append(dst, storeio.FreeDelta{Op: storeio.FreeOpSet, Extent: extent})
	}
	return dst
}

// markRetirementsDirty routes each retired extent to the segment that will have
// to describe it. A retirement recorded in the chain but not marked would be
// dropped by the next fold, which truncates the chain and rebuilds only the
// segments it believes changed.
func (c *Collection) markRetirementsDirty() {
	for _, extent := range c.retireScratch {
		c.markFreeDirty(extent.Offset)
	}
}

// appendFreeAllocationDeltas records what this transaction took from the free
// set. edits before start are already reflected in a base image written during
// this same commit and must not be restated.
func (c *Collection) appendFreeAllocationDeltas(
	dst []storeio.FreeDelta, edits []storeio.ReuseEdit, start int,
) ([]storeio.FreeDelta, error) {
	if start >= len(edits) {
		return dst, nil
	}
	c.freeAllocStamp++
	if c.freeAllocStamp == 0 {
		clear(c.freeAllocMark)
		c.freeAllocStamp = 1
	}
	// Walk the journal newest first and keep one record per extent. An extent
	// allocated from several times in one commit needs a single record saying
	// where it ended up, not one per allocation.
	for i := len(edits) - 1; i >= start; i-- {
		index := int(edits[i].Index)
		if index >= len(c.freeAllocMark) || index >= len(c.reusable) {
			return dst, fmt.Errorf("%w: reuse journal index", storeio.ErrInvalidWrite)
		}
		if c.freeAllocMark[index] == c.freeAllocStamp {
			continue
		}
		c.freeAllocMark[index] = c.freeAllocStamp
		if current := c.reusable[index]; current.Length != 0 {
			c.markFreeDirty(current.Offset)
			dst = append(dst, storeio.FreeDelta{Op: storeio.FreeOpSet, Extent: current})
			continue
		}
		c.markFreeDirty(edits[i].Before.Offset)
		// The extent was consumed whole. Its transaction-local entry retains the
		// offset so lazy promotion can remap rollback ranks; the journal remains
		// authoritative and finalize removes the zero-length entry.
		dst = append(dst, storeio.FreeDelta{
			Op: storeio.FreeOpDelete, Extent: storeio.FreeExtent{Offset: edits[i].Before.Offset},
		})
	}
	return dst, nil
}

// retireFreeLogPages hands the superseded pages to the reclaimer and folds them
// into the free set this same commit is writing.
//
// Only the segments this fold actually rewrote are retired. A segment carried
// forward by reference is still live — it is named by the new index — and
// retiring it would advertise as free the very bytes the next open reads the
// free set out of. That distinction is new with the segment index: when the
// image was a linked list every fold replaced every page, so retiring the whole
// image was correct by construction and there was nothing to get wrong.
//
// The fixed point is the price of that. Retiring a page makes it free, a free
// extent has to be described by the segment that owns its offset, and that
// segment may have been clean — so retiring dirties it, which retires its page,
// which may dirty another. Each round can only add segments and there are
// finitely many, so it terminates; the bound exists so that termination does not
// depend on that argument being right, and falling back to rebuilding every
// segment is always correct because the whole set is in memory.
//
// Retiring at the outgoing generation fences these pages exactly as state roots
// are fenced: they stay reserved until neither an active reader nor the
// alternate superblock can still name the generation that referenced them.
func (c *Collection) retireFreeLogPages(state *fileStoreState, folded bool) error {
	if !folded {
		return nil
	}
	for _, ref := range c.freeIndexPages {
		if err := c.appendFreeLogRetirement(state, ref); err != nil {
			return err
		}
	}
	for _, ref := range c.freeDeltaPages {
		if err := c.appendFreeLogRetirement(state, ref); err != nil {
			return err
		}
	}
	c.freeRetired = append(c.freeRetired[:0], make([]bool, len(c.freeSegments))...)
	return c.retireDirtyFreeSegments(state)
}

func (c *Collection) appendFreeLogRetirement(
	state *fileStoreState, ref storeio.PageRef,
) error {
	if state == nil {
		return storeio.ErrInvalidWrite
	}
	if len(c.retireScratch) == cap(c.retireScratch) {
		return storeio.ErrRetiredExtentCapacity
	}
	c.retireScratch = append(c.retireScratch, storeio.FreeExtent{
		Offset: ref.Offset, Length: uint64(ref.Length),
		RetiredGeneration: state.root.Generation,
	})
	c.rememberRetiredRef(ref)
	c.markFreeDirty(ref.Offset)
	return nil
}

// retireDirtyFreeSegments advances the self-retirement fixed point without
// restating the index and delta pages already appended by retireFreeLogPages.
// The repack fallback calls it a second time after setting dirty-all so every
// formerly carried segment page is retired exactly once.
func (c *Collection) retireDirtyFreeSegments(state *fileStoreState) error {
	if len(c.freeRetired) != len(c.freeSegments) {
		return storeio.ErrInvalidWrite
	}
	for round := 0; ; round++ {
		added := false
		for i, segment := range c.freeSegments {
			if c.freeRetired[i] || !c.freeDirtyAll && !c.freeDirty[i] {
				continue
			}
			c.freeRetired[i] = true
			added = true
			if err := c.appendFreeLogRetirement(state, segment.Ref); err != nil {
				return err
			}
		}
		if !added {
			return nil
		}
		if round == len(c.freeSegments) {
			c.freeDirtyAll = true
		}
	}
}

// commitFreeLog adopts a published commit's chain and drops the diff it carried
// to disk. Nothing here may run before Publish returns.
func (c *Collection) commitFreeLog(commit freeLogCommit) {
	if !commit.changed {
		return
	}
	if commit.folded {
		c.freeSegments = append(c.freeSegments[:0], c.freeNewSegments...)
		c.freeResident = append(c.freeResident[:0], c.freeNewResident...)
		c.freeIndexPages = append(c.freeIndexPages[:0], c.freeNewIndex...)
		c.freeDeltaPages = append(c.freeDeltaPages[:0], c.freeNewDelta...)
		c.resetFreeNonResident()
		c.resetFreeDirty()
		c.freeFoldRequired = false
	} else {
		c.freeDeltaPages = append(c.freeDeltaPages, c.freeNewDelta...)
	}
	c.freePending = c.freePending[:0]
}

// resetFreeDirty declares every published segment to match the in-memory set,
// which is true exactly after a fold has written the ones that did not.
func (c *Collection) resetFreeDirty() {
	c.freeDirty = append(c.freeDirty[:0], make([]bool, len(c.freeSegments))...)
	c.freeDirtyCount = 0
	c.freeDirtyAll = len(c.freeSegments) == 0
	if len(c.freeResident) != len(c.freeSegments) {
		// A fold republishes the whole index, so residency has to be restated
		// against the new segment list. Everything a fold wrote it also read, and
		// everything it carried forward it did not: the two lists are rebuilt
		// together in commitFreeLog, and this is the fallback for the paths that
		// do not go through it.
		c.freeResident = append(c.freeResident[:0], make([]bool, len(c.freeSegments))...)
	}
}

// segmentOfFreeOffset returns the published segment that owns offset, or -1 when
// no index has been published yet.
//
// The first segment owns everything below its own first offset and the last
// owns everything above, so every offset in the file has exactly one owner
// however far the free set has drifted from the index. Routing a change to one
// owner is what lets a fold rewrite one segment instead of all of them.
func (c *Collection) segmentOfFreeOffset(offset uint64) int {
	if len(c.freeSegments) == 0 {
		return -1
	}
	lo, hi := 0, len(c.freeSegments)
	for lo < hi {
		middle := int(uint(lo+hi) >> 1)
		if c.freeSegments[middle].FirstOffset <= offset {
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

// markFreeDirty records that the segment owning offset no longer matches its
// durable page. Marking is deliberately conservative: a segment marked without
// having changed costs one page rewrite at the next fold, while a segment that
// changed without being marked is published stale, and a stale segment
// advertises space a live page occupies.
func (c *Collection) markFreeDirty(offset uint64) {
	if c.freeDirtyAll {
		return
	}
	index := c.segmentOfFreeOffset(offset)
	if index < 0 {
		c.freeDirtyAll = true
		return
	}
	if index < len(c.freeDirty) && !c.freeDirty[index] {
		c.freeDirty[index] = true
		c.freeDirtyCount++
	}
}

// freeDirtySegments reports how many segments the next fold would rewrite.
func (c *Collection) freeDirtySegments() int {
	if c.freeDirtyAll {
		return max(1, freeLogPageCount(len(c.reusable), c.freeImagePerPage))
	}
	return c.freeDirtyCount
}

// finalizeReusable drops the entries this commit consumed whole. Their removal
// is already on disk as a delete record, so the in-memory set and the durable
// set agree again the moment this returns.
func (c *Collection) finalizeReusable() {
	out := c.reusable[:0]
	for _, extent := range c.reusable {
		if extent.Length != 0 {
			out = append(out, extent)
		}
	}
	clear(c.reusable[len(out):])
	c.reusable = out
	if !c.freeExtentIndex.Rebuild(c.reusable, c.freeExtentMaxima) {
		// Construction sizes the maxima arena from cap(c.reusable), so this can
		// only fail after an internal slice-capacity violation. Publication has
		// already succeeded; leave an invalid index that makes the next writer
		// fail closed rather than silently falling back to an inexact lookup.
		c.freeExtentIndex = storeio.FreeExtentIndex{}
	}
}

func (c *Collection) liveReusable() int {
	live := 0
	for _, extent := range c.reusable {
		if extent.Length != 0 {
			live++
		}
	}
	return live
}

// freeFoldThreshold is the chain length past which folding is worth its cost.
//
// A delta chain is live space and is read in full at every open, so its cost
// grows with every commit; a fold's cost does not. Break-even is therefore the
// size of the write the fold would make: once the chain is longer than the
// dirty segments plus the index that would replace it, the chain is pure
// overhead.
//
// This is where the segment index changes the arithmetic rather than merely the
// cost. The old threshold was the size of the whole image, because the whole
// image was what a fold rewrote, so a store with a large free set folded rarely
// and paid a large write when it did. The fold write is now bounded by the fold
// reserve however large the free set is, so the threshold no longer grows with
// the store and a terabyte store folds as cheaply as a megabyte one.
func (c *Collection) freeFoldThreshold() int {
	return min(storeio.FreeLogMaxChainPages,
		max(freeLogMinFoldChain, c.freeDirtySegments()+len(c.freeIndexPages)))
}

func freeLogPageCount(records, perPage int) int {
	if records <= 0 || perPage <= 0 {
		return 0
	}
	return (records + perPage - 1) / perPage
}
