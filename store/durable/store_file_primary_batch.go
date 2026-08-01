package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
)

// ErrPrimaryBatchUnsupportedLane reports an Update on an ordered-primary
// collection whose durability lane publishes through the committer generation
// fence rather than the canonical dirty frames a primary batch commits on. That
// is DurabilityAsyncVisible and the journal-less synchronous lane reached by
// reopening an async-created store as DurabilitySync. The three logical lanes a
// primary batch supports — buffered-visible, buffered-visible+journal, and
// sync-journal — all take the deferred canonical path, where one generation
// carries the whole group.
var ErrPrimaryBatchUnsupportedLane = errors.New(
	"vibejson: ordered primary Update requires a buffered-visible or sync-journal lane",
)

// primaryBatchMutation is one resolved key in a primary Update: its resident
// route and document bytes (nil for a delete). Keys and values borrow the
// WriteBatch arena, stable for the whole Update.
type primaryBatchMutation struct {
	key      []byte
	value    []byte
	resident storeio.ResidentPrimaryRoute
	remove   bool
}

// primaryBatchLeaf accumulates every mutation a batch routes to one leaf and the
// single rewritten frame they fold into. One frame is admitted per touched leaf,
// no matter how many of the batch's documents land in it: N puts into one leaf
// rewrite the leaf once, not N times.
type primaryBatchLeaf struct {
	resident     storeio.ResidentPrimaryRoute
	firstKey     []byte
	pending      filePrimaryPendingParent
	pendingIndex int
	nextLeaf     storeio.PageRef
	imageOffset  int
	imageLength  int
	applied      int
	frameGen     uint64
	initialLen   int
	finalLen     int
	docDelta     int
	mutationAt   int
	mutationEnd  int
	skip         bool
}

// updatePrimaryBatch applies one WriteBatch to the ordered primary graph as one
// logical failure-atomic publication. It is the engine of Collection.Update.
//
// Every document is routed and its final leaf frame is prepared before the
// logical batch becomes durable. If that final image does not fit, one
// content-equivalent topology generation prepares all required ranges and the
// batch is replanned. One journal record then covers the complete logical group
// with one sync, and every logical leaf change publishes in one generation, so a
// reader sees all of the batch or none of it. A failure after topology preparation
// can leave logical content unchanged while Generation advances.
func (c *Collection) updatePrimaryBatch(fn func(*WriteBatch) error) (err error) {
	if c == nil {
		return ErrClosed
	}
	c.writer.Lock()
	var journalTarget uint64
	defer func() {
		// The buffered-journal lane deposited the batch's single redo record under
		// the writer; release the writer, then share one journal sync across
		// concurrent Update/Put callers on the group fence (phase 1 group commit).
		groupWait := journalTarget != 0
		if groupWait {
			c.durabilityWait.Add(1)
		}
		c.writer.Unlock()
		if groupWait {
			err = errors.Join(err, c.journalGroupAwait(journalTarget))
			c.durabilityWait.Done()
		}
	}()
	if c.closed {
		return ErrClosed
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	if !c.deferredCanonicalLane() {
		return ErrPrimaryBatchUnsupportedLane
	}
	if c.primaryUnifiedOverlay.hasPending() {
		if err := c.materializePrimaryParentsLocked(); err != nil {
			return err
		}
		c.primaryOverlayFolds.Add(1)
	}
	batch := c.fileWriteBatch()
	defer c.releaseFileWriteBatch(batch)
	if err := fn(batch); err != nil {
		return err
	}
	if len(batch.entries) == 0 {
		return nil
	}
	_, target, applyErr := c.applyPrimaryBatch(batch)
	journalTarget = target
	return applyErr
}

// applyPrimaryBatch runs the prepare/durability/publish protocol above, retrying
// after a leaf split or a capacity checkpoint (both advance the published state,
// so the batch re-plans against it). It reports whether a generation was
// published and, for the buffered-journal lane, the group-commit ticket the
// caller must wait on for the shared sync after releasing the writer (zero when
// there is nothing to wait on). The sync lane fences before publish inside this
// call and returns a zero ticket.
func (c *Collection) applyPrimaryBatch(batch *WriteBatch) (bool, uint64, error) {
	// Each split strictly grows its tablet and each capacity checkpoint drains the
	// pending set, so both make monotonic progress; the budget bounds a pathological
	// placement loop the way primaryStructuralRetryLimit does for a single Put.
	budget := 2*len(batch.entries) + primaryStructuralRetryLimit + 8
	var lastErr error
	for attempt := 0; attempt < budget; attempt++ {
		state := c.state.Load()
		if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
			return false, 0, ErrClosed
		}
		if state.root.Generation == 0 || state.root.Generation >= uint64(1)<<48 {
			return false, 0, storeio.ErrGenerationOrder
		}
		if err := c.planPrimaryBatch(state, batch); err != nil {
			return false, 0, err
		}
		leafCount := len(c.batchPrimaryLeaves)
		if leafCount == 0 {
			// Every mutation resolved to a delete of an absent key: nothing changes,
			// nothing is journaled, nothing is published.
			return false, 0, nil
		}
		if leafCount > cap(c.primaryPendingParents) {
			// More distinct leaves than one atomic generation can hold pending. This
			// is the reservation ceiling; a batch that would exceed it is refused
			// whole rather than split across generations.
			return false, 0, ErrBatchTooLarge
		}
		checkpointed, err := c.ensurePrimaryBatchCapacity(leafCount)
		if err != nil {
			return false, 0, err
		}
		if checkpointed {
			// A checkpoint advanced FileEnd and rebound every leaf's physical
			// reference; the plan's routes are stale, so re-plan against fresh state.
			continue
		}
		splitKey, generation, buildErr := c.buildPrimaryBatchLeaves(state)
		if errors.Is(buildErr, ErrPrimaryLeafSplitRequired) {
			lastErr = buildErr
			if splitErr := c.preparePrimaryBatchTopology(
				state, batch, splitKey,
			); splitErr != nil {
				return false, 0, errors.Join(buildErr, splitErr)
			}
			continue
		}
		if buildErr != nil {
			return false, 0, buildErr
		}
		if !c.primaryBatchHasLiveLeaf() {
			return false, 0, nil
		}
		preparedExact, err := c.preparePrimaryBatchExact(state, generation)
		if err != nil {
			return false, 0, err
		}
		if err := c.admitPrimaryBatchLeaves(); err != nil {
			c.unwindPrimaryExactPrepared(&preparedExact)
			c.unadmitPrimaryBatchLeaves()
			return false, 0, err
		}
		// Point of no return for the sync lane: every fallible prepare has
		// succeeded and every leaf frame is admitted dirty but not yet
		// reader-visible. One batch record covers the whole group with one sync; a
		// device failure poisons and publishes nothing.
		if err := c.journalBatchBeforePublishLocked(generation, c.batchJournalEntries); err != nil {
			c.unwindPrimaryExactPrepared(&preparedExact)
			c.unadmitPrimaryBatchLeaves()
			return false, 0, err
		}
		// Point of no return passed: these frames are about to become reachable
		// through the batch's atomic router publication. Retire only the failure-
		// unwind bookkeeping; MarkUnreachable would discard live pages. Leaving
		// this slice populated also falsely makes every later buffered journal-
		// delta checkpoint ineligible.
		c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
		c.publishPrimaryBatch(state, generation, preparedExact)
		if c.buffered() {
			// Buffered-visible deposits its already-published batch's redo record
			// for the shared group sync after the writer is released.
			target, ackErr := c.journalBatchDepositLocked(generation, c.batchJournalEntries)
			if ackErr != nil {
				return true, 0, ackErr
			}
			return true, target, nil
		}
		return true, 0, nil
	}
	if lastErr == nil {
		lastErr = ErrPrimaryLeafSplitRequired
	}
	return false, 0, lastErr
}

// preparePrimaryBatchExact stages the exact-index half of one batch. Batch leaf
// construction may reassign stable slots, so each touched leaf contributes one
// absolute rebase group at the batch generation. All groups share one prepared
// chain-head set and are installed with the primary router flips.
//
// The resident overlay is deliberately fixed-size. If the complete batch does
// not fit, discard its unreachable records and build one fresh foreground epoch
// from the final images of all touched buckets. This is the same bounded
// pressure escape used by a single slot-reassigning mutation: publication stays
// atomic and no background or offline maintenance is required.
func (c *Collection) preparePrimaryBatchExact(
	state *fileStoreState, generation uint64,
) (primaryExactPrepared, error) {
	epoch := c.primaryEpoch
	if epoch == nil {
		return primaryExactPrepared{}, nil
	}
	prepared := primaryExactPrepared{
		active:         true,
		gen:            generation,
		termLinks:      c.exactTermLinkScratch[:0],
		tileLinks:      c.exactTileLinkScratch[:0],
		termRecordMark: epoch.termRecordN,
		tileRecordMark: epoch.tileRecordN,
	}
	bounds := c.primaryLeafBounds(state)
	pressure := false
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		image := c.batchPrimaryLeafArena[leaf.imageOffset : leaf.imageOffset+leaf.imageLength]
		ok, err := c.preparePrimaryExactRebase(
			epoch, &prepared, image, leaf.resident.Bucket,
			generation, bounds,
		)
		if err != nil {
			c.unwindPrimaryExactPrepared(&prepared)
			return primaryExactPrepared{}, err
		}
		if !ok {
			pressure = true
			break
		}
	}
	if !pressure {
		return prepared, nil
	}

	c.unwindPrimaryExactPrepared(&prepared)
	c.resetStructuralExactLocked()
	defer c.resetStructuralExactLocked()
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		image := c.batchPrimaryLeafArena[leaf.imageOffset : leaf.imageOffset+leaf.imageLength]
		if err := c.accumulateStructuralLeafLocked(
			leaf.resident.Bucket, image, bounds,
		); err != nil {
			return primaryExactPrepared{}, err
		}
	}
	return c.prepareStructuralExactLocked(generation)
}

// planPrimaryBatch validates and routes every WriteBatch entry, groups the
// mutations by the leaf they land in, and builds the batch's journal record —
// every entry in WriteBatch order, so replay reproduces the group. An absent-key
// delete is still journaled (it replays as a harmless no-op) but produces no leaf
// change; the build discovers that and skips it. Nothing here mutates state, so a
// validation error rejects the whole batch with nothing done.
func (c *Collection) planPrimaryBatch(state *fileStoreState, batch *WriteBatch) error {
	c.batchPrimaryLeaves = c.batchPrimaryLeaves[:0]
	c.batchPrimaryMutations = c.batchPrimaryMutations[:0]
	c.batchJournalEntries = c.batchJournalEntries[:0]
	for ei := range batch.entries {
		entry := batch.entries[ei]
		key := batch.key(entry)
		if len(key) == 0 || len(key) > c.options.MaxKeyBytes ||
			len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
			return ErrKeyTooLarge
		}
		var value []byte
		if !entry.remove {
			value = batch.value(entry)
			if len(value) == 0 || len(value) > c.options.MaxDocumentBytes ||
				len(value) > c.options.InlineValueBytes {
				return ErrDocumentTooLarge
			}
			if err := vibejson.Validate(value); err != nil {
				return err
			}
			if err := c.validatePrimarySchema(value); err != nil {
				return err
			}
		}
		resident, err := c.currentPrimaryResidentRoute(state, key)
		if err != nil {
			return err
		}
		c.batchPrimaryMutations = append(c.batchPrimaryMutations, primaryBatchMutation{
			key: key, value: value, resident: resident, remove: entry.remove,
		})
		kind := uint16(storeio.RecoveryRecordKindPut)
		var journalValue []byte
		if entry.remove {
			kind = storeio.RecoveryRecordKindDelete
		} else {
			journalValue = value
		}
		c.batchJournalEntries = append(c.batchJournalEntries, storeio.RecoveryBatchEntry{
			Kind: kind, Key: key, Value: journalValue,
		})
	}
	// The WriteBatch already deduplicated keys. Sort by the full stable bucket
	// identity and key, then form leaf ranges in one pass. This avoids the old
	// O(batch*touched-leaves) linear leaf lookup for dispersed batches and turns
	// final-image construction into one linear merge per leaf.
	slices.SortFunc(
		c.batchPrimaryMutations,
		func(a, b primaryBatchMutation) int {
			if a.resident.Bucket < b.resident.Bucket {
				return -1
			}
			if a.resident.Bucket > b.resident.Bucket {
				return 1
			}
			return bytes.Compare(a.key, b.key)
		},
	)
	leafIndex := -1
	var previousBucket storeio.BucketID
	for i := range c.batchPrimaryMutations {
		mutation := &c.batchPrimaryMutations[i]
		if leafIndex < 0 || mutation.resident.Bucket != previousBucket {
			c.batchPrimaryLeaves = append(c.batchPrimaryLeaves, primaryBatchLeaf{
				resident: mutation.resident, firstKey: mutation.key,
				pendingIndex: c.primaryPendingParentIndex(
					mutation.resident.Bucket,
				),
				mutationAt: i, mutationEnd: i + 1,
			})
			leafIndex++
			previousBucket = mutation.resident.Bucket
		} else {
			c.batchPrimaryLeaves[leafIndex].mutationEnd = i + 1
		}
	}
	return nil
}

func (c *Collection) primaryBatchHasLiveLeaf() bool {
	for i := range c.batchPrimaryLeaves {
		if !c.batchPrimaryLeaves[i].skip {
			return true
		}
	}
	return false
}

// ensurePrimaryBatchCapacity reserves everything the batch's publish will consume
// before it prepares a single frame, checkpointing to make room exactly as the
// single-document path does. It reports whether it checkpointed, because a
// checkpoint moves the published cut and the caller must re-plan. The single
// document path's own reservation is untouched; this is the batch's arithmetic:
//
//   - one pending-parent slot per newly touched leaf (a checkpoint drains the set);
//   - room in the preallocated journal for the whole batch record;
//   - dirty-cache room for the batch's leaf frames;
//   - one deferred volatile-reference slot per touched leaf, which under an active
//     snapshot cannot be freed and so fails closed as recoverable backpressure.
func (c *Collection) ensurePrimaryBatchCapacity(leafCount int) (bool, error) {
	before := c.automaticCheckpoints.Load()
	newDistinct := 0
	for i := range c.batchPrimaryLeaves {
		if c.batchPrimaryLeaves[i].pendingIndex < 0 {
			newDistinct++
		}
	}
	if len(c.primaryPendingParents)+newDistinct > cap(c.primaryPendingParents) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return false, err
		}
		c.automaticCheckpoints.Add(1)
	}
	if err := c.ensurePrimaryBatchJournalRoom(c.batchJournalEntries); err != nil {
		return false, err
	}
	required := uint64(leafCount) *
		c.cache.ReservationBytes(storeio.CommonPrimaryLeafWideBytes)
	if c.cache.DirtyCapacityAvailable() < required {
		if err := c.checkpointBufferedLocked(); err != nil {
			return false, err
		}
		c.automaticCheckpoints.Add(1)
	}
	c.clearPrimaryVolatileRetiredLocked()
	if len(c.primaryVolatileRetired)+leafCount > cap(c.primaryVolatileRetired) {
		return false, fmt.Errorf(
			"%w: buffered primary volatile-reference capacity %d",
			storeio.ErrRetiredExtentCapacity,
			cap(c.primaryVolatileRetired),
		)
	}
	return c.automaticCheckpoints.Load() != before, nil
}

// buildPrimaryBatchLeaves rewrites one frame per touched leaf, laying every frame
// out in the memory-only append region past FileEnd (the same virtual extents a
// single buffered mutation uses) so the batch reserves no physical space until a
// later checkpoint materializes it. On a split-required signal it returns the
// offending key so the caller can run the split as its own transaction and retry.
//
// It returns the batch's one published generation. Every final leaf image is
// derived directly from the previous cut and stamped base+1; mutation count does
// not inflate generations because no intermediate leaf image is published.
func (c *Collection) buildPrimaryBatchLeaves(
	state *fileStoreState,
) ([]byte, uint64, error) {
	baseGen := state.root.Generation
	c.batchPrimaryLeafArena = c.batchPrimaryLeafArena[:0]
	running := state.fileEnd
	if len(c.primaryPendingParents) == 0 {
		gap := uint64(c.options.maxTransactionPages) * uint64(c.options.MaxPageSize)
		if running > math.MaxUint64-gap {
			return nil, 0, storeio.ErrInvalidWrite
		}
		running += gap
	}
	live := false
	for li := range c.batchPrimaryLeaves {
		splitKey, err := c.buildPrimaryBatchLeaf(state, baseGen, li)
		if err != nil {
			return splitKey, 0, err
		}
		leaf := &c.batchPrimaryLeaves[li]
		if leaf.skip {
			continue
		}
		live = true
		if running > math.MaxUint64-uint64(leaf.imageLength) {
			return nil, 0, storeio.ErrInvalidWrite
		}
		leaf.nextLeaf = storeio.PageRef{
			Offset: running, LogicalID: leaf.resident.Ref.LogicalID,
			Generation: leaf.frameGen, Length: uint32(leaf.imageLength),
			Kind: storeio.PagePrimaryLeaf,
		}
		running += uint64(leaf.imageLength)
	}
	generation := baseGen
	if live {
		generation++
	}
	if generation == 0 || generation >= uint64(1)<<48 {
		return nil, 0, storeio.ErrGenerationOrder
	}
	router := c.primaryRouter.Load()
	for li := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[li]
		if leaf.skip {
			continue
		}
		if !router.CanUpdateLeaf(leaf.resident, leaf.nextLeaf, generation) {
			return nil, 0, storeio.ErrSegmentedTabletRouterCorrupt
		}
	}
	c.batchPrimaryFileEnd = running
	return nil, generation, nil
}

// buildPrimaryBatchLeaf renders the base leaf once, linearly merges its sorted
// rows with the leaf's sorted, deduplicated mutations, places the complete final
// set once, and encodes one final image. Work is O(base rows + mutations): no
// intermediate image exists and generation advances exactly once for the leaf.
func (c *Collection) buildPrimaryBatchLeaf(
	state *fileStoreState, baseGen uint64, li int,
) ([]byte, error) {
	leaf := &c.batchPrimaryLeaves[li]
	bounds := c.primaryLeafBounds(state)
	var (
		path  filePrimaryMutationPath
		lease storeio.PageLease
		page  []byte
	)
	if leaf.pendingIndex < 0 {
		if err := c.acquirePrimaryRoutingPath(
			&path, state, leaf.firstKey, leaf.resident,
		); err != nil {
			return nil, err
		}
		defer path.Release()
		leaf.pending = filePrimaryPendingParentFromPath(leaf.resident, &path)
		page = path.leafLease.Page()
	} else {
		acquired, err := c.cache.Acquire(leaf.resident.Ref)
		if err != nil {
			return nil, err
		}
		lease = acquired
		defer lease.Release()
		leaf.pending = c.primaryPendingParents[leaf.pendingIndex]
		page = lease.Page()
	}
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafUnified {
		return nil, storeio.ErrCommonPrimaryLeafCorrupt
	}
	unified, ok := storeio.AdmittedCommonPrimaryUnifiedLeaf(
		page, c.storeID, leaf.resident.Bucket, bounds,
	)
	if !ok {
		return nil, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseRows, err := unified.RenderRecordsWithScratch(c.primaryLeafMutationScratch)
	if err != nil {
		return nil, err
	}
	leaf.initialLen = len(baseRows)
	leaf.docDelta = 0
	final, applied, err := c.mergePrimaryBatchLeafRows(
		baseRows, leaf, c.structuralRows[:0],
	)
	if err != nil {
		return nil, err
	}
	leaf.applied = applied
	c.structuralRows = final
	if leaf.applied == 0 {
		leaf.skip = true
		return nil, nil
	}
	leaf.docDelta = len(final) - len(baseRows)
	leaf.finalLen = len(final)
	if len(final) > storeio.CommonPrimaryLeafWideSlots {
		c.primaryLeafSplitRequired.Add(1)
		return c.primaryBatchProspectiveSplitKey(final), errors.Join(
			ErrPrimaryLeafSplitRequired, storeio.ErrCommonPrimaryLeafFull,
		)
	}
	if err := storeio.PlaceCommonPrimaryLeafRecords(
		storeio.CommonPrimaryLeafWide, c.storeID, final,
	); err != nil {
		if errors.Is(err, storeio.ErrCommonPrimaryLeafNeedsWide) ||
			errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
			c.primaryLeafSplitRequired.Add(1)
			return c.primaryBatchProspectiveSplitKey(final), errors.Join(
				ErrPrimaryLeafSplitRequired, err,
			)
		}
		return nil, err
	}
	leaf.frameGen = baseGen + 1
	image, err := storeio.EncodeBestCommonPrimaryUnifiedLeaf(
		c.primaryLeafScratch,
		storeio.CommonPrimaryLeafHeader{
			StoreID: c.storeID, Generation: leaf.frameGen,
			Bucket: leaf.resident.Bucket,
		},
		c.storeID, final, bounds, c.primaryUnifiedBuilder,
	)
	if errors.Is(err, storeio.ErrCommonPrimaryLeafFull) {
		c.primaryLeafSplitRequired.Add(1)
		return c.primaryBatchProspectiveSplitKey(final), errors.Join(
			ErrPrimaryLeafSplitRequired, err,
		)
	}
	if err != nil {
		return nil, err
	}
	leaf.imageOffset = len(c.batchPrimaryLeafArena)
	c.batchPrimaryLeafArena = append(c.batchPrimaryLeafArena, image...)
	leaf.imageLength = len(c.batchPrimaryLeafArena) - leaf.imageOffset
	return nil, nil
}

// mergePrimaryBatchLeafRows computes one leaf's final logical row set. Both
// inputs are lexical and batch keys are deduplicated, so the merge is linear.
// dst may reuse Collection structural scratch. applied counts effective puts,
// replacements, and present deletes; absent deletes do not force publication.
func (c *Collection) mergePrimaryBatchLeafRows(
	baseRows []storeio.CommonPrimaryLeafRecord,
	leaf *primaryBatchLeaf,
	dst []storeio.CommonPrimaryLeafRecord,
) ([]storeio.CommonPrimaryLeafRecord, int, error) {
	final := dst
	applied := 0
	baseAt := 0
	mutationAt := leaf.mutationAt
	for baseAt < len(baseRows) || mutationAt < leaf.mutationEnd {
		if mutationAt >= leaf.mutationEnd {
			final = append(final, baseRows[baseAt:]...)
			baseAt = len(baseRows)
			break
		}
		mutation := &c.batchPrimaryMutations[mutationAt]
		if baseAt >= len(baseRows) {
			if !mutation.remove {
				final = append(final, storeio.CommonPrimaryLeafRecord{
					Key:   mutation.key,
					Value: storeio.CommonPrimaryLeafValue{Inline: mutation.value},
				})
				applied++
			}
			mutationAt++
			continue
		}
		base := baseRows[baseAt]
		order := bytes.Compare(base.Key, mutation.key)
		switch {
		case order < 0:
			final = append(final, base)
			baseAt++
		case order > 0:
			if !mutation.remove {
				final = append(final, storeio.CommonPrimaryLeafRecord{
					Key:   mutation.key,
					Value: storeio.CommonPrimaryLeafValue{Inline: mutation.value},
				})
				applied++
			}
			mutationAt++
		default:
			if base.Value.IsOverflow() {
				return nil, 0, fmt.Errorf(
					"%w: ordered primary overflow mutation",
					ErrPrimaryCutoverUnsupported,
				)
			}
			if !mutation.remove {
				final = append(final, storeio.CommonPrimaryLeafRecord{
					Key:   mutation.key,
					Value: storeio.CommonPrimaryLeafValue{Inline: mutation.value},
				})
			}
			applied++
			baseAt++
			mutationAt++
		}
	}
	return final, applied, nil
}

func (c *Collection) primaryBatchProspectiveSplitKey(
	rows []storeio.CommonPrimaryLeafRecord,
) []byte {
	if len(rows) == 0 {
		return nil
	}
	key := rows[len(rows)/2].Key
	c.batchPrimarySplitKey = append(c.batchPrimarySplitKey[:0], key...)
	return c.batchPrimarySplitKey
}

// admitPrimaryBatchLeaves admits every rewritten leaf frame into the cache as a
// buffered dirty page. The frames are not reader-visible until publishPrimaryBatch
// flips the router, so admitting them all before the journal fence changes nothing
// a reader can observe. Capacity was reserved, so a failure here is unexpected and
// unadmits what it staged, leaving nothing visible and nothing journaled.
func (c *Collection) admitPrimaryBatchLeaves() error {
	c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		image := c.batchPrimaryLeafArena[leaf.imageOffset : leaf.imageOffset+leaf.imageLength]
		if err := c.cache.AdmitBufferedDirty(leaf.nextLeaf, image, math.MaxUint64); err != nil {
			return err
		}
		c.batchPrimaryAdmitted = append(c.batchPrimaryAdmitted, leaf.nextLeaf)
	}
	return nil
}

func (c *Collection) unadmitPrimaryBatchLeaves() {
	if len(c.batchPrimaryAdmitted) == 0 {
		return
	}
	c.cache.MarkUnreachable(c.batchPrimaryAdmitted)
	c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
}

// publishPrimaryBatch makes the whole batch visible under one generation. Every
// step here is infallible by construction — routing, admission, split detection,
// and capacity all succeeded in prepare — so a reader that samples the router
// after the snapshotGate sees every rewritten leaf or, before it, none of them.
//
// The reader fence wraps the swap exactly as the single-document canonical
// publish does: retirePrimaryVolatileRefLocked decides each superseded volatile
// frame's immediate-versus-deferred reclaim from anyActiveReaders, which is a
// meaningful veto only while the fence diverts new epoch entries to the gated
// path and the writer holds the snapshot gate. The router flip and state
// publish precede the retirement scans, so a reader admitted before the fence
// is seen by those scans and its frame is deferred, and a reader arriving after
// the fence takes the gated slow path against the new root.
func (c *Collection) publishPrimaryBatch(
	state *fileStoreState,
	generation uint64,
	preparedExact primaryExactPrepared,
) {
	totalDelta := 0
	for i := range c.batchPrimaryLeaves {
		if c.batchPrimaryLeaves[i].skip {
			continue
		}
		totalDelta += c.batchPrimaryLeaves[i].docDelta
	}
	nextRoot := state.root
	nextRoot.Generation = generation
	nextRoot.DocumentCount = uint64(int64(state.root.DocumentCount) + int64(totalDelta))
	nextState := &fileStoreState{
		root: nextRoot, fileEnd: c.batchPrimaryFileEnd,
		freeHead: state.freeHead,
	}

	c.batchPrimaryPrevVolatile = c.batchPrimaryPrevVolatile[:0]
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		prev := leaf.pending.volatileRef
		leaf.pending.volatileRef = leaf.nextLeaf
		if leaf.pendingIndex < 0 {
			c.primaryPendingParents = append(c.primaryPendingParents, leaf.pending)
		} else {
			c.primaryPendingParents[leaf.pendingIndex] = leaf.pending
		}
		c.batchPrimaryPrevVolatile = append(c.batchPrimaryPrevVolatile, prev)
	}

	router := c.primaryRouter.Load()
	c.snapshotGate.Lock()
	c.beginReaderFence()
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		router.UpdateLeaf(leaf.resident, leaf.nextLeaf, generation)
	}
	c.installPrimaryExactResidentLocked(preparedExact)
	c.pageValidator.update(nextState)
	c.publishFileState(nextState)
	for _, prev := range c.batchPrimaryPrevVolatile {
		c.retirePrimaryVolatileRefLocked(prev)
	}
	c.endReaderFence()
	c.snapshotGate.Unlock()

	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		becameEmpty := leaf.initialLen > 0 && leaf.finalLen == 0
		filledEmpty := leaf.initialLen == 0 && leaf.finalLen > 0
		if becameEmpty && router.MarkEmpty(leaf.resident) {
			c.primaryEmptyLeaves.Add(1)
		}
		if filledEmpty && router.ClearEmpty(leaf.resident) {
			c.removePrimaryEmptyLeaf()
		}
	}
}
