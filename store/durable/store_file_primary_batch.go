package durable

import (
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/storeio"
	vibejson "github.com/thesyncim/vibejson"
)

// ErrPrimaryBatchUnsupportedLane reports an Update on an ordered-primary
// collection whose durability lane publishes through the committer generation
// fence rather than the canonical dirty frames a primary batch commits on. That
// is DurabilityAsyncVisible (and the degraded primary-sync store whose journal
// never opened, which a healthy store never reaches, since a primary sync store
// mints its journal unconditionally). The three lanes a primary batch supports —
// buffered-visible, buffered-visible+journal, and sync-journal — all take the
// deferred canonical path, where one generation carries the whole group.
var ErrPrimaryBatchUnsupportedLane = errors.New(
	"vibejson: ordered primary Update requires a buffered-visible or sync-journal lane",
)

// ErrPrimaryBatchIndexedUnsupported reports that Update was called on an
// indexed primary collection. Single-document mutations maintain the exact
// index atomically; the batch publish path does not yet, and refusing beats
// serving a stale index.
var ErrPrimaryBatchIndexedUnsupported = errors.New(
	"vibejson: transactional batch on an indexed primary collection is not yet supported",
)

// primaryBatchMutation is one resolved key in a primary Update: the leaf it
// routes to, its per-key hash, and the document bytes (nil for a delete). Keys
// and values borrow the WriteBatch arena, stable for the whole Update.
type primaryBatchMutation struct {
	key       []byte
	value     []byte
	hash      uint64
	leafIndex int
	remove    bool
}

// primaryBatchLeaf accumulates every mutation a batch routes to one leaf and the
// single rewritten frame they fold into. One frame is admitted per touched leaf,
// no matter how many of the batch's documents land in it, which is the same
// write-amplification win the chunk batch has: N puts into one leaf rewrite the
// leaf once, not N times.
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
	skip         bool
}

// updatePrimaryBatch applies one WriteBatch to the ordered primary graph as a
// single failure-atomic generation. It is the primary-layout half of
// Collection.Update; the chunk layout keeps its own committer-generation path.
//
// The whole batch prepares before any of it is durable, and publishes after: every
// document is routed and its leaf frame is rewritten (all fallible, including the
// leaf splits a member may force — each split commits as its own structural
// transaction before the batch publishes), then one journal record covers the
// group with one sync, then every leaf pointer flips under one generation so a
// reader sees all of the batch or none of it.
func (c *Collection) updatePrimaryBatch(fn func(*WriteBatch) error) (err error) {
	if c == nil {
		return ErrClosed
	}
	if c.primaryExactActive() {
		// Single-document mutations maintain the exact index inside their own
		// publish, but the batch publish path does not yet run the posting
		// maintainer; refusing is the honest contract until the composition is
		// built, because a batch that silently skipped index maintenance would
		// serve stale postings.
		return ErrPrimaryBatchIndexedUnsupported
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
			if splitErr := c.splitPrimaryBatchLeaf(state, splitKey); splitErr != nil {
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
		if err := c.admitPrimaryBatchLeaves(); err != nil {
			c.unadmitPrimaryBatchLeaves()
			return false, 0, err
		}
		// Point of no return for the sync lane: every fallible prepare has
		// succeeded and every leaf frame is admitted dirty but not yet
		// reader-visible. One batch record covers the whole group with one sync; a
		// device failure poisons and publishes nothing.
		if err := c.journalBatchBeforePublishLocked(generation, c.batchJournalEntries); err != nil {
			c.unadmitPrimaryBatchLeaves()
			return false, 0, err
		}
		c.publishPrimaryBatch(state, generation)
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
		li := c.findPrimaryBatchLeaf(resident.Bucket)
		if li < 0 {
			c.batchPrimaryLeaves = append(c.batchPrimaryLeaves, primaryBatchLeaf{
				resident:     resident,
				firstKey:     key,
				pendingIndex: c.primaryPendingParentIndex(resident.Bucket),
			})
			li = len(c.batchPrimaryLeaves) - 1
		}
		c.batchPrimaryMutations = append(c.batchPrimaryMutations, primaryBatchMutation{
			key: key, value: value, hash: resident.Hash, leafIndex: li, remove: entry.remove,
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
	return nil
}

func (c *Collection) findPrimaryBatchLeaf(bucket storeio.BucketID) int {
	for i := range c.batchPrimaryLeaves {
		if c.batchPrimaryLeaves[i].resident.Bucket == bucket {
			return i
		}
	}
	return -1
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
// It returns the batch's published generation. Several mutations folding onto one
// leaf must chain through strictly increasing generations, because each leaf
// codec rejects a rewrite whose generation does not advance; a leaf's frame lands
// at its own advanced generation and the batch publishes the state at the highest
// of them, which is why the whole group still commits as one atomic state
// publication even though the frames carry a small spread of generations — a leaf
// frame lagging the state generation is the ordinary resting shape of the graph.
func (c *Collection) buildPrimaryBatchLeaves(
	state *fileStoreState,
) ([]byte, uint64, error) {
	baseGen := state.root.Generation
	c.batchPrimaryLeafArena = c.batchPrimaryLeafArena[:0]
	running := state.super.FileEnd
	if len(c.primaryPendingParents) == 0 {
		gap := uint64(c.options.maxTransactionPages) * uint64(c.options.MaxPageSize)
		if running > math.MaxUint64-gap {
			return nil, 0, storeio.ErrInvalidWrite
		}
		running += gap
	}
	maxApplied := 0
	for li := range c.batchPrimaryLeaves {
		splitKey, err := c.buildPrimaryBatchLeaf(state, baseGen, li)
		if err != nil {
			return splitKey, 0, err
		}
		leaf := &c.batchPrimaryLeaves[li]
		if leaf.skip {
			continue
		}
		if leaf.applied > maxApplied {
			maxApplied = leaf.applied
		}
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
	generation := baseGen + uint64(maxApplied)
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

// buildPrimaryBatchLeaf folds every mutation routed to one leaf onto a single
// frame. Each mutation reuses preparePrimaryLeafMutation, the exact primitive the
// single-document path uses, so sibling changes to leaf geometry or posting
// maintenance compose here for free. Multiple mutations chain through an
// in-memory view of the accumulating image, each stamped with the next generation
// above baseGen, which is why the batch touches a crowded leaf once instead of
// once per document.
func (c *Collection) buildPrimaryBatchLeaf(
	state *fileStoreState, baseGen uint64, li int,
) ([]byte, error) {
	leaf := &c.batchPrimaryLeaves[li]
	bounds := c.primaryLeafBounds(state)
	var (
		path     filePrimaryMutationPath
		lease    storeio.PageLease
		havePath bool
	)
	if leaf.pendingIndex < 0 {
		if err := c.acquirePrimaryMutationPath(
			&path, state, leaf.firstKey, leaf.resident,
		); err != nil {
			return nil, err
		}
		havePath = true
		defer path.Release()
		leaf.pending = filePrimaryPendingParentFromPath(leaf.resident, &path)
	} else {
		acquired, err := c.cache.Acquire(leaf.resident.Ref)
		if err != nil {
			return nil, err
		}
		lease = acquired
		defer lease.Release()
		leaf.pending = c.primaryPendingParents[leaf.pendingIndex]
	}

	var accView storeio.CommonPrimaryLeafView
	curView := &path.leaf
	if !havePath {
		view, _, admitErr := storeio.AdmittedPrimaryLeafForMutation(
			lease.Page(), c.storeID, leaf.resident.Bucket, bounds,
		)
		if admitErr != nil {
			return nil, admitErr
		}
		accView = view
		curView = &accView
	}
	leaf.initialLen = curView.Len()
	leaf.applied = 0
	leaf.docDelta = 0
	for mi := range c.batchPrimaryMutations {
		m := &c.batchPrimaryMutations[mi]
		if m.leafIndex != li {
			continue
		}
		slot, _, overflow, found := curView.LookupRawHashed(m.hash, m.key)
		if overflow {
			return nil, fmt.Errorf(
				"%w: ordered primary overflow mutation",
				ErrPrimaryCutoverUnsupported,
			)
		}
		if m.remove && !found {
			continue
		}
		// Each fold onto this leaf advances the generation so the codec accepts the
		// rewrite; the running image already carries the previous fold's generation.
		stampGen := baseGen + uint64(leaf.applied) + 1
		preparePath := filePrimaryMutationPath{leaf: *curView}
		image, imageBytes, prepErr := c.preparePrimaryLeafMutation(
			&preparePath, stampGen, m.key,
			storeio.CommonPrimaryLeafValue{Inline: m.value},
			m.remove, found, slot,
		)
		if errors.Is(prepErr, ErrPrimaryLeafSplitRequired) {
			return m.key, prepErr
		}
		if prepErr != nil {
			return nil, prepErr
		}
		c.batchPrimaryLeafImage = append(c.batchPrimaryLeafImage[:0], image[:imageBytes]...)
		view, _, admitErr := storeio.AdmittedPrimaryLeafForMutation(
			c.batchPrimaryLeafImage, c.storeID, leaf.resident.Bucket, bounds,
		)
		if admitErr != nil {
			return nil, admitErr
		}
		accView = view
		curView = &accView
		if m.remove {
			leaf.docDelta--
		} else if !found {
			leaf.docDelta++
		}
		leaf.applied++
	}
	if leaf.applied == 0 {
		leaf.skip = true
		return nil, nil
	}
	leaf.frameGen = baseGen + uint64(leaf.applied)
	leaf.finalLen = curView.Len()
	leaf.imageOffset = len(c.batchPrimaryLeafArena)
	c.batchPrimaryLeafArena = append(c.batchPrimaryLeafArena, c.batchPrimaryLeafImage...)
	leaf.imageLength = len(c.batchPrimaryLeafArena) - leaf.imageOffset
	return nil, nil
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
func (c *Collection) publishPrimaryBatch(state *fileStoreState, generation uint64) {
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
	nextSuper := state.super
	nextSuper.Generation = generation
	nextSuper.FileEnd = c.batchPrimaryFileEnd
	nextState := &fileStoreState{
		root: nextRoot, super: nextSuper,
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

// splitPrimaryBatchLeaf runs the leaf split a batch member forced as its own
// structural transaction, exactly as the single-document Put path does: a split
// changes structure, not content, so committing it before the batch publishes
// preserves the batch's content atomicity — a crash sees the split (a pure
// re-shaping of the same keys and values) or not, and the batch that triggered it
// replays whole or not at all on its own record. It materializes any pending
// parents of the target tablet first so the split rewrites real pages.
func (c *Collection) splitPrimaryBatchLeaf(state *fileStoreState, key []byte) error {
	c.pointKeyScratch = append(c.pointKeyScratch[:0], key...)
	keyBytes := c.pointKeyScratch
	resident, err := c.currentPrimaryResidentRoute(state, keyBytes)
	if err != nil {
		return err
	}
	if c.primaryPendingTablet(resident.Bucket) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return err
		}
		c.automaticCheckpoints.Add(1)
	}
	return c.structuralSplitPrimaryLeaf(keyBytes)
}
