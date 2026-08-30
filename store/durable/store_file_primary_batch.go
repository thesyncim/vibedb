package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
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
	"vibedb: ordered primary Update requires a buffered-visible or sync-journal lane",
)

var errPrimaryBatchExactCheckpointRequired = errors.New(
	"vibedb: ordered primary stable-slot exact delta requires checkpoint",
)

// primaryBatchMutation is one resolved key in a primary Update: its resident
// route and document bytes (nil for a delete). Keys and values borrow the
// WriteBatch arena, stable for the whole Update.
type primaryBatchMutation struct {
	key      []byte
	value    []byte
	stored   storeio.CommonPrimaryLeafValue
	resident storeio.ResidentPrimaryRoute
	remove   bool
	found    bool
	oldSlot  uint8
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
	stableSlots  bool
	skip         bool
}

// stagedPrimaryBatch is the opaque product of stagePrimaryBatchLocked: dirty
// frames and exact-index prep are admitted but not reader-visible. The single-
// collection Update path fences with a kind-3 journal record before publish;
// multi-collection commit (T4) fences with a kind-4 conditional record via
// preparePrimaryBatchConditionalLocked, then publishes under externally held
// snapshot gates.
type stagedPrimaryBatch struct {
	state         *fileStoreState
	generation    uint64
	preparedExact primaryExactPrepared
	live          bool
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
//
// Single-collection Update recomposes stage → kind-3 journal fence → publish
// with its own snapshotGate acquisition so on-disk journal bytes stay
// byte-identical to the pre-phase-split path.
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
	if err := c.rejectCheckpointGroupOwner(); err != nil {
		return err
	}
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
	if err := c.ensureOrdinaryBufferedRecoveryJournalLocked(); err != nil {
		return err
	}
	_, target, applyErr := c.applyPrimaryBatch(batch)
	journalTarget = target
	return applyErr
}

// applyPrimaryBatch runs the stage/durability/publish protocol, retrying inside
// stage after a leaf split or a capacity checkpoint. It reports whether a
// generation was published and, for the buffered-journal lane, the group-commit
// ticket the caller must wait on for the shared sync after releasing the writer
// (zero when there is nothing to wait on). The sync lane fences before publish
// inside this call and returns a zero ticket.
func (c *Collection) applyPrimaryBatch(batch *WriteBatch) (bool, uint64, error) {
	staged, err := c.stagePrimaryBatchLocked(batch)
	if err != nil {
		return false, 0, err
	}
	if !staged.live {
		return false, 0, nil
	}
	// Point of no return for the sync lane: every fallible prepare has
	// succeeded and every leaf frame is admitted dirty but not yet
	// reader-visible. One kind-3 batch record covers the whole group with one
	// sync; a device failure poisons and publishes nothing.
	if err := c.journalBatchBeforePublishLocked(
		staged.generation, c.batchJournalEntries,
	); err != nil {
		c.unwindStagedPrimaryBatch(&staged)
		return false, 0, err
	}
	// Point of no return passed: these frames are about to become reachable
	// through the batch's atomic router publication. Retire only the failure-
	// unwind bookkeeping; MarkUnreachable would discard live pages. Leaving
	// this slice populated also falsely makes every later buffered journal-
	// delta checkpoint ineligible.
	c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
	c.publishPrimaryBatch(
		staged.state, staged.generation, staged.preparedExact,
	)
	if c.buffered() {
		// Buffered-visible deposits its already-published batch's redo record
		// for the shared group sync after the writer is released.
		target, ackErr := c.journalBatchDepositLocked(
			staged.generation, c.batchJournalEntries,
		)
		if ackErr != nil {
			return true, 0, ackErr
		}
		return true, target, nil
	}
	return true, 0, nil
}

// stagePrimaryBatchLocked materializes any pending overlay, plans and builds
// every touched leaf, reserves capacity, and admits overflow chains plus leaf
// frames. It stops before any journal fence: the caller chooses kind-3
// (single-collection Update) or kind-4 (conditional prepare). The writer must
// already be held. On success with live == true the returned unwind clears
// every admitted frame; call it on any failure before publish. On a no-op
// batch (absent deletes only) live is false and unwind is a no-op.
func (c *Collection) stagePrimaryBatchLocked(
	batch *WriteBatch,
) (stagedPrimaryBatch, error) {
	return c.stagePrimaryBatchForJournalLocked(batch, false)
}

// stagePrimaryBatchConditionalLocked stages a multi-collection participant
// after reserving room for its larger kind-4 conditional journal record. The
// ordinary buffered single-collection lane keeps its compact kind-3 journal
// policy and physical-checkpoint fallback.
func (c *Collection) stagePrimaryBatchConditionalLocked(
	batch *WriteBatch,
) (stagedPrimaryBatch, error) {
	return c.stagePrimaryBatchForJournalLocked(batch, true)
}

func (c *Collection) stagePrimaryBatchForJournalLocked(
	batch *WriteBatch, conditional bool,
) (stagedPrimaryBatch, error) {
	if c.primaryUnifiedOverlay.hasPending() {
		if err := c.materializePrimaryParentsLocked(primaryMaterializationBarrier); err != nil {
			return stagedPrimaryBatch{}, err
		}
	}
	// Each split strictly grows its tablet and each capacity checkpoint drains the
	// pending set, so both make monotonic progress; the budget bounds a pathological
	// placement loop the way primaryStructuralRetryLimit does for a single Put.
	budget, ok := primaryBatchStageAttemptBudget(len(batch.entries))
	if !ok {
		return stagedPrimaryBatch{}, ErrBatchTooLarge
	}
	var lastErr error
	for attempt := 0; attempt < budget; attempt++ {
		// A topology or checkpoint retry starts from no staged frames and no
		// retirements borrowed from the superseded state. The ordinary retry paths
		// occur before admission, but unadmitting defensively here keeps a future
		// retry insertion from turning stale staged pages into dirty-capacity leaks.
		c.unadmitPrimaryBatchLeaves()
		c.batchPrimaryOverflowVolatile = c.batchPrimaryOverflowVolatile[:0]
		c.batchPrimaryOverflowDurable = c.batchPrimaryOverflowDurable[:0]
		state := c.state.Load()
		if state == nil || state.root.PrimaryRoot == (storeio.PageRef{}) {
			return stagedPrimaryBatch{}, ErrClosed
		}
		if state.root.Generation == 0 || state.root.Generation >= uint64(1)<<48 {
			return stagedPrimaryBatch{}, storeio.ErrGenerationOrder
		}
		if err := c.planPrimaryBatch(state, batch); err != nil {
			return stagedPrimaryBatch{}, err
		}
		// Validate the complete final image before leaf rendering or structural
		// preparation. A conflicting insert routed to a highly compressed leaf
		// must report the typed constraint error without first trying an
		// unnecessary split. Certified sequential recovery already validated its
		// whole original record, so its one-entry intermediate images are exempt.
		if !(c.journalReplaying && c.primaryUniqueReplayValidated) {
			if err := c.validatePrimaryUniqueRecoveryBatch(
				c.batchJournalEntries,
			); err != nil {
				return stagedPrimaryBatch{}, err
			}
		}
		leafCount := len(c.batchPrimaryLeaves)
		if leafCount == 0 {
			// Every mutation resolved to a delete of an absent key: nothing changes,
			// nothing is journaled, nothing is published.
			return stagedPrimaryBatch{}, nil
		}
		if leafCount > cap(c.primaryPendingParents) {
			// More distinct leaves than one atomic generation can hold pending. This
			// is the reservation ceiling; a batch that would exceed it is refused
			// whole rather than split across generations.
			return stagedPrimaryBatch{}, ErrBatchTooLarge
		}
		splitKey, generation, buildErr := c.buildPrimaryBatchLeaves(state)
		if errors.Is(buildErr, ErrPrimaryLeafSplitRequired) {
			lastErr = buildErr
			if splitErr := c.preparePrimaryBatchTopology(
				state, batch, splitKey,
			); splitErr != nil {
				return stagedPrimaryBatch{}, errors.Join(buildErr, splitErr)
			}
			continue
		}
		if buildErr != nil {
			return stagedPrimaryBatch{}, buildErr
		}
		if !c.primaryBatchHasLiveLeaf() {
			return stagedPrimaryBatch{}, nil
		}
		if err := c.validatePrimaryUniqueBatch(); err != nil {
			return stagedPrimaryBatch{}, err
		}
		checkpointed, err := c.ensurePrimaryBatchCapacity(conditional)
		if err != nil {
			return stagedPrimaryBatch{}, err
		}
		if checkpointed {
			// A checkpoint advanced FileEnd, NextLogicalID, and every resident route;
			// discard this read-only plan and assign all overflow identities again.
			continue
		}
		c.batchPrimaryAdmitted = c.batchPrimaryAdmitted[:0]
		if err := c.admitPrimaryBatchOverflow(); err != nil {
			c.unadmitPrimaryBatchLeaves()
			return stagedPrimaryBatch{}, err
		}
		preparedExact, err := c.preparePrimaryBatchExact(state, generation)
		if errors.Is(err, errPrimaryBatchExactCheckpointRequired) {
			c.unadmitPrimaryBatchLeaves()
			if checkpointErr := c.checkpointBufferedLocked(); checkpointErr != nil {
				return stagedPrimaryBatch{}, checkpointErr
			}
			c.automaticCheckpoints.Add(1)
			lastErr = err
			continue
		}
		if err != nil {
			c.unadmitPrimaryBatchLeaves()
			return stagedPrimaryBatch{}, err
		}
		if err := c.admitPrimaryBatchLeaves(); err != nil {
			c.unwindPrimaryExactPrepared(&preparedExact)
			c.unadmitPrimaryBatchLeaves()
			return stagedPrimaryBatch{}, err
		}
		return stagedPrimaryBatch{
			state:         state,
			generation:    generation,
			preparedExact: preparedExact,
			live:          true,
		}, nil
	}
	if lastErr == nil {
		lastErr = ErrPrimaryLeafSplitRequired
	}
	return stagedPrimaryBatch{}, lastErr
}

func primaryBatchStageAttemptBudget(documents int) (int, bool) {
	constant := primaryStructuralRetryLimit + 8
	if documents < 0 || documents > (math.MaxInt-constant)/2 {
		return 0, false
	}
	return 2*documents + constant, true
}

// unwindStagedPrimaryBatch discards every dirty frame and exact-index record
// staged for a batch that will not publish. It is idempotent.
func (c *Collection) unwindStagedPrimaryBatch(staged *stagedPrimaryBatch) {
	if staged == nil || !staged.live {
		return
	}
	c.unwindPrimaryExactPrepared(&staged.preparedExact)
	c.unadmitPrimaryBatchLeaves()
	staged.live = false
}

// preparePrimaryBatchConditionalLocked appends one kind-4 conditional batch
// record for a previously staged batch. forceSync makes the append durable in
// place; multi-collection commit always passes true so every supported lane
// fences before the decision record. An append may report failure after its
// complete checksummed body reached the page cache, and a sync failure may have
// made that body durable. Both poison with ErrCommitOutcomeUnknown: recovery can
// observe the prepare even though the transaction itself remains unresolved.
// Conditional staging has already reserved the exact kind-4 bytes before any
// frame admission. This phase therefore only rechecks that immutable plan and
// appends it; it must never checkpoint or grow after dirty frames have
// been admitted. The writer must already be held.
func (c *Collection) preparePrimaryBatchConditionalLocked(
	staged *stagedPrimaryBatch,
	markerID [16]byte,
	markerEpoch, txnID uint64,
	forceSync bool,
) error {
	if staged == nil || !staged.live {
		return fmt.Errorf("%w: conditional prepare without staged batch", ErrClosed)
	}
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	if !c.journalEnabled() || c.journalReplaying {
		return fmt.Errorf(
			"%w: conditional prepare requires an open recovery journal",
			ErrConditionalPrepareUnsupportedJournal,
		)
	}
	plan, err := c.journal.PrepareConditionalBatch(c.batchJournalEntries)
	if err != nil {
		return err
	}
	if !c.journal.PreparedBatchFits(plan) {
		return storeio.ErrRecoveryJournalFull
	}
	if _, err := c.journal.AppendPreparedConditionalBatch(
		staged.generation, markerID, markerEpoch, txnID,
		c.batchJournalEntries, plan,
	); err != nil {
		return c.poisonJournalCommitOutcomeUnknown(err)
	}
	if forceSync {
		if err := c.journal.Sync(c.journalPowerSafe); err != nil {
			return c.poisonJournalCommitOutcomeUnknown(err)
		}
	}
	c.journalAcks.Add(1)
	return nil
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
	bounds := storeio.CommonPrimaryLeafBounds{
		FileEnd:           c.batchPrimaryFileEnd,
		NextLogicalID:     c.batchPrimaryNextLogicalID,
		AllocationQuantum: state.root.PageSize,
	}
	pressure := false
	stablePressure := false
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		if leaf.stableSlots {
			for mutationAt := leaf.mutationAt; mutationAt < leaf.mutationEnd; mutationAt++ {
				mutation := &c.batchPrimaryMutations[mutationAt]
				if !mutation.found {
					continue
				}
				oldRaw, found, resolveErr := c.resolvePrimaryGraph(
					c.overflowValueScratch[:0], state, mutation.key,
				)
				if resolveErr != nil {
					c.unwindPrimaryExactPrepared(&prepared)
					return primaryExactPrepared{}, resolveErr
				}
				if !found {
					c.unwindPrimaryExactPrepared(&prepared)
					return primaryExactPrepared{}, storeio.ErrPrimaryExactIndexCorrupt
				}
				c.overflowValueScratch = oldRaw
				ok, deltaErr := c.preparePrimaryExactDeltaRaw(
					epoch, &prepared, mutation.resident,
					oldRaw, mutation.value, mutation.remove, true,
					mutation.oldSlot, mutation.oldSlot, generation,
				)
				if deltaErr != nil {
					c.unwindPrimaryExactPrepared(&prepared)
					return primaryExactPrepared{}, deltaErr
				}
				if !ok {
					pressure = true
					stablePressure = true
					break
				}
			}
			if pressure {
				break
			}
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
	if stablePressure {
		return primaryExactPrepared{}, errPrimaryBatchExactCheckpointRequired
	}
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
	if err := c.canonicalizePrimaryBatchValues(batch); err != nil {
		return err
	}
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
			if len(value) == 0 || len(value) > c.options.MaxDocumentBytes {
				return ErrDocumentTooLarge
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
	// Atomic and conditional journal batches use a canonical strict key order.
	// WriteBatch has already deduplicated keys, and final-key mutations commute,
	// so this changes neither the published state nor generation semantics.
	slices.SortFunc(
		c.batchJournalEntries,
		func(a, b storeio.RecoveryBatchEntry) int {
			return bytes.Compare(a.Key, b.Key)
		},
	)
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
	return c.planPrimaryBatchOverflow(state)
}

// planPrimaryBatchOverflow assigns every new out-of-line value a stable volatile
// PageRef range. It performs no cache admission: the complete batch can still be
// rejected for topology, journal, retirement, or cache pressure without leaving
// a dirty frame behind. All chains precede all rewritten leaves, making one
// monotonically checked FileEnd interval sufficient for the generation.
func (c *Collection) planPrimaryBatchOverflow(state *fileStoreState) error {
	generation := state.root.Generation + 1
	if generation == 0 || generation >= uint64(1)<<48 {
		return storeio.ErrGenerationOrder
	}
	running := state.fileEnd
	if len(c.primaryPendingParents) == 0 {
		var reserveErr error
		running, reserveErr = c.primaryVolatileReservationEnd(running)
		if reserveErr != nil {
			return reserveErr
		}
	}
	nextLogicalID := state.root.NextLogicalID
	c.batchPrimaryOverflowPages = 0
	c.batchPrimaryOverflowDirty = 0
	for i := range c.batchPrimaryMutations {
		mutation := &c.batchPrimaryMutations[i]
		mutation.stored = storeio.CommonPrimaryLeafValue{}
		if mutation.remove {
			continue
		}
		if c.primaryOverflowValueIsInline(len(mutation.value)) {
			mutation.stored.Inline = mutation.value
			continue
		}
		head, bytes, pages, err := c.planBufferedPrimaryOverflowChain(
			mutation.value, generation, running, nextLogicalID,
		)
		if err != nil {
			return err
		}
		mutation.stored.Overflow = head
		if running > math.MaxUint64-bytes ||
			uint64(pages) > math.MaxUint64-nextLogicalID {
			return storeio.ErrInvalidWrite
		}
		running += bytes
		nextLogicalID += uint64(pages)
		if pages > math.MaxInt-c.batchPrimaryOverflowPages {
			return storeio.ErrInvalidWrite
		}
		c.batchPrimaryOverflowPages += pages
		remaining := len(mutation.value)
		for remaining != 0 {
			piece, extent, ok := c.primaryOverflowExtentGeometry(remaining)
			if !ok {
				return storeio.ErrInvalidWrite
			}
			reserved := c.cache.ReservationBytes(extent)
			if c.batchPrimaryOverflowDirty > math.MaxUint64-reserved {
				return storeio.ErrInvalidWrite
			}
			c.batchPrimaryOverflowDirty += reserved
			remaining -= piece
		}
	}
	c.batchPrimaryOverflowFileEnd = running
	c.batchPrimaryNextLogicalID = nextLogicalID
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
// before it admits a single frame, checkpointing to make room exactly as the
// single-document path does. It reports whether it checkpointed, because a
// checkpoint moves the published cut and the caller must re-plan. This runs
// after the read-only leaf/overflow plan, so every byte and retirement is exact:
//
//   - one pending-parent slot per newly touched leaf (a checkpoint drains the set);
//   - room in the preallocated journal for the whole batch record;
//   - dirty-cache room for every rounded overflow extent and encoded leaf frame;
//   - deferred retirement room for superseded leaves and volatile overflow pages;
//   - durable retirement room for every superseded on-device overflow page;
//   - admission bookkeeping for every new chain page and leaf.
func (c *Collection) ensurePrimaryBatchCapacity(conditional bool) (bool, error) {
	before := c.automaticCheckpoints.Load()
	newDistinct := 0
	liveLeaves := 0
	volatileRetirements := len(c.batchPrimaryOverflowVolatile)
	requiredDirty := c.batchPrimaryOverflowDirty
	for i := range c.batchPrimaryLeaves {
		leaf := &c.batchPrimaryLeaves[i]
		if leaf.skip {
			continue
		}
		liveLeaves++
		if leaf.pendingIndex < 0 {
			newDistinct++
		}
		if leaf.pending.volatileRef != (storeio.PageRef{}) {
			volatileRetirements++
		}
		reserved := c.cache.ReservationBytes(uint32(leaf.imageLength))
		if requiredDirty > math.MaxUint64-reserved {
			return false, storeio.ErrInvalidWrite
		}
		requiredDirty += reserved
	}
	if len(c.primaryPendingParents)+newDistinct > cap(c.primaryPendingParents) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return false, err
		}
		c.automaticCheckpoints.Add(1)
		return true, nil
	}
	var journalErr error
	if conditional {
		journalErr = c.ensurePrimaryBatchConditionalJournalRoom(c.batchJournalEntries)
	} else {
		journalErr = c.ensurePrimaryBatchJournalRoom(c.batchJournalEntries)
	}
	if journalErr != nil {
		return false, journalErr
	}
	if c.automaticCheckpoints.Load() != before {
		return true, nil
	}
	if c.cache.DirtyCapacityAvailable() < requiredDirty {
		if err := c.checkpointBufferedLocked(); err != nil {
			return false, err
		}
		c.automaticCheckpoints.Add(1)
		if c.cache.DirtyCapacityAvailable() < requiredDirty {
			return false, fmt.Errorf(
				"%w: ordered primary batch needs %d dirty bytes",
				storeio.ErrPageCachePinned, requiredDirty,
			)
		}
		return true, nil
	}
	// setupResidentPrimaryLocked allocates this queue with exactly
	// MaxRetiredExtents capacity. Check both the configured invariant and the
	// concrete slice before any admission; publish can then append by direct copy
	// with no allocation and no error path.
	if cap(c.primaryPendingOverflowRetire) != c.options.MaxRetiredExtents {
		return false, fmt.Errorf(
			"%w: ordered primary durable overflow retirement capacity %d, want %d",
			storeio.ErrInvalidWrite, cap(c.primaryPendingOverflowRetire),
			c.options.MaxRetiredExtents,
		)
	}
	if len(c.batchPrimaryOverflowDurable) > cap(c.primaryPendingOverflowRetire) {
		return false, fmt.Errorf(
			"%w: ordered primary batch durable overflow retirements %d",
			storeio.ErrRetiredExtentCapacity,
			len(c.batchPrimaryOverflowDurable),
		)
	}
	if len(c.primaryPendingOverflowRetire)+
		len(c.batchPrimaryOverflowDurable) > cap(c.primaryPendingOverflowRetire) {
		if err := c.checkpointBufferedLocked(); err != nil {
			return false, err
		}
		c.automaticCheckpoints.Add(1)
		return true, nil
	}
	c.clearPrimaryVolatileRetiredLocked()
	volatileNeeded := len(c.primaryVolatileRetired) + volatileRetirements
	if volatileNeeded > c.options.MaxRetiredExtents {
		c.retirementPressureCheckpoints.Add(1)
		if err := c.checkpointBufferedLocked(); err != nil &&
			!errors.Is(err, storeio.ErrRetiredExtentCapacity) {
			return false, err
		}
		c.clearPrimaryVolatileRetiredLocked()
		volatileNeeded = len(c.primaryVolatileRetired) + volatileRetirements
		if volatileNeeded > c.options.MaxRetiredExtents {
			return false, c.absorbRetirementPressure(fmt.Errorf(
				"%w: buffered primary volatile-reference capacity %d",
				storeio.ErrRetiredExtentCapacity,
				c.options.MaxRetiredExtents,
			))
		}
		c.automaticCheckpoints.Add(1)
		return true, nil
	}
	if volatileNeeded > cap(c.primaryVolatileRetired) {
		grown := slices.Grow(
			c.primaryVolatileRetired,
			volatileNeeded-len(c.primaryVolatileRetired),
		)
		// slices.Grow may over-allocate geometrically. Hide that spare backing
		// capacity so the single-mutation lane, which treats cap as its logical
		// retirement bound, cannot silently exceed the exact admitted high-water.
		c.primaryVolatileRetired = grown[:len(grown):volatileNeeded]
	}
	if liveLeaves > math.MaxInt-c.batchPrimaryOverflowPages {
		return false, storeio.ErrInvalidWrite
	}
	requiredFrames := c.batchPrimaryOverflowPages + liveLeaves
	if requiredFrames > cap(c.batchPrimaryAdmitted) {
		c.batchPrimaryAdmitted = slices.Grow(
			c.batchPrimaryAdmitted,
			requiredFrames-len(c.batchPrimaryAdmitted),
		)
	}
	return false, nil
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
	c.batchPrimaryOverflowVolatile = c.batchPrimaryOverflowVolatile[:0]
	c.batchPrimaryOverflowDurable = c.batchPrimaryOverflowDurable[:0]
	running := c.batchPrimaryOverflowFileEnd
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
	leaf.stableSlots = c.journalReplaying && c.primaryUniqueReplayValidated &&
		state.root.IndexCount != 0
	inputBounds := c.primaryLeafBounds(state)
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
	if storeio.PrimaryLeafClass(page) != storeio.CommonPrimaryLeafCompact {
		return nil, storeio.ErrCommonPrimaryLeafCorrupt
	}
	stripe, ok := storeio.AdmittedCompactPrimaryStripe(
		page, c.storeID, leaf.resident.Bucket,
	)
	if !ok {
		return nil, storeio.ErrCommonPrimaryLeafCorrupt
	}
	baseRows, err := stripe.RenderRecordsWithScratch(c.primaryLeafMutationScratch)
	if err != nil {
		return nil, err
	}
	if err := c.collectPrimaryBatchOverflowRetirements(
		baseRows, leaf, inputBounds,
	); err != nil {
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
	if leaf.stableSlots {
		for mutationAt := leaf.mutationAt; mutationAt < leaf.mutationEnd; mutationAt++ {
			if !c.batchPrimaryMutations[mutationAt].found {
				leaf.stableSlots = false
				break
			}
		}
	}
	leaf.docDelta = len(final) - len(baseRows)
	leaf.finalLen = len(final)
	if len(final) > storeio.CommonPrimaryLeafWideSlots {
		c.primaryLeafSplitRequired.Add(1)
		return c.primaryBatchProspectiveSplitKey(final), errors.Join(
			ErrPrimaryLeafSplitRequired, storeio.ErrCommonPrimaryLeafFull,
		)
	}
	if !leaf.stableSlots {
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
	}
	leaf.frameGen = baseGen + 1
	image, err := storeio.EncodeBestCompactPrimaryStripe(
		c.primaryLeafScratch,
		storeio.CommonPrimaryLeafHeader{
			StoreID: c.storeID, Generation: leaf.frameGen,
			Bucket: leaf.resident.Bucket,
		},
		c.storeID, final, c.primaryUnifiedBuilder,
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
					Key: mutation.key, Value: mutation.stored,
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
					Key: mutation.key, Value: mutation.stored,
				})
				applied++
			}
			mutationAt++
		default:
			mutation.found = true
			mutation.oldSlot = base.Slot
			if !mutation.remove {
				final = append(final, storeio.CommonPrimaryLeafRecord{
					Key: mutation.key, Value: mutation.stored, Slot: base.Slot,
				})
			}
			applied++
			baseAt++
			mutationAt++
		}
	}
	return final, applied, nil
}

// collectPrimaryBatchOverflowRetirements enumerates old chains replaced or
// deleted in this leaf. A chain at or above the checkpoint base is volatile and
// can be dropped with its old leaf after the atomic router flip; a lower chain
// is durable and must be retired by the next checkpoint transaction.
func (c *Collection) collectPrimaryBatchOverflowRetirements(
	baseRows []storeio.CommonPrimaryLeafRecord,
	leaf *primaryBatchLeaf,
	bounds storeio.CommonPrimaryLeafBounds,
) error {
	baseAt := 0
	mutationAt := leaf.mutationAt
	checkpointBase := c.primaryCheckpointBaseState()
	for baseAt < len(baseRows) && mutationAt < leaf.mutationEnd {
		mutation := &c.batchPrimaryMutations[mutationAt]
		order := bytes.Compare(baseRows[baseAt].Key, mutation.key)
		switch {
		case order < 0:
			baseAt++
		case order > 0:
			mutationAt++
		default:
			old := baseRows[baseAt].Value
			if old.IsOverflow() {
				if checkpointBase != nil &&
					old.Overflow.Offset >= checkpointBase.fileEnd {
					extents, err := c.collectPrimaryOverflowExtents(
						c.batchPrimaryOverflowVolatile,
						old.Overflow, bounds,
					)
					if err != nil {
						return err
					}
					c.batchPrimaryOverflowVolatile = extents
				} else {
					extents, err := c.collectPrimaryOverflowExtents(
						c.batchPrimaryOverflowDurable,
						old.Overflow, bounds,
					)
					if err != nil {
						return err
					}
					c.batchPrimaryOverflowDurable = extents
				}
			}
			baseAt++
			mutationAt++
		}
	}
	return nil
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

// admitPrimaryBatchOverflow materializes every pre-planned new chain. None of
// these PageRefs is reachable yet; batchPrimaryAdmitted records the exact prefix
// so any encode/cache/exact/WAL failure can discard all staged frames.
func (c *Collection) admitPrimaryBatchOverflow() error {
	generation := uint64(0)
	for i := range c.batchPrimaryMutations {
		mutation := &c.batchPrimaryMutations[i]
		if mutation.remove || !mutation.stored.IsOverflow() {
			continue
		}
		if generation == 0 {
			generation = mutation.stored.Overflow.Generation
		} else if generation != mutation.stored.Overflow.Generation {
			return storeio.ErrGenerationOrder
		}
		if err := c.admitBufferedPrimaryOverflowChain(
			mutation.value, generation, mutation.stored.Overflow,
			c.batchPrimaryFileEnd, c.batchPrimaryNextLogicalID,
			&c.batchPrimaryAdmitted,
		); err != nil {
			return err
		}
	}
	return nil
}

// admitPrimaryBatchLeaves admits every rewritten leaf frame into the cache as a
// buffered dirty page. The frames are not reader-visible until publishPrimaryBatch
// flips the router, so admitting them all before the journal fence changes nothing
// a reader can observe. Capacity was reserved, so a failure here is unexpected and
// unadmits what it staged, leaving nothing visible and nothing journaled.
func (c *Collection) admitPrimaryBatchLeaves() error {
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
	c.snapshotGate.Lock()
	c.publishPrimaryBatchGateHeld(stagedPrimaryBatch{
		state:         state,
		generation:    generation,
		preparedExact: preparedExact,
		live:          true,
	})
	c.snapshotGate.Unlock()
}

// publishPrimaryBatchGateHeld is the gate-held body of publishPrimaryBatch.
// Multi-collection commit acquires every participant's snapshotGate write side
// in process-global order before calling this per member; single-collection
// Update acquires the gate itself via publishPrimaryBatch. The writer must
// already be held. Infallible by construction — see publishPrimaryBatch.
func (c *Collection) publishPrimaryBatchGateHeld(staged stagedPrimaryBatch) {
	state := staged.state
	generation := staged.generation
	preparedExact := staged.preparedExact
	totalDelta := 0
	for i := range c.batchPrimaryLeaves {
		if c.batchPrimaryLeaves[i].skip {
			continue
		}
		totalDelta += c.batchPrimaryLeaves[i].docDelta
	}
	nextRoot := state.root
	nextRoot.Generation = generation
	nextRoot.NextLogicalID = c.batchPrimaryNextLogicalID
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
	for _, prev := range c.batchPrimaryOverflowVolatile {
		c.retirePrimaryVolatileRefLocked(prev)
	}
	c.endReaderFence()
	if len(c.batchPrimaryOverflowDurable) != 0 {
		// ensurePrimaryBatchCapacity proved len+batch <= cap, and setup fixes cap to
		// MaxRetiredExtents. append therefore neither allocates nor exposes a
		// recoverable failure after the router/state publication point of no return.
		c.primaryPendingOverflowRetire = append(
			c.primaryPendingOverflowRetire,
			c.batchPrimaryOverflowDurable...,
		)
	}

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
