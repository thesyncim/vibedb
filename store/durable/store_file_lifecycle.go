package durable

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// unlockCollectionWriter is a lifecycle fault seam. Production always uses
// the platform writer-lock release; tests interpose one failed attempt to prove
// Close resumes at the final phase without touching consumed resources.
var unlockCollectionWriter = storeio.UnlockWriter

func (c *Collection) ensureDirtyCapacityFor(
	transactionPages int, transactionBytes uint64,
) error {
	required := transactionBytes
	if c.cache.DirtyCapacityAvailable() >= required &&
		!c.committer.NeedsFrameCheckpointFor(transactionPages) {
		return nil
	}
	if err := c.checkpointGroupPhysicalFence(); err != nil {
		return err
	}
	var err error
	if c.buffered() {
		err = c.checkpointBufferedLocked()
	} else {
		err = c.committer.Flush()
	}
	if err != nil {
		return err
	}
	c.automaticCheckpoints.Add(1)
	if !c.buffered() {
		c.cache.MarkDurable(c.committer.DurableGeneration())
	}
	return nil
}

func (c *Collection) rememberRetiredRef(ref storeio.PageRef) {
	if ref == (storeio.PageRef{}) ||
		len(c.retireRefScratch) == cap(c.retireRefScratch) {
		return
	}
	c.retireRefScratch = append(c.retireRefScratch, ref)
}

// reserveFileRetirements hands the complete list to the reclaimer. It runs after
// syncFreeLogFor so that the free log's own superseded pages — which a fold only
// knows once it has decided to fold — are reserved with everything else, and so
// that a failure here still precedes Publish and rolls the whole commit back.
//
// A full retirement table is routed through absorbRetirementPressure so the
// error identifies either the reader pin or the undersized transaction bound.
// absorbRetirementPressure turns a retired-extent capacity failure into a
// caller-actionable error, distinguishing reader-pinned extents (release the
// snapshot or let the direct read finish) from a genuinely exhausted table
// (raise MaxRetiredExtents).
func (c *Collection) absorbRetirementPressure(err error) error {
	if c == nil || !errors.Is(err, storeio.ErrRetiredExtentCapacity) {
		return err
	}
	retired := c.reclaimer.Stats()
	current := c.visibleLogicalViewNoError().generation
	readers := c.readerSummary(current)
	if readers.active != 0 && readers.minimum <= current {
		return fmt.Errorf(
			"%w: %d of %d retired extents (%d bytes) are pinned by %d active reader(s) "+
				"(%d snapshot lease(s), %d direct read epoch(s)); the oldest physical "+
				"retention generation is %d against logical generation %d; close snapshots "+
				"or let current reads finish, or raise Options.MaxRetiredExtents",
			err, retired.Pending, retired.Capacity, retired.PendingBytes,
			readers.active, readers.snapshots, readers.direct,
			readers.minimum, current)
	}
	return fmt.Errorf(
		"%w: committing %d retired extents would exceed the capacity of %d; "+
			"nothing was published or abandoned; raise Options.MaxRetiredExtents",
		err, len(c.retireScratch), retired.Capacity)
}

// retryRetirementAfterPressure forces a checkpoint (buffered) or flush to
// advance the durable floor, absorbs whatever that frees back into the reusable
// set, and retries the retirement once. It reports ErrRetiredExtentCapacity
// when no reader is present to free anything.
func (c *Collection) retryRetirementAfterPressure() error {
	if err := c.checkpointGroupPhysicalFence(); err != nil {
		return err
	}
	current := c.visibleLogicalViewNoError().generation
	if c.readerSummary(current).active == 0 {
		return storeio.ErrRetiredExtentCapacity
	}
	c.retirementPressureCheckpoints.Add(1)
	var err error
	if c.buffered() {
		err = c.checkpointBufferedLocked()
	} else {
		err = c.committer.Flush()
		if err == nil {
			c.cache.MarkDurable(c.committer.DurableGeneration())
		}
	}
	if err != nil {
		return err
	}
	absorbed, err := c.reclaimer.AppendReusable(
		c.retirementAbsorbed[:0], current,
		c.committer.FallbackGeneration(), cap(c.retirementAbsorbed),
	)
	if err != nil {
		return err
	}
	c.retirementAbsorbed = absorbed
	// AppendReusable replaced the prior absorbed window rather than extending
	// it, so restart its tiny linear hole-punch source at the new prefix.
	c.holePunchAbsorbedCursor = 0
	if len(absorbed) == 0 {
		return storeio.ErrRetiredExtentCapacity
	}
	return c.reclaimer.RetireBatch(c.retireScratch)
}

func (c *Collection) reserveFileRetirements() error {
	if err := c.reclaimer.RetireBatch(c.retireScratch); err != nil {
		if errors.Is(err, storeio.ErrRetiredExtentCapacity) {
			if retryErr := c.retryRetirementAfterPressure(); retryErr == nil {
				return nil
			} else if !errors.Is(retryErr, storeio.ErrRetiredExtentCapacity) {
				return retryErr
			}
		}
		return c.absorbRetirementPressure(err)
	}
	return nil
}

func (c *Collection) waitPublished(generation uint64) error {
	if err := c.committer.Wait(generation); err != nil {
		return err
	}
	c.cache.MarkDurable(generation)
	return nil
}

// Flush waits until the current reader-visible generation is crash-safe.
// Ordinary buffered-visible stores may satisfy this with a recovery-journal
// batch over the unchanged durable root; pressure, exceptional mutations,
// snapshots, and Close still fold a complete physical root.
func (c *Collection) Flush() error {
	if c == nil || c.committer == nil {
		return ErrClosed
	}
	if err := c.rejectCheckpointGroupOwner(); err != nil {
		return err
	}
	if c.buffered() {
		if concurrentPrimaryExclusiveWaitHook != nil {
			concurrentPrimaryExclusiveWaitHook("flush")
		}
		c.writer.Lock()
		defer c.writer.Unlock()
		if err := c.rejectCheckpointGroupOwner(); err != nil {
			return err
		}
		if c.closed {
			return ErrClosed
		}
		deltaLane := c.bufferedJournalDeltaLane()
		handled, err := c.checkpointBufferedJournalDeltaLocked()
		if err != nil || handled {
			return err
		}
		if deltaLane {
			c.journalDeltaFullFallbacks.Add(1)
		}
		return c.checkpointBufferedLocked()
	}
	// The journal-backed synchronous lane already has durable redo records but
	// Flush folds and recycles them. Async-visible and the chain-fence sync lane
	// already publish through the committer and only need to wait.
	if c.syncJournalLane() {
		c.writer.Lock()
		defer c.writer.Unlock()
		if err := c.rejectCheckpointGroupOwner(); err != nil {
			return err
		}
		if c.closed {
			return ErrClosed
		}
		return c.checkpointBufferedLocked()
	}
	generation := c.Generation()
	if err := c.committer.Wait(generation); err != nil {
		return err
	}
	// Wait is outside writer so an explicit Flush does not hold publication
	// serialization across device latency. Reacquire it only for the bounded
	// post-fence work: this freezes allocator/reclaimer views, and Wait has
	// already completed the durable-state callback for generation. These lanes
	// cannot carry a recovery journal (async-created roots mint none, and a
	// journal-backed synchronous open selected syncJournalLane above).
	c.writer.Lock()
	defer c.writer.Unlock()
	if err := c.rejectCheckpointGroupOwner(); err != nil {
		return err
	}
	if c.closed {
		return ErrClosed
	}
	return c.completePhysicalDurabilityLocked(generation)
}

// Close fences every publication and releases bounded I/O resources. It does
// not close the caller-owned file. Active snapshots must be closed first.
func (c *Collection) Close() error {
	if c == nil {
		return nil
	}
	if err := c.rejectActiveCheckpointGroupOwner(); err != nil {
		return err
	}
	c.writer.Lock()
	if err := c.rejectActiveCheckpointGroupOwner(); err != nil {
		c.writer.Unlock()
		return err
	}
	if c.closeDone {
		result := c.closeErr
		c.writer.Unlock()
		return result
	}
	c.closed = true
	// Close reader admission before releasing writer so every later fast-path
	// read is diverted to an already-closed lease registry. Existing readers
	// drain normally and keep final teardown retryable.
	c.leases.BeginClose()
	c.readEpochs.BeginClose()
	if c.primaryConcurrentContexts != nil {
		c.primaryConcurrentContexts.close()
	}
	c.writer.Unlock()
	if c.mutationCombiner != nil {
		for _, request := range c.mutationCombiner.stop() {
			request.err = ErrClosed
			close(request.done)
		}
	}
	c.mutationWait.Wait()
	if c.primaryConcurrentContexts != nil {
		c.primaryConcurrentContexts.waitDrained()
	}
	// DurabilitySync publishers release the construction lock before their
	// durability wait so independent writers can share one device commit.
	// Closed prevents any new waiter from registering before this drain.
	c.durabilityWait.Wait()
	c.writer.Lock()
	defer c.writer.Unlock()
	// Concurrent Close calls may both have observed closeDone before waiting
	// for the last synchronous publisher. Recheck under the resource lock so
	// only one caller detaches and closes the mmap-backed arenas.
	if c.closeDone {
		return c.closeErr
	}
	if c.closePhase < closePhasePersistence {
		// Fold the final deferred window into a durable root and recycle the
		// journal so a reopen replays nothing. This boundary is recorded before
		// reader/resource teardown: a retry after a pinned reader or failed unlock
		// must never rerun persistence against resources already being detached.
		var persistErr error
		if c.buffered() || c.syncJournalLane() {
			persistErr = c.checkpointBufferedLocked()
		} else {
			// Async-visible and chain-fence synchronous Close have drained every
			// publisher, so this physical boundary is a stable final cut.
			persistErr = c.flushPublishedPhysicalLocked()
		}
		if persistErr != nil {
			// A sticky persistence failure is terminal for this live handle; finish
			// releasing resources and let recovery repair/select the image on reopen.
			// A non-persistence failure has not consumed the boundary and remains
			// retryable from this phase.
			if c.PersistenceError() == nil {
				return persistErr
			}
			c.rememberCloseError(persistErr)
		}
		c.closePhase = closePhasePersistence
	}
	// The epoch table closes before the lease table for the same reason the
	// lease table closes before resources: a still-in-flight direct read must
	// fail Close (and divert every later read) rather than race the arena
	// release below.
	if c.closePhase < closePhaseReadEpochs {
		if err := c.readEpochs.Close(); err != nil {
			return errors.Join(c.closeErr, err)
		}
		c.closePhase = closePhaseReadEpochs
	}
	if c.closePhase < closePhaseLeases {
		if err := c.leases.Close(); err != nil {
			return errors.Join(c.closeErr, err)
		}
		c.closePhase = closePhaseLeases
	}
	completed, closeErr := c.closeResourcesLocked()
	if !completed {
		return closeErr
	}
	c.closeDone = true
	return c.closeErr
}

// CloseCompleted reports whether Close has released every engine-owned
// resource and the writer lock. It deliberately differs from the admission
// state: the first Close attempt rejects new operations immediately, while a
// retryable cleanup error can leave teardown incomplete. Ownership adapters
// use this result to detach a collection that completed teardown with a sticky
// persistence error without mistaking a retryable failure for completion.
func (c *Collection) CloseCompleted() bool {
	if c == nil {
		return true
	}
	c.writer.Lock()
	done := c.closeDone
	c.writer.Unlock()
	return done
}

func (c *Collection) closeResources() error {
	c.writer.Lock()
	defer c.writer.Unlock()
	// Construction/recovery failure has no admitted readers. Jump directly to
	// the resource state machine so it can share the same monotonic cleanup.
	if c.closePhase < closePhaseLeases {
		c.closePhase = closePhaseLeases
	}
	_, err := c.closeResourcesLocked()
	return err
}

// closeResourcesLocked detaches every view into an external block before
// releasing that block. Stats uses the same writer lock, so it can observe
// either a complete live resource set or the detached state, never a slice or
// reclaimer whose backing mmap has already been unmapped.
func (c *Collection) closeResourcesLocked() (bool, error) {
	if c.closePhase < closePhaseJournal {
		c.rememberCloseError(c.closeRecoveryJournalLocked())
		c.closePhase = closePhaseJournal
	}
	if c.closePhase < closePhaseCommitter {
		if c.committer != nil {
			c.rememberCloseError(c.committer.Close())
			if c.cache != nil {
				c.cache.MarkDurable(c.committer.DurableGeneration())
			}
		}
		c.closePhase = closePhaseCommitter
	}
	if c.closePhase < closePhasePageCache {
		if c.cache != nil {
			if err := c.cache.Close(); err != nil {
				if errors.Is(err, storeio.ErrPageCachePinned) {
					return false, errors.Join(c.closeErr, err)
				}
				// The cache has already detached its arena for every other Close
				// error, so preserve the error but advance instead of retrying a
				// consumed release.
				c.rememberCloseError(err)
			}
		}
		c.closePhase = closePhasePageCache
	}
	if c.closePhase < closePhaseFiles {
		if c.readFile != nil {
			readFile := c.readFile
			c.readFile = nil
			c.rememberCloseError(readFile.Close())
		}
		if c.writeFile != nil {
			writeFile := c.writeFile
			c.writeFile = nil
			c.rememberCloseError(writeFile.Close())
		}
		c.closePhase = closePhaseFiles
	}
	if c.closePhase < closePhaseBlocks {
		reusableBlock := c.reusableBlock
		c.reclaimer = nil
		c.reusableBlock = nil
		c.reusable = nil
		c.freeExtentIndex = storeio.FreeExtentIndex{}
		c.freeExtentMaxima = nil
		if reusableBlock != nil {
			c.rememberCloseError(reusableBlock.Close())
		}
		if c.freeScratchBlock != nil {
			block := c.freeScratchBlock
			c.freeScratchBlock = nil
			c.freeFenced = nil
			c.freeImageScratch = nil
			c.freeFoldRanges = nil
			c.freeFoldOrder = nil
			c.rememberCloseError(block.Close())
		}
		if c.materializationBlock != nil {
			block := c.materializationBlock
			c.materializationBlock = nil
			c.materializationBefore = nil
			c.materializationAfter = nil
			c.rememberCloseError(block.Close())
		}
		c.closePhase = closePhaseBlocks
	}
	if c.closePhase < closePhaseUnlocked {
		if c.writerLocked {
			if err := unlockCollectionWriter(c.file); err != nil {
				// Unlock is the sole retryable operation after resource
				// consumption. Do not cache it as terminal; the next Close resumes
				// exactly here and a successful retry clears the condition.
				return false, errors.Join(c.closeErr, err)
			}
			c.writerLocked = false
		}
		c.closePhase = closePhaseUnlocked
	}
	return true, c.closeErr
}

func (c *Collection) rememberCloseError(err error) {
	if err == nil || errors.Is(c.closeErr, err) {
		return
	}
	if c.closeErr == nil {
		c.closeErr = err
		return
	}
	c.closeErr = errors.Join(c.closeErr, err)
}
