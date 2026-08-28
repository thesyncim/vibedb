package storeio

import (
	"fmt"
	"os"
	"runtime"
	"slices"
	"time"
)

const opportunisticPrewriteLimit = 16

func (c *Committer) stopAccepting() {
	c.closing.Store(true)
	for c.publishers.Load() != 0 {
		runtime.Gosched()
	}
}

func (c *Committer) run(file *os.File, initialized chan<- committerInit, open deviceOpener) {
	locked := c.deviceOptions.Backend != BackendPortable
	if locked {
		runtime.LockOSThread()
		c.deviceOptions.SingleIssuer = true
	}
	device, err := open(file, c.deviceOptions)
	if err != nil {
		if locked {
			runtime.UnlockOSThread()
		}
		c.setFailure(err)
		c.notifyFailureDrained()
		initialized <- committerInit{err: err}
		close(c.done)
		c.broadcast()
		return
	}
	if locked && device.Backend() != BackendIOUring {
		runtime.UnlockOSThread()
		locked = false
	}
	if locked {
		defer runtime.UnlockOSThread()
	}
	c.device = device
	c.backend = device.Backend()
	c.buffers = make([][]byte, c.bufferCount)
	for i := range c.buffers {
		buffer, bufferErr := device.Buffer(i)
		if bufferErr != nil {
			_ = device.Close()
			c.setFailure(bufferErr)
			initialized <- committerInit{err: bufferErr}
			close(c.done)
			c.broadcast()
			return
		}
		c.buffers[i] = buffer
	}
	initialized <- committerInit{}
	defer func() {
		if closeErr := device.Close(); closeErr != nil {
			c.setFailure(closeErr)
		}
		c.notifyFailureDrained()
		close(c.done)
		c.broadcast()
	}()

	var coalesce *time.Timer
	if c.options.CoalesceDelay != 0 {
		coalesce = time.NewTimer(time.Hour)
		coalesce.Stop()
		defer coalesce.Stop()
	}
	for {
		if !c.waitForCheckpointRequest() {
			return
		}
		checkpointTarget := c.checkpointThrough.Load()
		batch, ok := c.nextBatch(true)
		if !ok {
			return
		}
		if batch.materialized {
			if err := c.commitMaterialized(device, batch); err != nil {
				c.setFailure(err)
				c.release(batch)
				c.drainFailed()
				return
			}
			continue
		}
		// The first publication has already returned to an asynchronous
		// producer. A short bounded window lets nearby generations share both
		// durability barriers; Close interrupts it and drains immediately.
		//
		// Waiting unconditionally is what made this window unusable for a lone
		// synchronous writer, whose next Put cannot even begin until this commit
		// is durable: the window could never find anything to group and instead
		// added its full length to every acknowledged commit.
		if coalesce != nil && !c.closing.Load() && c.coalesceWorthwhile() {
			coalesce.Reset(c.options.CoalesceDelay)
			select {
			case <-coalesce.C:
			case <-c.stop:
				coalesce.Stop()
			}
		}
		c.groupScratch = c.groupScratch[:1]
		c.groupScratch[0] = batch
		c.commitScratch = c.commitScratch[:len(batch.pages)]
		copy(c.commitScratch, batch.pages)
		latest := batch
		groupLimit := c.options.GroupLimit
		if c.options.ManualCheckpoint {
			groupLimit = cap(c.groupScratch)
		}
		for len(c.groupScratch) < groupLimit {
			next, exists := c.peekBatch()
			if !exists || next.materialized ||
				(c.options.ManualCheckpoint && next.generation > checkpointTarget) ||
				len(c.commitScratch)+len(next.pages) > cap(c.commitScratch) {
				break
			}
			next, _ = c.nextBatch(false)
			groupIndex := len(c.groupScratch)
			c.groupScratch = c.groupScratch[:groupIndex+1]
			c.groupScratch[groupIndex] = next
			writeIndex := len(c.commitScratch)
			c.commitScratch = c.commitScratch[:writeIndex+len(next.pages)]
			copy(c.commitScratch[writeIndex:], next.pages)
			latest = next
		}
		// Manual buffered publication may have released checkpoint ownership
		// for exact frame writes a later accepted generation proved unreachable.
		// Their descriptors remain in the generation batch for bounded queue
		// accounting, but the recycled buffers may already hold unrelated
		// bytes and must never reach Device.
		if c.options.ManualCheckpoint {
			out := c.commitScratch[:0]
			var barrierWitness Write
			haveBarrierWitness := false
			for _, write := range c.commitScratch {
				if write.pendingFlags&pendingWriteSuperseded != 0 {
					continue
				}
				if write.pendingFlags&pendingWritePrewritten != 0 &&
					c.prewriteStillCurrent(write) {
					if !haveBarrierWitness {
						barrierWitness = write
						haveBarrierWitness = true
					}
					continue
				}
				if write.pendingFlags&pendingWriteTailWitness != 0 {
					c.tailWitnessWrites.Add(1)
					c.tailWitnessBytes.Add(uint64(write.Length))
				}
				out = append(out, write)
			}
			// Device.Commit's data barrier is conditional on at least one data
			// descriptor. If every page was pre-written, rewrite one as a
			// bounded barrier witness so those earlier writes are ordered and
			// stable before the alternate root can publish them.
			if len(out) == 0 && haveBarrierWitness {
				out = append(out, barrierWitness)
			}
			c.commitScratch = out
		}
		slices.SortFunc(c.commitScratch, func(a, b Write) int {
			if a.Offset < b.Offset {
				return -1
			}
			if a.Offset > b.Offset {
				return 1
			}
			return 0
		})
		// Always preserve the last durable superblock as the recovery fallback.
		// Grouping may publish generation N+2 directly over durable N, so the
		// The generation-derived slot prepared by SetInlineSuperblock is not
		// authoritative.
		// Toggle only after Device.Commit completes both durability barriers.
		rootSlot := c.nextRootSlot.Load()
		if latest.rootGeneration != 0 {
			layout, layoutErr := MutableStoreLayout(latest.root.Length)
			if layoutErr != nil {
				c.setFailure(layoutErr)
				for _, grouped := range c.groupScratch {
					c.release(grouped)
				}
				c.drainFailed()
				return
			}
			latest.root.Offset = int64(layout.RootOffsets[rootSlot])
		}
		committedBytes := uint64(latest.root.Length)
		for _, write := range c.commitScratch {
			committedBytes += uint64(write.Length)
		}
		if err := c.bindDeviceFrameArena(
			device, c.commitScratch,
		); err != nil {
			c.setFailure(err)
			for _, grouped := range c.groupScratch {
				c.release(grouped)
			}
			c.drainFailed()
			return
		}
		locked, lockErr := c.lockFrameWrites(c.commitScratch)
		if lockErr != nil {
			c.setFailure(lockErr)
			for _, grouped := range c.groupScratch {
				c.release(grouped)
			}
			c.drainFailed()
			return
		}
		commitErr := device.Commit(c.commitScratch, latest.root)
		c.unlockFrameWrites(c.commitScratch, locked)
		if commitErr != nil {
			c.setFailure(commitErr)
			for _, grouped := range c.groupScratch {
				c.release(grouped)
			}
			c.drainFailed()
			return
		}
		if latest.rootGeneration != 0 {
			c.nextRootSlot.Store(rootSlot ^ 1)
			fallback := c.durable.Load()
			if fallback == 0 {
				fallback = latest.generation
			}
			c.fallback.Store(fallback)
		}
		groupSize := uint32(len(c.groupScratch))
		c.deviceBytes.Add(committedBytes)
		c.deviceCommits.Add(1)
		c.batchesDone.Add(uint64(groupSize))
		for old := c.largestGroup.Load(); groupSize > old && !c.largestGroup.CompareAndSwap(old, groupSize); old = c.largestGroup.Load() {
		}
		c.durable.Store(latest.generation)
		if callbacks := c.callbacks.Load(); callbacks != nil &&
			callbacks.durable != nil {
			callbacks.durable(latest.generation)
		}
		settledGeneration := latest.generation
		for _, grouped := range c.groupScratch {
			c.release(grouped)
		}
		if c.options.ManualCheckpoint {
			c.prewritePulseDone.Store(false)
		}
		// A producer that has not recorded its state yet rechecks durable after
		// Publish, while Wait remains behind the completed callback and staging
		// recycle. Keeping these two generations separate closes both sides of
		// the publication race and makes a successful checkpoint immediately
		// reusable as bounded admission capacity.
		c.settled.Store(settledGeneration)
		c.broadcast()
	}
}

func (c *Committer) bindDeviceFrameArena(
	device Device, writes []Write,
) error {
	hasFrames := false
	for _, write := range writes {
		if write.frameNative() {
			hasFrames = true
			break
		}
	}
	if !hasFrames {
		return nil
	}
	cache := c.frameCache.Load()
	binder, ok := device.(frameArenaDevice)
	if cache == nil || !ok {
		return ErrUnsupported
	}
	return binder.bindFrameArena(cache.arena, cache.options.PageSize)
}

func (c *Committer) lockFrameWrites(writes []Write) (int, error) {
	cache := c.frameCache.Load()
	for index := range writes {
		write := &writes[index]
		if !write.frameNative() {
			continue
		}
		if cache == nil || int(write.frameIndex) >= len(cache.frames) {
			c.unlockFrameWrites(writes, index)
			return 0, ErrInvalidWrite
		}
		frame := &cache.frames[write.frameIndex]
		frame.lock.Lock()
		if frame.state != pageCacheReady ||
			frame.dirty == 0 ||
			frame.flags&pageCacheFrameWritePinned == 0 ||
			frame.key.offset != uint64(write.Offset) ||
			frame.key.length != write.Length ||
			frame.key.kind != write.kind {
			frame.lock.Unlock()
			c.unlockFrameWrites(writes, index)
			return 0, fmt.Errorf(
				"%w: cache frame changed before checkpoint",
				ErrInvalidWrite,
			)
		}
		if frame.flags&pageCacheFrameNeedsReseal != 0 {
			page := cache.extentBytes(int(write.frameIndex), write.Length)
			_, err := sealPage(page, false)
			if err != nil {
				frame.lock.Unlock()
				c.unlockFrameWrites(writes, index)
				return 0, fmt.Errorf(
					"%w: reseal frame before checkpoint: %v",
					ErrInvalidWrite, err,
				)
			}
			frame.flags &^= pageCacheFrameNeedsReseal
		}
	}
	return len(writes), nil
}

func (c *Committer) unlockFrameWrites(writes []Write, through int) {
	cache := c.frameCache.Load()
	for index := through - 1; index >= 0; index-- {
		if writes[index].frameNative() {
			cache.frames[writes[index].frameIndex].lock.Unlock()
		}
	}
}

// waitForCheckpointRequest keeps a manual committer completely off the device
// until Flush or Close authorizes the generation at the head of the queue.
// The request is a generation cut: publications newer than it remain buffered
// for a later checkpoint.
func (c *Committer) waitForCheckpointRequest() bool {
	if !c.options.ManualCheckpoint {
		return true
	}
	for {
		prewritePulsed := false
		head := c.head.Load()
		if head != c.tail.Load() {
			batch := c.pending[head&c.pendingMask]
			if batch != nil && batch.generation <= c.checkpointThrough.Load() {
				return true
			}
			if c.prewritePressure() {
				progress, err := c.prewriteOldestSealed()
				if err != nil {
					c.setFailure(err)
					return false
				}
				prewritePulsed = progress
			}
		}
		c.workerWait.Store(1)
		head = c.head.Load()
		if head != c.tail.Load() {
			batch := c.pending[head&c.pendingMask]
			if batch != nil && batch.generation <= c.checkpointThrough.Load() {
				c.workerWait.Store(0)
				continue
			}
			if !prewritePulsed && c.prewritePressure() {
				c.workerWait.Store(0)
				continue
			}
		}
		select {
		case <-c.wake:
		case <-c.stop:
			c.workerWait.Store(0)
			if c.head.Load() == c.tail.Load() {
				return false
			}
			continue
		}
		c.workerWait.Store(0)
	}
}

func (c *Committer) prewritePressure() bool {
	if !c.options.ManualCheckpoint {
		return false
	}
	if c.prewriteDisabled.Load() {
		return false
	}
	if c.prewritePulseDone.Load() {
		return false
	}
	if c.options.prewriteGenerationWatermark > 0 &&
		c.tail.Load()-c.head.Load() >=
			uint64(c.options.prewriteGenerationWatermark) {
		return true
	}
	cache := c.frameCache.Load()
	if cache == nil {
		return false
	}
	capacity := uint64(len(cache.frames)) * uint64(cache.options.PageSize)
	return cache.DirtyCapacityAvailable() <= capacity/2
}

func (c *Committer) prewriteStillCurrent(write Write) bool {
	cache := c.frameCache.Load()
	if cache == nil || !write.frameNative() ||
		int(write.frameIndex) >= len(cache.frames) {
		return false
	}
	frame := &cache.frames[write.frameIndex]
	frame.lock.Lock()
	current := frame.state == pageCacheReady && frame.dirty != 0 &&
		frame.flags&pageCacheFrameWritePinned != 0 &&
		frame.flags&pageCacheFrameNeedsReseal == 0 &&
		frame.key.offset == uint64(write.Offset) &&
		frame.key.length == write.Length && frame.key.kind == write.kind
	frame.lock.Unlock()
	return current
}

// prewriteOldestSealed submits a small oldest-first pulse without a barrier or
// root. Holding manualMu keeps producer-side supersession from recycling a
// selected frame, and the frame locks keep the bytes stable through submission.
func (c *Committer) prewriteOldestSealed() (bool, error) {
	device, ok := c.device.(prewriteDevice)
	if !ok {
		return false, nil
	}
	c.manualMu.Lock()
	defer c.manualMu.Unlock()
	cache := c.frameCache.Load()
	if cache == nil {
		return false, nil
	}
	writes := c.commitScratch[:0]
	head, tail := c.head.Load(), c.tail.Load()
	for position := head; position < tail && len(writes) < opportunisticPrewriteLimit; position++ {
		batch := c.pending[position&c.pendingMask]
		if batch == nil || batch.state.Load() != batchPublished {
			continue
		}
		for index := range batch.pages {
			write := &batch.pages[index]
			if len(writes) == opportunisticPrewriteLimit {
				break
			}
			if !write.frameNative() ||
				write.pendingFlags&(pendingWriteSuperseded|pendingWritePrewritten) != 0 ||
				int(write.frameIndex) >= len(cache.frames) {
				continue
			}
			frame := &cache.frames[write.frameIndex]
			frame.lock.Lock()
			if frame.state != pageCacheReady || frame.dirty == 0 ||
				frame.flags&pageCacheFrameWritePinned == 0 ||
				frame.flags&pageCacheFrameNeedsReseal != 0 ||
				frame.key.offset != uint64(write.Offset) ||
				frame.key.length != write.Length || frame.key.kind != write.kind {
				frame.lock.Unlock()
				continue
			}
			writes = append(writes, *write)
		}
	}
	if len(writes) == 0 {
		return false, nil
	}
	slices.SortFunc(writes, func(a, b Write) int {
		switch {
		case a.Offset < b.Offset:
			return -1
		case a.Offset > b.Offset:
			return 1
		default:
			return 0
		}
	})
	if err := c.bindDeviceFrameArena(c.device, writes); err != nil {
		c.unlockFrameWrites(writes, len(writes))
		c.prewriteDisabled.Store(true)
		return false, nil
	}
	err := device.Prewrite(writes)
	c.unlockFrameWrites(writes, len(writes))
	if err != nil {
		// This operation is deliberately opportunistic. A partial or failed
		// pre-write names no root, so disable further pulses and let the real
		// checkpoint retry the complete page set and report any durable error.
		c.prewriteDisabled.Store(true)
		return false, nil
	}
	for _, written := range writes {
		for position := head; position < tail; position++ {
			batch := c.pending[position&c.pendingMask]
			if batch == nil {
				continue
			}
			for index := range batch.pages {
				write := &batch.pages[index]
				if write.frameIndex == written.frameIndex &&
					write.Offset == written.Offset && write.Length == written.Length {
					write.pendingFlags |= pendingWritePrewritten
					break
				}
			}
		}
		c.prewrittenPageWrites.Add(1)
		c.prewrittenPageBytes.Add(uint64(written.Length))
		c.deviceBytes.Add(uint64(written.Length))
	}
	c.prewritePulseDone.Store(true)
	return true, nil
}

// coalesceWorthwhile reports whether waiting for company can pay for itself.
// Another generation already queued makes grouping certain. Otherwise the
// window is free only while nobody is blocked on this fence: a caller inside
// Wait has not been acknowledged yet, so every microsecond spent hoping for a
// neighbour is added directly to its latency, and a lone synchronous writer
// cannot even begin the neighbour it is being made to wait for.
func (c *Committer) coalesceWorthwhile() bool {
	return c.head.Load() != c.tail.Load() || c.waiters.Load() == 0
}

func (c *Committer) nextBatch(wait bool) (*Batch, bool) {
	for {
		if c.options.ManualCheckpoint {
			c.manualMu.Lock()
		}
		head := c.head.Load()
		if head != c.tail.Load() {
			batch := c.pending[head&c.pendingMask]
			c.pending[head&c.pendingMask] = nil
			c.head.Store(head + 1)
			if c.options.ManualCheckpoint {
				c.manualMu.Unlock()
			}
			return batch, true
		}
		if c.options.ManualCheckpoint {
			c.manualMu.Unlock()
		}
		if !wait {
			return nil, false
		}
		c.workerWait.Store(1)
		if c.head.Load() != c.tail.Load() {
			c.workerWait.Store(0)
			continue
		}
		select {
		case <-c.wake:
		case <-c.stop:
			c.workerWait.Store(0)
			if c.head.Load() == c.tail.Load() {
				return nil, false
			}
			continue
		}
		c.workerWait.Store(0)
	}
}

func (c *Committer) peekBatch() (*Batch, bool) {
	head := c.head.Load()
	if head == c.tail.Load() {
		return nil, false
	}
	return c.pending[head&c.pendingMask], true
}

func (c *Committer) drainFailed() {
	for {
		batch, ok := c.nextBatch(false)
		if !ok {
			return
		}
		c.release(batch)
	}
}

func (c *Committer) setFailure(err error) {
	if err == nil {
		return
	}
	c.failOnce.Do(func() {
		c.failure.Store(&commitFailure{err: err})
		if callbacks := c.callbacks.Load(); callbacks != nil &&
			callbacks.failed != nil {
			callbacks.failed(err)
		}
		c.stopAccepting()
		close(c.failed)
		c.broadcast()
	})
}

func (c *Committer) release(batch *Batch) {
	if c.options.ManualCheckpoint {
		c.manualMu.Lock()
		defer c.manualMu.Unlock()
	}
	released := batch.bufferIndexes[:0]
	for index, write := range batch.pages {
		if write.frameNative() {
			if cache := c.frameCache.Load(); cache != nil {
				cache.releaseFrameWrite(write, batch.generation)
			}
		}
		if index >= int(batch.dataBufferCount) {
			continue
		}
		if write.pendingFlags&pendingWriteSuperseded == 0 {
			released = append(released, uint32(write.Buffer))
		}
	}
	if batch.root.pendingFlags&pendingWriteSuperseded == 0 {
		released = append(released, uint32(batch.root.Buffer))
	}
	if batch.materialized {
		released = append(released, uint32(batch.journal.Buffer))
	}
	c.freeBuffers.pushN(released)
	batch.pages = batch.pages[:0]
	batch.root = Write{}
	batch.rootGeneration = 0
	batch.journal = Write{}
	batch.journalSequence = 0
	batch.journalSlot = 0
	batch.journalCapsuleChecksum = 0
	batch.materializationPatchCount = 0
	batch.materializationTargetMask = 0
	batch.materializationPatchChecksums = [MaterializationJournalMaxPatches]uint32{}
	batch.materialized = false
	batch.dataBufferCount = 0
	batch.publicationDescriptor = nil
	batch.expectedPreviousGeneration = 0
	batch.conditionalPublication = false
	batch.generation = 0
	batch.state.Store(batchFree)
	c.freeBatches.push(batch.index)
	if c.options.ManualCheckpoint {
		c.rebuildManualPendingLocked()
	}
}

func (c *Committer) broadcast() {
	c.waitMu.Lock()
	c.wait.Broadcast()
	c.waitMu.Unlock()
}
