package storeio

const (
	pendingWriteSuperseded uint8 = 1 << iota
	pendingWriteTailWitness
	pendingWriteFrameNative
	pendingWritePrewritten
)

// coalesceManualPagesLocked recycles exact older page writes that the Store
// has proved unreachable from every live and future snapshot. The Store passes
// only retirements from the generation being accepted while holding its
// snapshot publication gate and after observing no active generation lease.
//
// A write already included in checkpointThrough cannot be removed: the worker
// is allowed to publish that earlier root without the retiring generation.
// Likewise, a batch already dequeued is invisible here and remains untouched.
//
// The highest pending write is retained even when unreachable. The newest root
// carries a monotonic FileEnd, and without an explicit truncate operation that
// write may be the only operation extending the file to the advertised length.
// Its exact retirement generation stays on the descriptor so a later farther
// write can release and report that witness without asking the Store to repeat
// an already-published retirement proof.
func (c *Committer) coalesceManualPagesLocked(
	batch *Batch,
	generation uint64,
	retired []PageRef,
	retirements []FreeExtent,
	superseded []FreeExtent,
) []FreeExtent {
	if c == nil || !c.options.ManualCheckpoint || batch == nil ||
		batch.materialized || retired == nil {
		return superseded
	}
	checkpointThrough := c.checkpointThrough.Load()
	highest := uint64(0)
	observe := func(write Write) {
		if write.pendingFlags&pendingWriteSuperseded != 0 ||
			write.Offset < 0 || write.Length == 0 {
			return
		}
		end := uint64(write.Offset) + uint64(write.Length)
		if end > highest {
			highest = end
		}
	}
	head, tail := c.head.Load(), c.tail.Load()
	for position := head; position < tail; position++ {
		pending := c.pending[position&c.pendingMask]
		if pending == nil || pending.state.Load() != batchPublished {
			continue
		}
		for _, write := range pending.pages {
			observe(write)
		}
	}
	for _, write := range batch.pages {
		observe(write)
	}
	// A page retained by an earlier publication solely to extend FileEnd stops
	// being a witness once another retained write reaches farther. Its original
	// retirement proof remains valid; the current no-snapshot publication proof
	// lets us recycle it without requiring the Store to report the retirement
	// a second time.
	for position := head; position < tail; position++ {
		previous := c.pending[position&c.pendingMask]
		if previous == nil || previous.state.Load() != batchPublished ||
			previous.generation <= checkpointThrough {
			continue
		}
		for index := range previous.pages {
			write := &previous.pages[index]
			if write.pendingFlags&pendingWriteTailWitness == 0 ||
				write.pendingFlags&pendingWriteSuperseded != 0 ||
				write.Offset < 0 || write.Length == 0 {
				continue
			}
			end := uint64(write.Offset) + uint64(write.Length)
			if end >= highest {
				continue
			}
			write.pendingFlags &^= pendingWriteTailWitness
			write.pendingFlags |= pendingWriteSuperseded
			if write.retiredGeneration != 0 &&
				write.retiredGeneration < generation &&
				len(superseded) < cap(superseded) {
				superseded = append(superseded, FreeExtent{
					Offset: uint64(write.Offset), Length: uint64(write.Length),
					RetiredGeneration: write.retiredGeneration,
				})
			}
			write.retiredGeneration = 0
			c.releaseSupersededPage(write, previous.generation)
			c.supersededPageWrites.Add(1)
			c.supersededPageBytes.Add(uint64(write.Length))
		}
	}

	for _, ref := range retired {
		if ref.Offset == 0 || ref.Length == 0 ||
			ref.Generation == 0 || ref.Generation >= generation ||
			ref.Offset > ^uint64(0)-uint64(ref.Length) {
			continue
		}
		for position := tail; position > head; position-- {
			previous := c.pending[(position-1)&c.pendingMask]
			if previous == nil || previous.state.Load() != batchPublished ||
				previous.generation <= checkpointThrough ||
				previous.generation != ref.Generation {
				continue
			}
			matched := false
			for index := range previous.pages {
				write := &previous.pages[index]
				if write.pendingFlags&pendingWriteSuperseded != 0 ||
					write.Offset < 0 ||
					uint64(write.Offset) != ref.Offset ||
					write.Length != ref.Length {
					continue
				}
				matched = true
				end := uint64(write.Offset) + uint64(write.Length)
				if end == highest {
					write.pendingFlags |= pendingWriteTailWitness
					for _, extent := range retirements {
						if extent.Offset == ref.Offset &&
							extent.Length == uint64(ref.Length) &&
							extent.RetiredGeneration != 0 &&
							extent.RetiredGeneration < generation {
							write.retiredGeneration =
								extent.RetiredGeneration
							break
						}
					}
					break
				}
				write.pendingFlags |= pendingWriteSuperseded
				c.releaseSupersededPage(write, previous.generation)
				c.supersededPageWrites.Add(1)
				c.supersededPageBytes.Add(uint64(write.Length))
				// Only the exact transition above proves that this physical
				// write can never become reachable from a durable root. Copy
				// its authoritative retirement record when the caller supplied
				// an exact physical match and bounded output room. Failure to
				// report is conservative: normal generation/fallback
				// reclamation still owns the extent.
				if len(superseded) < cap(superseded) {
					for _, extent := range retirements {
						if extent.Offset == ref.Offset &&
							extent.Length == uint64(ref.Length) &&
							extent.RetiredGeneration != 0 &&
							extent.RetiredGeneration < generation {
							superseded = append(
								superseded, extent,
							)
							break
						}
					}
				}
				write.retiredGeneration = 0
				break
			}
			if matched {
				break
			}
		}
	}
	return superseded
}

func (c *Committer) releaseSupersededPage(
	write *Write, generation uint64,
) {
	if write.frameNative() {
		if cache := c.frameCache.Load(); cache != nil {
			cache.releaseFrameWrite(*write, generation)
		}
		write.pendingFlags &^= pendingWriteFrameNative
		return
	}
	c.freeBuffers.push(uint32(write.Buffer))
}

// coalesceManualBatchLocked returns only an older alternate-superblock staging
// buffer. A newer buffered generation makes that descriptor unreachable by any
// future checkpoint, while the decoded state retained by live snapshots does
// not borrow it.
//
// Data and state-root pages deliberately remain owned by their batches. A
// snapshot of an older buffered generation may fault one of those pages after
// the checkpoint marks dirty cache entries durable; the committer has neither
// reader nor snapshot lease knowledge with which to prove such a page dead.
func (c *Committer) coalesceManualBatchLocked(
	batch *Batch,
	generation uint64,
) {
	if c == nil || !c.options.ManualCheckpoint || batch == nil ||
		batch.materialized {
		return
	}
	if previous := c.pendingRoot; previous != nil &&
		previous.generation > c.checkpointThrough.Load() &&
		previous.generation < generation &&
		previous.root.pendingFlags&pendingWriteSuperseded == 0 {
		previous.root.pendingFlags |= pendingWriteSuperseded
		c.freeBuffers.push(uint32(previous.root.Buffer))
		c.supersededRootWrites.Add(1)
		c.supersededRootBytes.Add(uint64(previous.root.Length))
	}
	c.pendingRoot = batch
}

// rebuildManualPendingLocked forgets committed batches and finds the newest
// alternate root still outside the completed checkpoint cut.
func (c *Committer) rebuildManualPendingLocked() {
	if c == nil || !c.options.ManualCheckpoint {
		return
	}
	c.pendingRoot = nil
	head, tail := c.head.Load(), c.tail.Load()
	for position := head; position < tail; position++ {
		batch := c.pending[position&c.pendingMask]
		if batch == nil || batch.state.Load() != batchPublished ||
			batch.root.pendingFlags&pendingWriteSuperseded != 0 {
			continue
		}
		c.pendingRoot = batch
	}
}
