package durable

func (c *Collection) checkpointBufferedLocked() error {
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	if err := c.materializePrimaryParentsLocked(primaryMaterializationCheckpoint); err != nil {
		return err
	}
	return c.flushBufferedPublishedLocked()
}

// flushBufferedPublishedLocked advances an already-published deferred cut to
// the device and releases the bounded committer/journal staging it occupied.
// It deliberately does not materialize the current overlay: materialization
// uses this helper when the committer reports pressure, after its failed
// transaction has unwound, then retries the still-intact overlay against the
// newly durable base.
func (c *Collection) flushBufferedPublishedLocked() error {
	return c.flushPublishedPhysicalLocked()
}

// flushPublishedPhysicalLocked is the common foreground physical boundary for
// a caller that already holds writer. Buffered and journal-backed synchronous
// lanes reach it after materialization; Close uses it directly for
// async-visible and chain-fence synchronous stores after publications have
// been drained.
func (c *Collection) flushPublishedPhysicalLocked() error {
	if err := c.committer.Flush(); err != nil {
		return err
	}
	return c.completePhysicalDurabilityLocked(
		c.committer.DurableGeneration(),
	)
}

// completePhysicalDurabilityLocked performs the post-fence ordering shared by
// all foreground durability lanes. The caller holds writer, and generation has
// completed its committer callback before entry.
func (c *Collection) completePhysicalDurabilityLocked(generation uint64) error {
	c.cache.MarkDurable(generation)
	// The store root is now durable through this generation. Recycling the
	// journal head past it is the journal half of the same publication: a crash
	// between the root fence and this recycle leaves the old header, whose records
	// recovery re-applies idempotently onto the newer root.
	if err := c.recycleRecoveryJournalLocked(
		generation,
	); err != nil {
		return err
	}
	// Only now are both authorities stable: the physical root makes the current
	// free set durable, and the recycled redo header proves recovery cannot need
	// an older logical suffix. Filesystem deallocation is optional and never
	// mutates allocator metadata, but it must not run before either fence.
	return c.punchNewPhysicalGenerationLocked(generation)
}
