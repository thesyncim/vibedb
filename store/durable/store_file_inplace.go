package durable

func (c *Collection) checkpointBufferedLocked() error {
	if failure := c.PersistenceError(); failure != nil {
		return failure
	}
	if err := c.materializePrimaryParentsLocked(); err != nil {
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
	if err := c.committer.Flush(); err != nil {
		return err
	}
	c.cache.MarkDurable(c.committer.DurableGeneration())
	// The store root is now durable through this generation. Recycling the
	// journal head past it is the journal half of the same publication: a crash
	// between the root fence and this recycle leaves the old header, whose records
	// recovery re-applies idempotently onto the newer root.
	if err := c.recycleRecoveryJournalLocked(
		c.committer.DurableGeneration(),
	); err != nil {
		return err
	}
	return nil
}
