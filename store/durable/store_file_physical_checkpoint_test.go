package durable

// flushPhysicalForTest forces the rooted page graph to the current visible
// generation. Production Flush is intentionally allowed to satisfy ordinary
// buffered durability with one recovery-journal delta, so tests that inspect
// physical pages, fold output, retirement, or locality must request this
// stronger test-only boundary explicitly.
func flushPhysicalForTest(c *Collection) error {
	if c == nil || c.committer == nil {
		return ErrClosed
	}
	if !c.buffered() && !c.syncJournalLane() {
		return c.Flush()
	}
	c.writer.Lock()
	defer c.writer.Unlock()
	if c.closed {
		return ErrClosed
	}
	return c.checkpointBufferedLocked()
}
