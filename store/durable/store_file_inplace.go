package durable

import (
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func (c *Collection) bufferedFirstTouchContains(ref storeio.PageRef) bool {
	return slices.Contains(c.bufferedFirstTouches, ref)
}

func (c *Collection) takeBufferedFirstTouch(ref storeio.PageRef) bool {
	for index := range c.bufferedFirstTouches {
		if c.bufferedFirstTouches[index] != ref {
			continue
		}
		last := len(c.bufferedFirstTouches) - 1
		c.bufferedFirstTouches[index] = c.bufferedFirstTouches[last]
		c.bufferedFirstTouches[last] = storeio.PageRef{}
		c.bufferedFirstTouches = c.bufferedFirstTouches[:last]
		return true
	}
	return false
}

func (c *Collection) bufferedFirstTouchCapacityAvailable() bool {
	return len(c.bufferedFirstTouches) < cap(c.bufferedFirstTouches)
}

func (c *Collection) rememberBufferedFirstTouch(ref storeio.PageRef) {
	if ref == (storeio.PageRef{}) || c.bufferedFirstTouchContains(ref) {
		return
	}
	if !c.bufferedFirstTouchCapacityAvailable() {
		c.bufferedFirstTouchOverflows.Add(1)
		c.bufferedInplaceFallbacks.Add(1)
		return
	}
	c.bufferedFirstTouches = append(c.bufferedFirstTouches, ref)
}

func (c *Collection) clearBufferedInplaceLocked() {
	clear(c.bufferedFirstTouches)
	c.bufferedFirstTouches = c.bufferedFirstTouches[:0]
}

func (c *Collection) checkpointBufferedLocked() error {
	if err := c.materializePrimaryParentsLocked(); err != nil {
		return err
	}
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
	c.clearBufferedInplaceLocked()
	return nil
}
