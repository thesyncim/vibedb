package driver

import (
	"github.com/thesyncim/vibedb/internal/txnclock"
	"github.com/thesyncim/vibedb/store/durable"
)

// txConflictHistoryKeys caps retained per-table conflict history independently
// of table size or transaction lifetime. Reaching it is deliberately rare:
// the oldest active transaction is conservatively marked conflicting, history
// restarts at the overflowing write, and transactions begun afterwards retain
// exact per-key semantics.
const txConflictHistoryKeys = txnclock.HistoryKeys

const (
	txSerializableReadKeys  = txnclock.HistoryKeys
	txSerializableReadBytes = 1 << 20
)

// txConflictClock is the SQL driver's always-armed view of txnclock.Clock.
//
// Every method is called with database.mu held. The driver arms once on the
// first recorded write and never disarms: under db.mu the unarmed fast path is
// unused, matching the pre-extraction always-on semantics. writes and active
// mirror the inner clock's history maps so in-package white-box tests keep
// their existing field shape without touching other driver files.
type txConflictClock struct {
	clock       txnclock.Clock
	driverArmed bool
	writes      map[uint64]uint64
	active      map[uint64]uint32
}

func (c *txConflictClock) syncView() {
	c.writes = c.clock.Writes
	c.active = c.clock.Active
}

func (c *txConflictClock) armDriver() {
	if c.driverArmed {
		return
	}
	c.clock.Arm()
	c.driverArmed = true
}

func (c *txConflictClock) begin() uint64 {
	revision := c.clock.Begin()
	c.syncView()
	return revision
}

func (c *txConflictClock) conflict(
	begin uint64,
	keys []string,
) (key string, historyOverflow, conflict bool) {
	return c.clock.Conflict(begin, keys)
}

func (c *txConflictClock) changedSince(begin uint64) bool {
	return c.clock.ChangedSince(begin)
}

func (c *txConflictClock) observe() uint64 {
	return c.clock.Observe()
}

func (c *txConflictClock) finish(begin uint64) {
	c.clock.Finish(begin)
	c.syncView()
}

func (c *txConflictClock) recordKeys(keys []string) {
	if len(keys) == 0 {
		return
	}
	c.armDriver()
	c.clock.RecordKeys(keys)
	c.syncView()
}

func (c *txConflictClock) recordBinary(keys txnclock.BinaryKeys) {
	if keys == nil || keys.Len() == 0 {
		return
	}
	c.armDriver()
	c.clock.RecordBinary(keys)
	c.syncView()
}

func (c *txConflictClock) recordWriteIfNoActive() bool {
	c.armDriver()
	if !c.clock.RecordWriteIfNoActive() {
		return false
	}
	c.syncView()
	return true
}

func (c *txConflictClock) recordSeeds(seeds []seedDocument) {
	if len(seeds) == 0 {
		return
	}
	keys := make([]string, len(seeds))
	for i := range seeds {
		keys[i] = seeds[i].key
	}
	c.recordKeys(keys)
}

func (c *txConflictClock) recordTransaction(state *txTable) {
	keys := make([]string, 0, len(state.order))
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.existed || !mutation.remove {
			keys = append(keys, key)
		}
	}
	c.recordKeys(keys)
}

// collectionMutationPublished recognizes both an ordinarily visible
// publication and a durability failure whose in-process outcome is uncertain.
// The latter must advance the logical clock conservatively: allowing a
// transaction that began before that write to commit would reopen the ABA hole.
func collectionMutationPublished(
	collection *durable.Collection,
	before uint64,
	err error,
) bool {
	if collection == nil {
		return false
	}
	if collection.Generation() != before {
		return true
	}
	return err != nil && collection.PersistenceError() != nil
}
