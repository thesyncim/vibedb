package driver

import "github.com/thesyncim/vibedb/store/durable"

// txConflictHistoryKeys caps retained per-table conflict history independently
// of table size or transaction lifetime. Reaching it is deliberately rare:
// the oldest active transaction is conservatively marked conflicting, history
// restarts at the overflowing write, and transactions begun afterwards retain
// exact per-key semantics.
const txConflictHistoryKeys = 4096

// txConflictClock is a bounded, per-table first-committer-wins clock.
//
// Every method is called with database.mu held. writes retains only the newest
// write revision for keys that can still conflict with an active transaction.
// Once the oldest transaction finishes, finish rebuilds the map without
// obsolete entries; once the last transaction finishes, it releases the map
// entirely. Its live memory is therefore proportional to keys changed since
// the oldest active transaction began, rather than to the table's lifetime.
type txConflictClock struct {
	revision     uint64
	historyFloor uint64
	active       map[uint64]uint32
	writes       map[string]uint64
}

func (c *txConflictClock) begin() uint64 {
	revision := c.revision
	if c.active == nil {
		c.active = make(map[uint64]uint32)
	}
	c.active[revision]++
	return revision
}

func (c *txConflictClock) conflict(
	begin uint64,
	keys []string,
) (key string, historyOverflow, conflict bool) {
	if begin < c.historyFloor {
		return "", true, true
	}
	for _, key := range keys {
		if c.writes[key] > begin {
			return key, false, true
		}
	}
	return "", false, false
}

func (c *txConflictClock) finish(begin uint64) {
	count := c.active[begin]
	switch count {
	case 0:
		return
	case 1:
		delete(c.active, begin)
	default:
		c.active[begin] = count - 1
	}
	if len(c.active) == 0 {
		// Do not retain a historical high-water allocation when no transaction
		// can observe it.
		c.active = nil
		c.writes = nil
		c.historyFloor = 0
		return
	}
	if len(c.writes) == 0 {
		return
	}
	oldest := c.revision
	haveExact := false
	for revision := range c.active {
		if revision < c.historyFloor {
			continue
		}
		if !haveExact || revision < oldest {
			oldest = revision
			haveExact = true
		}
	}
	if !haveExact {
		c.writes = nil
		return
	}
	remaining := 0
	for _, revision := range c.writes {
		if revision > oldest {
			remaining++
		}
	}
	if remaining == len(c.writes) {
		return
	}
	if remaining == 0 {
		c.writes = nil
		return
	}
	// Rebuild instead of deleting in place so obsolete key storage becomes
	// collectible and the clock remains bounded by currently relevant writes.
	writes := make(map[string]uint64, remaining)
	for key, revision := range c.writes {
		if revision > oldest {
			writes[key] = revision
		}
	}
	c.writes = writes
}

func (c *txConflictClock) nextWrite(newKeys int) (uint64, bool) {
	c.revision++
	if len(c.active) == 0 {
		c.writes = nil
		c.historyFloor = 0
		return c.revision, false
	}
	if len(c.writes)+newKeys > txConflictHistoryKeys {
		// Transactions already active cannot distinguish a key overwritten
		// before this reset from one never touched. Doom exactly those older
		// transactions, release their key history, and let transactions begun
		// at or after this revision proceed with a fresh exact history.
		c.historyFloor = c.revision
		c.writes = nil
		return c.revision, false
	}
	if c.writes == nil {
		c.writes = make(map[string]uint64, newKeys)
	}
	return c.revision, true
}

func (c *txConflictClock) recordKeys(keys []string) {
	if len(keys) == 0 {
		return
	}
	newKeys := 0
	for i, key := range keys {
		if _, exists := c.writes[key]; exists {
			continue
		}
		duplicate := false
		for j := 0; j < i; j++ {
			if keys[j] == key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			newKeys++
		}
	}
	revision, retained := c.nextWrite(newKeys)
	if !retained {
		return
	}
	for _, key := range keys {
		c.writes[key] = revision
	}
}

func (c *txConflictClock) recordSeeds(seeds []seedDocument) {
	if len(seeds) == 0 {
		return
	}
	newKeys := 0
	for i, seed := range seeds {
		if _, exists := c.writes[seed.key]; exists {
			continue
		}
		duplicate := false
		for j := 0; j < i; j++ {
			if seeds[j].key == seed.key {
				duplicate = true
				break
			}
		}
		if !duplicate {
			newKeys++
		}
	}
	revision, retained := c.nextWrite(newKeys)
	if !retained {
		return
	}
	for _, seed := range seeds {
		c.writes[seed.key] = revision
	}
}

func (c *txConflictClock) recordTransaction(state *txTable) {
	effective := 0
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.existed || !mutation.remove {
			effective++
		}
	}
	if effective == 0 {
		return
	}
	newKeys := 0
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.existed || !mutation.remove {
			if _, exists := c.writes[key]; !exists {
				newKeys++
			}
		}
	}
	revision, retained := c.nextWrite(newKeys)
	if !retained {
		return
	}
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.existed || !mutation.remove {
			c.writes[key] = revision
		}
	}
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
