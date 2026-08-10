// Package txnclock owns bounded first-committer-wins conflict histories shared
// by the SQL driver and the native facade.
//
// Record is gated by Arm/Disarm: when the armed count is zero, RecordKeys is an
// atomic fast-path check and a return. Begin, Finish, and Conflict stay live so a
// validation edge can observe publications that raced the arm. The SQL driver
// keeps its clocks always-armed under database.mu; the facade arms only while
// at least one read-write transaction is open.
package txnclock

import "sync/atomic"

// HistoryKeys caps retained per-collection conflict history independently of
// collection size or transaction lifetime. Reaching it is deliberately rare:
// the oldest active transaction is conservatively marked conflicting, history
// restarts at the overflowing write, and transactions begun afterwards retain
// exact per-key semantics.
const HistoryKeys = 4096

// Clock is a bounded, internally revisioned first-committer-wins conflict
// clock. ExternalHistory is the externally revisioned alternative.
//
// Writes retains only the newest write revision for keys that can still
// conflict with an active transaction. Once the oldest transaction finishes,
// Finish rebuilds the map without obsolete entries; once the last transaction
// finishes, it releases the map entirely. Live memory is therefore proportional
// to keys changed since the oldest active transaction began, rather than to the
// collection's lifetime.
//
// Active and Writes are exported so in-module adapters can mirror them for
// white-box tests without duplicating clock state.
type Clock struct {
	armed           atomic.Uint32
	revision        uint64
	revisionStopped bool
	historyFloor    uint64
	Active          map[uint64]uint32
	Writes          map[string]uint64
}

const (
	maxRevision    = ^uint64(0)
	maxActiveCount = ^uint32(0)
)

// Arm declares one armed holder. Matching Disarm releases it. RecordKeys is a
// no-op while the armed count is zero.
func (c *Clock) Arm() {
	for {
		cur := c.armed.Load()
		if cur == maxActiveCount {
			// MaxUint32 is a permanent saturated sentinel. It deliberately gives
			// up exact decrementability at the representational boundary so writes
			// can never become invisible after too many Disarms.
			return
		}
		if c.armed.CompareAndSwap(cur, cur+1) {
			return
		}
	}
}

// Disarm releases one armed holder. Disarm on a zero count is a no-op so a
// mismatched caller cannot wrap the counter.
func (c *Clock) Disarm() {
	for {
		cur := c.armed.Load()
		if cur == 0 || cur == maxActiveCount {
			return
		}
		if c.armed.CompareAndSwap(cur, cur-1) {
			return
		}
	}
}

// Armed reports the current armed-holder count.
func (c *Clock) Armed() uint32 {
	return c.armed.Load()
}

// Begin captures the current revision for a new transaction.
func (c *Clock) Begin() uint64 {
	revision := c.revision
	if c.Active == nil {
		c.Active = make(map[uint64]uint32)
	}
	count := c.Active[revision]
	if count >= maxActiveCount-1 {
		// MaxUint32 is a permanent saturated sentinel. Making the bucket
		// immortal prevents a later Finish from retiring history while an
		// uncounted transaction is still live.
		c.Active[revision] = maxActiveCount
		return revision
	}
	c.Active[revision] = count + 1
	return revision
}

// Conflict reports whether any of keys was written after begin. A begin at or
// below the history floor is an overflow conflict: the exact key set that
// collided was discarded when the bounded history reset.
func (c *Clock) Conflict(
	begin uint64,
	keys []string,
) (key string, historyOverflow, conflict bool) {
	if c.revisionStopped {
		return "", true, true
	}
	if begin < c.historyFloor {
		return "", true, true
	}
	// With no revision advance, no retained key can have been published after
	// an ordinary begin. MaxUint64 is different: Observe can return it without
	// registering an active holder, and a later write is then allowed to reuse
	// the maximum. The token carries no provenance, so a still-active observed
	// maximum cannot be distinguished from a Begin(maximum) registered after
	// that same-revision write. Fail the exhausted boundary closed rather than
	// infer safety from the current Active bucket. Keep the general fail-closed
	// guards above this fast path as well.
	if begin == c.revision {
		if begin == maxRevision {
			return "", true, true
		}
		return "", false, false
	}
	for _, key := range keys {
		if c.Writes[key] > begin {
			return key, false, true
		}
	}
	return "", false, false
}

// ChangedSince reports whether any write was recorded after begin. Unlike
// Conflict, it does not consult or consume the bounded exact-key history: the
// monotonic revision is sufficient for a relation-coarse read dependency.
// Revision exhaustion fails closed for the same reason as Conflict.
func (c *Clock) ChangedSince(begin uint64) bool {
	return c.revisionStopped || c.revision > begin
}

// Observe returns the current revision without registering another active
// holder. Callers use it to stamp work derived from a cut captured while their
// publication mutex is held; the transaction's original Begin token remains
// active and retains every exact-key history entry the newer stamp may need.
func (c *Clock) Observe() uint64 {
	return c.revision
}

// Finish drops one transaction begun at begin and releases history that no
// remaining active transaction can observe.
func (c *Clock) Finish(begin uint64) {
	count := c.Active[begin]
	switch count {
	case 0:
		return
	case maxActiveCount:
		// The exact number of holders is no longer representable, so this
		// revision can never safely be declared quiescent.
		return
	case 1:
		delete(c.Active, begin)
	default:
		c.Active[begin] = count - 1
	}
	if len(c.Active) == 0 {
		// Do not retain a historical high-water allocation when no transaction
		// can observe it.
		c.Active = nil
		c.Writes = nil
		c.historyFloor = 0
		return
	}
	if len(c.Writes) == 0 {
		return
	}
	oldest := c.revision
	haveExact := false
	haveObservedFence := false
	for revision := range c.Active {
		if revision < c.historyFloor {
			// A holder whose original Begin token predates the floor is doomed
			// for work stamped with that token, but it may also own newer,
			// unregistered Observe tokens. Those tokens are used for statement
			// cuts captured under the publication mutex. Keep the complete
			// post-floor history until every such holder finishes: pruning to a
			// newer registered Begin could otherwise discard a write visible to
			// an older holder's post-floor observation.
			haveObservedFence = true
			continue
		}
		if !haveExact || revision < oldest {
			oldest = revision
			haveExact = true
		}
	}
	if haveObservedFence {
		return
	}
	if !haveExact {
		c.Writes = nil
		return
	}
	remaining := 0
	for _, revision := range c.Writes {
		if revision > oldest {
			remaining++
		}
	}
	if remaining == len(c.Writes) {
		return
	}
	if remaining == 0 {
		c.Writes = nil
		return
	}
	// Rebuild instead of deleting in place so obsolete key storage becomes
	// collectible and the clock remains bounded by currently relevant writes.
	writes := make(map[string]uint64, remaining)
	for key, revision := range c.Writes {
		if revision > oldest {
			writes[key] = revision
		}
	}
	c.Writes = writes
}

// RecordKeys records a committed write set. When the clock is unarmed this is
// an atomic fast-path check and a return.
func (c *Clock) RecordKeys(keys []string) {
	if c.armed.Load() == 0 {
		return
	}
	c.recordKeys(keys)
}

func (c *Clock) recordKeys(keys []string) {
	if len(keys) == 0 || c.revisionStopped {
		return
	}
	newKeys := 0
	for i, key := range keys {
		if _, exists := c.Writes[key]; exists {
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
		c.Writes[key] = revision
	}
}

func (c *Clock) nextWrite(newKeys int) (uint64, bool) {
	if c.revision == maxRevision {
		if c.Active[maxRevision] != 0 {
			// A transaction begun at MaxUint64 cannot distinguish any later
			// write using this clock. Stop the clock permanently and make every
			// subsequent validation conflict rather than wrap to revision zero.
			c.revisionStopped = true
			c.Writes = nil
			return c.revision, false
		}
		// It is safe to reuse MaxUint64 while every active transaction began
		// before it: the maximum revision still compares newer than all of
		// their begin tokens. A begin at the maximum fences the next write.
	} else {
		c.revision++
	}
	if len(c.Active) == 0 {
		c.Writes = nil
		c.historyFloor = 0
		return c.revision, false
	}
	if len(c.Writes)+newKeys > HistoryKeys {
		// Transactions already active cannot distinguish a key overwritten
		// before this reset from one never touched. Doom exactly those older
		// transactions, release their key history, and let transactions begun
		// at or after this revision proceed with a fresh exact history.
		c.historyFloor = c.revision
		c.Writes = nil
		return c.revision, false
	}
	if c.Writes == nil {
		c.Writes = make(map[string]uint64, newKeys)
	}
	return c.revision, true
}
