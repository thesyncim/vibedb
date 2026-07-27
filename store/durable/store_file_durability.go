package durable

const defaultFileVisibilitySlots = 64

type filePendingState struct {
	generation uint64
	state      *fileStoreState
}

func fileVisibilitySlots(configured int) int {
	if configured == 0 {
		return defaultFileVisibilitySlots
	}
	slots := 1
	for slots < configured {
		slots <<= 1
	}
	return slots
}

func (c *Collection) synchronous() bool {
	return c.options.Durability == DurabilitySync
}

func (c *Collection) buffered() bool {
	return c.options.Durability == DurabilityBufferedVisible
}

func (c *Collection) initializeFileState(state *fileStoreState) {
	c.state.Store(state)
	c.durableState.Store(state)
	c.visibleState.Store(state)
}

// publishFileState records the writer's applied state after the committer has
// accepted its generation. The fixed ring is bounded by the same queue that
// bounds unpublished durability work, so recording a generation allocates
// nothing.
func (c *Collection) publishFileState(state *fileStoreState) {
	c.state.Store(state)
	c.visibilityMu.Lock()
	if c.committer.Failure() != nil {
		c.visibilityMu.Unlock()
		return
	}
	slot := state.root.Generation & uint64(len(c.pendingVisible)-1)
	c.pendingVisible[slot] = filePendingState{
		generation: state.root.Generation,
		state:      state,
	}
	if !c.synchronous() {
		c.visibleState.Store(state)
	}
	c.promoteDurableStateLocked(c.committer.DurableGeneration())
	c.visibilityMu.Unlock()
}

// promoteDurableState is called by the persistence worker after the final
// root barrier. Group commit can skip logical generations, so it selects the
// newest recorded state at or below the durable generation instead of assuming
// an exact predecessor.
func (c *Collection) promoteDurableState(generation uint64) {
	c.snapshotGate.Lock()
	c.visibilityMu.Lock()
	c.promoteDurableStateLocked(generation)
	c.visibilityMu.Unlock()
	if c.cache != nil && c.options.MaterializationDamageGranule != 0 {
		// Canonical replacements remain pinned dirty until this exact fence.
		// Clearing them in the callback makes consecutive asynchronous updates
		// eligible without requiring an explicit Flush between mutations.
		c.cache.MarkDurable(generation)
	}
	c.snapshotGate.Unlock()
}

func (c *Collection) promoteDurableStateLocked(generation uint64) {
	current := c.durableState.Load()
	var candidate *fileStoreState
	if current != nil {
		candidate = current
	}
	for index := range c.pendingVisible {
		pending := &c.pendingVisible[index]
		if pending.state == nil || pending.generation > generation {
			continue
		}
		if candidate == nil ||
			pending.generation > candidate.root.Generation {
			candidate = pending.state
		}
		*pending = filePendingState{}
	}
	if candidate == nil ||
		current != nil && candidate.root.Generation <= current.root.Generation {
		return
	}
	c.durableState.Store(candidate)
	if c.synchronous() {
		c.visibleState.Store(candidate)
	}
}

// poisonPersistence rolls an automatically persisted asynchronous reader view
// back to the last confirmed root. Buffered-visible generations are different:
// their acknowledgement contract explicitly permits volatile state, and every
// page they reference remains owned by the failed committer/cache until Close,
// so reads may keep serving the already-acknowledged view. The committer
// remains sticky-failed in either case, rejecting every later mutation,
// checkpoint, and Close until the collection is reopened.
func (c *Collection) poisonPersistence(_ error) {
	c.snapshotGate.Lock()
	c.visibilityMu.Lock()
	// An asynchronous canonical replacement may have changed bytes addressed
	// by the last durable root before its journal/root sequence failed. Only
	// reopen recovery can repair/select that image, so reject reads instead of
	// pretending the retained root pointer is safe to serve live.
	if c.buffered() {
		// Preserve the last admitted immutable COW view. No canonical
		// materialization is allowed in this mode, so a failed checkpoint
		// cannot have modified any page reachable from the retained view.
	} else if !c.synchronous() &&
		c.options.MaterializationDamageGranule != 0 {
		c.visibleState.Store(nil)
	} else if durable := c.durableState.Load(); durable != nil {
		c.visibleState.Store(durable)
	}
	clear(c.pendingVisible)
	c.visibilityMu.Unlock()
	c.snapshotGate.Unlock()
}

// PersistenceError reports the sticky I/O failure that stopped this
// collection's writer. A non-nil result requires Close and Open before any
// further mutation; errors.Is may match ErrCommitOutcomeUnknown when recovery
// must determine the last committed generation.
func (c *Collection) PersistenceError() error {
	if c == nil || c.committer == nil {
		return ErrClosed
	}
	return c.committer.Failure()
}

// readerFileState is called while snapshotGate prevents the failure callback
// from changing the visible pointer underneath the decision. A raw committer
// failure is observable before that callback can acquire the gate, so the
// automatically persisted modes fail closed instead of briefly serving a
// volatile generation. Buffered-visible mode deliberately retains that
// acknowledged immutable view; canonical materialization always requires
// reopen recovery after failure.
func (c *Collection) readerFileState() (*fileStoreState, error) {
	state := c.visibleState.Load()
	failure := c.PersistenceError()
	if failure != nil {
		if !c.buffered() &&
			(c.options.MaterializationDamageGranule != 0 ||
				state != c.durableState.Load()) {
			return nil, failure
		}
	}
	if state == nil {
		if failure != nil {
			return nil, failure
		}
		return nil, ErrClosed
	}
	return state, nil
}

// readerFileStateNoError supplies the safe side of the same decision to
// observation methods without an error result. A failed canonical collection
// reports no live state until reopen; copy-on-write reports its last confirmed
// durable state even before the poison callback finishes.
func (c *Collection) readerFileStateNoError() *fileStoreState {
	if c == nil {
		return nil
	}
	if c.committer != nil && c.committer.Failure() != nil {
		if c.buffered() {
			return c.visibleState.Load()
		}
		if c.options.MaterializationDamageGranule != 0 {
			return nil
		}
		return c.durableState.Load()
	}
	return c.visibleState.Load()
}
