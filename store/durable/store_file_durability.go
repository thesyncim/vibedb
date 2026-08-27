package durable

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func publicReaderLeaseError(err error) error {
	if errors.Is(err, storeio.ErrGenerationLeasesClosed) {
		return ErrClosed
	}
	return err
}

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

// syncJournalLane reports whether this collection acknowledges DurabilitySync
// mutations through the recovery journal. It is true only for a synchronous
// store whose journal is open, which is exactly a primary-layout synchronous
// store (the journal has a mutation lane only there and is minted
// unconditionally for it). On this lane the redo record is appended and synced
// BEFORE the mutation is applied and published, so visibility strictly follows
// durability and no reader observes an un-durable acknowledged mutation.
func (c *Collection) syncJournalLane() bool {
	return c.synchronous() && c.journalEnabled()
}

// chainFenceSync reports whether a synchronous collection still acknowledges
// through the committer's root fence (publish, then wait) rather than the
// journal. One configuration reaches it today: a store created async-visible
// and reopened DurabilitySync carries no journal to open, so its sync lane
// falls back to the fence. It is the only configuration that transiently holds
// a visible generation ahead of its durable one, so it is precisely the set
// the fail-closed read path still has to guard.
func (c *Collection) chainFenceSync() bool {
	return c.synchronous() && !c.journalEnabled()
}

// deferredCanonicalLane reports whether mutations are applied to canonical
// in-memory frames and published with the durable root deferred to a
// checkpoint, rather than through a per-mutation committer generation.
// Buffered-visible always uses it; the journal-backed synchronous lane joins
// it, differing only in that it makes each acknowledgement durable with a
// journal append plus sync before the frame is published.
func (c *Collection) deferredCanonicalLane() bool {
	return c.buffered() || c.syncJournalLane()
}

// canonicalFramePathEligible reports whether this mutation takes the canonical
// copy-on-write frame path rather than the committer full-generation path.
// Buffered-visible steps aside for the committer path while any reader is
// observable — a snapshot's lease or a direct point read's epoch slot, both
// consulted through anyActiveReaders — because it needs no per-mutation
// durability and can afford that path's fence. The journal-backed sync lane
// cannot: its committer runs in manual-checkpoint mode with no per-generation
// root fence to wait on, and it must acknowledge through the journal, so it
// stays on the copy-on-write canonical path even under an active reader (the
// old bytes are retained by the copy-on-write, and the retired frame's reclaim
// is deferred until every reader releases). This is a lane heuristic, not a
// veto: the canonical publish sites re-check reader presence under a raised
// reader fence, so a reader that arrives after this sample is still honored.
func (c *Collection) canonicalFramePathEligible() bool {
	return c.deferredCanonicalLane() &&
		(!c.anyActiveReaders() || c.syncJournalLane())
}

func (c *Collection) initializeFileState(state *fileStoreState) {
	c.state.Store(state)
	c.durableState.Store(state)
	c.visibleState.Store(state)
	c.resetPackedLogicalCut(state)
	if c.options.AutomaticCompaction.Enabled && state != nil {
		c.autoCompactionBaseFileEnd.Store(state.fileEnd)
		c.autoCompactionBaseGeneration.Store(state.root.Generation)
		c.autoCompactionArmed.Store(true)
	}
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
	c.recordPendingFileStateLocked(
		state, c.committer.DurableGeneration(),
	)
	c.visibilityMu.Unlock()
	// The physical pointer (including visibleState on the packed lane) is the
	// first half of a fold/structural publication. Resetting the packed cut last
	// closes the old-physical/new-reset race via readers' state-cut-state retry.
	if fileLogicalCutBeforeResetHook != nil {
		fileLogicalCutBeforeResetHook(state)
	}
	c.resetPackedLogicalCut(state)
	c.considerAutomaticCompaction(state)
}

// recordPendingFileStateLocked installs state in the fixed visibility ring
// before considering a durability promotion. That order is load-bearing when
// a grouped commit skips logical generations: a durable watermark may already
// cover the state being published, and the promotion scan must be able to
// select the just-recorded pointer in the same critical section.
//
// The caller holds visibilityMu.
func (c *Collection) recordPendingFileStateLocked(
	state *fileStoreState, durableGeneration uint64,
) {
	slot := state.root.Generation & uint64(len(c.pendingVisible)-1)
	c.pendingVisible[slot] = filePendingState{
		generation: state.root.Generation,
		state:      state,
	}
	// The journal-backed synchronous lane appended and synced its redo record
	// before this publish, so the state is already durable and may become
	// visible now. Only the chain-fence sync lane must withhold visibility until
	// its root fence lands (promoteDurableStateLocked).
	if !c.chainFenceSync() {
		c.visibleState.Store(state)
	}
	c.promoteDurableStateIfAdvancedLocked(durableGeneration)
}

// promoteDurableStateIfAdvancedLocked avoids an O(len(pendingVisible)) scan
// while the physical durable watermark is unchanged. In buffered-visible and
// journal-backed synchronous operation that is almost every mutation between
// physical checkpoints (the competitive shape configures 1,024 visibility
// slots). No pending publication can become newly eligible without the
// watermark advancing, so retaining the ring verbatim is equivalent.
//
// The caller holds visibilityMu. The result is true when the scan ran; it is
// exposed for the focused invariant tests and is otherwise intentionally
// ignored.
func (c *Collection) promoteDurableStateIfAdvancedLocked(
	generation uint64,
) bool {
	current := c.durableState.Load()
	if current != nil && generation <= current.root.Generation {
		return false
	}
	c.promoteDurableStateLocked(generation)
	return true
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
	// Only the chain-fence sync lane ties visibility to durability here; the
	// journal-backed sync lane already advanced visibleState at publish (its
	// record was durable then), and moving it back to the lagging checkpoint
	// root would regress reads.
	if c.chainFenceSync() {
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
	if c.buffered() || c.journalEnabled() {
		// Preserve the last admitted immutable view. Both buffered-visible and
		// the journal-backed sync lane publish only copy-on-write frames, so a
		// failed checkpoint cannot have modified any page reachable from the
		// retained view, and on the sync lane every visible generation is also
		// journal-durable.
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

// joinCatalogCommitOutcomeUnknown joins a multi-collection decision append or sync
// failure into this collection's sticky persistence-failure slot as
// ErrCommitOutcomeUnknown. A failed positional append may still have written a
// complete checksummed body, and a failed sync may still have persisted it, so
// every collection registered with the decision log must refuse
// further writes until reopen resolves the atomic all-or-nothing outcome.
// T4's TxnLog coordinator is the only caller; pre-append prepare failures must
// never reach this hook.
func joinCatalogCommitOutcomeUnknown(c *Collection, cause error) error {
	if c == nil {
		return nil
	}
	return c.poisonJournalCommitOutcomeUnknown(cause)
}

// PersistenceError reports the sticky I/O failure that stopped this
// collection's writer. A non-nil result requires Close and Open before any
// further mutation; errors.Is may match ErrCommitOutcomeUnknown when recovery
// must determine the last committed generation.
func (c *Collection) PersistenceError() error {
	if c == nil || c.committer == nil {
		return ErrClosed
	}
	committerFailure := c.committer.Failure()
	if box := c.journalFailure.Load(); box != nil {
		if committerFailure != nil {
			return errors.Join(committerFailure, box.err)
		}
		return box.err
	}
	return committerFailure
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
		// A journal-backed collection (buffered-visible, or the synchronous lane
		// whose records are synced before visibility) never holds a visible
		// generation the journal has not already made durable, so it retains its
		// last admitted view. A journal-less synchronous failure always fails
		// closed: its resident router may already name the rejected COW generation
		// even after visibleState rolls back. An unjournaled asynchronous
		// collection also fails closed when its visible state leads the durable
		// state or canonical materialization made recovery mandatory.
		if !c.buffered() && !c.journalEnabled() &&
			(c.chainFenceSync() ||
				c.options.MaterializationDamageGranule != 0 ||
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
		if c.buffered() || c.journalEnabled() {
			return c.visibleState.Load()
		}
		if c.options.MaterializationDamageGranule != 0 {
			return nil
		}
		return c.durableState.Load()
	}
	return c.visibleState.Load()
}
