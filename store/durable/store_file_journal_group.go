package durable

import (
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// journalCommitGroup amortizes the buffered-journal lane's per-mutation fsync
// across concurrent callers. It is phase 1 of the parallel-tablet-writers design
// (docs/design/parallel-tablet-writers.md §5, §11): group-committed
// acknowledgements on the EXISTING single writer, no tablet sharding.
//
// Each caller appends its OWN redo record under c.writer, so journal append
// order still equals publish order still equals generation order — the on-disk
// record stream and every crash-recovery invariant (scanTail monotonicity, torn
// tail truncation) are byte-for-byte what the per-caller path produced. The only
// thing that moves off c.writer is the sync: after depositing, the caller
// releases the writer and blocks here until a journal sync covering its record
// completes. The first waiter to find no sync in flight becomes the LEADER and
// issues one journal sync — outside c.writer — that covers every record appended
// so far, then wakes every waiter that sync covered. This is flat combining: no
// dedicated syncer goroutine and no handoff on the uncontended path (a lone
// writer becomes its own leader and syncs its single record, the pre-group
// behaviour exactly, so the single-client lane never regresses).
//
// Why "one sync covers all records appended before it" is the correct fence
// (design option (b), the replay-prefix argument): records are appended
// (file.WriteAt returns) under c.writer in strictly increasing sequence, and a
// caller returns success only after a sync ISSUED AFTER its append completed. A
// crash inside the window — records appended but the covering sync not yet
// issued, or issued but not yet returned — can lose only records whose callers
// never returned success, which is exactly the volatile window the
// buffered-visible acknowledgement contract already permits. Recovery replays
// whatever contiguous fsync'd prefix the device kept (a torn/unflushed tail
// self-truncates at the first invalid record), and no ACKNOWLEDGED record is
// ever lost because its caller's success strictly followed a completed covering
// sync. See store_file_journal_group_test.go for the multi-caller crash window.
//
// This grouping is deliberately NOT applied to the DurabilitySync lane
// (journalBeforePublishLocked), which must fence BEFORE publish. Option (b) there
// would require releasing c.writer between staging and publish, but a
// staged-but-unpublished mutation is invisible to a concurrent stager on the
// single writer, so the concurrent stager would derive from stale state (two
// same-key writers COWing the same base leaf, lost updates, non-monotonic
// publish generations). Amortizing the sync lane needs the phase-2 tablet tokens
// and shard frames that make concurrent staging coherent; it is phase 1b in the
// design. The buffered lane has no such hole: it publishes UNDER c.writer before
// depositing, so the only work left outside the writer is the sync itself.
type journalCommitGroup struct {
	// journal and powerSafe are the stable references a leader syncs through
	// outside c.writer. They are set once when the journal is paired and never
	// change for the collection's life — Recycle mutates the RecoveryJournal in
	// place, it does not swap the pointer — so a leader may read them with no
	// lock. Close drains every waiter (durabilityWait) before it closes the
	// file, so a leader's Sync never races the journal's Close.
	journal   *storeio.RecoveryJournal
	powerSafe bool
	// linger is Options.CommitCoalesce repurposed (design §5.3) as the optional
	// upper bound a leader waits after taking the sync slot for more deposits to
	// join its group. Zero — the default — takes neither the sleep nor any
	// allocation, so the natural sync-in-flight window is the only window.
	linger time.Duration

	mu   sync.Mutex
	cond sync.Cond
	// deposited counts redo records appended in append order; a caller's value
	// after its own append (its target) is what it waits to see durable. It is an
	// abstract monotonic ticket DECOUPLED from the journal's internal sequence,
	// so a Recycle that resets the journal sequence never perturbs a parked
	// waiter. It is advanced under c.writer AND this mu (a deposit holds both) and
	// read by the leader under this mu.
	deposited uint64
	// synced is the highest deposited ticket a completed sync — or a checkpoint
	// recycle, which folds every deposited record into the durable root — has
	// proven durable. A waiter returns once synced >= its target.
	synced uint64
	// syncing is true while a leader holds a sync in flight; arrivals queue and
	// are taken by the next leader, so the device's own sync latency is the
	// natural, self-tuning group window.
	syncing bool
	// failed is the sticky group poison: a leader whose sync fails records the
	// error here and every current and future waiter returns it (die-don't-retry),
	// mirroring poisonJournal for the shared fence. syncing is held true
	// across the poison so no second waiter can ever become leader and retry a
	// terminally failed sync.
	failed error
}

// initJournalGroupLocked wires the sequencer to the paired journal. It is called
// under c.writer from openRecoveryJournalLocked, before the collection is
// reachable, so no waiter can be parked when the references are installed.
func (c *Collection) initJournalGroupLocked() {
	g := &c.journalGroup
	g.journal = c.journal
	g.powerSafe = c.journalPowerSafe
	g.linger = c.options.CommitCoalesce
	g.cond.L = &g.mu
	g.deposited = 0
	g.synced = 0
	g.syncing = false
	g.failed = nil
}

// depositBump records one appended record and returns the caller's target
// ticket. It is called under c.writer, immediately after a successful journal
// Append, so ticket order equals append order.
func (g *journalCommitGroup) depositBump() uint64 {
	g.mu.Lock()
	g.deposited++
	t := g.deposited
	g.mu.Unlock()
	return t
}

// recycleAdvance marks every record deposited so far durable, because a
// checkpoint recycle folded them all into the durable root. It is called under
// c.writer from recycleRecoveryJournalLocked, only after the journal actually
// recycled, so any waiter parked on an earlier ticket completes against the new
// durable root instead of waiting for a sync of records the recycle discarded.
func (g *journalCommitGroup) recycleAdvance() {
	g.mu.Lock()
	if g.deposited > g.synced {
		g.synced = g.deposited
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

// fail poisons the shared fence so every current and future waiter returns err.
// It is called under c.writer when an append device error kills the journal, so
// no waiter is acknowledged out of a journal that can no longer be synced.
func (g *journalCommitGroup) fail(err error) {
	g.mu.Lock()
	if g.failed == nil {
		g.failed = err
	}
	g.cond.Broadcast()
	g.mu.Unlock()
}

// journalGroupAwait blocks until a journal sync (or a checkpoint recycle) has
// made the caller's deposited record durable, driving the sync itself as the
// group leader if no other caller is. It is called with NO collection lock held
// (the writer was released after the deposit). A sync failure poisons the
// collection die-don't-retry and returns the sticky error to every waiter the
// failed sync would have covered.
func (c *Collection) journalGroupAwait(target uint64) error {
	g := &c.journalGroup
	g.mu.Lock()
	for {
		if g.failed != nil {
			err := g.failed
			g.mu.Unlock()
			return err
		}
		if g.synced >= target {
			g.mu.Unlock()
			return nil
		}
		if g.syncing {
			g.cond.Wait()
			continue
		}
		// Become the leader for one sync covering everything appended so far.
		g.syncing = true
		g.mu.Unlock()
		if g.linger > 0 {
			// CommitCoalesce repurposed: linger to grow the group at the cost of
			// acknowledged latency. Only useful where the fence dwarfs the apply
			// (power-safe); default zero skips it entirely.
			time.Sleep(g.linger)
		}
		g.mu.Lock()
		captured := g.deposited
		groupSize := captured - g.synced
		g.mu.Unlock()

		err := g.journal.Sync(g.powerSafe)
		if err != nil {
			// syncing stays true across the poison so no other waiter becomes a
			// second leader and retries a terminally failed sync.
			poisoned := c.poisonJournal(err)
			g.mu.Lock()
			if g.failed == nil {
				g.failed = poisoned
			}
			g.syncing = false
			g.cond.Broadcast()
			g.mu.Unlock()
			return poisoned
		}
		c.journalSyncs.Add(1)
		for {
			prev := c.journalLargestGroup.Load()
			if uint32(groupSize) <= prev ||
				c.journalLargestGroup.CompareAndSwap(prev, uint32(groupSize)) {
				break
			}
		}
		g.mu.Lock()
		g.syncing = false
		if captured > g.synced {
			g.synced = captured
		}
		g.cond.Broadcast()
		// Loop: synced now covers target (target <= captured, since the caller
		// appended before it became leader), so the next iteration returns nil.
	}
}
